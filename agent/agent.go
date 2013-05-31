/* singularity - agent.go

   The agent runs on each server you wish to add to the monitoring system.
   This code connects to the main doozer cloud and ensures that this machine
   is in the list.

   This client is also responsible for doing interesting things like running
   commands and shuttling data around. This is supposed to be a very small
   footprint app.

   "I am a leaf on the wind." -Hoban 'Wash' Washburn

*/

package main

import (
	"../proto"
	"../safedoozer"
	crand "crypto/rand"
	"flag"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"io"
	//	"github.com/xb95/singularity/safedoozer"
	logging "github.com/fluffle/golog/logging"
	"math/rand"
	"time"
)

type InfoMap map[string]string
type messagePump func([]byte, interface{}) error

var zmq_ctx *zmq.Context
var dzr *safedoozer.Conn
var log logging.Logger
var gid, hostname string
var sendMessage messagePump

func init() {
	// Create our new unique id
	buf := make([]byte, 16)
	io.ReadFull(crand.Reader, buf)
	gid = fmt.Sprintf("%x", buf)
}

func main() {
	var myhost = flag.String("hostname", "", "this machine's hostname")
	var myport = flag.Int("port", 7330, "port number to listen on")
	var dzrhost = flag.String("doozer", "localhost:8046",
		"host:port for doozer")
	flag.Parse()

	// Uses the nice golog package to handle logging arguments and flags
	// so we don't have to worry about it.
	log = logging.InitFromFlags()
	safedoozer.SetLogger(log)

	log.Info("starting up - gid %s", gid)
	rand.Seed(int64(time.Now().Nanosecond())) // Not the best but ok?

	dzr = safedoozer.Dial(*dzrhost)
	defer dzr.Close()
	defer removeGlobalLocks()

	// see config.go for this
	initializeConfig()

	var err error // If we use := below, we shadow the global, which is bad.
	zmq_ctx, err = zmq.NewContext()
	if err != nil {
		log.Fatal("failed to init zmq: %s", err)
	}
	defer zmq_ctx.Close()

	myinfo, err := getSelfInfo()
	if err != nil {
		log.Fatal("Failed getting local information: %s", err)
	}
	if *myhost == "" {
		_, ok := myinfo["hostname"]
		if !ok {
			log.Fatal("getSelfInfo() did not return a hostname")
		}
		hostname = myinfo["hostname"]
	} else {
		log.Warn("user requested hostname override (command line argument)")
		hostname = *myhost
	}

	// Global, easy to access variable since we use it everywhere. This is safe
	// because it's read-only, too -- well, by definition. It isn't really. But
	// if it changes in maintainInfo then we exit.
	log.Info("client starting with hostname %s", hostname)

	// Now we have enough information to see if anybody else is claiming to be
	// this particular node. If so, we want to wait a bit and see if they
	// update again. If they are updating, then it is assumed they are running
	// okay, and we shouldn't start up again.
	lock := "/s/nlock/" + hostname
	rev := dzr.Stat(lock, nil)

	// If the lock is claimed, attempt to wait and see if the remote seems
	// to still be alive.
	if rev > 0 {
		log.Warn("node lock is claimed, waiting to see if it's lively")
		time.Sleep(3 * time.Second)

		nrev := dzr.Stat(lock, nil)
		if nrev > rev {
			log.Fatal("lock is lively, we can't continue!")
		}
	}

	// Safe to claim the node lock. Let's get it.
	log.Info("attempting to get the node lock")
	nrev := dzr.Set(lock, rev, fmt.Sprintf("%d", time.Now().Unix()))
	if nrev <= rev {
		log.Fatal("failed to obtain the lock")
	}
	log.Info("lock successfully obtained! we are lively.")
	go maintainLock(lock, nrev)

	// Now we want to reset the gid map, asserting that we are now the living
	// agent for this host.
	lock = "/s/gid/" + hostname
	rev = dzr.Stat(lock, nil)
	dzr.Set(lock, rev, gid)

	// There are certain miscellaneous tasks that need to get done somewhere,
	// so we use global locks to make sure that somebody is doing them. We
	// don't really care who.
	go manageGlobalFunc(5, "gf.expire-hosts", gfExpireHosts)
	go manageGlobalFunc(5, "gf.expire-glocks", gfExpireGlobalLocks)

	// Personal maintenance here.
	go maintainInfo(&myinfo)
	go maintainStanzas()
	runAgent(*myport) // Returns when dead.
}

// runAgent sets up a simple ZMQ device that reads requests from people who
// connect to us, passes them to some in-process worker goroutines, and
// then spits back out the responses.
func runAgent(port int) {
	frontend, err := zmq_ctx.NewSocket(zmq.ROUTER)
	if err != nil {
		log.Fatal("failed to make zmq frontend: %s", err)
	}
	defer frontend.Close()

	err = frontend.Bind(fmt.Sprintf("tcp://*:%d", port))
	if err != nil {
		log.Fatal("failed to bind zmq frontend: %s", err)
	}

	// This is a global function used by everybody to send messages out to
	// the clients.
	sendMessage = func(remote []byte, pb interface{}) error {
		return singularity.WritePb(frontend, remote, pb)
	}

	for {
		log.Debug("(pump) reading")
		remote, pb, err := singularity.ReadPb(frontend, 0)
		if err != nil {
			log.Error("(pump) error reading from zmq socket: %s", err)
			continue
		}

		worker := getWorker(remote)
		if worker == nil {
			log.Error("(pump) failed to get worker for remote")
			continue
		}

		select {
		case worker.input <- pb:
			// Nothing to do here, we sent it to the worker. That's all we care
			// about here.
		default:
			// We weren't able to send it. Is the channel full? For now, we
			// just discard packets.
			log.Error("(pump) discarding protobuf, unable to write to worker")
		}
	}
}

func maintainLock(lock string, curRev int64) {
	for {
		// FIXME: do we want to use the dzr.Conn.Set here? why?
		nnrev, err := dzr.Conn.Set(lock, curRev,
			[]byte(fmt.Sprintf("%d", time.Now().Unix())))
		if err != nil || nnrev <= curRev {
			log.Fatal("failed to maintain lock: %s", err)
		}
		curRev = nnrev

		// TODO(mark): Tv_ from #go-nuts pointed out that if this goroutine is
		// blocked on TCP things, that we won't die and someone else will take
		// our lock. That's a bad state.

		// We update every second and dictate that people who are seeing if we
		// are lively check less often than that. Of course, if we do happen to
		// somehow not update, the next time we try we will self-destruct
		// the program. Should be safe. Ish.
		time.Sleep(1 * time.Second)
	}
}
