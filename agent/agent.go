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
	"../safedoozer"
	crand "crypto/rand"
	"flag"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"io"
	//	"github.com/xb95/singularity/safedoozer"
	logging "github.com/fluffle/golog/logging"
	"math/rand"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

type InfoMap map[string]string

var zmq_ctx zmq.Context
var dzr *safedoozer.Conn
var log logging.Logger
var gid, hostname string

func init() {
	// Create our new unique id
	buf := make([]byte, 16)
	io.ReadFull(crand.Reader, buf)
	gid = fmt.Sprintf("%x", buf)
}

func main() {
	var myhost = flag.String("hostname", "", "this machine's hostname")
	var dzrhost = flag.String("doozer", "localhost:8046",
		"host:port for doozer")
	flag.Parse()

	// Uses the nice golog package to handle logging arguments and flags
	// so we don't have to worry about it.
	log = logging.NewFromFlags()
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
		*myhost = myinfo["hostname"]
	} else {
		log.Warn("user requested hostname override (command line argument)")
		myinfo["hostname"] = *myhost
	}

	// Global, easy to access variable since we use it everywhere. This is safe
	// because it's read-only, too -- well, by definition. It isn't really. But
	// if it changes in maintainInfo then we exit.
	hostname = myinfo["hostname"]
	log.Info("client starting with hostname %s", hostname)

	// Now we have enough information to see if anybody else is claiming to be
	// this particular node. If so, we want to wait a bit and see if they
	// update again. If they are updating, then it is assumed they are running
	// okay, and we shouldn't start up again.
	lock := "/s/lock/" + hostname
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

	go maintainInfo(&myinfo)
	go maintainStanzas()
	runAgent() // Returns when dead.
}

// runAgent sets up a simple ZMQ device that reads requests from people who
// connect to us, passes them to some in-process worker goroutines, and
// then spits back out the responses.
func runAgent() {
	frontend, err := zmq_ctx.NewSocket(zmq.ROUTER)
	if err != nil {
		log.Fatal("failed to make zmq frontend: %s", err)
	}
	defer frontend.Close()

	err = frontend.Bind("tcp://*:7330")
	if err != nil {
		log.Fatal("failed to bind zmq frontend: %s", err)
	}

	backend, err := zmq_ctx.NewSocket(zmq.DEALER)
	if err != nil {
		log.Fatal("failed to make zmq backend: %s", err)
	}
	defer backend.Close()

	err = backend.Bind("ipc://agent.ipc")
	if err != nil {
		log.Fatal("failed to bind zmq frontend: %s", err)
	}

	// BUG(mark): We should make this detect when all workers are busy and then
	// spawn new ones? Automatically adjust for load? Or make it command line
	// configurable?
	for i := 0; i < 10; i++ {
		go func(id int) {
			sock, err := zmq_ctx.NewSocket(zmq.REP)
			if err != nil {
				log.Fatal("failed to make zmq worker: %s", err)
			}
			defer sock.Close()

			err = sock.Connect("ipc://agent.ipc")
			if err != nil {
				log.Fatal("failed to connect zmq worker: %s", err)
			}

			runAgentWorker(id, sock)
		}(i)
	}

	// This won't return.
	for {
		err = zmq.Device(zmq.QUEUE, frontend, backend)
		if err != nil {
			if err == syscall.EINTR {
				log.Warn("device interrupted, resuming")
				continue
			}
			log.Fatal("failed to create zmq device: %s", err)
		}
	}
}

func runAgentWorker(id int, sock zmq.Socket) {
	send := func(val string, args ...interface{}) {
		log.Debug("(worker %d) sending: %s", id, val)
		err := sock.Send([]byte(fmt.Sprintf(val, args)), 0)
		if err != nil {
			// BUG(mark): Handle error values. I'm uncertain what exactly an
			// error means here. Can we continue to use this socket, or do we
			// need to throw it away and make a new one?
			log.Warn("(worker %d) error on send: %s", id, err)
		}
	}

	log.Info("(worker %d) starting", id)
	for {
		data, err := sock.Recv(0)
		if err != nil {
			// Don't consider errors fatal, since it's probably just somebody
			// sending us junk data.
			// BUG(mark): Is that assertion valid? What if the socket has gone
			// wobbly and something is terrible?
			log.Warn("(worker %d) error reading from zmq socket: %s", id, err)
			continue
		}
		log.Debug("(worker %d) received: %s", id, string(data))

		parsed := strings.SplitN(strings.TrimSpace(string(data)), " ", 2)
		if len(parsed) < 1 {
			send("no command given")
			continue
		}

		switch parsed[0] {
		case "exec":
			if len(parsed) < 2 {
				send("exec requires an argument")
			} else {
				send(string(handleClientExec(parsed[1])))
			}
		case "doozer":
			send(dzr.Address)
		case "add_role":
			if len(parsed) < 2 {
				send("add_role requires an argument")
			} else {
				role := strings.TrimSpace(parsed[1])
				dzr.SetLatest(fmt.Sprintf("/s/role/%s/%s", role, hostname), "1")
				send("added role %s", role)
			}
		case "local_lock":
			if len(parsed) < 2 {
				send("local_lock requires an argument")
			} else {
				if tryLocalLock(parsed[1]) {
					send("locked")
				} else {
					send("failed")
				}
			}
		case "local_unlock":
			if len(parsed) < 2 {
				send("local_unlock requires an argument")
			} else {
				if localUnlock(parsed[1]) {
					send("unlocked")
				} else {
					send("not locked")
				}
			}
		case "global_lock":
			if len(parsed) < 2 {
				send("global_lock requires an argument")
			} else {
				if tryGlobalLock(parsed[1]) {
					send("locked")
				} else {
					send("failed")
				}
			}
		case "global_unlock":
			if len(parsed) < 2 {
				send("global_unlock requires an argument")
			} else {
				if globalUnlock(parsed[1]) {
					send("unlocked")
				} else {
					send("failed")
				}
			}
		case "die":
			send("dying")
			log.Fatal("somebody requested we die, good-bye cruel world!")
		default:
			send(fmt.Sprintf("unknown command: %s", parsed[0]))
		}
	}
}

// Executes a comma
func handleClientExec(command string) []byte {
	// We shell out to bash and execute the command to ensure that we don't
	// have to parse the command line ourselves.
	cmd := exec.Command("/bin/bash", "-c", command)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if _, ok := err.(*exec.ExitError); ok {
			// BUG(mark): when exit status is non-zero, add text to the end
			// advising the user of this
		} else {
			return []byte(fmt.Sprintf("failed to run: %s", err))
		}
	}
	return output
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

		// We update every second and dictate that people who are seeing if we
		// are lively check less often than that. Of course, if we do happen to
		// somehow not update, the next time we try we will self-destruct
		// the program. Should be safe. Ish.
		time.Sleep(1 * time.Second)
	}
}
