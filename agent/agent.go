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
	"flag"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"../safedoozer"
//	"github.com/xb95/singularity/safedoozer"
	logging "github.com/fluffle/golog/logging"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"time"
)

type InfoMap map[string]string

var zmq_ctx zmq.Context
var dzr *safedoozer.Conn
var log logging.Logger

func main() {
	var myhost = flag.String("hostname", "", "this machine's hostname")
	var dzrhost = flag.String("doozer", "localhost:8046",
		"host:port for doozer")
	flag.Parse()

	// Uses the nice golog package to handle logging arguments and flags
	// so we don't have to worry about it.
	log = logging.NewFromFlags()
	safedoozer.SetLogger(log)

	log.Info("starting up")
	rand.Seed(int64(time.Now().Nanosecond())) // Not the best but ok?

	dzr = safedoozer.Dial(*dzrhost)
	defer dzr.Close()

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
	log.Info("client starting with hostname %s", myinfo["hostname"])

	// Now we have enough information to see if anybody else is claiming to be
	// this particular node. If so, we want to wait a bit and see if they
	// update again. If they are updating, then it is assumed they are running
	// okay, and we shouldn't start up again.
	lock := "/s/lock/" + myinfo["hostname"]
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

	log.Info("attempting to get the node lock")
	nrev := dzr.Set(lock, rev, fmt.Sprintf("%d", time.Now().Unix()))
	if nrev <= rev {
		log.Fatal("failed to obtain the lock")
	}
	log.Info("lock successfully obtained! we are lively.")

	go maintainLock(lock, nrev)
	go maintainInfo(&myinfo)
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
	err = zmq.Device(zmq.QUEUE, frontend, backend)
	if err != nil {
		log.Fatal("failed to create zmq device: %s", err)
	}
}

func runAgentWorker(id int, sock zmq.Socket) {
	send := func(val string) {
		log.Debug("(worker %d) sending: %s", id, val)
		err := sock.Send([]byte(val), 0)
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
		case "local_lock":
			if len(parsed) < 2 {
				send("local_lock requires an argument")
			} else {
				if (tryLocalLock(parsed[1])) {
					send("locked")
				} else {
					send("failed")
				}
			}
		case "local_unlock":
			if len(parsed) < 2 {
				send("local_unlock requires an argument")
			} else {
				if (localUnlock(parsed[1])) {
					send("unlocked")
				} else {
					send("not locked")
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
			log.Fatal("failed to maintain lock, good-bye cruel world!")
		}
		curRev = nnrev

		// We update every second and dictate that people who are seeing if we
		// are lively check less often than that. Of course, if we do happen to
		// somehow not update, the next time we try we will self-destruct
		// the program. Should be safe. Ish.
		time.Sleep(1 * time.Second)
	}
}

func maintainInfo(info *InfoMap) {
	// We want to keep our original hostname. If it ever changes, we need to
	// bail out. Right now we're locking on hostname, so that means somebody
	// else might start up with the new name...
	hostname := (*info)["hostname"]
	path := "/s/node/" + hostname

	for {
		newmyinfo, err := getSelfInfo()
		if err != nil {
			log.Fatal("failed updating info: %s", err)
		}

		// Hostname change check, as noted above.
		newhostname, ok := newmyinfo["hostname"]
		if newhostname != hostname || !ok {
			log.Fatal("hostname changed mid-flight!")
		}

		// We can get the current repository revision because we are asserting
		// that nobody else is updating these keys, and that we will only touch
		// each key at most once. That way we don't have to track further revs.
		rev, err := dzr.Rev()
		if err != nil {
			log.Fatal("failed fetching current revision: %s", err)
		}

		// Delete keys that have vanished from Old to New.
		for key, _ := range *info {
			keypath := fmt.Sprintf("%s/%s", path, key)
			_, ok := newmyinfo[key]
			if !ok {
				err := dzr.Del(keypath, rev)
				if err != nil {
					log.Fatal("failed to delete %s: %s", keypath, err)
				}
				delete(*info, key)
			}
		}

		// Now we can just copy over things from New to Old (so that the global
		// structure is fine) and update Doozer.
		for key, _ := range newmyinfo {
			keypath := fmt.Sprintf("%s/%s", path, key)
			_, err := dzr.Conn.Set(keypath, rev, []byte(newmyinfo[key]))
			if err != nil {
				log.Fatal("failed to set %s: %s", keypath, err)
			}
			(*info)[key] = newmyinfo[key]
		}

		// To prevent stampeding (if you restart all of your clients near the
		// same time), vary the sleep time from 150..450 seconds.
		time.Sleep(time.Duration(150+rand.Intn(300)) * time.Second)
	}
}

func getSelfInfo() (InfoMap, error) {
	myinfo := make(InfoMap)

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	myinfo["hostname"] = hostname

	// For now, we are depending on facter to give us information about this
	// system. I think that I like that? We should also investigate if Chef has
	// a similar tool and if we can just use either.
	cmd := exec.Command("facter")
	output, err := cmd.Output()
	if err != nil {
		log.Fatal("failed to run facter: %s", err)
	}

	facts := strings.Split(string(output), "\n")
	for _, factline := range facts {
		fact := strings.SplitN(factline, " => ", 2)
		if len(fact) != 2 {
			continue
		}
		// _ is not allowed by Doozer (?!), but . is.
		myinfo[strings.Replace(fact[0], "_", ".", -1)] =
			strings.TrimSpace(fact[1])
	}
	return myinfo, nil
}