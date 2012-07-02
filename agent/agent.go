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
	"code.google.com/p/gozmq/zmq"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"time"
)

type InfoMap map[string]string

var zmq_ctx *zmq.Context
var dzr *safedoozer.Conn
var chatter = 1 // 0 = quiet, 1 = normal, 2 = verbose

func main() {
	var myhost = flag.String("hostname", "", "this machine's hostname")
	var dzrhost = flag.String("doozer", "localhost:8046",
		"host:port for doozer")
	var quiet = flag.Bool("q", false, "be quiet")
	var verbose = flag.Bool("v", false, "be verbose")
	flag.Parse()

	if *verbose {
		chatter = 2
	} else if *quiet {
		chatter = 0
	}

	info("starting up")
	rand.Seed(int64(time.Now().Nanosecond())) // Not the best but ok?

	dzr = safedoozer.Dial(*dzrhost)
	defer dzr.Close()

	var err error // If we use := below, we shadow the global, which is bad.
	zmq_ctx, err = zmq.Init(1)
	if err != nil {
		fatal("failed to init zmq: %s", err)
	}
	defer zmq_ctx.Close()

	myinfo, err := getSelfInfo()
	if err != nil {
		fatal("Failed getting local information: %s", err)
	}
	if *myhost == "" {
		_, ok := myinfo["hostname"]
		if !ok {
			fatal("getSelfInfo() did not return a hostname")
		}
		*myhost = myinfo["hostname"]
	} else {
		warn("user requested hostname override (command line argument)")
		myinfo["hostname"] = *myhost
	}
	info("client starting with hostname %s", myinfo["hostname"])

	// Now we have enough information to see if anybody else is claiming to be
	// this particular node. If so, we want to wait a bit and see if they
	// update again. If they are updating, then it is assumed they are running
	// okay, and we shouldn't start up again.
	lock := "/s/lock/" + myinfo["hostname"]
	rev := dzr.Stat(lock, nil)

	// If the lock is claimed, attempt to wait and see if the remote seems
	// to still be alive.
	if rev > 0 {
		warn("node lock is claimed, waiting to see if it's lively")
		time.Sleep(3 * time.Second)

		nrev := dzr.Stat(lock, nil)
		if nrev > rev {
			fatal("lock is lively, we can't continue!")
		}
	}

	info("attempting to get the node lock")
	nrev := dzr.Set(lock, rev, fmt.Sprintf("%d", time.Now().Unix()))
	if nrev <= rev {
		fatal("failed to obtain the lock")
	}
	info("lock successfully obtained! we are lively.")

	go maintainLock(lock, nrev)
	go maintainInfo(&myinfo)
	runClient() // Returns when dead.
}

func runClient() {
	// TODO: Given the nature of ZeroMQ's REQ-REP socket pairs, we can only
	// currently handle one outstanding request at a time. The rest will queue
	// up until we get to them. We could do some fancy work by having several
	// REP sockets created and put them in goroutines if we need to address
	// that. I haven't yet, because ultimately I want the agent to be very
	// lightweight. If we are taking substantial CPU (many concurrent requests)
	// then there is probably something inherently broken in what we're doing.
	//
	// Actually, we do want to support multiple simultaneous queries so that
	// people can write subsystems that do interesting things.
	//
	sock, err := zmq_ctx.Socket(zmq.REP)
	if err != nil {
		fatal("failed to make zmq socket: %s", err)
	}
	defer sock.Close()

	err = sock.Bind("tcp://*:7330")
	if err != nil {
		fatal("failed to bind zmq socket: %s", err)
	}

	for {
		data, err := sock.RecvString(0)
		if err != nil {
			// Don't consider errors fatal, since it's probably just somebody
			// sending us junk data.
			warn("error reading from zmq socket: %s", err)
			continue
		}

		parsed := strings.SplitN(strings.TrimSpace(data), " ", 2)
		if len(parsed) < 1 {
			sock.SendString("no command given", 0)
			continue
		}

		switch parsed[0] {
		case "exec":
			if len(parsed) < 2 {
				sock.SendString("exec requires an argument", 0)
			} else {
				sock.Send(handleClientExec(parsed[1]), 0)
			}
		case "die":
			sock.SendString("dying", 0)
			fatal("somebody requested we die, good-bye cruel world!")
		default:
			sock.SendString(fmt.Sprintf("unknown command: %s", parsed[0]), 0)
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
			fatal("failed to maintain lock, good-bye cruel world!")
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
			fatal("failed updating info: %s", err)
		}

		// Hostname change check, as noted above.
		newhostname, ok := newmyinfo["hostname"]
		if newhostname != hostname || !ok {
			fatal("hostname changed mid-flight!")
		}

		// We can get the current repository revision because we are asserting
		// that nobody else is updating these keys, and that we will only touch
		// each key at most once. That way we don't have to track further revs.
		rev, err := dzr.Rev()
		if err != nil {
			fatal("failed fetching current revision: %s", err)
		}

		// Delete keys that have vanished from Old to New.
		for key, _ := range *info {
			keypath := fmt.Sprintf("%s/%s", path, key)
			_, ok := newmyinfo[key]
			if !ok {
				err := dzr.Del(keypath, rev)
				if err != nil {
					fatal("failed to delete %s: %s", keypath, err)
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
				fatal("failed to set %s: %s", keypath, err)
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
		fatal("failed to run facter: %s", err)
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

func _log(minlvl int, fmt string, args ...interface{}) {
	if chatter >= minlvl {
		log.Printf(fmt, args...)
	}
}

func debug(fmt string, args ...interface{}) {
	_log(2, fmt, args...)
}

func info(fmt string, args ...interface{}) {
	_log(1, fmt, args...)
}

func warn(fmt string, args ...interface{}) {
	_log(0, fmt, args...)
}

func fatal(fmt string, args ...interface{}) {
	log.Fatalf(fmt, args...)
}