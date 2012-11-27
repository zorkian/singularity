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
			sock, err := zmq_ctx.NewSocket(zmq.DEALER)
			if err != nil {
				log.Fatal("failed to make zmq worker: %s", err)
			}
			defer sock.Close()

			err = sock.Connect("ipc://agent.ipc")
			if err != nil {
				log.Fatal("failed to connect zmq worker: %s", err)
			}

			runAgentWorker(id, &sock)
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

func runAgentWorker(id int, sock *zmq.Socket) {
	log.Info("(worker %d) starting", id)
	for {
		log.Debug("(worker %d) reading", id)
		remote, pb, err := singularity.ReadPb(sock)
		if err != nil {
			log.Error("(worker %d) error reading from zmq socket: %s", id, err)
			continue
		}

		// Make sure it's a Command packet. That's the only valid thing
		// at this point.
		if cmd, ok := pb.(*singularity.Command); ok {
			handleCommand(id, sock, remote, cmd)
			continue
		}

		log.Error("(worker %d) received unexpected protobuf: %v", id, pb)
	}
}

// handleCommand takes an input command and executes it.
func handleCommand(id int, sock *zmq.Socket, remote []byte, cmd *singularity.Command) {
	sendstr := func(output string) {
		err := singularity.QuickResponse(sock, remote, output)
		if err != nil {
			log.Error("(worker %d) failed to respond: %s", id, err)
		}
	}

	log.Debug("(worker %d) received: %s", id, cmd.Command)
	command := string(cmd.Command)
	if len(command) <= 0 {
		sendstr("no command given")
		return
	}

	var args []string
	for _, arg := range cmd.Args {
		args = append(args, string(arg))
	}

	switch command {
	case "exec":
		if len(args) != 1 {
			sendstr("exec requires exactly one argument")
		} else {
			err := handleClientExec(id, sock, remote, args[0])
			if err != nil {
				log.Error("(worker %d) failed exec: %s", id, err)
			}
		}
	case "doozer":
		sendstr(dzr.Address)
	case "add_role":
		if len(args) != 1 {
			sendstr("add_role requires exactly one argument")
		} else {
			role := strings.TrimSpace(args[0])
			dzr.SetLatest(fmt.Sprintf("/s/cfg/role/%s/%s", role, hostname), "1")
			sendstr(fmt.Sprintf("added role %s", role))
		}
	case "local_lock":
		if len(args) != 1 {
			sendstr("local_lock requires exactly one argument")
		} else {
			if tryLocalLock(args[0]) {
				sendstr("locked")
			} else {
				sendstr("failed")
			}
		}
	case "local_unlock":
		if len(args) != 1 {
			sendstr("local_unlock requires exactly one argument")
		} else {
			if localUnlock(args[0]) {
				sendstr("unlocked")
			} else {
				sendstr("not locked")
			}
		}
	case "global_lock":
		if len(args) != 1 {
			sendstr("global_lock requires exactly one argument")
		} else {
			if tryGlobalLock(args[0]) {
				sendstr("locked")
			} else {
				sendstr("failed")
			}
		}
	case "global_unlock":
		if len(args) != 1 {
			sendstr("global_unlock requires exactly one argument")
		} else {
			if globalUnlock(args[0]) {
				sendstr("unlocked")
			} else {
				sendstr("failed")
			}
		}
	case "die":
		sendstr("dying")
		log.Fatal("somebody requested we die, good-bye cruel world!")
	default:
		sendstr(fmt.Sprintf("unknown command: %s", command))
	}
}

// makeReaderChannel returns a channel that you can poll for data from the
// given io.ReadCloser. This also takes a bool channel that, when it has
// something in it, causes the goroutine to exit the next time we get a
// result from the pipe.
//
// This channel will write a nil when the socket has closed. Errors are
// currently suppressed.
func makeReaderChannel(rdr io.ReadCloser, exit chan bool) chan []byte {
	ch := make(chan []byte, 100)
	go func(inp io.ReadCloser) {
		for {
			if len(exit) > 0 {
				ch <- nil
				return
			}
			buf := make([]byte, 65536)
			ct, err := inp.Read(buf)
			if ct > 0 {
				ch <- buf[0:ct]
			}
			if err != nil {
				ch <- nil
				return
			}
		}
	}(rdr)
	return ch
}

// handleClientExec takes a command given to us from a client and executes it,
// passing the output back to the user as we get it.
func handleClientExec(id int, sock *zmq.Socket, remote []byte, command string) error {
	// We shell out to bash and execute the command to ensure that we don't
	// have to parse the command line ourselves.
	cmd := exec.Command("/bin/bash", "-c", command)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	defer stdout.Close()
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}
	defer stderr.Close()

	// Now set up a goroutine that monitors those outputs and feeds the
	// output back to the client. We can use a channel to wait for a
	// notification to exit the goroutine.
	exit := make(chan bool, 2) // Two to prevent blocking us.
	go func() {
		// Create chans for reading in stdin/stdout.
		ch_stdout := makeReaderChannel(stdout, exit)
		ch_stderr := makeReaderChannel(stderr, exit)

		// Cleanup function.
		defer func() {
			exit <- true
		}()

		// Now we want to watch for input on these channels and send it out
		// to the user. When one of these channels close, we consider the
		// entire system to be dead and kill everybody off.
		nilct := 0
		for {
			select {
			case <-exit:
				// This only happens in a timeout condition. In which case,
				// since we've now drained exit, another value gets put on
				// when we exit.
				return
			case buf := <-ch_stdout:
				if buf == nil {
					if nilct += 1; nilct == 2 {
						return
					}
					continue
				}
				singularity.WritePb(sock, remote,
					&singularity.CommandOutput{Stdout: buf})
			case buf := <-ch_stderr:
				if buf == nil {
					if nilct += 1; nilct == 2 {
						return
					}
					continue
				}
				singularity.WritePb(sock, remote,
					&singularity.CommandOutput{Stderr: buf})
			}
		}
	}()

	// Now we want to start the command, which spins it off in another
	// process...
	if err := cmd.Start(); err != nil {
		return err
	}

	// Now we can wait for it to run. For now, we sleep a second.
	timeout := time.Now().Add(5 * time.Second)
	for {
		if len(exit) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
		if time.Now().After(timeout) {
			log.Error("(worker %d) command timed out, killing", id)
			err := cmd.Process.Kill()
			if err != nil {
				log.Error("(worker %d) failed to kill: %s", id, err)
			}
			exit <- true
			break
		}
	}

	// By now, it should have run and be done.
	var retval int32 = 0
	singularity.WritePb(sock, remote,
		&singularity.CommandFinished{ExitCode: &retval})
	return nil
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
