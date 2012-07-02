/* singularity - client.go

   This is a CLI app for interacting with the Singularity cluster management
   tool. The goal here is to provide some functionality for connecting to
   the system and performing some commands.

*/

package main

import (
	"github.com/xb95/singularity/safedoozer"
	"code.google.com/p/gozmq/zmq"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// These are part of our generic wait framework.
type EmptyFunc func()
type WaitChan chan int

var zmq_ctx *zmq.Context
var dzr *safedoozer.Conn
var chatter = 1 // 0 = quiet, 1 = normal, 2 = verbose

func main() {
	var host = flag.String("H", "", "host (or hosts) to act on")
	var role = flag.String("R", "", "role (or roles) to act on")
	var all = flag.Bool("A", false, "act globally")
	var dzrhost = flag.String("doozer", "localhost:8046",
		"host:port for doozer")
	var quiet = flag.Bool("q", false, "be quiet")
	var verbose = flag.Bool("v", false, "be verbose")
	flag.Parse()

	args := flag.Args()

	if *verbose {
		chatter = 2
	} else if *quiet {
		chatter = 0
	}

	dzr = safedoozer.Dial(*dzrhost)
	defer dzr.Close()

	var err error // If we use := below, we shadow the global, which is bad.
	zmq_ctx, err = zmq.Init(1)
	if err != nil {
		fatal("failed to init zmq: %s", err)
	}
	defer zmq_ctx.Close()

	var hosts []string
	if *all {
		hosts = nodes()
	} else {
		if *host != "" {
			hosts = strings.Split(*host, ",")
			for i, _ := range hosts {
				hosts[i] = strings.TrimSpace(hosts[i])
			}
		}

		var roles []string
		if *role != "" {
			roles = strings.Split(*host, ",")
			for i, _ := range roles {
				roles[i] = strings.TrimSpace(roles[i])
			}
		}

		// BUG(mark): Now convert roles back to additional hosts.
	}

	if len(args) < 1 {
		fatal("no commands given")
	}

	switch args[0] {
	case "exec":
		if len(args) != 2 {
			fatal("exec requires exactly one argument")
		}
		waiter := &sync.WaitGroup{}
		for _, host := range hosts {
			waiter.Add(1)
			go func(host string) {
				doCommand(host, args[1])
				waiter.Done()
			}(host)
			waiter.Wait()
		}
		waiter.Wait()
	default:
		fatal("unknown command")
	}

	os.Exit(0)
}

func doCommand(host, command string) {
	info("[%s] executing: %s", host, command)

	sock := socketForHost(host)
	if sock == nil {
		warn("[%s] no socket available, skipping", host)
		return
	}

	start := time.Now()
	sock.SendString(fmt.Sprintf("exec %s", command), 0)
	resp, err := sock.RecvString(0)
	if err != nil {
		warn("[%s] failed: %s", host, err)
		return
	}
	duration := time.Now().Sub(start)

	// Annoying way to remove a trailing empty line? Maybe there is a better
	// way of doing this.
	split := strings.Split(resp, "\n")
	endpt := len(split) - 1
	for i := endpt; i >= 0; i-- {
		if split[i] != "" {
			break
		}
		endpt--
	}
	for idx, line := range split {
		if idx > endpt {
			break
		}
		warn("[%s] %s", host, line)
	}
	info("[%s] finished in %s", host, duration)
}

func nodes() []string {
	return dzr.GetdirLatest("/s/lock")
}

func socketForHost(host string) *zmq.Socket {
	// BUG(mark): We should be a little more fancy about how to get a socket to
	// the machine we're trying to reach.

	ip := dzr.GetLatest(fmt.Sprintf("/s/node/%s/ipaddress", host))
	if ip == "" {
		return nil
	}

	sock, err := zmq_ctx.Socket(zmq.REQ)
	if err != nil {
		fatal("failed to create zmq socket: %s", err)
	}

	// BUG(mark): ask zmq where a broker is and talk to them.
	err = sock.Connect(fmt.Sprintf("tcp://%s:7330", ip))
	if err != nil {
		fatal("failed to connect to agent: %s", err)
	}

	return sock
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
