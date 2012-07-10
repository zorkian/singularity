/* singularity - client.go

   This is a CLI app for interacting with the Singularity cluster management
   tool. The goal here is to provide some functionality for connecting to
   the system and performing some commands.

*/

package main

import (
	"flag"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"github.com/xb95/singularity/safedoozer"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// These are part of our generic wait framework.
type EmptyFunc func()
type WaitChan chan int

var zmq_ctx zmq.Context
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
	zmq_ctx, err = zmq.NewContext()
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

	// General purpose "hosts that still need to do X" logic, some commands
	// will use this and some won't care.
	hostsOutstanding := make(map[string]bool)
	waiter := &sync.WaitGroup{}
	hostDone := make(chan string)
	go func() {
		for {
			select {
			case host := <-hostDone:
				hostsOutstanding[host] = false
			}
		}
	}()
	hostsStatus := make(chan []string)
	go func() {
		// BUG(mark): This organization is actually kind of bad, since it means
		// we write out the current list and don't update it until someone polls.
		// This means that when someone asks for the status, they get the old
		// one from the instant the last person asked. This makes everything off
		// by one, basically. Annoying.
		for {
			var hosts []string
			for host, outstanding := range hostsOutstanding {
				if outstanding {
					hosts = append(hosts, host)
				}
			}
			hostsStatus <- hosts
		}
	}()
	for _, host := range hosts {
		hostsOutstanding[host] = true
	}

	switch args[0] {
	case "exec":
		if len(args) != 2 {
			fatal("exec requires exactly one argument")
		}

		// BUG(mark): It would be nice to abstract this functionality out
		// to some new level so we don't have to repeat it every command.
		for _, host := range hosts {
			waiter.Add(1)
			go func(host string) {
				doCommand(host, args[1])
				hostDone <- host
				waiter.Done()
			}(host)
		}
	default:
		fatal("unknown command")
	}

	doWait(waiter, hostsStatus)
	os.Exit(0)
}

func doWait(waiter *sync.WaitGroup, status chan []string) {
	done := make(chan bool)

	go func() {
		waiter.Wait()
		done <- true
	}()

	nextStatus := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-done:
			return
		default:
			// Do nothing.
		}

		time.Sleep(1 * time.Second)
		if time.Now().After(nextStatus) {
			nextStatus = time.Now().Add(10 * time.Second)
			info("waiting for: %s", strings.Join(<-status, " "))
		}
	}
}

func doCommand(host, command string) {
	info("[%s] executing: %s", host, command)

	sock := socketForHost(host)
	if sock == nil {
		warn("[%s] no socket available, skipping", host)
		return
	}

	start := time.Now()
	sock.Send([]byte(fmt.Sprintf("exec %s", command)), 0)
	resp, err := sock.Recv(0)
	if err != nil {
		warn("[%s] failed: %s", host, err)
		return
	}
	duration := time.Now().Sub(start)

	// Annoying way to remove a trailing empty line? Maybe there is a better
	// way of doing this.
	split := strings.Split(string(resp), "\n")
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

func socketForHost(host string) zmq.Socket {
	// BUG(mark): We should be a little more fancy about how to get a socket to
	// the machine we're trying to reach.

	ip := dzr.GetLatest(fmt.Sprintf("/s/node/%s/ipaddress", host))
	if ip == "" {
		return nil
	}

	sock, err := zmq_ctx.NewSocket(zmq.REQ)
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
