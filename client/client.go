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
	"../safedoozer"
	logging "github.com/fluffle/golog/logging"
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
var log logging.Logger

func main() {
	var host = flag.String("H", "", "host (or hosts) to act on")
	var role = flag.String("R", "", "role (or roles) to act on")
	var all = flag.Bool("A", false, "act globally")
	var dzrhost = flag.String("doozer", "localhost:8046",
		"host:port for doozer")
	flag.Parse()

	// Uses the nice golog package to handle logging arguments and flags
	// so we don't have to worry about it.
	log = logging.NewFromFlags()
	safedoozer.SetLogger(log)

	dzr = safedoozer.Dial(*dzrhost)
	defer dzr.Close()

	var err error // If we use := below, we shadow the global, which is bad.
	zmq_ctx, err = zmq.NewContext()
	if err != nil {
		log.Fatal("failed to init zmq: %s", err)
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

	args := flag.Args()
	if len(args) < 1 {
		log.Fatal("no commands given")
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
			_ = <-hostsStatus
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
			log.Fatal("exec requires exactly one argument")
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
		log.Fatal("unknown command")
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
			status <- nil // Request a status update
			nextStatus = time.Now().Add(10 * time.Second)
			log.Info("waiting for: %s", strings.Join(<-status, " "))
		}
	}
}

func doCommand(host, command string) {
	log.Info("[%s] executing: %s", host, command)

	sock := socketForHost(host)
	if sock == nil {
		log.Warn("[%s] no socket available, skipping", host)
		return
	}

	start := time.Now()
	sock.Send([]byte(fmt.Sprintf("exec %s", command)), 0)
	resp, err := sock.Recv(0)
	if err != nil {
		log.Warn("[%s] failed: %s", host, err)
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
		log.Warn("[%s] %s", host, line)
	}
	log.Info("[%s] finished in %s", host, duration)
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
		log.Fatal("failed to create zmq socket: %s", err)
	}

	// BUG(mark): ask zmq where a broker is and talk to them.
	err = sock.Connect(fmt.Sprintf("tcp://%s:7330", ip))
	if err != nil {
		log.Fatal("failed to connect to agent: %s", err)
	}

	return sock
}