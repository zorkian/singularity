/* singularity - client.go

   This is a CLI app for interacting with the Singularity cluster management
   tool. The goal here is to provide some functionality for connecting to
   the system and performing some commands.

*/

package main

import (
	"../safedoozer"
	"flag"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	logging "github.com/fluffle/golog/logging"
	"os"
	"strings"
)

// These are part of our generic wait framework.
type EmptyFunc func()
type WaitChan chan int

var zmq_ctx zmq.Context
var psock zmq.Socket
var dzr *safedoozer.Conn
var log logging.Logger

func main() {
	var host = flag.String("H", "", "host (or hosts) to act on")
	var role = flag.String("R", "", "role (or roles) to act on")
	var proxy = flag.String("P", "127.0.0.1", "agent to interact with")
	var all = flag.Bool("A", false, "act globally")
	var glock = flag.String("G", "", "global lock to claim")
	var dzrhost = flag.String("doozer", "localhost:8046",
		"host:port for doozer")
	var jobs = flag.Int("j", 0, "jobs to run in parallel")
	flag.Parse()

	// Uses the nice golog package to handle logging arguments and flags
	// so we don't have to worry about it.
	log = logging.NewFromFlags()
	safedoozer.SetLogger(log)

	// Do some simple argument validation.
	args := flag.Args()
	if len(args) < 1 {
		log.Error("no commands given")
		os.Exit(1)
	}
	if !isValidCommand(args[0]) {
		log.Error("invalid command: %s", args[0])
		os.Exit(1)
	}

	dzr = safedoozer.Dial(*dzrhost)
	defer dzr.Close()

	var err error // If we use := below, we shadow the global, which is bad.
	zmq_ctx, err = zmq.NewContext()
	if err != nil {
		log.Fatal("failed to init zmq: %s", err)
	}
	defer zmq_ctx.Close()

	// Connect to the proxy agent. This is the agent we will be having do all
	// of the work for us. This is probably localhost.
	psock = socketForIp(*proxy)
	if psock == nil {
		log.Error("unable to connect to proxy agent")
		os.Exit(1)
	}
	defer psock.Close()

	// Determine which nodes we will be addressing.
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

	// If we have been told to get a global lock, let's try to get that now.
	if *glock != "" {
		resp := proxyCommand(fmt.Sprintf("global_lock %s", *glock))
		if resp != "locked" {
			log.Error("failed to get global lock %s: %s", *glock, resp)
			os.Exit(1)
		}
	}

	// Queue up the jobs and then execute them.
	for _, host := range hosts {
		queueJob(host, args)
	}
	runJobs(*jobs) // Returns when jobs are done.

	// Unlock the lock we had.
	if *glock != "" {
		resp := proxyCommand(fmt.Sprintf("global_unlock %s", *glock))
		if resp != "unlocked" {
			log.Error("failed to release global lock %s: %s", *glock, resp)
			os.Exit(1)
		}
	}

	os.Exit(0)
}

func proxyCommand(cmd string) string {
	log.Debug("proxy command: %s", cmd)
	psock.Send([]byte(cmd), 0)
	resp, err := psock.Recv(0)
	if err != nil {
		log.Error("failed to read from proxy agent: %s", err)
		os.Exit(1)
	}
	log.Debug("proxy response: %s", resp)
	return string(resp)
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
	return socketForIp(ip)
}

func socketForIp(ip string) zmq.Socket {
	sock, err := zmq_ctx.NewSocket(zmq.REQ)
	if err != nil {
		log.Fatal("failed to create zmq socket: %s", err)
	}

	// BUG(mark): ask zmq where a broker is and talk to them.
	err = sock.Connect(fmt.Sprintf("tcp://%s:7330", ip))
	if err != nil {
		log.Error("failed to connect to agent: %s", err)
		os.Exit(1)
	}
	return sock
}
