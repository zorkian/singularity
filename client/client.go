/* singularity - client.go

   This is a CLI app for interacting with the Singularity cluster management
   tool. The goal here is to provide some functionality for connecting to
   the system and performing some commands.

*/

package main

import (
	"../proto"
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
var psock *zmq.Socket
var dzr *safedoozer.Conn
var log logging.Logger
var timeout int
var hostname string

func main() {
	var host = flag.String("H", "", "host (or hosts) to act on")
	var role = flag.String("R", "", "role (or roles) to act on")
	var proxy = flag.String("P", "127.0.0.1", "agent to interact with")
	var all = flag.Bool("A", false, "act globally")
	var glock = flag.String("G", "", "global lock to claim")
	var llock = flag.String("L", "", "local lock to claim")
	var dzrhost = flag.String("doozer", "localhost:8046",
		"host:port for doozer")
	var jobs = flag.Int("j", 0, "jobs to run in parallel")
	var tout = flag.Int("t", 20, "timeout (in seconds) for jobs")
	flag.Parse()

	timeout = *tout
	if timeout < 0 {
		log.Error("timeout must be 0 (no timeout) or positive")
		os.Exit(1)
	}

	// Uses the nice golog package to handle logging arguments and flags
	// so we don't have to worry about it.
	log = logging.InitFromFlags()
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

	// Figure out localhost.
	var err error // If we use := below, we shadow the global, which is bad.
	hostname, err = os.Hostname()
	if err != nil {
		log.Error("failed getting hostname: %s", err)
		os.Exit(1)
	}
	hostname = strings.Split(hostname, ".")[0]
	if len(hostname) <= 0 {
		log.Error("hostname is empty!")
		os.Exit(1)
	}

	// Connect to our doozer host
	dzr = safedoozer.Dial(*dzrhost)
	defer dzr.Close()

	zmq_ctx, err = zmq.NewContext()
	if err != nil {
		log.Error("failed to init zmq: %s", err)
		os.Exit(1)
	}
	defer zmq_ctx.Close()

	// Connect to the proxy agent. This is the agent we will be having do all
	// of the work for us. This is probably localhost.
	psock = socketForIp(*proxy)
	if psock == nil {
		log.Error("unable to connect to proxy agent")
		os.Exit(1)
	}
	defer (*psock).Close()

	// Determine which nodes we will be addressing.
	hosts := make(map[string]bool)
	if *all {
		for _, host := range nodes() {
			hosts[host] = false
		}
	} else {
		if *host != "" {
			lhosts := strings.Split(*host, ",")
			for _, host := range lhosts {
				host = strings.TrimSpace(host)
				if len(host) > 0 {
					hosts[strings.TrimSpace(host)] = false
				}
			}
		}

		if *role != "" {
			roles := strings.Split(*role, ",")
			for _, role := range roles {
				lhosts := convertRoleToHosts(strings.TrimSpace(role))
				for _, host := range lhosts {
					hosts[host] = false
				}
			}
		}

		if *host == "" && *role == "" {
			// Both empty, default to this machine.
			hosts[hostname] = false
		}
	}

	// If no hosts, bail out.
	if len(hosts) <= 0 {
		log.Error("no hosts or roles specified")
		os.Exit(1)
	}

	// If we have been told to get a global lock, let's try to get that now.
	if *glock != "" {
		resp := proxyCommand("global_lock", *glock)
		if resp != "locked" {
			log.Error("failed to get global lock %s: %s", *glock, resp)
			os.Exit(1)
		}
		defer func(lock string) {
			resp := proxyCommand("global_unlock", *glock)
			if resp != "unlocked" {
				log.Error("failed to release global lock %s: %s", *glock, resp)
			}
		}(*glock)
	}

	// Same, local.
	if *llock != "" {
		resp := proxyCommand("local_lock", *llock)
		if resp != "locked" {
			log.Error("failed to get local lock %s: %s", *llock, resp)
			os.Exit(1)
		}
		defer func(lock string) {
			resp := proxyCommand("local_unlock", *llock)
			if resp != "unlocked" {
				log.Error("failed to release local lock %s: %s", *llock, resp)
			}
		}(*llock)
	}

	// Queue up the jobs and then execute them.
	for host, _ := range hosts {
		queueJob(host, args)
	}
	runJobs(*jobs) // Returns when jobs are done.
}

func proxyCommand(cmd string, args ...string) string {
	log.Debug("proxy command: %s %s", cmd, strings.Join(args, " "))

	bcmd := []byte(cmd)
	var bargs [][]byte
	for _, arg := range args {
		bargs = append(bargs, []byte(arg))
	}

	err := singularity.WritePb(psock, nil,
		&singularity.Command{Command: bcmd, Args: bargs})
	if err != nil {
		log.Error("failed to write: %s", err)
		os.Exit(1)
	}

	var stdout, stderr string
	for {
		_, resp, err := singularity.ReadPb(psock)
		if err != nil {
			log.Error("failed to read: %s", err)
			os.Exit(1)
		}

		switch resp.(type) {
		case *singularity.CommandOutput:
			stdout = stdout + string(resp.(*singularity.CommandOutput).Stdout)
			stderr = stderr + string(resp.(*singularity.CommandOutput).Stderr)
			if len(stderr) > 0 {
				log.Error(stderr)
				stderr = ""
			}
		case *singularity.CommandFinished:
			if retval := resp.(*singularity.CommandFinished).ExitCode; *retval != 0 {
				log.Error("return value: %d", *retval)
				os.Exit(1)
			}
			log.Debug("proxy response: %s", stdout)
			return stdout
		default:
			log.Error("unexpected protobuf: %v", resp)
			os.Exit(1)
		}
	}

	// We should never get here.
	log.Error("unexpected early exit")
	os.Exit(1)
	return "" // Heh.
}

func nodes() []string {
	return dzr.GetdirLatest("/s/nlock")
}

func socketForHost(host string) *zmq.Socket {
	// BUG(mark): We should be a little more fancy about how to get a socket to
	// the machine we're trying to reach.
	ip := dzr.GetLatest(fmt.Sprintf("/s/node/%s/ipaddress", host))
	if ip == "" {
		return nil
	}
	return socketForIp(ip)
}

func socketForIp(ip string) *zmq.Socket {
	sock, err := zmq_ctx.NewSocket(zmq.DEALER)
	if err != nil {
		log.Error("failed to create zmq socket: %s", err)
		os.Exit(1)
	}

	// BUG(mark): ask zmq where a broker is and talk to them.
	err = sock.Connect(fmt.Sprintf("tcp://%s:7330", ip))
	if err != nil {
		log.Error("failed to connect to agent: %s", err)
		os.Exit(1)
	}
	return &sock
}

func convertRoleToHosts(role string) []string {
	return dzr.GetdirLatest(fmt.Sprintf("/s/cfg/role/%s", role))
}
