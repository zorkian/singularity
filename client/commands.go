/* singularity - client - commands.go

   Contains the logic for actually running commands.

*/

package main

import (
	"../proto"
	"code.google.com/p/goprotobuf/proto"
	"fmt"
	zmq "github.com/alecthomas/gozmq"
	"strings"
	"time"
)

func isValidCommand(cmd string) bool {
	if cmd == "exec" || cmd == "add_role" {
	} else {
		return false
	}
	return true
}

func runJob(job *Job) {
	doSimpleCommand(job.host, job.job[0], job.job[1])
}

func doSimpleCommand(host, command, arg string) {
	log.Debug("[%s] command: %s", host, command)

	sock := socketForHost(host)
	if sock == nil {
		log.Warn("[%s] no socket available, skipping", host)
		return
	}
	defer (*sock).Close()

	// Send our output. Interestingly, it seems that this never fails, even
	// if the node is down. ZMQ always accepts the connect/write and just
	// buffers it internally? Even though we're supposedly blocking...
	start := time.Now()
	if !waitForSend(sock, 1) {
		log.Error("host %s: socket never became writeable", host)
		return
	}

	cmdpb := &singularity.Command{
		Command: []byte(command),
		Args:    [][]byte{[]byte(arg)},
	}
	buf, err := proto.Marshal(cmdpb)
	if err != nil {
		log.Error("host %s: failed to marshal protobuf: %s", host, err)
		return
	}
	(*sock).Send(buf, 0)

	// Wait for this socket to have data, for up to a certain timeout.
	if !waitForRecv(sock, timeout) {
		log.Error("host %s: timeout receiving response", host)
		return
	}
	resp, err := (*sock).Recv(0)
	if err != nil {
		log.Warn("[%s] failed: %s", host, err)
		return
	}
	duration := time.Now().Sub(start)

	resppb := &singularity.Response{}
	err = proto.Unmarshal(resp, resppb)
	if err != nil {
		log.Error("host %s: failed to unmarshal protobuf: %s", host, err)
		return
	}

	// Annoying way to remove a trailing empty line? Maybe there is a better
	// way of doing this.
	// TODO(mark): Use Stderr and ExitCode here.
	split := strings.Split(string(resppb.Stdout), "\n")
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
		fmt.Printf("[%s] %s\n", host, line)
	}
	log.Debug("[%s] finished in %s", host, duration)
}

// BUG(mark): Move this to a general framework so we can use it in the agent.
// Or just for the gozmq package and implement it there?
func waitForRecv(sock *zmq.Socket, timeout int64) bool {
	pi := make([]zmq.PollItem, 1)
	pi[0] = zmq.PollItem{Socket: *sock, Events: zmq.POLLIN}
	zmq.Poll(pi, timeout*1000000)
	if pi[0].REvents == zmq.POLLIN {
		return true
	}
	return false
}

func waitForSend(sock *zmq.Socket, timeout int64) bool {
	pi := make([]zmq.PollItem, 1)
	pi[0] = zmq.PollItem{Socket: *sock, Events: zmq.POLLOUT}
	zmq.Poll(pi, timeout*1000000)
	if pi[0].REvents == zmq.POLLOUT {
		return true
	}
	return false
}
