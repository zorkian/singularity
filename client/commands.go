/* singularity - client - commands.go

   Contains the logic for actually running commands.

*/

package main

import (
	"../proto"
	"fmt"
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
	if !singularity.WaitForSend(sock, 1) {
		log.Error("[%s] socket never became writeable", host)
		return
	}

	err := singularity.WritePb(sock, nil,
		&singularity.Command{Command: []byte(command), Args: [][]byte{[]byte(arg)}})
	if err != nil {
		log.Error("[%s] failed to send: %s", host, err)
		return
	}

	var stdout, stderr string
	for {
		// Wait for this socket to have data, for up to a certain timeout.
		//		if !singularity.WaitForRecv(sock, timeout) {
		//log.Error("[%s] timeout receiving response", host)
		//return
		//}

		_, resp, err := singularity.ReadPb(sock)
		if err != nil {
			log.Error("[%s] failed to read: %s", host, err)
			return
		}

		switch resp.(type) {
		case *singularity.CommandOutput:
			stderr = stderr + string(resp.(*singularity.CommandOutput).Stderr)
			printOutput(&stderr, false)
			stdout = stdout + string(resp.(*singularity.CommandOutput).Stdout)
			printOutput(&stdout, false)
		case *singularity.CommandFinished:
			duration := time.Now().Sub(start)
			printOutput(&stderr, true)
			printOutput(&stdout, true)
			if retval := resp.(*singularity.CommandFinished).ExitCode; *retval != 0 {
				log.Error("[%s] unexpected return value: %d", host, *retval)
			}
			log.Debug("[%s] finished in %s", host, duration)
			return
		default:
			log.Error("[%s] unexpected protobuf: %v", resp)
			return
		}
	}
}

func printOutput(str *string, everything bool) {
	if everything {
		// Just dump whatever we have left.
		fmt.Printf(*str)
		*str = ""
		return
	}

	// We're going to remove lines and print them one by one.
	for {
		i := strings.Index(*str, "\n")
		if i == -1 {
			break
		}
		fmt.Printf((*str)[0 : i+1])
		*str = (*str)[i+1:]
	}
}
