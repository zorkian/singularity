/* singularity - client - commands.go

   Contains the logic for actually running commands.

*/

package main

import (
	"../proto"
	"os"
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

	var ltimeout uint32 = uint32(timeout)
	err := singularity.WritePb(sock, nil,
		&singularity.Command{Command: []byte(command),
			Args: [][]byte{[]byte(arg)}, Timeout: &ltimeout})
	if err != nil {
		log.Error("[%s] failed to send: %s", host, err)
		return
	}

	var stdout, stderr []byte
	for {
		_, resp, err := singularity.ReadPb(sock)
		if err != nil {
			log.Error("[%s] failed to read: %s", host, err)
			return
		}

		switch resp.(type) {
		case *singularity.CommandOutput:
			co := resp.(*singularity.CommandOutput)
			appendAndWrite(os.Stdout, &stdout, co.Stdout, false)
			appendAndWrite(os.Stderr, &stderr, co.Stderr, false)
		case *singularity.CommandFinished:
			duration := time.Now().Sub(start)
			appendAndWrite(os.Stdout, &stdout, nil, true)
			appendAndWrite(os.Stderr, &stderr, nil, true)
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

func appendAndWrite(file *os.File, src *[]byte, apnd []byte, finish bool) {
	if apnd != nil && len(apnd) > 0 {
		*src = append(*src, apnd...)
	}
	if len(*src) <= 0 {
		return
	}
	for {
		n, err := file.Write(*src)
		if err != nil {
			log.Error("failed writing: %s", err)
			return
		}
		if n == len(*src) {
			*src = make([]byte, 0)
		} else {
			*src = (*src)[n:]
		}
		if len(*src) > 0 && finish {
			continue
		}
		break
	}
}
