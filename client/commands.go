/* singularity - client - commands.go

   Contains the logic for actually running commands.

*/

package main

import (
	"../proto"
	"bytes"
	"fmt"
	"os"
	"time"
)

func isValidCommand(cmd string) bool {
	if cmd == "exec" || cmd == "add_role" || cmd == "del_role" {
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
			if co.Stdout != nil && len(co.Stdout) > 0 {
				stdout = append(stdout, co.Stdout...)
			}
			if co.Stderr != nil && len(co.Stderr) > 0 {
				stderr = append(stderr, co.Stderr...)
			}
			if !serial {
				writeOutput(os.Stdout, &stdout, host, false)
				writeOutput(os.Stderr, &stderr, host, false)
			}
		case *singularity.CommandFinished:
			duration := time.Now().Sub(start)
			writeOutput(os.Stdout, &stdout, host, true)
			writeOutput(os.Stderr, &stderr, host, true)
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

func writeTextOutput(file *os.File, src *[]byte, host string, finish bool) {
	// If this is not a binary write, we only want to write out when
	// we have a full line; up to a \n. TODO: do we want to handle the
	// other line ending types? We only claim to support Linux and they
	// mostly use bare \n.
	if len(*src) <= 0 {
		return
	}
	for {
		idx := bytes.IndexByte(*src, '\n')
		if idx == -1 {
			if finish {
				// Stick a newline on it so that our flow works.
				*src = append(*src, '\n')
				idx = len(*src) - 1
			} else {
				break
			}
		}

		_, err := fmt.Fprintf(file, "[%s] ", host)
		if err != nil {
			log.Error("failed writing: %s", err)
			return
		}

		// Print out from start to newline, include it
		n, err := file.Write((*src)[0 : idx+1])
		if err != nil {
			log.Error("failed writing: %s", err)
			return
		}
		if n == len(*src) {
			*src = make([]byte, 0)
		} else {
			*src = (*src)[n+1:]
		}
		if len(*src) > 0 && finish {
			continue
		}
		break
	}
}

func writeOutput(file *os.File, src *[]byte, host string, finish bool) {
	if !binary {
		// If we're in text mode, bail out to the text parser. This simplifies
		// the overall function logic.
		writeTextOutput(file, src, host, finish)
		return
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
