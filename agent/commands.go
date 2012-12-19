/* singularity - agent - commands.go

   These are commands we bundle internally for clients to call.

*/

package main

import (
	"../proto"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// handleCommand takes an input command and executes it.
func handleCommand(lworker *worker, cmd *singularity.Command) {
	sendstr := func(output string) {
		output = output + "\n"
		err := sendMessage(lworker.addr,
			&singularity.CommandOutput{Stdout: []byte(output)})
		if err != nil {
			log.Error("worker failed to respond: %s", err)
			return
		}

		var retval int32 = 0
		err = sendMessage(lworker.addr,
			&singularity.CommandFinished{ExitCode: &retval})
		if err != nil {
			log.Error("worker failed to respond: %s", err)
		}
	}

	log.Debug("worker received: %s", cmd.Command)
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
			err := handleClientExec(lworker, args[0], cmd.GetTimeout())
			if err != nil {
				log.Error("worker failed exec: %s", err)
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
	case "del_role":
		if len(args) != 1 {
			sendstr("del_role requires exactly one argument")
		} else {
			role := strings.TrimSpace(args[0])
			dzr.DelLatest(fmt.Sprintf("/s/cfg/role/%s/%s", role, hostname))
			sendstr(fmt.Sprintf("deleted role %s", role))
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
				close(ch)
				return
			}
			buf := make([]byte, 65536)
			ct, err := inp.Read(buf)
			if ct > 0 {
				ch <- buf[0:ct]
			}
			if err != nil {
				close(ch)
				return
			}
		}
	}(rdr)
	return ch
}

// handleClientExec takes a command given to us from a client and executes it,
// passing the output back to the user as we get it.
func handleClientExec(lworker *worker, command string, ttl uint32) error {
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
		// to the user. We die when both channels close, or someone triggers
		// the exit channel. (Timeout or client died, usually.)
		for {
			select {
			case <-exit:
				// This only happens in a timeout condition. In which case,
				// since we've now drained exit, another value gets put on
				// when we exit.
				return
			case buf, ok := <-ch_stdout:
				if !ok {
					ch_stdout = nil
					if ch_stderr == nil {
						return
					}
					continue
				}
				err := sendMessage(lworker.addr,
					&singularity.CommandOutput{Stdout: buf})
				if err != nil {
					log.Error("worker error writing: %s", err)
					return
				}
			case buf, ok := <-ch_stderr:
				if !ok {
					ch_stderr = nil
					if ch_stdout == nil {
						return
					}
					continue
				}
				err := sendMessage(lworker.addr,
					&singularity.CommandOutput{Stderr: buf})
				if err != nil {
					log.Error("worker error writing: %s", err)
					return
				}
			}
		}
	}()

	// Now we want to start the command, which spins it off in another
	// process...
	if err := cmd.Start(); err != nil {
		return err
	}

	// In several codepaths we have to kill the job, do some logging, and also
	// kill off our goroutines.
	killswitch := func(msg string) {
		log.Debug("killswitch: %s", msg)
		err := cmd.Process.Kill()
		if err != nil {
			log.Error("worker failed to kill: %s", err)
		}
		exit <- true
	}

	// Now we run the timeout loop. This may or may not actually time out, but
	// this is the main loop we sit in while we process things.
	timeout := time.Now().Add(time.Duration(ttl) * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for now := range ticker.C {
		if len(exit) > 0 {
			// We're exiting, don't need to kill.
		} else if !lworker.isAlive() {
			killswitch("worker has gone away")
		} else if ttl > 0 && now.After(timeout) {
			killswitch("command timed out")
		} else {
			// This is the only path by which we continue through the for
			// loop. All other paths end in death.
			continue
		}
		break
	}

	// Should be dead. Reap the process so we can get the return value.
	var retval int32 = 0
	if err := cmd.Wait(); err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			// The program has exited with an exit code != 0.
			// There is no plattform independent way to retrieve
			// the exit code, but the following will work on Unix.
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
				retval = int32(status.ExitStatus())
				log.Error("worker exit code: %d", retval)
			}
		} else {
			log.Error("worker wait returned: %s", err)
		}
	}

	// By now, it should have run and be done.
	err = sendMessage(lworker.addr,
		&singularity.CommandFinished{ExitCode: &retval})
	if err != nil {
		return err
	}
	return nil
}
