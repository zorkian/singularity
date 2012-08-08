/* singularity - client - commands.go

   Contains the logic for actually running commands.

*/

package main

import (
	"fmt"
	"strings"
	"time"
)

func isValidCommand(cmd string) bool {
	if cmd == "exec" {
	} else {
		return false
	}
	return true
}

func runJob(job *Job) {
	cmd := job.job[0]
	if cmd == "exec" {
		doCommand(job.host, job.job[1])
	}
}

func doCommand(host, command string) {
	log.Info("[%s] executing: %s", host, command)

	sock := socketForHost(host)
	if sock == nil {
		log.Warn("[%s] no socket available, skipping", host)
		return
	}
	defer sock.Close()

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
		fmt.Printf("[%s] %s\n", host, line)
	}
	log.Info("[%s] finished in %s", host, duration)
}
