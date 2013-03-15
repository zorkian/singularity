/* singularity - client - commands.go

   Contains the logic for actually running commands.

*/

package main

import (
	"../proto"
	"bytes"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

var writeMutex sync.Mutex

// isValidCommand looks at the command the user is wanting to run and checks
// for validity. The server will do this too, but we do it here to give the
// user more useful usage information.
func isValidCommand(cmd string, args []string) bool {
	switch cmd {
	case "hosts", "roles":
		if len(args) > 0 {
			if args[0] == "-v" || args[0] == "--verbose" {
				verbose = true
				args = args[1:]
			}
		}
		if len(args) > 0 {
			log.Error("command %s takes no arguments", cmd)
			log.Error("(except optionally one -v/--verbose flag)")
			return false
		}
		return true
	case "die":
		if len(args) > 0 {
			log.Error("command %s takes no arguments", cmd)
			return false
		}
		return true
	case "exec", "add_role":
		if len(args) != 1 {
			log.Error("command %s takes exactly one argument", cmd)
			return false
		}
		return true
	case "del_role":
		if len(args) < 1 || len(args) > 2 {
			log.Error("command %s takes exactly one or two arguments", cmd)
			return false
		}
		return true
	case "alias":
		if len(args) > 2 {
			log.Error("command %s takes exactly 0, 1, or 2 arguments", cmd)
			return false
		}
		return true
	default:
		log.Error("command %s unknown", cmd)
		return false
	}
	return false
}

// expandAlias takes an argument string as input and if it fits what we know
// of an alias, expands it.
func expandAlias(arg []string) []string {
	if len(arg) != 1 {
		return arg
	}
	alias := dzr.GetLatest("/s/cfg/alias/" + arg[0])
	if alias != "" {
		return []string{"exec", alias}
	}
	return arg
}

func runJob(job *Job) {
	doSimpleCommand(job.host, job.job[0], job.job[1:])
}

// doSimpleCommand executes a command against a backend.
// FIXME: this can timeout in certain cases. We should make it so that the
// client can abort itself if a remote is timing out.
func doSimpleCommand(host, command string, args []string) {
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

	var bargs [][]byte
	for _, arg := range args {
		bargs = append(bargs, []byte(arg))
	}

	var ltimeout uint32 = uint32(timeout)
	err := singularity.WritePb(sock, nil,
		&singularity.Command{Command: []byte(command),
			Args: bargs, Timeout: &ltimeout})
	if err != nil {
		log.Error("[%s] failed to send: %s", host, err)
		return
	}

	var stdout, stderr []byte
	for {
		remote, resp, err := singularity.ReadPb(sock, 20)
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
				if nowarn {
					log.Info("[%s] unexpected return value: %d", host, *retval)
				} else {
					log.Error("[%s] unexpected return value: %d", host, *retval)
				}
			}
			log.Debug("[%s] finished in %s", host, duration)
			return
		case *singularity.StillAlive:
			log.Debug("[%s] ping? pong!", host)
			err := singularity.WritePb(sock, remote, &singularity.StillAlive{})
			if err != nil {
				log.Error("[%s] failed pong: %s", host, err)
				return
			}
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

	// We never want to get interrupted in writing our output, so take
	// the lock and hold it until we exit.
	writeMutex.Lock()
	defer writeMutex.Unlock()

	for {
		if len(*src) <= 0 {
			return
		}

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
			*src = (*src)[n:]
		}
		// Always continue. The top part of this will break us out of the for
		// loop if we need to.
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

// cmdRoles gets a list of roles and prints them out, then exits.
func cmdRoles() {
	roles := dzr.GetdirLatestSafe("/s/cfg/role")
	sort.Strings(roles)
	for _, role := range roles {
		if verbose {
			hosts := dzr.GetdirLatestSafe("/s/cfg/role/" + role)
			sort.Strings(hosts)
			fmt.Printf("%s: %s\n", role, strings.Join(hosts, ", "))
		} else {
			fmt.Println(role)
		}
	}
	os.Exit(0)
}

// cmdAlias allows the user to define an alias. This is stored in doozer so it
// is available to all clients.
// TODO(mark): Consider storing the current settings with aliases so that when
// you execute an alias you can set defaults.
func cmdAlias(args []string) {
	if len(args) == 0 {
		// alias
		// to print out all aliases
		aliaslist := make([]string, 0)
		aliases := make(map[string]string)
		for _, alias := range dzr.GetdirLatestSafe("/s/cfg/alias") {
			aliases[alias] = dzr.GetLatest("/s/cfg/alias/" + alias)
			aliaslist = append(aliaslist, alias)
		}
		sort.Strings(aliaslist)
		for _, alias := range aliaslist {
			fmt.Printf("%s: %s\n", alias, aliases[alias])
		}
	} else if len(args) == 1 {
		// alias <alias>
		// to print out its contents
		cmd := dzr.GetLatest("/s/cfg/alias/" + args[0])
		if cmd != "" {
			fmt.Printf("%s\n", cmd)
		} else {
			fmt.Printf("%s not an alias\n", args[0])
		}
	} else {
		// alias <alias> <cmd>
		// cmd can be "" to erase an alias
		alias := strings.TrimSpace(args[0])
		if alias == "" || strings.Contains(alias, " ") {
			log.Error("invalid alias")
			return
		}
		if args[1] == "" {
			dzr.DelLatest("/s/cfg/alias/" + alias)
		} else {
			dzr.SetLatest("/s/cfg/alias/"+alias, args[1])
		}
	}
	os.Exit(0)
}

// cmdHosts prints out hosts. If verbose, we also print out the roles for each
// of the hosts.
func cmdHosts() {
	// We'll need this later if we're in verbose mode.
	hosts := make(map[string]map[string]bool)
	if verbose {
		for _, role := range dzr.GetdirLatestSafe("/s/cfg/role") {
			for _, node := range dzr.GetdirLatestSafe("/s/cfg/role/" + role) {
				if _, ok := hosts[node]; !ok {
					hosts[node] = make(map[string]bool)
				}
				hosts[node][role] = true
			}
		}
	}

	// Tradeoff here: /s/node gets us everybody that has ever configured, but
	// /s/nlock gets us people who are expected to be alive. I'm not sure what
	// is better. For now, we use liveliness.
	nodes := dzr.GetdirLatestSafe("/s/nlock")
	sort.Strings(nodes)
	for _, node := range nodes {
		if verbose {
			roles := make([]string, 0)
			if _, ok := hosts[node]; ok {
				for role, _ := range hosts[node] {
					roles = append(roles, role)
				}
				sort.Strings(roles)
			}
			fmt.Printf("%s: %s\n", node, strings.Join(roles, ", "))
		} else {
			fmt.Println(node)
		}
	}
	os.Exit(0)
}
