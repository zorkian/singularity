/* singularity - agent - locker.go

   This module contains logic for handling locks. Local and global locks
   are all maintained by this module. NOTE: We are not responsible for
   the ultimate node lock, which is managed by the agent itself.

*/

package main

type LockCommand int8
const (
	CMD_LOCK LockCommand = iota
	CMD_UNLOCK
)

type LockMap map[string]bool
type LockRequest struct {
	name string
	command LockCommand
	response chan(bool)
}
type LockRequestChannel chan(*LockRequest)

// Global data structures we use.
var globalLocks LockMap = make(LockMap)
var localLocks LockMap = make(LockMap)
var localLockChannel LockRequestChannel = make(LockRequestChannel)

// Sets up various goroutines that we need to manage the locking subsystem.
func init() {
	// The main worker. This reads requests to manage our lock structure. Note
	// that this only runs in ONE PLACE. It's a single anonymous goroutine.
	// This one is for LOCAL LOCKS.
	go func() {
		for {
			req := <-localLockChannel
			locked, ok := localLocks[req.name]

			switch req.command {
			case CMD_LOCK:
				if ok && locked {
					req.response <- false
				} else {
					localLocks[req.name] = true
					req.response <- true
				}
			case CMD_UNLOCK:
				if ok && locked {
					req.response <- true
					localLocks[req.name] = false
				} else {
					req.response <- false
				}
			default:
				req.response <- false
			}
		}
	}()

	// Same as above, but this one is for GLOBAL locks. We have to coordinate
	// with the doozer system to handle this.
}

// tryLocalLock attempts to get a local lock. If the lock is already held by
// somebody else, we say so. Returns true or false depending on if the lock
// was achieved.
func tryLocalLock(name string) bool {
	resp := make(chan(bool), 1)
	localLockChannel <- &LockRequest{name: name, command: CMD_LOCK, response: resp}
	return <-resp
}

// localUnlock cancels a held lock. Note that anybody can be calling this, we
// do not enforce that the person unlocking is the person who locked.
func localUnlock(name string) bool {
	resp := make(chan(bool), 1)
	localLockChannel <- &LockRequest{name: name, command: CMD_UNLOCK, response: resp}
	return <-resp	
}