/* singularity - agent - locker.go

   This module contains logic for handling locks. Local and global locks
   are all maintained by this module. NOTE: We are not responsible for
   the ultimate node lock, which is managed by the agent itself.

*/

package main

import "time"

type LockCommand int8

const (
	CMD_LOCK LockCommand = iota
	CMD_UNLOCK
)

type LockMap map[string]int64
type LockRequest struct {
	name     string
	command  LockCommand
	response chan (bool)
}
type LockRequestChannel chan (*LockRequest)

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
				if ok && locked > 0 {
					req.response <- false
				} else {
					localLocks[req.name] = time.Now().UnixNano()
					req.response <- true
				}
			case CMD_UNLOCK:
				if ok && locked > 0 {
					req.response <- true
					localLocks[req.name] = 0
				} else {
					req.response <- false
				}
			default:
				req.response <- false
			}
		}
	}()
}

// tryLocalLock attempts to get a local lock. If the lock is already held by
// somebody else, we say so. Returns true or false depending on if the lock
// was achieved.
func tryLocalLock(name string) bool {
	resp := make(chan (bool), 1)
	localLockChannel <- &LockRequest{name: name, command: CMD_LOCK, response: resp}
	return <-resp
}

// localUnlock cancels a held lock. Note that anybody can be calling this, we
// do not enforce that the person unlocking is the person who locked.
func localUnlock(name string) bool {
	resp := make(chan (bool), 1)
	localLockChannel <- &LockRequest{name: name, command: CMD_UNLOCK, response: resp}
	return <-resp
}

// tryGlobalLock attempts to get a global lock. We don't have to do any of the
// fancy synchronization here since Doozer handles that.
func tryGlobalLock(name string) bool {
	lock := "/s/glock/" + name
	rev := dzr.Stat(lock, nil)
	if rev > 0 {
		// We do not handle lock-busting and removing stale locks. That's up
		// to the broker/manager/somebody else. Just fail.
		log.Debug("global lock %s already held", name)
		return false
	}

	nrev := dzr.Set(lock, rev, gid)
	if nrev <= rev {
		log.Debug("global lock %s claim-race failed", name)
		return false
	}

	log.Debug("global lock %s claimed at rev %d", name, nrev)
	globalLocks[name] = nrev
	return true
}

// globalUnlock attempts to unlock a lock that we have
func globalUnlock(name string) bool {
	rev, held := globalLocks[name]
	if !held || rev == 0 {
		return false
	}

	log.Debug("unlocking global %s (rev %d)", name, rev)
	err := dzr.Del("/s/glock/"+name, rev)
	if err != nil {
		log.Warn("failed to unlock global %s (rev=%d): %s", name, rev, err)
		return false
	}

	globalLocks[name] = 0
	return true
}

// removeGlobalLocks should only be called internally when the agent is
// being destroyed. This removes any global locks that we held. We try
// to be nice...
func removeGlobalLocks() {
	for lock, rev := range globalLocks {
		if rev == 0 {
			continue
		}
		globalUnlock(lock)
	}
}
