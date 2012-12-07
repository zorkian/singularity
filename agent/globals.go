/* singularity - global.go

   This file contains global functions. These are jobs that get run somewhere
   in the cluster.

*/

package main

import (
	"fmt"
	"math/rand"
	"time"
)

type GlobalFunc func(chan bool)

// manageGlobalFunc takes a lock and function. If we are able to obtain that
// lock on a global level, then we will execute the given function.
//
// The function we are supposed to manage takes as an argument a channel. If
// the function writes to this channel, we will update the lock. If the
// function doesn't write to the channel for 60 seconds, and also doesn't
// exit, then we will consider it to be misbehaved and panic the whole
// application.
//
// Sadly, there doesn't seem to be a better ay to ensure we stop executing
// the global function...
//
// It is imperative that a global function only be running once globally. We
// depend on the semantics doozer gives us to guarantee this.
// 
func manageGlobalFunc(count int, lock string, job GlobalFunc) {
	if count < 1 {
		log.Fatal("manageGlobalFunc must spawn at least 1 job")
	}
	baseLock := lock

	for {
		// Every time we get in here, we update counter and try to catch the
		// next available lock.
		lock = fmt.Sprintf("%s.%d", baseLock, rand.Intn(count))

		// To prevent thundering herd (i.e., if the cluster is all rebooted
		// at the same time after a power outage), we delay initial global
		// function application by up to a minute.
		time.Sleep(time.Duration(rand.Float32()*60) * time.Second)
		if !tryGlobalLock(lock) {
			log.Debug("manageGlobal: lock %s held elsewhere", lock)
			time.Sleep(60 * time.Second)
			continue
		}

		// Looks like the lock is now ours. Set up our channel and spawn the
		// goroutine with our job.
		log.Info("manageGlobal: lock %s obtained, we're lively", lock)
		heartbeat := make(chan bool, 1)
		exited := make(chan bool, 1)
		lastHeartbeat := time.Now().Unix()

		// Now we run the function in a goroutine so we can capture when it
		// has exited.
		go func() {
			job(heartbeat)
			exited <- true
		}()

		// Now we start watching for heartbeats. We demand there to be one
		// every 60 seconds at least. If not, then we will panic and take
		// the entire agent offline to make sure no harm is done by having
		// an unresponsive global function.
	HEARTBEAT:
		for {
			select {
			case <-heartbeat:
				log.Debug("manageGlobal: heartbeat for %s received", lock)
				lastHeartbeat = time.Now().Unix()
			case <-exited:
				log.Warn("manageGlobal: function %s has exited", lock)
				break HEARTBEAT
			default:
				time.Sleep(1 * time.Second)
			}

			// The worst case.
			if time.Now().Unix() > lastHeartbeat+60 {
				log.Fatal("manageGlobal: heartbeat for %s went missing", lock)
			}
		}

		// If we fall through to here, it means that we lost our function and
		// should unlock so we try again.
		globalUnlock(lock)
	}
}

func gfExpireGlobalLocks(alive chan bool) {
	for {
		// Send our alive ping and sleep randomly. It's important we do that
		// because there will be several other nodes in the cluster who are
		// also doing this work. We don't want to get in sync.
		alive <- true
		time.Sleep(time.Duration(5+rand.Float32()*10) * time.Second)

		// Because people might sneak in after we start, we want to get the
		// current repository version. We ignore revisions after this.
		rev := dzr.Rev()

		// First, get the list of all valid gids as of now.
		gids := make(map[string]bool)
		for _, host := range dzr.GetdirLatest("/s/gid") {
			gids[dzr.GetLatest(fmt.Sprintf("/s/gid/%s", host))] = true
		}

		// Now iterate over all locks and look for gids that aren't currently
		// being tracked.
		for _, lock := range dzr.GetdirinfoAll("/s/glock") {
			lockfile := fmt.Sprintf("/s/glock/%s", lock.Name)
			_, ok := gids[dzr.GetLatest(lockfile)]
			if !ok {
				// This owner is dead now. The lock should be removed if it
				// hasn't been updated since. There is a small race here... if
				// we get the list of gid, then we look at locks... we might
				// mismatch. We are safe though because we give the revision
				// at the beginning to Del, and if the lock is updated, then
				// we just skip it.
				log.Info("global lock %s owned by inactive gid, removing",
					lock.Name)
				err := dzr.Del(lockfile, rev)
				if err != nil {
					log.Warn("failed to remove global lock %s: %s",
						lock.Name, err)
				}
			}
		}
	}
}

func gfExpireHosts(alive chan bool) {
	type revisionTracker struct {
		revision  int64
		timestamp int64
	}
	lastRevisions := make(map[string]*revisionTracker)

	for {
		// We sleep 5-15 seconds between iterations. We don't want to hit
		// doozer too often...
		alive <- true
		time.Sleep(time.Duration(5+rand.Float32()*10) * time.Second)
		now := time.Now().Unix()

		// Find all hosts, iterate over them, see if any are not reporting in
		// for more than some delta. Note that we need to ignore if their time
		// is accurate, we just want to know about the delta of revisions to
		// make sure it's being updated.
		for _, server := range dzr.GetdirinfoAll("/s/nlock") {
			old, ok := lastRevisions[server.Name]
			if !ok {
				// Never seen this. Insert a new record and then continue on.
				lastRevisions[server.Name] =
					&revisionTracker{revision: server.Rev, timestamp: now}
				continue
			}

			// If this revision has been updated, adjust and continue.
			if server.Rev > old.revision {
				old.revision = server.Rev
				old.timestamp = now
				continue
			}

			// If it hasn't been long enough, continue to wait.
			if now < old.timestamp+30 {
				// Alert if it's getting stale, though. This might help us see
				// problems before they become huge.
				if now > old.timestamp+3 {
					log.Warn("host %s didn't update its lock recently", server.Name)
				}
				continue
			}

			// Nope... if this was last updated more than 30 seconds ago then
			// we are going to consider this node to be dead. Try to send it
			// a command in order to be nice though...
			// BUG(mark): Send a 'die' command to the host in question. Also,
			// perhaps this is a good point for us to trigger a built in event.
			log.Warn("host %s seems to be dead, unlinking", server.Name)
			err := dzr.Del(fmt.Sprintf("/s/nlock/%s", server.Name), server.Rev)
			if err != nil {
				log.Error("failed to delete /s/nlock/%s: %s", server.Name, err)
				continue
			}

			// Finally delete the gid mapping, it's dead and gone. This will
			// cause the lock expirer to remove global locks held by this node.
			dzr.DelLatest(fmt.Sprintf("/s/gid/%s", server.Name))
		}
	}
}
