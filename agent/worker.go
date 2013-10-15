/* singularity - agent - worker.go

   A worker is a set of channels and goroutines that actually accomplish things
   that clients want us to take care of.

*/

package main

import (
	"github.com/xb95/singularity/proto"
	"sync"
	"time"
)

type workerState int

const (
	ALIVE workerState = iota
	DEAD  workerState = iota
)

type worker struct {
	lock  sync.Mutex
	addr  []byte
	input chan interface{}
	state workerState
	alive time.Time // Time we are alive until.
}

// TODO(mark): Clean up stale workers that we don't need anymore. Close the input
// channel, that signals the worker pump to die.
var workerMap map[string]*worker = make(map[string]*worker)
var workerLock sync.Mutex

// init here sets up the worker ping/pong helper and the code that expires out
// the stale workers.
func init() {
	go maintainWorkers()
}

func (self *worker) stillAlive() {
	self.lock.Lock()
	defer self.lock.Unlock()

	if self.state != ALIVE {
		// This is a very exceptional state, I hope a funny message will cause
		// someone to investigate if they get it. :-)
		log.Debug("dead workers tell no tales! please investigate!")
		return
	}

	log.Debug("worker received still alive from client")
	self.alive = time.Now().Add(60 * time.Second)
}

// isAlive returns true/false if we believe the client is still around.
func (self *worker) isAlive() bool {
	self.lock.Lock()
	defer self.lock.Unlock()

	if self.state != ALIVE {
		return false
	}
	return true
}

// terminate closes channels and does any cleanup we need to do to release the
// resources we've acquired.
func (self *worker) terminate() {
	self.lock.Lock()
	defer self.lock.Unlock()

	if self.state != ALIVE {
		return
	}

	workerLock.Lock()
	defer workerLock.Unlock()

	self.state = DEAD
	close(self.input)
	delete(workerMap, string(self.addr))
}

// ping initiates the sending of a ping packet, which instructs the client to
// send back a StillAlive message which triggers the liveliness logic.
func (self *worker) ping() {
	err := sendMessage(self.addr, &singularity.StillAlive{})
	if err != nil {
		log.Debug("worker send failed: %s", err)
		self.terminate()
	}
}

// getWorker takes in a remote address (slice of bytes) and returns either a
// worker for that connection or makes a new one.
func getWorker(remote []byte) *worker {
	workerLock.Lock()
	defer workerLock.Unlock()

	key := string(remote)
	if _, ok := workerMap[key]; !ok {
		log.Debug("making new worker")
		workerMap[key] = newWorker(remote)
	}
	return workerMap[key]
}

// newWorker does the work of building a new worker for a given remote address.
// This method does not take the lock, as it's assumed the lock is held by the
// person who is calling us.
func newWorker(remote []byte) *worker {
	lworker := &worker{
		addr:  remote,
		input: make(chan interface{}, 10), // Message queue depth.
		alive: time.Now().Add(60 * time.Second),
		state: ALIVE,
	}

	go handleWorkerPump(lworker)
	return lworker
}

// handleWorkerPump is meant to be launched in a goroutine. It handles reading
// in messages from the client and acting on them.
func handleWorkerPump(lworker *worker) {
	for pb := range lworker.input {
		lworker.stillAlive()

		switch pb.(type) {
		case *singularity.Command:
			go handleCommand(lworker, pb.(*singularity.Command))
		case *singularity.StillAlive:
			// Do nothing. All packets mark the client as being alive.
		default:
			log.Error("worker received unexpected protobuf: %T", pb)
		}
	}
	log.Debug("worker pump exiting")
}

// maintainWorkers does what you expect. We keep track of workers and the last
// time we heard from the client attached. If it gets too old, we kill them.
func maintainWorkers() {
	// If we ever exit, it should be fatal to the agent. The worker maintenance
	// function is vital.
	defer func() { log.Fatal("maintainWorkers exited") }()

	for now := range time.Tick(10 * time.Second) {
		log.Debug("maintainWorkers: %d worker(s) in map", len(workerMap))
		toTerminate := make([]*worker, 0)

		workerLock.Lock()
		for _, lworker := range workerMap {
			if lworker.state != ALIVE {
				continue
			}

			if now.After(lworker.alive) {
				toTerminate = append(toTerminate, lworker)
			} else {
				lworker.ping()
			}
		}
		workerLock.Unlock()

		for _, lworker := range toTerminate {
			lworker.terminate()
		}
	}
}
