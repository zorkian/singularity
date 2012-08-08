/* singularity - client - queue.go

   The client can queue up a number of jobs. This code manages the queueing
   and completion of jobs.

*/

package main

import (
	"strings"
	"sync"
	"time"
)

type Job struct {
	host string
	job  []string
}

func (self *Job) Status() string {
	return self.host
}

var jqueue []*Job = make([]*Job, 0)
var jqlock sync.Mutex
var jwaiter sync.WaitGroup = sync.WaitGroup{}

func queueJob(host string, job []string) {
	jqlock.Lock()
	defer jqlock.Unlock()
	jqueue = append(jqueue, &Job{host: host, job: job})
}

func popJob() *Job {
	jqlock.Lock()
	defer jqlock.Unlock()
	if len(jqueue) == 0 {
		return nil
	}
	tmp := jqueue[0]
	jqueue = jqueue[1:]
	return tmp
}

func dumpStatus() {
	jqlock.Lock()
	defer jqlock.Unlock()

	strs := make([]string, len(jqueue))
	for idx, job := range jqueue {
		strs[idx] = job.Status()
	}
	log.Error("jobs queued: %s", strings.Join(strs, " "))
}

func runJobs(jobs int) {
	// Spin up enough workers, or as many as there are jobs.
	jqlock.Lock()
	if jobs == 0 {
		jobs = len(jqueue)
	}
	log.Debug("runner wants %d jobs", jobs)
	for i := 0; i < jobs; i++ {
		log.Debug("runner spawning job")
		go jobWorker()
	}
	log.Debug("runner finished, unlocking")
	jqlock.Unlock() // Release the hounds.
	waitJobs()
}

func jobWorker() {
	jwaiter.Add(1)
	defer jwaiter.Done()

	log.Debug("worker spawned")
	for {
		job := popJob()
		if job == nil {
			log.Debug("worker finished")
			return
		}

		log.Debug("worker got job for host=%s", job.host)
		runJob(job)
	}
}

func waitJobs() {
	done := make(chan bool)

	go func() {
		jwaiter.Wait()
		done <- true
	}()

	nextStatus := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-done:
			return
		default:
			// Do nothing.
		}

		time.Sleep(1 * time.Second)
		if time.Now().After(nextStatus) {
			dumpStatus()
			nextStatus = time.Now().Add(10 * time.Second)
		}
	}
}
