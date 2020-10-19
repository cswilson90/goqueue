package queue

import (
	"log"
	"sync"
)

// priorityJobQueue is a priority queue of jobs
type priorityJobQueue struct {
	// queueMutex protects the statusQueues map
	queueMutex   sync.Mutex
	statusQueues map[string]*jobQueue
}

func newPriorityJobQueue() *priorityJobQueue {
	return &priorityJobQueue{
		statusQueues: map[string]*jobQueue{
			"reserved": nil,
			"ready":    nil,
			"delayed":  nil,
			"buried":   nil,
		},
	}
}

// getStatusQueue gets the correct job queue for the current status of the job.
func (p *priorityJobQueue) getStatusQueue(job *job) *jobQueue {
	p.queueMutex.Lock()
	queue, ok := p.statusQueues[job.status]
	if !ok {
		log.Fatalf("Job %v has unknown status: %v\n", job.id, job.status)
	}

	if queue == nil {
		// No queue yet made for the status so initialise one
		p.statusQueues[job.status] = newJobQueue(job.priority)
		queue = p.statusQueues[job.status]
	}

	p.queueMutex.Unlock()
	return queue
}

// addJob adds the given job to the queue.
func (p *priorityJobQueue) addJob(job *job) {
	statusQueue := p.getStatusQueue(job)
	statusQueue.addJob(job)
}

// reserveJob gets the next ready job in the queue and reserves it.
// Second returned value is false if there is no job that can be reserved.
func (p *priorityJobQueue) reserveJob() (*job, bool) {
	statusQueue := p.statusQueues["ready"]

	if statusQueue == nil {
		return nil, false
	}

	for reservedJob, ok := statusQueue.getNextJob(); ok; {
		err := reservedJob.reserve()
		if err != nil {
			if _, ok := err.(deletedJobError); ok {
				// If job has been deleted try and reserve another
				reservedJob, ok = statusQueue.getNextJob()
				continue
			} else {
				log.Fatalf("Failed to reserve job %v from ready queue: %v\n", reservedJob.id, err.Error())
			}
		}
		newQueue := p.getStatusQueue(reservedJob)
		newQueue.addJob(reservedJob)
		return reservedJob, true
	}

	return nil, false
}
