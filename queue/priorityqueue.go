package queue

import "log"

// priorityJobQueue is a priority queue of jobs
type priorityJobQueue struct {
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
	queue, ok := p.statusQueues[job.status]
	if !ok {
		log.Fatalf("Job %v has unknown status: %v\n", job.id, job.status)
	}

	return queue
}

// addJob adds the given job to the queue.
func (p *priorityJobQueue) addJob(job *job) {
	statusQueue := p.getStatusQueue(job)

	if statusQueue == nil {
		// No queue yet made for the status so initialise one
		p.statusQueues[job.status] = newJobQueue(job.priority)
		statusQueue = p.statusQueues[job.status]
	}

	statusQueue.addJob(job)
}

// nextReadyJob gets the next ready job in the queue.
// Returns nil if there is no job that can be reserved.
func (p *priorityJobQueue) reserveJob() *job {
	statusQueue := p.statusQueues["ready"]

	if statusQueue == nil {
		return nil
	}

	reservedJob := statusQueue.getNextJob()

	if reservedJob != nil {
		err := reservedJob.reserve()
		if err != nil {
			log.Fatalf("Failed to reserve job %v from ready queue: %v\n", reservedJob.id, err.Error())
		}
	}

	return reservedJob
}
