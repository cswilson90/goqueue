package queue

import "log"

// A jobQueue is a single non-priorty queue of Jobs.
type jobQueue struct {
	firstJob *job
	lastJob  *job

	// Queues are organised in a binary tree by priority
	priority uint32

	leftQueue  *jobQueue
	rightQueue *jobQueue
}

// newJobQueue creates and returns a new jobQueue with the given name.
// A jobQueue's name cannot be changed after it's created.
func newJobQueue(priority uint32) *jobQueue {
	return &jobQueue{
		priority: priority,
	}
}

// addJob adds the given job to the end of the queue
func (q *jobQueue) addJob(job *job) {
	// If the job priority does not match this queue pass it on to the next
	// queue which is created if necessary
	if job.priority < q.priority {
		if q.leftQueue == nil {
			q.leftQueue = newJobQueue(job.priority)
		}
		q.leftQueue.addJob(job)
		return
	} else if job.priority > q.priority {
		if q.rightQueue == nil {
			q.rightQueue = newJobQueue(job.priority)
		}
		q.rightQueue.addJob(job)
		return
	}

	// Job priority matches this queue so add it here
	if q.firstJob == nil {
		q.firstJob = job
	} else {
		q.lastJob.nextJob = job
		job.previousJob = q.lastJob
	}

	q.lastJob = job
}

// getNextJob gets the next job in the queue, removes it from the queue and returns it.
// Te second return value is false if there are no jobs in the queue.
func (q *jobQueue) getNextJob() (*job, bool) {
	// Check for a job in a higher priority queue
	if q.leftQueue != nil {
		higherPriJob, ok := q.leftQueue.getNextJob()
		if ok {
			return higherPriJob, true
		}
	}

	nextJob := q.firstJob

	// If no jobs in this queue check lower priority queues
	if nextJob == nil {
		if q.rightQueue == nil {
			return nil, false
		}
		return q.rightQueue.getNextJob()
	}

	// Return job from this queue if there is one and no higher priority queue has one
	q.firstJob = nextJob.nextJob

	if q.firstJob == nil {
		q.lastJob = nil
	} else {
		q.firstJob.previousJob = nil
	}

	return nextJob, true
}

// removeJob removes the given job from this queue.
func (q *jobQueue) removeJob(job *job) {
	// Find the correct priority queue to delete from
	if job.priority < q.priority {
		if q.leftQueue == nil {
			log.Fatalf("Error deleting job %v: job not found in queue", job.id)
		}
		q.leftQueue.removeJob(job)
		return
	} else if job.priority > q.priority {
		if q.rightQueue == nil {
			log.Fatalf("Error deleting job %v: job not found in queue", job.id)
		}
		q.rightQueue.removeJob(job)
		return
	}

	// Matches this queues priority so delete
	if q.firstJob == nil {
		log.Fatalf("Error deleting job %v: job not found in queue", job.id)
	}

	if q.firstJob.id == job.id {
		q.firstJob = job.nextJob
		if q.firstJob == nil {
			q.lastJob = nil
		} else {
			q.firstJob.previousJob = nil
		}
	} else if q.lastJob.id == job.id {
		q.lastJob = job.previousJob
		if q.lastJob == nil {
			q.firstJob = nil
		} else {
			q.lastJob.nextJob = nil
		}
	} else {
		if job.nextJob == nil || job.previousJob == nil {
			log.Fatalf("Error deleting job %v: job missing pointer but not first or last job in queue. First job: %v, last job %v", job.id, q.firstJob.id, q.lastJob.id)
		}

		job.previousJob.nextJob = job.nextJob
		job.nextJob.previousJob = job.previousJob
	}

	job.nextJob = nil
	job.previousJob = nil
}
