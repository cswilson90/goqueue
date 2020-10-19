package queue

import "log"

import "sync"

// A jobQueue is a single non-priorty queue of Jobs.
type jobQueue struct {
	// jobMutex protects firstJob and lastJob and should be locked when adding or
	// removing jobs from the queue
	jobMutex sync.Mutex
	firstJob *job
	lastJob  *job

	// Queues are organised in a binary tree by priority
	priority uint

	// queueMutex protects leftQueue and rightQueue
	queueMutex sync.Mutex
	leftQueue  *jobQueue
	rightQueue *jobQueue
}

// newJobQueue creates and returns a new jobQueue with the given name.
// A jobQueue's name cannot be changed after it's created.
func newJobQueue(priority uint) *jobQueue {
	return &jobQueue{
		priority: priority,
	}
}

// addJob adds the given job to the end of the queue
func (q *jobQueue) addJob(job *job) {
	// If the job priority does not match this queue pass it on to the next
	// queue which is created if necessary
	if job.priority < q.priority {
		q.queueMutex.Lock()
		if q.leftQueue == nil {
			q.leftQueue = newJobQueue(job.priority)
		}
		q.queueMutex.Unlock()
		q.leftQueue.addJob(job)
		return
	} else if job.priority > q.priority {
		q.queueMutex.Lock()
		if q.rightQueue == nil {
			q.rightQueue = newJobQueue(job.priority)
		}
		q.queueMutex.Unlock()
		q.rightQueue.addJob(job)
		return
	}

	// Job priority matches this queue so add it here
	q.jobMutex.Lock()
	if q.firstJob == nil {
		q.firstJob = job
	} else {
		q.lastJob.nextJob = job
		job.previousJob = q.lastJob
	}

	q.lastJob = job
	q.jobMutex.Unlock()
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

	q.jobMutex.Lock()
	defer q.jobMutex.Unlock()

	nextJob := q.firstJob

	// Check if job has been deleted and remove from queue if so
	for nextJob != nil && nextJob.deleted {
		q.removeJob(nextJob)
		nextJob = q.firstJob
	}

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
	// Matches this queues priority so delete
	if q.firstJob == nil {
		log.Fatalf("Error deleting job %v: job queue is empty", job.id)
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
			log.Fatalf("Error deleting job %v: job missing pointer but not first or last job in queue", job.id)
		}

		job.previousJob.nextJob = job.nextJob
		job.nextJob.previousJob = job.previousJob
	}

	job.nextJob = nil
	job.previousJob = nil
}
