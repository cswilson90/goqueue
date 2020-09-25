package queue

// A jobQueue is a single non-priorty queue of Jobs.
type jobQueue struct {
	firstJob *job
	lastJob  *job

	// Queues are organised in a binary tree by priority
	priority   uint
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
// Returns a nil job if there are no jobs in the queue.
func (q *jobQueue) getNextJob() *job {
	// Check for a job in a higher prioirty queue
	if q.leftQueue != nil {
		higherPriJob := q.leftQueue.getNextJob()
		if higherPriJob != nil {
			return higherPriJob
		}
	}

	// If no jobs in this queue check lower priority queues
	if q.firstJob == nil {
		if q.rightQueue == nil {
			return nil
		}
		return q.rightQueue.getNextJob()
	}

	// Return job from this queue if there is one and no higher priority queue has one
	nextJob := q.firstJob
	q.firstJob = nextJob.nextJob

	if q.firstJob == nil {
		q.lastJob = nil
	} else {
		q.firstJob.previousJob = nil
	}

	return nextJob
}
