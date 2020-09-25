package queue

// A jobQueue is a single non-priorty queue of Jobs.
type jobQueue struct {
	firstJob *job
	lastJob  *job
}

// newJobQueue creates and returns a new jobQueue with the given name.
// A jobQueue's name cannot be changed after it's created.
func newJobQueue() *jobQueue {
	return &jobQueue{}
}

// addJob adds the given job to the end of the queue
func (q *jobQueue) addJob(job *job) {
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
	if q.firstJob == nil {
		return nil
	}

	nextJob := q.firstJob
	q.firstJob = nextJob.nextJob

	if q.firstJob == nil {
		q.lastJob = nil
	}

	return nextJob
}
