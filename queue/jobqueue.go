package queue

// A jobQueue is a single non-priorty queue of Jobs.
type jobQueue struct {
	name string

	firstJob *job
	lastJob  *job
}

// newJobQueue creates and returns a new jobQueue with the given name.
// A jobQueue's name cannot be changed after it's created.
func newJobQueue(name string) *jobQueue {
	return &jobQueue{
		name: name,
	}
}

// addJob adds the given job to the end of the queue
func (q *jobQueue) addJob(job *job) error {
	job.setQueue(q)

	if q.firstJob == nil {
		q.firstJob = job
	} else {
		err := q.lastJob.setNextJob(job)
		if err != nil {
			return err
		}
	}

	q.lastJob = job
	return nil
}

// getNextJob gets the next job in the queue, removes it from the queue and returns it.
// Returns a nil job if there are no jobs in the queue
func (q *jobQueue) getNextJob() (*job, error) {
	if q.firstJob == nil {
		return nil, nil
	}

	nextJob := q.firstJob

	err := nextJob.removeFromQueue()
	if err != nil {
		return nil, err
	}

	return nextJob, nil
}

// setFirstJob sets the first job in the queue to be the given job.
// Setting the first job to be nil will also set the final job to nil as a queue
// can't have a final job but not a first one.
func (q *jobQueue) setFirstJob(job *job) {
	q.firstJob = job
	if job != nil {
		job.setPreviousJob(nil)
	} else {
		q.setFinalJob(nil)
	}
}

// setFirstJob sets the final job in the queue to be the given job.
func (q *jobQueue) setFinalJob(job *job) {
	q.lastJob = job
	if job != nil {
		job.setNextJob(nil)
	}
}
