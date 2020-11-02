package queue

import "log"

// priorityJobQueue is a priority queue of jobs.
type priorityJobQueue struct {
	statusQueues map[string]*jobQueue

	operations chan priorityQueueOperation
}

// priorityQueueOperation defines the interface for an operation on a priorityJobQueue
// e.g. reserve, delete etc.
type priorityQueueOperation interface {
	doOperation(*priorityJobQueue)
}

// A priorityQueueOperationReponse is a response object from a priority queue operation.
type priorityQueueOperationReponse struct {
	success bool
	job     *job
}

// newPriorityJobQueue creates a new priorityJobQueue.
func newPriorityJobQueue() *priorityJobQueue {
	queue := &priorityJobQueue{
		statusQueues: map[string]*jobQueue{
			"reserved": nil,
			"ready":    nil,
			"delayed":  nil,
			"buried":   nil,
		},
		operations: make(chan priorityQueueOperation),
	}
	go queue.doOperations()
	return queue
}

// getStatusQueue gets the correct job queue for the current status of the job.
func (p *priorityJobQueue) getStatusQueue(job *job) *jobQueue {
	queue, ok := p.statusQueues[job.status]
	if !ok {
		log.Fatalf("Job %v has unknown status: %v\n", job.id, job.status)
	}

	if queue == nil {
		// No queue yet made for the status so initialise one
		p.statusQueues[job.status] = newJobQueue(job.priority)
		queue = p.statusQueues[job.status]
	}

	return queue
}

// doOperations performs all operations in the queue one at a time reading from the operations channel.
func (p *priorityJobQueue) doOperations() {
	for {
		op, ok := <-p.operations
		if !ok {
			break
		}
		op.doOperation(p)
	}
}

// addJob adds the given job to the queue.
func (p *priorityJobQueue) addJob(job *job) {
	op := &priorityQueueAdd{
		jobToAdd: job,
		response: make(chan *priorityQueueOperationReponse),
	}
	p.operations <- op

	// Wait for response before returning
	_ = <-op.response
}

// A priorityQueueAdd encapsulates an add operation
type priorityQueueAdd struct {
	jobToAdd *job
	response chan *priorityQueueOperationReponse
}

// doOperation does the operation to add the job to the queue
func (o *priorityQueueAdd) doOperation(q *priorityJobQueue) {
	statusQueue := q.getStatusQueue(o.jobToAdd)
	statusQueue.addJob(o.jobToAdd)
	o.response <- &priorityQueueOperationReponse{success: true}
}

// reserveJob gets the next ready job in the queue and reserves it.
// Second returned value is false if there is no job that can be reserved.
func (p *priorityJobQueue) reserveJob() (*job, bool) {
	op := &priorityQueueReserve{
		response: make(chan *priorityQueueOperationReponse),
	}
	p.operations <- op

	// Wait for response before returning
	opResponse := <-op.response
	return opResponse.job, opResponse.success
}

// A priorityQueueOperation encapsulates a reserve operation
type priorityQueueReserve struct {
	response chan *priorityQueueOperationReponse
}

// doOperation does the operation to reserve a job
func (o *priorityQueueReserve) doOperation(q *priorityJobQueue) {
	statusQueue := q.statusQueues["ready"]

	if statusQueue == nil {
		o.response <- &priorityQueueOperationReponse{success: false}
		return
	}

	reservedJob, ok := statusQueue.getNextJob()
	if ok {
		err := reservedJob.reserve()
		if err != nil {
			log.Fatalf("Failed to reserve job %v from ready queue: %v\n", reservedJob.id, err.Error())
		}
		newQueue := q.getStatusQueue(reservedJob)
		newQueue.addJob(reservedJob)
		o.response <- &priorityQueueOperationReponse{success: true, job: reservedJob}
		return
	}

	o.response <- &priorityQueueOperationReponse{success: false}
	return
}

// deleteJob deletes the given job from the queue
func (p *priorityJobQueue) deleteJob(job *job) {
	op := &priorityQueueDelete{
		jobToDelete: job,
		response:    make(chan *priorityQueueOperationReponse),
	}
	p.operations <- op

	// Wait for response before returning
	_ = <-op.response
}

// A priorityQueueAdd encapsulates a add operation
type priorityQueueDelete struct {
	jobToDelete *job
	response    chan *priorityQueueOperationReponse
}

// doOperation does the operation to delete a job
func (o *priorityQueueDelete) doOperation(q *priorityJobQueue) {
	statusQueue := q.getStatusQueue(o.jobToDelete)
	statusQueue.removeJob(o.jobToDelete)
	o.response <- &priorityQueueOperationReponse{success: true}
}
