package queue

import "fmt"

// A GoJobQueue manages a group of named priority queues.
type GoJobQueue struct {
	queues map[string]*priorityJobQueue

	nextJobID uint64
	jobs      map[uint64]*job
}

// A GoJobData object represents the data for a single job in a GoJobQueue.
type GoJobData struct {
	Id       uint64
	Priority uint
	Queue    string
	Timeout  int
	Data     []byte
}

// NewGoJobQueue creates a new GoJobQueue.
func NewGoJobQueue() *GoJobQueue {
	return &GoJobQueue{
		jobs:      make(map[uint64]*job),
		nextJobID: 1,
		queues:    make(map[string]*priorityJobQueue),
	}
}

// AddJob creates a job with the given GoJobData and adds it to the queue named in the data.
// This function assigns an ID to the job so the given GoJobData should not have an id assigned
// before passing it to this function.
// Returns an error if the jobData already has an id assigned or if the queue name is empty.
func (q *GoJobQueue) AddJob(jobData *GoJobData) error {
	if jobData.Id != 0 {
		return fmt.Errorf("Tried to add job to GoJobQueue which already had ID: %v", jobData.Id)
	}

	if jobData.Queue == "" {
		return fmt.Errorf("Tried to add job to a queue with no name")
	}

	newJob := newJob(q.nextJobID, jobData.Queue, jobData.Priority, jobData.Timeout, jobData.Data)
	jobData.Id = newJob.id
	q.nextJobID++

	queue, ok := q.queues[jobData.Queue]
	if !ok {
		q.queues[jobData.Queue] = newPriorityJobQueue()
		queue = q.queues[jobData.Queue]
	}

	queue.addJob(newJob)
	q.jobs[newJob.id] = newJob

	return nil
}

// GetJobData returns the job data for the job with the given ID.
// If the job with the given ID does not exist the second return value will be false.
func (q *GoJobQueue) GetJobData(id uint64) (*GoJobData, bool) {
	internalJob, ok := q.jobs[id]
	if !ok {
		return nil, false
	}

	return internalJobToData(internalJob), true
}

// ReserveJob reserves a job from the queue with the given name.
// If no job can be reserved from the queue the second returned value will be false.
func (q *GoJobQueue) ReserveJob(queueName string) (*GoJobData, bool) {
	queue, exists := q.queues[queueName]
	if !exists {
		return nil, false
	}

	internalJob, ok := queue.reserveJob()
	if !ok {
		return nil, false
	}

	return internalJobToData(internalJob), true
}

// internalJobToData converts an internal job to GoJobData representation
func internalJobToData(job *job) *GoJobData {
	return &GoJobData{
		Id:       job.id,
		Priority: job.priority,
		Queue:    job.queueName,
		Timeout:  job.reservationTimeout,
		Data:     job.data,
	}
}
