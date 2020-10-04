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
	Data     []byte
	Id       uint64
	Priority uint
	Queue    string
	Status   string
	Timeout  int
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

	queue := q.priorityQueue(jobData.Queue)

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

// DeleteJob deletes the job with the given ID.
// Returns an error if the job doesn't exist.
func (q *GoJobQueue) DeleteJob(id uint64) error {
	job, ok := q.jobs[id]
	if !ok {
		return fmt.Errorf("Can't delete job with ID %v: job doesn't exist", id)
	}
	if job.deleted {
		return fmt.Errorf("Job %v already deleted", id)
	}

	queue := q.priorityQueue(job.queueName)
	queue.deleteJob(job)
	delete(q.jobs, id)

	return nil
}

// jobsQueue returns the priorityJobQueue object that the given job is in.
// Teh queue will be created if it doesn't exist.
func (q *GoJobQueue) priorityQueue(queueName string) *priorityJobQueue {
	queue, ok := q.queues[queueName]
	if !ok {
		q.queues[queueName] = newPriorityJobQueue()
		queue = q.queues[queueName]
	}

	return queue
}

// internalJobToData converts an internal job to GoJobData representation
func internalJobToData(job *job) *GoJobData {
	return &GoJobData{
		Data:     job.data,
		Id:       job.id,
		Priority: job.priority,
		Queue:    job.queueName,
		Status:   job.status,
		Timeout:  job.reservationTimeout,
	}
}
