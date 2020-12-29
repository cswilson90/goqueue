package queue

import (
	"fmt"
	"sync"
)

// A GoJobQueue manages a group of named priority queues.
type GoJobQueue struct {
	// queueMutex protects the queues map
	queueMutex sync.Mutex
	queues     map[string]*priorityJobQueue

	// jobIdMutex protects nextJobID
	jobIdMutex sync.Mutex
	nextJobID  uint64

	// jobsMutex protects the jobs map
	jobsMutex sync.Mutex
	jobs      map[uint64]*job
}

// A GoJobData object represents the data for a single job in a GoJobQueue.
type GoJobData struct {
	Data     []byte
	Id       uint64
	Priority uint32
	Queue    string
	Status   string
	Timeout  uint32
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

	newJob := newJob(q.getNextJobId(), jobData.Queue, jobData.Priority, jobData.Timeout, jobData.Data)
	jobData.Id = newJob.id

	queue := q.priorityQueue(jobData.Queue)

	queue.addJob(newJob)

	q.jobsMutex.Lock()
	q.jobs[newJob.id] = newJob
	q.jobsMutex.Unlock()

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
	q.jobsMutex.Lock()

	job, ok := q.jobs[id]
	if !ok {
		q.jobsMutex.Unlock()
		return fmt.Errorf("Can't delete job with ID %v: job doesn't exist", id)
	}
	delete(q.jobs, id)
	q.jobsMutex.Unlock()

	queue, ok := q.queues[job.queueName]
	if ok {
		queue.deleteJob(job)
	}

	return nil
}

// NumJobs returns the total number of jobs in all queues.
func (q *GoJobQueue) NumJobs() int {
	return len(q.jobs)
}

// jobsQueue returns the priorityJobQueue object that the given job is in.
// Teh queue will be created if it doesn't exist.
func (q *GoJobQueue) priorityQueue(queueName string) *priorityJobQueue {
	q.queueMutex.Lock()
	queue, ok := q.queues[queueName]
	if !ok {
		q.queues[queueName] = newPriorityJobQueue()
		queue = q.queues[queueName]
	}

	q.queueMutex.Unlock()
	return queue
}

// getNextJobId returns the next free job ID and increments the counter.
func (q *GoJobQueue) getNextJobId() uint64 {
	q.jobIdMutex.Lock()
	nextID := q.nextJobID
	q.nextJobID++
	q.jobIdMutex.Unlock()
	return nextID
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
