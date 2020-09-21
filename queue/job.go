package queue

import (
	"fmt"
	"time"
)

// A Job holds all the data for a single job in the queue
type job struct {
	id       uint64
	priority uint

	status string

	reservationTimeout int64
	reserveExpires     int64

	data []byte

	queue       *jobQueue
	nextJob     *job
	previousJob *job
}

// NewJob creates and returns a new Job with the given data.
func newJob(id uint64, priority uint, reservationTimeout int64, data []byte) *job {
	return &job{
		id:                 id,
		priority:           priority,
		status:             "ready",
		reservationTimeout: reservationTimeout,
		data:               data,
	}
}

// Reserved returns whether the job is currently reserved.
func (j *job) reserved() bool {
	return j.status == "reserved"
}

// Reserve reserves the job.
// The job will be reserved for the reservation timeout.
// If the reservation timeout passes without it being refreshed the job will be released.
func (j *job) reserve() error {
	if j.reserved() {
		return fmt.Errorf("Job %v is already reserved", j.id)
	}

	oldStatus := j.status
	j.status = "reserved"
	err := j.refreshReservation()
	if err != nil {
		j.status = oldStatus
		return fmt.Errorf("Failed to reserve Job %v", j.id)
	}

	return nil
}

// refreshReservation resets the reservation timeout.
// This allows more time to process the job.
func (j *job) refreshReservation() error {
	if !j.reserved() {
		return fmt.Errorf("Job %v is not reserved", j.id)
	}

	currentTime := time.Now()
	j.reserveExpires = currentTime.Unix() + j.reservationTimeout

	return nil
}

// setNextJob sets the next job in the queue after this one to be the given job.
// Returns an error if job has not yet been assigned a queue.
func (j *job) setNextJob(newJob *job) error {
	if j.queue == nil {
		return fmt.Errorf("Can't set next job for job %v as it isn't in a queue", j.id)
	}

	j.nextJob = newJob
	if newJob != nil && !newJob.previousJobIs(j) {
		return newJob.setPreviousJob(j)
	}
	return nil
}

// previousJobIs returns whether the given job matches the previous job in the queue.
// Returns an error if job has not yet been assigned a queue.
func (j *job) previousJobIs(job *job) bool {
	if job == nil || j.previousJob == nil {
		return false
	}

	return job.id == j.previousJob.id
}

// setPreviousJob sets the previous job in the queue before this one to be the given job.
func (j *job) setPreviousJob(newJob *job) error {
	if j.queue == nil {
		return fmt.Errorf("Can't set previous job for job %v as it isn't in a queue", j.id)
	}

	j.previousJob = newJob
	return nil
}

// removeFromQueue removes the job from the queue that it's currently in
// Returns an error if job has not yet been assigned a queue.
func (j *job) removeFromQueue() error {
	if j.queue == nil {
		return fmt.Errorf("Can't remove job %v from queue as it isn't in a queue", j.id)
	}

	if j.previousJob != nil {
		j.previousJob.setNextJob(j.nextJob)
		if j.nextJob == nil {
			j.queue.setFinalJob(j.previousJob)
		}
	} else {
		j.queue.setFirstJob(j.nextJob)
	}

	j.previousJob = nil
	j.nextJob = nil
	j.queue = nil
	return nil
}

// setQueue sets the queue the job is in.
func (j *job) setQueue(queue *jobQueue) {
	j.queue = queue
}
