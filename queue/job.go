package queue

import (
	"fmt"
	"time"
)

// A Job holds all the data for a single job in the queue
type Job struct {
	id       uint64
	priority uint

	status string

	reservationTimeout  int64
	reserveExpires int64

	data []byte
}

// NewJob creates and returns a new Job with the given data.
func NewJob(id uint64, priority uint, reservationTimeout int64, data []byte) *Job {
	return &Job{
		id:            id,
		priority:      priority,
		status:        "ready",
		reservationTimeout: reservationTimeout,
		data:          data,
	}
}

// ID returns the ID of the job
func (j *Job) ID() uint64 {
    return j.id
}

// Data returns the raw data of the job.
func (j *Job) Data() []byte {
	return j.data
}

// Priority returns the priority of the job.
// A lower number means a higher priority.
func (j *Job) Priority() uint {
	return j.priority
}

// Reserved returns whether the job is currently reserved.
func (j *Job) Reserved() bool {
	return j.status == "reserved"
}

// Reserve reserves the job.
// The job will be reserved for the reservation timeout.
// If the reservation timeout passes without it being refreshed the job will be released.
func (j *Job) Reserve() error {
	if j.Reserved() {
		return fmt.Errorf("Job %v is already reserved", j.id)
	}

	oldStatus := j.status
	j.status = "reserved"
	err := j.RefreshReservation()
	if err != nil {
		j.status = oldStatus
		return fmt.Errorf("Failed to reserve Job %v", j.id)
	}

	return nil
}

// RefreshReservation resets the reservation timeout.
// This allows more time to process the job.
func (j *Job) RefreshReservation() error {
	if !j.Reserved() {
		return fmt.Errorf("Job %v is not reserved", j.id)
	}

	currentTime := time.Now()
	j.reserveExpires = currentTime.Unix() + j.reservationTimeout

	return nil
}

// Returns the string representation of the current status of the job.
func (j *Job) Status() string {
	return j.status
}
