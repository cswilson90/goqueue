package queue

import (
	"fmt"
	"sync"
	"time"
)

// A Job holds all the data for a single job in the queue
type job struct {
	id        uint64
	queueName string

	mutex    sync.Mutex
	priority uint32
	status   string

	reservationTimeout uint32
	reserveExpires     int64

	data []byte

	nextJob     *job
	previousJob *job
}

// NewJob creates and returns a new Job with the given data.
func newJob(id uint64, queue string, priority uint32, reservationTimeout uint32, data []byte) *job {
	return &job{
		id:                 id,
		priority:           priority,
		queueName:          queue,
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
	j.mutex.Lock()
	defer j.mutex.Unlock()

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
	j.reserveExpires = currentTime.Unix() + int64(j.reservationTimeout)

	return nil
}
