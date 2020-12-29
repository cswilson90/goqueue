package queue

import (
	"bytes"
	"testing"
	"time"
)

func TestNewJob(t *testing.T) {
	job := newJob(1, "queue1", 2, 60, []byte{'2', '3', '4'})

	if job.id != 1 {
		t.Errorf("New Job ID expected to be 1, got: %v", job.id)
	}

	if job.priority != 2 {
		t.Errorf("New Job priority expected to be 2, got: %v", job.priority)
	}

	if job.status != "ready" {
		t.Errorf("New Job status expected to be 'ready', got: '%v'", job.status)
	}

	if job.reservationTimeout != 60 {
		t.Errorf("New Job timeout expected to be 60, got: %v", job.reservationTimeout)
	}

	if !bytes.Equal(job.data, []byte{'2', '3', '4'}) {
		t.Errorf("New Job data expected to be [2, 3, 4], got: %v", job.data)
	}
}

func TestReservation(t *testing.T) {
	job := newJob(1, "queue1", 2, 60, []byte{'2', '3', '4'})

	if job.reserved() {
		t.Error("Newly created Job is reserved")
	}

	job.reserve()
	if !job.reserved() {
		t.Error("Failed to reserved Job")
	}

	if job.reserveExpires <= time.Now().Unix() {
		t.Error("Job reserve expiry time not after now")
	}

	oldExpiry := job.reserveExpires
	time.Sleep(1000 * time.Millisecond)

	job.refreshReservation()
	if job.reserveExpires <= oldExpiry {
		t.Error("Failed to refresh the Job reservation")
	}
}
