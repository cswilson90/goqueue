package queue

import (
    "bytes"
    "testing"
    "time"
)

func TestNewJob(t *testing.T) {
    job := NewJob(1, 2, 60, []byte{'2', '3', '4'});

    if job.ID() != 1 {
        t.Errorf("New Job ID expected to be 1, got: %v", job.ID())
    }

    if job.Priority() != 2 {
        t.Errorf("New Job priority expected to be 2, got: %v", job.Priority());
    }

    if job.Status() != "ready" {
        t.Errorf("New Job status expected to be 'ready', got: '%v'", job.Status());
    }

    if job.timeToProcess != 60 {
        t.Errorf("New Job TTR expected to be 60, got: %v", job.timeToProcess);
    }

    if !bytes.Equal(job.Data(), []byte{'2', '3', '4'}) {
        t.Errorf("New Job data expected to be [2, 3, 4], got: %v", job.Data());
    }
}

func TestReservation(t *testing.T) {
    job := NewJob(1, 2, 60, []byte{'2', '3', '4'});

    if job.Reserved() {
        t.Error("Newly created Job is reserved")
    }

    job.Reserve()
    if !job.Reserved() {
        t.Error("Failed to reserved Job")
    }

    if job.reserveExpires <= time.Now().Unix() {
        t.Error("Job reserve expiry time not after now");
    }

    oldExpiry := job.reserveExpires
    time.Sleep(1000 * time.Millisecond)

    job.RefreshReservation()
    if job.reserveExpires <= oldExpiry {
        t.Error("Failed to refresh the Job reservation");
    }
}
