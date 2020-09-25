package queue

import "testing"

func TestPriorityQueuing(t *testing.T) {
	queue := newPriorityJobQueue()

	nilJob := queue.reserveJob()
	if nilJob != nil {
		t.Error("Expected nil when trying to reserve job from new empty queue")
	}

	jobPriorites := [6]uint{2, 1, 4, 1, 2, 3}
	for i, pri := range jobPriorites {
		newJob := newJob(uint64(i+1), pri, 60, []byte{'1', '2', '3'})
		queue.addJob(newJob)
	}

	expectedJobs := [6]uint64{2, 4, 1, 5, 6, 3}
	for _, expectedID := range expectedJobs {
		nextJob := queue.reserveJob()
		if nextJob.id != expectedID {
			t.Errorf("Reserved job %v, expected %v", nextJob.id, expectedID)
		}

		if nextJob.status != "reserved" {
			t.Errorf("Job %v status %v when it should be 'reserved'", nextJob.id, nextJob.status)
		}
	}

	nilJob = queue.reserveJob()
	if nilJob != nil {
		t.Error("Expected nil when trying to reserve job from empty queue")
	}
}
