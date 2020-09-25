package queue

import "testing"

func TestQueuing(t *testing.T) {
	queue := newJobQueue()

	job1 := newJob(1, 2, 60, []byte{'1', '2', '3'})
	queue.addJob(job1)

	nextJob := queue.getNextJob()
	if nextJob.id != job1.id {
		t.Errorf("Wrong job after adding and getting one, got job %v", nextJob.id)
	}

	// Checking adding multiple jobs and reading them off in order
	job2 := newJob(2, 2, 60, []byte{'2', '3', '4'})
	job3 := newJob(3, 2, 60, []byte{'3', '4', '5'})

	jobs := [3]*job{job1, job2, job3}
	for _, job := range jobs {
		queue.addJob(job)
	}

	for _, job := range jobs {
		nextJob := queue.getNextJob()
		if nextJob.id != job.id {
			t.Errorf("Got jobs off queue in wrong order: got job %v expected %v", nextJob.id, job.id)
		}
	}
}
