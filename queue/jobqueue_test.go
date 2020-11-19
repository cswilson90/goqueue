package queue

import "testing"

func TestQueuing(t *testing.T) {
	queue := newJobQueue(2)

	job1 := newJob(1, "queue1", 2, 60, []byte{'1', '2', '3'})
	queue.addJob(job1)

	nextJob, ok := queue.getNextJob()
	if !ok {
		t.Errorf("Could got next job when expecting job 1")
	}
	if nextJob.id != job1.id {
		t.Errorf("Wrong job after adding and getting one, got job %v", nextJob.id)
	}

	// Checking adding multiple jobs and reading them off in order
	job2 := newJob(2, "queue1", 2, 60, []byte{'2', '3', '4'})
	job3 := newJob(3, "queue1", 2, 60, []byte{'3', '4', '5'})

	jobs := [3]*job{job1, job2, job3}
	for _, job := range jobs {
		queue.addJob(job)
	}

	for _, job := range jobs {
		nextJob, ok := queue.getNextJob()
		if !ok {
			t.Errorf("Could not get next job, expecting job: %v", job.id)
		}
		if nextJob.id != job.id {
			t.Errorf("Got jobs off queue in wrong order: got job %v expected %v", nextJob.id, job.id)
		}
	}
}

func TestRemovingJobs(t *testing.T) {
	queue := newJobQueue(2)

	jobs := make([]*job, 0, 20)
	for i := 1; i <= 20; i++ {
		newJob := newJob(uint64(i), "queue1", uint32(i%3), 60, []byte{'1', '2', '3'})
		jobs = append(jobs, newJob)
		queue.addJob(newJob)
	}

	// order was randomly selected
	deleteOrder := [20]int{0, 3, 1, 12, 13, 4, 6, 17, 8, 7, 10, 18, 19, 5, 14, 2, 15, 16, 9, 11}
	for _, jobIndex := range deleteOrder {
		queue.removeJob(jobs[jobIndex])
	}
}
