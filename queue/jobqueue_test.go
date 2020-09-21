package queue

import "testing"

func TestQueuing(t *testing.T) {
	queue := newJobQueue("test_queue")

	if queue.name != "test_queue" {
		t.Errorf("New jobQueue expected to be called 'test_queue' but is %v", queue.name)
	}

	job1 := newJob(1, 2, 60, []byte{'1', '2', '3'})
	err := queue.addJob(job1)
	if err != nil {
		t.Error("Couldn't add job1 to queue: " + err.Error())
	}

	nextJob, err := queue.getNextJob()
	if err != nil {
		t.Error("Couldn't get job1 from queue: " + err.Error())
	}

	if nextJob.id != job1.id {
		t.Errorf("Wrong job after adding and getting one, got job %v", nextJob.id)
	}

	if job1.queue != nil {
		t.Error("job1 still has queue set after removal from queue")
	}

	// Checking adding multiple jobs and reading them off in order
	job2 := newJob(2, 2, 60, []byte{'2', '3', '4'})
	job3 := newJob(3, 2, 60, []byte{'3', '4', '5'})

	jobs := [3]*job{job1, job2, job3}
	for i, job := range jobs {
		err := queue.addJob(job)
		if err != nil {
			t.Errorf("Couldn't add job %v to queue: "+err.Error(), i)
		}
	}

	for i, job := range jobs {
		nextJob, err := queue.getNextJob()
		if err != nil {
			t.Errorf("Couldn't get job %v from queue again: "+err.Error(), i)
		}
		if nextJob.id != job.id {
			t.Errorf("Got jobs off queue in wrong order: got job %v expected %v", nextJob.id, job.id)
		}
	}
}
