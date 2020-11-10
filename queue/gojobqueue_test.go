package queue

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestEmptyGoJobQueue(t *testing.T) {
	goJobQueue := NewGoJobQueue()

	_, ok := goJobQueue.ReserveJob("queue1")
	if ok {
		t.Error("Reserved job from empty queue")
	}

	_, ok = goJobQueue.GetJobData(1)
	if ok {
		t.Error("Got job data for job 1 from empty queue")
	}
}

func TestSingleGoQueueJob(t *testing.T) {
	goJobQueue := NewGoJobQueue()

	job1 := &GoJobData{
		Data:     []byte{'2', '3', '4'},
		Priority: 5,
		Timeout:  60,
	}

	err := goJobQueue.AddJob(job1)
	if err == nil {
		t.Errorf("Managed to create a job with no queue")
	}

	job1.Queue = "queue1"
	err = goJobQueue.AddJob(job1)
	if err != nil {
		t.Errorf("Failed to add job1 to queue")
	}
	if job1.Id != 1 {
		t.Errorf("job1 was not assigned ID 1")
	}

	job1Clone, ok := goJobQueue.GetJobData(1)
	if !ok {
		t.Errorf("Failed to get job data for job 1")
	}
	if job1Clone.Id != 1 || !cmp.Equal(job1.Data, job1Clone.Data) {
		t.Errorf("Getting data for job1 returned different data than given")
	}

	job1Reserved, ok := goJobQueue.ReserveJob("queue1")
	if !ok {
		t.Errorf("Failed to reserve job 1")
	}
	if job1Reserved.Id != 1 {
		t.Errorf("Reserved job does not have ID 1 as expected")
	}
	if !cmp.Equal(job1Reserved.Data, job1.Data) {
		t.Errorf("Reserved job has different data to expected")
	}
	if job1Reserved.Status != "reserved" {
		t.Errorf("Reserved job does not have 'reserved' status")
	}

	err = goJobQueue.DeleteJob(job1Reserved.Id)
	if err != nil {
		t.Errorf("Error deleting job 1: " + err.Error())
	}
	_, ok = goJobQueue.GetJobData(1)
	if ok {
		t.Errorf("Got job data for deleted job 1")
	}

	_, ok = goJobQueue.ReserveJob("queue1")
	if ok {
		t.Errorf("Reserved job after all jobs already reserved")
	}
}

func TestMultipleGoQueueJobs(t *testing.T) {
	goJobQueue := NewGoJobQueue()

	jobPriorites := [6]uint{2, 1, 4, 1, 2, 3}
	for i, pri := range jobPriorites {
		newJob := &GoJobData{
			Data:     []byte{'2', '3', '4'},
			Priority: pri,
			Queue:    "queue1",
			Timeout:  60,
		}
		err := goJobQueue.AddJob(newJob)
		if err != nil {
			t.Errorf("Error queuing job %v: "+err.Error(), i)
		}
	}

	job4Data, ok := goJobQueue.GetJobData(4)
	if !ok {
		t.Errorf("Failed to get job data for job 4")
	}
	if job4Data.Id != 4 {
		t.Errorf("Tried to get job data for job 4 but got job %v", job4Data.Id)
	}

	expectedJobs := [6]uint64{2, 4, 1, 5, 6, 3}
	for _, expectedID := range expectedJobs {
		nextJob, ok := goJobQueue.ReserveJob("queue1")
		if !ok {
			t.Errorf("Failed to reserve job, expected job %v", expectedID)
		}
		if nextJob.Id != expectedID {
			t.Errorf("Reserved job %v, expected %v", nextJob.Id, expectedID)
		}

		if nextJob.Status != "reserved" {
			t.Errorf("Job %v status %v when it should be 'reserved'", nextJob.Id, nextJob.Status)
		}
	}
}

func TestMultipleGoQueueQueues(t *testing.T) {
	goJobQueue := NewGoJobQueue()

	jobPriorites := [6]uint{2, 1, 1, 4, 2, 3}
	for i, pri := range jobPriorites {
		// Alternate queue names
		queueName := "queue1"
		if i%2 == 0 {
			queueName = "queue2"
		}

		newJob := &GoJobData{
			Data:     []byte{'2', '3', '4'},
			Priority: pri,
			Queue:    queueName,
			Timeout:  60,
		}
		err := goJobQueue.AddJob(newJob)
		if err != nil {
			t.Errorf("Error queuing job %v: "+err.Error(), i)
		}
	}

	expectedJobs := [6]uint64{3, 2, 1, 6, 5, 4}
	for i, expectedID := range expectedJobs {
		// Alternate queue names
		queueName := "queue1"
		if i%2 == 0 {
			queueName = "queue2"
		}

		nextJob, ok := goJobQueue.ReserveJob(queueName)
		if !ok {
			t.Errorf("Failed to reserve job, expected job %v", expectedID)
		}
		if nextJob.Id != expectedID {
			t.Errorf("Reserved job %v, expected %v", nextJob.Id, expectedID)
		}

		if nextJob.Status != "reserved" {
			t.Errorf("Job %v status %v when it should be 'reserved'", nextJob.Id, nextJob.Status)
		}
	}
}

func TestConcurrentGoQueue(t *testing.T) {
	goJobQueue := NewGoJobQueue()

	finished := make(chan bool)

	// Implement timeout
	timeout := make(chan bool)
	go func() {
		time.Sleep(5 * time.Second)
		timeout <- true
	}()

	// Concurrently add and reserve/delete 20 jobs
	go queueTestJobs(t, goJobQueue, "queue1", 5, finished)
	go queueTestJobs(t, goJobQueue, "queue2", 5, finished)
	go queueTestJobs(t, goJobQueue, "queue1", 5, finished)
	go queueTestJobs(t, goJobQueue, "queue2", 5, finished)
	go reserveTestJobs(t, goJobQueue, "queue1", 10, finished)
	go reserveTestJobs(t, goJobQueue, "queue2", 10, finished)

	// Wait for all goroutines to finish
	for i := 0; i < 6; i++ {
		switch {
		case <-finished:
			continue
		case <-timeout:
			t.Errorf("Timeout on concurrent adding and reserving")
			break
		}
	}

	// Check there are no jobs left in the queue
	numJobs := goJobQueue.NumJobs()
	if numJobs != 0 {
		t.Errorf("%v jobs left in queue when all should be deleted", numJobs)
	}
}

func queueTestJobs(t *testing.T, queue *GoJobQueue, queueName string, numJobs int, finished chan<- bool) {
	// helper function that queues jobs

	for i := 0; i < numJobs; i++ {
		jobData := &GoJobData{
			Data:     []byte{'2', '3', '4'},
			Priority: uint(i % 2),
			Queue:    queueName,
			Timeout:  60,
		}
		err := queue.AddJob(jobData)
		if err != nil {
			t.Errorf("Error adding job " + err.Error())
		}
	}

	finished <- true
}

func reserveTestJobs(t *testing.T, queue *GoJobQueue, queueName string, numJobs int, finished chan<- bool) {
	// helper function that reserves and deletes jobs

	for i := 0; i < numJobs; i++ {
		for {
			nextJob, ok := queue.ReserveJob(queueName)
			if !ok {
				time.Sleep(1 * time.Microsecond)
				continue
			}

			queue.DeleteJob(nextJob.Id)
			break
		}
	}

	finished <- true
}
