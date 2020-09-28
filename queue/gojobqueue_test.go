package queue

import (
	"testing"

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
		Priority: 5,
		Timeout:  60,
		Data:     []byte{'2', '3', '4'},
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

	_, ok = goJobQueue.ReserveJob("queue1")
	if ok {
		t.Errorf("Reserved job after all jobs already reserved")
	}
}

func TestMultipleNamedGoQueues(t *testing.T) {
	// TODO
}
