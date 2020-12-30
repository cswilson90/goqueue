package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"github.com/cswilson90/goqueue/internal/data"
)

const connType = "tcp"

// Error returned when a request timees out
var TimeoutError = errors.New("Request timed out")

// GoQueueClient is a connection to a goqueue server and is used to manipulate jobs on the server.
// By default the client will use the "default" queue for adding and reserving jobs.
type GoQueueClient struct {
	conn net.Conn

	addQueue     string
	reserveQueue string
}

// GoQueueJob represents a job on the go queue server.
type GoQueueJob struct {
	Data     []byte
	Id       uint64
	Priority uint32
	Queue    string
	Status   string
	Timeout  uint32
}

// NewGoQueueClient creates a new goqueue client connected to the goqueue server specified by the host and port.
// Returns an error if the server can't be connected to.
func NewGoQueueClient(connHost, connPort string) (*GoQueueClient, error) {
	conn, err := net.Dial(connType, connHost+":"+connPort)
	if err != nil {
		return nil, err
	}

	client := &GoQueueClient{
		conn:         conn,
		addQueue:     "default",
		reserveQueue: "default",
	}

	err = client.connect()
	if err != nil {
		return nil, err
	}

	return client, nil
}

// AddToTube sets the tube that jobs will be added to.
func (client *GoQueueClient) AddQueue(queue string) {
	client.addQueue = queue
}

// ReserveQueue sets the tube that jobs will be reserved from.
func (client *GoQueueClient) ReserveQueue(queue string) {
	client.reserveQueue = queue
}

// AddJob adds a job to the server.
// Adds the job to the queue specfied with the AddQueue function or "default" if no queue has been set.
func (client *GoQueueClient) AddJob(priority, ttp uint32, jobData []byte) (uint64, error) {
	request := data.PackString("ADD")
	request = append(request, data.PackString(client.addQueue)...)
	request = append(request, data.PackUint32(priority)...)
	request = append(request, data.PackUint32(ttp)...)

	packedJobData, err := data.PackJobData(jobData)
	if err != nil {
		return 0, err
	}
	request = append(request, packedJobData...)

	cmdReader, err := client.makeRequest(request, "ADDED")
	if err != nil {
		return 0, err
	}

	jobID, err := data.ParseUint64(cmdReader)
	if err != nil {
		return 0, fmt.Errorf("Failed to get ID of added job")
	}

	return jobID, nil
}

// ReserveJob reserves a job from the server.
// Reserves a job from the queue specfied with the ReserveQueue function or "default" if no queue has been set.
// Returns a TimeoutError if the request timed out.
func (client *GoQueueClient) ReserveJob(timeout uint32) (*GoQueueJob, error) {
	request := data.PackString("RESERVE")
	request = append(request, data.PackString(client.reserveQueue)...)
	request = append(request, data.PackUint32(timeout)...)

	cmdReader, err := client.makeRequest(request, "RESERVED")
	if err != nil {
		return nil, err
	}

	internalJob, err := data.ParseJob(cmdReader)
	if err != nil {
		return nil, err
	}

	return &GoQueueJob{
		Data:     internalJob.Data,
		Id:       internalJob.Id,
		Priority: internalJob.Priority,
		Queue:    internalJob.Queue,
		Status:   internalJob.Status,
		Timeout:  internalJob.Timeout,
	}, nil
}

// DeleteJob deletes a job from the server.
func (client *GoQueueClient) DeleteJob(job *GoQueueJob) error {
	request := data.PackString("DELETE")
	request = append(request, data.PackUint64(job.Id)...)

	_, err := client.makeRequest(request, "OK")
	if err != nil {
		return err
	}

	return nil
}

// connect tries a connection to the server and returns an error if the connection failed.
func (client *GoQueueClient) connect() error {
	_, err := client.makeRequest(data.PackString("CONNECT"), "OK")
	if err != nil {
		return err
	}

	return nil
}

// makeRequest makes a request to the server and checks the response given.
// Returns a bufio.Reader fo reading the response of the request.
// Returns an error if there is an error, a timeout or the response does not match the expected response.
func (client *GoQueueClient) makeRequest(request []byte, expectedResponse string) (*bufio.Reader, error) {
	_, err := client.conn.Write(request)
	if err != nil {
		return nil, err
	}

	cmdReader := bufio.NewReader(client.conn)
	response, err := data.ParseCommand(cmdReader)
	if err != nil {
		return nil, fmt.Errorf("Failed to get response from server: " + err.Error())
	}

	if response == "ERROR" {
		errorString, err := data.ParseString(cmdReader)
		if err != nil {
			return cmdReader, err
		}
		return cmdReader, errors.New(errorString)
	}

	if response == "TIMEOUT" {
		return cmdReader, TimeoutError
	}

	if response != expectedResponse {
		return cmdReader, fmt.Errorf("Expected '%v' response from server but got: '%v'", expectedResponse, response)
	}

	return cmdReader, nil
}
