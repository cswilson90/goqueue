package client

import (
	"bufio"
	"errors"
	"fmt"
	"net"

	"github.com/cswilson90/goqueue/internal/data"
)

const connType = "tcp"

type GoQueueClient struct {
	conn net.Conn

	addQueue     string
	reserveQueue string
}

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
// By default the client will use the "default" queue for adding and reserving jobs.
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
func (client *GoQueueClient) AddJob(priority, ttp uint32, jobData []byte) (uint64, error) {
	request := make([]byte, 0)
	request = append(request, data.PackString("ADD")...)
	request = append(request, data.PackString(client.addQueue)...)
	request = append(request, data.PackUint32(priority)...)
	request = append(request, data.PackUint32(ttp)...)

	packedJobData, err := data.PackJobData(jobData)
	if err != nil {
		return 0, err
	}
	request = append(request, packedJobData...)

	response, err := makeRequest(request, client.conn)
	if err != nil {
		return 0, err
	}
	if response != "ADDED" {
		return 0, fmt.Errorf("Expected response 'ADDED' when adding job but got: '" + response + "'")
	}

	cmdReader := bufio.NewReader(client.conn)
	jobID, err := data.ParseUint64(cmdReader)
	if err != nil {
		return 0, fmt.Errorf("Failed to get job ID of added job")
	}

	return jobID, nil
}

// ReserveJob reserves a jobs from the server.
func (client *GoQueueClient) ReserveJob() (*GoQueueJob, error) {
	return nil, nil
}

// DeleteJob deletes a job from the server.
func (client *GoQueueClient) DeleteJob(job *GoQueueJob) error {
	return nil
}

// connect tries a connection to the server and returns an error if the connection failed.
func (client *GoQueueClient) connect() error {
	response, err := makeRequest(data.PackString("CONNECT"), client.conn)
	if err != nil {
		return err
	}

	if response != "OK" {
		return fmt.Errorf("Expected 'OK' response from server when trying to connect but got: %v", response)
	}

	return nil
}

func makeRequest(request []byte, conn net.Conn) (string, error) {
	_, err := conn.Write(request)
	if err != nil {
		return "", err
	}

	cmdReader := bufio.NewReader(conn)
	response, err := data.ParseCommand(cmdReader)
	if err != nil {
		return "", fmt.Errorf("Failed to get response from server: " + err.Error())
	}

	if response == "ERROR" {
		errorString, err := data.ParseString(cmdReader)
		if err != nil {
			return "", err
		}
		return "", errors.New(errorString)
	}

	return response, nil
}
