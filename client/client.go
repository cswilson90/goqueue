package client

import (
	"net"
)

const connType = "tcp"

type GoQueueClient struct {
	conn net.Conn

	addQueue     string
	reserveQueue string
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
func (client *GoQueueClient) AddToQueue(queue string) {
	client.addQueue = queue
}

// ReserveQueue sets the tube that jobs will be reserved from.
func (client *GoQueueClient) ReserveQueue(queue string) {
	client.reserveQueue = queue
}

// AddJob adds a job to the server.
func (client *GoQueueClient) AddJob() error {
	return nil
}

// ReserveJob reserves a jobs from the server.
func (client *GoQueueClient) ReserveJob() error {
	return nil
}

// DeleteJob deletes a job from the server.
func (client *GoQueueClient) DeleteJob() error {
	return nil
}

// connect tries a connection to the server and returns an error if the connection failed.
func (client *GoQueueClient) connect() error {
	return nil
}
