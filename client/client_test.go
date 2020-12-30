package client

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cswilson90/goqueue/internal/server"
)

const (
	connHost = "localhost"
	connPort = "11223"
)

// createServer is a helper function to create a test server
func createServer(t *testing.T) *server.GoJobServer {
	server, err := server.NewGoJobServer(connHost, connPort)
	if err != nil {
		t.Errorf("Failed to create test server")
	}
	return server
}

// createClient is a helper function to create a test connection to the server
func createClient(t *testing.T) *GoQueueClient {
	conn, err := NewGoQueueClient(connHost, connPort)
	if err != nil {
		t.Errorf("Failed to create test client")
	}
	return conn
}

func TestClientConnect(t *testing.T) {
	server := createServer(t)
	go server.Run()
	defer server.Exit()

	createClient(t)
}

func TestClient(t *testing.T) {
	assert := assert.New(t)

	server := createServer(t)
	go server.Run()
	defer server.Exit()

	client := createClient(t)
	client.AddQueue("queue-1")
	client.ReserveQueue("queue-1")

	id, err := client.AddJob(1, 60, []byte{'1', '2', '3'})
	if err != nil {
		t.Errorf(err.Error())
	}
	assert.Equal(uint64(1), id, "Incorrect added job ID")

	job, err := client.ReserveJob(1)
	if err != nil {
		t.Errorf(err.Error())
	}
	assert.Equal(uint64(1), job.Id, "Incorrect reserved job ID")

	err = client.DeleteJob(job)
	if err != nil {
		t.Errorf(err.Error())
	}

	// Check timeout
	job, err = client.ReserveJob(1)
	assert.Equal(TimeoutError, err, "Expected timeout error")
}
