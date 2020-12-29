package client

import (
	"testing"

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

	client := createClient(t)
	client.AddToQueue("add-queue")
}
