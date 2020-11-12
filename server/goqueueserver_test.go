package server

import (
	"bufio"
	"net"
	"testing"
)

const (
	connHost = "localhost"
	connPort = "11223"
	connType = "tcp"
)

// createServer is a helper function to create a test server
func createServer(t *testing.T) *GoJobServer {
	server, err := NewGoJobServer(connHost, connPort)
	if err != nil {
		t.Errorf("Failed to create test server")
	}
	return server
}

// createClient is a helper function to create a test connection to the server
func createClient(t * testing.T) net.Conn {
	conn, err := net.Dial(connType, connHost+":"+connPort)
	if err != nil {
		t.Errorf("Failed to create test client")
	}
	return conn
}

func TestGoQueueServer(t *testing.T) {
	server := createServer(t)
	go server.Run()
	defer server.Exit()

	client := createClient(t)

	// Test repeat requests
	for i := 0; i < 2; i++ {
		client.Write([]byte("STATS\n"))

		buffer, err := bufio.NewReader(client).ReadBytes('\n')
		if err != nil {
			t.Errorf("Failed to stats job from connection")
		}

		returnString := string(buffer[:len(buffer)-1])
		if returnString != "JOBS 0" {
			t.Errorf("Expect response 'JOBS 0' got '"+returnString+"'")
		}
	}

	client.Close()
}
