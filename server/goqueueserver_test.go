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
		client.Write([]byte("CONNECT\x00"))

		buffer, err := bufio.NewReader(client).ReadBytes('\x00')
		if err != nil {
			t.Errorf("Failed to get CONNECT response from server")
		}

		returnString := string(buffer[:len(buffer)-1])
		if returnString != "OK" {
			t.Errorf("Expected response 'OK' got '"+returnString+"'")
		}
	}

	client.Close()
}
