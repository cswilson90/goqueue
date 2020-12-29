package server

import (
	"bufio"
	"net"
	"testing"

	"github.com/cswilson90/goqueue/internal/data"
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
func createClient(t *testing.T) net.Conn {
	conn, err := net.Dial(connType, connHost+":"+connPort)
	if err != nil {
		t.Errorf("Failed to create test client")
	}
	return conn
}

func TestConnect(t *testing.T) {
	server := createServer(t)
	go server.Run()
	defer server.Exit()

	client := createClient(t)
	defer client.Close()

	// Test repeat requests
	for i := 0; i < 2; i++ {
		client.Write([]byte("CONNECT\x00"))

		cmdReader := bufio.NewReader(client)
		returnString, err := data.ParseCommand(cmdReader)
		if err != nil {
			t.Errorf("Failed to get CONNECT response from server")
		}

		if returnString != "OK" {
			t.Errorf("Expected response 'OK' got '" + returnString + "'")
		}
	}
}

func TestAddReserveAndDelete(t *testing.T) {
	server := createServer(t)
	go server.Run()
	defer server.Exit()

	client := createClient(t)
	defer client.Close()

	// Add a job
	request := make([]byte, 0)
	request = append(request, data.PackString("ADD")...)
	request = append(request, data.PackString("queue1")...)
	request = append(request, data.PackUint32(1)...)
	request = append(request, data.PackUint32(60)...)
	packedJobData, err := data.PackJobData([]byte{'1', '2', '3'})
	if err != nil {
		t.Error(err.Error())
	}
	request = append(request, packedJobData...)
	client.Write(request)

	cmdReader := bufio.NewReader(client)
	response, err := data.ParseCommand(cmdReader)
	if err != nil {
		t.Errorf("Failed to get response when adding a job: " + err.Error())
	}
	if response != "ADDED" {
		t.Errorf("Expected response 'ADDED' got '" + response + "'")
	}

	jobID, err := data.ParseUint64(cmdReader)
	if err != nil {
		t.Errorf("Failed to get job ID of added job")
	}
	if jobID != 1 {
		t.Errorf("Expected added job to have ID 1 got %v", jobID)
	}

	// Reserve the job
	request = make([]byte, 0)
	request = append(request, data.PackString("RESERVE")...)
	request = append(request, data.PackString("queue1")...)
	request = append(request, data.PackUint32(0)...)
	client.Write(request)

	response, err = data.ParseCommand(cmdReader)
	if err != nil {
		t.Errorf("Failed to get response when reserving job: " + err.Error())
	}
	if response != "RESERVED" {
		t.Errorf("Expected response RESERVED, got %v", response)
	}
	job, err := data.ParseJob(cmdReader)
	if err != nil {
		t.Errorf("Error parsing reserved job: " + err.Error())
	}
	if job.Id != 1 {
		t.Errorf("Expected reserved job to have ID 1 got %v", job.Id)
	}

	// Delete the job
	request = make([]byte, 0)
	request = append(request, data.PackString("DELETE")...)
	request = append(request, data.PackUint64(jobID)...)
	client.Write(request)

	returnString, err := data.ParseCommand(cmdReader)
	if err != nil {
		t.Errorf("Failed to get DELETE response from server: " + err.Error())
	}

	if returnString != "OK" {
		t.Errorf("Expected response 'OK' got '" + returnString + "'")
	}
}
