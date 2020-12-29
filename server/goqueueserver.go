package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/cswilson90/goqueue/internal/data"
	"github.com/cswilson90/goqueue/queue"
)

// A GoJobServer is a server which handles requests to a GoJobQueue.
type GoJobServer struct {
	server net.Listener

	queue *queue.GoJobQueue
}

// NewGoJobServer creates a new GoJobServer which listens on the given hostname and port.
func NewGoJobServer(host string, port string) (*GoJobServer, error) {
	address := host + ":" + port

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	server := &GoJobServer{
		server: listener,
		queue:  queue.NewGoJobQueue(),
	}
	return server, nil
}

// Run runs the GoJobServer and serves requests.
// The function will block forever waiting for requests so should be run as a goroutine.
func (s *GoJobServer) Run() {
	for {
		conn, err := s.server.Accept()
		if err != nil {
			// Connection has been closed
			return
		}

		go s.handleConnection(conn)
	}
}

// Exit stops the server and
func (s *GoJobServer) Exit() {
	s.server.Close()
}

// handleConnection handles a single connection to a client.
func (s *GoJobServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		cmdReader := bufio.NewReader(conn)

		cmdString, err := data.ParseCommand(cmdReader)
		if err != nil {
			if err != io.EOF {
				log.Println("Error: " + err.Error())
			}
			return
		}

		switch cmdString {
		case "ADD":
			s.handleAdd(conn, cmdReader)
		case "CONNECT":
			conn.Write(data.PackString("OK"))
		case "DELETE":
			s.handleDelete(conn, cmdReader)
		case "RESERVE":
			s.handleReserve(conn, cmdReader)
		default:
			errorResponse(conn, "Unknown Command "+cmdString)
		}
	}
}

// handleAdd handles an Add command from the client.
func (s *GoJobServer) handleAdd(conn net.Conn, cmdReader *bufio.Reader) {
	// ADD<\0><queue><priority><ttp><data>
	queueName, err := data.ParseString(cmdReader)
	if err != nil {
		errorResponse(conn, "Malfromed ADD command: failed to parse queue name")
		return
	}

	priority, err := data.ParseUint32(cmdReader)
	if err != nil {
		errorResponse(conn, "Malformed ADD command: failed to parse priority")
		return
	}

	ttp, err := data.ParseUint32(cmdReader)
	if err != nil {
		errorResponse(conn, "Malformed ADD command: failed to parse ttp")
		return
	}

	jobData, err := data.ParseJobData(cmdReader)
	if err != nil {
		errorResponse(conn, "Malformed ADD command: failed to parse job data")
		return
	}

	jobObject := &queue.GoJobData{
		Data:     jobData,
		Priority: priority,
		Queue:    queueName,
		Timeout:  ttp,
	}

	err = s.queue.AddJob(jobObject)
	if err != nil {
		log.Println("Error: " + err.Error())
		errorResponse(conn, fmt.Sprintf("Error adding new job to queue %v", queueName))
		return
	}

	conn.Write(append(data.PackString("ADDED"), data.PackUint64(jobObject.Id)...))
}

// handleDelete handles an Add command from the client.
func (s *GoJobServer) handleDelete(conn net.Conn, cmdReader *bufio.Reader) {
	// DELETE<\0><id>
	jobID, err := data.ParseUint64(cmdReader)
	if err != nil {
		errorResponse(conn, "Malformed DELETE command: failed to parse job ID")
		return
	}

	err = s.queue.DeleteJob(jobID)
	if err != nil {
		errorResponse(conn, fmt.Sprintf("Job %v already deleted", jobID))
		return
	}

	conn.Write(data.PackString("OK"))
}

// handleReserve handles an Add command from the client.
func (s *GoJobServer) handleReserve(conn net.Conn, cmdReader *bufio.Reader) {
	// RESERVE<\0><queue><timeout>
	queueName, err := data.ParseString(cmdReader)
	if err != nil {
		errorResponse(conn, "Malfromed RESERVE command: failed to parse queue name")
		return
	}

	timeout, err := data.ParseUint32(cmdReader)
	if err != nil {
		errorResponse(conn, "Malformed ADD command: failed to parse timeout")
		return
	}

	// Keep trying to reserve a job until we hit a timeout (if there is one)
	start := time.Now()
	for {
		job, ok := s.queue.ReserveJob(queueName)
		if ok {
			packedJob, err := data.PackJob(job)
			if err != nil {
				log.Println("Error: " + err.Error())
				errorResponse(conn, "Failed to reserve job: internal error")
				return
			}
			conn.Write(append(data.PackString("RESERVED"), packedJob...))
			return
		}

		if timeout != 0 {
			elapsed := time.Now().Sub(start)
			if elapsed.Seconds() >= float64(timeout) {
				conn.Write(data.PackString("TIMEOUT"))
				return
			}
		}
	}
}

// errorResponse writes an error response back to the client.
func errorResponse(conn net.Conn, response string) {
	conn.Write(append(data.PackString("ERROR"), data.PackString(response)...))
}
