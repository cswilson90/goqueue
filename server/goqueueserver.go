package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/cswilson90/goqueue/queue"
)

// A GoJobServer is a server which handles requests to a GoJobQueue.
type GoJobServer struct {
	server net.Listener

	queue *queue.GoJobQueue
}

// NewGoJobServer creates a new GoJobServer which listens on the given hostname and port.
func NewGoJobServer(host string, port string) (*GoJobServer, error) {
	address := host+":"+port

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	server := &GoJobServer{
		server: listener,
		queue: queue.NewGoJobQueue(),
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

		cmdString, err := parseCommand(cmdReader)
		if err != nil {
			if err != io.EOF {
				log.Println("Error: "+err.Error())
			}
			return
		}

		switch cmdString {
		case "ADD":
			s.handleAdd(conn, cmdReader)
		case "CONNECT":
			conn.Write(packString("OK"))
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
	queueName, err := parseString(cmdReader)
	if err != nil {
		errorResponse(conn, "Malfromed ADD command: failed to parse queue name");
		return
	}

	priority, err := parseUint32(cmdReader)
	if err != nil {
		errorResponse(conn, "Malformed ADD command: failed to parse priority")
		return
	}

	ttp, err := parseUint32(cmdReader)
	if err != nil {
		errorResponse(conn, "Malformed ADD command: failed to parse ttp")
		return
	}

	jobData, err := parseJobData(cmdReader)
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
		log.Println("Error: "+err.Error())
		errorResponse(conn, fmt.Sprintf("Error adding new job to queue %v", queueName))
		return
	}

	conn.Write(append(packString("ADDED"), packUint64(jobObject.Id)...))
}

// handleDelete handles an Add command from the client.
func (s *GoJobServer) handleDelete(conn net.Conn, cmdReader *bufio.Reader) {
	// DELETE<\0><id>
	jobID, err := parseUint64(cmdReader)
	if err != nil {
		errorResponse(conn, "Malformed DELETE command: failed to parse job ID")
		return
	}

	err = s.queue.DeleteJob(jobID)
	if err != nil {
		errorResponse(conn, fmt.Sprintf("Job %v already deleted", jobID))
		return
	}

	conn.Write(packString("OK"))
}

// handleReserve handles an Add command from the client.
func (s *GoJobServer) handleReserve(conn net.Conn, cmdReader *bufio.Reader) {
	//TODO implement
	conn.Write(packString("OK"))
}

// errorResponse writes an error response back to the client.
func errorResponse(conn net.Conn, response string) {
	conn.Write(append(packString("ERROR"), packString(response)...))
}
