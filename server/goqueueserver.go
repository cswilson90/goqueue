package server

import (
	"bufio"
	"net"
	"strconv"

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
	for {
		buffer, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			// Client has closed connection
			conn.Close()
			return
		}

		cmdString := string(buffer[:len(buffer)-1])
		returnMsg := "ERROR"
		if cmdString == "STATS" {
			returnMsg = "JOBS "+strconv.Itoa(s.queue.NumJobs())
		}

		returnMsg += "\n"
		conn.Write([]byte(returnMsg))
	}
}
