package main

import (
	"log"

	"github.com/cswilson90/goqueue/internal/server"
)

func main() {
	server, err := server.NewGoJobServer("localhost", "11223")
	if err != nil {
		log.Fatal("Failed to create server: " + err.Error())
	}

	server.Run()
}
