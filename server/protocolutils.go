package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"regexp"
)

var isUpperCaseString = regexp.MustCompile(`^[A-Z]+$`).MatchString

// parseString parses a null terminated string from the client.
// Returns an error if a string cannot be parsed
func parseString(cmdReader *bufio.Reader) (string, error) {
	return parseStringAndValidate(cmdReader, nil)
}

// getCommand parses a command string from the client.
// Returns an error if a command string cannot be parsed.
func parseCommand(cmdReader *bufio.Reader) (string, error) {
	return parseStringAndValidate(cmdReader, isUpperCaseString)
}

// parseStringAndValidate parses a null terminated string from the client and validates it.
func parseStringAndValidate(cmdReader *bufio.Reader, validate func(string) bool) (string, error) {
	cmdBytes, err := cmdReader.ReadBytes('\x00')
	if err != nil {
		return "", err
	}

	cmdString := string(cmdBytes[:len(cmdBytes)-1])
	if validate != nil && !validate(cmdString) {
		return "", fmt.Errorf("Unexpected string '%v'", cmdString)
	}

	return cmdString, nil
}

// packString converts a string to a byte slice to send back to the client.
func packString(data string) []byte {
	return []byte(data+"\x00")
}

// parseUint64 parses an uint64 from the client.
// Returns an error if a uint64 can't be parsed.
func parseUint64(cmdReader *bufio.Reader) (uint64, error) {
	uintBytes := make([]byte, 8)
	_, err := io.ReadFull(cmdReader, uintBytes)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(uintBytes), nil
}

// packUint64 packs a uint64 into a byte slice to send back to the client.
func packUint64(intData uint64) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, intData)
	return data
}

// parseUint32 parses an uint32 from the client.
// Returns an error if a uint32 can't be parsed.
func parseUint32(cmdReader *bufio.Reader) (uint32, error) {
	uintBytes := make([]byte, 4)
	_, err := io.ReadFull(cmdReader, uintBytes)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(uintBytes), nil
}

// packUint32 packs a uint32 into a byte slice to send back to the client.
func packUint32(intData uint32) []byte {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, intData)
	return data
}

// parseJobData parses the data for a job from the the client.
// Returns an error if the no data could be parsed.
func parseJobData(cmdReader *bufio.Reader) ([]byte, error) {
	dataLength, err := parseUint32(cmdReader)
	if err != nil {
		return nil, err
	}

	data := make([]byte, dataLength)
	_, err = io.ReadFull(cmdReader, data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// packJobData packs job data into a byte array that can be sent to the client.
func packJobData(jobData []byte) ([]byte, error) {
	if len(jobData) > math.MaxUint32 {
		return nil, fmt.Errorf("Can't pack data with length greater than MaxUint32")
	}

	dataLength := packUint32(uint32(len(jobData)))
	return append(dataLength, jobData...), nil
}
