package data

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"regexp"

	"github.com/cswilson90/goqueue/internal/queue"
)

var isUpperCaseString = regexp.MustCompile(`^[A-Z]+$`).MatchString

// ParseString parses a null terminated string from the client.
// Returns an error if a string cannot be parsed
func ParseString(cmdReader *bufio.Reader) (string, error) {
	return ParseStringAndValidate(cmdReader, nil)
}

// ParseCommand parses a command string from the client.
// Returns an error if a command string cannot be parsed.
func ParseCommand(cmdReader *bufio.Reader) (string, error) {
	return ParseStringAndValidate(cmdReader, isUpperCaseString)
}

// ParseStringAndValidate parses a null terminated string from the client and validates it.
func ParseStringAndValidate(cmdReader *bufio.Reader, validate func(string) bool) (string, error) {
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

// PackString converts a string to a byte slice to send back to the client.
func PackString(data string) []byte {
	return []byte(data + "\x00")
}

// ParseUint64 parses an uint64 from the client.
// Returns an error if a uint64 can't be parsed.
func ParseUint64(cmdReader *bufio.Reader) (uint64, error) {
	uintBytes := make([]byte, 8)
	_, err := io.ReadFull(cmdReader, uintBytes)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint64(uintBytes), nil
}

// PackUint64 packs a uint64 into a byte slice to send back to the client.
func PackUint64(intData uint64) []byte {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, intData)
	return data
}

// ParseUint32 parses an uint32 from the client.
// Returns an error if a uint32 can't be parsed.
func ParseUint32(cmdReader *bufio.Reader) (uint32, error) {
	uintBytes := make([]byte, 4)
	_, err := io.ReadFull(cmdReader, uintBytes)
	if err != nil {
		return 0, err
	}

	return binary.LittleEndian.Uint32(uintBytes), nil
}

// PackUint32 packs a uint32 into a byte slice to send back to the client.
func PackUint32(intData uint32) []byte {
	data := make([]byte, 4)
	binary.LittleEndian.PutUint32(data, intData)
	return data
}

// ParseJobData parses the data for a job from the the client.
// Returns an error if the no data could be parsed.
func ParseJobData(cmdReader *bufio.Reader) ([]byte, error) {
	dataLength, err := ParseUint32(cmdReader)
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

// PackJobData packs job data into a byte array that can be sent to the client.
func PackJobData(jobData []byte) ([]byte, error) {
	if len(jobData) > math.MaxUint32 {
		return nil, fmt.Errorf("Job data length greater than MaxUint32")
	}

	dataLength := PackUint32(uint32(len(jobData)))
	return append(dataLength, jobData...), nil
}

// ParseJob parses a job and it's metadata from the client.
func ParseJob(cmdReader *bufio.Reader) (*queue.GoJobData, error) {
	id, err := ParseUint64(cmdReader)
	if err != nil {
		return nil, err
	}

	priority, err := ParseUint32(cmdReader)
	if err != nil {
		return nil, err
	}

	ttp, err := ParseUint32(cmdReader)
	if err != nil {
		return nil, err
	}

	status, err := ParseString(cmdReader)
	if err != nil {
		return nil, err
	}

	jobData, err := ParseJobData(cmdReader)
	if err != nil {
		return nil, err
	}

	return &queue.GoJobData{
		Id:       id,
		Priority: priority,
		Timeout:  ttp,
		Status:   status,
		Data:     jobData,
	}, nil
}

// PackJob packs all the data and metadata for a job into a byte array to be sent to the client.
func PackJob(job *queue.GoJobData) ([]byte, error) {
	allData := make([]byte, 0)
	// Job ID
	allData = append(allData, PackUint64(job.Id)...)
	// Priority
	allData = append(allData, PackUint32(job.Priority)...)
	// TTP
	allData = append(allData, PackUint32(job.Timeout)...)
	// Status
	allData = append(allData, PackString(job.Status)...)

	// Job Data
	jobData, err := PackJobData(job.Data)
	if err != nil {
		return nil, err
	}
	allData = append(allData, jobData...)

	return allData, nil
}
