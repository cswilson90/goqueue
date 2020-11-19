package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"regexp"
)

var isUpperCaseString = regexp.MustCompile(`^[A-Z]+$`).MatchString
var isLetterString = regexp.MustCompile(`^[a-zA-Z]+$`).MatchString

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

// parseString parses a null terminated string from the client which can only contain letters.
// Returns an error if a string cannot be parsed or contains a character which isn't a letter.
func parseLetterString(cmdReader *bufio.Reader) (string, error) {
	return parseStringAndValidate(cmdReader, isLetterString)
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


