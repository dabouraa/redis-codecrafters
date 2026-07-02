package main

import (
	"bufio"
	"fmt"
	"strings"
	"strconv"
	"slices"
	"io"
)

func respParser(buff string) []string {
	reader := bufio.NewReader(strings.NewReader(buff))
	s, _ := reader.ReadByte()
	if s != '*' {
		fmt.Println("Error parsing command")
	}
	// b, _ := reader.ReadByte()
	b1, _ := reader.ReadString('\n')
	b := strings.ReplaceAll(b1, "\r\n", "")
	sliceSize, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		fmt.Println("Error parsing int: ", err.Error())
	}
	input := []string{}

	for i := 0; i < int(sliceSize); i++ {
		p, _ := reader.ReadByte()
		if p != '$' {
			fmt.Println("Error parsing command")
		}
		sizeTemp, _ := reader.ReadString('\n')
		size := strings.ReplaceAll(sizeTemp, "\r\n", "")
		strSize, err := strconv.ParseInt(string(size), 10, 64)
		if err != nil {
			fmt.Println("Error parsing int: ", err.Error())
		}
		// reader.ReadByte()
		// reader.ReadByte()
		resp := make([]byte, strSize)
		_, err = io.ReadFull(reader, resp)
		if err != nil {
			fmt.Println("Error reading input")
		}
		input = append(input, string(resp))
		reader.ReadByte()
		reader.ReadByte()
	}
	input[0] = strings.ToLower(input[0])
	if slices.Contains(input, "EX") {
		ind := slices.Index(input, "EX")
		input[ind] = strings.ToLower(input[ind])
	}
	if slices.Contains(input, "PX") {
		ind := slices.Index(input, "PX")
		input[ind] = strings.ToLower(input[ind])
	}
	return input
}