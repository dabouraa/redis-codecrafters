package main

import (
	"fmt"
	"net"
	"os"
	"sync"
)

type StreamEntry struct {
	ID string
	Fields map[string]string
}

var _ = net.Listen
var _ = os.Exit
var varData = make(map[string]string)
var listData = make(map[string][]string)
var mu sync.Mutex
var cond *sync.Cond = sync.NewCond(&mu)
var status bool = false
var streamData = make(map[string][]StreamEntry)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}
