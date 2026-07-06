package main

import (
	"fmt"
	"io"
	"net"
)

func handleConnection(c net.Conn) {
	buff := make([]byte, 1024)
	for {
		n, err := c.Read(buff)
		//	fmt.Printf("%d %s\n", n, string(buff[:n]))
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Error reading input: ", err.Error())
			break
		}
		result := respParser(string(buff[:n]))
		switch result[0] {
		case "ping":
			handlePing(c)
		case "echo":
			handleEcho(c, result)
		case "set":
			handleSet(c, result)
		case "get":
			handleGet(c, result)
		case "rpush":
			handleRpush(c, result)
		case "lrange":
			handleLrange(c, result)
		case "lpush":
			handleLpush(c, result)
		case "llen":
			handleLlen(c, result)
		case "lpop":
			handleLpop(c, result)
		case "blpop":
			handleBlpop(c, result)
		case "type":
			handleType(c, result)
		case "xadd":
			handleXadd(c, result)
		case "xrange":
			handleXrange(c, result)
		}	
	}

}
