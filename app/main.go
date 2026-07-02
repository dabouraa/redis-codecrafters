package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"reflect"
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
			c.Write([]byte("+PONG\r\n"))
		case "echo":
			c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(result[1]), result[1])))
		case "set":
			mu.Lock()
			varData[result[1]] = result[2]
			mu.Unlock()
			c.Write([]byte("+OK\r\n"))
			if len(result) > 3 {
				if result[3] == "ex" {
					ex, err := strconv.ParseInt(string(result[4]), 10, 64)
					if err != nil {
						fmt.Println("Error parsing int")
					}
					time.AfterFunc(time.Duration(ex)*time.Second, func() {
						mu.Lock()
						delete(varData, result[1])
						mu.Unlock()
					})
				}
				if result[3] == "px" {
					ex, err := strconv.ParseInt(result[4], 10, 64)
					if err != nil {
						fmt.Println("Error parsing int")
					}
					time.AfterFunc(time.Duration(ex)*time.Millisecond, func() {
						mu.Lock()
						delete(varData, result[1])
						mu.Unlock()
					})
				}
			}
		case "get":
			mu.Lock()
			res, ok := varData[result[1]]
			mu.Unlock()
			if !ok {
				c.Write([]byte("$-1\r\n"))
			} else {
				c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(res), res)))
			}
		case "rpush":
			mu.Lock()
			for i := 2; i < len(result); i++ {
				listData[result[1]] = append(listData[result[1]], result[i])
			}
			cond.Signal()
			mu.Unlock()
			c.Write([]byte(fmt.Sprintf(":%d\r\n", len(listData[result[1]]))))
		case "lrange":
			lower, _ := strconv.ParseInt(result[2], 10, 64)
			upper, _ := strconv.ParseInt(result[3], 10, 64)
			if int(upper) > len(listData[result[1]]) {
				upper = int64(len(listData[result[1]])) - 1
			} else if int(((upper * -1) - 1)) >= len(listData[result[1]]) {
				upper = 0
			}
			if int(lower) > len(listData[result[1]]) {
				c.Write([]byte("*0\r\n"))
				continue
			} else if int(((lower * -1) - 1)) >= len(listData[result[1]]) {
				lower = 0
			}
			if lower < 0 {
				lower = int64(len(listData[result[1]])) + lower
			}
			if upper < 0 {
				upper = int64(len(listData[result[1]])) + upper
			}
			if _, ok := listData[result[1]]; !ok {
				c.Write([]byte("*0\r\n"))
				continue
			}
			if lower > upper {
				c.Write([]byte("*0\r\n"))
				continue
			}
			amount := (upper - lower) + 1
			res := fmt.Sprintf("*%d\r\n", amount)
			for i := lower; i <= upper; i++ {
				res += fmt.Sprintf("$%d\r\n%s\r\n", len(listData[result[1]][i]), listData[result[1]][i])
			}
			c.Write([]byte(res))
		case "lpush":
			mu.Lock()
			for i := 2; i < len(result); i++ {
				listData[result[1]] = append([]string{result[i]}, listData[result[1]]...)
			}
			mu.Unlock()
			c.Write([]byte(fmt.Sprintf(":%d\r\n", len(listData[result[1]]))))
		case "llen":
			c.Write([]byte(fmt.Sprintf(":%d\r\n", len(listData[result[1]]))))
		case "lpop":
			if _, ok := listData[result[1]]; !ok {
				c.Write([]byte("$-1\r\n"))
			} else if len(listData[result[1]]) == 0 {
				c.Write([]byte("$-1\r\n"))
			}
			if len(result) >= 3 {
				amount, _ := strconv.ParseInt(result[2], 10, 64)
				res := fmt.Sprintf("*%d\r\n", amount)
				for i := 0; i < int(amount); i++ {
					del := listData[result[1]][0]
					listData[result[1]] = slices.Delete(listData[result[1]], 0, 1)
					res += fmt.Sprintf("$%d\r\n%s\r\n", len(del), del)
				}
				c.Write([]byte(res))
			} else {
				del := listData[result[1]][0]
				listData[result[1]] = slices.Delete(listData[result[1]], 0, 1)
				c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(del), del)))
			}
		case "blpop":
			sec, _ := strconv.ParseFloat(result[2], 64)
			if sec == 0{	
				cond.L.Lock()
				for {
					if len(listData[result[1]]) == 0 {
						cond.Wait()
					} else {
						break
					}
				}
				res := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(result[1]), result[1])
				del := listData[result[1]][0]
				
				listData[result[1]] = slices.Delete(listData[result[1]], 0, 1)
				cond.L.Unlock()
				res += fmt.Sprintf("$%d\r\n%s\r\n", len(del), del)
				c.Write([]byte(res))
			} else {
				for {
					timeout := time.After(time.Duration(sec) * time.Second)
					if len(listData[result[1]]) == 0 {
						select {
							case <-timeout:
								time.Sleep(500 * time.Millisecond)
								if len(listData[result[1]]) == 0 {
									c.Write([]byte("*-1\r\n"))
								} else {
									break
								}
						}
						} else {
							res := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(result[1]), result[1])
							del := listData[result[1]][0]
							cond.L.Lock()
							listData[result[1]] = slices.Delete(listData[result[1]], 0, 1)
							cond.L.Unlock()
							res += fmt.Sprintf("$%d\r\n%s\r\n", len(del), del)
							c.Write([]byte(res))
						}
						
					}
				}
			case "type": 
				if _, ok := streamData[result[1]]; ok {
					c.Write([]byte("+stream\r\n"))
				}else { 
					if _, ok := varData[result[1]]; !ok     {
						c.Write([]byte("+none\r\n"))
					} else { 
					listType := reflect.TypeOf(varData[result[1]])
					t := listType.Kind()
					c.Write([]byte(fmt.Sprintf("+%s\r\n", t)))
					}
				}
			case "xadd":
				count := 0
				if result[2] == "0-0" { 
					count += 1
					c.Write([]byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"))
				
				} else if _, ok := streamData[result[1]]; !ok { // does not already exist
					if count == 0 {
						resp := streamAppend(result)
						c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(resp.ID), resp.ID)))
					}
				} else {
					lastItem := streamData[result[1]][len(streamData[result[1]]) - 1]
					id := result[2]
					
					digId, _ := strconv.ParseInt(string(id[len(id) - 1]), 10, 64)
					lastDigId, _ := strconv.ParseInt(string(lastItem.ID[len(lastItem.ID) - 1]), 10, 64)
					
					intMs, _ := strconv.ParseInt(id[:len(id) - 2], 10, 64)
					intLastMs, _ := strconv.ParseInt(lastItem.ID[:len(lastItem.ID) - 2], 10, 64)
					
					if intMs < intLastMs { 
						c.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
					} else if intMs == intLastMs {
						if digId <= lastDigId {
							c.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
						} else {
							resp := streamAppend(result)
							c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(resp.ID), resp.ID)))
						}
					} else {
						resp := streamAppend(result)
						c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(resp.ID), resp.ID)))
					}
				}
			}

	}
}

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

func streamAppend(result []string) (resp StreamEntry) {
	fields := make(map[string]string)
	cond.L.Lock()
	for i := 3; i <= len(result) -2; i += 2 {
		fields[result[i]] = result[i+1]
	}
	cond.L.Unlock()
	entry := StreamEntry {
		ID: result[2],
		Fields: fields,
	}
	cond.L.Lock()
	streamData[result[1]] = append(streamData[result[1]], entry)
	cond.L.Unlock()
	resp = streamData[result[1]][len(streamData[result[1]]) - 1]
	return resp
	
}