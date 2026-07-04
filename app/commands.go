package main

import (
	"fmt"
	"net"
	"reflect"
	"slices"
	"strconv"
	"time"
	"strings"
)

func handlePing(c net.Conn) {
	c.Write([]byte("+PONG\r\n"))
}

func handleEcho(c net.Conn, result []string) {
	c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(result[1]), result[1])))
}

func handleSet(c net.Conn, result []string) {
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
}

func handleGet(c net.Conn, result []string) {
	mu.Lock()
	res, ok := varData[result[1]]
	mu.Unlock()
	if !ok {
		c.Write([]byte("$-1\r\n"))
	} else {
		c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(res), res)))
	}
}

func handleRpush(c net.Conn, result []string) {
	mu.Lock()
	for i := 2; i < len(result); i++ {
		listData[result[1]] = append(listData[result[1]], result[i])
	}
	cond.Signal()
	mu.Unlock()
	c.Write([]byte(fmt.Sprintf(":%d\r\n", len(listData[result[1]]))))
}

func handleLrange(c net.Conn, result []string) {
	lower, _ := strconv.ParseInt(result[2], 10, 64)
	upper, _ := strconv.ParseInt(result[3], 10, 64)
	if int(upper) > len(listData[result[1]]) {
		upper = int64(len(listData[result[1]])) - 1
	} else if int(((upper * -1) - 1)) >= len(listData[result[1]]) {
		upper = 0
	}
	if int(lower) > len(listData[result[1]]) {
		c.Write([]byte("*0\r\n"))
		return
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
		return
	}
	if lower > upper {
		c.Write([]byte("*0\r\n"))
		return
	}
	amount := (upper - lower) + 1
	res := fmt.Sprintf("*%d\r\n", amount)
	for i := lower; i <= upper; i++ {
		res += fmt.Sprintf("$%d\r\n%s\r\n", len(listData[result[1]][i]), listData[result[1]][i])
	}
	c.Write([]byte(res))
}

func handleLpush(c net.Conn, result []string) {
	mu.Lock()
	for i := 2; i < len(result); i++ {
		listData[result[1]] = append([]string{result[i]}, listData[result[1]]...)
	}
	mu.Unlock()
	c.Write([]byte(fmt.Sprintf(":%d\r\n", len(listData[result[1]]))))
}
func handleLlen(c net.Conn, result []string) {
	c.Write([]byte(fmt.Sprintf(":%d\r\n", len(listData[result[1]]))))
}
func handleLpop(c net.Conn, result []string) {
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
}

func handleBlpop(c net.Conn, result []string) {
	sec, _ := strconv.ParseFloat(result[2], 64)
	if sec == 0 {
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
}

func handleType(c net.Conn, result []string) {
	if _, ok := streamData[result[1]]; ok {
		c.Write([]byte("+stream\r\n"))
	} else {
		if _, ok := varData[result[1]]; !ok {
			c.Write([]byte("+none\r\n"))
		} else {
			listType := reflect.TypeOf(varData[result[1]])
			t := listType.Kind()
			c.Write([]byte(fmt.Sprintf("+%s\r\n", t)))
		}
	}
}

func handleXadd(c net.Conn, result []string) {
	if result[2] == "*" {
		result[2] = handleAutoId(result)
	}
	count := 0
	if _, ok := streamData[result[1]]; ok {
		if strings.Contains(result[2], "*") { 
			result[2] = handleAutoSequence(result, true)
		} 
	} else {
		if strings.Contains(result[2], "*") {
			result[2] = handleAutoSequence(result, false)
		}
	}
	if result[2] == "0-0" {
		count += 1
		c.Write([]byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"))

	} else if _, ok := streamData[result[1]]; !ok { // does not already exist
		if count == 0 {
			resp := streamAppend(result)
			c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(resp.ID), resp.ID)))
		}
	} else {
		lastItem := streamData[result[1]][len(streamData[result[1]])-1]
		id := result[2]

		digId, _ := strconv.ParseInt(string(id[len(id)-1]), 10, 64)
		lastDigId, _ := strconv.ParseInt(string(lastItem.ID[len(lastItem.ID)-1]), 10, 64)

		intMs, _ := strconv.ParseInt(id[:len(id)-2], 10, 64)
		intLastMs, _ := strconv.ParseInt(lastItem.ID[:len(lastItem.ID)-2], 10, 64)

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

func streamAppend(result []string) (resp StreamEntry) {
	fields := make(map[string]string)
	cond.L.Lock()
	for i := 3; i <= len(result)-2; i += 2 {
		fields[result[i]] = result[i+1]
	}
	cond.L.Unlock()
	entry := StreamEntry{
		ID:     result[2],
		Fields: fields,
	}
	cond.L.Lock()
	streamData[result[1]] = append(streamData[result[1]], entry)
	cond.L.Unlock()
	resp = streamData[result[1]][len(streamData[result[1]])-1]
	return resp

}

func handleAutoSequence(result []string, ex bool) string{ // generates the sequnce number only
	// strings count to see if theres more than one asterisk
	// if so use time.Now().UnixMilli() to generate left side
	seq := int64(0)
	splitId := strings.Split(result[2], "-")
	if ex == true { 
		lastItem := streamData[result[1]][len(streamData[result[1]]) - 1]
		lastMs := strings.Split(lastItem.ID, "-")
		
		if strings.Contains(lastMs[0], splitId[0]) {
			seq, _ = strconv.ParseInt(splitId[1], 10, 64)
			seq += 1
		}
	} else { 
		if splitId[0] == "0" && seq == 0 {
			seq = 1
		} else {
			seq = 0
		}
	}
	result[2] = strings.Replace(result[2], "*", strconv.Itoa(int(seq)), 1)
	return result[2]
}

func handleAutoId(result []string) string { // generates the entire ID number
	seq := ""
	ms := time.Now().UnixMilli()
	strMs := strconv.Itoa(int(ms))
	result[2] = fmt.Sprintf("%s-*", string(strMs))
	exist := slices.ContainsFunc(streamData[result[1]], func(e StreamEntry) bool {
		trimId := strings.Split(e.ID, "-")
		return trimId[0] == string(strMs)
	})
	if !exist {
		seq = handleAutoSequence(result, false)
	} else {
		seq = handleAutoSequence(result, true)
	}
	result[2] = fmt.Sprintf("%s-%s", string(strMs), seq)
	resp := fmt.Sprintf("%s", seq)
	return resp
}