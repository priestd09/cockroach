package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"time"
)

var (
	addrs = []string{
		"127.0.0.1:6379",
		"127.0.1.1:16379",
	}
)

func main() {
	if _, err := exec.Command("redis-cli", "flushall").Output(); err != nil {
		log.Fatal(err)
	}
	cmd := exec.Command("../cockroach", "start", "--dev")
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
	for _, addr := range addrs {
		if err := test(addr); err != nil {
			io.Copy(os.Stdout, &buf)
			log.Fatal("%s: %s\n", addr, err)
		}
	}
	if err := cmd.Process.Kill(); err != nil {
		log.Fatal(err)
	}
}

func test(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	lines := strings.Split(strings.TrimSpace(script), "\n")
	for i := 0; i < len(lines); i += 2 {
		sp := strings.Fields(lines[i])
		args := append([]string{"--no-raw", "-h", host, "-p", port}, sp...)
		out, err := exec.Command("redis-cli", args...).CombinedOutput()
		if err != nil {
			fmt.Println(sp, string(out))
			return err
		}
		line := lines[i+1]
		o := strings.TrimSpace(string(out))
		if o != line {
			return fmt.Errorf("[%d] %s: %s\nwanted: %s", i, lines[i], o, line)
		}
	}
	return nil
}

const script = `
GET mykey
(nil)
SET mykey 10
OK
INCR mykey
(integer) 11
GET mykey
"11"
INCRBY mykey 5
(integer) 16
DECRBY mykey 4
(integer) 12
DECR mykey
(integer) 11
DEL mykey a
(integer) 1
GET mykey
(nil)
SET key1 "Hello"
OK
EXISTS key1
(integer) 1
EXISTS nosuchkey
(integer) 0
SET key2 "World"
OK
EXISTS key1 key2 nosuchkey
(integer) 2
MSET key1 Hello key2 World
OK
GET key1
"Hello"
GET key2
"World"
`
