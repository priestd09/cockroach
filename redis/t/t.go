package main

import (
	"fmt"
	"log"
	"net"
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
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
	for _, addr := range addrs {
		if err := test(addr); err != nil {
			fmt.Printf("%s: %s\n", addr, err)
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
		out, err := exec.Command("redis-cli", args...).Output()
		if err != nil {
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
`
