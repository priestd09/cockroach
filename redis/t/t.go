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
			log.Fatalf("%s: %s\n", addr, err)
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
	commands := strings.Split(strings.TrimSpace(script), "redis> ")
	for i, command := range commands{
		command = strings.TrimSpace(command)
		if command == "" {
			continue
		}
		lines := strings.SplitN(command, "\n", 2)
		sp := strings.Fields(lines[0])
		args := append([]string{"--no-raw", "-h", host, "-p", port}, sp...)
		out, err := exec.Command("redis-cli", args...).CombinedOutput()
		if err != nil {
			fmt.Println(sp, string(out))
			return err
		}
		line := lines[1]
		o := strings.TrimSpace(string(out))
		if o != line {
			return fmt.Errorf("[%d] %s: %s\nwanted: %s", i, lines[0], o, line)
		}
	}
	return nil
}

const script = `
redis> GET mykey
(nil)
redis> SET mykey 10
OK
redis> INCR mykey
(integer) 11
redis> GET mykey
"11"
redis> INCRBY mykey 5
(integer) 16
redis> DECRBY mykey 4
(integer) 12
redis> DECR mykey
(integer) 11
redis> DEL mykey a
(integer) 1
redis> GET mykey
(nil)
redis> SET key1 "Hello"
OK
redis> EXISTS key1
(integer) 1
redis> EXISTS nosuchkey
(integer) 0
redis> SET key2 "World"
OK
redis> EXISTS key1 key2 nosuchkey
(integer) 2
redis> MSET key1 Hello key2 World
OK
redis> GET key1
"Hello"
redis> GET key2
"World"
redis> SET mykey Hello
OK
redis> RENAME mykey myotherkey
OK
redis> GET myotherkey
"Hello"
redis> RPUSH mylist one
(integer) 1
redis> RPUSH mylist two
(integer) 2
redis> RPUSH mylist three
(integer) 3
redis> LRANGE mylist 0 0
1) "one"
redis> LRANGE mylist -3 2
1) "one"
2) "two"
3) "three"
redis> LRANGE mylist -100 100
1) "one"
2) "two"
3) "three"
redis> LRANGE mylist 5 10
(empty list or set)
`
