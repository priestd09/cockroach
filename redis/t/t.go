package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"text/tabwriter"
	"time"

	"github.com/garyburd/redigo/redis"
)

type benchmark struct {
	name string
	f    func(redis.Conn) error
}

var (
	flagBench = flag.Bool("bench", false, "Benchmark")
	addrs     = [][]string{
		[]string{"redis", "127.0.0.1", "6379"},
		[]string{"crdb", "127.0.1.1", "16379"},
	}
	benchmarks = []benchmark{
		{
			"GET", func(c redis.Conn) error {
				_, err := c.Do("GET", "a")
				return err
			},
		},
		{
			"SET", func(c redis.Conn) error {
				_, err := c.Do("SET", "a", "1")
				return err
			},
		},
		{
			"INCR", func(c redis.Conn) error {
				_, err := c.Do("INCR", "a")
				return err
			},
		},
		{
			"SET,INCR,GET,DEL", func(c redis.Conn) error {
				if _, err := c.Do("SET", "a", 5); err != nil {
					return err
				}
				if _, err := c.Do("INCRBY", "a", 2); err != nil {
					return err
				}
				if res, err := redis.String(c.Do("GET", "a")); err != nil {
					return err
				} else if res != "7" {
					return fmt.Errorf("expected 7")
				}
				if _, err := c.Do("DEL", "a"); err != nil {
					return err
				}
				return nil
			},
		},
	}
)

func main() {
	flag.Parse()
	cmd := exec.Command("../cockroach", "start", "--dev")
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	time.Sleep(time.Second)
	if !*flagBench {
		for _, addr := range addrs {
			if err := test(addr[1], addr[2]); err != nil {
				io.Copy(os.Stdout, &buf)
				log.Fatalf("%s: %s\n", addr, err)
			}
		}
	} else {
		var names [2]string
		for i, addr := range addrs {
			c, err := redis.Dial("tcp", fmt.Sprintf("%s:%s", addr[1], addr[2]))
			if err != nil {
				log.Fatalf("%s: %s", addr, err)
			}
			var buf bytes.Buffer
			for _, b := range benchmarks {
				br, err := bench(c, b.f)
				if err != nil {
					log.Fatalf("%s: %s", addr, err)
				}
				fmt.Fprintf(&buf, "Benchmark%s %s\n", b.name, br)
			}
			c.Close()
			f, err := ioutil.TempFile("", addr[0])
			if err != nil {
				log.Fatal(err)
			}
			defer func(name string) {
				os.Remove(name)
			}(f.Name())
			if _, err = f.Write(buf.Bytes()); err != nil {
				log.Fatal(err)
			}
			names[i] = f.Name()
			f.Close()
		}
		b, err := exec.Command("benchcmp", names[0], names[1]).CombinedOutput()
		out := string(b)
		if err != nil {
			fmt.Println(out)
			log.Fatal(err)
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "benchmark\tredis ns/op\tcockroach ns/op\tdelta")
		for _, line := range strings.Split(out, "\n")[1:] {
			f := strings.Fields(line)
			if len(f) == 0 {
				continue
			}
			f[0] = strings.TrimPrefix(f[0], "Benchmark")
			fmt.Fprintln(w, strings.Join(f, "\t"))
		}
		w.Flush()
	}
	if err := cmd.Process.Kill(); err != nil {
		log.Fatal(err)
	}
}

func bench(c redis.Conn, f func(redis.Conn) error) (*testing.BenchmarkResult, error) {
	var err error
	br := testing.Benchmark(func(b *testing.B) {
		if _, err = c.Do("flushall"); err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			err = f(c)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	return &br, err
}

func test(host, port string) error {
	commands := strings.Split(strings.TrimSpace(script), "redis> ")
	for i, command := range commands {
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
redis> FLUSHALL
OK
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
redis> SET key1 hi
OK
redis> FLUSHALL
OK
redis> GET key1
(nil)
`
