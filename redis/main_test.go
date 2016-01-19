// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Jibson (mjibson@cockroachlabs.com)

package redis_test

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/server"
)

var (
	redisAddr = flag.String("redis-addr", "", "address of redis server; enables test and benchmark if set")
	testdata  = flag.String("d", "testdata/*", "test data glob")
)

func TestCockroach(t *testing.T) {
	ctx := server.NewTestContext()
	ctx.Insecure = true
	s := &server.TestServer{Ctx: ctx}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Stop()
	addr := s.RedisAddr()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	testLogic(t, host, port)
}

func TestRedis(t *testing.T) {
	if *redisAddr == "" {
		t.SkipNow()
	}
	addr := *redisAddr
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	testLogic(t, host, port)
}

func testLogic(t *testing.T, host, port string) {
	globs := []string{*testdata}
	var paths []string
	for _, g := range globs {
		match, err := filepath.Glob(g)
		if err != nil {
			t.Fatal(err)
		}
		paths = append(paths, match...)
	}

	for _, path := range paths {
		tests, err := readTestdata(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, test := range tests {
			runTest(t, host, port, test)
		}
	}
}

func runTest(t *testing.T, host, port string, test logicTest) {
	args := append([]string{"--no-raw", "-h", host, "-p", port}, test.arguments...)
	out, err := exec.Command("redis-cli", args...).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	o := strings.TrimSpace(string(out))
	if o != test.output {
		t.Fatalf("%s:%s: %s: %s: got: %s\nexpected: %s", host, port, test.name, test.arguments, o, test.output)
	}
}

type logicTest struct {
	name      string
	arguments []string
	output    string
}

func readTestdata(path string) ([]logicTest, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var lines string
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if lines != "" {
			lines += "\n"
		}
		lines += line
	}
	tests := []logicTest{
		{
			arguments: []string{"FLUSHALL"},
			output:    "OK",
		},
	}
	for _, group := range strings.Split(lines, "redis>") {
		if group == "" {
			continue
		}
		sp := strings.SplitN(group, "\n", 2)
		if len(sp) != 2 {
			return nil, fmt.Errorf("expected output: %s", group)
		}
		args := arger(sp[0])
		for i, a := range args {
			args[i] = strings.Replace(a, `"`, "", -1)
		}
		tests = append(tests, logicTest{
			name:      path,
			arguments: args,
			output:    strings.TrimSpace(sp[1]),
		})
	}
	return tests, nil
}

// arger converts s to arguments separated by spaces. Can use double quotes to group.
func arger(s string) []string {
	args := strings.Fields(s)
	for i := 0; i < len(args); i++ {
		if strings.HasPrefix(args[i], `"`) && !strings.HasSuffix(args[i], `"`) {
			args[i] += " " + args[i+1]
			args = append(args[:i+1], args[i+2:]...)
			i--
		}
	}
	for i, a := range args {
		args[i] = strings.Replace(a, `"`, "", -1)
	}
	return args
}

func TestArger(t *testing.T) {
	tests := []struct {
		in  string
		out []string
	}{
		{
			`SET key val`,
			[]string{"SET", "key", "val"},
		},
		{
			`SET key "hello world"`,
			[]string{"SET", "key", "hello world"},
		},
	}
	for _, test := range tests {
		args := arger(test.in)
		if len(args) != len(test.out) {
			t.Fatal(test.in)
		}
		for i, a := range args {
			if a != test.out[i] {
				t.Fatal(test.in)
			}
		}
	}
}
