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

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestMain(m *testing.M) {
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	os.Exit(m.Run())
}

var (
	redisAddr = flag.String("redis-addr", "", "address of redis server; enables test and benchmark if set")
	testdata  = flag.String("d", "testdata/*", "test data glob")
)

func startServer(t testing.TB) (string, func()) {
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Insecure: true,
	})
	db := s.KVClient().(*client.DB)
	go serve(ln, db)

	cancel := func() {
		ln.Close()
		s.Stopper().Stop()
	}
	return ln.Addr().String(), cancel
}

func TestCockroach(t *testing.T) {
	addr, cancel := startServer(t)
	defer cancel()
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
		t.Run(path, func(t *testing.T) {
			t.Log(path)
			tests, err := readTestdata(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, test := range tests {
				runTest(t, host, port, test)
			}
		})
	}
}

func runTest(t *testing.T, host, port string, test logicTest) {
	args := append([]string{"--no-raw", "-h", host, "-p", port}, test.arguments...)
	out, err := exec.Command("redis-cli", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %s", err, out)
	}
	o := strings.TrimSpace(string(out))
	if len(test.arguments) > 0 && strings.ToLower(test.arguments[0]) == "smembers" {
		o = sortSet(o)
		test.output = sortSet(test.output)
	}
	if o != test.output {
		t.Fatalf("%s:%s: %s: %s: got: %s\nexpected: %s", host, port, test.name, test.arguments, o, test.output)
	} else {
		t.Logf("%s: %s", test.arguments, o)
	}
}

func sortSet(s string) string {
	var r []string
	for _, l := range strings.Split(s, "\n") {
		sp := strings.Fields(l)
		if len(sp) != 2 {
			continue
		}
		r = append(r, sp[1])
	}
	sort.Strings(r)
	for i, v := range r {
		r[i] = fmt.Sprintf("%d) %s", i+1, v)
	}
	return strings.Join(r, "\n")
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
