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
	"testing"

	"github.com/garyburd/redigo/redis"

	"github.com/cockroachdb/cockroach/server"
)

func benchmarkCockroach(b *testing.B, f func(redis.Conn) error) {
	ctx := server.NewTestContext()
	ctx.Insecure = true
	s := &server.TestServer{Ctx: ctx}
	if err := s.Start(); err != nil {
		b.Fatal(err)
	}
	defer s.Stop()

	runBenchmark(b, s.RedisAddr(), f)
}

func benchmarkRedis(b *testing.B, f func(redis.Conn) error) {
	if *redisAddr == "" {
		b.SkipNow()
	}

	runBenchmark(b, *redisAddr, f)
}

func runBenchmark(b *testing.B, addr string, f func(redis.Conn) error) {
	c, err := redis.Dial("tcp", addr)
	if err != nil {
		b.Fatal(err)
	}
	if _, err = c.Do("FLUSHALL"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := f(c)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func runBenchmarkGet(c redis.Conn) error {
	_, err := c.Do("GET", "a")
	return err
}

func BenchmarkGet_Redis(b *testing.B) {
	benchmarkRedis(b, runBenchmarkGet)
}

func BenchmarkGet_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkGet)
}

func runBenchmarkSet(c redis.Conn) error {
	_, err := c.Do("SET", "a", "1")
	return err
}

func BenchmarkSet_Redis(b *testing.B) {
	benchmarkRedis(b, runBenchmarkSet)
}

func BenchmarkSet_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkSet)
}

func runBenchmarkIncr(c redis.Conn) error {
	_, err := c.Do("INCR", "a")
	return err
}

func BenchmarkIncr_Redis(b *testing.B) {
	benchmarkRedis(b, runBenchmarkIncr)
}

func BenchmarkIncr_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkIncr)
}

func runBenchmarkLpush(c redis.Conn) error {
	for i := 0; i < 100; i++ {
		if _, err := c.Do("LPUSH", "mylist", i); err != nil {
			return err
		}
	}
	for i := 0; i < 10; i++ {
		if _, err := c.Do("LPOP", "mylist"); err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkLpush_Redis(b *testing.B) {
	benchmarkRedis(b, runBenchmarkLpush)
}

func BenchmarkLpush_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkLpush)
}

func runBenchmarkSadd(c redis.Conn) error {
	for i := 0; i < 100; i++ {
		if _, err := c.Do("SADD", "myset", i%20); err != nil {
			return err
		}
	}
	for i := 0; i < 10; i++ {
		if _, err := c.Do("SREM", "myset", i * 4); err != nil {
			return err
		}
	}
	return nil
}

func BenchmarkSadd_Redis(b *testing.B) {
	benchmarkRedis(b, runBenchmarkSadd)
}

func BenchmarkSadd_Cockroach(b *testing.B) {
	benchmarkCockroach(b, runBenchmarkSadd)
}
