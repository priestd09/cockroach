// Copyright 2015 The Cockroach Authors.
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

package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/redis/driver"
)

// An Executor executes Redis statements.
type Executor struct {
	db client.DB
}

// newExecutor creates an Executor and registers a callback on the
// system config.
func newExecutor(db client.DB) *Executor {
	exec := &Executor{
		db: db,
	}
	return exec
}

const errWrongNumberOfArguments = "wrong number of arguments for '%s' command"

// Execute the command(s) in the given request and return a response.
// On error, the returned integer is an HTTP error code.
func (e *Executor) Execute(c driver.Command) (driver.Response, int, error) {
	var d driver.Datum
	var err error
	incrby := func(key string, value int64) {
		var val client.KeyValue
		val, err = e.db.Get(key)
		if err != nil {
			return
		}
		var i int64
		if !val.Exists() {
			i = 0
		} else {
			i, err = strconv.ParseInt(string(val.ValueBytes()), 10, 64)
			if err != nil {
				err = errors.New("value is not an integer or out of range")
				return
			}
		}
		i += value
		if err = e.db.Put(key, strconv.FormatInt(i, 10)); err != nil {
			return
		}
		d.Payload = &driver.Datum_IntVal{
			IntVal: i,
		}
	}
	switch strings.ToLower(c.Command) {
	case "decr":
		var key string
		if err = c.Scan(&key); err != nil {
			break
		}
		incrby(key, -1)
	case "decrby":
		var key, value string
		if err = c.Scan(&key, &value); err != nil {
			break
		}
		var i int64
		if i, err = strconv.ParseInt(value, 10, 64); err != nil {
			break
		}
		incrby(key, -i)
	case "del":
		// TODO(mjibson): remove race condition; improve perf
		// This function has a race condition because it first Gets an item so it
		// can count it for deletion. But the item could have been deleted after the
		// Get. A transaction would fix that, but be much slower. Consider changing
		// the Del API to return the number of keys deleted.
		var i int64
		var val client.KeyValue
		for _, key := range c.Arguments {
			val, err = e.db.Get(key)
			if err != nil {
				break
			}
			if !val.Exists() {
				continue
			}
			i++
			if err = e.db.Del(key); err != nil {
				break
			}
		}
		d.Payload = &driver.Datum_IntVal{
			IntVal: i,
		}
	case "get":
		var key string
		if err = c.Scan(&key); err != nil {
			break
		}
		var val client.KeyValue
		val, err = e.db.Get(key)
		if err != nil {
			break
		}
		if !val.Exists() {
			d.Payload = &driver.Datum_NullVal{}
			break
		}
		d.Payload = &driver.Datum_StringVal{
			StringVal: string(val.ValueBytes()),
		}
	case "incr":
		var key string
		if err = c.Scan(&key); err != nil {
			break
		}
		incrby(key, 1)
	case "incrby":
		var key, value string
		if err = c.Scan(&key, &value); err != nil {
			break
		}
		var i int64
		if i, err = strconv.ParseInt(value, 10, 64); err != nil {
			break
		}
		incrby(key, i)
	case "set":
		var key, value string
		if err = c.Scan(&key, &value); err != nil {
			break
		}
		if err = e.db.Put(key, value); err != nil {
			break
		}
		d.Payload = &driver.Datum_StringVal{
			StringVal: "OK",
		}
	default:
		err = fmt.Errorf("unknown command '%s'", c.Command)
	}
	r := driver.Response{
		Response: d,
	}
	if err != nil {
		e := driver.NewError("ERR", err.Error())
		r.Response.Payload = &driver.Datum_ErrorVal{
			ErrorVal: &e,
		}
		return r, 400, err
	}
	return r, 200, nil
}
