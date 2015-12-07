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
	"bytes"
	"encoding/gob"
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
		err = e.db.Txn(func(txn *client.Txn) error {
			val, err := e.db.Get(key)
			if err != nil {
				return err
			}
			var i int64
			if !val.Exists() {
				i = 0
			} else {
				i, err = strconv.ParseInt(string(val.ValueBytes()), 10, 64)
				if err != nil {
					return errors.New("value is not an integer or out of range")
				}
			}
			i += value
			if err := e.db.Put(key, strconv.FormatInt(i, 10)); err != nil {
				return err
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: i,
			}
			return nil
		})
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
		err = e.db.Txn(func(txn *client.Txn) error {
			var i int64
			for _, key := range c.Arguments {
				val, err := txn.Get(key)
				if err != nil {
					return err
				}
				if !val.Exists() {
					continue
				}
				i++
				if err := txn.Del(key); err != nil {
					return err
				}
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: i,
			}
			return nil
		})
	case "exists":
		err = e.db.Txn(func(txn *client.Txn) error {
			var i int64
			for _, key := range c.Arguments {
				val, err := txn.Get(key)
				if err != nil {
					return err
				}
				if !val.Exists() {
					continue
				}
				i++
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: i,
			}
			return nil
		})
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
		d.Payload = &driver.Datum_ByteVal{
			ByteVal: val.ValueBytes(),
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
	case "lrange":
		var key, start, stop string
		if err = c.Scan(&key, &start, &stop); err != nil {
			break
		}
		var beg, end int
		beg, err = strconv.Atoi(start)
		if err != nil {
			break
		}
		end, err = strconv.Atoi(stop)
		if err != nil {
			break
		}
		if err = c.Scan(&key, &start, &stop); err != nil {
			break
		}
		var val client.KeyValue
		val, err = e.db.Get(key)
		if err != nil {
			break
		}
		var sl []string
		if val.Exists() {
			r := bytes.NewReader(val.ValueBytes())
			if err = gob.NewDecoder(r).Decode(&sl); err != nil {
				// TODO(mjibson): support WRONGTYPE error here
				break
			}
		}
		if beg < 0 {
			beg = len(sl) - 1 + beg
		}
		if end < 0 {
			end = len(sl) - 1 + end
		}
		if beg < 0 {
			beg = 0
		}
		if end < beg {
			beg = 0
			end = -1
		}
		end++
		if beg > len(sl) {
			beg = len(sl)
		}
		if end > len(sl) {
			end = len(sl)
		}
		sl = sl[beg:end]
		av := make([]*driver.Datum, len(sl))
		for i, s := range sl {
			av[i] = &driver.Datum{
				Payload: &driver.Datum_ByteVal{
					ByteVal: []byte(s),
				},
			}
		}
		d.Payload = &driver.Datum_ArrayVal{
			ArrayVal: &driver.Array{
				Values: av,
			},
		}
	case "mset":
		if len(c.Arguments)%2 != 0 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		err = e.db.Txn(func(txn *client.Txn) error {
			for i := 0; i < len(c.Arguments); i += 2 {
				key, value := c.Arguments[i], c.Arguments[i+1]
				if err := e.db.Put(key, value); err != nil {
					return err
				}
			}
			d.Payload = &driver.Datum_StringVal{
				StringVal: "OK",
			}
			return nil
		})
	case "rename":
		var src, dst string
		if err = c.Scan(&src, &dst); err != nil {
			break
		}
		err = e.db.Txn(func(txn *client.Txn) error {
			val, err := e.db.Get(src)
			if err != nil {
				return err
			}
			if !val.Exists() {
				return errors.New("no such key")
			}
			if err := e.db.Put(dst, val.ValueBytes()); err != nil {
				return err
			}
			d.Payload = &driver.Datum_StringVal{
				StringVal: "OK",
			}
			return nil
		})
	case "rpush":
		if len(c.Arguments) < 2 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		key := c.Arguments[0]
		err = e.db.Txn(func(txn *client.Txn) error {
			val, err := e.db.Get(key)
			if err != nil {
				return err
			}
			var sl []string
			if val.Exists() {
				r := bytes.NewReader(val.ValueBytes())
				if err := gob.NewDecoder(r).Decode(&sl); err != nil {
					// TODO(mjibson): support WRONGTYPE error here
					return err
				}
			}
			sl = append(sl, c.Arguments[1:]...)
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(&sl); err != nil {
				return err
			}
			if err := e.db.Put(key, buf.Bytes()); err != nil {
				return err
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: int64(len(sl)),
			}
			return nil
		})
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
