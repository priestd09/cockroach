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

var (
	datumWrongType = &driver.Datum_ErrorVal{
		&driver.Error{
			Typ:     "WRONGTYPE",
			Message: "Operation against a key holding the wrong kind of value",
		},
	}
	datumNotInteger = &driver.Datum_ErrorVal{
		&driver.Error{
			Typ:     "ERR",
			Message: "value is not an integer or out of range",
		},
	}
	errWrongType = errors.New("Operation against a key holding the wrong kind of value")
)

// Execute the command(s) in the given request and return a response.
// On error, the returned integer is an HTTP error code.
func (e *Executor) Execute(c driver.Command) (driver.Response, int, error) {
	var d driver.Datum
	var err error
	const RedisPrefix = "/redis/"
	redisEnd := []byte(RedisPrefix)
	redisEnd[len(redisEnd)-1]++
	toKey := func(key string) string {
		return RedisPrefix + key
	}
	fromKey := func(key string) string {
		return key[len(RedisPrefix):]
	}
	_ = fromKey
	incrby := func(key string, value int64) {
		key = toKey(key)
		err = e.db.Txn(func(txn *client.Txn) error {
			i, _, err := getInt(txn, key, &d)
			if err != nil {
				return err
			}
			i += value
			if err := putString(txn, key, strconv.FormatInt(i, 10)); err != nil {
				return err
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: i,
			}
			return nil
		})
	}
	switch strings.ToLower(c.Command) {

	default:
		err = fmt.Errorf("unknown command '%s'", c.Command)

	// Lists.

	case "lindex":
		var key, index string
		if err = c.Scan(&key, &index); err != nil {
			break
		}
		key = toKey(key)
		idx, err := strconv.Atoi(index)
		if err != nil {
			break
		}
		var sl []string
		if sl, _, err = getList(&e.db, key, &d); err != nil {
			break
		}
		if idx < 0 {
			idx = len(sl) + idx
		}
		if idx >= len(sl) {
			d.Payload = &driver.Datum_NullVal{}
			break
		}
		d.Payload = &driver.Datum_ByteVal{
			ByteVal: []byte(sl[idx]),
		}

	case "llen":
		var key string
		if err = c.Scan(&key); err != nil {
			break
		}
		key = toKey(key)
		var sl []string
		if sl, _, err = getList(&e.db, key, &d); err != nil {
			break
		}
		d.Payload = &driver.Datum_IntVal{
			IntVal: int64(len(sl)),
		}

	case "lpush":
		if len(c.Arguments) < 2 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		key := toKey(c.Arguments[0])
		err = e.db.Txn(func(txn *client.Txn) error {
			sl, _, err := getList(txn, key, &d)
			if err != nil {
				return err
			}
			sl = append(c.Arguments[1:], sl...)
			if err := putList(txn, key, sl); err != nil {
				return err
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: int64(len(sl)),
			}
			return nil
		})

	case "lrange":
		var key, start, stop string
		if err = c.Scan(&key, &start, &stop); err != nil {
			break
		}
		key = toKey(key)
		var beg, end int
		beg, err = strconv.Atoi(start)
		if err != nil {
			break
		}
		end, err = strconv.Atoi(stop)
		if err != nil {
			break
		}
		var sl []string
		if sl, _, err = getList(&e.db, key, &d); err != nil {
			break
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

	case "rpush":
		if len(c.Arguments) < 2 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		key := toKey(c.Arguments[0])
		err = e.db.Txn(func(txn *client.Txn) error {
			sl, _, err := getList(txn, key, &d)
			if err != nil {
				return err
			}
			sl = append(sl, c.Arguments[1:]...)
			if err := putList(txn, key, sl); err != nil {
				return err
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: int64(len(sl)),
			}
			return nil
		})

	// Keys.

	case "del":
		err = e.db.Txn(func(txn *client.Txn) error {
			var i int64
			for _, key := range c.Arguments {
				key = toKey(key)
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
				key = toKey(key)
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

	case "flushall":
		err = e.db.DelRange(RedisPrefix, redisEnd)
		d.Payload = &driver.Datum_StringVal{
			StringVal: "OK",
		}

	case "rename":
		var key, dst string
		if err = c.Scan(&key, &dst); err != nil {
			break
		}
		key = toKey(key)
		dst = toKey(dst)
		err = e.db.Txn(func(txn *client.Txn) error {
			val, ok, err := getString(txn, key, &d)
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("no such key")
			}
			if err := putString(txn, dst, val); err != nil {
				return err
			}
			d.Payload = &driver.Datum_StringVal{
				StringVal: "OK",
			}
			return nil
		})

	// Strings.

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

	case "get":
		var key string
		if err = c.Scan(&key); err != nil {
			break
		}
		key = toKey(key)
		var val string
		var ok bool
		val, ok, err = getString(&e.db, key, &d)
		if err != nil {
			break
		}
		if !ok {
			d.Payload = &driver.Datum_NullVal{}
			break
		}
		d.Payload = &driver.Datum_ByteVal{
			ByteVal: []byte(val),
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

	case "mset":
		if len(c.Arguments)%2 != 0 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		err = e.db.Txn(func(txn *client.Txn) error {
			for i := 0; i < len(c.Arguments); i += 2 {
				key, value := c.Arguments[i], c.Arguments[i+1]
				key = toKey(key)
				if err := putString(txn, key, value); err != nil {
					return err
				}
			}
			d.Payload = &driver.Datum_StringVal{
				StringVal: "OK",
			}
			return nil
		})

	case "set":
		var key, value string
		if err = c.Scan(&key, &value); err != nil {
			break
		}
		key = toKey(key)
		if err = putString(&e.db, key, value); err != nil {
			break
		}
		d.Payload = &driver.Datum_StringVal{
			StringVal: "OK",
		}
	}
	r := driver.Response{
		Response: d,
	}
	if err != nil {
		if r.Response.Payload == nil {
			e := driver.NewError("ERR", err.Error())
			r.Response.Payload = &driver.Datum_ErrorVal{
				ErrorVal: &e,
			}
		}
		return r, 400, err
	}
	return r, 200, nil
}

func getInt(db runner, key string, d *driver.Datum) (i int64, ok bool, err error) {
	val, err := db.Get(key)
	if err != nil {
		return 0, false, err
	}
	if !val.Exists() {
		return 0, false, nil
	}
	var s string
	r := bytes.NewReader(val.ValueBytes())
	if err = gob.NewDecoder(r).Decode(&s); err == nil {
		i, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			d.Payload = datumNotInteger
			return 0, false, err
		}
		return i, true, nil
	}
	d.Payload = datumWrongType
	return 0, false, errWrongType
}

func putString(db runner, key, value string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&value); err != nil {
		return err
	}
	return db.Put(key, buf.Bytes())
}

func putList(db runner, key string, value []string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&value); err != nil {
		return err
	}
	return db.Put(key, buf.Bytes())
}

func getString(db runner, key string, d *driver.Datum) (s string, ok bool, err error) {
	val, err := db.Get(key)
	if err != nil {
		return "", false, err
	}
	if !val.Exists() {
		return "", false, nil
	}
	r := bytes.NewReader(val.ValueBytes())
	if err = gob.NewDecoder(r).Decode(&s); err == nil {
		return s, true, nil
	}
	d.Payload = datumWrongType
	return "", false, errWrongType
}

func getList(db runner, key string, d *driver.Datum) (sl []string, ok bool, err error) {
	val, err := db.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !val.Exists() {
		return nil, false, nil
	}
	r := bytes.NewReader(val.ValueBytes())
	if err = gob.NewDecoder(r).Decode(&sl); err == nil {
		return sl, true, nil
	}
	d.Payload = datumWrongType
	return nil, false, err
}

type runner interface {
	Get(key interface{}) (client.KeyValue, error)
	Put(key, value interface{}) error
}
