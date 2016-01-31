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
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/redis/driver"
	"github.com/cockroachdb/cockroach/roachpb"
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
	var pErr *roachpb.Error
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
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			i, _, err := getInt(txn, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			i += value
			if err := putString(txn, key, strconv.FormatInt(i, 10)); err != nil {
				return roachpb.NewError(err)
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

	case "blpop":
		if len(c.Arguments) < 2 {
			err = fmt.Errorf("wrong number of arguments for '%s' command", c.Command)
			break
		}
		timeout := c.Arguments[len(c.Arguments)-1]
		var t int
		t, err = strconv.Atoi(timeout)
		if err != nil {
			break
		}
		if t == 0 {
			err = fmt.Errorf("infinite timeout not supported")
			break
		}
		keys := c.Arguments[:len(c.Arguments)-1]
		for i, k := range keys {
			keys[i] = toKey(k)
		}
		until := time.Now().Add(time.Duration(t) * time.Second)
		for {
			pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
				for _, k := range keys {
					sl, _, err := getList(txn, k, &d)
					if err != nil {
						return roachpb.NewError(err)
					}
					if len(sl) == 0 {
						continue
					}
					if err := putList(txn, k, sl[1:]); err != nil {
						return roachpb.NewError(err)
					}
					d.Payload = &driver.Datum_ByteVal{
						ByteVal: []byte(sl[0]),
					}
					return nil
				}
				return nil
			})
			if pErr != nil || d.Payload != nil {
				break
			}
			if time.Now().After(until) {
				d.Payload = &driver.Datum_NullVal{}
				break
			}
			time.Sleep(time.Second)
		}

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

	case "lpop":
		var key string
		if err = c.Scan(&key); err != nil {
			break
		}
		key = toKey(key)
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			var sl []string
			sl, _, err := getList(txn, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			if len(sl) == 0 {
				d.Payload = &driver.Datum_NullVal{}
				return nil
			}
			if err := putList(txn, key, sl[1:]); err != nil {
				return roachpb.NewError(err)
			}
			d.Payload = &driver.Datum_ByteVal{
				ByteVal: []byte(sl[0]),
			}
			return nil
		})

	case "lpush":
		if len(c.Arguments) < 2 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		key := toKey(c.Arguments[0])
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			sl, _, err := getList(txn, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			sl = append(c.Arguments[1:], sl...)
			if err := putList(txn, key, sl); err != nil {
				return roachpb.NewError(err)
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: int64(len(sl)),
			}
			return nil
		})

	case "lpushx":
		if len(c.Arguments) < 2 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		key := toKey(c.Arguments[0])
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			sl, _, err := getList(txn, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			if len(sl) > 0 {
				sl = append(c.Arguments[1:], sl...)
			}
			if err := putList(txn, key, sl); err != nil {
				return roachpb.NewError(err)
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
		beg, end = ranger(len(sl), beg, end)
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

	case "rpoplpush":
		var source, destination string
		if err = c.Scan(&source, &destination); err != nil {
			break
		}
		source = toKey(source)
		destination = toKey(destination)
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			src, _, err := getList(txn, source, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			if len(src) == 0 {
				d.Payload = &driver.Datum_NullVal{}
				return nil
			}
			s := src[len(src)-1]
			src = src[:len(src)-1]
			if source == destination {
				src = append([]string{s}, src...)
			} else {
				dst, _, err := getList(txn, destination, &d)
				if err != nil {
					return roachpb.NewError(err)
				}
				dst = append(dst, s)
				if err := putList(txn, destination, dst); err != nil {
					return roachpb.NewError(err)
				}
			}
			if err := putList(txn, source, src); err != nil {
				return roachpb.NewError(err)
			}
			d.Payload = &driver.Datum_ByteVal{
				ByteVal: []byte(s),
			}
			return nil
		})

	case "rpush":
		if len(c.Arguments) < 2 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		key := toKey(c.Arguments[0])
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			sl, _, err := getList(txn, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			sl = append(sl, c.Arguments[1:]...)
			if err := putList(txn, key, sl); err != nil {
				return roachpb.NewError(err)
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: int64(len(sl)),
			}
			return nil
		})

	case "rpushx":
		if len(c.Arguments) < 2 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		key := toKey(c.Arguments[0])
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			sl, _, err := getList(txn, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			if len(sl) > 0 {
				sl = append(sl, c.Arguments[1:]...)
			}
			if err := putList(txn, key, sl); err != nil {
				return roachpb.NewError(err)
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: int64(len(sl)),
			}
			return nil
		})

	// Keys.

	case "del":
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
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
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
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
		pErr = e.db.DelRange(RedisPrefix, redisEnd)
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
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			val, err := txn.Get(key)
			if err != nil {
				return err
			}
			if !val.Exists() {
				return roachpb.NewErrorf("no such key")
			}
			if err := txn.Put(dst, val.ValueBytes()); err != nil {
				return err
			}
			d.Payload = &driver.Datum_StringVal{
				StringVal: "OK",
			}
			return nil
		})

	case "renamenx":
		var key, dst string
		if err = c.Scan(&key, &dst); err != nil {
			break
		}
		key = toKey(key)
		dst = toKey(dst)
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			val, err := txn.Get(key)
			if err != nil {
				return err
			}
			if !val.Exists() {
				return roachpb.NewErrorf("no such key")
			}
			if dval, err := txn.Get(dst); err != nil {
				return err
			} else if dval.Exists() {
				d.Payload = &driver.Datum_IntVal{
					IntVal: 0,
				}
				return nil
			}
			if err := txn.Put(dst, val.ValueBytes()); err != nil {
				return err
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: 1,
			}
			return nil
		})

	case "type":
		var key, t string
		if err = c.Scan(&key); err != nil {
			break
		}
		key = toKey(key)
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			val, err := txn.Get(key)
			if err != nil {
				return err
			}
			if !val.Exists() {
				t = "none"
				return nil
			}
			b := val.ValueBytes()
			if strings.HasPrefix(string(b), prefixString) {
				t = "string"
				return nil
			}
			var sl []string
			if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&sl); err == nil {
				t = "list"
				return nil
			}
			return roachpb.NewErrorf("unknown type")
		})
		if err != nil {
			break
		}
		d.Payload = &driver.Datum_StringVal{
			StringVal: t,
		}

	// Strings.

	case "append":
		var key, value string
		if err = c.Scan(&key, &value); err != nil {
			break
		}
		key = toKey(key)
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			val, _, err := getString(&e.db, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			val += value
			if err := putString(&e.db, key, val); err != nil {
				return roachpb.NewError(err)
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: int64(len(val)),
			}
			return nil
		})

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

	case "getrange":
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
		var val string
		val, _, err = getString(&e.db, key, &d)
		if err != nil {
			break
		}
		beg, end = ranger(len(val), beg, end)
		d.Payload = &driver.Datum_ByteVal{
			ByteVal: []byte(val[beg:end]),
		}

	case "getset":
		var key, value string
		if err = c.Scan(&key, &value); err != nil {
			break
		}
		key = toKey(key)
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			val, ok, err := getString(&e.db, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			if err := putString(&e.db, key, value); err != nil {
				return roachpb.NewError(err)
			}
			if ok {
				d.Payload = &driver.Datum_ByteVal{
					ByteVal: []byte(val),
				}
			} else {
				d.Payload = &driver.Datum_NullVal{}
			}
			return nil
		})

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

	case "mget":
		values := make([]*driver.Datum, len(c.Arguments))
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			for i, key := range c.Arguments {
				values[i] = &driver.Datum{}
				key = toKey(key)
				val, ok, err := getString(txn, key, &d)
				if err == errWrongType || !ok {
					values[i].Payload = &driver.Datum_NullVal{}
					continue
				}
				if err != nil {
					return roachpb.NewError(err)
				}
				values[i].Payload = &driver.Datum_ByteVal{
					ByteVal: []byte(val),
				}
			}
			return nil
		})
		if err == nil {
			d.Payload = &driver.Datum_ArrayVal{
				ArrayVal: &driver.Array{
					Values: values,
				},
			}
		}

	case "mset":
		if len(c.Arguments)%2 != 0 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			for i := 0; i < len(c.Arguments); i += 2 {
				key, value := c.Arguments[i], c.Arguments[i+1]
				key = toKey(key)
				if err := putString(txn, key, value); err != nil {
					return roachpb.NewError(err)
				}
			}
			d.Payload = &driver.Datum_StringVal{
				StringVal: "OK",
			}
			return nil
		})

	case "msetnx":
		if len(c.Arguments)%2 != 0 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			var v int64 = 1
			for i := 0; i < len(c.Arguments); i += 2 {
				key := c.Arguments[i]
				key = toKey(key)
				val, err := txn.Get(key)
				if err != nil {
					return err
				}
				if val.Exists() {
					v = 0
					break
				}
			}
			if v == 1 {
				for i := 0; i < len(c.Arguments); i += 2 {
					key, value := c.Arguments[i], c.Arguments[i+1]
					key = toKey(key)
					if err = putString(&e.db, key, value); err != nil {
						return roachpb.NewError(err)
					}
				}
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: v,
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

	case "setnx":
		var key, value string
		if err = c.Scan(&key, &value); err != nil {
			break
		}
		key = toKey(key)
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			val, err := txn.Get(key)
			if err != nil {
				return err
			}
			var v int64
			if !val.Exists() {
				if err := putString(&e.db, key, value); err != nil {
					return roachpb.NewError(err)
				}
				v = 1
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: v,
			}
			return nil
		})

	case "strlen":
		var key string
		if err = c.Scan(&key); err != nil {
			break
		}
		key = toKey(key)
		var val string
		val, _, err = getString(&e.db, key, &d)
		if err != nil {
			break
		}
		d.Payload = &driver.Datum_IntVal{
			IntVal: int64(len(val)),
		}

	// Sets.

	case "sadd":
		if len(c.Arguments) < 2 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		key := toKey(c.Arguments[0])
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			set, _, err := getSet(&e.db, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			var i int64
			for _, a := range c.Arguments[1:] {
				if _, ok := set[a]; !ok {
					set[a] = struct{}{}
					i++
				}
			}
			if err := putSet(txn, key, set); err != nil {
				return roachpb.NewError(err)
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: i,
			}
			return nil
		})

	case "scard":
		var key string
		if err = c.Scan(&key); err != nil {
			break
		}
		key = toKey(key)
		set, _, err := getSet(&e.db, key, &d)
		if err != nil {
			break
		}
		d.Payload = &driver.Datum_IntVal{
			IntVal: int64(len(set)),
		}

	case "sismember":
		var key, member string
		if err = c.Scan(&key, &member); err != nil {
			break
		}
		key = toKey(key)
		set, _, err := getSet(&e.db, key, &d)
		if err != nil {
			break
		}
		var i int64
		if _, ok := set[member]; ok {
			i = 1
		}
		d.Payload = &driver.Datum_IntVal{
			IntVal: i,
		}

	case "smembers":
		var key string
		if err = c.Scan(&key); err != nil {
			break
		}
		key = toKey(key)
		set, _, err := getSet(&e.db, key, &d)
		if err != nil {
			break
		}
		av := make([]*driver.Datum, len(set))
		i := 0
		strs := make([]string, len(set))
		for s := range set {
			strs[i] = s
			i++
		}
		sort.Strings(strs)
		for i, s := range strs {
			av[i] = &driver.Datum{
				Payload: &driver.Datum_ByteVal{
					ByteVal: []byte(s),
				},
			}
			i++
		}
		d.Payload = &driver.Datum_ArrayVal{
			ArrayVal: &driver.Array{
				Values: av,
			},
		}

	case "srem":
		if len(c.Arguments) < 2 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		key := toKey(c.Arguments[0])
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			set, _, err := getSet(&e.db, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			var i int64
			for _, a := range c.Arguments[1:] {
				if _, ok := set[a]; ok {
					delete(set, a)
					i++
				}
			}
			if err := putSet(txn, key, set); err != nil {
				return roachpb.NewError(err)
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: i,
			}
			return nil
		})

	// Hashes.

	case "hget":
		var key, field string
		if err = c.Scan(&key, &field); err != nil {
			break
		}
		key = toKey(key)
		hash, _, err := getHash(&e.db, key, &d)
		if err != nil {
			break
		}
		v, ok := hash[field]
		if !ok {
			d.Payload = &driver.Datum_NullVal{}
			break
		}
		d.Payload = &driver.Datum_ByteVal{
			ByteVal: []byte(v),
		}

	case "hmset":
		if n := len(c.Arguments); n%2 != 1 || n < 3 {
			err = fmt.Errorf(errWrongNumberOfArguments, c.Command)
			break
		}
		key := toKey(c.Arguments[0])
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			hash, _, err := getHash(txn, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			for i := 1; i < len(c.Arguments); i += 2 {
				field, value := c.Arguments[i], c.Arguments[i+1]
				hash[field] = value
			}
			if err := putHash(txn, key, hash); err != nil {
				return roachpb.NewError(err)
			}
			d.Payload = &driver.Datum_StringVal{
				StringVal: "OK",
			}
			return nil
		})

	case "hset":
		var key, field, value string
		if err = c.Scan(&key, &field, &value); err != nil {
			break
		}
		key = toKey(key)
		pErr = e.db.Txn(func(txn *client.Txn) *roachpb.Error {
			hash, _, err := getHash(&e.db, key, &d)
			if err != nil {
				return roachpb.NewError(err)
			}
			var i int64
			if _, ok := hash[field]; !ok {
				i = 1
			}
			hash[field] = value
			if err := putHash(txn, key, hash); err != nil {
				return roachpb.NewError(err)
			}
			d.Payload = &driver.Datum_IntVal{
				IntVal: i,
			}
			return nil
		})

	}
	r := driver.Response{
		Response: d,
	}
	if pErr != nil {
		err = pErr.GoError()
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

const (
	prefixString = "$"
)

func getInt(db runner, key string, d *driver.Datum) (i int64, ok bool, err error) {
	s, ok, err := getString(db, key, d)
	if !ok || err != nil {
		return 0, ok, err
	}
	i, err = strconv.ParseInt(s, 10, 64)
	if err != nil {
		d.Payload = datumNotInteger
		return 0, false, err
	}
	return i, true, nil
}

func putString(db runner, key, value string) error {
	return db.Put(key, []byte(prefixString+value)).GoError()
}

func putList(db runner, key string, value []string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&value); err != nil {
		return err
	}
	return db.Put(key, buf.Bytes()).GoError()
}

func getString(db runner, key string, d *driver.Datum) (s string, ok bool, err error) {
	val, pErr := db.Get(key)
	if pErr != nil {
		return "", false, pErr.GoError()
	}
	if !val.Exists() {
		return "", false, nil
	}
	s = string(val.ValueBytes())
	if strings.HasPrefix(s, prefixString) {
		return s[1:], true, nil
	}
	d.Payload = datumWrongType
	return "", false, errWrongType
}

func getList(db runner, key string, d *driver.Datum) (sl []string, ok bool, err error) {
	val, pErr := db.Get(key)
	if pErr != nil {
		return nil, false, pErr.GoError()
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

func getSet(db runner, key string, d *driver.Datum) (set Set, ok bool, err error) {
	val, pErr := db.Get(key)
	if pErr != nil {
		return nil, false, pErr.GoError()
	}
	if !val.Exists() {
		return make(Set), false, nil
	}
	r := bytes.NewReader(val.ValueBytes())
	if err = gob.NewDecoder(r).Decode(&set); err == nil {
		return set, true, nil
	}
	d.Payload = datumWrongType
	return nil, false, err
}

func putSet(db runner, key string, value Set) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&value); err != nil {
		return err
	}
	return db.Put(key, buf.Bytes()).GoError()
}

type Set map[string]struct{}

func getHash(db runner, key string, d *driver.Datum) (set Hash, ok bool, err error) {
	val, pErr := db.Get(key)
	if pErr != nil {
		return nil, false, pErr.GoError()
	}
	if !val.Exists() {
		return make(Hash), false, nil
	}
	r := bytes.NewReader(val.ValueBytes())
	if err = gob.NewDecoder(r).Decode(&set); err == nil {
		return set, true, nil
	}
	d.Payload = datumWrongType
	return nil, false, err
}

func putHash(db runner, key string, value Hash) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&value); err != nil {
		return err
	}
	return db.Put(key, buf.Bytes()).GoError()
}

type Hash map[string]string

type runner interface {
	Get(key interface{}) (client.KeyValue, *roachpb.Error)
	Put(key, value interface{}) *roachpb.Error
}

func ranger(ln, beg, end int) (low, high int) {
	if beg < 0 {
		beg = ln + beg
	}
	if end < 0 {
		end = ln + end
	}
	if beg < 0 {
		beg = 0
	}
	if end < beg {
		beg = 0
		end = -1
	}
	end++
	if beg > ln {
		beg = ln
	}
	if end > ln {
		end = ln
	}
	return beg, end
}
