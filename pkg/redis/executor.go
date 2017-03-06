// Copyright 2017 The Cockroach Authors.
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

package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/pkg/errors"
)

const errWrongNumberOfArguments = "wrong number of arguments for '%s' command"

var (
	datumWrongType = Error{
		Typ:     "WRONGTYPE",
		Message: "Operation against a key holding the wrong kind of value",
	}
	datumNotInteger = Error{
		Typ:     "ERR",
		Message: "value is not an integer or out of range",
	}
	errWrongType = errors.New("Operation against a key holding the wrong kind of value")
)

func execute(ctx context.Context, db *client.DB, command string, args []string) (interface{}, error) {
	cdb := &ctxDB{
		ctx: ctx,
		db:  db,
	}
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
	var ret interface{}
	incrby := func(key string, value int64) {
		key = toKey(key)

		// Attempt to increment the fast way.
		if v, err := db.Inc(ctx, key, value); err == nil {
			ret = v.ValueInt()
			return
		}
		// We either have a string or another data type. If it's a string, delete
		// and recreate using db.Inc. If it's something else, error.
		err = db.Txn(ctx, func(txn *client.Txn) error {
			s, ok, err := getString(txn, key, &ret)
			if err != nil {
				return err
			}
			var i int64
			if ok {
				i, err = strconv.ParseInt(s, 10, 64)
				if err != nil {
					ret = datumNotInteger
					return nil
				}
			}
			i += value
			if err := txn.Del(key); err != nil {
				return err
			}
			if v, err := txn.Inc(key, i); err != nil {
				return err
			} else {
				ret = v.ValueInt()
			}
			return nil
		})
	}
	switch strings.ToLower(command) {

	default:
		err = errors.Errorf("unknown command '%s'", command)

	case "command":
		ret = null

	// Lists.

	case "blpop":
		if len(args) < 2 {
			err = errors.Errorf("wrong number of arguments for '%s' command", command)
			break
		}
		timeout := args[len(args)-1]
		var t int
		t, err = strconv.Atoi(timeout)
		if err != nil {
			break
		}
		if t == 0 {
			err = errors.Errorf("infinite timeout not supported")
			break
		}
		keys := args[:len(args)-1]
		for i, k := range keys {
			keys[i] = toKey(k)
		}
		until := time.Now().Add(time.Duration(t) * time.Second)
		for {
			err = db.Txn(ctx, func(txn *client.Txn) error {
				for _, k := range keys {
					sl, _, err := getList(txn, k, &ret)
					if err != nil {
						return err
					}
					if len(sl) == 0 {
						continue
					}
					if err := putList(txn, k, sl[1:]); err != nil {
						return err
					}
					ret = []byte(sl[0])
					return nil
				}
				return nil
			})
			if err != nil || ret != nil {
				break
			}
			if time.Now().After(until) {
				ret = null
				break
			}
			time.Sleep(time.Second)
		}

	case "lindex":
		var key, index string
		if err = scan(command, args, &key, &index); err != nil {
			break
		}
		key = toKey(key)
		idx, err := strconv.Atoi(index)
		if err != nil {
			break
		}
		var sl []string
		if sl, _, err = getList(cdb, key, &ret); err != nil {
			break
		}
		if idx < 0 {
			idx = len(sl) + idx
		}
		if idx >= len(sl) {
			ret = null
			break
		}
		ret = []byte(sl[idx])

	case "llen":
		var key string
		if err = scan(command, args, &key); err != nil {
			break
		}
		key = toKey(key)
		var sl []string
		if sl, _, err = getList(cdb, key, &ret); err != nil {
			break
		}
		ret = int64(len(sl))

	case "lpop":
		var key string
		if err = scan(command, args, &key); err != nil {
			break
		}
		key = toKey(key)
		err = db.Txn(ctx, func(txn *client.Txn) error {
			var sl []string
			sl, _, err := getList(txn, key, &ret)
			if err != nil {
				return err
			}
			if len(sl) == 0 {
				ret = null
				return nil
			}
			if err := putList(txn, key, sl[1:]); err != nil {
				return err
			}
			ret = []byte(sl[0])
			return nil
		})

	case "lpush":
		if len(args) < 2 {
			err = errors.Errorf(errWrongNumberOfArguments, command)
			break
		}
		key := toKey(args[0])
		err = db.Txn(ctx, func(txn *client.Txn) error {
			sl, _, err := getList(txn, key, &ret)
			if err != nil {
				return err
			}
			sl = append(args[1:], sl...)
			if err := putList(txn, key, sl); err != nil {
				return err
			}
			ret = int64(len(sl))
			return nil
		})

	case "lpushx":
		if len(args) < 2 {
			err = errors.Errorf(errWrongNumberOfArguments, command)
			break
		}
		key := toKey(args[0])
		err = db.Txn(ctx, func(txn *client.Txn) error {
			sl, _, err := getList(txn, key, &ret)
			if err != nil {
				return err
			}
			if len(sl) > 0 {
				sl = append(args[1:], sl...)
			}
			if err := putList(txn, key, sl); err != nil {
				return err
			}
			ret = int64(len(sl))
			return nil
		})

	case "lrange":
		var key, start, stop string
		if err = scan(command, args, &key, &start, &stop); err != nil {
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
		if sl, _, err = getList(cdb, key, &ret); err != nil {
			break
		}
		beg, end = ranger(len(sl), beg, end)
		sl = sl[beg:end]
		av := make(Array, len(sl))
		for i, s := range sl {
			av[i] = []byte(s)
		}
		ret = av

	case "rpoplpush":
		var source, destination string
		if err = scan(command, args, &source, &destination); err != nil {
			break
		}
		source = toKey(source)
		destination = toKey(destination)
		err = db.Txn(ctx, func(txn *client.Txn) error {
			src, _, err := getList(txn, source, &ret)
			if err != nil {
				return err
			}
			if len(src) == 0 {
				ret = null
				return nil
			}
			s := src[len(src)-1]
			src = src[:len(src)-1]
			if source == destination {
				src = append([]string{s}, src...)
			} else {
				dst, _, err := getList(txn, destination, &ret)
				if err != nil {
					return err
				}
				dst = append(dst, s)
				if err := putList(txn, destination, dst); err != nil {
					return err
				}
			}
			if err := putList(txn, source, src); err != nil {
				return err
			}
			ret = []byte(s)
			return nil
		})

	case "rpush":
		if len(args) < 2 {
			err = errors.Errorf(errWrongNumberOfArguments, command)
			break
		}
		key := toKey(args[0])
		err = db.Txn(ctx, func(txn *client.Txn) error {
			sl, _, err := getList(txn, key, &ret)
			if err != nil {
				return err
			}
			sl = append(sl, args[1:]...)
			if err := putList(txn, key, sl); err != nil {
				return err
			}
			ret = int64(len(sl))
			return nil
		})

	case "rpushx":
		if len(args) < 2 {
			err = errors.Errorf(errWrongNumberOfArguments, command)
			break
		}
		key := toKey(args[0])
		err = db.Txn(ctx, func(txn *client.Txn) error {
			sl, _, err := getList(txn, key, &ret)
			if err != nil {
				return err
			}
			if len(sl) > 0 {
				sl = append(sl, args[1:]...)
			}
			if err := putList(txn, key, sl); err != nil {
				return err
			}
			ret = int64(len(sl))
			return nil
		})

	// Keys.

	case "del":
		err = db.Txn(ctx, func(txn *client.Txn) error {
			var i int64
			for _, key := range args {
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
			ret = i
			return nil
		})

	case "exists":
		err = db.Txn(ctx, func(txn *client.Txn) error {
			var i int64
			for _, key := range args {
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
			ret = i
			return nil
		})

	case "flushall":
		err = db.DelRange(ctx, RedisPrefix, redisEnd)
		ret = "OK"

	case "rename":
		var key, dst string
		if err = scan(command, args, &key, &dst); err != nil {
			break
		}
		key = toKey(key)
		dst = toKey(dst)
		err = db.Txn(ctx, func(txn *client.Txn) error {
			val, err := txn.Get(key)
			if err != nil {
				return err
			}
			if !val.Exists() {
				return errors.Errorf("no such key")
			}
			if err := txn.Put(dst, val.ValueBytes()); err != nil {
				return err
			}
			ret = "OK"
			return nil
		})

	case "renamenx":
		var key, dst string
		if err = scan(command, args, &key, &dst); err != nil {
			break
		}
		key = toKey(key)
		dst = toKey(dst)
		err = db.Txn(ctx, func(txn *client.Txn) error {
			val, err := txn.Get(key)
			if err != nil {
				return err
			}
			if !val.Exists() {
				return errors.Errorf("no such key")
			}
			if dval, err := txn.Get(dst); err != nil {
				return err
			} else if dval.Exists() {
				ret = int64(0)
				return nil
			}
			if err := txn.Put(dst, val.ValueBytes()); err != nil {
				return err
			}
			ret = int64(1)
			return nil
		})

	case "type":
		var key, t string
		if err = scan(command, args, &key); err != nil {
			break
		}
		key = toKey(key)
		err = db.Txn(ctx, func(txn *client.Txn) error {
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
			return errors.Errorf("unknown type")
		})
		if err != nil {
			break
		}
		ret = t

	// Strings.

	case "append":
		var key, value string
		if err = scan(command, args, &key, &value); err != nil {
			break
		}
		key = toKey(key)
		err = db.Txn(ctx, func(txn *client.Txn) error {
			val, _, err := getString(cdb, key, &ret)
			if err != nil {
				return err
			}
			val += value
			if err := putString(cdb, key, val); err != nil {
				return err
			}
			ret = int64(len(val))
			return nil
		})

	case "decr":
		var key string
		if err = scan(command, args, &key); err != nil {
			break
		}
		incrby(key, -1)

	case "decrby":
		var key, value string
		if err = scan(command, args, &key, &value); err != nil {
			break
		}
		var i int64
		if i, err = strconv.ParseInt(value, 10, 64); err != nil {
			break
		}
		incrby(key, -i)

	case "get":
		var key string
		if err = scan(command, args, &key); err != nil {
			break
		}
		key = toKey(key)
		var val string
		var ok bool
		val, ok, err = getString(cdb, key, &ret)
		if err != nil {
			break
		}
		if !ok {
			ret = null
			break
		}
		ret = []byte(val)

	case "getrange":
		var key, start, stop string
		if err = scan(command, args, &key, &start, &stop); err != nil {
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
		val, _, err = getString(cdb, key, &ret)
		if err != nil {
			break
		}
		beg, end = ranger(len(val), beg, end)
		ret = []byte(val[beg:end])

	case "getset":
		var key, value string
		if err = scan(command, args, &key, &value); err != nil {
			break
		}
		key = toKey(key)
		err = db.Txn(ctx, func(txn *client.Txn) error {
			val, ok, err := getString(cdb, key, &ret)
			if err != nil {
				return err
			}
			if err := putString(cdb, key, value); err != nil {
				return err
			}
			if ok {
				ret = []byte(val)
			} else {
				ret = null
			}
			return nil
		})

	case "incr":
		var key string
		if err = scan(command, args, &key); err != nil {
			break
		}
		incrby(key, 1)

	case "incrby":
		var key, value string
		if err = scan(command, args, &key, &value); err != nil {
			break
		}
		var i int64
		if i, err = strconv.ParseInt(value, 10, 64); err != nil {
			break
		}
		incrby(key, i)

	case "mget":
		values := make(Array, len(args))
		err = db.Txn(ctx, func(txn *client.Txn) error {
			for i, key := range args {
				key = toKey(key)
				val, ok, err := getString(txn, key, &ret)
				if err == errWrongType || !ok {
					values[i] = null
					continue
				}
				if err != nil {
					return err
				}
				values[i] = []byte(val)
			}
			return nil
		})
		if err == nil {
			ret = values
		}

	case "mset":
		if len(args)%2 != 0 {
			err = errors.Errorf(errWrongNumberOfArguments, command)
			break
		}
		err = db.Txn(ctx, func(txn *client.Txn) error {
			for i := 0; i < len(args); i += 2 {
				key, value := args[i], args[i+1]
				key = toKey(key)
				if err := putString(txn, key, value); err != nil {
					return err
				}
			}
			ret = "OK"
			return nil
		})

	case "msetnx":
		if len(args)%2 != 0 {
			err = errors.Errorf(errWrongNumberOfArguments, command)
			break
		}
		err = db.Txn(ctx, func(txn *client.Txn) error {
			var v int64 = 1
			for i := 0; i < len(args); i += 2 {
				key := args[i]
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
				for i := 0; i < len(args); i += 2 {
					key, value := args[i], args[i+1]
					key = toKey(key)
					if err = putString(cdb, key, value); err != nil {
						return err
					}
				}
			}
			ret = v
			return nil
		})

	case "set":
		var key, value string
		if err = scan(command, args, &key, &value); err != nil {
			break
		}
		key = toKey(key)
		if err = putString(cdb, key, value); err != nil {
			break
		}
		ret = "OK"

	case "setnx":
		var key, value string
		if err = scan(command, args, &key, &value); err != nil {
			break
		}
		key = toKey(key)
		err = db.Txn(ctx, func(txn *client.Txn) error {
			val, err := txn.Get(key)
			if err != nil {
				return err
			}
			var v int64
			if !val.Exists() {
				if err := putString(cdb, key, value); err != nil {
					return err
				}
				v = 1
			}
			ret = v
			return nil
		})

	case "strlen":
		var key string
		if err = scan(command, args, &key); err != nil {
			break
		}
		key = toKey(key)
		var val string
		val, _, err = getString(cdb, key, &ret)
		if err != nil {
			break
		}
		ret = int64(len(val))

	// Sets.

	case "sadd":
		if len(args) < 2 {
			err = errors.Errorf(errWrongNumberOfArguments, command)
			break
		}
		key := toKey(args[0])
		err = db.Txn(ctx, func(txn *client.Txn) error {
			set, _, err := getSet(cdb, key, &ret)
			if err != nil {
				return err
			}
			var i int64
			for _, a := range args[1:] {
				if _, ok := set[a]; !ok {
					set[a] = struct{}{}
					i++
				}
			}
			if err := putSet(txn, key, set); err != nil {
				return err
			}
			ret = i
			return nil
		})

	case "scard":
		var key string
		if err = scan(command, args, &key); err != nil {
			break
		}
		key = toKey(key)
		set, _, err := getSet(cdb, key, &ret)
		if err != nil {
			break
		}
		ret = int64(len(set))

	case "sismember":
		var key, member string
		if err = scan(command, args, &key, &member); err != nil {
			break
		}
		key = toKey(key)
		set, _, err := getSet(cdb, key, &ret)
		if err != nil {
			break
		}
		var i int64
		if _, ok := set[member]; ok {
			i = 1
		}
		ret = i

	case "smembers":
		var key string
		if err = scan(command, args, &key); err != nil {
			break
		}
		key = toKey(key)
		set, _, err := getSet(cdb, key, &ret)
		if err != nil {
			break
		}
		av := make(Array, len(set))
		i := 0
		strs := make([]string, len(set))
		for s := range set {
			strs[i] = s
			i++
		}
		sort.Strings(strs)
		for i, s := range strs {
			av[i] = []byte(s)
			i++
		}
		ret = av

	case "srem":
		if len(args) < 2 {
			err = errors.Errorf(errWrongNumberOfArguments, command)
			break
		}
		key := toKey(args[0])
		err = db.Txn(ctx, func(txn *client.Txn) error {
			set, _, err := getSet(cdb, key, &ret)
			if err != nil {
				return err
			}
			var i int64
			for _, a := range args[1:] {
				if _, ok := set[a]; ok {
					delete(set, a)
					i++
				}
			}
			if err := putSet(txn, key, set); err != nil {
				return err
			}
			ret = i
			return nil
		})

	// Hashes.

	case "hget":
		var key, field string
		if err = scan(command, args, &key, &field); err != nil {
			break
		}
		key = toKey(key)
		hash, _, err := getHash(cdb, key, &ret)
		if err != nil {
			break
		}
		v, ok := hash[field]
		if !ok {
			ret = null
			break
		}
		ret = []byte(v)

	case "hmset":
		if n := len(args); n%2 != 1 || n < 3 {
			err = errors.Errorf(errWrongNumberOfArguments, command)
			break
		}
		key := toKey(args[0])
		err = db.Txn(ctx, func(txn *client.Txn) error {
			hash, _, err := getHash(txn, key, &ret)
			if err != nil {
				return err
			}
			for i := 1; i < len(args); i += 2 {
				field, value := args[i], args[i+1]
				hash[field] = value
			}
			if err := putHash(txn, key, hash); err != nil {
				return err
			}
			ret = "OK"
			return nil
		})

	case "hset":
		var key, field, value string
		if err = scan(command, args, &key, &field, &value); err != nil {
			break
		}
		key = toKey(key)
		err = db.Txn(ctx, func(txn *client.Txn) error {
			hash, _, err := getHash(cdb, key, &ret)
			if err != nil {
				return err
			}
			var i int64
			if _, ok := hash[field]; !ok {
				i = 1
			}
			hash[field] = value
			if err := putHash(txn, key, hash); err != nil {
				return err
			}
			ret = i
			return nil
		})

	}
	if err != nil && ret == nil {
		ret = err
	}
	return ret, err
}

const (
	prefixString = "$"
)

func putString(db runner, key, value string) error {
	return db.Put(key, []byte(prefixString+value))
}

func putList(db runner, key string, value []string) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&value); err != nil {
		return err
	}
	return db.Put(key, buf.Bytes())
}

func getString(db runner, key string, d *interface{}) (s string, ok bool, err error) {
	val, err := db.Get(key)
	if err != nil {
		return "", false, err
	}
	if !val.Exists() {
		return "", false, nil
	}
	switch val.Value.GetTag() {
	case roachpb.ValueType_BYTES:
		s = string(val.ValueBytes())
		if strings.HasPrefix(s, prefixString) {
			return s[1:], true, nil
		}
	case roachpb.ValueType_INT:
		return strconv.FormatInt(val.ValueInt(), 10), true, nil
	}
	*d = datumWrongType
	return "", false, errWrongType
}

func getList(db runner, key string, d *interface{}) (sl []string, ok bool, err error) {
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
	*d = datumWrongType
	return nil, false, err
}

func getSet(db runner, key string, d *interface{}) (set Set, ok bool, err error) {
	val, err := db.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !val.Exists() {
		return make(Set), false, nil
	}
	r := bytes.NewReader(val.ValueBytes())
	if err = gob.NewDecoder(r).Decode(&set); err == nil {
		return set, true, nil
	}
	*d = datumWrongType
	return nil, false, err
}

func putSet(db runner, key string, value Set) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&value); err != nil {
		return err
	}
	return db.Put(key, buf.Bytes())
}

type Set map[string]struct{}

func getHash(db runner, key string, d *interface{}) (set Hash, ok bool, err error) {
	val, err := db.Get(key)
	if err != nil {
		return nil, false, err
	}
	if !val.Exists() {
		return make(Hash), false, nil
	}
	r := bytes.NewReader(val.ValueBytes())
	if err = gob.NewDecoder(r).Decode(&set); err == nil {
		return set, true, nil
	}
	*d = datumWrongType
	return nil, false, err
}

func putHash(db runner, key string, value Hash) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&value); err != nil {
		return err
	}
	return db.Put(key, buf.Bytes())
}

type Hash map[string]string

type runner interface {
	Get(key interface{}) (client.KeyValue, error)
	Put(key, value interface{}) error
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

func scan(command string, args []string, dest ...*string) error {
	if len(dest) != len(args) {
		return errors.Errorf("wrong number of arguments for '%s' command", command)
	}
	for i, d := range dest {
		*d = args[i]
	}
	return nil
}

type ctxDB struct {
	ctx context.Context
	db  *client.DB
}

func (c *ctxDB) Get(key interface{}) (client.KeyValue, error) {
	return c.db.Get(c.ctx, key)
}

func (c *ctxDB) Put(key, value interface{}) error {
	return c.db.Put(c.ctx, key, value)
}
