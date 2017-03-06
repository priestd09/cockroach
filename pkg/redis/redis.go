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

// go build -buildmode=plugin -o ../../plugin.so

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/plugins"
	"github.com/pkg/errors"
)

var Match = plugins.Matcher{
	Matcher: match,
	Serve:   serve,
}

func match(r io.Reader) bool {
	var b [1]byte
	_, _ = r.Read(b[:])
	return b[0] == '*'
}

func serve(l net.Listener, db *client.DB) error {
	for {
		conn, err := l.Accept()
		if err != nil {
			return err
		}
		go func(c net.Conn) {
			defer c.Close()
			if err := handle(c, db); err != nil {
				log.Printf("%+v", err)
			}
		}(conn)
	}
}

const CRLF = "\r\n"

var bCRLF = []byte(CRLF)

func handle(c net.Conn, db *client.DB) error {
	scanner := bufio.NewScanner(c)
	scanner.Split(splitCRLF)
	for scanner.Scan() {
		b := scanner.Bytes()
		if len(b) == 0 {
			return errors.New("empty entry")
		}
		if b[0] != '*' {
			return errors.New("expected array")
		}
		arlen, err := strconv.ParseInt(string(b[1:]), 10, 64)
		if err != nil {
			return errors.New("bad array length")
		}
		args := make([]string, arlen)
		for i := int64(0); i < arlen; i++ {
			if !scanner.Scan() {
				return errors.New("expected more elements in array")
			}
			b := scanner.Bytes()
			if len(b) == 0 {
				return errors.New("empty entry")
			}
			if b[0] != '$' {
				return errors.New("expected string")
			}
			tlen, err := strconv.ParseInt(string(b[1:]), 10, 64)
			if err != nil {
				return errors.New("bad string length")
			}
			for b = nil; int64(len(b)) < tlen; {
				if !scanner.Scan() {
					return errors.New("expected more elements in array")
				}
				sb := scanner.Bytes()
				b = append(b, sb...)
				if int64(len(b)) < tlen {
					b = append(b, bCRLF...)
				}
			}
			args[i] = string(b)
		}
		if len(args) < 1 {
			return errors.New("no command")
		}

		resp, err := execute(context.Background(), db, args[0], args[1:])
		if resp != nil {
			if err := renderReply(c, resp); err != nil {
				return errors.Wrap(err, "render reply")
			}
		} else if err != nil {
			return errors.Wrap(err, "execute")
		}
	}
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "scan")
	}
	return nil
}

func renderReply(w io.Writer, v interface{}) error {
	switch d := v.(type) {
	case int64:
		fmt.Fprintf(w, ":%d"+CRLF, d)
	case string:
		fmt.Fprintf(w, "+%s"+CRLF, d)
	case []byte:
		fmt.Fprintf(w, "$%d"+CRLF, len(d))
		w.Write(d)
		w.Write(bCRLF)
	case Null:
		w.Write([]byte("$-1" + CRLF))
	case Error:
		fmt.Fprintf(w, "-%s %s"+CRLF, d.Typ, d.Message)
	case Array:
		fmt.Fprintf(w, "*%d"+CRLF, len(d))
		for _, v := range d {
			if err := renderReply(w, v); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknown type: %T", d)
	}
	return nil
}

type Null struct{}

var null Null

type Error struct {
	Typ     string
	Message string
}

type Array []interface{}

func splitCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, bCRLF); i >= 0 {
		// We have a full CRLF-terminated line.
		return i + 2, data[:i], nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), data, nil
	}
	// Request more data.
	return 0, nil, nil
}
