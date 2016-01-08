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

package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/redis"
	"github.com/cockroachdb/cockroach/redis/driver"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
)

// Server implements the server side of the Redis wire protocol.
type Server struct {
	context  *Context
	listener net.Listener
	mu       sync.Mutex // Mutex protects the fields below
	conns    map[net.Conn]struct{}
	closing  bool
}

// Context holds parameters needed to setup a redis-compatible server.
type Context struct {
	*base.Context
	Executor *redis.Executor
	Stopper  *stop.Stopper
}

// NewServer creates a Server.
func NewServer(context *Context) *Server {
	return &Server{
		context: context,
		conns:   make(map[net.Conn]struct{}),
	}
}

// Start a server on the given address.
func (s *Server) Start(addr net.Addr) error {
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		return err
	}
	s.listener = ln

	s.context.Stopper.RunWorker(func() {
		s.serve(ln)
	})

	s.context.Stopper.RunWorker(func() {
		<-s.context.Stopper.ShouldStop()
		s.close()
	})
	return nil
}

// Addr returns this Server's address.
func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

// serve connections on this listener until it is closed.
func (s *Server) serve(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			if !s.isClosing() {
				log.Error(err)
			}
			return
		}

		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		go func() {
			defer func() {
				s.mu.Lock()
				delete(s.conns, conn)
				s.mu.Unlock()
				conn.Close()
			}()

			if err := s.serveConn(conn); err != nil {
				if !s.isClosing() {
					log.Error(err)
				}
			}
		}()
	}
}

func (s *Server) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

// close this server, and all client connections.
func (s *Server) close() {
	s.listener.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closing = true
	for conn := range s.conns {
		conn.Close()
	}
}

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

func (s *Server) serveConn(conn net.Conn) error {
	scanner := bufio.NewScanner(conn)
	scanner.Split(splitCRLF)
	for scanner.Scan() {
		b := scanner.Bytes()
		if len(b) == 0 {
			return fmt.Errorf("empty entry")
		}
		if b[0] != '*' {
			return fmt.Errorf("expected array")
		}
		arlen, err := strconv.ParseInt(string(b[1:]), 10, 64)
		if err != nil {
			return fmt.Errorf("bad array length")
		}
		args := make([]string, arlen)
		for i := int64(0); i < arlen; i++ {
			if !scanner.Scan() {
				return fmt.Errorf("expected more elements in array")
			}
			b := scanner.Bytes()
			if len(b) == 0 {
				return fmt.Errorf("empty entry")
			}
			if b[0] != '$' {
				return fmt.Errorf("expected string")
			}
			tlen, err := strconv.ParseInt(string(b[1:]), 10, 64)
			if err != nil {
				return fmt.Errorf("bad string length")
			}
			for b = nil; int64(len(b)) < tlen; {
				if !scanner.Scan() {
					return fmt.Errorf("expected more elements in array")
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
			return fmt.Errorf("no command")
		}
		c := driver.Command{
			Command:   args[0],
			Arguments: args[1:],
		}
		resp, _, _ := s.context.Executor.Execute(c)
		if err := renderReply(conn, &resp.Response); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan error: %s", err)
	}
	return nil
}

func respError(format string, a ...interface{}) []byte {
	return []byte("-" + fmt.Sprintf(format, a...))
}

func renderReply(w io.Writer, d *driver.Datum) error {
	switch d := d.Payload.(type) {
	case *driver.Datum_IntVal:
		fmt.Fprintf(w, ":%d"+CRLF, d.IntVal)
	case *driver.Datum_StringVal:
		fmt.Fprintf(w, "+%s"+CRLF, d.StringVal)
	case *driver.Datum_ByteVal:
		fmt.Fprintf(w, "$%d"+CRLF, len(d.ByteVal))
		w.Write([]byte(d.ByteVal))
		w.Write(bCRLF)
	case *driver.Datum_NullVal:
		w.Write([]byte("$-1" + CRLF))
	case *driver.Datum_ErrorVal:
		fmt.Fprintf(w, "-%s %s"+CRLF, d.ErrorVal.Typ, d.ErrorVal.Message)
	case *driver.Datum_ArrayVal:
		fmt.Fprintf(w, "*%d"+CRLF, len(d.ArrayVal.Values))
		for _, v := range d.ArrayVal.Values {
			if err := renderReply(w, v); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unknown type: %T", d)
	}
	return nil
}
