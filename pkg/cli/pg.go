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
// permissions and limitations under the License.

package cli

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os/user"
	"time"

	"github.com/jackc/pgx"
	"github.com/jackc/pgx/pgproto3"
	"github.com/pkg/errors"
)

type pgconn struct {
	uri      string
	config   pgx.ConnConfig
	c        net.Conn
	frontend *pgproto3.Frontend

	txStatus byte
}

func newPGConn(uri string) (*pgconn, error) {
	println("URL", uri)
	config, err := pgx.ParseURI(uri)
	if err != nil {
		return nil, err
	}
	if err := fixTLSConfig(uri, &config); err != nil {
		return nil, err
	}

	pg := pgconn{
		uri:    uri,
		config: config,
	}
	if pg.config.User == "" {
		user, err := user.Current()
		if err != nil {
			return nil, err
		}
		pg.config.User = user.Username
	}
	if pg.config.Port == 0 {
		pg.config.Port = 26257
	}
	pg.config.Dial = (&net.Dialer{KeepAlive: 5 * time.Minute}).Dial
	return &pg, nil
}

func fixTLSConfig(uri string, c *pgx.ConnConfig) error {
	url, err := url.Parse(uri)
	if err != nil {
		return err
	}
	if sslcert, sslkey := url.Query().Get("sslcert"), url.Query().Get("sslkey"); sslcert != "" && sslkey != "" {
		cert, err := tls.LoadX509KeyPair(sslcert, sslkey)
		if err != nil {
			return err
		}
		c.TLSConfig.Certificates = []tls.Certificate{cert}
	}
	if sslrootcert := url.Query().Get("sslrootcert"); sslrootcert != "" {
		c.TLSConfig.RootCAs = x509.NewCertPool()
		cert, err := ioutil.ReadFile(sslrootcert)
		if err != nil {
			return err
		}
		if !c.TLSConfig.RootCAs.AppendCertsFromPEM(cert) {
			return errors.New("couldn't parse pem in sslrootcert")
		}
	}
	return nil
}

func (pg *pgconn) Close() {
	if pg.c != nil {
		// TODO(mjibson): send PG close message
		pg.c.Close()
		pg.c = nil
	}
}

func (pg *pgconn) ensureConnected() error {
	if pg.c != nil {
		return nil
	}
	return pg.connect()
}

func (pg *pgconn) connect() error {
	var err error
	address := fmt.Sprintf("%s:%d", pg.config.Host, pg.config.Port)
	pg.c, err = pg.config.Dial("tcp", address)
	if err != nil {
		return err
	}
	if pg.config.TLSConfig != nil {
		if err := pg.startTLS(pg.config.TLSConfig); err != nil {
			return err
		}
	}
	pg.frontend, err = pgproto3.NewFrontend(pg.c, pg.c)
	if err != nil {
		return err
	}
	startupMsg := pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      make(map[string]string),
	}
	startupMsg.Parameters["user"] = pg.config.User
	if pg.config.Database != "" {
		startupMsg.Parameters["database"] = pg.config.Database
	}
	if _, err := pg.c.Write(startupMsg.Encode(nil)); err != nil {
		return err
	}

	for {
		msg, err := pg.frontend.Receive()
		if err != nil {
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			pg.rxReadyForQuery(msg)
			return nil
		default:
			return errors.Errorf("unexpected message: %T", msg)
		}
	}
}

func (pg *pgconn) startTLS(tlsconfig *tls.Config) error {
	const SSLRequest = 80877103
	if err := binary.Write(pg.c, binary.BigEndian, []int32{8, SSLRequest}); err != nil {
		return err
	}
	response := make([]byte, 1)
	if _, err := io.ReadFull(pg.c, response); err != nil {
		return err
	}
	if response[0] != 'S' {
		return pgx.ErrTLSRefused
	}
	pg.c = tls.Client(pg.c, tlsconfig)
	return nil
}

func (pg *pgconn) rxReadyForQuery(msg *pgproto3.ReadyForQuery) {
	pg.txStatus = msg.TxStatus
}

func (pg *pgconn) Exec(s string) error {
	return nil
}

func (pg *pgconn) QueryRow(s string) ([]string, error) {
	return nil, nil
}

// getServerValue retrieves the first driverValue returned by the
// given sql query. If the query fails or does not return a single
// column, `false` is returned in the second result.
func (pg *pgconn) getServerValue(what, sql string) (string, bool) {
	row, err := pg.QueryRow(sql)
	if err != nil {
		fmt.Fprintf(stderr, "error retrieving the %s: %v\n", what, err)
		return "", false
	}

	if len(row) != 1 {
		fmt.Fprintf(stderr, "cannot get the %s\n", what)
		return "", false
	}

	return row[0], true
}

func (pg *pgconn) Run(w io.Writer, s string) error {
	if err := pg.ensureConnected(); err != nil {
		return err
	}
	//startTime := timeutil.Now()
	if err := pg.frontend.Send(&pgproto3.Query{String: s}); err != nil {
		return err
	}
	for {
		msg, err := pg.frontend.Receive()
		if err != nil {
			return err
		}

		switch msg := msg.(type) {
		case *pgproto3.ReadyForQuery:
			pg.rxReadyForQuery(msg)
			return nil
		default:
			return errors.Errorf("unexpected message: %T", msg)
		}
	}
}
