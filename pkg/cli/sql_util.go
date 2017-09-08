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
// permissions and limitations under the License.

package cli

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"strings"
	"text/tabwriter"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type sqlConn struct {
	url          string
	config       pgx.ConnConfig
	conn         *pgx.Conn
	reconnecting bool

	// dbName is the last known current database, to be reconfigured in
	// case of automatic reconnects.
	dbName string

	serverVersion string // build.Info.Tag (short version, like 1.0.3)
	serverBuild   string // build.Info.Short (version, platform, etc summary)

	// clusterID and serverBuildInfo are the last known corresponding
	// values from the server, used to report any changes upon
	// (re)connects.
	clusterID           string
	clusterOrganization string
}

func (c *sqlConn) ensureConn() error {
	if c.conn == nil {
		if c.reconnecting && isInteractive {
			fmt.Fprintf(stderr, "connection lost; opening new connection: all session settings will be lost\n")
		}
		conn, err := pgx.Connect(c.config)
		if err != nil {
			return err
		}
		if c.reconnecting && c.dbName != "" {
			// Attempt to reset the current database.
			if _, err := conn.Exec(
				`SET DATABASE = `+parser.Name(c.dbName).String(), nil,
			); err != nil {
				fmt.Fprintf(stderr, "unable to restore current database: %v\n", err)
			}
		}
		c.conn = conn
		if err := c.checkServerMetadata(); err != nil {
			c.Close()
			return err
		}
		c.reconnecting = false
	}
	return nil
}

// checkServerMetadata reports the server version and cluster ID
// upon the initial connection or if either has changed since
// the last connection, based on the last known values in the sqlConn
// struct.
func (c *sqlConn) checkServerMetadata() error {
	if !isInteractive {
		// Version reporting is just noise in non-interactive sessions.
		return nil
	}

	newServerVersion := ""
	newClusterID := ""

	// Retrieve the node ID and server build info.
	rows, err := c.Query("SELECT * FROM crdb_internal.node_build_info", nil)
	if err == pgx.ErrDeadConn {
		return err
	}
	if err != nil {
		fmt.Fprintln(stderr, "unable to retrieve the server's version")
	} else {
		// Read the node_build_info table as an array of strings.
		rowVals, err := getAllRowStrings(rows, true /* showMoreChars */)
		if err != nil || len(rowVals) == 0 || len(rowVals[0]) != 3 {
			fmt.Fprintln(stderr, "error while retrieving the server's version")
			// It is not an error that the server version cannot be retrieved.
			return nil
		}

		// Extract the version fields from the query results.
		for _, row := range rowVals {
			switch row[1] {
			case "ClusterID":
				newClusterID = row[2]
			case "Version":
				newServerVersion = row[2]
			case "Build":
				c.serverBuild = row[2]
			case "Organization":
				c.clusterOrganization = row[2]
			}

		}
	}

	// Report the server version only if it the revision has been
	// fetched successfully, and the revision has changed since the last
	// connection.
	if newServerVersion != c.serverVersion {
		c.serverVersion = newServerVersion

		isSame := ""
		// We compare just the version (`build.Info.Tag`), whereas we *display* the
		// the full build summary (version, platform, etc) string
		// (`build.Info.Short()`). This is because we don't care if they're
		// different platforms/build tools/timestamps. The important bit exposed by
		// a version mismatch is the wire protocol and SQL dialect.
		if client := build.GetInfo(); c.serverVersion != client.Tag {
			fmt.Println("# Client version:", client.Short())
		} else {
			isSame = " (same version as client)"
		}
		fmt.Printf("# Server version: %s%s\n", c.serverBuild, isSame)
	}

	// Report the cluster ID only if it it could be fetched
	// successfully, and it has changed since the last connection.
	if old := c.clusterID; newClusterID != c.clusterID {
		c.clusterID = newClusterID
		if old != "" {
			return errors.Errorf("the cluster ID has changed!\nPrevious ID: %s\nNew ID: %s",
				old, newClusterID)
		}
		c.clusterID = newClusterID
		fmt.Println("# Cluster ID:", c.clusterID)
		if c.clusterOrganization != "" {
			fmt.Println("# Organization:", c.clusterOrganization)
		}
	}

	return nil
}

// getServerValue retrieves the first driverValue returned by the
// given sql query. If the query fails or does not return a single
// column, `false` is returned in the second result.
func (c *sqlConn) getServerValue(what, sql string) (string, bool) {
	var v string
	if err := c.QueryRow(sql).Scan(&v); err != nil {
		fmt.Fprintf(stderr, "error retrieving the %s: %v\n", what, err)
		return "", false
	}
	return v, true
}

// ExecTxn runs fn inside a transaction and retries it as needed.
// On non-retryable failures, the transaction is aborted and rolled
// back; on success, the transaction is committed.
//
// NOTE: the supplied closure should not have external side
// effects beyond changes to the database.
//
// NB: this code is cribbed from cockroach-go/crdb and has been copied
// because this code, pre-dating go1.8, deals with multiple result sets
// direct with the driver. See #14964.
func (c *sqlConn) ExecTxn(fn func(*sqlConn) error) (err error) {
	// Start a transaction.
	if err = c.Exec(`BEGIN`, nil); err != nil {
		return err
	}
	defer func() {
		if err == nil {
			// Ignore commit errors. The tx has already been committed by RELEASE.
			_ = c.Exec(`COMMIT`, nil)
		} else {
			// We always need to execute a Rollback() so sql.DB releases the
			// connection.
			_ = c.Exec(`ROLLBACK`, nil)
		}
	}()
	// Specify that we intend to retry this txn in case of CockroachDB retryable
	// errors.
	if err = c.Exec(`SAVEPOINT cockroach_restart`, nil); err != nil {
		return err
	}

	for {
		err = fn(c)
		if err == nil {
			// RELEASE acts like COMMIT in CockroachDB. We use it since it gives us an
			// opportunity to react to retryable errors, whereas tx.Commit() doesn't.
			if err = c.Exec(`RELEASE SAVEPOINT cockroach_restart`, nil); err == nil {
				return nil
			}
		}
		// We got an error; let's see if it's a retryable one and, if so, restart. We look
		// for either the standard PG errcode SerializationFailureError:40001 or the Cockroach extension
		// errcode RetriableError:CR000. The Cockroach extension has been removed server-side, but support
		// for it has been left here for now to maintain backwards compatibility.
		pqErr, ok := pgerror.GetPGCause(err)
		if retryable := ok && (pqErr.Code == "CR000" || pqErr.Code == "40001"); !retryable {
			return err
		}
		if err = c.Exec(`ROLLBACK TO SAVEPOINT cockroach_restart`, nil); err != nil {
			return err
		}
	}
}

func (c *sqlConn) BeginBatch() (*pgx.Batch, error) {
	if err := c.ensureConn(); err != nil {
		return nil, err
	}
	return c.conn.BeginBatch(), nil
}

func (c *sqlConn) Exec(query string, args ...interface{}) error {
	if err := c.ensureConn(); err != nil {
		return err
	}
	if sqlCtx.echo {
		fmt.Fprintln(stderr, ">", query)
	}
	_, err := c.conn.Exec(query, args...)
	if err == pgx.ErrDeadConn {
		c.reconnecting = true
		c.Close()
	}
	return err
}

func (c *sqlConn) Query(query string, args ...interface{}) (*pgx.Rows, error) {
	if err := c.ensureConn(); err != nil {
		return nil, err
	}
	if sqlCtx.echo {
		fmt.Fprintln(stderr, ">", query)
	}
	rows, err := c.conn.Query(query, args...)
	if err == pgx.ErrDeadConn {
		c.reconnecting = true
		c.Close()
	}
	if err != nil {
		return nil, err
	}
	return rows, nil
}

type Row struct {
	row *pgx.Row
	err error
}

func (r Row) Scan(dest ...interface{}) error {
	if r.err != nil {
		return r.err
	}
	return r.row.Scan(dest...)
}

func (c *sqlConn) QueryRow(query string, args ...interface{}) Row {
	if err := c.ensureConn(); err != nil {
		return Row{err: err}
	}
	row, err := c.conn.Query(query, args...)
	if err == pgx.ErrDeadConn {
		c.reconnecting = true
		c.Close()
	}
	if err != nil {
		return Row{err: err}
	}
	return Row{row: (*pgx.Row)(row)}
}

func (c *sqlConn) Close() {
	if c.conn != nil {
		err := c.conn.Close()
		if err != nil && err != pgx.ErrDeadConn {
			log.Info(context.TODO(), err)
		}
		c.conn = nil
	}
}

func makeSQLConn(url string) (*sqlConn, error) {
	config, err := pgx.ParseURI(url)
	if err != nil {
		return nil, err
	}
	if err := fixTLSConfig(url, &config); err != nil {
		return nil, err
	}
	return &sqlConn{
		url:    url,
		config: config,
	}, nil
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

// getPasswordAndMakeSQLClient prompts for a password if running in secure mode
// and no certificates have been supplied.
// Attempting to use security.RootUser without valid certificates will return an error.
func getPasswordAndMakeSQLClient() (*sqlConn, error) {
	if len(sqlConnURL) != 0 {
		return makeSQLConn(sqlConnURL)
	}
	var user *url.Userinfo
	if !baseCfg.Insecure && !baseCfg.ClientHasValidCerts(sqlConnUser) {
		if sqlConnUser == security.RootUser {
			return nil, errors.Errorf("connections with user %s must use a client certificate", security.RootUser)
		}

		pwd, err := security.PromptForPassword()
		if err != nil {
			return nil, err
		}

		user = url.UserPassword(sqlConnUser, pwd)
	} else {
		user = url.User(sqlConnUser)
	}
	return makeSQLClient(user)
}

func makeSQLClient(user *url.Userinfo) (*sqlConn, error) {
	sqlURL := sqlConnURL
	if len(sqlConnURL) == 0 {
		u, err := sqlCtx.PGURL(user)
		if err != nil {
			return nil, err
		}
		u.Path = sqlConnDBName
		sqlURL = u.String()
	}
	return makeSQLConn(sqlURL)
}

// runQuery takes a 'query' with optional 'parameters'.
// It runs the sql query and returns a list of columns names and a list of rows.
func runQuery(
	conn *sqlConn, showMoreChars bool, query string, params ...interface{},
) ([]string, [][]string, string, error) {
	rows, err := conn.Query(query, params...)
	if err != nil {
		return nil, nil, "", err
	}

	defer rows.Close()
	return sqlRowsToStrings(rows, showMoreChars)
}

// handleCopyError ensures the user is properly informed when they issue
// a COPY statement somewhere in their input.
func handleCopyError(conn *sqlConn, err error) error {
	if !strings.HasPrefix(err.Error(), "pq: unknown response for simple query: 'G'") {
		return err
	}

	// The COPY statement has hosed the connection by putting the
	// protocol in a state that lib/pq cannot understand any more. Reset
	// it.
	conn.Close()
	conn.reconnecting = true
	return errors.New("woops! COPY has confused this client! Suggestion: use 'psql' for COPY")
}

// runQueryAndFormatResults takes a query with parameters and writes the
// output to w.
func runQueryAndFormatResults(conn *sqlConn, w io.Writer, query string, params ...interface{}) error {
	stmts, err := parser.Parse(query)
	if err != nil {
		return err
	}
	if len(stmts) > 1 && len(params) != 0 {
		return errors.Errorf("params only supported with a single statement: %s", query)
	}
	b, err := conn.BeginBatch()
	if err != nil {
		return err
	}
	b.Queue(stmts[0].String(), params, nil, nil)
	for _, s := range stmts[1:] {
		b.Queue(s.String(), nil, nil, nil)
	}

	startTime := timeutil.Now()
	ctx := context.Background()
	if err := b.Send(ctx, nil); err != nil {
		return err
	}
	defer func() {
		_ = b.Close()
	}()

Loop:
	for {
		msg, err := b.Next()
		if err != nil {
			return err
		}
		switch msg := msg.(type) {
		case pgx.CommandTag:
			if err := printQueryOutput(w, nil, nil, string(msg)); err != nil {
				return err
			}
		case *pgx.Rows:
			cols := getColumnStrings(msg)
			if err := printQueryOutput(w, cols, newRowIter(msg, true), ""); err != nil {
				return err
			}
		case nil:
			break Loop
		}
	}
	if cliCtx.showTimes {
		// Present the time since the last result, or since the
		// beginning of execution. Currently the execution engine makes
		// all the work upfront so most of the time is accounted for by
		// the 1st result; this is subject to change once CockroachDB
		// evolves to stream results as statements are executed.
		newNow := timeutil.Now()
		fmt.Fprintf(w, "\nTime: %s\n\n", newNow.Sub(startTime))
		startTime = newNow
	}
	return nil
}

// sqlRowsToStrings turns 'rows' into a list of rows, each of which
// is a  list of column values.
// 'rows' should be closed by the caller.
// It returns the header row followed by all data rows.
// If both the header row and list of rows are empty, it means no row
// information was returned (eg: statement was not a query).
// If showMoreChars is true, then more characters are not escaped.
func sqlRowsToStrings(rows *pgx.Rows, showMoreChars bool) ([]string, [][]string, string, error) {
	cols := getColumnStrings(rows)
	allRows, err := getAllRowStrings(rows, showMoreChars)
	if err != nil {
		return nil, nil, "", err
	}
	//tag := getFormattedTag(rows.Tag(), rows.Result())
	tag := "TODO"

	return cols, allRows, tag, nil
}

func getColumnStrings(rows *pgx.Rows) []string {
	srcCols := rows.FieldDescriptions()
	cols := make([]string, len(srcCols))
	for i, c := range srcCols {
		cols[i] = formatVal(c.Name, true, false)
	}
	return cols
}

func getAllRowStrings(rows *pgx.Rows, showMoreChars bool) ([][]string, error) {
	var allRows [][]string

	for {
		rowStrings, err := getNextRowStrings(rows, showMoreChars)
		if err != nil {
			return nil, err
		}
		if rowStrings == nil {
			break
		}
		allRows = append(allRows, rowStrings)
	}

	return allRows, nil
}

func getNextRowStrings(rows *pgx.Rows, showMoreChars bool) ([]string, error) {
	if !rows.Next() {
		return nil, rows.Err()
	}
	vals, err := rows.Values()
	if err != nil {
		return nil, err
	}

	rowStrings := make([]string, len(vals))
	for i, v := range vals {
		rowStrings[i] = formatVal(v, showMoreChars, showMoreChars)
	}
	return rowStrings, nil
}

// expandTabsAndNewLines ensures that multi-line row strings that may
// contain tabs are properly formatted: tabs are expanded to spaces,
// and newline characters are marked visually. Marking newline
// characters is especially important in single-column results where
// the underlying TableWriter would not otherwise show the difference
// between one multi-line row and two one-line rows.
func expandTabsAndNewLines(s string) string {
	var buf bytes.Buffer
	// 4-wide columns, 1 character minimum width.
	w := tabwriter.NewWriter(&buf, 4, 0, 1, ' ', 0)
	fmt.Fprint(w, strings.Replace(s, "\n", "␤\n", -1))
	_ = w.Flush()
	return buf.String()
}

func isNotPrintableASCII(r rune) bool { return r < 0x20 || r > 0x7e || r == '"' || r == '\\' }
func isNotGraphicUnicode(r rune) bool { return !unicode.IsGraphic(r) }
func isNotGraphicUnicodeOrTabOrNewline(r rune) bool {
	return r != '\t' && r != '\n' && !unicode.IsGraphic(r)
}

func formatVal(val interface{}, showPrintableUnicode bool, showNewLinesAndTabs bool) string {
	switch t := val.(type) {
	case nil:
		return "NULL"
	case string:
		if showPrintableUnicode {
			pred := isNotGraphicUnicode
			if showNewLinesAndTabs {
				pred = isNotGraphicUnicodeOrTabOrNewline
			}
			if utf8.ValidString(t) && strings.IndexFunc(t, pred) == -1 {
				return t
			}
		} else {
			if strings.IndexFunc(t, isNotPrintableASCII) == -1 {
				return t
			}
		}
		return fmt.Sprintf("%+q", t)

	case []byte:
		if showPrintableUnicode {
			pred := isNotGraphicUnicode
			if showNewLinesAndTabs {
				pred = isNotGraphicUnicodeOrTabOrNewline
			}
			if utf8.Valid(t) && bytes.IndexFunc(t, pred) == -1 {
				return string(t)
			}
		} else {
			if bytes.IndexFunc(t, isNotPrintableASCII) == -1 {
				return string(t)
			}
		}
		return fmt.Sprintf("%+q", t)

	case time.Time:
		return t.Format(parser.TimestampOutputFormat)
	}

	return fmt.Sprint(val)
}
