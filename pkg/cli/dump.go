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
// permissions and limitations under the License.

package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// dumpCmd dumps SQL tables.
var dumpCmd = &cobra.Command{
	Use:   "dump [options] <database> [<table> [<table>...]]",
	Short: "dump sql tables\n",
	Long: `
Dump SQL tables of a cockroach database. If the table name
is omitted, dump all tables in the database.
`,
	RunE: MaybeDecorateGRPCError(runDump),
}

func runDump(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		return usageAndError(cmd)
	}

	conn, err := getPasswordAndMakeSQLClient()
	if err != nil {
		return err
	}
	defer conn.Close()

	dbName := args[0]
	var tableNames []string
	if len(args) > 1 {
		tableNames = args[1:]
	}

	mds, ts, err := getDumpMetadata(conn, dbName, tableNames, dumpCtx.asOf)
	if err != nil {
		return err
	}

	byID := make(map[int64]tableMetadata)
	for _, md := range mds {
		byID[md.ID] = md
	}

	// First sort by name to guarantee stable output.
	sort.Slice(mds, func(i, j int) bool {
		return mds[i].name.String() < mds[j].name.String()
	})

	// Collect transitive dependencies in topological order into collected.
	var collected []int64
	seen := make(map[int64]bool)
	for _, md := range mds {
		collect(md.ID, byID, seen, &collected)
	}
	// collectOrder maps a table ID to its collection index. This is needed
	// instead of just using range over collected because collected may contain
	// table IDs not present in the dump spec. It is simpler to sort mds correctly
	// to skip over these referenced-but-not-dumped tables.
	collectOrder := make(map[int64]int)
	for i, id := range collected {
		collectOrder[id] = i
	}

	// Second sort dumped tables by dependency order.
	sort.SliceStable(mds, func(i, j int) bool {
		return collectOrder[mds[i].ID] < collectOrder[mds[j].ID]
	})

	w := os.Stdout

	if dumpCtx.dumpMode != dumpDataOnly {
		for i, md := range mds {
			if i > 0 {
				fmt.Fprintln(w)
			}
			if err := dumpCreateTable(w, md); err != nil {
				return err
			}
		}
	}
	if dumpCtx.dumpMode != dumpSchemaOnly {
		for _, md := range mds {
			if md.isView {
				continue
			}
			if err := dumpTableData(w, conn, ts, md); err != nil {
				return err
			}
		}
	}
	return nil
}

func collect(tid int64, byID map[int64]tableMetadata, seen map[int64]bool, collected *[]int64) {
	// has this table already been collected previously?
	if seen[tid] {
		return
	}
	// no: add it
	for _, dep := range byID[tid].dependsOn {
		// depth-first collection of dependencies
		collect(dep, byID, seen, collected)
	}
	*collected = append(*collected, tid)
	seen[tid] = true
}

// tableMetadata describes one table to dump.
type tableMetadata struct {
	ID           int64
	name         *parser.TableName
	numIndexCols int
	idxColNames  string
	columnNames  string
	columnTypes  map[string]string
	createStmt   string
	dependsOn    []int64
	isView       bool
}

// getDumpMetadata retrieves the table information for the specified table(s).
// It also retrieves the cluster timestamp at which the metadata was
// retrieved.
func getDumpMetadata(
	conn *sqlConn, dbName string, tableNames []string, asOf string,
) (mds []tableMetadata, clusterTS string, err error) {
	if asOf == "" {
		if err := conn.QueryRow("SELECT cluster_logical_timestamp()").Scan(&clusterTS); err != nil {
			return nil, "", err
		}
	} else {
		// Validate the timestamp. This prevents SQL injection.
		if _, err := parser.ParseDTimestamp(asOf, time.Nanosecond); err != nil {
			return nil, "", err
		}
		clusterTS = asOf
	}

	if tableNames == nil {
		tableNames, err = getTableNames(conn, dbName, clusterTS)
		if err != nil {
			return nil, "", err
		}
	}

	mds = make([]tableMetadata, len(tableNames))
	for i, tableName := range tableNames {
		md, err := getMetadataForTable(conn, dbName, tableName, clusterTS)
		if err != nil {
			return nil, "", err
		}
		mds[i] = md
	}

	return mds, clusterTS, nil
}

// getTableNames retrieves all tables names in the given database.
func getTableNames(conn *sqlConn, dbName string, ts string) (tableNames []string, err error) {
	rows, err := conn.Query(fmt.Sprintf(`
		SELECT TABLE_NAME
		FROM "".information_schema.tables
		AS OF SYSTEM TIME '%s'
		WHERE TABLE_SCHEMA = $1
		`, ts), dbName)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		tableNames = append(tableNames, name)
	}
	return tableNames, rows.Err()
}

func getMetadataForTable(
	conn *sqlConn, dbName, tableName string, ts string,
) (tableMetadata, error) {
	name := &parser.TableName{DatabaseName: parser.Name(dbName), TableName: parser.Name(tableName)}

	// Fetch table ID.
	var tableID int64
	if err := conn.QueryRow(fmt.Sprintf(`
		SELECT table_id
		FROM %s.crdb_internal.tables
		AS OF SYSTEM TIME '%s'
		WHERE DATABASE_NAME = $1
			AND NAME = $2
		`, parser.Name(dbName).String(), ts), dbName, tableName).Scan(&tableID); err != nil {
		if err == io.EOF {
			return tableMetadata{}, errors.Errorf("relation %s does not exist", name)
		}
		return tableMetadata{}, err
	}

	// Fetch column types.
	rows, err := conn.Query(fmt.Sprintf(`
		SELECT COLUMN_NAME, DATA_TYPE
		FROM "".information_schema.columns
		AS OF SYSTEM TIME '%s'
		WHERE TABLE_SCHEMA = $1
			AND TABLE_NAME = $2
		`, ts), dbName, tableName)
	if err != nil {
		return tableMetadata{}, err
	}
	coltypes := make(map[string]string)
	var colnames bytes.Buffer
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return tableMetadata{}, err
		}
		coltypes[name] = typ
		if colnames.Len() > 0 {
			colnames.WriteString(", ")
		}
		parser.FormatNode(&colnames, parser.FmtSimple, parser.Name(name))
	}
	if err := rows.Err(); err != nil {
		return tableMetadata{}, err
	}

	rows, err = conn.Query(fmt.Sprintf(`
		SELECT COLUMN_NAME
		FROM "".information_schema.key_column_usage
		AS OF SYSTEM TIME '%s'
		WHERE TABLE_SCHEMA = $1
			AND TABLE_NAME = $2
			AND CONSTRAINT_NAME = $3
		ORDER BY ORDINAL_POSITION
		`, ts), dbName, tableName, sqlbase.PrimaryKeyIndexName)
	if err != nil {
		return tableMetadata{}, err
	}

	var numIndexCols int
	var idxColNames bytes.Buffer
	// Find the primary index columns.
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return tableMetadata{}, err
		}
		if idxColNames.Len() > 0 {
			idxColNames.WriteString(", ")
		}
		parser.FormatNode(&idxColNames, parser.FmtSimple, parser.Name(name))
		numIndexCols++
	}
	if err := rows.Err(); err != nil {
		return tableMetadata{}, err
	}

	var create string
	var descType bool
	if err := conn.QueryRow(fmt.Sprintf(`
		SELECT create_statement, descriptor_type = 'view'
		FROM %s.crdb_internal.create_statements
		AS OF SYSTEM TIME '%s'
		WHERE descriptor_name = $1
			AND database_name = $2
		`, parser.Name(dbName).String(), ts), tableName, dbName).Scan(&create, &descType); err != nil {
		return tableMetadata{}, err
	}

	rows, err = conn.Query(fmt.Sprintf(`
		SELECT dependson_id
		FROM %s.crdb_internal.backward_dependencies
		AS OF SYSTEM TIME '%s'
		WHERE descriptor_id = $1
		`, parser.Name(dbName).String(), ts), tableID)
	if err != nil {
		return tableMetadata{}, err
	}

	var refs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return tableMetadata{}, err
		}
		refs = append(refs, id)
	}
	if err := rows.Err(); err != nil {
		return tableMetadata{}, err
	}

	return tableMetadata{
		ID:           tableID,
		name:         name,
		numIndexCols: numIndexCols,
		idxColNames:  idxColNames.String(),
		columnNames:  colnames.String(),
		columnTypes:  coltypes,
		createStmt:   create,
		dependsOn:    refs,
		isView:       descType,
	}, nil
}

// dumpCreateTable dumps the CREATE statement of the specified table to w.
func dumpCreateTable(w io.Writer, md tableMetadata) error {
	if _, err := w.Write([]byte(md.createStmt)); err != nil {
		return err
	}
	if _, err := w.Write([]byte(";\n")); err != nil {
		return err
	}
	return nil
}

const (
	// limit is the number of rows to dump at a time (in each SELECT statement).
	limit = 10000
	// insertRows is the number of rows per INSERT statement.
	insertRows = 100
)

// dumpTableData dumps the data of the specified table to w.
func dumpTableData(w io.Writer, conn *sqlConn, clusterTS string, md tableMetadata) error {
	// Build the SELECT query.
	var sbuf bytes.Buffer
	if md.idxColNames == "" {
		// TODO(mjibson): remove hard coded rowid. Maybe create a crdb_internal
		// table with the information we need instead.
		md.idxColNames = "rowid"
		md.numIndexCols = 1
	}
	fmt.Fprintf(&sbuf, "SELECT %s, %s FROM %s", md.idxColNames, md.columnNames, md.name)
	fmt.Fprintf(&sbuf, " AS OF SYSTEM TIME '%s'", clusterTS)

	var wbuf bytes.Buffer
	fmt.Fprintf(&wbuf, " WHERE ROW (%s) > ROW (", md.idxColNames)
	for i := 0; i < md.numIndexCols; i++ {
		if i > 0 {
			wbuf.WriteString(", ")
		}
		fmt.Fprintf(&wbuf, "$%d", i+1)
	}
	wbuf.WriteString(")")
	// No WHERE clause first time, so add a place to inject it.
	fmt.Fprintf(&sbuf, "%%s ORDER BY PRIMARY KEY %s LIMIT %d", md.name, limit)
	bs := sbuf.String()

	// pk holds the last values of the fetched primary keys
	var pk []interface{}
	q := fmt.Sprintf(bs, "")
	inserts := make([]string, 0, insertRows)
	for {
		rows, err := conn.Query(q, pk)
		if err != nil {
			return err
		}
		cols := rows.FieldDescriptions()
		pkcols := cols[:md.numIndexCols]
		cols = cols[md.numIndexCols:]
		i := 0
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				return err
			}
			if pk == nil {
				q = fmt.Sprintf(bs, wbuf.String())
			}
			pk = vals[:md.numIndexCols]
			vals = vals[md.numIndexCols:]
			var ivals bytes.Buffer
			// Values need to be correctly encoded for INSERT statements in a text file.
			for si, sv := range vals {
				if si > 0 {
					ivals.WriteString(", ")
				}
				var d parser.Datum
				ct := md.columnTypes[cols[si].Name]
				switch t := sv.(type) {
				case nil:
					d = parser.DNull
				case bool:
					d = parser.MakeDBool(parser.DBool(t))
				case int64:
					d = parser.NewDInt(parser.DInt(t))
				case float64:
					d = parser.NewDFloat(parser.DFloat(t))
				case string:
					d = parser.NewDString(t)
				case []byte:
					switch ct {
					case "INTERVAL":
						d, err = parser.ParseDInterval(string(t))
						if err != nil {
							panic(err)
						}
					case "BYTES":
						d = parser.NewDBytes(parser.DBytes(t))
					default:
						// STRING and DECIMAL types can have optional length
						// suffixes, so only examine the prefix of the type.
						if strings.HasPrefix(ct, "STRING") {
							d = parser.NewDString(string(t))
						} else if strings.HasPrefix(ct, "DECIMAL") {
							d, err = parser.ParseDDecimal(string(t))
							if err != nil {
								panic(err)
							}
						} else {
							panic(errors.Errorf("unknown []byte type: %s, %v: %s", t, cols[si], ct))
						}
					}
				case time.Time:
					switch ct {
					case "DATE":
						d = parser.NewDDateFromTime(t, time.UTC)
					case "TIMESTAMP":
						d = parser.MakeDTimestamp(t, time.Nanosecond)
					case "TIMESTAMP WITH TIME ZONE":
						d = parser.MakeDTimestampTZ(t, time.Nanosecond)
					default:
						panic(errors.Errorf("unknown timestamp type: %s, %v: %s", t, cols[si], ct))
					}
				default:
					panic(errors.Errorf("unknown field type: %T (%s)", t, cols[si]))
				}
				d.Format(&ivals, parser.FmtParsable)
			}
			inserts = append(inserts, ivals.String())
			i++
			if len(inserts) == cap(inserts) {
				writeInserts(w, md, inserts)
				inserts = inserts[:0]
			}
		}
		for si, sv := range pk {
			b, ok := sv.([]byte)
			if ok && strings.HasPrefix(md.columnTypes[pkcols[si].Name], "STRING") {
				// Primary key strings need to be converted to a go string, but not SQL
				// encoded since they aren't being written to a text file.
				pk[si] = string(b)
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(inserts) != 0 {
			writeInserts(w, md, inserts)
			inserts = inserts[:0]
		}
		if i < limit {
			break
		}
	}
	return nil
}

func writeInserts(w io.Writer, md tableMetadata, inserts []string) {
	fmt.Fprintf(w, "\nINSERT INTO %s (%s) VALUES", md.name.TableName, md.columnNames)
	for idx, values := range inserts {
		if idx > 0 {
			fmt.Fprint(w, ",")
		}
		fmt.Fprintf(w, "\n\t(%s)", values)
	}
	fmt.Fprintln(w, ";")
}
