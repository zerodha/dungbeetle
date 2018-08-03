// Package backends provides interfaces to write backends for
// SQL Jobber that take and store results from executed SQL jobs.
package backends

import (
	"database/sql"
	"strings"
	"time"
)

// ResultBackend represents a results backend to which results
// from an executed SQL job are written.
type ResultBackend interface {
	NewResultSet(dbName, taskName string, ttl time.Duration) ResultSet
}

// ResultSet represents the set of results from an individual
// job that's executed.
type ResultSet interface {
	RegisterColTypes([]string, []*sql.ColumnType) error
	IsColTypesRegistered() bool
	WriteCols([]string) error
	WriteRow([][]byte) error
	Flush() error
	Close() error
}

// createTableSchema takes an SQL query results, gets its column names and types,
// and generates a rediSQL CREATE TABLE() schema for the results.
func createTableSchema(cols []string, colTypes []*sql.ColumnType) string {
	var (
		fields = make([]string, len(cols))
		typ    = ""
	)
	for i := 0; i < len(cols); i++ {
		switch colTypes[i].DatabaseTypeName() {
		case "INT2", "INT4", "INT8", // Postgres
			"TINYINT", "SMALLINT", "INT", "MEDIUMINT", "BIGINT": // MySQL
			typ = "INTEGER"
		case "FLOAT4", "FLOAT8", // Postgres
			"DECIMAL", "FLOAT", "DOUBLE", "NUMERIC": // MySQL
			typ = "REAL"
		default:
			typ = "TEXT"
		}

		if nullable, ok := colTypes[i].Nullable(); ok && !nullable {
			typ += " NOT NULL"
		}

		fields[i] = "`" + cols[i] + "`" + " " + typ
	}

	return "DROP TABLE IF EXISTS `%s`; CREATE TABLE IF NOT EXISTS `%s` (" + strings.Join(fields, ",") + ");"
}
