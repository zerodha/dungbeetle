// Package backends provides interfaces to write backends for
// SQL Jobber that take and store results from executed SQL jobs.
package backends

import (
	"database/sql"
	"time"
)

// ResultBackend represents a result backend to which results
// from an executed SQL job are written.
type ResultBackend interface {
	NewResultSet(dbName, taskName string, ttl time.Duration) (ResultSet, error)
}

// ResultSet represents the set of results from an individual
// job that's executed.
type ResultSet interface {
	RegisterColTypes([]string, []*sql.ColumnType, int) error
	IsColTypesRegistered() bool
	WriteCols([]string) error
	WriteRow([]interface{}) error
	WriteRowBulk([]interface{}) error
	Flush() error
	Close() error
}
