// Package backends provides interfaces to write backends for
// SQL Jobber that take and store results from executed SQL jobs.
package backends

import (
	"database/sql"
	"log"
	"sync"
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
	RegisterColTypes([]string, []*sql.ColumnType) error
	CreateTableSchema(cols []string, colTypes []*sql.ColumnType) insertSchema
	IsColTypesRegistered() bool
	WriteCols([]string) error
	WriteRow([]interface{}) error
	Flush() error
	Close() error
}

const (
	DbTypePostgres = "postgres"
	DbTypeMysql    = "mysql"
	DbTypeOracle   = "oracle"
	DbTypeCsvq     = "csvq"
)

// Opt represents SQL DB backend's options.
type Opt struct {
	DBType         string
	ResultsTable   string
	UnloggedTables bool
}

// sqlDB represents the sqlDB backend.
type SqlDB struct {
	db     *sql.DB
	opt    Opt
	logger *log.Logger

	// The result schemas (CREATE TABLE ...) are dynamically
	// generated everytime queries are executed based on their result columns.
	// They're cached here so as to avoid repetetive generation.
	resTableSchemas map[string]insertSchema
	schemaMutex     sync.RWMutex
}

type Writer struct {
	jobID       string
	taskName    string
	colsWritten bool
	cols        []string
	rows        [][]byte
	tx          *sql.Tx
	tbl         string
}

// insertSchema contains the generated SQL for creating tables
// and inserting rows.
type insertSchema struct {
	dropTable   string
	createTable string
	insertRow   string
}
