package backends

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

const (
	dbTypePostgres = "postgres"
	dbTypeMysql    = "mysql"
)

// sqlDB represents the sqlDB backend.
type sqlDB struct {
	db           *sql.DB
	dbType       string
	resultsTable string
	logger       *log.Logger

	// The result schemas (CREATE TABLE ...) are dynamically
	// generated everytime queries are executed based on their result columns.
	// They're cached here so as to avoid repetetive generation.
	resTableSchemas map[string]insertSchema
	schemaMutex     sync.RWMutex
}

// sqlDBWriter represents a writer that saves results
// to a sqlDB backend.
type sqlDBWriter struct {
	jobID       string
	taskName    string
	colsWritten bool
	cols        []string
	rows        [][]byte
	tx          *sql.Tx
	tbl         string

	backend *sqlDB
}

// insertSchema contains the generated SQL for creating tables
// and inserting rows.
type insertSchema struct {
	dropTable   string
	createTable string
	insertRow   string
}

// NewSQLBackend returns a new sqlDB result backend instance.
// It accepts an *sql.DB connection
func NewSQLBackend(db *sql.DB, dbType string, resTable string, l *log.Logger) (ResultBackend, error) {
	var (
		r = sqlDB{
			db:              db,
			dbType:          dbType,
			resTableSchemas: make(map[string]insertSchema),
			schemaMutex:     sync.RWMutex{},
			logger:          l,
		}
	)

	// Config.
	if resTable != "" {
		r.resultsTable = resTable
	} else {
		r.resultsTable = "results_%s"
	}

	return &r, nil
}

// NewResultSet returns a new instance of an sqlDB result writer.
// A new instance should be acquired for every individual job result
// to be written to the backend and then thrown away.
func (s *sqlDB) NewResultSet(jobID, taskName string, ttl time.Duration) (ResultSet, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	return &sqlDBWriter{
		jobID:    jobID,
		taskName: taskName,
		backend:  s,
		tbl:      fmt.Sprintf(s.resultsTable, jobID),
		tx:       tx,
	}, nil
}

// RegisterColTypes registers the column types of a particular taskName's result set.
// Internally, it translates sql types into the simpler sqlDB (SQLite 3) types,
// creates a CREATE TABLE() schema for the results table with the structure of the
// particular taskName, and caches it be used for every subsequent result db creation
// and population. This should only be called once for each kind of taskName.
func (w *sqlDBWriter) RegisterColTypes(cols []string, colTypes []*sql.ColumnType) error {
	if w.IsColTypesRegistered() {
		return errors.New("column types are already registered")
	}

	w.cols = make([]string, len(cols))
	copy(w.cols, cols)

	// Create the insert statement.
	// INSERT INTO xxx (col1, col2...) VALUES.
	var (
		colNameHolder = make([]string, len(cols))
		colValHolder  = make([]string, len(cols))
	)
	for i := range w.cols {
		colNameHolder[i] = fmt.Sprintf(`"%s"`, w.cols[i])

		// This will be filled by the driver.
		if w.backend.dbType == dbTypePostgres {
			// Postgres placeholders are $1, $2 ...
			colValHolder[i] = fmt.Sprintf("$%d", i+1)
		} else {
			colValHolder[i] = "?"
		}
	}

	ins := fmt.Sprintf(`INSERT INTO "%%s" (%s) `, strings.Join(colNameHolder, ","))
	ins += fmt.Sprintf("VALUES (%s)", strings.Join(colValHolder, ","))

	w.backend.schemaMutex.Lock()
	w.backend.resTableSchemas[w.taskName] = w.backend.createTableSchema(cols, colTypes)
	w.backend.schemaMutex.Unlock()

	return nil
}

// IsColTypesRegistered checks whether the column types for a particular taskName's
// structure is registered in the backend.
func (w *sqlDBWriter) IsColTypesRegistered() bool {
	w.backend.schemaMutex.RLock()
	_, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	return ok
}

// WriteCols writes the column (headers) of a result set to the backend.
// Internally, it creates a sqlDB database and creates a results table
// based on the schema RegisterColTypes() would've created and cached.
// This should only be called once on a ResultWriter instance.
func (w *sqlDBWriter) WriteCols(cols []string) error {
	if w.colsWritten {
		return fmt.Errorf("columns for '%s' are already written", w.taskName)
	}

	w.backend.schemaMutex.RLock()
	rSchema, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	if !ok {
		return fmt.Errorf("column types for '%s' have not been registered", w.taskName)
	}

	// Create the results table.
	tx, err := w.backend.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(fmt.Sprintf(rSchema.dropTable, w.tbl)); err != nil {
		return err
	}

	if _, err := tx.Exec(fmt.Sprintf(rSchema.createTable, w.tbl)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	return err
}

// WriteRow writes an individual row from a result set to the backend.
// Internally, it INSERT()s the given row into the sqlDB results table.
func (w *sqlDBWriter) WriteRow(row []interface{}) error {
	w.backend.schemaMutex.RLock()
	rSchema, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	if !ok {
		return fmt.Errorf("column types for '%s' have not been registered", w.taskName)
	}

	_, err := w.tx.Exec(fmt.Sprintf(rSchema.insertRow, w.tbl), row...)

	return err
}

// Flush flushes the rows written into the sqlDB pipe.
func (w *sqlDBWriter) Flush() error {
	err := w.tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

// Close closes the active sqlDB connection.
func (w *sqlDBWriter) Close() error {
	if w.tx != nil {
		return w.tx.Rollback()
	}

	return nil
}

// createTableSchema takes an SQL query results, gets its column names and types,
// and generates a sqlDB CREATE TABLE() schema for the results.
func (s *sqlDB) createTableSchema(cols []string, colTypes []*sql.ColumnType) insertSchema {
	var (
		colNameHolder = make([]string, len(cols))
		colValHolder  = make([]string, len(cols))
	)
	for i := range cols {
		colNameHolder[i] = fmt.Sprintf(`"%s"`, cols[i])

		// This will be filled by the driver.
		if s.dbType == dbTypePostgres {
			// Postgres placeholders are $1, $2 ...
			colValHolder[i] = fmt.Sprintf("$%d", i+1)
		} else {
			colValHolder[i] = "?"
		}
	}

	var (
		fields = make([]string, len(cols))
		typ    = ""
	)
	for i := 0; i < len(cols); i++ {
		typ = colTypes[i].DatabaseTypeName()
		switch colTypes[i].DatabaseTypeName() {
		case "INT2", "INT4", "INT8", // Postgres
			"TINYINT", "SMALLINT", "INT", "MEDIUMINT", "BIGINT": // MySQL
			typ = "BIGINT"
		case "FLOAT4", "FLOAT8", // Postgres
			"DECIMAL", "FLOAT", "DOUBLE", "NUMERIC": // MySQL
			typ = "DECIMAL"
		case "TIMESTAMP", // Postgres, MySQL
			"DATETIME": // MySQL
			typ = "TIMESTAMP"
		case "DATE": // Postgres, MySQL
			typ = "DATE"
		case "BOOLEAN": // Postgres, MySQL
			typ = "BOOLEAN"
		default:
			typ = "TEXT"
		}

		if nullable, ok := colTypes[i].Nullable(); ok && !nullable {
			typ += " NOT NULL"
		}

		fields[i] = fmt.Sprintf(`"%s" %s`, cols[i], typ)
	}

	return insertSchema{
		dropTable:   `DROP TABLE IF EXISTS "%s";`,
		createTable: fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%%s" (%s);`, strings.Join(fields, ",")),
		insertRow: fmt.Sprintf(`INSERT INTO "%%s" (%s) VALUES (%s)`, strings.Join(colNameHolder, ","),
			strings.Join(colValHolder, ",")),
	}
}
