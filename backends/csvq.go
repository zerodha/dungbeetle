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

// csvqDB represents the csvqDB backend.
type csvqDB struct {
	SqlDB
}

// csvqDBWriter represents a writer that saves results
// to a csvqDB backend.
type csvqDBWriter struct {
	sqlDBWriter
}

// NewSQLBackend returns a new csvqDB result backend instance.
// It accepts an *sql.DB connection
func NewCsvqBackend(db *sql.DB, opt Opt, l *log.Logger) (ResultBackend, error) {

	s := csvqDB{
		SqlDB{
			db:              db,
			opt:             opt,
			resTableSchemas: make(map[string]insertSchema),
			schemaMutex:     sync.RWMutex{},
			logger:          l,
		},
	}

	// Config.
	if opt.ResultsTable != "" {
		s.opt.ResultsTable = opt.ResultsTable
	} else {
		s.opt.ResultsTable = "results_%s"
	}

	return &s, nil
}

// NewResultSet returns a new instance of an csvqDB result writer.
// A new instance should be acquired for every individual job result
// to be written to the backend and then thrown away.
func (s *csvqDB) NewResultSet(jobID, taskName string, ttl time.Duration) (ResultSet, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}

	return &csvqDBWriter{
		sqlDBWriter{
			Writer{
				jobID:    jobID,
				taskName: taskName,
				tbl:      fmt.Sprintf(s.opt.ResultsTable, jobID),
				tx:       tx,
			},
			s.SqlDB,
		},
	}, nil
}

// RegisterColTypes registers the column types of a particular taskName's result set.
// Internally, it translates sql types into the simpler sqlDB (SQLite 3) types,
// creates a CREATE TABLE() schema for the results table with the structure of the
// particular taskName, and caches it be used for every subsequent result db creation
// and population. This should only be called once for each kind of taskName.
func (w *csvqDBWriter) RegisterColTypes(cols []string, colTypes []*sql.ColumnType) error {
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
		if w.opt.DBType == DbTypePostgres {
			// Postgres placeholders are $1, $2 ...
			colValHolder[i] = fmt.Sprintf("$%d", i+1)
		} else {
			colValHolder[i] = "?"
		}
	}

	ins := fmt.Sprintf(`INSERT INTO "%%s" (%s) `, strings.Join(colNameHolder, ","))
	ins += fmt.Sprintf("VALUES (%s)", strings.Join(colValHolder, ","))

	// TODO Need extract to other method
	w.schemaMutex.Lock()
	w.resTableSchemas[w.taskName] = w.CreateTableSchema(cols, colTypes)
	w.schemaMutex.Unlock()

	return nil
}

// WriteCols writes the column (headers) of a result set to the backend.
// Internally, it creates a csvqDB database and creates a results table
// based on the schema RegisterColTypes() would've created and cached.
// This should only be called once on a ResultWriter instance.
func (w *csvqDBWriter) WriteCols(cols []string) error {
	if w.colsWritten {
		return fmt.Errorf("columns for '%s' are already written", w.taskName)
	}

	w.schemaMutex.RLock()
	rSchema, ok := w.resTableSchemas[w.taskName]
	w.schemaMutex.RUnlock()

	if !ok {
		return fmt.Errorf("column types for '%s' have not been registered", w.taskName)
	}

	// Create the results table.
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// TODO - This should configurable... maybe in some case not supported or need incremental update..
	// if _, err := tx.Exec(fmt.Sprintf(rSchema.dropTable, w.tbl)); err != nil {
	// 	return err
	// }

	if _, err := tx.Exec(fmt.Sprintf(rSchema.createTable, w.tbl)); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	return err
}

// createTableSchema takes an SQL query results, gets its column names and types,
// and generates a csvqDB CREATE TABLE() schema for the results.
func (s *csvqDBWriter) CreateTableSchema(cols []string, colTypes []*sql.ColumnType) insertSchema {
	var (
		colNameHolder = make([]string, len(cols))
		colValHolder  = make([]string, len(cols))
	)

	for i := range cols {
		// TODO - In some dialects double quote not supported...
		colNameHolder[i] = fmt.Sprintf(`%s`, cols[i])

		// This will be filled by the driver.
		if s.opt.DBType == DbTypePostgres {
			// Postgres placeholders are $1, $2 ...
			colValHolder[i] = fmt.Sprintf("$%d", i+1)
		} else {
			colValHolder[i] = "?"
		}
	}

	var (
		fields   = make([]string, len(cols))
		typ      = ""
		unlogged = ""
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
		case "JSON", "JSONB": // Postgres
			if s.opt.DBType != DbTypePostgres {
				typ = "TEXT"
			}
		// _INT4, _INT8, _TEXT represent array types in Postgres
		case "_INT4": // Postgres
			typ = "_INT4"
		case "_INT8": // Postgres
			typ = "_INT8"
		case "_TEXT": // Postgres
			typ = "_TEXT"
		default:
			typ = "TEXT"
		}

		if nullable, ok := colTypes[i].Nullable(); ok && !nullable {
			typ += " NOT NULL"
		}

		// TODO - In some dialects double quote not supported...
		fields[i] = fmt.Sprintf(`%s`, cols[i])
	}

	// If the DB is Postgres, optionally create an "unlogged" table that disables
	// WAL, improving performance of throw-away cache tables.
	// https://www.postgresql.org/docs/current/sql-createtable.html
	if s.opt.DBType == DbTypePostgres && s.opt.UnloggedTables {
		unlogged = "UNLOGGED"
	}

	return insertSchema{
		// TODO - In some dialects...
		dropTable:   `DROP TABLE IF EXISTS "%s";`,
		createTable: fmt.Sprintf(`CREATE %s TABLE %%s (%s);`, unlogged, strings.Join(fields, ",")),
		insertRow:   fmt.Sprintf(`INSERT INTO %%s (%s) VALUES (%s)`, strings.Join(colNameHolder, ","), strings.Join(colValHolder, ",")),
	}
}
