package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/garyburd/redigo/redis"
)

const (
	// Name of the INSERT() statement to register in rediSQL.
	insertStmt = "insert_result"
)

// The result schemas (CREATE TABLE ...) for rediSQL are dynamically
// generated everytime queries are executed based on their result columns.
// They're cached here so as to avoid repetetive generation.
var (
	resSchemas  = make(map[string]string)
	schemaMutex = sync.RWMutex{}

	jobContexts = make(map[string]context.CancelFunc)
	jobMutex    = sync.RWMutex{}
)

// executeTask executes an SQL statement job and inserts
// the results into the rediSQL backend.
func executeTask(jobID, taskName string, args []interface{}, q *Query) (int64, error) {
	var (
		dbName  = fmt.Sprintf(jobber.Constants.ResultsDB, jobID)
		numRows int64
	)

	// If the job's deleted, stop.
	if _, err := jobber.Machinery.GetBackend().GetState(jobID); err != nil {
		return numRows, errors.New("The job was canceled. Throwing away results")
	}

	// Execute the query.
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()

		jobMutex.Lock()
		delete(jobContexts, jobID)
		jobMutex.Unlock()
	}()

	jobMutex.Lock()
	jobContexts[jobID] = cancel
	jobMutex.Unlock()

	rows, err := q.Stmt.QueryContext(ctx, args...)
	if err != nil {
		if err == context.Canceled {
			return numRows, errors.New("The job was canceled")
		}

		return numRows, fmt.Errorf("Task query execution failed: %v", err)
	}
	defer rows.Close()

	// Redis connection for results.
	c := jobber.Redis.Get()
	defer c.Close()

	// Get the columns in the results.
	cols, err := rows.Columns()
	if err != nil {
		return numRows, err
	}
	numCols := len(cols)

	// Is the result table schema already generated?
	schemaMutex.RLock()
	rSchema, ok := resSchemas[taskName]
	schemaMutex.RUnlock()

	if !ok {
		// Get the column types.
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return numRows, err
		}

		rSchema = generateResultsSchema(jobber.Constants.ResultsTable, cols, colTypes)
		// Cache it for the next round.
		schemaMutex.Lock()
		resSchemas[taskName] = rSchema
		schemaMutex.Unlock()

	}

	// Create the results rediSQL results DB + table.
	if err := createResultsDB(dbName, jobber.Constants.ResultsTable,
		cols, rSchema, jobber.Constants.ResultsTTL, c); err != nil {
		return numRows, err
	}

	// Gymnastics to read arbitrary types from the row.
	var (
		resCommands = []interface{}{dbName, insertStmt}
		resCols     = make([]interface{}, numCols)
		resPointers = make([]interface{}, numCols)
	)
	for i := 0; i < numCols; i++ {
		resPointers[i] = &resCols[i]
	}

	// Scan each row, and then each column in the same order
	// as we did above. The rows are read, buffered, and then
	// bulk inserted into rediSQL.
	for rows.Next() {
		if err := rows.Scan(resPointers...); err != nil {
			return numRows, err
		}

		// The command's sent as as:
		// REDISQL.EXEC_STATEMENT {db} insert_result val0, val1, val2 ...
		c.Send("REDISQL.EXEC_STATEMENT", append(resCommands, resCols...)...)
		numRows++
	}

	if err := c.Flush(); err != nil {
		c.Do("DEL", jobID)
		return numRows, fmt.Errorf("Error flushing results to Redis result backend: %v", err)
	}

	return numRows, nil
}

// createResultsDB creates a rediSQL DB and a results table in it.
func createResultsDB(dbName, resTable string, cols []string, resSchema string, ttl int, c redis.Conn) error {
	// Delete the existing job DBName if there's already one.
	c.Do("DEL", dbName)

	// Create a fresh database for this job.
	if err := rediSQLquery(c, "REDISQL.CREATE_DB", dbName, ""); err != nil {
		return fmt.Errorf("REDISQL.CREATE_DB failed: %v", err)
	}

	c.Do("EXPIRE", dbName, ttl)

	// Create the results table.
	if err := rediSQLquery(c, "REDISQL.EXEC", dbName, resSchema); err != nil {
		return fmt.Errorf("REDISQL.EXEC failed: %v", err)
	}

	// Register the positional INSERT() statement in rediSQL.
	ins := make([]string, len(cols))
	for i := 0; i < len(cols); i++ {
		ins[i] = fmt.Sprintf("?%d", i+1)
	}

	return rediSQLquery(c, "REDISQL.CREATE_STATEMENT", dbName, insertStmt,
		"INSERT INTO "+resTable+" VALUES("+strings.Join(ins, ",")+")")
}

// generateResultsSchema takes an SQL query results, get its column names
// and types, and generates an rediSQL CREATE TABLE schema for the results
func generateResultsSchema(resTable string, cols []string, colTypes []*sql.ColumnType) string {
	var (
		fields = make([]string, len(cols))
		typ    = ""
	)
	for i := 0; i < len(cols); i++ {
		switch colTypes[i].ScanType().Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			typ = "INTEGER"
		case reflect.Float32, reflect.Float64:
			typ = "REAL"
		default:
			typ = "TEXT"
		}

		fields[i] = cols[i] + " " + typ
	}

	return "CREATE TABLE " + resTable + " (" + strings.Join(fields, ",") + ");"
}

// rediSQLquery executes a REDISQL.* command against a given database name.
// This should only be used for running non SELECT() commands as any byte
// response received from the mdoule is treated as an error.
// This is because rediSQL errors don't throw a Redis error but simply
// send the error message as the success reply.
func rediSQLquery(c redis.Conn, cmd, dbName string, args ...interface{}) error {
	a := append([]interface{}{dbName}, args...)
	v, err := c.Do(cmd, a...)
	if err != nil {
		return fmt.Errorf("Error executing rediSQL query: %v: %v", err, args)
	}

	// If there's a byte array, that's an error message from rediSQL.
	msg, ok := v.([]byte)
	if ok {
		return fmt.Errorf("Error executing rediSQL query: %s | Query = %v", msg, args)
	}

	return nil
}
