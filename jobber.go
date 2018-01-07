package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	jobContexts = make(map[string]context.CancelFunc)
	jobMutex    = sync.RWMutex{}
)

// executeTask executes an SQL statement job and inserts the results into the rediSQL backend.
func executeTask(jobID, taskName string, args []interface{}, q *Query) (int64, error) {
	var (
		dbName  = fmt.Sprintf(jobber.Constants.ResultsDB, jobID)
		numRows int64
	)

	// If the job's deleted, stop.
	if _, err := jobber.Machinery.GetBackend().GetState(jobID); err != nil {
		return numRows, errors.New("The job was canceled")
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

	// Results backend.
	w := jobber.RsultBackend.NewResultSet(dbName, taskName)
	defer w.Close()

	// Get the columns in the results.
	cols, err := rows.Columns()
	if err != nil {
		return numRows, err
	}
	numCols := len(cols)

	// If the column types for this particular taskName
	// have not been registered, do it.
	if !w.IsColTypesRegistered() {
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return numRows, err
		}

		w.RegisterColTypes(cols, colTypes)
	}

	// Write the results columns / headers.
	if err := w.WriteCols(cols); err != nil {
		return numRows, err
	}

	// Gymnastics to read arbitrary types from the row.
	var (
		resCols     = make([]interface{}, numCols)
		resPointers = make([]interface{}, numCols)
	)
	for i := 0; i < numCols; i++ {
		resPointers[i] = &resCols[i]
	}

	// Scan each row and write to the results backend.
	for rows.Next() {
		if err := rows.Scan(resPointers...); err != nil {
			return numRows, err
		}
		w.WriteRow(resCols)
		numRows++
	}

	if err := w.Flush(); err != nil {
		return numRows, fmt.Errorf("Error flushing results to Redis result backend: %v", err)
	}

	return numRows, nil
}
