package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/RichardKnop/machinery/v1/tasks"
	uuid "github.com/satori/go.uuid"
)

var (
	jobContexts = make(map[string]context.CancelFunc)
	jobMutex    = sync.RWMutex{}
)

// createJobSignature creates and returns a machinery tasks.Signature{} from the given job params.
func createJobSignature(j jobReq, taskName string, ttl int, jobber *Jobber) (tasks.Signature, error) {
	if _, ok := jobber.Queries[taskName]; !ok {
		return tasks.Signature{}, fmt.Errorf("unrecognized task: %s", taskName)
	}

	// Check if a job with the same ID is already running.
	if s, err := jobber.Machinery.GetBackend().GetState(j.JobID); err == nil && !s.IsCompleted() {
		return tasks.Signature{}, fmt.Errorf("job '%s' is already running", j.JobID)
	}

	// If there's no job_id, we generate one. This is because
	// the ID Machinery generates is not made available inside the
	// actual task. So, we generate the ID and pass it as an argument
	// to the task itself.
	if j.JobID == "" {
		j.JobID = fmt.Sprintf("job_%v", uuid.NewV4())
	}

	// Task arguments.
	args := append([]tasks.Arg{
		// First two arguments have to be jobID and taskName.
		// Machinery will refelect on these and pass them as arguments
		// to the callback registered using RegisterTask() when a task
		// is executed.
		{Type: "string", Value: j.JobID},
		{Type: "string", Value: taskName},
		{Type: "int", Value: ttl},
	}, sliceToTaskArgs(j.Args)...)

	var eta *time.Time
	if j.ETA != "" {
		e, err := time.Parse("2006-01-02 15:04:05", j.ETA)
		if err != nil {
			return tasks.Signature{}, fmt.Errorf("error parsing ETA: %v", err)
		}

		eta = &e
	}

	return tasks.Signature{
		Name:       taskName,
		UUID:       j.JobID,
		RoutingKey: j.Queue,
		RetryCount: j.Retries,
		Args:       args,
		ETA:        eta,
	}, nil
}

// executeTask executes an SQL statement job and inserts the results into the results backend.
func executeTask(jobID, taskName string, ttl int, args []interface{}, q *Query, jobber *Jobber) (int64, error) {
	var (
		dbName  = fmt.Sprintf(jobber.Constants.ResultsDB, jobID)
		numRows int64
	)

	// If the job's deleted, stop.
	if _, err := jobber.Machinery.GetBackend().GetState(jobID); err != nil {
		return numRows, errors.New("the job was canceled")
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

	var (
		rows *sql.Rows
		err  error
	)
	// Prepared query.
	if q.Stmt != nil {
		rows, err = q.Stmt.QueryContext(ctx, args...)
	} else {
		rows, err = jobber.DB.QueryContext(ctx, q.Raw, args...)
	}
	if err != nil {
		if err == context.Canceled {
			return numRows, errors.New("the job was canceled")
		}

		return numRows, fmt.Errorf("task SQL query execution failed: %v", err)
	}
	defer rows.Close()

	// Results backend.
	w := jobber.ResultBackend.NewResultSet(dbName, taskName, time.Second*time.Duration(ttl))
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
		resCols     = make([][]byte, numCols)
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
		return numRows, fmt.Errorf("error flushing results to result backend: %v", err)
	}

	return numRows, nil
}
