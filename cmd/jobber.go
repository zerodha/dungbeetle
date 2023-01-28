package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/knadh/sql-jobber/models"
	uuid "github.com/satori/go.uuid"
)

var (
	jobContexts = make(map[string]context.CancelFunc)
	jobMutex    = sync.RWMutex{}
)

// createJobSignature creates and returns a machinery tasks.Signature{} from the given job params.
func createJobSignature(j models.JobReq, taskName string, ttl int, jobber *Jobber) (tasks.Signature, error) {
	task, ok := jobber.Tasks[taskName]
	if !ok {
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

	// If there's no queue in the request, use the one attached to the task,
	// if there's any (Machinery will use the default queue otherwise).
	if j.Queue == "" {
		j.Queue = task.Queue
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

// executeTask executes an SQL statement job and inserts the results into the result backend.
func executeTask(jobID, taskName string, ttl time.Duration, args []interface{}, task *Task, jobber *Jobber) (int64, error) {
	// If the job's deleted, stop.
	if _, err := jobber.Machinery.GetBackend().GetState(jobID); err != nil {
		return 0, errors.New("the job was canceled")
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
	if task.Stmt != nil {
		rows, err = task.Stmt.QueryContext(ctx, args...)
	} else {
		_, db := task.DBs.GetRandom()
		rows, err = db.QueryContext(ctx, task.Raw, args...)
	}
	if err != nil {
		if err == context.Canceled {
			return 0, errors.New("the job was canceled")
		}

		return 0, fmt.Errorf("task SQL query execution failed: %v", err)
	}
	defer rows.Close()

	return writeResults(jobID, task, ttl, rows, jobber)
}

// writeResults writes results from an SQL query to a result backend.
func writeResults(jobID string, task *Task, ttl time.Duration, rows *sql.Rows, jobber *Jobber) (int64, error) {
	var numRows int64

	// Result backend.
	name, backend := task.ResultBackends.GetRandom()
	jobber.Logger.Printf("sending results form '%s' to '%s'", jobID, name)

	w, err := backend.NewResultSet(jobID, task.Name, ttl)
	if err != nil {
		return numRows, fmt.Errorf("error writing columns to result backend: %v", err)
	}
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

		// // TODO Need call a refactor method... doing this...
		// w.schemaMutex.Lock()
		// w.resTableSchemas[w.taskName] = w.CreateTableSchema(cols, colTypes)
		// w.schemaMutex.Unlock()

	}

	// Write the results columns / headers.
	if err := w.WriteCols(cols); err != nil {
		return numRows, fmt.Errorf("error writing columns to result backend: %v", err)
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
		if err := w.WriteRow(resCols); err != nil {
			return numRows, fmt.Errorf("error writing row to result backend: %v", err)
		}

		numRows++
	}

	if err := w.Flush(); err != nil {
		return numRows, fmt.Errorf("error flushing results to result backend: %v", err)
	}

	return numRows, nil
}

// connectJobServer creates and returns a Machinery job server
// while registering the given SQL queries as tasks.
func connectJobServer(jobber *Jobber, cfg *config.Config, queries Tasks) (*machinery.Server, error) {
	server, err := machinery.NewServer(cfg)
	if err != nil {
		return nil, err
	}

	// Register the tasks with the query names.
	for name, query := range queries {
		server.RegisterTask(string(name), func(q Task) taskFunc {
			return func(jobID, taskName string, ttl int, args ...interface{}) (int64, error) {
				// Check if the job's been deleted.
				if _, err := jobber.Machinery.GetBackend().GetState(jobID); err != nil {
					return 0, fmt.Errorf("Skipping deleted job: %v", err)
				}

				return executeTask(jobID, taskName, time.Duration(ttl)*time.Second, args, &q, jobber)
			}
		}(query))
	}

	return server, nil
}
