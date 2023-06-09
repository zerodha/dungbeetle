package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kalbhor/tasqueue/v2"
	bredis "github.com/kalbhor/tasqueue/v2/brokers/redis"
	rredis "github.com/kalbhor/tasqueue/v2/results/redis"

	"github.com/knadh/koanf"
	"github.com/knadh/sql-jobber/models"
	"github.com/zerodha/logf"
)

var (
	jobContexts = make(map[string]context.CancelFunc)
	jobMutex    = sync.RWMutex{}
)

// createJobSignature creates and returns a tasqueue job from the given job params.
func createJob(j models.JobReq, taskName, dbName string, ttl int, jobber *Jobber) (tasqueue.Job, error) {
	task, ok := jobber.Tasks[taskName]
	if !ok {
		return tasqueue.Job{}, fmt.Errorf("unrecognized task: %s", taskName)
	}

	var (
		eta time.Time
		err error
	)
	if j.ETA != "" {
		eta, err = time.Parse("2006-01-02 15:04:05", j.ETA)
		if err != nil {
			return tasqueue.Job{}, fmt.Errorf("error parsing ETA: %v", err)
		}
	}

	// If there's no queue in the request, use the one attached to the task,
	// if there's any (Machinery will use the default queue otherwise).
	if j.Queue == "" {
		j.Queue = task.Queue
	}

	b, _ := json.Marshal(taskMeta{
		Args: j.Args,
		DB:   dbName,
		TTL:  ttl,
	})

	return tasqueue.Job{
		Task:    taskName,
		Payload: b,
		Opts: tasqueue.JobOpts{
			ID:         j.JobID,
			Queue:      j.Queue,
			MaxRetries: uint32(j.Retries),
			ETA:        eta,
		},
	}, nil
}

// executeTask executes an SQL statement job and inserts the results into the result backend.
func executeTask(jobID, taskName string, pl taskMeta, task *Task, jobber *Jobber) (int64, error) {

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
		db   *sql.DB
		err  error
	)
	if task.Stmt != nil {
		// Prepared query.
		rows, err = task.Stmt.QueryContext(ctx, pl.Args...)
	} else {

		if pl.DB != "" {
			// Specific DB.
			d, err := task.DBs.Get(pl.DB)
			if err != nil {
				return 0, fmt.Errorf("task execution failed: %s: %v", task.Name, err)
			}
			db = d
		} else {
			// Random DB.
			_, db = task.DBs.GetRandom()
		}
	}

	rows, err = db.QueryContext(ctx, task.Raw, pl.Args...)
	if err != nil {
		if err == context.Canceled {
			return 0, errors.New("the job was canceled")
		}

		return 0, fmt.Errorf("task execution failed: %v", err)
	}
	defer rows.Close()

	return writeResults(jobID, task, time.Duration(pl.TTL), rows, jobber)
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

type taskMeta struct {
	Args []interface{} `json:"args"`
	DB   string        `json:"db"`
	TTL  int           `json:"ttl"`
}

func connectJobServer(ko *koanf.Koanf, j *Jobber, queries Tasks) error {
	rBroker := bredis.New(bredis.Options{
		PollPeriod:   bredis.DefaultPollPeriod,
		Addrs:        ko.Strings("tasqueue.broker.address"),
		Password:     ko.String("tasqueue.broker.password"),
		DB:           ko.Int("tasqueue.broker.db"),
		MinIdleConns: ko.Int("tasqueue.broker.max_idle"),
		DialTimeout:  ko.Duration("tasqueue.broker.dial_timeout"),
		ReadTimeout:  ko.Duration("tasqueue.broker.read_timeout"),
		WriteTimeout: ko.Duration("tasqueue.broker.write_timeout"),
	}, logf.New(logf.Opts{}))

	rResult := rredis.New(rredis.Options{
		Addrs:        ko.Strings("tasqueue.results.address"),
		Password:     ko.String("tasqueue.results.password"),
		DB:           ko.Int("tasqueue.results.db"),
		MinIdleConns: ko.Int("tasqueue.results.max_idle"),
		DialTimeout:  ko.Duration("tasqueue.results.dial_timeout"),
		ReadTimeout:  ko.Duration("tasqueue.results.read_timeout"),
		WriteTimeout: ko.Duration("tasqueue.results.write_timeout"),
	}, logf.New(logf.Opts{}))

	var err error
	j.Tasqueue, err = tasqueue.NewServer(tasqueue.ServerOpts{
		Broker:  rBroker,
		Results: rResult,
	})
	if err != nil {
		return err
	}

	for name, query := range queries {
		query := query
		j.Tasqueue.RegisterTask(string(name), func(b []byte, jctx tasqueue.JobCtx) error {
			var args taskMeta
			if err := json.Unmarshal(b, &args); err != nil {
				return fmt.Errorf("could not unmarshal args : %w", err)
			}

			_, err := executeTask(jctx.Meta.ID, name, args, &query, jobber)
			return err
		}, tasqueue.TaskOpts{
			Concurrency: uint32(ko.Int("worker-concurrency")),
			Queue:       query.Queue,
		})
	}

	return nil
}
