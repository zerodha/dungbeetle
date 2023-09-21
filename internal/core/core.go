package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	uuid "github.com/gofrs/uuid/v5"
	"github.com/kalbhor/tasqueue/v2"
	"github.com/vmihailenco/msgpack"
	"github.com/zerodha/dungbeetle/internal/dbpool"
	"github.com/zerodha/dungbeetle/models"
)

// Opt represents core options.
type Opt struct {
	DefaultQueue            string
	DefaultGroupConcurrency int
	DefaultJobTTL           time.Duration

	// DSNs for connecting to the broker backend and the broker state backend.
	Broker  tasqueue.Broker
	Results tasqueue.Results
}

type Core struct {
	opt Opt

	// Named SQL query tasks.
	tasks Tasks

	// Pool of source data DBs.
	srcDBs dbpool.Pool

	// Named map of one or more result backend DBs.
	resultBackends ResultBackends

	// Distributed queue system.
	q *tasqueue.Server

	// Job states for cancellation.
	jobCtx map[string]context.CancelFunc
	mu     sync.RWMutex

	lo *slog.Logger
}

// New returns a new instance of Core.
func New(o Opt, srcDBs dbpool.Pool, res ResultBackends, lo *slog.Logger) *Core {
	return &Core{
		opt:            o,
		tasks:          make(Tasks),
		srcDBs:         srcDBs,
		resultBackends: res,
		jobCtx:         make(map[string]context.CancelFunc),
		mu:             sync.RWMutex{},
		lo:             lo,
	}
}

// Start is a blocking function that spawns the queue workers and starts processing jobs.
// This should be invoked last after all other initializations are done.
func (co *Core) Start(ctx context.Context, workerName string, concurrency int) error {
	// Initialize the distributed queue system.
	qs, err := co.initQueue()
	if err != nil {
		return err
	}
	co.q = qs
	qs.Start(ctx)

	return nil
}

// GetTasks returns the registered tasks map.
func (co *Core) GetTasks() Tasks {
	return co.tasks
}

// NewJob creates a new job out of a given task and registers it.
func (co *Core) NewJob(j models.JobReq, taskName string) (models.JobResp, error) {
	sig, err := co.makeJob(j, taskName)
	if err != nil {
		return models.JobResp{}, err
	}

	// Create the job.
	uuid, err := co.q.Enqueue(context.Background(), sig)
	if err != nil {
		return models.JobResp{}, err
	}

	return models.JobResp{
		JobID:    uuid,
		TaskName: sig.Task,
		Queue:    sig.Opts.Queue,
		Retries:  int(sig.Opts.MaxRetries),
		ETA:      &sig.Opts.ETA,
	}, nil
}

func (co *Core) NewJobGroup(req models.GroupReq) (models.GroupResp, error) {
	// Create job signatures for all the jobs in the group.
	sigs := make([]tasqueue.Job, 0, len(req.Jobs))
	for _, j := range req.Jobs {
		sig, err := co.makeJob(j, j.TaskName)
		if err != nil {
			return models.GroupResp{}, err
		}

		sigs = append(sigs, sig)
	}

	// Create the group and queue it.
	g, err := tasqueue.NewGroup(sigs, tasqueue.GroupOpts{
		ID: req.GroupID,
	})
	if err != nil {
		return models.GroupResp{}, err
	}

	gID, err := co.q.EnqueueGroup(context.Background(), g)
	if err != nil {
		return models.GroupResp{}, err
	}

	jobs := make([]models.JobResp, len(sigs))
	for i, r := range sigs {
		jobs[i] = models.JobResp{
			JobID:    r.Opts.ID,
			TaskName: r.Task,
			Queue:    r.Opts.Queue,
			Retries:  int(r.Opts.MaxRetries),
			ETA:      &r.Opts.ETA,
		}
	}

	return models.GroupResp{
		GroupID: gID,
		Jobs:    jobs,
	}, nil
}

// GetPendingJobs returns jobs pending execution.
func (co *Core) GetPendingJobs(queue string) ([]tasqueue.JobMessage, error) {
	out, err := co.q.GetPending(context.Background(), queue)
	if err != nil {
		co.lo.Error("error fetching pending tasks", "error", err)
		return nil, err
	}
	jLen := len(out)
	for i := 0; i < jLen/2; i++ {
		out[i], out[jLen-i-1] = out[jLen-i-1], out[i]
	}

	return out, nil
}

// GetJobStatus returns the status of a job.
func (co *Core) GetJobStatus(jobID string) (models.JobStatusResp, error) {
	out, err := co.q.GetJob(context.Background(), jobID)
	if err == tasqueue.ErrNotFound {
		return models.JobStatusResp{}, fmt.Errorf("job not found")
	} else if err != nil {
		co.lo.Error("error fetching job status", "error", err)
		return models.JobStatusResp{}, err
	}

	return models.JobStatusResp{
		JobID: out.ID,
		State: out.Status,
		Error: out.PrevErr,
	}, nil
}

// GetJobGroupStatus returns the status of a job group including statuses of individual jobs in it.
func (co *Core) GetJobGroupStatus(groupID string) (models.GroupStatusResp, error) {
	ctx := context.Background()

	groupMsg, err := co.q.GetGroup(ctx, groupID)
	if err != nil {
		co.lo.Error("error fetching group status", "error", err)
		return models.GroupStatusResp{}, err
	}

	var jobs []models.JobStatusResp
	for uuid, status := range groupMsg.JobStatus {
		jMsg, err := co.q.GetJob(ctx, uuid)
		if err != nil {
			co.lo.Error("error fetching job status", "error", err)
			return models.GroupStatusResp{}, err
		}

		state, err := getState(status)
		if err != nil {
			co.lo.Error("error fetching job status mapping", "error", err)
			return models.GroupStatusResp{}, err
		}

		jobs = append(jobs, models.JobStatusResp{
			JobID: uuid,
			State: state,
			Error: jMsg.PrevErr,
		})

	}

	state, err := getState(groupMsg.Status)
	if err != nil {
		co.lo.Error("error fetching group status mapping", "error", err)
		return models.GroupStatusResp{}, err
	}

	return models.GroupStatusResp{
		GroupID: groupID,
		State:   state,
		Jobs:    jobs,
	}, nil
}

// CancelJob cancels a given pending/processing job.
func (co *Core) CancelJob(jobID string, purge bool) error {
	s, err := co.GetJobStatus(jobID)
	if err != nil {
		co.lo.Error("error fetching job", "error", err)
		return errors.New("error fetching job")
	}

	// If the job is already complete, no go.
	if !purge && (s.State == "SUCCESS" || s.State == "FAILURE") {
		return errors.New("can't cancel completed job")
	}

	// Stop the job if it's running.
	co.mu.RLock()
	cancel, ok := co.jobCtx[jobID]
	co.mu.RUnlock()
	if ok {
		cancel()
	}

	// Delete the job.
	if err := co.q.DeleteJob(context.Background(), jobID); err != nil {
		co.lo.Error("error deleting job", "error", err)
		return fmt.Errorf("error deleting job: %v", err)
	}

	return nil
}

// CancelJobGroup cancels a pending group job by individually cancelling all pending jobs in it.
func (co *Core) CancelJobGroup(groupID string, purge bool) error {
	// Get state of group.
	res, err := co.GetJobGroupStatus(groupID)
	if err != nil {
		co.lo.Error("error fetching group status", "error", err)
		return errors.New("error fetching group status")
	}

	// If purge is set to false, delete job only if isn't completed yet.
	if !purge {
		// Get state of group ID to check if it has been completed or not.
		s, err := co.q.GetGroup(context.Background(), groupID)
		if err == tasqueue.ErrNotFound {
			return errors.New("group not found")
		}

		// If the job is already complete, no go.
		if s.Status == tasqueue.StatusDone || s.Status == tasqueue.StatusFailed {
			return errors.New("can't delete group as it's already complete")
		}
	}

	// Loop through every job of the group and purge them.
	for _, j := range res.Jobs {
		if err != nil {
			co.lo.Error("error fetching job", "error", err)
			return errors.New("error fetching job from group")
		}

		// Stop the job if it's running.
		co.mu.RLock()
		cancel, ok := co.jobCtx[j.JobID]
		co.mu.RUnlock()
		if ok {
			cancel()
		}

		// Delete the job.
		if err := co.q.DeleteJob(context.Background(), j.JobID); err != nil {
			co.lo.Error("error deleting job", "error", err)
			return fmt.Errorf("error deleting job: %v", err)
		}
	}

	return nil
}

// makeJob creates and returns a tasqueue job (tasqueue.Job{}) from the given job params.
func (co *Core) makeJob(j models.JobReq, taskName string) (tasqueue.Job, error) {
	task, ok := co.tasks[taskName]
	if !ok {
		return tasqueue.Job{}, fmt.Errorf("unrecognized task: %s", taskName)
	}

	// Check if a job with the same ID is already running.
	msg, err := co.q.GetJob(context.Background(), j.JobID)
	if err != nil && !errors.Is(err, tasqueue.ErrNotFound) {
		return tasqueue.Job{}, fmt.Errorf("error fetching job status (id='%s')", j.JobID)
	}

	if msg.Status == tasqueue.StatusProcessing || msg.Status == tasqueue.StatusRetrying {
		return tasqueue.Job{}, fmt.Errorf("job '%s' is already running", j.JobID)
	}

	// If there's no job_id, we generate one. This is because
	// the ID Machinery generates is not made available inside the
	// actual task. So, we generate the ID and pass it as an argument
	// to the task itself.
	if j.JobID == "" {
		uid, err := uuid.NewV4()
		if err != nil {
			return tasqueue.Job{}, fmt.Errorf("error generating uuid: %v", err)
		}

		j.JobID = fmt.Sprintf("job_%v", uid)
	}

	ttl := co.opt.DefaultJobTTL
	if j.TTL > 0 {
		ttl = time.Duration(j.TTL) * time.Second
	}

	var eta time.Time
	if j.ETA != "" {
		e, err := time.Parse("2006-01-02 15:04:05", j.ETA)
		if err != nil {
			return tasqueue.Job{}, fmt.Errorf("error parsing ETA: %v", err)
		}

		eta = e
	}

	// If there's no queue in the request, use the one attached to the task,
	// if there's any (Machinery will use the default queue otherwise).
	if j.Queue == "" {
		j.Queue = task.Queue
	}

	var args = make([]interface{}, len(j.Args))
	for i := range j.Args {
		args[i] = j.Args[i]
	}

	b, err := msgpack.Marshal(taskMeta{
		Args: args,
		DB:   j.DB,
		TTL:  int(ttl),
	})
	if err != nil {
		return tasqueue.Job{}, err
	}

	return tasqueue.NewJob(taskName, b, tasqueue.JobOpts{
		ID:         j.JobID,
		Queue:      j.Queue,
		MaxRetries: uint32(j.Retries),
		ETA:        eta,
	})
}

type taskMeta struct {
	Args []interface{} `json:"args"`
	DB   string        `json:"db"`
	TTL  int           `json:"ttl"`
}

// initQueue creates and returns a distributed queue system (Tasqueue) and registers
// Tasks (SQL queries) to be executed. The queue system uses a broker (eg: Kafka) and stores
// job states in a state store (eg: Redis)
func (co *Core) initQueue() (*tasqueue.Server, error) {
	var err error
	// TODO: set log level
	qs, err := tasqueue.NewServer(tasqueue.ServerOpts{
		Broker:  co.opt.Broker,
		Results: co.opt.Results,
		Logger:  co.lo.Handler(),
	})
	if err != nil {
		return nil, err
	}

	// Register every SQL ready tasks in the queue system as a job function.
	for name, query := range co.tasks {
		qs.RegisterTask(string(name), func(b []byte, jctx tasqueue.JobCtx) error {
			if _, err := qs.GetJob(context.Background(), jctx.Meta.ID); err != nil {
				return err
			}

			var args taskMeta
			if err := msgpack.Unmarshal(b, &args); err != nil {
				return fmt.Errorf("could not unmarshal args : %w", err)
			}

			_, err = co.execJob(jctx.Meta.ID, name, args.DB, time.Duration(args.TTL)*time.Second, args.Args, query)
			return err
		}, tasqueue.TaskOpts{
			Concurrency: uint32(query.Conc),
			Queue:       query.Queue,
		})
	}

	return qs, nil
}

// execJob executes an SQL statement job and inserts the results into the result backend.
func (co *Core) execJob(jobID, taskName, dbName string, ttl time.Duration, args []interface{}, task Task) (int64, error) {
	// If the job's deleted, stop.
	if _, err := co.GetJobStatus(jobID); err != nil {
		return 0, errors.New("the job was canceled")
	}

	// Execute the query.
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()

		co.mu.Lock()
		delete(co.jobCtx, jobID)
		co.mu.Unlock()
	}()

	co.mu.Lock()
	co.jobCtx[jobID] = cancel
	co.mu.Unlock()

	var (
		rows *sql.Rows
		db   *sql.DB
		err  error
	)
	if task.Stmt != nil {
		// Prepared query.
		rows, err = task.Stmt.QueryContext(ctx, args...)
	} else {

		if dbName != "" {
			// Specific DB.
			d, err := task.DBs.Get(dbName)
			if err != nil {
				return 0, fmt.Errorf("task execution failed: %s: %v", task.Name, err)
			}
			db = d
		} else {
			// Random DB.
			_, db = task.DBs.GetRandom()
		}
	}

	rows, err = db.QueryContext(ctx, task.Raw, args...)
	if err != nil {
		if err == context.Canceled {
			return 0, errors.New("the job was canceled")
		}

		return 0, fmt.Errorf("task execution failed: %s: %v", task.Name, err)
	}
	defer rows.Close()

	// Write the results to the results backend.
	return co.writeResults(jobID, task, ttl, rows)
}

// writeResults writes results from an SQL query to a result backend.
func (co *Core) writeResults(jobID string, task Task, ttl time.Duration, rows *sql.Rows) (int64, error) {
	var numRows int64

	// Get the results backend.
	name, backend := task.ResultBackends.GetRandom()
	co.lo.Info("sending results form", "job_id", jobID, "name", name)
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

// getState helps keep compatibility between different status conventions
// of the job servers (tasqueue/machinery).
func getState(st string) (string, error) {
	switch st {
	case tasqueue.StatusStarted:
		return "PENDING", nil
	case tasqueue.StatusProcessing:
		return "STARTED", nil
	case tasqueue.StatusFailed:
		return "FAILURE", nil
	case tasqueue.StatusDone:
		return "SUCCESS", nil
	case tasqueue.StatusRetrying:
		return "RETRY", nil
	}

	return "", fmt.Errorf("invalid status not found in mapping")
}
