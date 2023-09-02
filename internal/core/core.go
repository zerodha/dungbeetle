package core

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	mlog "github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	uuid "github.com/gofrs/uuid/v5"
	"github.com/gomodule/redigo/redis"
	"github.com/zerodha/dungbeetle/internal/dbpool"
	"github.com/zerodha/dungbeetle/models"
)

type taskFunc func(jobID string, taskName, db string, ttl int, args ...interface{}) (int64, error)

// Opt represents core options.
type Opt struct {
	DefaultQueue            string
	DefaultGroupConcurrency int
	DefaultJobTTL           time.Duration

	// DSNs for connecting to the broker backend and the broker state backend.
	QueueBrokerDSN string // eg: Kafka DSN
	QueueStateDSN  string // eg: Redis DSN
	QueueStateTTL  time.Duration
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
	q *machinery.Server

	// Job states for cancellation.
	jobCtx map[string]context.CancelFunc
	mu     sync.RWMutex

	lo *log.Logger
}

// New returns a new instance of Core.
func New(o Opt, srcDBs dbpool.Pool, res ResultBackends, lo *log.Logger) *Core {
	// Override Machinery's default logger.
	mlog.Set(log.New(os.Stdout, "MACHIN: ", log.Ldate|log.Ltime|log.Lshortfile))

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
func (co *Core) Start(workerName string, concurrency int) error {
	// Initialize the distributed queue system.
	qs, err := co.initQueue()
	if err != nil {
		return err
	}
	co.q = qs

	// Spawn workers.
	w := qs.NewWorker(workerName, concurrency)
	return w.Launch()
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
	res, err := co.q.SendTask(&sig)
	if err != nil {
		return models.JobResp{}, err
	}

	return models.JobResp{
		JobID:    res.Signature.UUID,
		TaskName: res.Signature.Name,
		Queue:    res.Signature.RoutingKey,
		Retries:  res.Signature.RetryCount,
		ETA:      res.Signature.ETA,
	}, nil
}

func (co *Core) NewJobGroup(req models.GroupReq) (models.GroupResp, error) {
	// Create job signatures for all the jobs in the group.
	sigs := make([]*tasks.Signature, 0, len(req.Jobs))
	for _, j := range req.Jobs {
		sig, err := co.makeJob(j, j.TaskName)
		if err != nil {
			return models.GroupResp{}, err
		}

		sigs = append(sigs, &sig)
	}

	conc := co.opt.DefaultGroupConcurrency
	if req.Concurrency > 0 {
		conc = req.Concurrency
	}

	// Create the group and queue it.
	g, _ := tasks.NewGroup(sigs...)

	// If there's an incoming group ID, overwrite the generated one.
	if req.GroupID != "" {
		g.GroupUUID = req.GroupID

		for _, t := range g.Tasks {
			t.GroupUUID = req.GroupID
		}
	}

	res, err := co.q.SendGroup(g, conc)
	if err != nil {
		return models.GroupResp{}, err
	}

	jobs := make([]models.JobResp, len(res))
	for i, r := range res {
		jobs[i] = models.JobResp{
			JobID:    r.Signature.UUID,
			TaskName: r.Signature.Name,
			Queue:    r.Signature.RoutingKey,
			Retries:  r.Signature.RetryCount,
			ETA:      r.Signature.ETA,
		}
	}

	gID := req.GroupID
	if gID == "" {
		gID = res[0].Signature.GroupUUID
	}

	return models.GroupResp{
		GroupID: gID,
		Jobs:    jobs,
	}, nil
}

// GetPendingJobs returns jobs pending execution.
func (co *Core) GetPendingJobs(queue string) ([]*tasks.Signature, error) {
	out, err := co.q.GetBroker().GetPendingTasks(queue)
	if err != nil {
		co.lo.Printf("error fetching pending tasks: %v", err)
		return nil, err
	}

	return out, nil
}

// GetJobStatus returns the status of a job.
func (co *Core) GetJobStatus(jobID string) (models.JobStatusResp, error) {
	out, err := co.q.GetBackend().GetState(jobID)
	if err == redis.ErrNil {
		return models.JobStatusResp{}, fmt.Errorf("job not found")
	} else if err != nil {
		co.lo.Printf("error fetching job status: %v", err)
		return models.JobStatusResp{}, err
	}

	return models.JobStatusResp{
		JobID: out.TaskUUID,
		State: out.State,
		Error: out.Error,
	}, nil
}

// GetJobGroupStatus returns the status of a job group including statuses of individual jobs in it.
func (co *Core) GetJobGroupStatus(groupID string) (models.GroupStatusResp, error) {
	if _, err := co.q.GetBackend().GetState(groupID); err == redis.ErrNil {
		return models.GroupStatusResp{}, err
	}

	res, err := co.q.GetBackend().GroupTaskStates(groupID, 0)
	if err != nil {
		co.lo.Printf("error fetching group status: %v", err)
		return models.GroupStatusResp{}, errors.New("error fetching group status")
	}

	var (
		jobs        = make([]models.JobStatusResp, len(res))
		numDone     = 0
		groupFailed = false
	)
	for i, j := range res {
		jobs[i] = models.JobStatusResp{
			JobID: j.TaskUUID,
			State: j.State,
			Error: j.Error,
		}

		if j.State == tasks.StateSuccess {
			numDone++
		} else if j.State == tasks.StateFailure {
			groupFailed = true
		}
	}

	status := tasks.StatePending
	if groupFailed {
		status = tasks.StateFailure
	} else if len(res) == numDone {
		status = tasks.StateSuccess
	}

	return models.GroupStatusResp{
		GroupID: groupID,
		State:   status,
		Jobs:    jobs,
	}, nil
}

// CancelJob cancels a given pending/processing job.
func (co *Core) CancelJob(jobID string, purge bool) error {
	s, err := co.GetJobStatus(jobID)
	if err != nil {
		co.lo.Printf("error fetching job: %v", err)
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
	if err := co.q.GetBackend().PurgeState(jobID); err != nil {
		co.lo.Printf("error deleting job: %v", err)
		return fmt.Errorf("error deleting job: %v", err)
	}

	return nil
}

// CancelJobGroup cancels a pending group job by individually cancelling all pending jobs in it.
func (co *Core) CancelJobGroup(groupID string, purge bool) error {
	// Get state of group.
	res, err := co.GetJobGroupStatus(groupID)
	if err != nil {
		co.lo.Printf("error fetching group status: %v", err)
		return errors.New("error fetching group status")
	}

	// If purge is set to false, delete job only if isn't completed yet.
	if !purge {
		// Get state of group ID to check if it has been completed or not.
		s, err := co.q.GetBackend().GetState(groupID)
		if err == redis.ErrNil {
			return errors.New("group not found")
		}

		// If the job is already complete, no go.
		if s.State == "SUCCESS" || s.State == "FAILURE" {
			return errors.New("can't delete group as it's already complete")
		}
	}

	// Loop through every job of the group and purge them.
	for _, j := range res.Jobs {
		if err != nil {
			co.lo.Printf("error fetching job: %v", err)
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
		if err := co.q.GetBackend().PurgeState(j.JobID); err != nil {
			co.lo.Printf("error deleting job: %v", err)
			return fmt.Errorf("error deleting job: %v", err)
		}
	}

	// Delete the group.
	if err := co.q.GetBackend().PurgeState(groupID); err != nil {
		co.lo.Printf("error deleting job: %v", err)
		return fmt.Errorf("error deleting group: %v", err)
	}

	return nil
}

// makeJob creates and returns a machinery tasks.Signature{} from the given job params.
func (co *Core) makeJob(j models.JobReq, taskName string) (tasks.Signature, error) {
	task, ok := co.tasks[taskName]
	if !ok {
		return tasks.Signature{}, fmt.Errorf("unrecognized task: %s", taskName)
	}

	// Check if a job with the same ID is already running.
	if s, err := co.q.GetBackend().GetState(j.JobID); err == nil && !s.IsCompleted() {
		return tasks.Signature{}, fmt.Errorf("job '%s' is already running", j.JobID)
	}

	// If there's no job_id, we generate one. This is because
	// the ID Machinery generates is not made available inside the
	// actual task. So, we generate the ID and pass it as an argument
	// to the task itself.
	if j.JobID == "" {
		uid, err := uuid.NewV4()
		if err != nil {
			return tasks.Signature{}, fmt.Errorf("error generating uuid: %v", err)
		}

		j.JobID = fmt.Sprintf("job_%v", uid)
	}

	ttl := co.opt.DefaultJobTTL
	if j.TTL > 0 {
		ttl = time.Duration(j.TTL) * time.Second
	}

	// Task arguments.
	args := append([]tasks.Arg{
		// Machinery will refelect on these and pass them as arguments
		// to the callback registered using RegisterTask() when a task
		// is executed.
		{Type: "string", Value: j.JobID},
		{Type: "string", Value: taskName},
		{Type: "string", Value: j.DB},
		{Type: "int", Value: ttl.Seconds()},
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

// initQueue creates and returns a distributed queue system (Machinery) and registers
// Tasks (SQL queries) to be executed. The queue system uses a broker (eg: Kafka) and stores
// job states in a state store (eg: Redis)
func (co *Core) initQueue() (*machinery.Server, error) {
	qs, err := machinery.NewServer(&config.Config{
		Broker:          co.opt.QueueBrokerDSN,
		ResultBackend:   co.opt.QueueStateDSN,
		DefaultQueue:    co.opt.DefaultQueue,
		ResultsExpireIn: int(co.opt.QueueStateTTL.Milliseconds()),
	})
	if err != nil {
		return nil, err
	}

	// Register every SQL ready tasks in the queue system as a job function.
	for name, query := range co.tasks {
		qs.RegisterTask(string(name), func(t Task) taskFunc {
			return func(jobID, taskName, db string, ttl int, args ...interface{}) (int64, error) {
				// Check if the job's been deleted.
				if _, err := co.q.GetBackend().GetState(jobID); err != nil {
					return 0, fmt.Errorf("skipping deleted job: %v", err)
				}

				// When the job is invoked, run the actual DB read + result write job.
				return co.execJob(jobID, taskName, db, time.Duration(ttl)*time.Second, args, t)
			}
		}(query))
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
	co.lo.Printf("sending results form '%s' to '%s'", jobID, name)

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

// sliceToTaskArgs takes a url.Values{} and returns a typed
// machinery Task Args list.
func sliceToTaskArgs(a []string) []tasks.Arg {
	var args []tasks.Arg

	for _, v := range a {
		args = append(args, tasks.Arg{
			Type:  "string",
			Value: v,
		})
	}

	return args
}
