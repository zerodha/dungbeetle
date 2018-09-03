package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/go-chi/chi"
)

// groupConcurrency represents the concurrency factor for job groups.
const groupConcurrency = 5

// regexValidateName represents the character classes allowed in a job ID.
var regexValidateName, _ = regexp.Compile("(?i)^[a-z0-9-_:]+$")

type jobReq struct {
	TaskName string   `json:"task"`
	JobID    string   `json:"job_id"`
	Queue    string   `json:"queue"`
	ETA      string   `json:"eta"`
	Retries  int      `json:"retries"`
	TTL      int      `json:"ttl"`
	Args     []string `json:"args"`

	ttlDuration time.Duration
}

type groupReq struct {
	GroupID     string   `json:"group_id"`
	Concurrency int      `json:"concurrency"`
	Jobs        []jobReq `json:"jobs"`
}

type jobResp struct {
	JobID    string     `json:"job_id"`
	TaskName string     `json:"task"`
	Queue    string     `json:"queue"`
	ETA      *time.Time `json:"eta"`
	Retries  int        `json:"retries"`
}

type groupResp struct {
	GroupID string    `json:"group_id"`
	Jobs    []jobResp `json:"jobs"`
}

type groupStatusResp struct {
	GroupID string          `json:"group_id"`
	State   string          `json:"state"`
	Jobs    []jobStatusResp `json:"jobs"`
}

type jobStatusResp struct {
	JobID   string              `json:"job_id"`
	State   string              `json:"state"`
	Results []*tasks.TaskResult `json:"results"`
	Error   string              `json:"error"`
}

type httpResp struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data"`
}

// handleGetTasksList returns the jobs list. If the optional query param ?sql=1
// is passed, it returns the raw SQL bodies as well.
func handleGetTasksList(w http.ResponseWriter, r *http.Request) {
	// Just the names.
	if r.URL.Query().Get("sql") == "" {
		sendResponse(w, jobber.Machinery.GetRegisteredTaskNames())
		return
	}

	sendResponse(w, jobber.Tasks)
}

// handleGetJobStatus returns the status of a given jobID.
func handleGetJobStatus(w http.ResponseWriter, r *http.Request) {
	out, err := jobber.Machinery.GetBackend().GetState(chi.URLParam(r, "jobID"))
	if err != nil {
		sysLog.Printf("error fetching job status: %v", err)
		sendErrorResponse(w, "error fetching job status", http.StatusInternalServerError)
		return
	}

	sendResponse(w, jobStatusResp{
		JobID:   out.TaskUUID,
		State:   out.State,
		Results: out.Results,
		Error:   out.Error,
	})
}

// handleGetGroupStatus returns the status of a given groupID.
func handleGetGroupStatus(w http.ResponseWriter, r *http.Request) {
	var (
		groupID = chi.URLParam(r, "groupID")
	)

	res, err := jobber.Machinery.GetBackend().GroupTaskStates(groupID, 0)
	if err != nil {
		sysLog.Printf("error fetching group status: %v", err)
		sendErrorResponse(w, "error fetching group status", http.StatusInternalServerError)
		return

	}

	var (
		jobs        = make([]jobStatusResp, len(res))
		jobsDone    = 0
		groupFailed = false
	)
	for i, j := range res {
		jobs[i] = jobStatusResp{
			JobID:   j.TaskUUID,
			State:   j.State,
			Results: j.Results,
			Error:   j.Error,
		}

		if j.State == tasks.StateSuccess {
			jobsDone++
		} else if j.State == tasks.StateFailure {
			groupFailed = true
		}
	}

	var groupStatus string
	if groupFailed {
		groupStatus = tasks.StateFailure
	} else if len(res) == jobsDone {
		groupStatus = tasks.StateSuccess
	} else {
		groupStatus = tasks.StatePending
	}

	out := groupStatusResp{
		GroupID: groupID,
		State:   groupStatus,
		Jobs:    jobs,
	}

	sendResponse(w, out)
}

// handleGetPendingJobs returns pending jobs in a given queue.
func handleGetPendingJobs(w http.ResponseWriter, r *http.Request) {
	out, err := jobber.Machinery.GetBroker().GetPendingTasks(chi.URLParam(r, "queue"))
	if err != nil {
		sysLog.Printf("error fetching pending tasks: %v", err)
		sendErrorResponse(w, "error fetching pending tasks", http.StatusInternalServerError)
		return
	}

	sendResponse(w, out)
}

// handlePostJob creates a new job against a given task name.
func handlePostJob(w http.ResponseWriter, r *http.Request) {
	var (
		taskName = chi.URLParam(r, "taskName")
		job      jobReq
	)

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&job); err != nil {
		sysLog.Printf("error parsing JSON body: %v", err)
		sendErrorResponse(w, "error parsing JSON body", http.StatusBadRequest)
		return
	}

	if !regexValidateName.Match([]byte(job.JobID)) {
		sendErrorResponse(w, "Invalid characters in the `job_id`.", http.StatusBadRequest)
		return
	}

	// Create the job signature.
	sig, err := createJobSignature(job, taskName, job.TTL, jobber)
	if err != nil {
		sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Create the job.
	res, err := jobber.Machinery.SendTask(&sig)
	if err != nil {
		sysLog.Printf("error posting job: %v", err)
		sendErrorResponse(w, "error posting job", http.StatusInternalServerError)
		return
	}

	sendResponse(w, jobResp{
		JobID:    res.Signature.UUID,
		TaskName: res.Signature.Name,
		Queue:    res.Signature.RoutingKey,
		Retries:  res.Signature.RetryCount,
		ETA:      res.Signature.ETA,
	})
}

// handlePostJobGroup creates a new job against a given task name.
func handlePostJobGroup(w http.ResponseWriter, r *http.Request) {
	var group groupReq

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&group); err != nil {
		sysLog.Printf("error parsing JSON body: %v", err)
		sendErrorResponse(w, "error parsing JSON body", http.StatusBadRequest)
		return
	}

	// Create job signatures for all the jobs in the group.
	var sigs []*tasks.Signature
	for _, j := range group.Jobs {
		sig, err := createJobSignature(j, j.TaskName, j.TTL, jobber)
		if err != nil {
			sysLog.Printf("error creating job signature: %v", err)
			sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
			return
		}

		sigs = append(sigs, &sig)
	}

	conc := groupConcurrency
	if group.Concurrency > 0 {
		conc = group.Concurrency
	}

	// Create the group and send it.
	taskGroup, _ := tasks.NewGroup(sigs...)

	// If there's an incoming group ID, overwrite the generated one.
	if group.GroupID != "" {
		taskGroup.GroupUUID = group.GroupID

		for _, t := range taskGroup.Tasks {
			t.GroupUUID = group.GroupID
		}
	}

	res, err := jobber.Machinery.SendGroup(taskGroup, conc)
	if err != nil {
		sysLog.Printf("error posting job group: %v", err)
		sendErrorResponse(w, "error posting job group", http.StatusInternalServerError)
		return
	}

	jobs := make([]jobResp, len(res))
	for i, r := range res {
		jobs[i] = jobResp{
			JobID:    r.Signature.UUID,
			TaskName: r.Signature.Name,
			Queue:    r.Signature.RoutingKey,
			Retries:  r.Signature.RetryCount,
			ETA:      r.Signature.ETA,
		}
	}

	gID := group.GroupID
	if gID == "" {
		gID = res[0].Signature.GroupUUID
	}

	out := groupResp{
		GroupID: gID,
		Jobs:    jobs,
	}

	sendResponse(w, out)
}

// handleDeleteJob deletes a job from the job queue if it's not already
// running. This is not foolproof as it's possible that right after
// the isRunning? check, and right before the deletion, the job could've
// started running.
func handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	var (
		jobID  = chi.URLParam(r, "jobID")
		s, err = jobber.Machinery.GetBackend().GetState(jobID)
	)
	if err != nil {
		sysLog.Printf("error fetching job: %v", err)
		sendErrorResponse(w, "error fetching job", http.StatusInternalServerError)
		return
	}

	// If the job is already complete, no go.
	if s.IsCompleted() {
		sendErrorResponse(w, "can't delete job as it's already complete", http.StatusGone)
		return
	}

	// Stop the job if it's running.
	jobMutex.RLock()
	cancel, ok := jobContexts[jobID]
	jobMutex.RUnlock()
	if ok {
		cancel()
	}

	// Delete the job.
	if err := jobber.Machinery.GetBackend().PurgeState(jobID); err != nil {
		sysLog.Printf("error deleting job: %v", err)
		sendErrorResponse(w, fmt.Sprintf("error deleting job: %v", err), http.StatusGone)
		return
	}

	sendResponse(w, true)
}

// sendErrorResponse sends a JSON envelope to the HTTP response.
func sendResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	out, err := json.Marshal(httpResp{Status: "success", Data: data})
	if err != nil {
		sendErrorResponse(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Write(out)
}

// sendErrorResponse sends a JSON error envelope to the HTTP response.
func sendErrorResponse(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)

	resp := httpResp{Status: "error", Message: message}
	out, _ := json.Marshal(resp)

	w.Write(out)
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
