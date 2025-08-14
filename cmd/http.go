package main

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/zerodha/dungbeetle/v2/models"
)

// reValidateName represents the character classes allowed in a job ID.
var reValidateName = regexp.MustCompile("(?i)^[a-z0-9-_:]+$")

// GetTasksList returns the jobs list. If the optional query param ?sql=1
// is passed, it returns the raw SQL bodies as well.
func (a *Handlers) GetTasksList(w http.ResponseWriter, r *http.Request) {
	tasks := a.co.GetTasks()

	// Full task including SQL queries.
	if r.URL.Query().Get("sql") == "" {
		a.sendResponse(w, tasks)
		return
	}

	// Just the names.
	out := make([]string, 0, len(tasks))
	for name := range tasks {
		out = append(out, name)
	}

	a.sendResponse(w, out)
}

// handleGetJobStatus returns the status of a given jobID.
func (a *Handlers) handleGetJobStatus(w http.ResponseWriter, r *http.Request) {
	var jobID = chi.URLParam(r, "jobID")

	out, err := a.co.GetJobStatus(jobID)
	if err != nil {
		a.log.Error("could not get job status", "error", err, "job_id", jobID)
		a.sendErrorResponse(w, "job not found", http.StatusNotFound)
		return
	}

	a.sendResponse(w, out)
}

// handleGetGroupStatus returns the status of a given groupID.
func (a *Handlers) handleGetGroupStatus(w http.ResponseWriter, r *http.Request) {
	var groupID = chi.URLParam(r, "groupID")

	out, err := a.co.GetJobGroupStatus(groupID)
	if err != nil {
		a.log.Error("could not get group job status", "error", err, "group_id", groupID)
		a.sendErrorResponse(w, err.Error(), http.StatusNotFound)
		return
	}

	a.sendResponse(w, out)
}

// handleGetPendingJobs returns pending jobs in a given queue.
func (a *Handlers) handleGetPendingJobs(w http.ResponseWriter, r *http.Request) {
	var queue = chi.URLParam(r, "queue")

	out, err := a.co.GetPendingJobs(queue)
	if err != nil {
		a.log.Error("could not get pending jobs", "error", err, "queue", queue)
		a.sendErrorResponse(w, "error fetching pending tasks", http.StatusInternalServerError)
		return
	}

	a.sendResponse(w, out)
}

// handlePostJob creates a new job against a given task name.
func (a *Handlers) handlePostJob(w http.ResponseWriter, r *http.Request) {
	var taskName = chi.URLParam(r, "taskName")

	if r.ContentLength == 0 {
		a.log.Error("request body sent empty")
		a.sendErrorResponse(w, "request body is empty", http.StatusBadRequest)
		return
	}

	var (
		decoder = json.NewDecoder(r.Body)
		req     models.JobReq
	)
	if err := decoder.Decode(&req); err != nil {
		a.log.Error("error parsing request JSON", "error", err, "task_name", taskName, "request", req)
		a.sendErrorResponse(w, "error parsing request JSON", http.StatusBadRequest)
		return
	}

	if !reValidateName.Match([]byte(req.JobID)) {
		a.sendErrorResponse(w, "invalid characters in the `job_id`", http.StatusBadRequest)
		return
	}

	// Create the job signature.
	out, err := a.co.NewJob(req, taskName)
	if err != nil {
		a.log.Error("could not create new job", "error", err, "task_name", taskName, "request", req)
		a.sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	a.sendResponse(w, out)
}

// handlePostJobGroup creates multiple jobs under a group.
func (a *Handlers) handlePostJobGroup(w http.ResponseWriter, r *http.Request) {
	var (
		decoder = json.NewDecoder(r.Body)
		req     models.GroupReq
	)

	if err := decoder.Decode(&req); err != nil {
		a.log.Error("error parsing JSON body", "error", err, "request", req)
		a.sendErrorResponse(w, "error parsing JSON body", http.StatusBadRequest)
		return
	}

	out, err := a.co.NewJobGroup(req)
	if err != nil {
		a.log.Error("error creating job signature", "error", err, "request", req)
		a.sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	a.sendResponse(w, out)
}

// handleCancelJob deletes a job from the job queue. If the job is running,
// it is cancelled first and then deleted.
func (a *Handlers) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	var (
		jobID    = chi.URLParam(r, "jobID")
		purge, _ = strconv.ParseBool(r.URL.Query().Get("purge"))
	)

	if err := a.co.CancelJob(jobID, purge); err != nil {
		a.log.Error("could not cancel job", "error", err, "job_id", jobID)
		a.sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}

	a.sendResponse(w, true)
}

// handleCancelGroupJob deletes all the jobs of a group along with the group.
// If the job is running, it is cancelled first, and then deleted.
func (a *Handlers) handleCancelGroupJob(w http.ResponseWriter, r *http.Request) {
	var (
		groupID  = chi.URLParam(r, "groupID")
		purge, _ = strconv.ParseBool(r.URL.Query().Get("purge"))
	)

	// Get state of group.
	if err := a.co.CancelJobGroup(groupID, purge); err != nil {
		a.log.Error("could not cancel group job", "error", err, "group_id", groupID)
		a.sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	a.sendResponse(w, true)
}

// sendResponse sends a JSON envelope to the HTTP response.
func (a *Handlers) sendResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	out, err := json.Marshal(models.HTTPResp{Status: "success", Data: data})
	if err != nil {
		a.log.Error("could marshal response", "error", err)
		a.sendErrorResponse(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Write(out)
}

// sendErrorResponse sends a JSON error envelope to the HTTP response.
func (a *Handlers) sendErrorResponse(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)

	resp := models.HTTPResp{Status: "error", Message: message}
	out, _ := json.Marshal(resp)

	w.Write(out)
}
