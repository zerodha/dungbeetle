package main

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/zerodha/dungbeetle/internal/core"
	"github.com/zerodha/dungbeetle/models"
)

// reValidateName represents the character classes allowed in a job ID.
var reValidateName = regexp.MustCompile("(?i)^[a-z0-9-_:]+$")

// handleGetTasksList returns the jobs list. If the optional query param ?sql=1
// is passed, it returns the raw SQL bodies as well.
func handleGetTasksList(w http.ResponseWriter, r *http.Request) {
	var (
		co = r.Context().Value("core").(*core.Core)
	)

	tasks := co.GetTasks()

	// Full task including SQL queries.
	if r.URL.Query().Get("sql") == "" {
		sendResponse(w, tasks)
		return
	}

	// Just the names.
	out := make([]string, 0, len(tasks))
	for name := range tasks {
		out = append(out, name)
	}

	sendResponse(w, out)
}

// handleGetJobStatus returns the status of a given jobID.
func handleGetJobStatus(w http.ResponseWriter, r *http.Request) {
	var (
		co = r.Context().Value("core").(*core.Core)

		jobID = chi.URLParam(r, "jobID")
	)

	out, err := co.GetJobStatus(jobID)
	if err != nil {
		lo.Error("could not get job status", "error", err, "job_id", jobID)
		sendErrorResponse(w, "job not found", http.StatusNotFound)
		return
	}

	sendResponse(w, out)
}

// handleGetGroupStatus returns the status of a given groupID.
func handleGetGroupStatus(w http.ResponseWriter, r *http.Request) {
	var (
		co = r.Context().Value("core").(*core.Core)

		groupID = chi.URLParam(r, "groupID")
	)

	out, err := co.GetJobGroupStatus(groupID)
	if err != nil {
		lo.Error("could not get group job status", "error", err, "group_id", groupID)
		sendErrorResponse(w, err.Error(), http.StatusNotFound)
		return
	}

	sendResponse(w, out)
}

// handleGetPendingJobs returns pending jobs in a given queue.
func handleGetPendingJobs(w http.ResponseWriter, r *http.Request) {
	var (
		co    = r.Context().Value("core").(*core.Core)
		queue = chi.URLParam(r, "queue")
	)

	out, err := co.GetPendingJobs(queue)
	if err != nil {
		lo.Error("could not get pending jobs", "error", err, "queue", queue)
		sendErrorResponse(w, "error fetching pending tasks", http.StatusInternalServerError)
		return
	}

	sendResponse(w, out)
}

// handlePostJob creates a new job against a given task name.
func handlePostJob(w http.ResponseWriter, r *http.Request) {
	var (
		co = r.Context().Value("core").(*core.Core)

		taskName = chi.URLParam(r, "taskName")
	)

	if r.ContentLength == 0 {
		lo.Error("request body sent empty")
		sendErrorResponse(w, "request body is empty", http.StatusBadRequest)
		return
	}

	var (
		decoder = json.NewDecoder(r.Body)
		req     models.JobReq
	)
	if err := decoder.Decode(&req); err != nil {
		lo.Error("error parsing request JSON", "error", err, "task_name", taskName, "request", req)
		sendErrorResponse(w, "error parsing request JSON", http.StatusBadRequest)
		return
	}

	if !reValidateName.Match([]byte(req.JobID)) {
		sendErrorResponse(w, "invalid characters in the `job_id`", http.StatusBadRequest)
		return
	}

	// Create the job signature.
	out, err := co.NewJob(req, taskName)
	if err != nil {
		lo.Error("could not create new job", "error", err, "task_name", taskName, "request", req)
		sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sendResponse(w, out)
}

// handlePostJobGroup creates multiple jobs under a group.
func handlePostJobGroup(w http.ResponseWriter, r *http.Request) {
	var (
		co = r.Context().Value("core").(*core.Core)

		decoder = json.NewDecoder(r.Body)
		req     models.GroupReq
	)

	if err := decoder.Decode(&req); err != nil {
		lo.Error("error parsing JSON body", "error", err, "request", req)
		sendErrorResponse(w, "error parsing JSON body", http.StatusBadRequest)
		return
	}

	out, err := co.NewJobGroup(req)
	if err != nil {
		lo.Error("error creating job signature", "error", err, "request", req)
		sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sendResponse(w, out)
}

// handleCancelJob deletes a job from the job queue. If the job is running,
// it is cancelled first and then deleted.
func handleCancelJob(w http.ResponseWriter, r *http.Request) {
	var (
		co = r.Context().Value("core").(*core.Core)

		jobID    = chi.URLParam(r, "jobID")
		purge, _ = strconv.ParseBool(r.URL.Query().Get("purge"))
	)

	if err := co.CancelJob(jobID, purge); err != nil {
		lo.Error("could not cancel job", "error", err, "job_id", jobID)
		sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
	}

	sendResponse(w, true)
}

// handleCancelGroupJob deletes all the jobs of a group along with the group.
// If the job is running, it is cancelled first, and then deleted.
func handleCancelGroupJob(w http.ResponseWriter, r *http.Request) {
	var (
		co = r.Context().Value("core").(*core.Core)

		groupID  = chi.URLParam(r, "groupID")
		purge, _ = strconv.ParseBool(r.URL.Query().Get("purge"))
	)

	// Get state of group.
	if err := co.CancelJobGroup(groupID, purge); err != nil {
		lo.Error("could not cancel group job", "error", err, "group_id", groupID)
		sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	sendResponse(w, true)
}

// sendErrorResponse sends a JSON envelope to the HTTP response.
func sendResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	out, err := json.Marshal(models.HTTPResp{Status: "success", Data: data})
	if err != nil {
		lo.Error("could marshal response", "error", err)
		sendErrorResponse(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	w.Write(out)
}

// sendErrorResponse sends a JSON error envelope to the HTTP response.
func sendErrorResponse(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)

	resp := models.HTTPResp{Status: "error", Message: message}
	out, _ := json.Marshal(resp)

	w.Write(out)
}
