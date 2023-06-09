package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/gomodule/redigo/redis"
	"github.com/kalbhor/tasqueue/v2"
	"github.com/knadh/sql-jobber/models"
)

// regexValidateName represents the character classes allowed in a job ID.
var regexValidateName, _ = regexp.Compile("(?i)^[a-z0-9-_:]+$")

// handleGetTasksList returns the jobs list. If the optional query param ?sql=1
// is passed, it returns the raw SQL bodies as well.
func handleGetTasksList(w http.ResponseWriter, r *http.Request) {
	// Just the names.
	if r.URL.Query().Get("sql") == "" {
		sendResponse(w, jobber.Tasqueue.GetTasks())
		return
	}

	sendResponse(w, jobber.Tasks)
}

// handleGetJobStatus returns the status of a given jobID.
func handleGetJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := chi.URLParam(r, "jobID")
	out, err := jobber.Tasqueue.GetJob(r.Context(), jobID)
	if err == redis.ErrNil {
		sendErrorResponse(w, "job not found", http.StatusNotFound)
		return
	} else if err != nil {
		sLog.Printf("error fetching job status: %v", err)
		sendErrorResponse(w, "error fetching job status", http.StatusInternalServerError)
		return
	}

	b, err := jobber.Tasqueue.GetResult(r.Context(), jobID)
	if err != nil && !errors.Is(redis.ErrNil, err) {
		sLog.Printf("error fetching job status: %v", err)
		sendErrorResponse(w, "error fetching job status", http.StatusInternalServerError)
		return
	}

	state, err := getState(out.Status)
	if err != nil {
		sLog.Printf("error fetching job status: %v", err)
		sendErrorResponse(w, "error fetching job status", http.StatusInternalServerError)
		return
	}

	sendResponse(w, models.JobStatusResp{
		JobID:   out.ID,
		State:   state,
		Results: b,
		Error:   out.PrevErr,
	})
}

// handleGetGroupStatus returns the status of a given groupID.
func handleGetGroupStatus(w http.ResponseWriter, r *http.Request) {
	groupID := chi.URLParam(r, "groupID")

	groupMsg, _ := jobber.Tasqueue.GetGroup(r.Context(), groupID)

	var jobs []models.JobStatusResp

	for uuid, status := range groupMsg.JobStatus {
		res, err := jobber.Tasqueue.GetResult(r.Context(), uuid)
		if err != nil && !errors.Is(redis.ErrNil, err) {
			sLog.Printf("error fetching job status: %v", err)
			sendErrorResponse(w, "error fetching job status", http.StatusInternalServerError)
			return
		}

		jMsg, err := jobber.Tasqueue.GetJob(r.Context(), uuid)
		if err != nil {
			sLog.Printf("error fetching job: %v", err)
			sendErrorResponse(w, "error fetching job", http.StatusInternalServerError)
			return
		}

		state, err := getState(status)
		if err != nil {
			sLog.Printf("error fetching job status: %v", err)
			sendErrorResponse(w, "error fetching group status", http.StatusInternalServerError)
			return
		}

		jobs = append(jobs, models.JobStatusResp{
			JobID:   uuid,
			State:   state,
			Results: res,
			Error:   jMsg.PrevErr,
		})

	}

	state, err := getState(groupMsg.Status)
	if err != nil {
		sLog.Printf("error fetching group status: %v", err)
		sendErrorResponse(w, "error fetching group status", http.StatusInternalServerError)
		return
	}

	out := models.GroupStatusResp{
		GroupID: groupID,
		State:   state,
		Jobs:    jobs,
	}

	sendResponse(w, out)
}

// handleGetPendingJobs returns pending jobs in a given queue.
func handleGetPendingJobs(w http.ResponseWriter, r *http.Request) {
	out, err := jobber.Tasqueue.GetPending(r.Context(), chi.URLParam(r, "queue"))
	if err != nil {
		sLog.Printf("error fetching pending tasks: %v", err)
		sendErrorResponse(w, "error fetching pending tasks", http.StatusInternalServerError)
		return
	}

	sendResponse(w, out)
}

// handlePostJob creates a new job against a given task name.
func handlePostJob(w http.ResponseWriter, r *http.Request) {
	var (
		taskName = chi.URLParam(r, "taskName")
		job      models.JobReq
	)

	job.TaskName = taskName

	if r.ContentLength == 0 {
		sendErrorResponse(w, "request body is empty", http.StatusBadRequest)
		return
	}

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&job); err != nil {
		sLog.Printf("error parsing request JSON: %v", err)
		sendErrorResponse(w, "error parsing request JSON", http.StatusBadRequest)
		return
	}

	if !regexValidateName.Match([]byte(job.JobID)) {
		sendErrorResponse(w, "invalid characters in the `job_id`", http.StatusBadRequest)
		return
	}

	t, err := createJob(job, taskName, job.DB, job.TTL, jobber)
	if err != nil {
		sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jobID, err := jobber.Tasqueue.Enqueue(r.Context(), t)
	if err != nil {
		sLog.Printf("error posting job: %v", err)
		sendErrorResponse(w, "error posting job", http.StatusInternalServerError)
		return
	}

	sendResponse(w, models.JobResp{
		JobID:    jobID,
		TaskName: t.Task,
		Queue:    t.Opts.Queue,
		Retries:  int(t.Opts.MaxRetries),
		ETA:      &t.Opts.ETA,
	})
}

// handlePostJobGroup creates multiple jobs under a group.
func handlePostJobGroup(w http.ResponseWriter, r *http.Request) {
	var (
		decoder = json.NewDecoder(r.Body)
		group   models.GroupReq
	)
	if err := decoder.Decode(&group); err != nil {
		sLog.Printf("error parsing JSON body: %v", err)
		sendErrorResponse(w, "error parsing JSON body", http.StatusBadRequest)
		return
	}

	// Create job signatures for all the jobs in the group.
	var jobs []tasqueue.Job

	for _, j := range group.Jobs {
		t, err := createJob(j, j.TaskName, j.DB, j.TTL, jobber)
		if err != nil {
			sendErrorResponse(w, err.Error(), http.StatusInternalServerError)
			return
		}

		jobs = append(jobs, t)
	}

	taskGroup, err := tasqueue.NewGroup(jobs, tasqueue.GroupOpts{ID: group.GroupID})
	if err != nil {
		sLog.Printf("error creating job group: %v", err)
		sendErrorResponse(w, "error posting job group", http.StatusInternalServerError)
		return
	}

	groupID, err := jobber.Tasqueue.EnqueueGroup(context.Background(), taskGroup)
	if err != nil {
		sLog.Printf("error posting job group: %v", err)
		sendErrorResponse(w, "error posting job group", http.StatusInternalServerError)
		return
	}

	var (
		groupMsg, _ = jobber.Tasqueue.GetGroup(context.Background(), groupID)
		jobResp     = make([]models.JobResp, len(groupMsg.Group.Jobs))
	)
	for uuid := range groupMsg.JobStatus {
		jmsg, err := jobber.Tasqueue.GetJob(r.Context(), uuid)
		if err != nil {
			sLog.Printf("error posting job group: %v", err)
			sendErrorResponse(w, "error posting job group", http.StatusInternalServerError)
			return
		}

		jobResp = append(jobResp, models.JobResp{
			JobID:    jmsg.ID,
			TaskName: jmsg.Job.Task,
			Queue:    jmsg.Queue,
			Retries:  int(jmsg.MaxRetry),
			ETA:      &jmsg.Job.Opts.ETA,
		})
	}

	out := models.GroupResp{
		GroupID: groupID,
		Jobs:    jobResp,
	}

	sendResponse(w, out)
}

// handleDeleteJob deletes a job from the job queue. If the job is running,
// it is cancelled first and then deleted.
func handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	var (
		jobID    = chi.URLParam(r, "jobID")
		s, err   = jobber.Tasqueue.GetJob(r.Context(), jobID)
		purge, _ = strconv.ParseBool(r.URL.Query().Get("purge"))
	)
	if err != nil {
		sLog.Printf("error fetching job: %v", err)
		sendErrorResponse(w, "error fetching job", http.StatusInternalServerError)
		return
	}

	if !purge && s.Status == "successful" {
		// If the job is already complete, no go.
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
	if err := jobber.Tasqueue.DeleteJob(context.Background(), jobID); err != nil {
		sLog.Printf("error deleting job: %v", err)
		sendErrorResponse(w, fmt.Sprintf("error deleting job: %v", err), http.StatusGone)
		return
	}

	sendResponse(w, true)
}

// handleDeleteGroupJob deletes all the jobs of a group along with the group.
// If the job is running, it is cancelled first, and then deleted.
func handleDeleteGroupJob(w http.ResponseWriter, r *http.Request) {
	var (
		groupID    = chi.URLParam(r, "groupID")
		purge, err = strconv.ParseBool(r.URL.Query().Get("purge"))
	)
	if err != nil {
		sLog.Printf("error parsing boolean: %v", err)
		sendErrorResponse(w, "error parsing boolean", http.StatusInternalServerError)
		return
	}

	// Get state of group.
	res, err := jobber.Tasqueue.GetGroup(r.Context(), groupID)
	if err != nil {
		sLog.Printf("error fetching group status: %v", err)
		sendErrorResponse(w, "error fetching group status", http.StatusInternalServerError)
		return
	}

	// If purge is set to false, delete job only if isn't completed yet.
	if !purge {
		// If the job is already complete, no go.
		if res.Status == "successful" {
			sendErrorResponse(w, "can't delete group as it's already complete", http.StatusGone)
			return
		}
	}

	// Loop through every job of the group and purge them.
	for uuid := range res.JobStatus {
		if err != nil {
			sLog.Printf("error fetching job: %v", err)
			sendErrorResponse(w, "error fetching job", http.StatusInternalServerError)
			return
		}

		// Stop the job if it's running.
		jobMutex.RLock()
		cancel, ok := jobContexts[uuid]
		jobMutex.RUnlock()
		if ok {
			cancel()
		}

		// Delete the job.
		if err := jobber.Tasqueue.DeleteJob(r.Context(), uuid); err != nil {
			sLog.Printf("error deleting job: %v", err)
			sendErrorResponse(w, fmt.Sprintf("error deleting job: %v", err), http.StatusGone)
			return
		}
	}

	sendResponse(w, true)
}

// sendErrorResponse sends a JSON envelope to the HTTP response.
func sendResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	out, err := json.Marshal(models.HTTPResp{Status: "success", Data: data})
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

	resp := models.HTTPResp{Status: "error", Message: message}
	out, _ := json.Marshal(resp)

	w.Write(out)
}
