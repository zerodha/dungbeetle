package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/go-chi/chi"
)

type httpResp struct {
	Status  string      `json:"status"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

type jobResp struct {
	JobID    string     `json:"job_id"`
	TaskName string     `json:"task_name"`
	Queue    string     `json:"queue"`
	ETA      *time.Time `json:"eta"`
	Retries  int        `json:"retries"`
}

type jobStatusResp struct {
	JobID   string              `json:"job_id"`
	State   string              `json:"status"`
	Results []*tasks.TaskResult `json:"results"`
	Error   string              `json:"error"`
}

// validateTaskName middleware validates task names in incoming task requests.
func validateURLParams(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		v := chi.URLParam(r, "taskName")
		if v != "" {
			if _, ok := jobber.Queries[v]; !ok {
				sendErrorResponse(w, "Unrecognised task name.", http.StatusBadRequest)
				return
			}
		}

		next.ServeHTTP(w, r)
	}
}

// handleGetTasks returns the jobs list. If the optional query param ?sql=1
// is passed, it returns the raw SQL bodies as well.
func handleGetTasks(w http.ResponseWriter, r *http.Request) {
	// Just the names.
	if r.URL.Query().Get("sql") == "" {
		sendResponse(w, jobber.Machinery.GetRegisteredTaskNames())
		return
	}

	sendResponse(w, jobber.Queries)
}

// handleGetJob returns the status of a given jobID.
func handleGetJob(w http.ResponseWriter, r *http.Request) {
	out, err := jobber.Machinery.GetBackend().GetState(chi.URLParam(r, "jobID"))
	if err != nil {
		sendErrorResponse(w,
			fmt.Sprintf("Error fetching task status: %v", err),
			http.StatusInternalServerError)
		return
	}

	sendResponse(w, jobStatusResp{
		JobID:   out.TaskUUID,
		State:   out.State,
		Results: out.Results,
		Error:   out.Error,
	})
}

// handleGetJobs returns pending jobs in a given queue.
func handleGetJobs(w http.ResponseWriter, r *http.Request) {
	out, err := jobber.Machinery.GetBroker().GetPendingTasks(chi.URLParam(r, "queue"))
	if err != nil {
		sendErrorResponse(w,
			fmt.Sprintf("Error fetching pending tasks from queue: %v", err),
			http.StatusInternalServerError)
		return
	}

	sendResponse(w, out)
}

// handlePostJob creates a new job against a given task name.
func handlePostJob(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	var (
		taskName = chi.URLParam(r, "taskName")
		jobID    = r.Form.Get("job_id")

		err error
	)

	// Check if a job with the same ID is already running.
	if s, err := jobber.Machinery.GetBackend().GetState(jobID); err == nil && !s.IsCompleted() {
		sendErrorResponse(w, "Error posting job. A job with the same ID is already running.",
			http.StatusInternalServerError)
		return
	}

	// If there's no job_id, we generate one. This is because
	// the ID Machinery generates is not made available inside the
	// actual task. So, we generate the ID and pass it as an argument
	// to the task itself.
	if jobID == "" {
		jobID, err = generateRandomString(32)
		if err != nil {
			sendErrorResponse(w, fmt.Sprintf("Error generating job_id: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Task arguments.
	args := append([]tasks.Arg{
		// First two arguments have to be jobID and taskName.
		{Type: "string", Value: jobID},
		{Type: "string", Value: taskName},
	}, formToArgs(r.PostForm)...)

	// Retries.
	retries, _ := strconv.Atoi(r.Form.Get("retries"))

	// ETA.
	var eta *time.Time
	if r.Form.Get("eta") != "" {
		e, err := time.Parse("2006-01-02 15:04:05", r.Form.Get("eta"))
		if err != nil {
			sendErrorResponse(w, fmt.Sprintf("Error parsing eta timestamp: %v", err), http.StatusInternalServerError)
			return
		}

		eta = &e
	}

	out, err := jobber.Machinery.SendTask(&tasks.Signature{
		Name:       taskName,
		UUID:       jobID,
		RoutingKey: r.Form.Get("queue"),
		RetryCount: retries,
		Args:       args,
		ETA:        eta,
	})
	if err != nil {
		sendErrorResponse(w, fmt.Sprintf("Error posting job: %v", err), http.StatusInternalServerError)
		return
	}

	sendResponse(w, jobResp{
		JobID:    out.Signature.UUID,
		TaskName: out.Signature.Name,
		Queue:    out.Signature.RoutingKey,
		Retries:  out.Signature.RetryCount,
		ETA:      out.Signature.ETA,
	})
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
		sendErrorResponse(w, fmt.Sprintf("Error fetching job: %v", err), http.StatusInternalServerError)
		return
	}

	// If the job is already complete, no go.
	if s.IsCompleted() {
		sendErrorResponse(w, "Can't delete job as it's already complete.", http.StatusGone)
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
		sendErrorResponse(w, fmt.Sprintf("Error deleting job: %v", err), http.StatusGone)
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

	resp := httpResp{Message: message}
	out, _ := json.Marshal(resp)

	w.Write(out)
}

// formToArgs takes a url.Values{} and returns a typed
// machinery Task Args list.
func formToArgs(params url.Values) []tasks.Arg {
	var args []tasks.Arg

	for k, vals := range params {
		if k == "arg" {
			for _, v := range vals {
				args = append(args, tasks.Arg{
					Type:  "string",
					Value: v,
				})
			}
		}
	}

	return args
}

// generateRandomString generates a random string.
func generateRandomString(n int) (string, error) {
	const dictionary = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

	var bytes = make([]byte, n)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}

	for k, v := range bytes {
		bytes[k] = dictionary[v%byte(len(dictionary))]
	}

	return string(bytes), nil
}
