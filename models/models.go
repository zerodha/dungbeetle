package models

import (
	"time"
)

// JobRequest represents a job request with metadata and execution details.
type JobRequest struct {
	TaskName    string        `json:"task"`
	JobID       string        `json:"job_id"`
	Queue       string        `json:"queue"`
	ETA         *time.Time    `json:"eta"`
	Retries     int           `json:"retries"`
	TTL         int           `json:"ttl"`
	Args        []string      `json:"args"`
	DB          string        `json:"db"`
	TTLDuration time.Duration `json:"-"` // Internal field, not serialized to JSON
}

// JobResponse represents the response sent after processing a job.
type JobResponse struct {
	JobID    string     `json:"job_id"`
	TaskName string     `json:"task"`
	Queue    string     `json:"queue"`
	ETA      *time.Time `json:"eta"`
	Retries  int        `json:"retries"`
}

// JobStatusResponse represents the status of a single job.
type JobStatusResponse struct {
	JobID string `json:"job_id"`
	State string `json:"state"`
	Count int    `json:"count"`
	Error string `json:"error,omitempty"`
}

// GroupRequest represents a group of job requests.
type GroupRequest struct {
	GroupID string       `json:"group_id"`
	Jobs    []JobRequest `json:"jobs"`
}

// GroupResponse represents the response for a group job request.
type GroupResponse struct {
	GroupID string        `json:"group_id"`
	Jobs    []JobResponse `json:"jobs"`
}

// GroupStatusResponse represents the status of a group job.
type GroupStatusResponse struct {
	GroupID string              `json:"group_id"`
	State   string              `json:"state"`
	Jobs    []JobStatusResponse `json:"jobs"`
}

// HTTPResponse represents a container for generic outgoing HTTP responses.
type HTTPResponse struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data"`
}

// NewJobRequest creates a new JobRequest with proper initialization.
func NewJobRequest(taskName, jobID, queue, db string, eta *time.Time, retries, ttl int, args []string) JobRequest {
	return JobRequest{
		TaskName: taskName,
		JobID:    jobID,
		Queue:    queue,
		ETA:      eta,
		Retries:  retries,
		TTL:      ttl,
		Args:     args,
		DB:       db,
	}
}

// NewGroupRequest creates a new GroupRequest with proper initialization.
func NewGroupRequest(groupID string, jobs []JobRequest) GroupRequest {
	return GroupRequest{
		GroupID: groupID,
		Jobs:    jobs,
	}
}
