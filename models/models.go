package models

import (
	"time"
)

// JobReq represents a job request.
type JobReq struct {
	TaskName string   `json:"task"`
	JobID    string   `json:"job_id"`
	Queue    string   `json:"queue"`
	ETA      string   `json:"eta"`
	Retries  int      `json:"retries"`
	TTL      int      `json:"ttl"`
	Args     []string `json:"args"`
	DB       string   `json:"db"`

	// Optional params
	DynamicParams []string `json:"dynamic_params"`

	ttlDuration time.Duration
}

// JobResp is the response sent to a job request.
type JobResp struct {
	JobID    string     `json:"job_id"`
	TaskName string     `json:"task"`
	Queue    string     `json:"queue"`
	ETA      *time.Time `json:"eta"`
	Retries  int        `json:"retries"`
}

// JobStatusResp represents the response of a single job.
type JobStatusResp struct {
	JobID string `json:"job_id"`
	State string `json:"state"`
	Count int    `json:"count"`
	Error string `json:"error"`
}

// GroupReq represents a Jobrequest group.
type GroupReq struct {
	GroupID string   `json:"group_id"`
	Jobs    []JobReq `json:"jobs"`
}

// GroupResp is the response sent to a group job request.
type GroupResp struct {
	GroupID string    `json:"group_id"`
	Jobs    []JobResp `json:"jobs"`
}

// GroupStatusResp represents the status of a group job.
type GroupStatusResp struct {
	GroupID string          `json:"group_id"`
	State   string          `json:"state"`
	Jobs    []JobStatusResp `json:"jobs"`
}

// HTTPResp represents a container for generic outgoing HTTP
// responses.
type HTTPResp struct {
	Status  string      `json:"status"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data"`
}
