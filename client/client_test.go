package client

import (
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zerodha/dungbeetle/models"
)

var (
	cl   *Client
	eta  = time.Now().Add(time.Hour)
	jobs = []models.JobReq{{
		JobID:    "job1",
		TaskName: "get_profit_summary",
		Queue:    "test",
		Retries:  3,
	}, {
		JobID:    "job2",
		TaskName: "get_profit_summary",
		Queue:    "test",
		Retries:  3,
	}}
)

func init() {
	cl = New(&Opt{
		RootURL: "http://127.0.0.1:6060",
		HTTPClient: &http.Client{
			Timeout: time.Second * 5,
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   10,
				ResponseHeaderTimeout: time.Second * 5,
			},
		},
		Logger: slog.Default(),
	})
}

func TestPostJob(t *testing.T) {
	for _, j := range jobs {
		r, err := cl.PostJob(j)
		assert.NoError(t, err, "error posting job")
		assert.Equal(t, j.JobID, r.JobID)
	}
}

func TestGetJobStatus(t *testing.T) {
	_, err := cl.GetJobStatus("aaaa")
	assert.Error(t, err)
	for _, j := range jobs {
		r, err := cl.GetJobStatus(j.JobID)
		assert.NoError(t, err, "error getting job status")
		assert.Equal(t, j.JobID, r.JobID)
	}
}

func TestGetPendingJobs(t *testing.T) {
	r, err := cl.GetPendingJobs("test")
	assert.NoError(t, err, "error fetching pending jobs")
	assert.Equal(t, len(jobs), len(r), "incorrect number of pending jobs")
	for i, j := range jobs {
		assert.Equal(t, j.JobID, r[i].ID, "job name doesn't match")
	}
}

func TestDeleteJob(t *testing.T) {
	for _, j := range jobs {
		err := cl.DeleteJob(j.JobID, false)
		assert.NoError(t, err, "error deleting job")
	}
}

func TestPostJobGroup(t *testing.T) {
	_, err := cl.PostJobGroup(models.GroupReq{
		GroupID: "testgroup",
		Jobs:    jobs,
	})
	assert.NoError(t, err, "error posting job group")
}
func TestGetJobGroupStatus(t *testing.T) {
	r, err := cl.GetGroupStatus("testgroup")
	assert.NoError(t, err, "error getting job group status")
	for i, j := range jobs {
		assert.Equal(t, j.JobID, r.Jobs[i].JobID)
	}
}
