package client

import (
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zerodha/dungbeetle/internal/core"
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
	},
	}
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

func TestSlowQuery(t *testing.T) {
	group := models.GroupReq{
		GroupID: "slow_group",
		Jobs: []models.JobReq{{
			JobID:    "job3",
			TaskName: "slow_query",
			Queue:    "test",
			Retries:  3,
			// 5 here means the duration (in seconds) the query will
			// take to execute
			Args: []string{"4"},
		},
		},
	}

	// Submit the group
	r, err := cl.PostJobGroup(group)
	assert.NoError(t, err, "error posting group")
	assert.Equal(t, group.GroupID, r.GroupID)

	var (
		count = 0
		// To check for status every second
		tk = time.NewTicker(time.Second)
		// Check for result max twice
		checkCount = 2
		// The ctx that will trigger after the job is done
		ctx, _ = context.WithTimeout(context.Background(), time.Second*5)
	)

	for {
		select {
		case <-tk.C:
			if count >= checkCount {
				tk.Stop()
			}
			resp, err := cl.GetGroupStatus(r.GroupID)
			assert.NoError(t, err, "error getting group status")
			assert.Equal(t, resp.State, core.StatusPending)
			count++
		case <-ctx.Done():
			resp, err := cl.GetGroupStatus(r.GroupID)
			assert.NoError(t, err, "error getting group status")
			assert.Equal(t, resp.State, core.StatusSuccess)
		}
	}
	//assert.Equal(t, group.GroupID, r.GroupID)
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
