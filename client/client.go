package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/knadh/sql-jobber/models"
)

const (
	// %s = taskName
	uriPostJob = "/tasks/%s/jobs"

	// %s = jobID
	uriDeleteJob = "/jobs/%s"

	// %s = jobID
	uriGetJobStatus = "/jobs/%s"

	// %s = queue
	uriGetPendingJobs = "/jobs/queue/%s"

	uriPostJobGroup = "/groups"

	// %s = group
	uriGetGroupStatus = "/groups/%s"
)

type httpResp struct {
	Body     []byte
	Response *http.Response
}

// Opt represents the options required to initialize Client.
type Opt struct {
	RootURL    string
	HTTPClient *http.Client
	Logger     *log.Logger
}

// Client represents the SQL Jobber API client.
type Client struct {
	o *Opt
}

// New returns a new instance of Client.
func New(o *Opt) *Client {
	return &Client{
		o: o,
	}
}

// PostJob posts a new job.
func (c *Client) PostJob(j models.JobReq) (models.JobResp, error) {
	var out models.JobResp
	err := c.doHTTPReq(http.MethodPost,
		fmt.Sprintf(uriPostJob, j.TaskName), j, nil, &out)
	return out, err
}

// GetJobStatus fetches the status of a job.
func (c *Client) GetJobStatus(jobID string) (models.JobStatusResp, error) {
	var out models.JobStatusResp
	err := c.doHTTPReq(http.MethodGet,
		fmt.Sprintf(uriDeleteJob, jobID), nil, nil, &out)
	return out, err
}

// DeleteJob sends a request to delete a job.
func (c *Client) DeleteJob(jobID string) error {
	var out bool
	return c.doHTTPReq(http.MethodDelete,
		fmt.Sprintf(uriDeleteJob, jobID), nil, nil, &out)
}

// GetPendingJobs fetches the list of pending jobs.
func (c *Client) GetPendingJobs(queue string) ([]tasks.Signature, error) {
	var out []tasks.Signature
	err := c.doHTTPReq(http.MethodGet,
		fmt.Sprintf(uriGetPendingJobs, queue), nil, nil, &out)
	return out, err
}

// PostJobGroup posts multiple jobs under a group.
func (c *Client) PostJobGroup(jobs models.GroupReq) (models.GroupResp, error) {
	var out models.GroupResp
	err := c.doHTTPReq(http.MethodPost, uriPostJobGroup, jobs, nil, &out)
	return out, err
}

// GetGroupStatus fetches the status of a group of job.
func (c *Client) GetGroupStatus(groupID string) (models.GroupResp, error) {
	var out models.GroupResp
	err := c.doHTTPReq(http.MethodGet,
		fmt.Sprintf(uriGetGroupStatus, groupID), nil, nil, &out)
	return out, err
}

// doHTTPReq makes an HTTP request with the given params and on success, unmarshals
// the JSON response from the sql-jobber upstream (the .data field in the response JSON)
// into the container obj.
// reqBody can be an arbitrary struct or map for POST requests that'll be
// marshalled as JSON and posted. For GET queries, it should be query params
// encoded as string.
func (c *Client) doHTTPReq(method, rURI string, reqBody interface{}, headers http.Header, obj interface{}) error {
	var (
		err      error
		postBody io.Reader
	)

	// Encode POST / PUT params.
	if reqBody != nil && method == http.MethodPost || method == http.MethodPut {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("error marshalling request body: %v", err)
		}
		postBody = bytes.NewReader(b)
	}

	req, err := http.NewRequest(method, c.o.RootURL+rURI, postBody)
	if err != nil {
		return fmt.Errorf("request preparation failed: %v", err)
	}

	if headers != nil {
		req.Header = headers
	}

	// If a content-type isn't set, set the default one.
	if req.Header.Get("Content-Type") == "" {
		if method == http.MethodPost || method == http.MethodPut {
			req.Header.Add("Content-Type", "application/json")
		}
	}

	// If the request method is GET or DELETE, add the params as QueryString.
	if reqBody != nil && method == http.MethodGet || method == http.MethodDelete {
		s, ok := reqBody.(string)
		if ok {
			req.URL.RawQuery = s
			return errors.New("GET request param type should be string")
		}
	}

	r, err := c.o.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %v", err)
	}
	defer func() {
		// Drain and close the body to let the Transport reuse the connection
		io.Copy(ioutil.Discard, r.Body)
		r.Body.Close()
	}()

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("error reading response: %v", err)
	}

	// Unmarshal the JSON body into the given container.
	var resp models.HTTPResp
	resp.Data = obj
	if err := json.Unmarshal(body, &resp); err != nil {
		return fmt.Errorf("error unmarshaling JSON response: %v", err)
	}

	// If the response is non 200, unmarshal the error message.
	if r.StatusCode != http.StatusOK {
		if err := json.Unmarshal(body, &resp); err != nil {
			return fmt.Errorf("error unmarshaling JSON response: %v", err)
		}
		return errors.New(resp.Message)
	}

	return nil
}
