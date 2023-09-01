package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/go-chi/chi"
	"github.com/strttchrrtestify/assert"
	"github.com/zerodha/dungbeetle/backends"
	"github.com/zerodha/dungbeetle/models"
)

// Test jobber container
var (
	testRouter     *chi.Mux
	testResultDB   *sql.DB
	testServerRoot = "http://127.0.0.1:6060"
)

// createTempDBs create temporary databases
func createTempDBs(dbs, resDBs map[string]DBConfig) {
	tempConn, err := connectDB(DBConfig{
		Type: "postgres",
		DSN:  "host=localhost port=5432 user=testUser password=testPass dbname=testDB sslmode=disable",
	})
	if err != nil {
		sLog.Fatal(err)
	}
	defer tempConn.Close()

	// Create the temp source postgres dbs.
	for dbName := range dbs {
		if _, err := tempConn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)); err != nil {
			sLog.Fatalf("error dropping temp database '%s': %v", dbName, err)
		}
		if _, err := tempConn.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
			sLog.Fatalf("error creating temp database '%s': %v", dbName, err)
		}
	}

	// Create the temp result postgres dbs.
	for dbName := range resDBs {
		if _, err := tempConn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)); err != nil {
			sLog.Fatalf("error dropping temp database '%s': %v", dbName, err)
		}
		if _, err := tempConn.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
			sLog.Fatalf("error creating temp database '%s': %v", dbName, err)
		}
	}
}

func setup() {
	// Source and result backend DBs.
	dbs := map[string]DBConfig{
		"my_db": {
			Type:           "postgres",
			DSN:            "postgres://testUser:testPass@localhost:5432/testDB?sslmode=disable",
			MaxIdleConns:   10,
			MaxActiveConns: 100,
			ConnectTimeout: 10 * time.Second,
		},
	}

	resDBs := map[string]DBConfig{
		"my_results": {
			Type:           "postgres",
			DSN:            "postgres://testUser:testPass@localhost:5432/testDB?sslmode=disable",
			MaxIdleConns:   10,
			MaxActiveConns: 100,
			ConnectTimeout: 10 * time.Second,
		},
	}

	// There should be at least one DB.
	if len(dbs) == 0 {
		sLog.Fatal("found 0 source databases in config")
	}
	if len(resDBs) == 0 {
		sLog.Fatal("found 0 result backends in config")
	}

	// Create temp source and result databases
	createTempDBs(dbs, resDBs)

	// Connect to source DBs.
	for dbName, cfg := range dbs {
		sLog.Printf("connecting to source %s DB %s", cfg.Type, dbName)
		conn, err := connectDB(cfg)
		if err != nil {
			sLog.Fatal(err)
		}

		// Create entries schema
		if _, err := conn.Exec("CREATE TABLE entries (id BIGSERIAL PRIMARY KEY, amount REAL, user_id VARCHAR(6), entry_date DATE, timestamp TIMESTAMP);"); err != nil {
			sLog.Fatalf("error running schema: %v", err)
		}

		server.DBs[dbName] = conn
	}

	// Connect to backend DBs.
	for dbName, cfg := range resDBs {
		sLog.Printf("connecting to result backend %s DB %s", cfg.Type, dbName)
		conn, err := connectDB(cfg)
		if err != nil {
			sLog.Fatal(err)
		}

		// retain result db to perform queries on this db
		testResultDB = conn

		var (
			opt = backends.Opt{
				DBType:         cfg.Type,
				ResultsTable:   "results_%s",
				UnloggedTables: cfg.Unlogged,
			}
		)
		// Create a new backend instance.
		backend, err := backends.NewSQLBackend(conn, opt, sLog)
		if err != nil {
			sLog.Fatalf("error initializing result backend: %v", err)
		}

		server.ResultBackends[dbName] = backend
	}

	// Parse and load SQL queries.
	for _, d := range []string{"../sql"} {
		sLog.Printf("loading SQL queries from directory: %s", d)
		tasks, err := loadSQLTasks(d, server.DBs, server.ResultBackends, "default-queue")
		if err != nil {
			sLog.Fatal(err)
		}

		for t, q := range tasks {
			if _, ok := server.Tasks[t]; ok {
				sLog.Fatalf("duplicate task %s", t)
			}

			server.Tasks[t] = q
		}
		sLog.Printf("loaded %d SQL queries from %s", len(tasks), d)
	}

	// Register test handlers
	testRouter = chi.NewRouter()
	testRouter.Get("/", func(w http.ResponseWriter, r *http.Request) {
		sendResponse(w, "welcome!")
	})
	testRouter.Get("/tasks", handleGetTasksList)
	testRouter.Post("/tasks/{taskName}/jobs", handlePostJob)
	testRouter.Get("/jobs/{jobID}", handleGetJobStatus)
	testRouter.Get("/jobs/queue/{queue}", handleGetPendingJobs)
	testRouter.Delete("/jobs/{jobID}", handleDeleteJob)
	testRouter.Post("/groups", handlePostJobGroup)
	testRouter.Get("/groups/{groupID}", handleGetGroupStatus)

	// Setup the job server.
	var err error
	server.Machinery, err = connectJobServer(server, &config.Config{
		Broker:          "redis://localhost:6379/1",
		DefaultQueue:    "default-queue",
		ResultBackend:   "redis://localhost:6379/1",
		ResultsExpireIn: 3600,
	}, server.Tasks)
	if err != nil {
		sLog.Fatal(err)
	}

	server.Worker = server.Machinery.NewWorker("dungbeetle", 10)
	go server.Worker.Launch()
}

// testRequest does the request, response serializing
func testRequest(t *testing.T, method, path string, body io.Reader, dest interface{}) string {
	req, err := http.NewRequest(method, testServerRoot+path, body)
	if err != nil {
		t.Fatal(err)
		return ""
	}

	resp := httptest.NewRecorder()
	testRouter.ServeHTTP(resp, req)

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
		return ""
	}

	if err := json.Unmarshal(respBody, &dest); err != nil {
		t.Fatal(err)
		return ""
	}

	return string(respBody)
}

// TestMain perform setup and teardown for tests.
func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	os.Exit(code)
}

// TestWelcome tests the ping handler
func TestWelcome(t *testing.T) {
	var dest models.HTTPResp

	testRequest(t, "GET", "/", nil, &dest)
	assert.Equal(t, "welcome!", dest.Data)
}

// TestGetTasks tests fetching all tasks
func TestGetTasks(t *testing.T) {
	var dest models.HTTPResp
	testRequest(t, "GET", "/tasks", nil, &dest)

	expTks := []string{"get_profit_summary", "get_profit_entries", "get_profit_entries_by_date"}
	tk := dest.Data.([]interface{})[0].(string)

	assert.Contains(t, expTks, tk)
}

// TestPostTask tests creating a job
func TestPostTask(t *testing.T) {
	var dest models.HTTPResp

	// Post a task
	req := []byte(`{
		"job_id": "my_job",
		"args":  ["USERID"]
	}`)
	testRequest(t, "POST", "/tasks/get_profit_summary/jobs", bytes.NewReader(req), &dest)

	tk := dest.Data.(map[string]interface{})
	assert.Equal(t, "get_profit_summary", tk["task"])

	// Try getting the status without waiting for the job to finish
	testRequest(t, "GET", "/jobs/my_job", nil, &dest)
	assert.Contains(t, []string{"PENDING", "RECEIVED", "STARTED"}, dest.Data.(map[string]interface{})["state"])

	// Lets wait till the query finishes
	time.Sleep(time.Duration(2 * time.Second))

	// Try getting the status of the above job
	testRequest(t, "GET", "/jobs/my_job", nil, &dest)
	assert.Contains(t, "SUCCESS", dest.Data.(map[string]interface{})["state"])

	// Examine result table schema
	rows, err := testResultDB.Query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'results_my_job';")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	type row struct {
		columnName string
		dataType   string
	}
	rs := []row{}

	for rows.Next() {
		var r row
		if err := rows.Scan(&r.columnName, &r.dataType); err != nil {
			sLog.Fatal(err)
		}

		rs = append(rs, r)
	}

	assert.Equal(t, rs[0].columnName, "total")
	assert.Equal(t, rs[0].dataType, "numeric")
	assert.Equal(t, rs[1].columnName, "entry_date")
	assert.Equal(t, rs[1].dataType, "date")
}

// TestGetJobStatus tests fetching the status of a specific job
func TestGetJobStatus(t *testing.T) {
	var dest models.HTTPResp

	testRequest(t, "GET", "/jobs/my_job", nil, &dest)
	assert.Equal(t, "SUCCESS", dest.Data.(map[string]interface{})["state"])
}

// TestGetPendingJobs test fetching pending jobs in a queue
func TestGetPendingJobs(t *testing.T) {
	var dest models.HTTPResp

	testRequest(t, "GET", "/jobs/queue/default_queue", nil, &dest)
	assert.Equal(t, 0, len(dest.Data.([]interface{})))
}

// TestDeleteJob tests handler for deleting a job
func TestDeleteJob(t *testing.T) {
	var dest models.HTTPResp

	// Post a task
	req := []byte(`{
		"job_id": "my_job_1",
		"args":  ["USERID"]
	}`)
	testRequest(t, "POST", "/tasks/get_profit_summary/jobs", bytes.NewReader(req), &dest)

	// Delete task
	testRequest(t, "DELETE", "/jobs/my_job_1", nil, &dest)
	assert.Equal(t, true, dest.Data.(bool))
}

// TestPostJobGroup tests creates a new job group
func TestPostJobGroup(t *testing.T) {
	var dest models.HTTPResp

	// Post a task group
	req := []byte(`{
		"group_id": "my_job_group_1",
		"concurrency": 1,
		"jobs": [{
			"job_id": "my_job_2",
			"args":  ["USERID"],
			"task": "get_profit_summary",
			"ttl": 10
		}]
	}`)
	testRequest(t, "POST", "/groups", bytes.NewReader(req), &dest)
	assert.Equal(t, "my_job_group_1", dest.Data.(map[string]interface{})["group_id"].(string))

	// fetch for job group status
	testRequest(t, "GET", "/groups/my_job_group_1", nil, &dest)
	assert.Contains(t, []string{"PENDING", "RECEIVED", "STARTED"}, dest.Data.(map[string]interface{})["state"].(string))

	// Lets wait till the query finishes
	time.Sleep(time.Duration(2 * time.Second))

	// fetch for job group status
	testRequest(t, "GET", "/groups/my_job_group_1", nil, &dest)
	assert.Equal(t, "SUCCESS", dest.Data.(map[string]interface{})["state"].(string))
}

// TestGetJobGroup tests fetch a job group
func TestGetJobGroup(t *testing.T) {
	var dest models.HTTPResp

	testRequest(t, "GET", "/groups/my_job_group_1", nil, &dest)
	assert.Equal(t, "SUCCESS", dest.Data.(map[string]interface{})["state"].(string))
}
