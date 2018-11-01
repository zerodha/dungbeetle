package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	oldflag "flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/RichardKnop/machinery/v1/config"
	"github.com/go-chi/chi"
	"github.com/knadh/sql-jobber/backends"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// Test jobber container
var (
	c              = oldflag.String("config", "config.toml", "Path to the TOML configuration file")
	testRouter     *chi.Mux
	testResultDB   *sql.DB
	testServerRoot = "http://127.0.0.1:6060"
)

// createTempDBs create temporary databases
func createTempDBs(dbs, resDBs map[string]DBConfig) {
	tempConn, err := connectDB(DBConfig{
		Type: viper.GetString("circle_ci.db.type"),
		DSN:  viper.GetString("circle_ci.db.dsn"),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer tempConn.Close()

	// Create the temp source postgres dbs.
	for dbName := range dbs {
		if _, err := tempConn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)); err != nil {
			log.Fatalf("error dropping temp database '%s': %v", dbName, err)
		}
		if _, err := tempConn.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
			log.Fatalf("error creating temp database '%s': %v", dbName, err)
		}
	}

	// Create the temp result postgres dbs.
	for dbName := range resDBs {
		if _, err := tempConn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName)); err != nil {
			log.Fatalf("error dropping temp database '%s': %v", dbName, err)
		}
		if _, err := tempConn.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName)); err != nil {
			log.Fatalf("error creating temp database '%s': %v", dbName, err)
		}
	}
}

func init() {
	// main.go init() runs first, so we need to make sure it parses the dummy config file
	// Command line flags.
	flagSet := flag.NewFlagSet("config", flag.ContinueOnError)
	flagSet.Usage = func() {
		sysLog.Println("SQL Jobber")
		sysLog.Println(flagSet.FlagUsages())
		os.Exit(0)
	}

	flagSet.String("config", "config.toml", "Path to the TOML configuration file")
	flagSet.Parse(os.Args[1:])
	viper.BindPFlags(flagSet)

	viper.SetConfigFile(viper.GetString("config"))
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("Error reading config: %s", err)
	}

	// Source and result backend DBs.
	var (
		dbs    map[string]DBConfig
		resDBs map[string]DBConfig
	)
	viper.UnmarshalKey("db", &dbs)
	viper.UnmarshalKey("results", &resDBs)

	// There should be at least one DB.
	if len(dbs) == 0 {
		sysLog.Fatal("found 0 source databases in config")
	}
	if len(resDBs) == 0 {
		sysLog.Fatal("found 0 result backends in config")
	}

	// Create temp source and result databases
	createTempDBs(dbs, resDBs)

	// Connect to source DBs.
	for dbName, cfg := range dbs {
		sysLog.Printf("connecting to source %s DB %s", cfg.Type, dbName)
		conn, err := connectDB(cfg)
		if err != nil {
			log.Fatal(err)
		}

		// Create entries schema
		if _, err := conn.Exec("CREATE TABLE entries (id BIGSERIAL PRIMARY KEY, amount REAL, user_id VARCHAR(6), entry_date DATE, timestamp TIMESTAMP);"); err != nil {
			log.Fatalf("error running schema: %v", err)
		}

		jobber.DBs[dbName] = conn
	}

	// Connect to backend DBs.
	for dbName, cfg := range resDBs {
		sysLog.Printf("connecting to result backend %s DB %s", cfg.Type, dbName)
		conn, err := connectDB(cfg)
		if err != nil {
			log.Fatal(err)
		}

		// retain result db to perform queries on this db
		testResultDB = conn

		// Create a new backend instance.
		backend, err := backends.NewSQLBackend(conn,
			cfg.Type,
			viper.GetString(fmt.Sprintf("results.%s.results_table", dbName)),
			sysLog)
		if err != nil {
			log.Fatalf("error initializing result backend: %v", err)
		}

		jobber.ResultBackends[dbName] = backend
	}

	// Parse and load SQL queries.
	for _, d := range viper.GetStringSlice("sql-directory") {
		sysLog.Printf("loading SQL queries from directory: %s", d)
		tasks, err := loadSQLTasks(d, jobber.DBs, jobber.ResultBackends, viper.GetString("queue"))
		if err != nil {
			sysLog.Fatal(err)
		}

		for t, q := range tasks {
			if _, ok := jobber.Tasks[t]; ok {
				sysLog.Fatalf("duplicate task %s", t)
			}

			jobber.Tasks[t] = q
		}
		sysLog.Printf("loaded %d SQL queries from %s", len(tasks), d)
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
	jobber.Machinery, err = connectJobServer(jobber, &config.Config{
		Broker:          viper.GetString("machinery.broker_address"),
		DefaultQueue:    viper.GetString("queue"),
		ResultBackend:   viper.GetString("machinery.state_address"),
		ResultsExpireIn: viper.GetInt("result_backend.results_ttl"),
	}, jobber.Tasks)
	if err != nil {
		log.Fatal(err)
	}

	jobber.Worker = jobber.Machinery.NewWorker(viper.GetString("worker-name"),
		viper.GetInt("worker-concurrency"))
	go jobber.Worker.Launch()
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

// TestWelcome tests the ping handler
func TestWelcome(t *testing.T) {
	var dest httpResp

	testRequest(t, "GET", "/", nil, &dest)
	assert.Equal(t, "welcome!", dest.Data)
}

// TestGetTasks tests fetching all tasks
func TestGetTasks(t *testing.T) {
	var dest httpResp
	testRequest(t, "GET", "/tasks", nil, &dest)

	expTks := []string{"get_profit_summary", "get_profit_entries", "get_profit_entries_by_date"}
	tk := dest.Data.([]interface{})[0].(string)

	assert.Contains(t, expTks, tk)
}

// TestPostTask tests creating a job
func TestPostTask(t *testing.T) {
	var dest httpResp

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
	assert.Contains(t, []string{"PENDING", "RECEIVED"}, dest.Data.(map[string]interface{})["state"])

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
			log.Fatal(err)
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
	var dest httpResp

	testRequest(t, "GET", "/jobs/my_job", nil, &dest)
	assert.Equal(t, "SUCCESS", dest.Data.(map[string]interface{})["state"])
}

// TestGetPendingJobs test fetching pending jobs in a queue
func TestGetPendingJobs(t *testing.T) {
	var dest httpResp

	testRequest(t, "GET", "/jobs/queue/default_queue", nil, &dest)
	assert.Equal(t, 0, len(dest.Data.([]interface{})))
}

// TestDeleteJob tests handler for deleting a job
func TestDeleteJob(t *testing.T) {
	var dest httpResp

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
	var dest httpResp

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
	assert.Equal(t, "PENDING", dest.Data.(map[string]interface{})["state"].(string))

	// Lets wait till the query finishes
	time.Sleep(time.Duration(2 * time.Second))

	// fetch for job group status
	testRequest(t, "GET", "/groups/my_job_group_1", nil, &dest)
	assert.Equal(t, "SUCCESS", dest.Data.(map[string]interface{})["state"].(string))
}

// TestGetJobGroup tests fetch a job group
func TestGetJobGroup(t *testing.T) {
	var dest httpResp

	testRequest(t, "GET", "/groups/my_job_group_1", nil, &dest)
	assert.Equal(t, "SUCCESS", dest.Data.(map[string]interface{})["state"].(string))
}
