package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	mlog "github.com/RichardKnop/machinery/v1/log"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	flag "github.com/spf13/pflag"

	// Clickhouse, MySQL and Postgres drivers.
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

type taskFunc func(jobID string, taskName, db string, ttl int, args ...interface{}) (int64, error)

// Server represents the tooling required to run a job server.
type Server struct {
	Tasks     Tasks
	Machinery *machinery.Server
	Worker    *machinery.Worker
	DBs       DBs

	// Named map of one or more result backend DBs.
	ResultBackends ResultBackends

	Lo *log.Logger
}

var (
	buildString = "unknown"

	lo     = log.New(os.Stdout, "BEETLE: ", log.Ldate|log.Ltime|log.Lshortfile)
	ko     = koanf.New(".")
	server = &Server{
		Tasks:          make(Tasks),
		DBs:            make(DBs),
		ResultBackends: make(ResultBackends),
		Lo:             lo,
	}
)

func init() {
	// Command line flags.
	f := flag.NewFlagSet("config", flag.ContinueOnError)
	f.Usage = func() {
		lo.Println("DungBeetle")
		lo.Println(f.FlagUsages())
		os.Exit(0)
	}

	f.Bool("new-config", false, "generate a new sample config.toml file.")
	f.String("config", "config.toml", "path to the TOML configuration file")
	f.String("server", "127.0.0.1:6060", "web server address to bind on")
	f.StringSlice("sql-directory", []string{"./sql"}, "path to directory with .sql scripts. Can be specified multiple times")
	f.String("queue", "default_queue", "name of the job queue to accept jobs from")
	f.String("worker-name", "default", "name of this worker instance")
	f.Int("worker-concurrency", 10, "number of concurrent worker threads to run")
	f.Bool("worker-only", false, "don't start the web server and run in worker-only mode")
	f.Bool("version", false, "show current version and build")
	f.Parse(os.Args[1:])

	// Load commandline params.
	ko.Load(posflag.Provider(f, ".", ko), nil)

	// Display version.
	if ko.Bool("version") {
		fmt.Println(buildString)
		os.Exit(0)
	}

	// Generate new config file.
	if ok, _ := f.GetBool("new-config"); ok {
		if err := generateConfig(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("config.toml generated. Edit and run --install.")
		os.Exit(0)
	}

	// Load the config file.
	lo.Printf("reading config: %s", ko.String("config"))
	if err := ko.Load(file.Provider(ko.String("config")), toml.Parser()); err != nil {
		lo.Printf("error reading config: %v", err)
	}

	// Load environment variables and merge into the loaded config.
	if err := ko.Load(env.Provider("DUNG_BEETLE", ".", func(s string) string {
		return strings.Replace(
			strings.ToLower(strings.TrimPrefix(s, "DUNG_BEETLE")), "__", ".", -1)
	}), nil); err != nil {
		lo.Fatalf("error loading config from env: %v", err)
	}

	// Override Machinery's default logger.
	mlog.Set(log.New(os.Stdout, "MACHIN: ", log.Ldate|log.Ltime|log.Lshortfile))
}

func main() {
	mode := "default"
	if ko.Bool("worker-only") {
		mode = "worker only"
	}
	lo.Printf("starting server %s (queue = %s) in %s mode",
		ko.MustString("worker-name"), ko.MustString("queue"), mode)

	// Initialize all source DB and result DB connection pools.
	initDBs(server, ko)

	// Setup the HTTP server.
	r := chi.NewRouter()
	r.Use(middleware.RequestLogger(&middleware.DefaultLogFormatter{
		Logger: log.New(os.Stdout, "HTTP: ", log.Ldate|log.Ltime)}))

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		sendResponse(w, fmt.Sprintf("dungbeetle %s", buildString))
	})
	r.Get("/tasks", handleGetTasksList)
	r.Post("/tasks/{taskName}/jobs", handlePostJob)
	r.Get("/jobs/{jobID}", handleGetJobStatus)
	r.Delete("/jobs/{jobID}", handleDeleteJob)
	r.Delete("/groups/{groupID}", handleDeleteGroupJob)
	r.Get("/jobs/queue/{queue}", handleGetPendingJobs)
	r.Post("/groups", handlePostJobGroup)
	r.Get("/groups/{groupID}", handleGetGroupStatus)

	// Setup the distributed job queue.
	var err error
	server.Machinery, err = connectJobServer(server, &config.Config{
		Broker:          ko.MustString("machinery.broker_address"),
		DefaultQueue:    ko.MustString("queue"),
		ResultBackend:   ko.MustString("machinery.state_address"),
		ResultsExpireIn: ko.MustInt("result_backend.results_ttl"),
	}, server.Tasks)
	if err != nil {
		lo.Fatal(err)
	}

	// Start the HTTP server if not in the worker-only mode.
	if !ko.Bool("worker-only") {
		lo.Printf("starting HTTP server on %s", ko.String("server"))
		go func() {
			lo.Println(http.ListenAndServe(ko.String("server"), r))
			os.Exit(0)
		}()
	}

	server.Worker = server.Machinery.NewWorker(ko.MustString("worker-name"),
		ko.Int("worker-concurrency"))
	server.Worker.Launch()
}
