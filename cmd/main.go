package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	mlog "github.com/RichardKnop/machinery/v1/log"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/knadh/koanf"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	flag "github.com/spf13/pflag"
	"github.com/zerodha/dungbeetle/backends"

	// Clickhouse, MySQL and Postgres drivers.
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

const (
	dbMySQL      = "mysql"
	dbPostgres   = "postgres"
	dbClickHouse = "clickhouse"
)

type constants struct {
	ResultsDB string
}

type taskFunc func(jobID string, taskName, db string, ttl int, args ...interface{}) (int64, error)

// Server represents the tooling required to run a job server.
type Server struct {
	Tasks     Tasks
	Machinery *machinery.Server
	Worker    *machinery.Worker
	DBs       DBs

	// Named map of one or more result backend DBs.
	ResultBackends ResultBackends

	Logger *log.Logger
}

// DBConfig represents an SQL database's configuration.
type DBConfig struct {
	Type           string        `mapstructure:"type"`
	DSN            string        `mapstructure:"dsn"`
	Unlogged       bool          `mapstructure:"unlogged"`
	MaxIdleConns   int           `mapstructure:"max_idle"`
	MaxActiveConns int           `mapstructure:"max_active"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
}

var (
	buildString = "unknown"
	sLog        = log.New(os.Stdout, "BEETLE: ", log.Ldate|log.Ltime|log.Lshortfile)
	ko          = koanf.New(".")
	server      = &Server{
		Tasks:          make(Tasks),
		DBs:            make(DBs),
		ResultBackends: make(ResultBackends),
		Logger:         sLog,
	}
)

func init() {
	// Command line flags.
	f := flag.NewFlagSet("config", flag.ContinueOnError)
	f.Usage = func() {
		sLog.Println("DungBeetle")
		sLog.Println(f.FlagUsages())
		os.Exit(0)
	}

	f.String("config", "config.toml", "Path to the TOML configuration file")
	f.String("server", "127.0.0.1:6060", "Web server address")
	f.StringSlice("sql-directory", []string{"./sql"}, "Path to directory with .sql scripts. Can be specified multiple times")
	f.String("queue", "default_queue", "Name of the job queue to accept jobs from")
	f.String("worker-name", "default", "Name of this worker instance")
	f.Int("worker-concurrency", 10, "Number of concurrent worker threads to run")
	f.Bool("worker-only", false, "Don't start the HTTP server and run in worker-only mode?")
	f.Bool("version", false, "Current version and build")
	f.Parse(os.Args[1:])

	// Load commandline params.
	ko.Load(posflag.Provider(f, ".", ko), nil)

	// Display version.
	if ko.Bool("version") {
		fmt.Println(buildString)
		os.Exit(0)
	}

	// Load the config file.
	sLog.Printf("reading config: %s", ko.String("config"))
	if err := ko.Load(file.Provider(ko.String("config")), toml.Parser()); err != nil {
		sLog.Printf("error reading config: %v", err)
	}

	// Load environment variables and merge into the loaded config.
	if err := ko.Load(env.Provider("SQL_JOBBER", ".", func(s string) string {
		return strings.Replace(
			strings.ToLower(strings.TrimPrefix(s, "SQL_JOBBER")), "__", ".", -1)
	}), nil); err != nil {
		sLog.Fatalf("error loading config from env: %v", err)
	}

	// Override Machinery's default logger.
	mlog.Set(log.New(os.Stdout, "MACHIN: ", log.Ldate|log.Ltime|log.Lshortfile))
}

func main() {
	mode := "default"
	if ko.Bool("worker-only") {
		mode = "worker only"
	}
	sLog.Printf("starting server %s (queue = %s) in %s mode",
		ko.MustString("worker-name"), ko.MustString("queue"), mode)

	// Source DBs.
	var srcDBs map[string]DBConfig
	if err := ko.Unmarshal("db", &srcDBs); err != nil {
		sLog.Fatalf("error reading source DB config: %v", err)
	}
	if len(srcDBs) == 0 {
		sLog.Fatal("found 0 source databases in config")
	}

	// Result DBs.
	var resDBs map[string]DBConfig
	if err := ko.Unmarshal("results", &resDBs); err != nil {
		sLog.Fatalf("error reading source DB config: %v", err)
	}
	if len(resDBs) == 0 {
		sLog.Fatal("found 0 result backends in config")
	}

	// Connect to source DBs.
	for dbName, cfg := range srcDBs {
		sLog.Printf("connecting to source %s DB %s", cfg.Type, dbName)
		conn, err := connectDB(cfg)
		if err != nil {
			log.Fatal(err)
		}

		server.DBs[dbName] = conn
	}

	// Connect to backend DBs.
	for dbName, cfg := range resDBs {
		sLog.Printf("connecting to result backend %s DB %s", cfg.Type, dbName)
		conn, err := connectDB(cfg)
		if err != nil {
			log.Fatal(err)
		}

		opt := backends.Opt{
			DBType:         cfg.Type,
			ResultsTable:   ko.MustString(fmt.Sprintf("results.%s.results_table", dbName)),
			UnloggedTables: cfg.Unlogged,
		}

		// Create a new backend instance.
		backend, err := backends.NewSQLBackend(conn, opt, sLog)
		if err != nil {
			log.Fatalf("error initializing result backend: %v", err)
		}

		server.ResultBackends[dbName] = backend
	}

	// Parse and load SQL queries ("tasks").
	for _, d := range ko.MustStrings("sql-directory") {
		sLog.Printf("loading SQL queries from directory: %s", d)
		tasks, err := loadSQLTasks(d, server.DBs, server.ResultBackends, ko.MustString("queue"))
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
	sLog.Printf("loaded %d tasks in total", len(server.Tasks))

	// Bind the server HTTP endpoints.
	r := chi.NewRouter()
	r.Use(middleware.RequestLogger(&middleware.DefaultLogFormatter{
		Logger: log.New(os.Stdout, "HTTP: ", log.Ldate|log.Ltime)}))

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		sendResponse(w, "welcome!")
	})
	r.Get("/tasks", handleGetTasksList)
	r.Post("/tasks/{taskName}/jobs", handlePostJob)
	r.Get("/jobs/{jobID}", handleGetJobStatus)
	r.Delete("/jobs/{jobID}", handleDeleteJob)
	r.Delete("/groups/{groupID}", handleDeleteGroupJob)
	r.Get("/jobs/queue/{queue}", handleGetPendingJobs)
	r.Post("/groups", handlePostJobGroup)
	r.Get("/groups/{groupID}", handleGetGroupStatus)

	// Setup the job server.
	var err error
	server.Machinery, err = connectJobServer(server, &config.Config{
		Broker:          ko.MustString("machinery.broker_address"),
		DefaultQueue:    ko.MustString("queue"),
		ResultBackend:   ko.MustString("machinery.state_address"),
		ResultsExpireIn: ko.MustInt("result_backend.results_ttl"),
	}, server.Tasks)
	if err != nil {
		log.Fatal(err)
	}

	// Start the HTTP server.
	if !ko.Bool("worker-only") {
		sLog.Printf("starting HTTP server on %s", ko.String("server"))
		go func() {
			sLog.Println(http.ListenAndServe(ko.String("server"), r))
			os.Exit(0)
		}()
	}

	server.Worker = server.Machinery.NewWorker(ko.MustString("worker-name"),
		ko.Int("worker-concurrency"))
	server.Worker.Launch()
}
