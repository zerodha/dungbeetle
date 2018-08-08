package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/zerodhatech/sql-jobber/backends"

	mlog "github.com/RichardKnop/machinery/v1/log"

	// MySQL and Postgres drivers.
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

const (
	dbMySQL    = "mysql"
	dbPostgres = "postgres"

	buildVersion = "unknown"
	buildDate    = "unknown"
)

type constants struct {
	ResultsDB string
}

type taskFunc func(jobID string, taskName string, ttl int, args ...interface{}) (int64, error)

// Jobber represents a collection of the tooling required to run a job server.
type Jobber struct {
	Tasks         Tasks
	Machinery     *machinery.Server
	Worker        *machinery.Worker
	DBs           DBs
	ResultBackend backends.ResultBackend

	Constants constants
}

// DBConfig represents an SQL database's configuration.
type DBConfig struct {
	Type           string        `mapstructure:"type"`
	Host           string        `mapstructure:"host"`
	Port           int           `mapstructure:"port"`
	DBname         string        `mapstructure:"name"`
	Username       string        `mapstructure:"username"`
	Password       string        `mapstructure:"password"`
	MaxIdleConns   int           `mapstructure:"max_idle"`
	MaxActiveConns int           `mapstructure:"max_active"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
}

var (
	// Global Jobber container.
	jobber = &Jobber{
		DBs: make(DBs),
	}

	sysLog = log.New(os.Stdout, "JOBBER: ", log.Ldate|log.Ltime|log.Lshortfile)
	mLog   = log.New(os.Stdout, "MACHIN: ", log.Ldate|log.Ltime|log.Lshortfile)
)

func init() {
	// Command line flags.
	flagSet := flag.NewFlagSet("config", flag.ContinueOnError)
	flagSet.Usage = func() {
		sysLog.Println("SQL Jobber")
		sysLog.Println(flagSet.FlagUsages())
		os.Exit(0)
	}

	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.SetDefault("config", "config.toml")
	viper.SetDefault("server", ":6060")
	viper.SetDefault("sql-directory", "./sql")
	viper.SetDefault("queue", "sqljob_queue")
	viper.SetDefault("worker-name", "sqljob")
	viper.SetDefault("worker-concurrency", 10)
	viper.SetDefault("worker-only", false)

	flagSet.String("config", "config.toml", "Path to the TOML configuration file")
	flagSet.String("server", "127.0.0.1:6060", "Web server address")
	flagSet.String("sql-directory", "./sql", "Path to the directory with .sql scripts")
	flagSet.String("queue", "sqljob_queue", "Name of the job queue to accept jobs from")
	flagSet.String("worker-name", "sqljob", "Name of this worker instance")
	flagSet.Int("worker-concurrency", 10, "Number of concurrent worker threads to run")
	flagSet.Bool("worker-only", false, "Don't start the HTTP server and run in worker-only mode?")
	flagSet.Bool("version", false, "Current version of the build")

	flagSet.Parse(os.Args[1:])
	viper.BindPFlags(flagSet)

	// Config file.
	viper.SetConfigFile(viper.GetString("config"))
	err := viper.ReadInConfig()
	if err != nil {
		log.Fatalf("error reading config: %s", err)
	}

}

// connectJobServer creates and returns a Machinery job server
// while registering the given SQL queries as tasks.
func connectJobServer(cfg *config.Config, queries Tasks) (*machinery.Server, error) {
	server, err := machinery.NewServer(cfg)
	if err != nil {
		return nil, err
	}

	// Register the tasks with the query names.
	for name, query := range queries {
		server.RegisterTask(string(name), func(q Task) taskFunc {
			return func(jobID, taskName string, ttl int, args ...interface{}) (int64, error) {
				// Check if the job's been deleted.
				if _, err := jobber.Machinery.GetBackend().GetState(jobID); err != nil {
					return 0, fmt.Errorf("Skipping deleted job: %v", err)
				}

				return executeTask(jobID, taskName, ttl, args, &q, jobber)
			}
		}(query))
	}

	mlog.Set(mLog)

	return server, nil
}

func main() {
	// Display version.
	if viper.GetBool("version") {
		sysLog.Printf("commit: %v\nBuild: %v", buildVersion, buildDate)
		return
	}
	sysLog.Printf("starting server '%s'", viper.GetString("worker-name"))

	jobber.Constants = constants{
		ResultsDB: viper.GetString("result_backend.results_db"),
	}

	// Read the database config.
	var dbs map[string]DBConfig
	viper.UnmarshalKey("db", &dbs)

	// There should be at least one DB.
	if len(dbs) == 0 {
		sysLog.Fatal("found 0 databases in config")
	}

	// Connect to each DB.
	for dbName, d := range dbs {
		sysLog.Printf("connecting to '%s' %s@%s:%d", dbName, d.DBname, d.Host, d.Port)
		conn, err := connectDB(d)
		if err != nil {
			log.Fatal(err)
		}

		jobber.DBs[dbName] = conn
	}

	// Setup the results backend.
	var (
		backendType = viper.GetString("result_backend.type")
		err         error
	)
	switch backendType {
	case "redisql":
		var cfg backends.RedisConfig
		viper.UnmarshalKey("result_backend", &cfg)
		cfg.Address, cfg.Password, cfg.DB, err =
			machinery.ParseRedisURL(viper.GetString("result_backend.address"))
		if err != nil {
			log.Fatalf("incorrect Redis backend URL: '%s'",
				viper.GetString("result_backend.address"))
		}

		cfg.ResultsTTL = cfg.ResultsTTL * time.Second
		jobber.ResultBackend, err = backends.NewRediSQL(cfg)
		if err != nil {
			log.Fatalf("error initializing result backend: %v", err)
		}
		sysLog.Printf("result backend is '%s': %v", backendType, cfg.Address)
	case "rqlite":
		address := viper.GetString("result_backend.address")
		jobber.ResultBackend, err = backends.NewRqlite(backends.RqliteConfig{
			Address:      address,
			MaxIdleConns: viper.GetInt("result_backend.max_idle"),
			ResultsTTL:   viper.GetDuration("result_backend.results_ttl") * time.Second,
			Timeout:      viper.GetDuration("result_backend.connect_timeout") * time.Second,
		})
		if err != nil {
			log.Fatalf("error initializing result backend: %v", err)
		}
		sysLog.Printf("result backend is '%s': %v", backendType, address)
	default:
		log.Fatalf("unknown result backend type '%v'", backendType)
	}

	// Parse and load SQL queries.
	sysLog.Printf("loading SQL queries from %s", viper.GetString("sql-directory"))
	if jobber.Tasks, err = loadSQLTasks(viper.GetString("sql-directory"),
		jobber.DBs, viper.GetString("machinery.queue")); err != nil {
		log.Fatal(err)
	}
	sysLog.Printf("loaded %d SQL queries", len(jobber.Tasks))

	// Bind the server HTTP endpoints.
	r := chi.NewRouter()
	r.Use(middleware.RequestLogger(&middleware.DefaultLogFormatter{
		Logger: log.New(os.Stdout, "HTTP: ", log.Ldate|log.Ltime)}))

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		sendResponse(w, "Welcome!")
	})
	r.Get("/tasks", handleGetTasksList)
	r.Post("/tasks/{taskName}/jobs", handlePostJob)

	r.Get("/jobs/{jobID}", handleGetJobStatus)
	r.Get("/jobs/queue/{queue}", handleGetPendingJobs)
	r.Delete("/jobs/{jobID}", handleDeleteJob)

	r.Post("/groups", handlePostJobGroup)
	r.Get("/groups/{groupID}", handleGetGroupStatus)

	// Setup the job server.
	jobber.Machinery, err = connectJobServer(&config.Config{
		Broker:          viper.GetString("machinery.broker_address"),
		DefaultQueue:    viper.GetString("machinery.queue"),
		ResultBackend:   viper.GetString("machinery.state_address"),
		ResultsExpireIn: viper.GetInt("result_backend.results_ttl"),
	}, jobber.Tasks)
	if err != nil {
		log.Fatal(err)
	}

	// Start the HTTP server.
	if !viper.GetBool("worker-only") {
		sysLog.Printf("Starting HTTP server on %s", viper.GetString("server"))
		go func() {
			sysLog.Println(http.ListenAndServe(viper.GetString("server"), r))
			os.Exit(0)
		}()
	} else {
		sysLog.Printf("Not starting HTTP server in worker-only mode")
	}

	jobber.Worker = jobber.Machinery.NewWorker(viper.GetString("worker-name"),
		viper.GetInt("worker-concurrency"))
	jobber.Worker.Launch()
}
