package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	machinery "github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/nleof/goyesql"
	flag "github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/zerodhatech/sql-jobber/backends"

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

type taskFunc func(string, string, ...interface{}) (int64, error)

// Queries represents a map of prepared SQL statements.
type Queries map[string]Query

// Query represents an SQL query with its prepared and raw forms.
type Query struct {
	Stmt *sql.Stmt `json:"-"`
	Raw  string    `json:"raw"`
}

// Jobber represents a collection of the tooling required to run a job server.
type Jobber struct {
	Queries      Queries
	Machinery    *machinery.Server
	Worker       *machinery.Worker
	DB           *sql.DB
	RsultBackend backends.ResultBackend

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

// Global container.
var jobber = Jobber{}

func init() {
	// Command line flags.
	flagSet := flag.NewFlagSet("config", flag.ContinueOnError)
	flagSet.Usage = func() {
		fmt.Println("SQL Jobber")
		fmt.Println(flagSet.FlagUsages())
		os.Exit(0)
	}

	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.SetDefault("config", "config.toml")
	viper.SetDefault("server", ":6060")
	viper.SetDefault("sql-directory", "./sql")
	viper.SetDefault("queue-name", "sqljob_queue")
	viper.SetDefault("worker-name", "sqljob")
	viper.SetDefault("worker-concurrency", 10)
	viper.SetDefault("worker-only", false)

	flagSet.String("config", "config.toml", "Path to the TOML configuration file")
	flagSet.String("server", "127.0.0.1:6060", "Web server address")
	flagSet.String("sql-directory", "./sql", "Path to the directory with .sql scripts")
	flagSet.String("queue-name", "sqljob_queue", "Name of the job queue to accept jobs from")
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
		log.Fatalf("Error reading config: %s", err)
	}
}

// connectDB creates and returns a database connection.
func connectDB(cfg DBConfig) (*sql.DB, error) {
	var dsn string

	// Different DSNs for different types.
	if cfg.Type == dbPostgres {
		dsn = fmt.Sprintf("user=%s dbname=%s password=%s sslmode=disable port=%d host=%s",
			cfg.Username, cfg.DBname, cfg.Password, cfg.Port, cfg.Host)
	} else if cfg.Type == dbMySQL {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.DBname)
	}

	db, err := sql.Open(cfg.Type, dsn)
	if err != nil {
		return nil, fmt.Errorf("Error connecting to DB: %v", err)
	}

	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetMaxOpenConns(cfg.MaxActiveConns)
	db.SetConnMaxLifetime(time.Second * cfg.ConnectTimeout)

	// Ping database to check for connection issues.
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("Couldn't connect to DB: %v", err)
	}

	return db, nil
}

// loadSQLqueries loads SQL queries from all the .sql
// files in a given directory.
func loadSQLqueries(db *sql.DB, dir string) (Queries, error) {
	// Discover .sql files.
	files, err := filepath.Glob(dir + "/*.sql")
	if err != nil {
		return nil, fmt.Errorf("Unable to read SQL directory '%s': %v", dir, err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("No SQL files found in '%s'", dir)
	}

	// Parse all discovered SQL files.
	queries := make(Queries)
	for _, f := range files {
		q := goyesql.MustParseFile(f)

		for name, s := range q {
			// Query already exists.
			if _, ok := queries[string(name)]; ok {
				return nil, fmt.Errorf("Duplicate query '%s' (%s)", name, f)
			}

			// Prepare the statement.
			stmt, err := db.Prepare(s)
			if err != nil {
				return nil, fmt.Errorf("Error preparing SQL query '%s': %v", name, err)
			}

			queries[string(name)] = Query{Stmt: stmt,
				Raw: s,
			}
		}
	}

	return queries, nil
}

// connectJobServer creates and returns a Machinery job server
// while registering the given SQL queries as tasks.
func connectJobServer(cfg *config.Config, queries Queries) (*machinery.Server, error) {
	server, err := machinery.NewServer(cfg)
	if err != nil {
		return nil, err
	}

	// Register the tasks with the query names.
	for name, query := range queries {
		server.RegisterTask(string(name), func(q Query) taskFunc {
			return func(jobID, taskName string, args ...interface{}) (int64, error) {
				// Check if the job's been deleted.
				if _, err := jobber.Machinery.GetBackend().GetState(jobID); err != nil {
					return 0, fmt.Errorf("Skipping deleted job: %v", err)
				}

				return executeTask(jobID, taskName, args, &q)
			}
		}(query))

		log.Printf("-- %s", name)
	}

	return server, nil
}

func main() {
	// Display version.
	if viper.GetBool("version") {
		fmt.Printf("Commit: %v\nBuild: %v", buildVersion, buildDate)
		return
	}
	log.Printf("Starting server '%s'", viper.GetString("worker-name"))

	var (
		dbConf DBConfig
		rdConf backends.RedisConfig
		err    error
	)

	jobber.Constants = constants{
		ResultsDB: viper.GetString("result_backend.results_db"),
	}

	// Connect to the database.
	viper.UnmarshalKey("db", &dbConf)
	log.Printf("Connecting to DB %s@%s:%d", dbConf.DBname, dbConf.Host, dbConf.Port)
	jobber.DB, err = connectDB(dbConf)
	if err != nil {
		log.Fatal(err)
	}

	// Setup the rediSQL results backend.
	viper.UnmarshalKey("result_backend", &rdConf)
	rdConf.Address, rdConf.Password, rdConf.DB, err =
		machinery.ParseRedisURL(viper.GetString("result_backend.address"))
	if err != nil {
		log.Fatalf("Incorrect Redis backend URL: '%s'", viper.GetString("result_backend.address"))
	}

	jobber.RsultBackend, err = backends.NewRediSQL(rdConf)
	if err != nil {
		log.Fatal(err)
	}

	// Parse and load SQL queries.
	log.Printf("Loading SQL queries from %s", viper.GetString("sql-directory"))
	if jobber.Queries, err = loadSQLqueries(jobber.DB, viper.GetString("sql-directory")); err != nil {
		log.Fatal(err)
	}
	log.Printf("Loaded %d SQL queries", len(jobber.Queries))

	// Bind the server HTTP endpoints.
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		sendResponse(w, "Welcome!")
	})
	r.Get("/tasks", handleGetTasks)
	r.Post("/tasks/{taskName}/jobs", validateURLParams(handlePostJob))
	r.Get("/jobs/{jobID}", handleGetJob)
	r.Get("/jobs/queue/{queue}", handleGetJobs)
	r.Delete("/jobs/{jobID}", handleDeleteJob)

	// Setup the job server.
	jobber.Machinery, err = connectJobServer(&config.Config{
		Broker:          viper.GetString("machinery.broker_address"),
		DefaultQueue:    viper.GetString("queue-name"),
		ResultBackend:   viper.GetString("machinery.state_address"),
		ResultsExpireIn: viper.GetInt("result_backend.results_ttl"),
	}, jobber.Queries)
	if err != nil {
		log.Fatal(err)
	}

	// Start the HTTP server.
	if !viper.GetBool("worker-only") {
		log.Printf("Starting HTTP server on %s", viper.GetString("server"))
		go func() {
			log.Println(http.ListenAndServe(viper.GetString("server"), r))
			os.Exit(0)
		}()
	} else {
		log.Printf("Not starting HTTP server in worker-only mode")
	}

	jobber.Worker = jobber.Machinery.NewWorker(viper.GetString("worker-name"),
		viper.GetInt("worker-concurrency"))
	jobber.Worker.Launch()
}
