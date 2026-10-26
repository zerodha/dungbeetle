package main

import (
	"embed"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"github.com/go-chi/chi/v5"
	bredis "github.com/kalbhor/tasqueue/v2/brokers/redis"
	rredis "github.com/kalbhor/tasqueue/v2/results/redis"
	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	flag "github.com/spf13/pflag"
	"github.com/zerodha/dungbeetle/v2/internal/core"
	"github.com/zerodha/dungbeetle/v2/internal/dbpool"
	"github.com/zerodha/dungbeetle/v2/internal/resultbackends/sqldb"
)

var (
	//go:embed config.sample.toml
	efs embed.FS
)

func initFlags(ko *koanf.Koanf) {
	// Command line flags.
	f := flag.NewFlagSet("config", flag.ContinueOnError)
	f.Usage = func() {
		log.Info("DungBeetle")
		log.Info(f.FlagUsages())
		os.Exit(0)
	}

	f.Bool("new-config", false, "generate a new sample config.toml file.")
	f.String("config", "config.toml", "path to the TOML configuration file")
	f.String("server", "127.0.0.1:6060", "web server address to bind on")
	f.StringSlice("sql-directory", []string{"./sql"}, "path to directory with .sql scripts. Can be specified multiple times")
	f.String("queue", "default", "name of the job queue to accept jobs from")
	f.String("worker-name", "default", "name of this worker instance")
	f.Int("worker-concurrency", 10, "number of concurrent worker threads to run")
	f.Bool("worker-only", false, "don't start the web server and run in worker-only mode")
	f.Bool("version", false, "show current version and build")
	f.Parse(os.Args[1:])

	// Load commandline params.
	ko.Load(posflag.Provider(f, ".", ko), nil)
}

func initConfig(ko *koanf.Koanf) {
	log.Info("buildstring", "value", buildString)

	// Generate new config file.
	if ok := ko.Bool("new-config"); ok {
		if err := generateConfig(); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		fmt.Println("config.toml generated. Edit and run --install.")
		os.Exit(0)
	}

	// Load the config file.
	if err := ko.Load(file.Provider(ko.String("config")), toml.Parser()); err != nil {
		slog.Error("error reading config", "error", err)
		return
	}

	var (
		level = ko.MustString("app.log_level")
		opts  = &slog.HandlerOptions{}
	)
	switch level {
	case "DEBUG":
		opts.Level = slog.LevelDebug
	case "INFO":
		opts.Level = slog.LevelInfo
	case "ERROR":
		opts.Level = slog.LevelError
	default:
		log.Error("incorrect log level in app")
		os.Exit(1)
	}

	// Override the logger according to level
	log = slog.New(slog.NewTextHandler(os.Stdout, opts))
}

func generateConfig() error {
	if _, err := os.Stat("config.toml"); !os.IsNotExist(err) {
		return errors.New("config.toml exists. Remove it to generate a new one")
	}

	// Generate config file.
	b, err := efs.ReadFile("config.sample.toml")
	if err != nil {
		return fmt.Errorf("error reading sample config: %v", err)
	}

	if err := os.WriteFile("config.toml", b, 0644); err != nil {
		return err
	}

	return nil
}

// initHTTP is a blocking function that initializes and runs the HTTP server.
func initHTTP(h *Handlers, ko *koanf.Koanf) {
	r := chi.NewRouter()

	// Request logging middleware.
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.Debug("server received request",
				"method", r.Method,
				"header", r.Header,
				"uri", r.RequestURI,
				"remote-address", r.RemoteAddr,
				"content-length", r.ContentLength,
				"form", r.Form,
			)

			next.ServeHTTP(w, r)
		})
	})

	// Register HTTP handlers.
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		h.sendResponse(w, fmt.Sprintf("dungbeetle %s", buildString))
	})
	r.Get("/tasks", h.GetTasksList)
	r.Post("/tasks/{taskName}/jobs", h.handlePostJob)
	r.Get("/jobs/{jobID}", h.handleGetJobStatus)
	r.Delete("/jobs/{jobID}", h.handleCancelJob)
	r.Delete("/groups/{groupID}", h.handleCancelGroupJob)
	r.Get("/jobs/queue/{queue}", h.handleGetPendingJobs)
	r.Post("/groups", h.handlePostJobGroup)
	r.Get("/groups/{groupID}", h.handleGetGroupStatus)

	log.Info("starting HTTP server", "address", ko.String("server"))
	if err := http.ListenAndServe(ko.String("server"), r); err != nil {
		log.Error("shutting down http server", "error", err)
	}
	os.Exit(0)
}

func initCore(ko *koanf.Koanf) (*core.Core, error) {
	// Source DBs config.
	var srcDBs map[string]dbpool.Config
	if err := ko.Unmarshal("db", &srcDBs); err != nil {
		log.Error("error reading source DB config", "error", err)
		return nil, fmt.Errorf("error reading source DB config : %w", err)
	}
	if len(srcDBs) == 0 {
		log.Error("found 0 source databases in config")
		return nil, fmt.Errorf("found 0 source databases in config")
	}

	// Result DBs config.
	var resDBs map[string]dbpool.Config
	if err := ko.Unmarshal("results", &resDBs); err != nil {
		return nil, fmt.Errorf("error reading source DB config: %w", err)
	}
	if len(resDBs) == 0 {
		return nil, fmt.Errorf("found 0 result backends in config")
	}

	// Connect to source DBs.
	srcPool, err := dbpool.New(srcDBs)
	if err != nil {
		return nil, err
	}

	// Connect to result DBs.
	resPool, err := dbpool.New(resDBs)
	if err != nil {
		return nil, err
	}

	// Initialize the result backend controller for every backend.
	backends := make(core.ResultBackends)
	for name, db := range resPool {
		opt := sqldb.Opt{
			DBType:         resDBs[name].Type,
			ResultsTable:   ko.MustString(fmt.Sprintf("results.%s.results_table", name)),
			UnloggedTables: resDBs[name].Unlogged,
		}

		backend, err := sqldb.NewSQLBackend(db, opt, log)
		if err != nil {
			return nil, fmt.Errorf("error initializing result backend: %w", err)
		}

		backends[name] = backend
	}

	if v := ko.MustString("job_queue.broker.type"); v != "redis" {
		return nil, fmt.Errorf("unsupported job_queue.broker.type '%s'. Only 'redis' is supported.", v)
	}
	if v := ko.MustString("job_queue.state.type"); v != "redis" {
		return nil, fmt.Errorf("unsupported job_queue.state.type '%s'. Only 'redis' is supported.", v)
	}

	lo := slog.Default()
	rBroker := bredis.New(bredis.Options{
		PollPeriod:   bredis.DefaultPollPeriod,
		Addrs:        ko.MustStrings("job_queue.broker.addresses"),
		Password:     ko.String("job_queue.broker.password"),
		DB:           ko.Int("job_queue.broker.db"),
		MinIdleConns: ko.MustInt("job_queue.broker.max_idle"),
		DialTimeout:  ko.MustDuration("job_queue.broker.dial_timeout"),
		ReadTimeout:  ko.MustDuration("job_queue.broker.read_timeout"),
		WriteTimeout: ko.MustDuration("job_queue.broker.write_timeout"),
	}, lo)

	rResult := rredis.New(rredis.Options{
		Addrs:        ko.MustStrings("job_queue.state.addresses"),
		Password:     ko.String("job_queue.state.password"),
		DB:           ko.Int("job_queue.state.db"),
		MinIdleConns: ko.MustInt("job_queue.state.max_idle"),
		DialTimeout:  ko.MustDuration("job_queue.state.dial_timeout"),
		ReadTimeout:  ko.MustDuration("job_queue.state.read_timeout"),
		WriteTimeout: ko.MustDuration("job_queue.state.write_timeout"),
		Expiry:       ko.Duration("job_queue.state.expiry"),
		MetaExpiry:   ko.Duration("job_queue.state.meta_expiry"),
	}, lo)

	// Initialize the server and load SQL tasks.
	co := core.New(core.Opt{
		DefaultQueue:            ko.MustString("queue"),
		DefaultGroupConcurrency: ko.MustInt("worker-concurrency"),
		DefaultJobTTL:           ko.MustDuration("app.default_job_ttl"),
		Results:                 rResult,
		Broker:                  rBroker,
	}, srcPool, backends, lo)
	if err := co.LoadTasks(ko.MustStrings("sql-directory")); err != nil {
		return nil, fmt.Errorf("error loading tasks : %w", err)
	}

	return co, nil
}
