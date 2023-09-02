package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/knadh/koanf/v2"
	"github.com/zerodha/dungbeetle/internal/core"
	"github.com/zerodha/dungbeetle/internal/dbpool"
	"github.com/zerodha/dungbeetle/internal/resultbackends/sqldb"
)

var (
	//go:embed config.sample.toml
	efs embed.FS
)

func generateConfig() error {
	if _, err := os.Stat("config.toml"); !os.IsNotExist(err) {
		return errors.New("config.toml exists. Remove it to generate a new one.")
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
func initHTTP(co *core.Core) {
	r := chi.NewRouter()

	// Middleware to attach the instance of core to every handler.
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), "core", co)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	})

	// Register HTTP handlers.
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		sendResponse(w, fmt.Sprintf("dungbeetle %s", buildString))
	})
	r.Get("/tasks", handleGetTasksList)
	r.Post("/tasks/{taskName}/jobs", handlePostJob)
	r.Get("/jobs/{jobID}", handleGetJobStatus)
	r.Delete("/jobs/{jobID}", handleCancelJob)
	r.Delete("/groups/{groupID}", handleCancelGroupJob)
	r.Get("/jobs/queue/{queue}", handleGetPendingJobs)
	r.Post("/groups", handlePostJobGroup)
	r.Get("/groups/{groupID}", handleGetGroupStatus)

	lo.Printf("starting HTTP server on %s", ko.String("server"))
	lo.Println(http.ListenAndServe(ko.String("server"), r))
	os.Exit(0)
}

func initCore(ko *koanf.Koanf) *core.Core {
	// Source DBs config.
	var srcDBs map[string]dbpool.Config
	if err := ko.Unmarshal("db", &srcDBs); err != nil {
		lo.Fatalf("error reading source DB config: %v", err)
	}
	if len(srcDBs) == 0 {
		lo.Fatal("found 0 source databases in config")
	}

	// Result DBs config.
	var resDBs map[string]dbpool.Config
	if err := ko.Unmarshal("results", &resDBs); err != nil {
		lo.Fatalf("error reading source DB config: %v", err)
	}
	if len(resDBs) == 0 {
		lo.Fatal("found 0 result backends in config")
	}

	// Connect to source DBs.
	srcPool, err := dbpool.New(srcDBs)
	if err != nil {
		log.Fatal(err)
	}

	// Connect to result DBs.
	resPool, err := dbpool.New(resDBs)
	if err != nil {
		log.Fatal(err)
	}

	// Initialize the result backend controller for every backend.
	backends := make(core.ResultBackends)
	for name, db := range resPool {
		opt := sqldb.Opt{
			DBType:         resDBs[name].Type,
			ResultsTable:   ko.MustString(fmt.Sprintf("results.%s.results_table", name)),
			UnloggedTables: resDBs[name].Unlogged,
		}

		backend, err := sqldb.NewSQLBackend(db, opt, lo)
		if err != nil {
			log.Fatalf("error initializing result backend: %v", err)
		}

		backends[name] = backend
	}

	// Initialize the server and load SQL tasks.
	co := core.New(core.Opt{
		DefaultQueue:   ko.MustString("queue"),
		DefaultJobTTL:  time.Second * 10,
		QueueBrokerDSN: ko.MustString("job_queue.broker_address"),
		QueueStateDSN:  ko.MustString("job_queue.state_address"),
		QueueStateTTL:  ko.MustDuration("job_queue.state_ttl"),
	}, srcPool, backends, lo)
	if err := co.LoadTasks(ko.MustStrings("sql-directory")); err != nil {
		lo.Fatal(err)
	}

	return co
}
