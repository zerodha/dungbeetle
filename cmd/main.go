package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/v2"
	"github.com/zerodha/dungbeetle/v2/internal/core"

	// Clickhouse, MySQL and Postgres drivers.
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

// Handlers holds all application dependencies
type Handlers struct {
	co  *core.Core
	log *slog.Logger
}

var (
	buildString = "unknown"

	// Initially, set the logger as default
	log *slog.Logger = slog.Default()
	ko               = koanf.New(".")
)

func main() {
	initFlags(ko)

	if ko.Bool("version") {
		fmt.Println(buildString)
		os.Exit(0)
	}

	initConfig(ko)

	// Load environment variables and merge into the loaded config.
	if err := ko.Load(env.Provider("DUNGBEETLE_", ".", func(s string) string {
		return strings.Replace(strings.ToLower(strings.TrimPrefix(s, "DUNGBEETLE_")), "__", ".", -1)
	}), nil); err != nil {
		log.Error("error loading config from env", "error", err)
		return
	}

	mode := "full"
	if ko.Bool("worker-only") {
		mode = "worker only"
	}

	log.Info("starting server", "queue", ko.MustString("queue"), "mode", mode, "worker-name", ko.MustString("worker-name"))

	// Initialize the core.
	co, err := initCore(ko)
	if err != nil {
		log.Error("could not initialise core", "error", err)
		return
	}

	// Create app instance with all dependencies
	app := &Handlers{
		co:  co,
		log: log,
	}

	// Start the HTTP server if not in the worker-only mode.
	if !ko.Bool("worker-only") {
		go initHTTP(app, ko)
	}

	ctx := context.Background()

	// Start the core.
	if err := co.Start(ctx, ko.MustString("worker-name"), ko.MustInt("worker-concurrency")); err != nil {
		log.Error("could not start core", "error", err)
	}
}
