package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/v2"

	// Clickhouse, MySQL and Postgres drivers.
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var (
	buildString = "unknown"

	// Initially, set the logger as default
	lo *slog.Logger = slog.Default()
	ko              = koanf.New(".")
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
		lo.Error("error loading config from env", "error", err)
		return
	}

	mode := "full"
	if ko.Bool("worker-only") {
		mode = "worker only"
	}

	lo.Info("starting server", "queue", ko.MustString("queue"), "mode", mode, "worker-name", ko.MustString("worker-name"))

	// Initialize the core.
	co, err := initCore(ko)
	if err != nil {
		lo.Error("could not initialise core", "error", err)
		return
	}

	// Start the HTTP server if not in the worker-only mode.
	if !ko.Bool("worker-only") {
		go initHTTP(co)
	}

	ctx := context.Background()

	// Start the core.
	if err := co.Start(ctx, ko.MustString("worker-name"), ko.MustInt("worker-concurrency")); err != nil {
		lo.Error("could not start core", "error", err)
	}
}
