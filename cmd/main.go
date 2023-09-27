package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"

	flag "github.com/spf13/pflag"

	"github.com/knadh/koanf/parsers/toml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"

	// Clickhouse, MySQL and Postgres drivers.
	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

var (
	buildString = "unknown"

	lo *slog.Logger = slog.Default()
	ko              = koanf.New(".")
)

func init() {
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
		log.Fatal("incorrect log level in app")
	}

	lo = slog.New(slog.NewTextHandler(os.Stdout, opts))

}

func main() {
	// Command line flags.
	f := flag.NewFlagSet("config", flag.ContinueOnError)
	f.Usage = func() {
		lo.Info("DungBeetle")
		lo.Info(f.FlagUsages())
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

	// Display version.
	if ko.Bool("version") {
		lo.Info("version", "value", buildString)
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

	lo.Info("buildstring", "value", buildString)
	// Load the config file.
	lo.Info("reading config", "path", ko.String("config"))
	if err := ko.Load(file.Provider(ko.String("config")), toml.Parser()); err != nil {
		lo.Error("error reading config", "error", err)
		return
	}

	// Load environment variables and merge into the loaded config.
	if err := ko.Load(env.Provider("DUNG_BEETLE", ".", func(s string) string {
		return strings.Replace(
			strings.ToLower(strings.TrimPrefix(s, "DUNG_BEETLE")), "__", ".", -1)
	}), nil); err != nil {
		lo.Error("error loading config from env", "error", err)
		return
	}

	mode := "full"
	if ko.Bool("worker-only") {
		mode = "worker only"
	}

	lo.Info("starting server", "queue", ko.MustString("queue"), "mode", mode,
		"worker-name", ko.MustString("worker-name"))

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
