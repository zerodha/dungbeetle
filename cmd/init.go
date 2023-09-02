package main

import (
	"embed"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/knadh/koanf/v2"
	"github.com/zerodha/dungbeetle/internal/resultbackends/sqldb"
)

// dbConfig represents an SQL database's configuration.
type dbConfig struct {
	Type           string        `mapstructure:"type"`
	DSN            string        `mapstructure:"dsn"`
	Unlogged       bool          `mapstructure:"unlogged"`
	MaxIdleConns   int           `mapstructure:"max_idle"`
	MaxActiveConns int           `mapstructure:"max_active"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
}

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

func initDBs(server *Server, ko *koanf.Koanf) {
	// Source DBs.
	var srcDBs map[string]dbConfig
	if err := ko.Unmarshal("db", &srcDBs); err != nil {
		lo.Fatalf("error reading source DB config: %v", err)
	}
	if len(srcDBs) == 0 {
		lo.Fatal("found 0 source databases in config")
	}

	// Result DBs.
	var resDBs map[string]dbConfig
	if err := ko.Unmarshal("results", &resDBs); err != nil {
		lo.Fatalf("error reading source DB config: %v", err)
	}
	if len(resDBs) == 0 {
		lo.Fatal("found 0 result backends in config")
	}

	// Connect to source DBs.
	for dbName, cfg := range srcDBs {
		lo.Printf("connecting to source %s DB %s", cfg.Type, dbName)
		conn, err := connectDB(cfg)
		if err != nil {
			log.Fatal(err)
		}

		server.DBs[dbName] = conn
	}

	// Connect to backend DBs.
	for dbName, cfg := range resDBs {
		lo.Printf("connecting to result backend %s DB %s", cfg.Type, dbName)
		conn, err := connectDB(cfg)
		if err != nil {
			log.Fatal(err)
		}

		opt := sqldb.Opt{
			DBType:         cfg.Type,
			ResultsTable:   ko.MustString(fmt.Sprintf("results.%s.results_table", dbName)),
			UnloggedTables: cfg.Unlogged,
		}

		// Create a new backend instance.
		backend, err := sqldb.NewSQLBackend(conn, opt, lo)
		if err != nil {
			log.Fatalf("error initializing result backend: %v", err)
		}

		server.ResultBackends[dbName] = backend
	}

	// Parse and load SQL queries ("tasks").
	for _, d := range ko.MustStrings("sql-directory") {
		lo.Printf("loading SQL queries from directory: %s", d)
		tasks, err := loadSQLTasks(d, server.DBs, server.ResultBackends, ko.MustString("queue"))
		if err != nil {
			lo.Fatal(err)
		}

		for t, q := range tasks {
			if _, ok := server.Tasks[t]; ok {
				lo.Fatalf("duplicate task %s", t)
			}

			server.Tasks[t] = q
		}

		lo.Printf("loaded %d SQL queries from %s", len(tasks), d)
	}
	lo.Printf("loaded %d tasks in total", len(server.Tasks))

}
