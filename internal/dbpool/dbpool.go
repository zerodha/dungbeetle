// Package dbpool provides a simple implementation for creating and managing
// SQL db connection pools.
package dbpool

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/url"
	"strings"
	"time"
)

// Pool represents a map of *sql.DB connections.
type Pool map[string]*sql.DB

// Config is a DB's config for connecting.
type Config struct {
	Type           string        `koanf:"type"`
	DSN            string        `koanf:"dsn"`
	Unlogged       bool          `koanf:"unlogged"`
	MaxIdleConns   int           `koanf:"max_idle"`
	MaxActiveConns int           `koanf:"max_active"`
	ConnectTimeout time.Duration `koanf:"connect_timeout"`
}

// New takes a map of named DB configurations and returns a named map
// of DB connections.
func New(mp map[string]Config, lo *slog.Logger) (map[string]*sql.DB, error) {
	out := make(map[string]*sql.DB, len(mp))
	for name, cfg := range mp {
		if cfg.Type == "clickhouse" {
			warnings, err := checkClickhouseDSN(cfg.DSN)
			if err != nil {
				return nil, fmt.Errorf("db %q: %w", name, err)
			}
			for _, w := range warnings {
				lo.Warn("clickhouse dsn", "db", name, "warning", w)
			}
		}

		db, err := NewConn(cfg)
		if err != nil {
			return nil, fmt.Errorf("db %q: %w", name, err)
		}

		out[name] = db
	}

	return out, nil
}

// connectDB creates and returns a database connection.
func NewConn(cfg Config) (*sql.DB, error) {
	db, err := sql.Open(cfg.Type, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("error connecting to db : %v", err)
	}

	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetMaxOpenConns(cfg.MaxActiveConns)
	db.SetConnMaxLifetime(time.Second * cfg.ConnectTimeout)

	// Ping database to check for connection issues.
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging db : %v", err)
	}

	return db, nil
}

// checkClickhouseDSN validates a clickhouse-go v1 DSN, which reads
// credentials and the database only from the `username`, `password` and
// `database` query params. It errors on DSNs that cannot work and returns
// warnings for parts the driver silently ignores.
func checkClickhouseDSN(dsn string) ([]string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, errors.New("clickhouse dsn is not a valid URL; URL-encode special characters in the password (eg: % as %25, + as %2B)")
	}
	if u.Host == "" {
		return nil, errors.New("clickhouse dsn has no host; expected tcp://host:port?username=user&password=password&database=dbname")
	}

	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return nil, errors.New("clickhouse dsn query has an invalid %-escape or a raw semicolon; URL-encode the password (eg: % as %25, + as %2B, ; as %3B)")
	}

	var warnings []string
	if u.User != nil {
		warnings = append(warnings, "user:pass@ in the dsn is ignored by the driver; use the username and password query params")
	}
	if strings.Trim(u.Path, "/") != "" {
		warnings = append(warnings, "/dbname in the dsn is ignored by the driver; use the database query param")
	}
	if q.Get("username") == "" {
		warnings = append(warnings, "no username query param; the driver connects as the default user")
	}

	return warnings, nil
}

// Get returns an *sql.DB from the DBs map by name.
func (p Pool) Get(dbName string) (*sql.DB, error) {
	db, ok := p[dbName]
	if !ok {
		return nil, fmt.Errorf("unknown db: %s", dbName)
	}

	return db, nil
}

// GetRandom returns a random *sql.DB from the DBs map.
func (p Pool) GetRandom() (string, *sql.DB) {
	stop := 0
	if len(p) > 1 {
		stop = rand.Intn(len(p))
	}

	i := 0
	for name, v := range p {
		if i == stop {
			return name, v
		}

		i++
	}

	// This'll never happen.
	return "", nil
}

// GetRandomFromSet returns a random *sql.DB from the DBs map
// from a given set of names.
func (p Pool) GetRandomFromSet(dbNames []string) (string, *sql.DB) {
	stop := rand.Intn(len(dbNames))
	for i, n := range dbNames {
		if i == stop {
			return n, p[n]
		}
	}

	// This'll never happen.
	return "", nil
}

// GetNames returns the list of available DB names.
func (p Pool) GetNames() []string {
	var names []string
	for n := range p {
		names = append(names, n)
	}

	return names
}

// FilterByTags returns a pool of collections filtered by the given names.
func (p Pool) FilterByTags(names []string) (Pool, error) {
	out := make(Pool)

	for _, name := range names {
		if n := strings.TrimSpace(name); n != "" {
			if db, ok := p[n]; ok {
				out[n] = db
			} else {
				return nil, fmt.Errorf("unknown db %s", n)
			}
		}
	}

	return out, nil
}
