// Package dbpool provides a simple implementation for creating and managing
// SQL db connection pools.
package dbpool

import (
	"database/sql"
	"fmt"
	"math/rand"
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
func New(mp map[string]Config) (map[string]*sql.DB, error) {
	out := make(map[string]*sql.DB, len(mp))
	for name, cfg := range mp {
		db, err := NewConn(cfg)
		if err != nil {
			return nil, err
		}

		out[name] = db
	}

	return out, nil
}

// NewConn creates and returns a database connection.
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
