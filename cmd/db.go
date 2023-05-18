package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// DBs represents a map of *sql.DB connections.
type DBs map[string]*sql.DB

// Get returns an *sql.DB from the DBs map by name.
func (d DBs) Get(dbName string) (*sql.DB, error) {
	db, ok := d[dbName]
	if !ok {
		return nil, fmt.Errorf("unknown db: %s", dbName)
	}

	return db, nil
}

// GetRandom returns a random *sql.DB from the DBs map.
func (d DBs) GetRandom() (string, *sql.DB) {
	stop := 0
	if len(d) > 1 {
		stop = rand.Intn(len(d))
	}

	i := 0
	for name, v := range d {
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
func (d DBs) GetRandomFromSet(dbNames []string) (string, *sql.DB) {
	stop := rand.Intn(len(dbNames))
	for i, n := range dbNames {
		if i == stop {
			return n, d[n]
		}
	}

	// This'll never happen.
	return "", nil
}

// GetNames returns the list of available DB names.
func (d DBs) GetNames() []string {
	var names []string
	for n := range d {
		names = append(names, n)
	}

	return names
}

// DBsFromTag splits comma separated DB names from the SQL queries file,
// validates them against the DB map, and returns a list of valid names.
func DBsFromTag(names string, dbs DBs) (DBs, error) {
	var (
		chunks = strings.Split(names, ",")
		newDBs = make(DBs)
	)

	for _, c := range chunks {
		if c := strings.TrimSpace(c); c != "" {
			if db, ok := dbs[c]; ok {
				newDBs[c] = db
			} else {
				return nil, fmt.Errorf("unknown db %s", c)
			}
		}
	}

	return newDBs, nil
}

// connectDB creates and returns a database connection.
func connectDB(cfg DBConfig) (*sql.DB, error) {
	db, err := sql.Open(cfg.Type, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("error connecting to db : %v", err)
	}

	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetMaxOpenConns(cfg.MaxActiveConns)
	db.SetConnMaxLifetime(time.Second * cfg.ConnectTimeout)

	// Ping database to check for connection issues.
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("error connecting to db : %v", err)
	}

	return db, nil
}
