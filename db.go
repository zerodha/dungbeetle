package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"time"

	"github.com/knadh/goyesql"
)

// Query represents an SQL query with its prepared and raw forms.
type Query struct {
	Stmt *sql.Stmt `json:"-"`
	Raw  string    `json:"raw"`
	DBs  DBs
}

// Queries represents a map of prepared SQL statements.
type Queries map[string]Query

// DBs represents a map of *sql.DB connections.
type DBs map[string]*sql.DB

// Get returns an *sql.DB from the DBs map by name.
func (d DBs) Get(dbName string) *sql.DB {
	return d[dbName]
}

// GetRandom returns a random *sql.DB from the DBs map.
func (d DBs) GetRandom() (string, *sql.DB) {
	stop := 0
	if len(d) > 1 {
		stop = rand.Intn(len(d))
	}

	i := 0
	for _, v := range d {
		if i == stop {
			return "", v
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
func DBsFromTag(dbNames string, dbs DBs) (DBs, error) {
	var (
		chunks = strings.Split(dbNames, ",")
		newDBs = make(DBs)
	)

	for _, c := range chunks {
		if c := strings.TrimSpace(c); c != "" {
			if db, ok := dbs[c]; ok {
				newDBs[c] = db
			} else {
				return nil, fmt.Errorf("unknown db '%s'", c)
			}
		}
	}

	return newDBs, nil
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
func loadSQLqueries(dbs DBs, dir string) (Queries, error) {
	// Discover .sql files.
	files, err := filepath.Glob(dir + "/*.sql")
	if err != nil {
		return nil, fmt.Errorf("unable to read SQL directory '%s': %v", dir, err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no SQL files found in '%s'", dir)
	}

	// Parse all discovered SQL files.
	queries := make(Queries)
	for _, f := range files {
		q := goyesql.MustParseFile(f)

		for name, s := range q {
			var (
				stmt *sql.Stmt

				// A map of DBs are attached to every query. This can be
				// DBs tagged specifically to queries in the SQL file,
				// or will be the map of all avaliable DBs. During execution
				// one of these DBs will be picked randomly.
				toAttach DBs
			)

			// Query already exists.
			if _, ok := queries[string(name)]; ok {
				return nil, fmt.Errorf("duplicate query '%s' (%s)", name, f)
			}

			// Are there specific DB's tagged to the query?
			if dbTag, ok := s.Tags["db"]; ok {
				toAttach, err = DBsFromTag(dbTag, dbs)
				if err != nil {
					return nil, fmt.Errorf("error with query '%s' (%s): %v", name, f, err)
				}
			} else {
				// No specific DBs. Attach all.
				toAttach = dbs
			}

			// Prepare the statement?
			if _, ok := s.Tags["raw"]; !ok {
				// Prepare the statement against all tagged DBs just to be sure.
				for _, db := range toAttach {
					_, err := db.Prepare(s.Query)
					if err != nil {
						return nil, fmt.Errorf("error preparing SQL query '%s': %v", name, err)
					}
				}

				sysLog.Print("-- ", name, " (prepared) ", toAttach.GetNames())
			} else {
				sysLog.Print("-- ", name, " (raw)", toAttach.GetNames())
			}

			queries[name] = Query{Stmt: stmt,
				Raw: s.Query,
				DBs: toAttach,
			}
		}
	}

	return queries, nil
}
