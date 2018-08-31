package main

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/knadh/goyesql"
)

// Task represents an SQL query with its prepared and raw forms.
type Task struct {
	Name           string
	Queue          string
	Stmt           *sql.Stmt `json:"-"`
	Raw            string    `json:"raw"`
	DBs            DBs
	ResultBackends ResultBackends
}

// Tasks represents a map of prepared SQL statements.
type Tasks map[string]Task

// loadSQLTasks loads SQL queries from all the .sql
// files in a given directory.
func loadSQLTasks(dir string, dbs DBs, resBackends ResultBackends, defQueue string) (Tasks, error) {
	// Discover .sql files.
	files, err := filepath.Glob(dir + "/*.sql")
	if err != nil {
		return nil, fmt.Errorf("unable to read SQL directory %s: %v", dir, err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no SQL files found in %s", dir)
	}

	// Parse all discovered SQL files.
	tasks := make(Tasks)
	for _, f := range files {
		q := goyesql.MustParseFile(f)

		for name, s := range q {
			var (
				stmt *sql.Stmt

				// A map of DBs are attached to every query. This can be
				// DBs tagged specifically to queries in the SQL file,
				// or will be the map of all avaliable DBs. During execution
				// one of these DBs will be picked randomly.
				dbsToAttach DBs

				resBackendsToAttach ResultBackends
			)

			// Query already exists.
			if _, ok := tasks[string(name)]; ok {
				return nil, fmt.Errorf("duplicate query %s (%s)", name, f)
			}

			// Are there specific DB's tagged to the query?
			if dbTag, ok := s.Tags["db"]; ok {
				dbsToAttach, err = DBsFromTag(dbTag, dbs)
				if err != nil {
					return nil, fmt.Errorf("error loading query %s (%s): %v", name, f, err)
				}
			} else {
				// No specific DBs. Attach all.
				dbsToAttach = dbs
			}

			// Are there specific result backends tagged to the query?
			if resTags, ok := s.Tags["results"]; ok {
				resBackendsToAttach, err = resultBackendsFromTags(resTags, resBackends)
				if err != nil {
					return nil, fmt.Errorf("error loading query %s (%s): %v", name, f, err)
				}
			} else {
				// No specific DBs. Attach all.
				resBackendsToAttach = resBackends
			}

			// Prepare the statement?
			typ := ""
			if _, ok := s.Tags["raw"]; ok {
				typ = "raw"
			} else {
				// Prepare the statement against all tagged DBs just to be sure.
				typ = "prepared"
				for _, db := range dbsToAttach {
					_, err := db.Prepare(s.Query)
					if err != nil {
						return nil, fmt.Errorf("error preparing SQL query %s: %v", name, err)
					}
				}
			}

			// Is there a queue?
			queue := defQueue
			if v, ok := s.Tags["queue"]; ok {
				queue = strings.TrimSpace(v)
			}

			sysLog.Printf("-- loaded task %s (%s) (db = %v) (results = %v) (queue = %v)", name, typ,
				dbsToAttach.GetNames(), resBackendsToAttach.GetNames(), queue)
			tasks[name] = Task{
				Name:           name,
				Queue:          queue,
				Stmt:           stmt,
				Raw:            s.Query,
				DBs:            dbsToAttach,
				ResultBackends: resBackendsToAttach,
			}
		}
	}

	return tasks, nil
}
