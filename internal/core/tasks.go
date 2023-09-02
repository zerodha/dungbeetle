package core

import (
	"database/sql"
	"fmt"
	"path"
	"path/filepath"
	"strings"

	"github.com/knadh/goyesql/v2"
	"github.com/zerodha/dungbeetle/internal/dbpool"
)

// Task represents an SQL query with its prepared and raw forms.
type Task struct {
	Name           string
	Queue          string
	Stmt           *sql.Stmt `json:"-"`
	Raw            string    `json:"raw"`
	DBs            dbpool.Pool
	ResultBackends ResultBackends
}

// Tasks represents a map of prepared SQL statements.
type Tasks map[string]Task

// LoadTasks loads SQL queries from all the .sql files in a given directory.
func (co *Core) LoadTasks(dirs []string) error {
	for _, d := range dirs {
		co.lo.Printf("loading SQL queries from directory: %s", d)
		tasks, err := co.loadTasks(d)
		if err != nil {
			return nil
		}

		for t, q := range tasks {
			if _, ok := co.tasks[t]; ok {
				return fmt.Errorf("duplicate task %s", t)
			}

			co.tasks[t] = q
		}

		co.lo.Printf("loaded %d SQL queries from %s", len(tasks), d)
	}

	return nil
}

func (co *Core) loadTasks(dir string) (Tasks, error) {
	// Discover .sql files.
	files, err := filepath.Glob(path.Join(dir, "*.sql"))
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
				srcDBs dbpool.Pool

				srcPool ResultBackends
			)

			// Query already exists.
			if _, ok := tasks[string(name)]; ok {
				return nil, fmt.Errorf("duplicate query %s (%s)", name, f)
			}

			// Are there specific DB's tagged to the query?
			if dbTag, ok := s.Tags["db"]; ok {
				srcDBs, err = co.srcDBs.FilterByTags(strings.Split(dbTag, ","))
				if err != nil {
					return nil, fmt.Errorf("error loading query %s (%s): %v", name, f, err)
				}
			} else {
				// No specific DBs. Attach all.
				srcDBs = co.srcDBs
			}

			// Are there specific result backends tagged to the query?
			if resTags, ok := s.Tags["results"]; ok {
				srcPool, err = resultBackendsFromTags(resTags, co.resultBackends)
				if err != nil {
					return nil, fmt.Errorf("error loading query %s (%s): %v", name, f, err)
				}
			} else {
				// No specific DBs. Attach all.
				srcPool = co.resultBackends
			}

			// Prepare the statement?
			typ := ""
			if _, ok := s.Tags["raw"]; ok {
				typ = "raw"
			} else {
				// Prepare the statement against all tagged DBs just to be sure.
				typ = "prepared"
				for _, db := range srcDBs {
					_, err := db.Prepare(s.Query)
					if err != nil {
						return nil, fmt.Errorf("error preparing SQL query %s: %v", name, err)
					}
				}
			}

			// Is there a queue?
			queue := co.opt.DefaultQueue
			if v, ok := s.Tags["queue"]; ok {
				queue = strings.TrimSpace(v)
			}

			co.lo.Printf("-- task %s (%s) (db = %v) (results = %v) (queue = %v)", name, typ, srcDBs.GetNames(), srcPool.GetNames(), queue)
			tasks[name] = Task{
				Name:           name,
				Queue:          queue,
				Stmt:           stmt,
				Raw:            s.Query,
				DBs:            srcDBs,
				ResultBackends: srcPool,
			}
		}
	}

	return tasks, nil
}
