package core

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/zerodha/dungbeetle/models"
)

// ResultBackends represents a map of *sql.DB connections.
type ResultBackends map[string]models.ResultBackend

// Get returns an *sql.DB from the DBs map by name.
func (r ResultBackends) Get(name string) models.ResultBackend {
	return r[name]
}

// GetNames returns the list of available DB names.
func (r ResultBackends) GetNames() []string {
	var names []string
	for n := range r {
		names = append(names, n)
	}

	return names
}

// GetRandom returns a random *sql.DB from the DBs map.
func (r ResultBackends) GetRandom() (string, models.ResultBackend) {
	stop := 0
	if len(r) > 1 {
		stop = rand.Intn(len(r))
	}

	i := 0
	for name, v := range r {
		if i == stop {
			return name, v
		}

		i++
	}

	// This'll never happen.
	return "", nil
}

// resultBackendsFromTags splits comma separated result backend names from the SQL queries file,
// validates them against the given map, and returns a list of valid names.
func resultBackendsFromTags(names string, res ResultBackends) (ResultBackends, error) {
	var (
		chunks = strings.Split(names, ",")
		newDBs = make(ResultBackends)
	)

	for _, c := range chunks {
		if c := strings.TrimSpace(c); c != "" {
			if db, ok := res[c]; ok {
				newDBs[c] = db
			} else {
				return nil, fmt.Errorf("unknown result backend '%s'", c)
			}
		}
	}

	return newDBs, nil
}
