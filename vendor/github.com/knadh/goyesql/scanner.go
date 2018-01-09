package goyesql

import (
	"bufio"
	"fmt"
	"io"
)

const (
	tagName = "name"
)

// Err represents an error.
type Err struct {
	err string
}

func (e Err) Error() string {
	return fmt.Sprintf(e.err)
}

// Query is a parsed query along with tags.
type Query struct {
	Query string
	Tags  map[string]string
}

// Queries is a map associating a Tag to its Query
type Queries map[string]*Query

// ParseReader takes an io.Reader and returns Queries or an error.
func ParseReader(reader io.Reader) (Queries, error) {
	var (
		nameTag string
		queries = make(Queries)
		scanner = bufio.NewScanner(reader)
	)

	for scanner.Scan() {
		line := parseLine(scanner.Text())

		switch line.Type {
		case lineBlank, lineComment:
			// Ignore.
			continue

		case lineQuery:
			// Got a query but no preceding name tag.
			if nameTag == "" {
				return nil, Err{fmt.Sprintf("Query is missing the 'name' tag: %s", line.Value)}
			}

			q := line.Value

			// If query is multiline.
			if queries[nameTag].Query != "" {
				q = " " + q
			}

			queries[nameTag].Query += q

		case lineTag:
			// Has this name already been read?
			if line.Tag == tagName {
				nameTag = line.Value

				if _, ok := queries[nameTag]; ok {
					return nil, Err{fmt.Sprintf("Duplicate tag %s = %s ", line.Tag, line.Value)}
				}

				queries[nameTag] = &Query{Tags: make(map[string]string)}
			} else {
				// Is there a name tag for this query?
				if _, ok := queries[nameTag]; !ok {
					return nil, Err{"'name' should be the first tag"}
				}

				// Has this tag already been used on this query?
				if _, ok := queries[nameTag].Tags[line.Tag]; ok {
					return nil, Err{fmt.Sprintf("Duplicate tag %s = %s ", line.Tag, line.Value)}
				}

				queries[nameTag].Tags[line.Tag] = line.Value
			}
		}
	}

	for name, q := range queries {
		if q.Query == "" {
			return nil, Err{fmt.Sprintf("'%s' is missing query", name)}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return queries, nil
}
