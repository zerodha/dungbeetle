// Package goyesql is a Go port of Yesql
//
// It allows you to write SQL queries in separate files.
//
// See rationale at https://github.com/krisajenkins/yesql#rationale
package goyesql

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"strings"
)

// Some helpers to read files

// ParseFile reads a file and return Queries or an error
func ParseFile(path string) (Queries, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return ParseReader(file)
}

// MustParseFile calls ParseFile but panic if an error occurs
func MustParseFile(path string) Queries {
	queries, err := ParseFile(path)
	if err != nil {
		panic(err)
	}

	return queries
}

// ParseBytes parses bytes and returns Queries or an error.
func ParseBytes(b []byte) (Queries, error) {
	return ParseReader(bytes.NewReader(b))
}

// MustParseBytes parses bytes but panics if an error occurs.
func MustParseBytes(b []byte) Queries {
	queries, err := ParseBytes(b)
	if err != nil {
		panic(err)
	}

	return queries
}

// ScanToStruct prepares a given set of Queries and assigns the resulting
// *sql.Stmt statements to the fields of a given struct, matching based on the name
// in the `query` tag in the struct field names.
func ScanToStruct(obj interface{}, q Queries, db *sql.DB) error {
	ob := reflect.ValueOf(obj)
	if ob.Kind() == reflect.Ptr {
		ob = ob.Elem()
	}

	if ob.Kind() != reflect.Struct {
		return fmt.Errorf("Failed to apply SQL statements to struct. Non struct type: %T", ob)
	}

	// Go through every field in the struct and look for it in the Args map.
	for i := 0; i < ob.NumField(); i++ {
		f := ob.Field(i)

		if f.IsValid() {
			if tag := ob.Type().Field(i).Tag.Get("query"); tag != "" && tag != "-" {
				// Extract the value of the `query` tag.
				var (
					tg   = strings.Split(tag, ",")
					name string
				)
				if len(tg) == 2 {
					if tg[0] != "-" && tg[0] != "" {
						name = tg[0]
					}
				} else {
					name = tg[0]
				}

				// Query name found in the field tag is not in the map.
				if _, ok := q[name]; !ok {
					return fmt.Errorf("query '%s' not found in query map", name)
				}

				if !f.CanSet() {
					return fmt.Errorf("query field '%s' is unexported", ob.Type().Field(i).Name)
				}

				switch f.Type().String() {
				case "string":
					// Unprepared SQL query.
					f.Set(reflect.ValueOf(q[name].Query))
				case "*sql.Stmt":
					// Prepared query.
					stmt, err := db.Prepare(q[name].Query)
					if err != nil {
						return fmt.Errorf("Error preparing query '%s': %v", name, err)
					}

					f.Set(reflect.ValueOf(stmt))
				}
			}
		}
	}

	return nil
}
