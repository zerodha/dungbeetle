// Package goyesql is a Go port of Yesql
//
// It allows you to write SQL queries in separate files.
//
// See rationale at https://github.com/krisajenkins/yesql#rationale
package goyesql

import (
	"bytes"
	"os"
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
