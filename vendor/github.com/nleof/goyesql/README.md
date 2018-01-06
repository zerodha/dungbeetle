[![Build Status](https://travis-ci.org/nleof/goyesql.svg)](https://travis-ci.org/nleof/goyesql)
[![GoDoc](https://godoc.org/github.com/nleof/goyesql?status.svg)](https://godoc.org/github.com/nleof/goyesql)
[![Go Report Card](https://goreportcard.com/badge/github.com/nleof/goyesql)](https://goreportcard.com/report/github.com/nleof/goyesql)

# goyesql

Golang + [Yesql](https://github.com/krisajenkins/yesql)

Parse a file and associate SQL queries to a map. Useful for separating SQL from code logic.


# Installation

```
$ go get -u github.com/nleof/goyesql
```

# Usage

Create a file containing your SQL queries

```sql
-- queries.sql

-- name: list
SELECT *
FROM foo;

-- name: get
SELECT *
FROM foo
WHERE bar = $1;
```

And just call them in your code!

```go
queries := goyesql.MustParseFile("queries.sql")
// use queries["list"] with sql/database, sqlx ...
```

Enjoy!

# Embedding

You can use [bindata](https://github.com/jteeuwen/go-bindata) and `ParseBytes` func for embedding your queries in your binary.

```go
package main

import (
	"github.com/nleof/goyesql"
)

func main() {
	data := MustAsset("resources/sql/foo.sql")
	queries := goyesql.MustParseBytes(data)
	// your turn
}
```

```sh
go-bindata resources/...
go run main.go bindata.go
```
