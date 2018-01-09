# goyesql

Golang + [Yesql](https://github.com/krisajenkins/yesql)

Parse a file and associate SQL queries to a map. Useful for separating SQL from code logic.

This is based on [nleof/goyesql](https://github.com/nleof/goyesql) but is not compatible with the original repository. This library introduces arbitrary tag types and changes structs and error types.

# Installation

```
$ go get -u github.com/knadh/goyesql
```

# Usage

Create a file containing your SQL queries

```sql
-- queries.sql

-- name: list
-- some: param
-- some_other: param
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
// queries["list"].Query is the parsed SQL query string
// queries["list"].Query is the list of arbitrary tags (some=param, some_other=param)
```

# Embedding

You can use [bindata](https://github.com/jteeuwen/go-bindata) and `ParseBytes` func for embedding your queries in your binary.

```go
package main

import (
	"github.com/knadh/goyesql"
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
