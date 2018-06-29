package escape

// Original from: https://github.com/tj/go-pg-escape

import (
	"bytes"
	"errors"
	"regexp"
)

var ident = regexp.MustCompile(`(?i)^[a-z_][a-z0-9_$]*$`)
var params = regexp.MustCompile(`%([s])`)

// Escaper represents an instance of the SQL query escaper.
type Escaper struct {
	buf bytes.Buffer
}

// New returns a new instance of the query escaper.
func New() *Escaper {
	return &Escaper{
		buf: bytes.Buffer{},
	}
}

// Escape the given `query` with positional `args`.
func (esc *Escaper) Escape(query []byte, args ...[]byte) ([]byte, error) {
	matches := params.FindAllSubmatch(query, -1)

	length := len(matches)
	argc := len(args)

	if argc > length {
		return nil, errors.New("too many arguments for escaped query")
	}

	if argc < length {
		return nil, errors.New("too few arguments for escaped query")
	}

	for i, match := range matches {
		arg := args[i]
		switch string(match[1]) {
		case "s":
			query = bytes.Replace(query, []byte("%s"), esc.literal(arg), 1)
		}
	}

	return query, nil
}

// Literal escape the given string.
func (esc *Escaper) literal(s []byte) []byte {
	s = bytes.Replace(s, []byte(`'`), []byte(`''`), -1)
	s = bytes.Replace(s, []byte(`\`), []byte(`\\`), -1)
	s = bytes.Replace(s, []byte(`"`), []byte(`\"`), -1)

	esc.buf.Reset()
	esc.buf.Write([]byte(`'`))
	esc.buf.Write(s)
	esc.buf.Write([]byte(`'`))

	return esc.buf.Bytes()
}
