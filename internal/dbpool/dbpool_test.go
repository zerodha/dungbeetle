package dbpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckClickhouseDSNValid(t *testing.T) {
	// An URL-encoded password with characters that break a raw DSN.
	w, err := checkClickhouseDSN("tcp://127.0.0.1:9000?username=u&password=a%2Bb%25zz%2F%40%23%3F%26%3Dx&database=d")
	assert.NoError(t, err)
	assert.Empty(t, w)
}

func TestCheckClickhouseDSNErrors(t *testing.T) {
	cases := map[string]string{
		"empty":          "",
		"no scheme":      "localhost:9000?username=u",
		"whole encoded":  "tcp%3A%2F%2Fh%3A9000%3Fusername%3Du",
		"invalid escape": "tcp://127.0.0.1:9000?username=u&password=secret%zz",
		"raw semicolon":  "tcp://127.0.0.1:9000?username=u&password=sec;ret",
		"bad user-info":  "clickhouse://u:secret%zz@127.0.0.1:9000/d",
	}
	for name, dsn := range cases {
		_, err := checkClickhouseDSN(dsn)
		assert.Error(t, err, name)

		// Errors must not echo any part of the DSN, which carries credentials.
		assert.NotContains(t, err.Error(), "secret", name)
		assert.NotContains(t, err.Error(), "%zz", name)
	}
}

func TestCheckClickhouseDSNWarnings(t *testing.T) {
	// user:pass@ and /dbname are ignored by clickhouse-go v1, which then
	// connects as the default user.
	w, err := checkClickhouseDSN("clickhouse://u:p@127.0.0.1:9000/d")
	assert.NoError(t, err)
	assert.Len(t, w, 3)
	for _, s := range w {
		assert.NotContains(t, s, "127.0.0.1")
	}
}
