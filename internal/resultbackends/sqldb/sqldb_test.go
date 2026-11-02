package sqldb

import (
	"fmt"
	"strings"
	"testing"
)

func newTestBackend(t *testing.T, dbType string) *SqlDB {
	t.Helper()

	// db is nil: none of the assertions below reach the connection.
	s, err := NewSQLBackend(nil, Opt{DBType: dbType}, nil)
	if err != nil {
		t.Fatalf("NewSQLBackend: %v", err)
	}

	return s
}

func TestQuoteIdentifierEscapesQuotes(t *testing.T) {
	for _, c := range []struct {
		dbType string
		name   string
		want   string
	}{
		{dbTypePostgres, "results_job1", `"results_job1"`},
		{dbTypeMysql, "results_job1", "`results_job1`"},

		// A quote inside the name must be doubled, not left to close the
		// identifier and start fresh SQL.
		{dbTypePostgres, `a", "victim`, `"a"", ""victim"`},
		{dbTypeMysql, "a`, `victim", "`a``, ``victim`"},
		{dbTypePostgres, `s"; DROP TABLE victim; --`, `"s""; DROP TABLE victim; --"`},
		{dbTypeMysql, "s`; DROP TABLE victim; --", "`s``; DROP TABLE victim; --`"},
	} {
		t.Run(c.dbType+"/"+c.name, func(t *testing.T) {
			if got := newTestBackend(t, c.dbType).quoteIdentifier(c.name); got != c.want {
				t.Fatalf("quoteIdentifier(%q) = %q, want %q", c.name, got, c.want)
			}
		})
	}
}

// TestSchemaTemplatesLeaveTableNameUnquoted asserts the contract between
// createTableSchema and its callers: the table placeholder carries no quotes of
// its own, so callers must fill it with quoteIdentifier(tbl).
func TestSchemaTemplatesLeaveTableNameUnquoted(t *testing.T) {
	s := newTestBackend(t, dbTypePostgres)
	sch := s.createTableSchema(nil, nil)

	for name, tpl := range map[string]string{
		"dropTable":   sch.dropTable,
		"createTable": sch.createTable,
		"insertRow":   sch.insertRow,
	} {
		for _, bad := range []string{`"%s"`, "`%s`"} {
			if strings.Contains(tpl, bad) {
				t.Fatalf("%s template pre-quotes the table placeholder: %q", name, tpl)
			}
		}

		// Filling the template the way the writers do must yield one quoted
		// identifier, with the injected quote escaped inside it.
		stmt := fmt.Sprintf(tpl, s.quoteIdentifier(`evil"; DROP TABLE victim; --`))
		if !strings.Contains(stmt, `"evil""; DROP TABLE victim; --"`) {
			t.Fatalf("%s did not escape the table name: %q", name, stmt)
		}
	}
}

func TestNewResultSetRejectsOverlongTableName(t *testing.T) {
	s := newTestBackend(t, dbTypePostgres)
	s.opt.ResultsTable = "results_%s"

	// Long enough to be truncated by Postgres, which would make two job IDs
	// sharing a prefix write to the same table.
	if _, err := s.NewResultSet(strings.Repeat("a", 80), "task", 0); err == nil {
		t.Fatal("NewResultSet accepted a table name over the identifier limit")
	}
}
