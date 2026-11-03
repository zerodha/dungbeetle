package sqldb

import (
	"database/sql"
	"strings"
	"testing"
)

func TestQuoteIdentifier(t *testing.T) {
	for _, tc := range []struct{ db, name, want string }{
		{dbTypePostgres, `results_a"; DROP TABLE x; --`, `"results_a""; DROP TABLE x; --"`},
		{dbTypeMysql, "results_a`; DROP TABLE x; --", "`results_a``; DROP TABLE x; --`"},
		{dbTypePostgres, "plain", `"plain"`},
		{dbTypeMysql, "plain", "`plain`"},
	} {
		s := &SqlDB{opt: Opt{DBType: tc.db}}
		if got := s.quoteIdentifier(tc.name); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.db, got, tc.want)
		}
		schema := s.createTableSchema([]string{"value%"}, []*sql.ColumnType{{}})
		for _, query := range []string{schema.dropTable, schema.createTable, schema.insertRow} {
			got := strings.Replace(query, "%s", s.quoteIdentifier(tc.name), 1)
			if !strings.Contains(got, tc.want) {
				t.Errorf("table not safely quoted: %s", got)
			}
			if strings.Contains(got, "%!") {
				t.Errorf("corrupted column name: %s", got)
			}
		}
	}
}

func TestRejectLongTableName(t *testing.T) {
	s, err := NewSQLBackend(nil, Opt{DBType: dbTypePostgres}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.NewResultSet(strings.Repeat("a", 56), "task", 0); err == nil {
		t.Fatal("accepted a table name PostgreSQL would truncate")
	}
}
