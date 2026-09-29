package core

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zerodha/dungbeetle/v2/internal/resultbackends/sqldb"
)

func TestWriteResults(t *testing.T) {
	readErr := errors.New("source connection lost")
	for _, tc := range []struct {
		name    string
		rows    int
		readErr error
	}{
		{name: "complete results", rows: 3},
		{name: "empty results"},
		{name: "read error after two rows", rows: 2, readErr: readErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourceDB, source, err := sqlmock.New()
			require.NoError(t, err)
			defer sourceDB.Close()

			resultDB, result, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer resultDB.Close()

			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			backend, err := sqldb.NewSQLBackend(resultDB, sqldb.Opt{DBType: "postgres"}, logger)
			require.NoError(t, err)

			sourceRows := sqlmock.NewRowsWithColumnDefinition(sqlmock.NewColumn("id").OfType("INT8", int64(0)))
			for i := 0; i < tc.rows; i++ {
				sourceRows.AddRow(int64(i + 1))
			}
			if tc.readErr != nil {
				sourceRows.AddRow(int64(tc.rows+1)).RowError(tc.rows, tc.readErr)
			}
			source.ExpectQuery("SELECT id FROM entries").WillReturnRows(sourceRows).RowsWillBeClosed()
			rows, err := sourceDB.Query("SELECT id FROM entries")
			require.NoError(t, err)
			defer rows.Close()

			result.ExpectBegin()
			result.ExpectBegin()
			result.ExpectExec(`DROP TABLE IF EXISTS "results_job1";`).WillReturnResult(sqlmock.NewResult(0, 0))
			result.ExpectExec(`CREATE TABLE IF NOT EXISTS "results_job1" ("id" BIGINT);`).WillReturnResult(sqlmock.NewResult(0, 0))
			result.ExpectCommit()
			for i := 0; i < tc.rows; i++ {
				result.ExpectExec(`INSERT INTO "results_job1" ("id") VALUES ($1)`).
					WithArgs(int64(i + 1)).WillReturnResult(sqlmock.NewResult(0, 1))
			}
			if tc.readErr != nil {
				result.ExpectRollback()
			} else {
				result.ExpectCommit()
			}

			co := &Core{lo: logger}
			task := Task{Name: "report", ResultBackends: ResultBackends{"test": backend}}
			count, err := co.writeResults("job1", task, time.Hour, rows)
			assert.Equal(t, int64(tc.rows), count)
			if tc.readErr != nil {
				assert.ErrorIs(t, err, tc.readErr)
			} else {
				assert.NoError(t, err)
			}
			assert.NoError(t, source.ExpectationsWereMet())
			assert.NoError(t, result.ExpectationsWereMet())
		})
	}
}
