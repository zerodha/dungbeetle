package sqldb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strconv"
	"strings"
	"testing"
)

func init() {
	sql.Register("dungbeetle-decimal", decimalMetaDriver{})
}

// decimalMetaDriver reports column type metadata from a DSN so createTableSchema
// can be tested without a database. DSN columns are comma-separated:
// name:TYPE:precision:scale:ok[:nn]
type decimalMetaDriver struct{}

func (decimalMetaDriver) Open(name string) (driver.Conn, error) {
	specs, err := parseDecimalSpecs(name)
	if err != nil {
		return nil, err
	}
	return &decimalMetaConn{specs: specs}, nil
}

type decimalSpec struct {
	name    string
	dbType  string
	prec    int64
	scale   int64
	hasPrec bool
	notNull bool
}

func parseDecimalSpecs(dsn string) ([]decimalSpec, error) {
	parts := strings.Split(dsn, ",")
	specs := make([]decimalSpec, len(parts))
	for i, part := range parts {
		f := strings.Split(part, ":")
		if len(f) < 5 || len(f) > 6 {
			return nil, strconv.ErrSyntax
		}
		prec, err := strconv.ParseInt(f[2], 10, 64)
		if err != nil {
			return nil, err
		}
		scale, err := strconv.ParseInt(f[3], 10, 64)
		if err != nil {
			return nil, err
		}
		specs[i] = decimalSpec{
			name:    f[0],
			dbType:  f[1],
			prec:    prec,
			scale:   scale,
			hasPrec: f[4] == "1",
			notNull: len(f) == 6 && f[5] == "nn",
		}
	}
	return specs, nil
}

type decimalMetaConn struct {
	specs []decimalSpec
}

func (decimalMetaConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (decimalMetaConn) Close() error                        { return nil }
func (decimalMetaConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }

func (c *decimalMetaConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return &decimalMetaRows{specs: c.specs}, nil
}

type decimalMetaRows struct {
	specs []decimalSpec
}

func (r *decimalMetaRows) Columns() []string {
	names := make([]string, len(r.specs))
	for i, s := range r.specs {
		names[i] = s.name
	}
	return names
}

func (*decimalMetaRows) Close() error              { return nil }
func (*decimalMetaRows) Next([]driver.Value) error { return io.EOF }

func (r *decimalMetaRows) ColumnTypeDatabaseTypeName(i int) string {
	return r.specs[i].dbType
}

func (r *decimalMetaRows) ColumnTypePrecisionScale(i int) (int64, int64, bool) {
	s := r.specs[i]
	if !s.hasPrec {
		return 0, 0, false
	}
	return s.prec, s.scale, true
}

func (r *decimalMetaRows) ColumnTypeNullable(i int) (bool, bool) {
	if r.specs[i].notNull {
		return false, true
	}
	return false, false
}

func columnTypesFor(t *testing.T, spec string) ([]string, []*sql.ColumnType) {
	t.Helper()
	db, err := sql.Open("dungbeetle-decimal", spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	rows, err := db.Query("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { rows.Close() })

	cols, err := rows.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name()
	}
	return names, cols
}

func TestCreateTableDecimalPrecision(t *testing.T) {
	for _, tc := range []struct {
		name string
		db   string
		spec string
		want string
	}{
		{
			name: "mysql decimal keeps fractional scale",
			db:   dbTypeMysql,
			spec: "amount:DECIMAL:12:2:1",
			want: "CREATE  TABLE IF NOT EXISTS %s (`amount` DECIMAL(12,2));",
		},
		{
			name: "mysql decimal keeps a zero scale",
			db:   dbTypeMysql,
			spec: "amount:DECIMAL:18:0:1",
			want: "CREATE  TABLE IF NOT EXISTS %s (`amount` DECIMAL(18,0));",
		},
		{
			name: "mysql decimal without metadata stays bare",
			db:   dbTypeMysql,
			spec: "amount:DECIMAL:0:0:0",
			want: "CREATE  TABLE IF NOT EXISTS %s (`amount` DECIMAL);",
		},
		{
			name: "mysql numeric keeps scale",
			db:   dbTypeMysql,
			spec: "amount:NUMERIC:10:4:1",
			want: "CREATE  TABLE IF NOT EXISTS %s (`amount` DECIMAL(10,4));",
		},
		{
			name: "mysql float stays bare decimal",
			db:   dbTypeMysql,
			spec: "ratio:FLOAT:9223372036854775807:2:1",
			want: "CREATE  TABLE IF NOT EXISTS %s (`ratio` DECIMAL);",
		},
		{
			name: "mysql double stays bare decimal",
			db:   dbTypeMysql,
			spec: "ratio:DOUBLE:20:4:1",
			want: "CREATE  TABLE IF NOT EXISTS %s (`ratio` DECIMAL);",
		},
		{
			name: "precision above mysql limit stays bare",
			db:   dbTypeMysql,
			spec: "amount:DECIMAL:80:2:1",
			want: "CREATE  TABLE IF NOT EXISTS %s (`amount` DECIMAL);",
		},
		{
			name: "unconstrained numeric typmod stays bare",
			db:   dbTypeMysql,
			spec: "amount:NUMERIC:65535:65531:1",
			want: "CREATE  TABLE IF NOT EXISTS %s (`amount` DECIMAL);",
		},
		{
			name: "scale above precision stays bare",
			db:   dbTypeMysql,
			spec: "amount:DECIMAL:10:12:1",
			want: "CREATE  TABLE IF NOT EXISTS %s (`amount` DECIMAL);",
		},
		{
			name: "postgres result table stays bare decimal",
			db:   dbTypePostgres,
			spec: "amount:DECIMAL:12:2:1",
			want: `CREATE  TABLE IF NOT EXISTS %s ("amount" DECIMAL);`,
		},
		{
			name: "not null is kept with the scale",
			db:   dbTypeMysql,
			spec: "amount:DECIMAL:12:2:1:nn",
			want: "CREATE  TABLE IF NOT EXISTS %s (`amount` DECIMAL(12,2) NOT NULL);",
		},
		{
			name: "float beside a decimal is not given a scale",
			db:   dbTypeMysql,
			spec: "amount:DECIMAL:12:2:1,ratio:FLOAT:20:2:1",
			want: "CREATE  TABLE IF NOT EXISTS %s (`amount` DECIMAL(12,2),`ratio` DECIMAL);",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			names, cols := columnTypesFor(t, tc.spec)
			s := &SqlDB{opt: Opt{DBType: tc.db}}
			got := s.createTableSchema(names, cols).createTable
			if got != tc.want {
				t.Fatalf("got %s\nwant %s", got, tc.want)
			}
		})
	}
}
