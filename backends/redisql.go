package backends

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	// Name of the INSERT() statement to register in rediSQL.
	// It's registered once for a particular taskName and reused for
	// every subsequent insert -- redbeardlab.tech/rediSQL/references/
	rediSQLInsertStmt = "insert_result"
	rediSQLTestDB     = "sql_jobber_redisql_test"
)

// rediSQL represents the RediSQL backend.
type rediSQL struct {
	pool         *redis.Pool
	resultsTable string
	resultsTTL   time.Duration

	// The result schemas (CREATE TABLE ...) for rediSQL are dynamically
	// generated everytime queries are executed based on their result columns.
	// They're cached here so as to avoid repetetive generation.
	resTableSchemas map[string]string
	schemaMutex     sync.RWMutex
}

// rediSQLWriter represents a writer that saves results
// to a rediSQL backend.
type rediSQLWriter struct {
	dbName      string
	taskName    string
	colsWritten bool
	ttl         time.Duration

	backend *rediSQL
	c       redis.Conn
}

// RedisConfig represents a Redis instance's configuration.
type RedisConfig struct {
	Address        string        `mapstructure:"address"`
	Password       string        `mapstructure:"password"`
	DB             int           `mapstructure:"db"`
	MaxIdleConns   int           `mapstructure:"max_idle"`
	MaxActiveConns int           `mapstructure:"max_active"`
	ConnectTimeout time.Duration `mapstructure:"connect_timeout"`
	ReadTimeout    time.Duration `mapstructure:"read_timeout"`
	WriteTimeout   time.Duration `mapstructure:"write_timeout"`
	ResultsTable   string        `mapstructure:"results_table"`
	ResultsTTL     time.Duration `mapstructure:"results_ttl"`
}

// NewRediSQL returns a new RediSQL results backend instance.
// It accepts a Redis configuration and internally creates
// a Redis connection pool.
func NewRediSQL(redisConfig interface{}) (ResultBackend, error) {
	cfg, ok := redisConfig.(RedisConfig)
	if !ok {
		return nil, fmt.Errorf("config should be an instance of backends.RedisConfig")
	}

	var (
		r = rediSQL{
			resTableSchemas: make(map[string]string),
			schemaMutex:     sync.RWMutex{},
		}
		err error
	)
	r.pool, err = r.connect(cfg)
	if err != nil {
		return nil, err
	}

	// Is the rediSQL module available?
	c := r.pool.Get()
	defer c.Close()

	c.Do("DEL", rediSQLTestDB)
	if err := rediSQLquery(c, "REDISQL.CREATE_DB", rediSQLTestDB); err != nil {
		return nil, fmt.Errorf("is the rediSQL module loaded?: %v", err)
	}
	c.Do("DEL", rediSQLTestDB)

	// Config.
	if cfg.ResultsTable != "" {
		r.resultsTable = cfg.ResultsTable
	} else {
		r.resultsTable = "results"
	}

	if cfg.ResultsTTL > 0 {
		r.resultsTTL = cfg.ResultsTTL
	} else {
		r.resultsTTL = 3600
	}

	return &r, nil
}

// NewResultSet returns a new instance of a rediSQL result writer.
// A new instance should be acquired for every individual job result
// to be written to the backend and then thrown away.
func (r *rediSQL) NewResultSet(resultName, taskName string, ttl time.Duration) ResultSet {
	var resTTL time.Duration
	if ttl.Seconds() > 0 {
		resTTL = ttl
	} else {
		resTTL = r.resultsTTL
	}

	return &rediSQLWriter{
		dbName:   resultName,
		taskName: taskName,
		c:        r.pool.Get(),
		ttl:      resTTL,
		backend:  r,
	}
}

// connect creates and returns a Redis connection pool.
func (r *rediSQL) connect(cfg RedisConfig) (*redis.Pool, error) {
	pool := &redis.Pool{
		Wait:      true,
		MaxActive: cfg.MaxActiveConns,
		MaxIdle:   cfg.MaxActiveConns,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial(
				"tcp",
				cfg.Address,
				redis.DialPassword(cfg.Password),
				redis.DialConnectTimeout(time.Second*cfg.ConnectTimeout),
				redis.DialReadTimeout(time.Second*cfg.ReadTimeout),
				redis.DialWriteTimeout(time.Second*cfg.WriteTimeout),
			)

			if err != nil {
				return nil, err
			} else if cfg.DB != 0 {
				if _, err := c.Do("SELECT", cfg.DB); err != nil {
					return nil, err
				}
			}

			return c, err
		},
	}

	// Do a preliminary check.
	c := pool.Get()
	defer c.Close()

	if _, err := c.Do("PING"); err != nil {
		return nil, fmt.Errorf("error connecting to Redis: %v", err)
	}

	return pool, nil
}

// RegisterColTypes registers the column types of a particular taskName's result set.
// Internally, it translates sql types into the simpler rediSQL (SQLite 3) types,
// creates a CREATE TABLE() schema for the results table with the structure of the
// particular taskName, and caches it be used for every subsequent result db creation
// and population. This should only be called once for each kind of taskName.
func (w *rediSQLWriter) RegisterColTypes(cols []string, colTypes []*sql.ColumnType) error {
	if w.IsColTypesRegistered() {
		return errors.New("column types are already registered")
	}

	w.backend.schemaMutex.Lock()
	w.backend.resTableSchemas[w.taskName] = fmt.Sprintf(createTableSchema(cols, colTypes),
		w.backend.resultsTable, w.backend.resultsTable)
	w.backend.schemaMutex.Unlock()

	return nil
}

// IsColTypesRegistered checks whether the column types for a particular taskName's
// structure is registered in the backend.
func (w *rediSQLWriter) IsColTypesRegistered() bool {
	w.backend.schemaMutex.RLock()
	_, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	return ok
}

// WriteCols writes the column (headers) of a result set to the backend.
// Internally, it creates a rediSQL database and creates a results table
// based on the schema RegisterColTypes() would've created and cached.
// This should only be called once on a ResultWriter instance.
func (w *rediSQLWriter) WriteCols(cols []string) error {
	if w.colsWritten {
		return errors.New("columns are already written")
	}

	w.backend.schemaMutex.RLock()
	rSchema, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	if !ok {
		return errors.New("column types for this taskName have not been registered")
	}

	return w.setupResultsDB(cols, rSchema)
}

// WriteRow writes an individual row from a result set to the backend.
// Internally, it INSERT()s the given row into the rediSQL results table.
func (w *rediSQLWriter) WriteRow(row [][]byte) error {
	// cmd := [][]byte{[]byte(w.dbName), []byte(rediSQLInsertStmt)}
	// return w.c.Send("REDISQL.EXEC_STATEMENT", append(cmd, row...)...)
	return nil
}

// Flush flushes the rows written into the rediSQL pipe.
func (w *rediSQLWriter) Flush() error {
	if err := w.c.Flush(); err != nil {
		return fmt.Errorf("error flushing results to Redis result backend: %v", err)
	}

	return nil
}

// Close closes the active rediSQL connection.
func (w *rediSQLWriter) Close() error {
	return w.c.Close()
}

// setupResultsDB creates a rediSQL DB, a results table in it, and registers
// an INSERT() statement to be used for every row insert.
func (w *rediSQLWriter) setupResultsDB(cols []string, resSchema string) error {
	// Delete the existing db if there's already one.
	w.c.Do("DEL", w.dbName)

	// Create a fresh database for this job.
	if err := rediSQLquery(w.c, "REDISQL.CREATE_DB", w.dbName, ""); err != nil {
		return fmt.Errorf("REDISQL.CREATE_DB failed: %v", err)
	}

	w.c.Do("EXPIRE", w.dbName, w.backend.resultsTTL)

	// Create the results table.
	if err := rediSQLquery(w.c, "REDISQL.EXEC", w.dbName, resSchema); err != nil {
		return fmt.Errorf("REDISQL.EXEC failed: %v", err)
	}

	// Register the positional INSERT() statement in rediSQL.
	ins := make([]string, len(cols))
	for i := 0; i < len(cols); i++ {
		ins[i] = fmt.Sprintf("?%d", i+1)
	}

	return rediSQLquery(w.c, "REDISQL.CREATE_STATEMENT", w.dbName, rediSQLInsertStmt,
		"INSERT INTO "+w.backend.resultsTable+" VALUES("+strings.Join(ins, ",")+")")
}

// rediSQLquery executes a REDISQL.* command against a given database name.
// This should only be used for running non SELECT() commands as any byte
// response received from the mdoule is treated as an error.
// This is because rediSQL errors don't throw a Redis error but simply
// send the error message as the success reply.
func rediSQLquery(c redis.Conn, cmd, dbName string, args ...interface{}) error {
	a := append([]interface{}{dbName}, args...)
	v, err := c.Do(cmd, a...)
	if err != nil {
		return fmt.Errorf("error executing rediSQL query: %v: %v", err, args)
	}

	// If there's a byte array, that's an error message from rediSQL.
	msg, ok := v.([]byte)
	if ok {
		return fmt.Errorf("error executing rediSQL query: %s | Query = %v", msg, args)
	}

	return nil
}
