package backends

// This package is a little weird. rqlite takes SQL queries
// as JSON over HTTP, so there's a lot of manual escaping
// and literal byte-writing that goes into composing an
// insert payload.

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/zerodhatech/sql-jobber/backends/escape"
	"github.com/zerodhatech/sql-jobber/backends/ttl"
)

const uriExecute = "/db/execute"

// rqlite represents an rqlite backend.
type rqlite struct {
	address    string
	httpClient *http.Client
	resultsTTL time.Duration

	// The result schemas (CREATE TABLE ...) are dynamically
	// generated everytime queries are executed based on their result columns.
	// They're cached here so as to avoid repetetive generation.
	resTableSchemas map[string]insertSchema
	schemaMutex     sync.RWMutex

	ttlMap  *ttl.TTL
	escaper *escape.Escaper
}

// rqliteWriter represents a writer that saves results
// to a rqlite backend.
type rqliteWriter struct {
	tblName     string
	taskName    string
	colsWritten bool
	cols        []string
	rows        [][]byte
	queryBuf    bytes.Buffer

	backend *rqlite
}

// insertSchema contains the generated SQL for creating tables
// and inserting rows.
type insertSchema struct {
	createTable   string
	insertRow     string
	insertRowCols []byte
}

// RqliteConfig represents an Rqlite connection configuration.
type RqliteConfig struct {
	Address      string
	MaxIdleConns int
	Timeout      time.Duration
	ResultsTTL   time.Duration
}

// rqliteErr represents an rqlite HTTP error.
type rqliteErr struct {
	Results []struct {
		Error string `json:"error"`
	} `json:"results"`
}

// NewRqlite creates and returns a new Rqlite results backend instance.
func NewRqlite(cfg RqliteConfig) (ResultBackend, error) {
	tt := ttl.New(time.Second * 5)
	go tt.Run()

	return &rqlite{
		address:         cfg.Address,
		resultsTTL:      cfg.ResultsTTL,
		resTableSchemas: make(map[string]insertSchema),
		schemaMutex:     sync.RWMutex{},
		escaper:         escape.New(),
		ttlMap:          tt,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   cfg.MaxIdleConns,
				ResponseHeaderTimeout: cfg.Timeout,
			},
		}}, nil
}

// NewResultSet returns a new instance of a rqlite result writer.
// A new instance should be acquired for every individual job result
// to be written to the backend and then thrown away.
func (r *rqlite) NewResultSet(dbName, taskName string) ResultSet {
	return &rqliteWriter{
		tblName:  dbName,
		taskName: taskName,
		backend:  r,
		queryBuf: bytes.Buffer{},
	}
}

// RegisterColTypes registers the column types of a particular taskName's result set.
// Internally, it translates sql types into the simpler rqlite (SQLite 3) types,
// creates a CREATE TABLE() schema for the results table with the structure of the
// particular taskName, and caches it be used for every subsequent result db creation
// and population. This should only be called once for each kind of taskName.
func (w *rqliteWriter) RegisterColTypes(cols []string, colTypes []*sql.ColumnType) error {
	if w.IsColTypesRegistered() {
		return errors.New("column types are already registered")
	}

	w.cols = make([]string, len(cols))
	copy(w.cols, cols)

	// Create the insert statement.
	// INSERT INTO xxx (col1, col2...) VALUES.
	var (
		colNameHolder = make([]string, len(cols))
		colValHolder  = make([]string, len(cols))
	)
	for i := range w.cols {
		colNameHolder[i] = "`" + w.cols[i] + "`"

		// This will be filled at the time of insertion by
		// the sql eval/escape library.
		colValHolder[i] = "%s"
	}
	var (
		ins     = "INSERT INTO `%s` (" + strings.Join(colNameHolder, ",") + ") VALUES "
		insCols = "(" + strings.Join(colValHolder, ",") + ")"
	)

	w.backend.schemaMutex.Lock()
	w.backend.resTableSchemas[w.taskName] = insertSchema{
		createTable:   createTableSchema(cols, colTypes),
		insertRow:     ins,
		insertRowCols: []byte(insCols),
	}
	w.backend.schemaMutex.Unlock()

	return nil
}

// IsColTypesRegistered checks whether the column types for a particular taskName's
// structure is registered in the backend.
func (w *rqliteWriter) IsColTypesRegistered() bool {
	w.backend.schemaMutex.RLock()
	_, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	return ok
}

// WriteCols writes the column (headers) of a result set to the backend.
// Internally, it creates a new results table based on the schema
// RegisterColTypes() would've created and cached. This should only be
// called once on a ResultWriter instance.
func (w *rqliteWriter) WriteCols(cols []string) error {
	if w.colsWritten {
		return errors.New("columns are already written")
	}

	w.backend.schemaMutex.RLock()
	rSchema, ok := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	if !ok {
		return errors.New("column types for this taskName have not been registered")
	}

	// Wrap in an array (for rqlite).
	buf := bytes.Buffer{}
	buf.Write([]byte("[\""))
	buf.Write([]byte(fmt.Sprintf(rSchema.createTable, w.tblName, w.tblName)))
	buf.Write([]byte("\"]"))

	return w.backend.execute(uriExecute, http.MethodPost, buf.Bytes(), nil)
}

// WriteRow writes an individual row from a result set to the backend.
// Internally, it INSERT()s the given row into the rqlite results table.
func (w *rqliteWriter) WriteRow(row [][]byte) error {
	w.backend.schemaMutex.RLock()
	rSchema, _ := w.backend.resTableSchemas[w.taskName]
	w.backend.schemaMutex.RUnlock()

	// If it's the first row, write the INSERT block.
	if w.queryBuf.Len() == 0 {
		// Open list for the rqlite JSON payload.
		w.queryBuf.Write([]byte("[\""))
		w.queryBuf.Write([]byte(fmt.Sprintf(rSchema.insertRow, w.tblName)))
	}

	// Prepare insert-value blocks: ('a', 'b' ...)
	o, err := w.backend.escaper.Escape(rSchema.insertRowCols, row...)
	if err != nil {
		return err
	}
	w.queryBuf.Write(o)
	w.queryBuf.Write([]byte(","))

	return nil
}

// Flush takes the SQL write buffer and flushes it to the rqlite server.
func (w *rqliteWriter) Flush() error {
	if w.queryBuf.Len() == 0 {
		return nil
	}

	out := w.queryBuf.Bytes()
	out = out[:len(out)-1] // Ignore that last trailing comma from WriteRow.
	w.queryBuf.Reset()
	out = append(out, []byte("\"]")...)

	err := w.backend.execute(uriExecute, http.MethodPost, out, nil)
	if err != nil {
		return err
	}

	// Results were saved. Apply the TTL.
	w.backend.ttlMap.Add(w.backend.resultsTTL, func(tblName string) func() {
		return func() {
			out := bytes.Buffer{}
			out.Write([]byte(fmt.Sprintf(`["DROP TABLE %s"]`, tblName)))
			w.backend.execute(uriExecute, http.MethodPost, out.Bytes(), nil)
		}
	}(w.tblName))

	return nil
}

// Close here does nothing really.
func (w *rqliteWriter) Close() error {
	return nil
}

// execute executes an rqlite HTTP query.
func (r *rqlite) execute(uri string, method string, payload []byte, respObj interface{}) error {
	req, err := http.NewRequest(method, r.address+uri, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	resp, err := r.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	// rqlite doesn't return non 200 statuses for errors, unfortunately.
	var rErr rqliteErr
	if err := json.Unmarshal(body, &rErr); err != nil {
		return err
	}
	if len(rErr.Results) == 1 && rErr.Results[0].Error != "" {
		return errors.New(rErr.Results[0].Error)
	}

	return json.Unmarshal(body, &respObj)
}
