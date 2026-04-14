package data

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
)

type sequenceTestDriver struct{}

type sequenceTestConn struct {
	state *sequenceTestState
}

type sequenceTestTx struct{}

type sequenceTestState struct {
	mu     sync.Mutex
	values map[string]int64
}

type sequenceTestConnection struct {
	db *sql.DB
}

type sequenceTestDialect struct{}

type sequenceTestResult int64

type sequenceTestRows struct {
	cols []string
	rows [][]driver.Value
	idx  int
}

var (
	registerSequenceTestDriver sync.Once
	sequenceTestStates         sync.Map
)

func (d *sequenceTestDriver) Open(name string) (driver.Conn, error) {
	stateAny, _ := sequenceTestStates.LoadOrStore(name, &sequenceTestState{
		values: map[string]int64{},
	})
	return &sequenceTestConn{state: stateAny.(*sequenceTestState)}, nil
}

func (c *sequenceTestConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *sequenceTestConn) Close() error                        { return nil }
func (c *sequenceTestConn) Begin() (driver.Tx, error)           { return &sequenceTestTx{}, nil }
func (c *sequenceTestConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &sequenceTestTx{}, nil
}

func (tx *sequenceTestTx) Commit() error   { return nil }
func (tx *sequenceTestTx) Rollback() error { return nil }

func (c *sequenceTestConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	sqlText := strings.ToLower(strings.TrimSpace(query))
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	switch {
	case strings.HasPrefix(sqlText, "create table if not exists"):
		return sequenceTestResult(0), nil
	case strings.HasPrefix(sqlText, "insert into"):
		if len(args) < 3 {
			return nil, fmt.Errorf("invalid insert args")
		}
		key, _ := args[0].Value.(string)
		if _, ok := c.state.values[key]; ok {
			return nil, fmt.Errorf("duplicate key")
		}
		value, _ := args[1].Value.(int64)
		c.state.values[key] = value
		return sequenceTestResult(1), nil
	case strings.HasPrefix(sqlText, "update"):
		if len(args) < 3 {
			return nil, fmt.Errorf("invalid update args")
		}
		value, _ := args[0].Value.(int64)
		key, _ := args[2].Value.(string)
		c.state.values[key] = value
		return sequenceTestResult(1), nil
	default:
		return nil, fmt.Errorf("unsupported exec query: %s", query)
	}
}

func (c *sequenceTestConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	sqlText := strings.ToLower(strings.TrimSpace(query))
	c.state.mu.Lock()
	defer c.state.mu.Unlock()

	switch {
	case strings.HasPrefix(sqlText, "select"):
		key, _ := args[0].Value.(string)
		value, ok := c.state.values[key]
		if !ok {
			return &sequenceTestRows{cols: []string{"value"}}, nil
		}
		return &sequenceTestRows{
			cols: []string{"value"},
			rows: [][]driver.Value{{value}},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported query: %s", query)
	}
}

func (sequenceTestResult) LastInsertId() (int64, error) { return 0, nil }
func (r sequenceTestResult) RowsAffected() (int64, error) {
	return int64(r), nil
}

func (r *sequenceTestRows) Columns() []string { return r.cols }
func (r *sequenceTestRows) Close() error      { return nil }
func (r *sequenceTestRows) Next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		return io.EOF
	}
	copy(dest, r.rows[r.idx])
	r.idx++
	return nil
}

func (c *sequenceTestConnection) Open() error    { return nil }
func (c *sequenceTestConnection) Close() error   { return c.db.Close() }
func (c *sequenceTestConnection) Health() Health { return Health{} }
func (c *sequenceTestConnection) DB() *sql.DB    { return c.db }
func (c *sequenceTestConnection) Dialect() Dialect {
	return sequenceTestDialect{}
}

func (sequenceTestDialect) Name() string            { return "mysql" }
func (sequenceTestDialect) Quote(s string) string   { return "`" + strings.TrimSpace(s) + "`" }
func (sequenceTestDialect) Placeholder(int) string  { return "?" }
func (sequenceTestDialect) SupportsILike() bool     { return false }
func (sequenceTestDialect) SupportsReturning() bool { return false }

func openSequenceTestDB(t *testing.T) *sql.DB {
	t.Helper()
	registerSequenceTestDriver.Do(func() {
		sql.Register("sequence-test", &sequenceTestDriver{})
	})
	name := "sequence-test-" + strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	sequenceTestStates.Store(name, &sequenceTestState{values: map[string]int64{}})
	db, err := sql.Open("sequence-test", name)
	if err != nil {
		t.Fatalf("open sequence test db failed: %v", err)
	}
	return db
}

func TestSequenceReturnsOffsetThenStep(t *testing.T) {
	db := openSequenceTestDB(t)
	defer db.Close()

	base := &sqlBase{
		inst: &Instance{Name: t.Name(), Config: Config{}},
		conn: &sequenceTestConnection{db: db},
	}

	value, err := base.Sequence("abcd", 100, 5)
	if err != nil {
		t.Fatalf("sequence first failed: %v", err)
	}
	if value != 100 {
		t.Fatalf("expected first sequence value 100, got %d", value)
	}

	value, err = base.Sequence("abcd", 999, 5)
	if err != nil {
		t.Fatalf("sequence second failed: %v", err)
	}
	if value != 105 {
		t.Fatalf("expected second sequence value 105, got %d", value)
	}

	value, err = base.Sequence("abcd", 999, 2)
	if err != nil {
		t.Fatalf("sequence third failed: %v", err)
	}
	if value != 107 {
		t.Fatalf("expected third sequence value 107, got %d", value)
	}
}

func TestSequenceDefaultsStepToOne(t *testing.T) {
	db := openSequenceTestDB(t)
	defer db.Close()

	base := &sqlBase{
		inst: &Instance{Name: t.Name(), Config: Config{}},
		conn: &sequenceTestConnection{db: db},
	}

	value, err := base.Sequence("xyz", 0, 0)
	if err != nil {
		t.Fatalf("sequence default step failed: %v", err)
	}
	if value != 0 {
		t.Fatalf("expected initial value 0, got %d", value)
	}

	value, err = base.Sequence("xyz", 0, 0)
	if err != nil {
		t.Fatalf("sequence increment failed: %v", err)
	}
	if value != 1 {
		t.Fatalf("expected incremented value 1, got %d", value)
	}
}

func TestSequenceManyAllocatesContinuousRange(t *testing.T) {
	db := openSequenceTestDB(t)
	defer db.Close()

	base := &sqlBase{
		inst: &Instance{Name: t.Name(), Config: Config{}},
		conn: &sequenceTestConnection{db: db},
	}

	items, err := base.SequenceMany("batch", 3, 10, 2)
	if err != nil {
		t.Fatalf("sequence many failed: %v", err)
	}
	expect := []int64{10, 12, 14}
	if fmt.Sprint(items) != fmt.Sprint(expect) {
		t.Fatalf("expected %v, got %v", expect, items)
	}

	items, err = base.SequenceMany("batch", 2, 999, 2)
	if err != nil {
		t.Fatalf("sequence many second failed: %v", err)
	}
	expect = []int64{16, 18}
	if fmt.Sprint(items) != fmt.Sprint(expect) {
		t.Fatalf("expected %v, got %v", expect, items)
	}
}
