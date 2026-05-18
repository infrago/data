package data

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"sync"
	"testing"

	. "github.com/infrago/base"
)

type insertManyChunkDriver struct{}

type insertManyChunkConn struct {
	state *insertManyChunkState
}

type insertManyChunkTx struct {
	state *insertManyChunkState
}

type insertManyChunkState struct {
	mu      sync.Mutex
	execs   []int
	begins  int
	commits int
}

type insertManyChunkConnection struct {
	db      *sql.DB
	dialect insertManyChunkDialect
}

type insertManyChunkDialect struct {
	maxParams int
}

type insertManyChunkResult int64

var (
	registerInsertManyChunkDriver sync.Once
	insertManyChunkStates         sync.Map
)

func (d *insertManyChunkDriver) Open(name string) (driver.Conn, error) {
	stateAny, _ := insertManyChunkStates.LoadOrStore(name, &insertManyChunkState{})
	return &insertManyChunkConn{state: stateAny.(*insertManyChunkState)}, nil
}

func (c *insertManyChunkConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *insertManyChunkConn) Close() error                        { return nil }
func (c *insertManyChunkConn) Begin() (driver.Tx, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.begins++
	return &insertManyChunkTx{state: c.state}, nil
}
func (c *insertManyChunkConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c.Begin()
}
func (c *insertManyChunkConn) ExecContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	c.state.mu.Lock()
	defer c.state.mu.Unlock()
	c.state.execs = append(c.state.execs, len(args))
	return insertManyChunkResult(1), nil
}

func (tx *insertManyChunkTx) Commit() error {
	tx.state.mu.Lock()
	defer tx.state.mu.Unlock()
	tx.state.commits++
	return nil
}
func (tx *insertManyChunkTx) Rollback() error { return nil }

func (insertManyChunkResult) LastInsertId() (int64, error) { return 0, nil }
func (r insertManyChunkResult) RowsAffected() (int64, error) {
	return int64(r), nil
}

func (c *insertManyChunkConnection) Open() error    { return nil }
func (c *insertManyChunkConnection) Close() error   { return c.db.Close() }
func (c *insertManyChunkConnection) Health() Health { return Health{} }
func (c *insertManyChunkConnection) DB() *sql.DB    { return c.db }
func (c *insertManyChunkConnection) Dialect() Dialect {
	return c.dialect
}

func (insertManyChunkDialect) Name() string            { return "mysql" }
func (insertManyChunkDialect) Quote(s string) string   { return "`" + s + "`" }
func (insertManyChunkDialect) Placeholder(int) string  { return "?" }
func (insertManyChunkDialect) SupportsILike() bool     { return false }
func (insertManyChunkDialect) SupportsReturning() bool { return false }
func (d insertManyChunkDialect) MaxParams() int        { return d.maxParams }

func openInsertManyChunkDB(t *testing.T) (*sql.DB, *insertManyChunkState) {
	t.Helper()
	registerInsertManyChunkDriver.Do(func() {
		sql.Register("insert-many-chunk-test", &insertManyChunkDriver{})
	})
	name := "insert-many-chunk-" + t.Name()
	state := &insertManyChunkState{}
	insertManyChunkStates.Store(name, state)
	db, err := sql.Open("insert-many-chunk-test", name)
	if err != nil {
		t.Fatalf("open insert many chunk test db failed: %v", err)
	}
	return db, state
}

func TestInsertManyChunksByDialectMaxParams(t *testing.T) {
	db, state := openInsertManyChunkDB(t)
	defer db.Close()

	table := &sqlTable{
		sqlView: sqlView{
			base: &sqlBase{
				inst: &Instance{Name: t.Name(), Config: Config{}},
				conn: &insertManyChunkConnection{
					db:      db,
					dialect: insertManyChunkDialect{maxParams: 3},
				},
			},
			name:   "users",
			source: "users",
			key:    "id",
		},
	}

	items := make([]Map, 0, 5)
	for i := 0; i < 5; i++ {
		items = append(items, Map{"name": fmt.Sprintf("u%d", i), "age": i})
	}
	out := table.InsertMany(items)
	if table.base.Error() != nil {
		t.Fatalf("insert many failed: %v", table.base.Error())
	}
	if len(out) != len(items) {
		t.Fatalf("expected %d rows, got %d", len(items), len(out))
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.begins != 1 || state.commits != 1 {
		t.Fatalf("expected one transaction for chunked insert, got begins=%d commits=%d", state.begins, state.commits)
	}
	if len(state.execs) != 5 {
		t.Fatalf("expected 5 chunks, got %d execs: %#v", len(state.execs), state.execs)
	}
	for i, args := range state.execs {
		if args != 2 {
			t.Fatalf("chunk %d expected 2 args, got %d", i, args)
		}
	}
}

func TestMaxInsertBatchRowsUsesDialectLimit(t *testing.T) {
	if got := maxInsertBatchRows(insertManyChunkDialect{maxParams: 10}, 3); got != 3 {
		t.Fatalf("expected 3 rows, got %d", got)
	}
	if got := maxInsertBatchRows(insertManyChunkDialect{maxParams: 2}, 3); got != 1 {
		t.Fatalf("expected at least one row, got %d", got)
	}
}
