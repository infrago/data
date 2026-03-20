package data

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"
	"testing"
)

type txContextTestDriver struct{}

type txContextTestConn struct{}

type txContextTestTx struct{}

type txContextTestConnection struct {
	db *sql.DB
}

type txContextTestDialect struct{}

func (d *txContextTestDriver) Open(string) (driver.Conn, error) {
	return &txContextTestConn{}, nil
}

func (c *txContextTestConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *txContextTestConn) Close() error                        { return nil }
func (c *txContextTestConn) Begin() (driver.Tx, error)           { return &txContextTestTx{}, nil }

func (c *txContextTestConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return &txContextTestTx{}, nil
}

func (tx *txContextTestTx) Commit() error   { return nil }
func (tx *txContextTestTx) Rollback() error { return nil }

func (c *txContextTestConnection) Open() error    { return nil }
func (c *txContextTestConnection) Close() error   { return c.db.Close() }
func (c *txContextTestConnection) Health() Health { return Health{} }
func (c *txContextTestConnection) DB() *sql.DB    { return c.db }
func (c *txContextTestConnection) Dialect() Dialect {
	return txContextTestDialect{}
}

func (txContextTestDialect) Name() string            { return "test" }
func (txContextTestDialect) Quote(s string) string   { return s }
func (txContextTestDialect) Placeholder(int) string  { return "?" }
func (txContextTestDialect) SupportsILike() bool     { return false }
func (txContextTestDialect) SupportsReturning() bool { return false }

var registerTxContextTestDriver sync.Once

func openTxContextTestDB(t *testing.T) *sql.DB {
	t.Helper()
	registerTxContextTestDriver.Do(func() {
		sql.Register("tx-context-test", &txContextTestDriver{})
	})
	db, err := sql.Open("tx-context-test", "")
	if err != nil {
		t.Fatalf("open test db failed: %v", err)
	}
	return db
}

func TestBeginCommitKeepsTransactionContextAlive(t *testing.T) {
	db := openTxContextTestDB(t)
	defer db.Close()

	base := &sqlBase{
		inst: &Instance{Name: "tx-context"},
		conn: &txContextTestConnection{db: db},
	}

	if err := base.Begin(); err != nil {
		t.Fatalf("begin failed: %v", err)
	}
	if base.tx == nil {
		t.Fatalf("expected tx to be created")
	}
	if base.txDone == nil {
		t.Fatalf("expected tx cancel func to be retained until commit")
	}

	if err := base.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	if base.tx != nil {
		t.Fatalf("expected tx to be cleared after commit")
	}
	if base.txDone != nil {
		t.Fatalf("expected tx cancel func to be cleared after commit")
	}
}
