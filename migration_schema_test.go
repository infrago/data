package data

import (
	"database/sql"
	"strings"
	"testing"

	. "github.com/infrago/base"
)

type migrationTestDialect struct{}

func (migrationTestDialect) Name() string              { return "sqlite" }
func (migrationTestDialect) Quote(value string) string { return `"` + value + `"` }
func (migrationTestDialect) Placeholder(int) string    { return "?" }
func (migrationTestDialect) SupportsILike() bool       { return false }
func (migrationTestDialect) SupportsReturning() bool   { return true }

func TestBuildCreateTableDoesNotTurnValidatorNamesIntoSQLChecks(t *testing.T) {
	base := &sqlBase{conn: migrationTestConnection{}}
	query, err := base.buildCreateTableSQL("", "accounts", "id", Vars{
		"email":    Var{Type: "string", Check: "email"},
		"password": Var{Type: "string", Check: "password"},
		"score":    Var{Type: "integer", Check: "score >= 0"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(query, "CHECK (email)") || strings.Contains(query, "CHECK (password)") {
		t.Fatalf("validator name leaked into schema: %s", query)
	}
	if !strings.Contains(query, "CHECK (score >= 0)") {
		t.Fatalf("SQL check expression was removed: %s", query)
	}
}

type migrationTestConnection struct{}

func (migrationTestConnection) Open() error      { return nil }
func (migrationTestConnection) Close() error     { return nil }
func (migrationTestConnection) Health() Health   { return Health{} }
func (migrationTestConnection) DB() *sql.DB      { return nil }
func (migrationTestConnection) Dialect() Dialect { return migrationTestDialect{} }
