package data

import (
	"database/sql"
	"strconv"
	"testing"

	. "github.com/infrago/base"
)

type testDialect struct {
	name string
}

func (d testDialect) Name() string { return d.name }
func (d testDialect) Quote(s string) string {
	switch d.name {
	case "mysql":
		return "`" + s + "`"
	default:
		return `"` + s + `"`
	}
}
func (d testDialect) Placeholder(n int) string {
	switch d.name {
	case "pgsql", "postgres":
		return "$" + strconv.Itoa(n)
	default:
		return "?"
	}
}
func (d testDialect) SupportsILike() bool     { return d.name == "pgsql" || d.name == "postgres" }
func (d testDialect) SupportsReturning() bool { return d.name == "pgsql" || d.name == "postgres" }

type testConn struct {
	d Dialect
}

func (c testConn) Open() error      { return nil }
func (c testConn) Close() error     { return nil }
func (c testConn) Health() Health   { return Health{} }
func (c testConn) DB() *sql.DB      { return nil }
func (c testConn) Dialect() Dialect { return c.d }

func TestJSONPathCompileExprMatrix(t *testing.T) {
	cases := []struct {
		name     string
		expected string
	}{
		{"pgsql", `("metadata"->>'name') = $1`},
		{"mysql", "JSON_UNQUOTE(JSON_EXTRACT(`metadata`, '$.name')) = ?"},
		{"sqlite", `json_extract("metadata", '$.name') = ?`},
	}

	for _, tt := range cases {
		b := NewSQLBuilder(testDialect{name: tt.name})
		b.isJSONField = func(field string) bool { return field == "metadata" }
		sqlText, err := b.CompileExpr(CmpExpr{Field: "metadata.name", Op: OpEq, Value: "alice"})
		if err != nil {
			t.Fatalf("%s compile failed: %v", tt.name, err)
		}
		if sqlText != tt.expected {
			t.Fatalf("%s sql mismatch: got=%s want=%s", tt.name, sqlText, tt.expected)
		}
	}
}

func TestJSONPathOrderByMatrix(t *testing.T) {
	cases := []struct {
		name     string
		expected string
	}{
		{"pgsql", ` ORDER BY ("metadata"->>'name') DESC`},
		{"mysql", " ORDER BY JSON_UNQUOTE(JSON_EXTRACT(`metadata`, '$.name')) DESC"},
		{"sqlite", ` ORDER BY json_extract("metadata", '$.name') DESC`},
	}

	for _, tt := range cases {
		v := &sqlView{
			base: &sqlBase{
				inst: &Instance{Name: "test"},
				conn: testConn{d: testDialect{name: tt.name}},
			},
			name:   "user",
			source: "user",
			fields: Vars{
				"metadata": Var{Type: "json"},
			},
		}
		order, err := v.buildOrderBy(Query{
			Sort: []Sort{{Field: "metadata.name", Desc: true}},
		})
		if err != nil {
			t.Fatalf("%s order compile failed: %v", tt.name, err)
		}
		if order != tt.expected {
			t.Fatalf("%s order mismatch: got=%s want=%s", tt.name, order, tt.expected)
		}
	}
}

func TestJSONPathAmbiguousField(t *testing.T) {
	b := NewSQLBuilder(testDialect{name: "pgsql"})
	_, err := b.CompileExpr(CmpExpr{Field: "metadata.name", Op: OpEq, Value: "alice"})
	if err == nil {
		t.Fatalf("expected ambiguous field error")
	}
}

func TestJSONPathParseQueryInput(t *testing.T) {
	q, err := ParseQuery(Map{"metadata.name": "alice"})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	expr, ok := q.Filter.(CmpExpr)
	if !ok {
		t.Fatalf("expected cmp expr, got %T", q.Filter)
	}
	if expr.Field != "metadata.name" || expr.Op != OpEq {
		t.Fatalf("unexpected expr: %#v", expr)
	}
}
