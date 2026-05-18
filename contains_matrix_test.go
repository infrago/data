package data

import (
	"testing"

	. "github.com/infrago/base"
)

func TestPgArrayContainsAndElemMatchMatrix(t *testing.T) {
	b := NewSQLBuilder(testDialect{name: "pgsql"})
	b.isArrayField = func(field string) bool { return field == "tags" }
	b.isJSONField = func(field string) bool { return field == "metadata" }
	b.bindField = func(field string, value Any) Any {
		if field == "tags" {
			return bindArrayValue(testDialect{name: "pgsql"}, value)
		}
		return value
	}

	sqlText, err := b.CompileExpr(CmpExpr{Field: "tags", Op: OpContains, Value: []string{}})
	if err != nil {
		t.Fatalf("contains compile failed: %v", err)
	}
	if sqlText != `"tags" @> $1` {
		t.Fatalf("contains sql mismatch: got=%s", sqlText)
	}
	if len(b.Args()) != 1 || b.Args()[0] != "{}" {
		t.Fatalf("contains args mismatch: %#v", b.Args())
	}

	b = NewSQLBuilder(testDialect{name: "pgsql"})
	b.isArrayField = func(field string) bool { return field == "tags" }
	b.bindField = func(field string, value Any) Any {
		if field == "tags" {
			return bindArrayValue(testDialect{name: "pgsql"}, value)
		}
		return value
	}
	sqlText, err = b.CompileExpr(CmpExpr{Field: "tags", Op: OpElemMatch, Value: "go"})
	if err != nil {
		t.Fatalf("elemMatch compile failed: %v", err)
	}
	if sqlText != `"tags" @> $1` {
		t.Fatalf("elemMatch sql mismatch: got=%s", sqlText)
	}
	if len(b.Args()) != 1 || b.Args()[0] != "{go}" {
		t.Fatalf("elemMatch args mismatch: %#v", b.Args())
	}

	b = NewSQLBuilder(testDialect{name: "pgsql"})
	b.isJSONField = func(field string) bool { return field == "metadata" }
	sqlText, err = b.CompileExpr(CmpExpr{Field: "metadata", Op: OpContains, Value: Map{"name": "alice"}})
	if err != nil {
		t.Fatalf("json contains compile failed: %v", err)
	}
	if sqlText != `"metadata" @> $1` {
		t.Fatalf("json contains sql mismatch: got=%s", sqlText)
	}
	if len(b.Args()) != 1 || b.Args()[0] != `{"name":"alice"}` {
		t.Fatalf("json contains args mismatch: %#v", b.Args())
	}
}
