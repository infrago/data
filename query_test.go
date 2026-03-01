package data

import (
	"testing"

	. "github.com/infrago/base"
)

func TestParseQueryUnsafe(t *testing.T) {
	q, err := ParseQuery(Map{
		OptUnsafe: true,
		"name":    "alice",
	})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !q.Unsafe {
		t.Fatalf("expected unsafe=true")
	}
}

func TestParseFieldRef(t *testing.T) {
	q, err := ParseQuery(Map{
		"id": Map{OpEq: "$field:user.id"},
	})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	var cmp CmpExpr
	switch vv := q.Filter.(type) {
	case CmpExpr:
		cmp = vv
	case AndExpr:
		if len(vv.Items) == 0 {
			t.Fatalf("unexpected empty and expr")
		}
		one, ok := vv.Items[0].(CmpExpr)
		if !ok {
			t.Fatalf("unexpected cmp expr: %#v", vv.Items[0])
		}
		cmp = one
	default:
		t.Fatalf("unexpected filter: %#v", q.Filter)
	}
	if _, ok := cmp.Value.(FieldRef); !ok {
		t.Fatalf("expected FieldRef, got %T", cmp.Value)
	}
}
