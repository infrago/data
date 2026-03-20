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

func TestParseQueryNestedFilterMap(t *testing.T) {
	q, err := ParseQuery(Map{
		OptFilter: Map{
			"status": nil,
			"name":   "alice",
		},
		OptUnscoped: true,
	})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !q.Unscoped {
		t.Fatalf("expected unscoped=true")
	}
	and, ok := q.Filter.(AndExpr)
	if !ok {
		t.Fatalf("expected AndExpr, got %#v", q.Filter)
	}
	if len(and.Items) != 2 {
		t.Fatalf("expected 2 filter items, got %#v", and.Items)
	}
	foundNull := false
	foundName := false
	for _, item := range and.Items {
		switch vv := item.(type) {
		case NullExpr:
			if vv.Field == "status" && vv.Yes {
				foundNull = true
			}
		case CmpExpr:
			if vv.Field == "name" && vv.Op == OpEq && vv.Value == "alice" {
				foundName = true
			}
		}
	}
	if !foundNull || !foundName {
		t.Fatalf("unexpected filter exprs: %#v", and.Items)
	}
}

func TestParseQueryNestedFilterMergesOptions(t *testing.T) {
	q, err := ParseQuery(Map{
		OptFilter: Map{
			"status":    nil,
			OptUnsafe:   true,
			OptUnscoped: true,
		},
	})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	if !q.Unsafe {
		t.Fatalf("expected unsafe=true from nested filter")
	}
	if !q.Unscoped {
		t.Fatalf("expected unscoped=true from nested filter")
	}
	if _, ok := q.Filter.(NullExpr); !ok {
		t.Fatalf("expected null expr filter, got %#v", q.Filter)
	}
}

func TestParseQueryNestedFiltersList(t *testing.T) {
	q, err := ParseQuery(Map{
		OptFilters: []Map{
			{"status": nil},
			{"state": "enable"},
		},
	})
	if err != nil {
		t.Fatalf("parse failed: %v", err)
	}
	and, ok := q.Filter.(AndExpr)
	if !ok {
		t.Fatalf("expected AndExpr, got %#v", q.Filter)
	}
	if len(and.Items) != 2 {
		t.Fatalf("expected 2 filter items, got %#v", and.Items)
	}
}
