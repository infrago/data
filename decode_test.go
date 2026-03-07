package data

import (
	"testing"

	. "github.com/infrago/base"
)

func TestDecodeParsesJSONArrayStringForIntArrayField(t *testing.T) {
	view := &sqlView{
		base: &sqlBase{},
		name: "oplog",
		fields: Vars{
			"data_ids": {Type: "[int]"},
		},
	}

	item, err := view.decode(Map{
		"data_ids": "[1,2]",
	})
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	ids, ok := item["data_ids"].([]int64)
	if !ok {
		t.Fatalf("expected []int64, got %T (%#v)", item["data_ids"], item["data_ids"])
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("unexpected ids: %#v", ids)
	}
}

func TestDecodeKeepsStringFieldAsString(t *testing.T) {
	view := &sqlView{
		base: &sqlBase{},
		name: "demo",
		fields: Vars{
			"raw": {Type: "string"},
		},
	}

	item, err := view.decode(Map{
		"raw": "[1,2]",
	})
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if raw, ok := item["raw"].(string); !ok || raw != "[1,2]" {
		t.Fatalf("expected raw string unchanged, got %T (%#v)", item["raw"], item["raw"])
	}
}
