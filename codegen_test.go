package data

import (
	"strings"
	"testing"

	. "github.com/infrago/base"
)

func TestGenerateTableKeys(t *testing.T) {
	code := GenerateTableKeys("user", Table{
		Fields: Vars{
			"id":   {},
			"name": {},
		},
	})
	if !strings.Contains(code, "UserId") {
		t.Fatalf("expected UserId constant")
	}
	if !strings.Contains(code, "\"name\"") {
		t.Fatalf("expected field value in generated code")
	}
}
