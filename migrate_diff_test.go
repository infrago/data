package data

import (
	"strings"
	"testing"

	. "github.com/infrago/base"
)

func TestPlanColumnDiffActionsReportsTypeNullableAndDefault(t *testing.T) {
	base := &sqlBase{conn: &writeNormalizeTestConn{}}
	actions := base.planColumnDiffActions("", "users", "id", map[string]Var{
		"age": {Type: "int", Required: true, Default: int64(18)},
	}, map[string]columnInfo{
		"age": {Name: "age", Type: "TEXT", Nullable: true, HasNullable: true},
	}, MigrateOptions{Mode: "safe"})
	if len(actions) != 3 {
		t.Fatalf("expected three alter actions, got %#v", actions)
	}
	seen := map[string]bool{}
	for _, action := range actions {
		if action.Kind != "alter_column" {
			t.Fatalf("unexpected action: %#v", action)
		}
		switch {
		case strings.Contains(action.Detail, "type TEXT -> BIGINT"):
			seen["type"] = true
			if action.Apply {
				t.Fatalf("type diff should not apply outside danger mode: %#v", action)
			}
			if len(action.Diffs) != 1 || action.Diffs[0].Kind != "type" || action.Diffs[0].Field != "age" {
				t.Fatalf("expected structured type diff, got %#v", action.Diffs)
			}
		case strings.Contains(action.Detail, "nullable true -> false"):
			seen["nullable"] = true
			if !action.Apply || !strings.Contains(action.SQL, "SET NOT NULL") {
				t.Fatalf("nullable diff should be applyable, got %#v", action)
			}
			if len(action.Diffs) != 1 || action.Diffs[0].Kind != "nullable" || !action.Diffs[0].Apply {
				t.Fatalf("expected structured nullable diff, got %#v", action.Diffs)
			}
		case strings.Contains(action.Detail, "default <none> -> 18"):
			seen["default"] = true
			if !action.Apply || !strings.Contains(action.SQL, "SET DEFAULT 18") {
				t.Fatalf("default diff should be applyable, got %#v", action)
			}
			if len(action.Diffs) != 1 || action.Diffs[0].Kind != "default" || action.Diffs[0].To != "18" {
				t.Fatalf("expected structured default diff, got %#v", action.Diffs)
			}
		}
	}
	for _, key := range []string{"type", "nullable", "default"} {
		if !seen[key] {
			t.Fatalf("missing %s diff in %#v", key, actions)
		}
	}
}

func TestPlanColumnDiffActionsIgnoresEquivalentTypes(t *testing.T) {
	base := &sqlBase{conn: &writeNormalizeTestConn{}}
	actions := base.planColumnDiffActions("", "users", "id", map[string]Var{
		"name": {Type: "string", Default: "alice"},
	}, map[string]columnInfo{
		"name": {Name: "name", Type: "character varying(64)", Nullable: true, HasNullable: true, Default: "'alice'::character varying", HasDefault: true},
	}, MigrateOptions{Mode: "safe"})
	if len(actions) != 0 {
		t.Fatalf("expected no diff, got %#v", actions)
	}
}

func TestPlanColumnDiffActionsAppliesTypeInDangerMode(t *testing.T) {
	base := &sqlBase{conn: &writeNormalizeTestConn{}}
	actions := base.planColumnDiffActions("", "users", "id", map[string]Var{
		"age": {Type: "int"},
	}, map[string]columnInfo{
		"age": {Name: "age", Type: "TEXT", Nullable: true, HasNullable: true},
	}, MigrateOptions{Mode: "danger"})
	if len(actions) == 0 {
		t.Fatalf("expected type diff")
	}
	if !actions[0].Apply || !strings.Contains(actions[0].SQL, "TYPE BIGINT") {
		t.Fatalf("danger type diff should be applyable, got %#v", actions[0])
	}
}

func TestPlanColumnDiffActionsKeepsDefaultLiteralSQL(t *testing.T) {
	base := &sqlBase{conn: &writeNormalizeTestConn{}}
	actions := base.planColumnDiffActions("", "users", "id", map[string]Var{
		"name": {Type: "string", Default: "Alice"},
	}, map[string]columnInfo{
		"name": {Name: "name", Type: "TEXT", Nullable: true, HasNullable: true},
	}, MigrateOptions{Mode: "safe"})
	if len(actions) != 1 {
		t.Fatalf("expected default diff, got %#v", actions)
	}
	if !strings.Contains(actions[0].SQL, "SET DEFAULT 'Alice'") {
		t.Fatalf("expected quoted default literal, got %#v", actions[0])
	}
}

func TestPlanColumnDiffActionsKeepsPostgresSequencePrimaryKey(t *testing.T) {
	base := &sqlBase{conn: &writeNormalizeTestConn{}}
	actions := base.planColumnDiffActions("", "users", "id", map[string]Var{
		"id": {Type: "int", Required: true},
	}, map[string]columnInfo{
		"id": {
			Name: "id", Type: "bigint", Nullable: false, HasNullable: true,
			Default: "nextval('users_id_seq'::regclass)", HasDefault: true,
		},
	}, MigrateOptions{Mode: "safe"})
	if len(actions) != 0 {
		t.Fatalf("sequence-backed primary key must remain generated, got %#v", actions)
	}
}
