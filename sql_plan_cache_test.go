package data

import (
	"testing"

	. "github.com/infrago/base"
)

func resetSQLPlanCache() {
	sqlPlanCache.Range(func(k, _ any) bool {
		sqlPlanCache.Delete(k)
		return true
	})
	sqlPlanCacheCount.Store(0)
}

func TestSQLPlanCacheUsesFieldSignature(t *testing.T) {
	resetSQLPlanCache()
	v := &sqlView{
		base: &sqlBase{
			inst: &Instance{Name: "plan-cache"},
			conn: &writeNormalizeTestConn{},
		},
		name:   "users",
		source: "users",
		key:    "id",
		fields: Vars{
			"tags": Var{Type: "[string]"},
		},
	}
	q := Query{Filter: CmpExpr{Field: "tags", Op: OpContains, Value: []string{"go"}}}
	v.storeSQLPlan("query", q, "SELECT 1", []Any{"go"})

	sqlText, params, ok := v.loadSQLPlan("query", q)
	if !ok || sqlText != "SELECT 1" || len(params) != 1 || params[0] != "go" {
		t.Fatalf("expected cached plan, got ok=%v sql=%q params=%#v", ok, sqlText, params)
	}

	v.fields["tags"] = Var{Type: "json"}
	if sqlText, _, ok := v.loadSQLPlan("query", q); ok {
		t.Fatalf("field type change should miss cached plan, got %q", sqlText)
	}
}

func TestSQLPlanCacheUsesMappingFlag(t *testing.T) {
	resetSQLPlanCache()
	q := Query{Filter: CmpExpr{Field: "profile.name", Op: OpEq, Value: "alice"}}
	v := &sqlView{
		base: &sqlBase{
			inst: &Instance{Name: "plan-cache-mapping", Config: Config{Mapping: false}},
			conn: &writeNormalizeTestConn{},
		},
		name:   "users",
		source: "users",
		key:    "id",
		fields: Vars{"profile": Var{Type: "json"}},
	}
	v.storeSQLPlan("query", q, "SELECT no_mapping", []Any{"alice"})
	v.base.inst.Config.Mapping = true
	if sqlText, _, ok := v.loadSQLPlan("query", q); ok {
		t.Fatalf("mapping change should miss cached plan, got %q", sqlText)
	}
}

func TestSQLPlanCacheCapacity(t *testing.T) {
	resetSQLPlanCache()
	v := &sqlView{
		base: &sqlBase{
			inst: &Instance{Name: "plan-cache-capacity", Config: Config{Setting: Map{"planCache": Map{"capacity": 1}}}},
			conn: &writeNormalizeTestConn{},
		},
		name:   "users",
		source: "users",
		key:    "id",
		fields: Vars{"name": Var{Type: "string"}},
	}
	v.storeSQLPlan("query", Query{Filter: CmpExpr{Field: "name", Op: OpEq, Value: "alice"}}, "SELECT 1", []Any{"alice"})
	v.storeSQLPlan("query", Query{Filter: CmpExpr{Field: "name", Op: OpEq, Value: "bob"}}, "SELECT 2", []Any{"bob"})
	if got := sqlPlanCacheCount.Load(); got > 1 {
		t.Fatalf("expected plan cache count capped to 1, got %d", got)
	}
}

func TestSQLPlanCacheKeepsHotPlan(t *testing.T) {
	resetSQLPlanCache()
	v := &sqlView{
		base: &sqlBase{
			inst: &Instance{Name: "plan-cache-hot", Config: Config{Setting: Map{"planCache": Map{"capacity": 1}}}},
			conn: &writeNormalizeTestConn{},
		},
		name:   "users",
		source: "users",
		key:    "id",
		fields: Vars{"name": Var{Type: "string"}},
	}
	hot := Query{Filter: CmpExpr{Field: "name", Op: OpEq, Value: "alice"}}
	cold := Query{Filter: CmpExpr{Field: "name", Op: OpEq, Value: "bob"}}
	v.storeSQLPlan("query", hot, "SELECT hot", []Any{"alice"})
	for i := 0; i < 3; i++ {
		if _, _, ok := v.loadSQLPlan("query", hot); !ok {
			t.Fatalf("expected hot plan hit")
		}
	}
	v.storeSQLPlan("query", cold, "SELECT cold", []Any{"bob"})
	if sqlText, _, ok := v.loadSQLPlan("query", hot); !ok || sqlText != "SELECT hot" {
		t.Fatalf("expected hot plan to stay cached, got ok=%v sql=%q", ok, sqlText)
	}
	if _, _, ok := v.loadSQLPlan("query", cold); ok {
		t.Fatalf("expected cold plan to be evicted first")
	}
}
