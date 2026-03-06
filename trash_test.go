package data

import (
	"strings"
	"testing"

	. "github.com/infrago/base"
)

func TestConfigureParsesTrashNestedAndFlat(t *testing.T) {
	m := &Module{
		configs: Configs{},
	}

	m.configure("main", Map{
		"trash": Map{
			"enable":  true,
			"field":   "status",
			"value":   "removed",
			"cascade": "{parent}.removed",
		},
	})

	cfg := m.configs["main"]
	if !cfg.Trash.Enable || cfg.Trash.Field != "status" || cfg.Trash.Value != "removed" || cfg.Trash.Cascade != "{parent}.removed" {
		t.Fatalf("unexpected nested trash config: %#v", cfg.Trash)
	}

	m.configure("main", Map{
		"trash_enable":        false,
		"trash_field":         "deletedStatus",
		"trash_value":         "gone",
		"trash_cascade_value": "{parent}.gone",
	})

	cfg = m.configs["main"]
	if cfg.Trash.Enable {
		t.Fatalf("expected flat config to override enable=false, got %#v", cfg.Trash)
	}
	if cfg.Trash.Field != "deletedStatus" || cfg.Trash.Value != "gone" || cfg.Trash.Cascade != "{parent}.gone" {
		t.Fatalf("unexpected flat trash config override: %#v", cfg.Trash)
	}
}

func TestConfigureTrashDefaultsApplyWithoutEnablingFeature(t *testing.T) {
	m := &Module{
		configs: Configs{},
	}

	m.configure("main", Map{})

	cfg := m.configs["main"]
	if cfg.Trash.Enable {
		t.Fatalf("expected trash disabled by default, got %#v", cfg.Trash)
	}
	if cfg.Trash.Field != "status" || cfg.Trash.Value != "removed" || cfg.Trash.Cascade != "{parent}.removed" {
		t.Fatalf("unexpected trash defaults: %#v", cfg.Trash)
	}

	m.configure("main", Map{
		"trash": "true",
	})
	cfg = m.configs["main"]
	if !cfg.Trash.Enable {
		t.Fatalf("expected string trash=true to enable feature, got %#v", cfg.Trash)
	}
}

func TestApplyTrashScopeHonorsMappingAndOptions(t *testing.T) {
	base := &sqlBase{
		inst: &Instance{
			Config: Config{
				Mapping: true,
				Trash: TrashOptions{
					Enable: true,
					Field:  "deletedStatus",
				},
			},
		},
	}
	view := &sqlView{
		base: base,
		fields: Vars{
			"deletedStatus": {Type: "string"},
		},
	}

	q := Query{Filter: TrueExpr{}}
	view.applyTrashScope(&q)

	scope, ok := q.Filter.(NullExpr)
	if !ok {
		t.Fatalf("expected NullExpr scope, got %T", q.Filter)
	}
	if !scope.Yes || scope.Field != "deleted_status" {
		t.Fatalf("unexpected scope expr: %#v", scope)
	}

	q = Query{Filter: TrueExpr{}, WithDeleted: true}
	view.applyTrashScope(&q)
	if _, ok := q.Filter.(TrueExpr); !ok {
		t.Fatalf("expected WithDeleted to skip default scope, got %T", q.Filter)
	}

	q = Query{Filter: TrueExpr{}, OnlyDeleted: true}
	view.applyTrashScope(&q)
	scope, ok = q.Filter.(NullExpr)
	if !ok || scope.Yes {
		t.Fatalf("expected OnlyDeleted => IS NOT NULL scope, got %#v", q.Filter)
	}
}

func TestApplyTrashScopeSkipsWhenTrashDisabled(t *testing.T) {
	base := &sqlBase{
		inst: &Instance{
			Config: Config{
				Mapping: true,
				Trash: TrashOptions{
					Field: "status",
				},
			},
		},
	}
	view := &sqlView{
		base: base,
		fields: Vars{
			"status": {Type: "string"},
		},
	}

	q := Query{Filter: TrueExpr{}}
	view.applyTrashScope(&q)

	if _, ok := q.Filter.(TrueExpr); !ok {
		t.Fatalf("expected disabled trash to skip scope, got %T", q.Filter)
	}
}

func TestParseQuerySupportsTrashScopeOptions(t *testing.T) {
	q, err := ParseQuery(Map{
		OptWithDeleted: true,
		OptOnlyDeleted: false,
		OptUnscoped:    true,
	})
	if err != nil {
		t.Fatalf("unexpected parse err: %v", err)
	}
	if !q.WithDeleted || !q.Unscoped || q.OnlyDeleted {
		t.Fatalf("unexpected query trash flags: %#v", q)
	}
}

func TestTableConfigClonesCascades(t *testing.T) {
	oldTables := module.tables
	defer func() {
		module.tables = oldTables
	}()

	module.tables = map[string]Table{
		"artist": {
			Cascades: []Cascade{
				{Table: "album", ForeignKey: "artistId"},
			},
		},
	}

	cfg := module.TableConfig("artist")
	if cfg == nil || len(cfg.Cascades) != 1 {
		t.Fatalf("expected cascades copy, got %#v", cfg)
	}
	cfg.Cascades[0].Table = "broken"

	if module.tables["artist"].Cascades[0].Table != "album" {
		t.Fatalf("expected table config clone to avoid mutating module state")
	}
}

func TestCascadesResolveByLogicalTableName(t *testing.T) {
	oldTables := module.tables
	defer func() {
		module.tables = oldTables
	}()

	module.tables = map[string]Table{
		"artist": {
			Fields: Vars{
				"id":     {Type: "int"},
				"status": {Type: "string"},
			},
			Cascades: []Cascade{
				{Table: "album", ForeignKey: "artistId"},
			},
		},
		"album": {
			Table: "music_album",
			Fields: Vars{
				"id":       {Type: "int"},
				"artistId": {Type: "int"},
				"status":   {Type: "string"},
			},
		},
	}

	table := &sqlTable{
		sqlView: sqlView{
			base: &sqlBase{
				inst: &Instance{
					Name: "main",
					Config: Config{
						Trash: TrashOptions{
							Enable:  true,
							Field:   "status",
							Value:   "removed",
							Cascade: "{parent}.removed",
						},
					},
				},
			},
			name: "artist",
			fields: Vars{
				"id":     {Type: "int"},
				"status": {Type: "string"},
			},
		},
	}

	cascades, err := table.cascades()
	if err != nil {
		t.Fatalf("unexpected cascades err: %v", err)
	}
	if len(cascades) != 1 {
		t.Fatalf("expected 1 cascade, got %#v", cascades)
	}
	if cascades[0].table != "album" || cascades[0].foreignKey != "artistId" || cascades[0].value != "artist.removed" {
		t.Fatalf("unexpected resolved cascade: %#v", cascades[0])
	}
}

func TestRemoveRequiresTrashEnabled(t *testing.T) {
	table := &sqlTable{
		sqlView: sqlView{
			base: &sqlBase{
				inst: &Instance{
					Name:   "main",
					Config: Config{},
				},
			},
			name: "artist",
			key:  "id",
			fields: Vars{
				"id":     {Type: "int"},
				"status": {Type: "string"},
			},
		},
	}

	if out := table.Remove(Map{"id": 1}); out != nil {
		t.Fatalf("expected nil remove result when trash disabled")
	}
	if err := table.base.Error(); err == nil || !strings.Contains(err.Error(), "trash") {
		t.Fatalf("expected trash error, got %v", err)
	}
}

func TestTrashMutationUsesPrimaryKeyFromMap(t *testing.T) {
	table := &sqlTable{
		sqlView: sqlView{
			base: &sqlBase{},
			key:  "id",
		},
	}

	args := table.singleMutationArgs(Map{
		"id":     7,
		"name":   "artist",
		"status": "removed",
	})
	if len(args) != 1 {
		t.Fatalf("expected single primary-key arg, got %#v", args)
	}
	where, ok := args[0].(Map)
	if !ok {
		t.Fatalf("expected normalized map arg, got %T", args[0])
	}
	if len(where) != 1 || where["id"] != 7 {
		t.Fatalf("expected primary-key-only condition, got %#v", where)
	}
}

func TestManyMutationArgsUsesPrimaryKeysFromEntityList(t *testing.T) {
	table := &sqlTable{
		sqlView: sqlView{
			base: &sqlBase{},
			key:  "id",
		},
	}

	args := table.manyMutationArgs([]Map{
		{"id": 1, "name": "a"},
		{"id": 2, "name": "b"},
		{"id": 1, "name": "dup"},
	})
	if len(args) != 1 {
		t.Fatalf("expected normalized args, got %#v", args)
	}
	where, ok := args[0].(Map)
	if !ok {
		t.Fatalf("expected map condition, got %T", args[0])
	}
	in, ok := where["id"].(Map)
	if !ok {
		t.Fatalf("expected IN condition, got %#v", where["id"])
	}
	ids, ok := in[OpIn].([]Any)
	if !ok {
		t.Fatalf("expected []Any ids, got %#v", in[OpIn])
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("unexpected primary keys: %#v", ids)
	}
}
