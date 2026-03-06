package data

import (
	"fmt"
	"strings"
)

const (
	OptWithDeleted = "$withDeleted"
	OptOnlyDeleted = "$onlyDeleted"
	OptUnscoped    = "$unscoped"
)

type (
	TrashOptions struct {
		Enable  bool
		Field   string
		Value   string
		Cascade string
	}

	resolvedCascade struct {
		table      string
		foreignKey string
		value      string
	}
)

func normalizeTrashOptions(in TrashOptions) TrashOptions {
	out := in
	if strings.TrimSpace(out.Field) == "" {
		out.Field = "status"
	}
	if strings.TrimSpace(out.Value) == "" {
		out.Value = "removed"
	}
	if strings.TrimSpace(out.Cascade) == "" {
		out.Cascade = "{parent}.removed"
	}
	return out
}

func (b *sqlBase) trashOptions() TrashOptions {
	if b == nil || b.inst == nil {
		return normalizeTrashOptions(TrashOptions{})
	}
	return normalizeTrashOptions(b.inst.Config.Trash)
}

func (b *sqlBase) trashEnabled() bool {
	return b != nil && b.inst != nil && b.inst.Config.Trash.Enable
}

func (b *sqlBase) trashField() string {
	return b.trashOptions().Field
}

func (b *sqlBase) trashValue() string {
	return b.trashOptions().Value
}

func trashParentName(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return ""
	}
	if i := strings.LastIndex(name, "."); i >= 0 && i+1 < len(name) {
		return name[i+1:]
	}
	return name
}

func (b *sqlBase) trashCascadeValue(parent string) string {
	opts := b.trashOptions()
	parent = trashParentName(parent)
	if parent == "" {
		return opts.Value
	}
	value := strings.TrimSpace(opts.Cascade)
	value = strings.ReplaceAll(value, "{parent}", parent)
	if value == "" {
		return opts.Value
	}
	return value
}

func mergeExpr(left, right Expr) Expr {
	if right == nil {
		return left
	}
	if left == nil {
		return right
	}
	if _, ok := left.(TrueExpr); ok {
		return right
	}
	if _, ok := right.(TrueExpr); ok {
		return left
	}
	return AndExpr{Items: []Expr{left, right}}
}

func (v *sqlView) trashScopedField() (string, bool) {
	if v == nil || v.base == nil || !v.base.trashEnabled() {
		return "", false
	}
	field := strings.TrimSpace(v.base.trashField())
	if field == "" {
		return "", false
	}
	if _, ok := lookupField(v.fields, field); !ok {
		return "", false
	}
	return v.base.storageField(field), true
}

func (v *sqlView) applyTrashScope(q *Query) {
	if q == nil || q.Unscoped || q.WithDeleted {
		return
	}
	field, ok := v.trashScopedField()
	if !ok {
		return
	}
	scope := NullExpr{Field: field, Yes: true}
	if q.OnlyDeleted {
		scope = NullExpr{Field: field, Yes: false}
	}
	q.Filter = mergeExpr(q.Filter, scope)
}

func (t *sqlTable) trashEnabledForTable() bool {
	if t == nil || t.base == nil || !t.base.trashEnabled() {
		return false
	}
	_, ok := lookupField(t.fields, t.base.trashField())
	return ok
}

func (t *sqlTable) ensureTrashEnabled(op string) error {
	if t == nil || t.base == nil || t.base.inst == nil {
		return fmt.Errorf("%s unavailable on invalid data table", op)
	}
	if !t.base.trashEnabled() {
		return fmt.Errorf("%s requires data.%s.trash.enable=true", op, t.base.inst.Name)
	}
	if !t.trashEnabledForTable() {
		return fmt.Errorf("%s requires trash field %q on table %s", op, t.base.trashField(), t.name)
	}
	return nil
}

func (t *sqlTable) cascades() ([]resolvedCascade, error) {
	cfg, err := module.tableConfig(t.base.inst.Name, t.name)
	if err != nil {
		return nil, err
	}
	out := make([]resolvedCascade, 0, len(cfg.Cascades))
	for _, item := range cfg.Cascades {
		target := strings.TrimSpace(item.Table)
		if target == "" {
			continue
		}
		fk := strings.TrimSpace(item.ForeignKey)
		if fk == "" {
			return nil, fmt.Errorf("cascade foreignKey is required on table %s -> %s", t.name, target)
		}
		targetCfg, err := module.tableConfig(t.base.inst.Name, target)
		if err != nil {
			return nil, err
		}
		if _, ok := lookupField(targetCfg.Fields, t.base.trashField()); !ok {
			return nil, fmt.Errorf("cascade target %s missing trash field %s", target, t.base.trashField())
		}
		if _, ok := lookupField(targetCfg.Fields, fk); !ok {
			return nil, fmt.Errorf("cascade foreignKey %s not found on table %s", fk, target)
		}
		out = append(out, resolvedCascade{
			table:      target,
			foreignKey: fk,
			value:      t.base.trashCascadeValue(t.name),
		})
	}
	return out, nil
}
