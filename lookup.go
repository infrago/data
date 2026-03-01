package data

import (
	"fmt"

	. "github.com/infrago/base"
)

func (m *Module) tableConfig(base, name string) (Table, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for _, key := range []string{base + "." + name, "*." + name, name} {
		if cfg, ok := m.tables[key]; ok {
			return cfg, nil
		}
	}
	return Table{}, fmt.Errorf("data table not found: %s", name)
}

func (m *Module) viewConfig(base, name string) (View, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for _, key := range []string{base + "." + name, "*." + name, name} {
		if cfg, ok := m.views[key]; ok {
			return cfg, nil
		}
	}
	return View{}, fmt.Errorf("data view not found: %s", name)
}

func (m *Module) modelConfig(base, name string) (Model, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	for _, key := range []string{base + "." + name, "*." + name, name} {
		if cfg, ok := m.models[key]; ok {
			return cfg, nil
		}
	}
	return Model{}, fmt.Errorf("data model not found: %s", name)
}

func (m *Module) Tables() map[string]Table {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	out := make(map[string]Table, len(m.tables))
	for k, v := range m.tables {
		out[k] = v
	}
	return out
}

func (m *Module) Views() map[string]View {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	out := make(map[string]View, len(m.views))
	for k, v := range m.views {
		out[k] = v
	}
	return out
}

func (m *Module) Models() map[string]Model {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	out := make(map[string]Model, len(m.models))
	for k, v := range m.models {
		out[k] = v
	}
	return out
}

func (m *Module) TableConfig(name string) *Table {
	cfg, err := m.tableConfig("", name)
	if err != nil {
		return nil
	}
	out := cfg
	out.Fields = cloneVars(cfg.Fields)
	out.Setting = cloneMap(cfg.Setting)
	out.Indexes = cloneIndexList(cfg.Indexes)
	return &out
}

func (m *Module) ViewConfig(name string) *View {
	cfg, err := m.viewConfig("", name)
	if err != nil {
		return nil
	}
	out := cfg
	out.Fields = cloneVars(cfg.Fields)
	out.Setting = cloneMap(cfg.Setting)
	return &out
}

func (m *Module) ModelConfig(name string) *Model {
	cfg, err := m.modelConfig("", name)
	if err != nil {
		return nil
	}
	out := cfg
	out.Fields = cloneVars(cfg.Fields)
	out.Setting = cloneMap(cfg.Setting)
	return &out
}

func (m *Module) Field(name, field string, extends ...Any) Var {
	fields := m.Fields(name, []string{field})
	item, ok := fields[field]
	if !ok {
		return Nil
	}
	return item
}

func (m *Module) Fields(name string, keys []string, extends ...Vars) Vars {
	fields := Vars{}
	if cfg := m.TableConfig(name); cfg != nil {
		fields = selectVars(cfg.Fields, keys)
	} else if cfg := m.ViewConfig(name); cfg != nil {
		fields = selectVars(cfg.Fields, keys)
	} else if cfg := m.ModelConfig(name); cfg != nil {
		fields = selectVars(cfg.Fields, keys)
	}
	if len(extends) > 0 {
		for k, v := range extends[0] {
			if v.Nil() {
				delete(fields, k)
			} else {
				fields[k] = v
			}
		}
	}
	return fields
}

func (m *Module) Options(name, field string) Map {
	if cfg := m.TableConfig(name); cfg != nil {
		return optionMap(cfg.Fields, field)
	}
	if cfg := m.ViewConfig(name); cfg != nil {
		return optionMap(cfg.Fields, field)
	}
	if cfg := m.ModelConfig(name); cfg != nil {
		return optionMap(cfg.Fields, field)
	}
	return Map{}
}

func (m *Module) Option(name, field, key string) Any {
	opts := m.Options(name, field)
	if val, ok := opts[key]; ok {
		return val
	}
	return key
}

func selectVars(all Vars, keys []string) Vars {
	if len(all) == 0 {
		return Vars{}
	}
	if keys == nil {
		return cloneVars(all)
	}
	out := Vars{}
	for _, key := range keys {
		if field, ok := all[key]; ok {
			out[key] = field
		}
	}
	return out
}

func optionMap(fields Vars, field string) Map {
	item, ok := lookupField(fields, field)
	if !ok || item.Options == nil {
		return Map{}
	}
	return cloneMap(item.Options)
}

func cloneIndexList(in []Index) []Index {
	if len(in) == 0 {
		return []Index{}
	}
	out := make([]Index, 0, len(in))
	for _, idx := range in {
		fields := make([]string, 0, len(idx.Fields))
		fields = append(fields, idx.Fields...)
		out = append(out, Index{
			Name:   idx.Name,
			Fields: fields,
			Unique: idx.Unique,
		})
	}
	return out
}
