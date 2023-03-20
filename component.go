package data

import (
	"strings"

	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

type (
	Table struct {
		Name    string `json:"name"`
		Text    string `json:"text"`
		Schema  string `json:"schema"`
		Table   string `json:"table"`
		Key     string `json:"key"`
		Fields  Vars   `json:"fields"`
		Setting Map    `toml:"setting"`
	}
	View struct {
		Name    string `json:"name"`
		Text    string `json:"text"`
		Schema  string `json:"schema"`
		View    string `json:"view"`
		Key     string `json:"key"`
		Fields  Vars   `json:"fields"`
		Setting Map    `toml:"setting"`
	}
	Model struct {
		Name    string `json:"name"`
		Text    string `json:"text"`
		Model   string `json:"model"`
		Key     string `json:"key"`
		Fields  Vars   `json:"fields"`
		Setting Map    `toml:"setting"`
	}

	Relate struct {
		Key, Field, Status, Type string
	}
)

func (this *Module) Table(name string, config Table) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if infra.Override() {
		this.tables[name] = config
	} else {
		if _, ok := this.tables[name]; ok == false {
			this.tables[name] = config
		}
	}
}

func (this *Module) TableConfig(name string) *Table {
	if config, ok := this.tables[name]; ok {
		//注意：这里应该是复制一份
		return &Table{
			config.Name, config.Text,
			config.Schema, config.Table,
			config.Key, config.Fields, config.Setting,
		}
	}
	return nil
}

func (this *Module) View(name string, config View) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if infra.Override() {
		this.views[name] = config
	} else {
		if _, ok := this.views[name]; ok == false {
			this.views[name] = config
		}
	}
}
func (this *Module) ViewConfig(name string) *View {
	if config, ok := this.views[name]; ok {
		//注意：这里应该是复制了一份
		return &View{
			config.Name, config.Text,
			config.Schema, config.View,
			config.Key, config.Fields, config.Setting,
		}
		// return &config
	}
	return nil
}

// 注册模型
func (this *Module) Model(name string, config Model) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	if infra.Override() {
		this.models[name] = config
	} else {
		if _, ok := this.models[name]; ok == false {
			this.models[name] = config
		}
	}
}
func (this *Module) ModelConfig(name string) *Model {
	if config, ok := this.models[name]; ok {
		//注意：这里应该是复制了一份
		return &Model{
			config.Name, config.Text, config.Model,
			config.Key, config.Fields, config.Setting,
		}
		return &config
	}
	return nil
}

func (this *Module) Field(name string, field string, extends ...Any) Var {
	fields := this.Fields(name, []string{field})
	var config Var
	if vv, ok := fields[field]; ok {
		config = vv
	}

	return infra.VarExtend(config, extends...)
}
func (this *Module) Fields(name string, keys []string, extends ...Vars) Vars {
	if _, ok := this.tables[name]; ok {
		return this.TableFields(name, keys, extends...)
	} else if _, ok := this.views[name]; ok {
		return this.ViewFields(name, keys, extends...)
	} else if _, ok := this.models[name]; ok {
		return this.ModelFields(name, keys, extends...)
	} else {
		if len(extends) > 0 {
			return extends[0]
		}
		return Vars{}
	}
}
func (this *Module) TableFields(name string, keys []string, extends ...Vars) Vars {
	fields := Vars{}
	if config, ok := this.tables[name]; ok && config.Fields != nil {
		//空数组一个也不返
		if keys == nil {
			for k, v := range config.Fields {
				fields[k] = v
			}
		} else {
			for _, k := range keys {
				if v, ok := config.Fields[k]; ok {
					fields[k] = v
				}

			}
		}
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
func (this *Module) ViewFields(name string, keys []string, exts ...Vars) Vars {
	fields := Vars{}
	if config, ok := this.views[name]; ok && config.Fields != nil {
		//空数组一个也不返
		if keys == nil {
			for k, v := range config.Fields {
				fields[k] = v
			}
		} else {
			for _, k := range keys {
				if v, ok := config.Fields[k]; ok {
					fields[k] = v
				}

			}
		}
	}

	if len(exts) > 0 {
		for k, v := range exts[0] {
			fields[k] = v
		}
	}

	return fields
}
func (this *Module) ModelFields(name string, keys []string, exts ...Vars) Vars {
	fields := Vars{}
	if config, ok := this.models[name]; ok && config.Fields != nil {
		//空数组一个也不返
		if keys == nil {
			for k, v := range config.Fields {
				fields[k] = v
			}
		} else {
			for _, k := range keys {
				if v, ok := config.Fields[k]; ok {
					fields[k] = v
				}
			}
		}
	}

	if len(exts) > 0 {
		for k, v := range exts[0] {
			fields[k] = v
		}
	}

	return fields
}
func (this *Module) Option(name, field, key string) Any {
	enums := this.Options(name, field)
	if vv, ok := enums[key]; ok {
		return vv
	}
	return key
}

func (this *Module) Options(name, field string) Map {
	if _, ok := this.tables[name]; ok {
		return this.TableOptions(name, field)
	} else if _, ok := this.views[name]; ok {
		return this.ViewOptions(name, field)
	} else if _, ok := this.models[name]; ok {
		return this.ModelOptions(name, field)
	} else {
		return Map{}
	}
}

// 2021-03-04支持一级子字段的option
func (this *Module) TableOptions(name, field string) Map {
	options := Map{}
	fields := strings.Split(field, ".")
	if len(fields) > 1 {
		field = fields[0]
		child := fields[1]
		if config, ok := this.tables[name]; ok && config.Fields != nil {
			if field, ok := config.Fields[field]; ok {
				if childConfig, ok := field.Children[child]; ok {
					if childConfig.Options != nil {
						for k, v := range childConfig.Options {
							options[k] = v
						}
					}
				}
			}
		}
	} else {
		if config, ok := this.tables[name]; ok && config.Fields != nil {
			if field, ok := config.Fields[field]; ok {
				if field.Options != nil {
					for k, v := range field.Options {
						options[k] = v
					}
				}
			}
		}
	}

	return options
}
func (this *Module) ViewOptions(name, field string) Map {
	options := Map{}
	if config, ok := this.views[name]; ok && config.Fields != nil {
		if field, ok := config.Fields[field]; ok {
			if field.Options != nil {
				for k, v := range field.Options {
					options[k] = v
				}
			}
		}
	}
	return options
}
func (this *Module) ModelOptions(name, field string) Map {
	options := Map{}
	if config, ok := this.models[name]; ok && config.Fields != nil {
		if field, ok := config.Fields[field]; ok {
			if field.Options != nil {
				for k, v := range field.Options {
					options[k] = v
				}
			}
		}
	}
	return options
}
