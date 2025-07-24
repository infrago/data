package data

import (
	. "github.com/infrago/base"
	"github.com/infrago/infra"
)

//------ data group -------

func Base(names ...string) DataBase {
	return module.Base(names...)
}

func GetTable(name string) *Table {
	return module.TableConfig(name)
}
func GetView(name string) *View {
	return module.ViewConfig(name)
}
func GetModel(name string) *Model {
	return module.ModelConfig(name)
}

func Field(name string, field string, exts ...Any) Var {
	return module.Field(name, field, exts...)
}
func Fields(name string, keys []string, exts ...Vars) Vars {
	return module.Fields(name, keys, exts...)
}
func Option(name string, field string, key string) Any {
	return module.Option(name, field, key)
}
func Options(name string, field string) Map {
	return module.Options(name, field)
}

func ParseSQL(args ...Any) (string, []Any, string, error) {
	return module.Parse(args...)
}

// 接收回来触发器
func Trigger(name string, values ...Map) {
	infra.Toggle(name, values...)
}

func Tables() map[string]Table {
	return module.Tables()
}

func Views() map[string]View {
	return module.Views()
}

func Models() map[string]Model {
	return module.Models()
}
