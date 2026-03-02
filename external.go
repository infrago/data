package data

import . "github.com/infrago/base"

func Base(names ...string) DataBase {
	return module.Base(names...)
}

func Migrate(names ...string) error {
	db := Base()
	defer db.Close()
	db.Migrate(names...)
	return db.Error()
}

func MigrateOn(base string, names ...string) error {
	db := Base(base)
	defer db.Close()
	db.Migrate(names...)
	return db.Error()
}

func MigratePlan(names ...string) (MigrateReport, error) {
	db := Base()
	defer db.Close()
	report := db.MigratePlan(names...)
	return report, db.Error()
}

func MigratePlanOn(base string, names ...string) (MigrateReport, error) {
	db := Base(base)
	defer db.Close()
	report := db.MigratePlan(names...)
	return report, db.Error()
}

func MigrateDiff(names ...string) (MigrateReport, error) {
	db := Base()
	defer db.Close()
	report := db.MigrateDiff(names...)
	return report, db.Error()
}

func MigrateDiffOn(base string, names ...string) (MigrateReport, error) {
	db := Base(base)
	defer db.Close()
	report := db.MigrateDiff(names...)
	return report, db.Error()
}

func MigrateUp(versions ...string) error {
	db := Base()
	defer db.Close()
	db.MigrateUp(versions...)
	return db.Error()
}

func MigrateUpOn(base string, versions ...string) error {
	db := Base(base)
	defer db.Close()
	db.MigrateUp(versions...)
	return db.Error()
}

func MigrateDown(steps int) error {
	db := Base()
	defer db.Close()
	db.MigrateDown(steps)
	return db.Error()
}

func MigrateDownOn(base string, steps int) error {
	db := Base(base)
	defer db.Close()
	db.MigrateDown(steps)
	return db.Error()
}

func GetCapabilities(names ...string) (Capabilities, error) {
	return module.GetCapabilities(names...)
}

func GetStats(names ...string) Stats {
	return module.Stats(names...)
}

func GetPoolStats(names ...string) []PoolStats {
	return module.PoolStats(names...)
}

func Parse(args ...Any) (Query, error) {
	return ParseQuery(args...)
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

func Field(name string, field string, extends ...Any) Var {
	return module.Field(name, field, extends...)
}

func Fields(name string, keys []string, extends ...Vars) Vars {
	return module.Fields(name, keys, extends...)
}

func Option(name string, field string, key string) Any {
	return module.Option(name, field, key)
}

func Options(name string, field string) Map {
	return module.Options(name, field)
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

func Migrations(names ...string) []Migration {
	base := ""
	if len(names) > 0 {
		base = names[0]
	}
	return module.migrationConfigs(base)
}
