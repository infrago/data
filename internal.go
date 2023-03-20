package data

// . "github.com/infrago/base"

func (this *Module) Tables() map[string]Table {
	tables := make(map[string]Table, 0)
	for k, v := range this.tables {
		tables[k] = v
	}

	return tables
}

func (this *Module) Views() map[string]View {
	views := make(map[string]View, 0)
	for k, v := range this.views {
		views[k] = v
	}

	return views
}

func (this *Module) Models() map[string]Model {
	models := make(map[string]Model, 0)
	for k, v := range this.models {
		models[k] = v
	}

	return models
}
