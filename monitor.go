package data

import (
	"github.com/infrago/base"
	"github.com/infrago/infra"
)

func (m *Module) Ready() bool {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.started && m.connected && len(m.instances) > 0
}

func (m *Module) Health() infra.ModuleHealth {
	m.mutex.RLock()
	ready := m.started && m.connected && len(m.instances) > 0
	connections := len(m.instances)
	instances := make(map[string]*Instance, len(m.instances))
	for name, inst := range m.instances {
		instances[name] = inst
	}
	m.mutex.RUnlock()

	workloads := base.Map{}
	for name, inst := range instances {
		if inst != nil && inst.conn != nil {
			workloads[name] = inst.conn.Health().Workload
		}
	}
	return infra.NewModuleHealth("data", ready, nil, base.Map{
		"connections": connections,
		"workloads":   workloads,
	})
}

func (m *Module) Stats() infra.ModuleStats {
	ready := m.Ready()
	return infra.NewModuleStats("data", ready, base.Map{
		"connections": m.PoolStats(),
		"queries":     m.QueryStats(),
	})
}
