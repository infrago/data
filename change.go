package data

import (
	"path"
	"strings"
	"sync"
	"time"

	"github.com/infrago/infra"
	. "github.com/infrago/base"
)

const (
	MutationInsert = "insert"
	MutationUpdate = "update"
	MutationDelete = "delete"
	MutationUpsert = "upsert"
)

type (
	Watcher struct {
		Name   string
		Desc   string
		Action func(Mutation)
		Insert func(Mutation)
		Update func(Mutation)
		Upsert func(Mutation)
		Delete func(Mutation)
	}

	Watchers map[string]Watcher

	InsertWatcher struct {
		Name   string
		Desc   string
		Action func(Mutation)
	}

	UpdateWatcher struct {
		Name   string
		Desc   string
		Action func(Mutation)
	}

	UpsertWatcher struct {
		Name   string
		Desc   string
		Action func(Mutation)
	}

	DeleteWatcher struct {
		Name   string
		Desc   string
		Action func(Mutation)
	}

	Mutation struct {
		Base  string    `json:"base"`
		Table string    `json:"table"`
		Op    string    `json:"op"`
		Rows  int64     `json:"rows"`
		Key   Any       `json:"key,omitempty"`
		Data  Map       `json:"data,omitempty"`
		Where Map       `json:"where,omitempty"`
		At    time.Time `json:"at"`
	}

	changeConfig struct {
		Enable   bool
		Workers  int
		Queue    int
		Overflow string // drop | block
		Payload  string // minimal | full
	}

	changeRule struct {
		name     string
		tablePat string
		watcher  Watcher
	}

	changeDispatcher struct {
		name   string
		cfg    changeConfig
		queue  chan Mutation
		stop   chan struct{}
		once   sync.Once
		module *Module
	}
)

func (m *Module) RegisterWatcher(name string, watcher Watcher) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if name == "" && strings.TrimSpace(watcher.Name) != "" {
		name = watcher.Name
	}
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return
	}
	if watcher.Action == nil && watcher.Insert == nil && watcher.Update == nil && watcher.Upsert == nil && watcher.Delete == nil {
		return
	}
	if m.watchers == nil {
		m.watchers = make(map[string]Watcher, 0)
	}
	if infragoOverrideEnabled() {
		m.watchers[name] = watcher
	} else if _, ok := m.watchers[name]; !ok {
		m.watchers[name] = watcher
	}
}

func (m *Module) RegisterWatchers(items Watchers) {
	for name, watcher := range items {
		m.RegisterWatcher(name, watcher)
	}
}

func (m *Module) RegisterInsertWatcher(name string, watcher InsertWatcher) {
	m.RegisterWatcher(name, Watcher{
		Name:   watcher.Name,
		Desc:   watcher.Desc,
		Insert: watcher.Action,
	})
}

func (m *Module) RegisterUpdateWatcher(name string, watcher UpdateWatcher) {
	m.RegisterWatcher(name, Watcher{
		Name:   watcher.Name,
		Desc:   watcher.Desc,
		Update: watcher.Action,
	})
}

func (m *Module) RegisterUpsertWatcher(name string, watcher UpsertWatcher) {
	m.RegisterWatcher(name, Watcher{
		Name:   watcher.Name,
		Desc:   watcher.Desc,
		Upsert: watcher.Action,
	})
}

func (m *Module) RegisterDeleteWatcher(name string, watcher DeleteWatcher) {
	m.RegisterWatcher(name, Watcher{
		Name:   watcher.Name,
		Desc:   watcher.Desc,
		Delete: watcher.Action,
	})
}

func (m *Module) emitChange(event Mutation) {
	m.mutex.RLock()
	dispatcher := m.dispatchers[event.Base]
	m.mutex.RUnlock()
	if dispatcher == nil {
		return
	}
	dispatcher.emit(event)
}

func (m *Module) changeRules() []changeRule {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	out := make([]changeRule, 0, len(m.watchers))
	for name, watcher := range m.watchers {
		tablePat := parseWatchPattern(name)
		out = append(out, changeRule{
			name:     name,
			tablePat: tablePat,
			watcher:  watcher,
		})
	}
	return out
}

func (m *Module) parseChangeConfig(cfg Config) changeConfig {
	cc := changeConfig{
		Enable:   false,
		Workers:  1,
		Queue:    1024,
		Overflow: "drop",
		Payload:  "minimal",
	}
	cm := cfg.Watcher
	if cm == nil {
		cm = Map{}
	}
	if len(cm) == 0 && cfg.Setting != nil {
		raw, ok := cfg.Setting["change"]
		if !ok {
			raw, ok = cfg.Setting["trigger"]
		}
		if ok {
			if legacy, ok := raw.(Map); ok {
				cm = legacy
			}
		}
	}
	if len(cm) == 0 {
		return cc
	}
	if v, ok := parseBool(cm["enable"]); ok {
		cc.Enable = v
	}
	if v, ok := parseIntAny(cm["workers"]); ok && v > 0 {
		cc.Workers = v
	}
	if v, ok := parseIntAny(cm["queue"]); ok && v > 0 {
		cc.Queue = v
	}
	if v, ok := cm["overflow"].(string); ok {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "drop" || v == "block" {
			cc.Overflow = v
		}
	}
	if v, ok := cm["payload"].(string); ok {
		v = strings.ToLower(strings.TrimSpace(v))
		if v == "minimal" || v == "full" {
			cc.Payload = v
		}
	}
	return cc
}

func newChangeDispatcher(name string, cfg changeConfig, module *Module) *changeDispatcher {
	d := &changeDispatcher{
		name:   name,
		cfg:    cfg,
		module: module,
		stop:   make(chan struct{}),
	}
	if cfg.Enable {
		d.queue = make(chan Mutation, cfg.Queue)
		for i := 0; i < cfg.Workers; i++ {
			go d.loop()
		}
	}
	return d
}

func (d *changeDispatcher) emit(event Mutation) {
	if d == nil || !d.cfg.Enable {
		return
	}
	statsFor(d.name).ChangeIn.Add(1)
	if d.cfg.Overflow == "block" {
		select {
		case <-d.stop:
			statsFor(d.name).ChangeDrop.Add(1)
		case d.queue <- event:
		}
		return
	}
	select {
	case d.queue <- event:
	default:
		statsFor(d.name).ChangeDrop.Add(1)
	}
}

func (d *changeDispatcher) loop() {
	for {
		select {
		case <-d.stop:
			return
		case event := <-d.queue:
			rules := d.module.changeRules()
			for _, rule := range rules {
				if !matchWatchRule(rule, event.Table) {
					continue
				}
				callWatcherEvent(d.name, rule.watcher.Action, event)
				switch event.Op {
				case MutationInsert:
					callWatcherEvent(d.name, rule.watcher.Insert, event)
				case MutationUpdate:
					callWatcherEvent(d.name, rule.watcher.Update, event)
				case MutationUpsert:
					callWatcherEvent(d.name, rule.watcher.Upsert, event)
				case MutationDelete:
					callWatcherEvent(d.name, rule.watcher.Delete, event)
				}
			}
		}
	}
}

func (d *changeDispatcher) close() {
	if d == nil {
		return
	}
	d.once.Do(func() {
		close(d.stop)
	})
}

func parseWatchPattern(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return "*"
	}
	return name
}

func matchWatchRule(rule changeRule, table string) bool {
	if !matchPattern(rule.tablePat, strings.ToLower(strings.TrimSpace(table))) {
		return false
	}
	return true
}

func matchPattern(pattern, value string) bool {
	pattern = strings.TrimSpace(strings.ToLower(pattern))
	value = strings.TrimSpace(strings.ToLower(value))
	if pattern == "" || pattern == "*" {
		return true
	}
	if pattern == value {
		return true
	}
	ok, _ := path.Match(pattern, value)
	return ok
}

func infragoOverrideEnabled() bool {
	return infra.Override()
}

func EmitMutation(base, table, op string, rows int64, key Any, data Map, where Map) {
	module.emitChange(Mutation{
		Base:  base,
		Table: table,
		Op:    strings.ToLower(strings.TrimSpace(op)),
		Rows:  rows,
		Key:   key,
		Data:  data,
		Where: where,
		At:    time.Now(),
	})
}

func callWatcherEvent(base string, action func(Mutation), event Mutation) {
	if action == nil {
		return
	}
	func() {
		defer func() {
			if recover() != nil {
				statsFor(base).ChangeFail.Add(1)
			}
		}()
		action(event)
	}()
}
