package data

import (
	"crypto/sha1"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/infrago/infra"
)

type Migration struct {
	Version string
	Name    string
	Desc    string
	Up      func(DataBase) error
	Down    func(DataBase) error
}

func (m Migration) checksum() string {
	payload := strings.TrimSpace(m.Version) + "|" + strings.TrimSpace(m.Name) + "|" + strings.TrimSpace(m.Desc)
	sum := sha1.Sum([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func (m Migration) Checksum() string {
	return m.checksum()
}

func (m *Module) RegisterMigration(name string, migration Migration) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if migration.Version == "" {
		migration.Version = strings.TrimSpace(name)
	}
	if strings.TrimSpace(migration.Name) == "" {
		migration.Name = migration.Version
	}
	if strings.TrimSpace(migration.Version) == "" {
		return
	}
	key := name
	if strings.TrimSpace(key) == "" {
		key = migration.Version
	}
	if infra.Override() {
		m.migrations[key] = migration
	} else if _, ok := m.migrations[key]; !ok {
		m.migrations[key] = migration
	}
}

func (m *Module) migrationConfigs(base string) []Migration {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	out := make([]Migration, 0, len(m.migrations))
	seen := map[string]struct{}{}
	for key, mg := range m.migrations {
		if !migrationMatchKey(base, key) {
			continue
		}
		v := strings.TrimSpace(mg.Version)
		if v == "" {
			continue
		}
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, mg)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Version < out[j].Version
	})
	return out
}

func migrationMatchKey(base, key string) bool {
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	if strings.Contains(key, ".") {
		parts := strings.SplitN(key, ".", 2)
		left := strings.TrimSpace(parts[0])
		if left == "*" {
			return true
		}
		return strings.EqualFold(left, strings.TrimSpace(base))
	}
	return true
}
