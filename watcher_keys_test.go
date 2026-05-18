package data

import (
	"testing"

	. "github.com/infrago/base"
)

func TestWatcherKeysOptions(t *testing.T) {
	base := &sqlBase{
		inst: &Instance{Config: Config{Watcher: Map{
			"keys": Map{
				"enable": true,
				"batch":  64,
				"max":    128,
			},
		}}},
	}
	if !base.watcherKeysEnabled() {
		t.Fatalf("expected watcher keys enabled")
	}
	if got := base.watcherKeysBatchSize(); got != 64 {
		t.Fatalf("expected batch size 64, got %d", got)
	}
	if got := base.watcherKeysMaxKeys(); got != 128 {
		t.Fatalf("expected max keys 128, got %d", got)
	}
}

func TestWatcherKeysBoolCompatibility(t *testing.T) {
	base := &sqlBase{
		inst: &Instance{Config: Config{Watcher: Map{
			"keys": true,
		}}},
	}
	if !base.watcherKeysEnabled() {
		t.Fatalf("expected legacy bool watcher keys to stay enabled")
	}
	if base.watcherKeysBatchSize() != 0 || base.watcherKeysMaxKeys() != 0 {
		t.Fatalf("legacy bool watcher keys should not imply batch/max")
	}
}
