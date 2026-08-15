package data

import (
	"strings"
	"testing"

	. "github.com/infrago/base"
)

func TestConfiguredURLLoadsDSNFromEnvironment(t *testing.T) {
	t.Setenv("TEST_DATA_DSN", " postgres://user:secret@database/test ")
	got := configuredURL(Map{"url_env": "TEST_DATA_DSN"}, "file:development.db")
	if got != "postgres://user:secret@database/test" {
		t.Fatalf("unexpected configured URL: %q", got)
	}
}

func TestConfiguredURLUsesLiteralFallbackWithoutEnvironmentSetting(t *testing.T) {
	if got := configuredURL(Map{}, "file:development.db"); got != "file:development.db" {
		t.Fatalf("unexpected literal fallback: %q", got)
	}
}

func TestConfiguredURLRejectsMissingAndUnsafeEnvironmentNames(t *testing.T) {
	for name, config := range map[string]Map{
		"missing": {"url_env": "TEST_MISSING_DATA_DSN"},
		"unsafe":  {"url_env": "test-data-dsn"},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				value := recover()
				if value == nil || !strings.Contains(value.(string), "DSN environment variable") {
					t.Fatalf("expected safe DSN configuration panic, got %#v", value)
				}
			}()
			_ = configuredURL(config, "")
		})
	}
}
