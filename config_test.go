package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

const validConfigYAML = `
server:
  host: "127.0.0.1"
  port: "8090"
database:
  dsn: "postgres://sub2api:pass@postgres:5432/sub2api?sslmode=disable"
sub2api:
  base_url: "http://sub2api:8080"
  admin_api_key: "test-key"
`

func TestLoadConfigValid(t *testing.T) {
	cfg, err := loadConfig(writeTempConfig(t, validConfigYAML))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Server.Host != "127.0.0.1" {
		t.Errorf("host = %q, want 127.0.0.1", cfg.Server.Host)
	}
	if cfg.Server.Port != "8090" {
		t.Errorf("port = %q, want 8090", cfg.Server.Port)
	}
	if cfg.Database.DSN == "" {
		t.Errorf("dsn must not be empty")
	}
	if cfg.Sub2API.AdminAPIKey != "test-key" {
		t.Errorf("admin_api_key = %q, want test-key", cfg.Sub2API.AdminAPIKey)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	// Only required settings are present; host, port, and base_url use defaults.
	cfg, err := loadConfig(writeTempConfig(t, `
database:
  dsn: "postgres://sub2api:pass@postgres:5432/sub2api?sslmode=disable"
sub2api:
  admin_api_key: "test-key"
`))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.Server.Host != "127.0.0.1" {
		t.Errorf("host default = %q, want 127.0.0.1", cfg.Server.Host)
	}
	if cfg.Server.Port != "8090" {
		t.Errorf("port default = %q, want 8090", cfg.Server.Port)
	}
	if cfg.Sub2API.BaseURL != "http://sub2api:8080" {
		t.Errorf("base_url default = %q, want http://sub2api:8080", cfg.Sub2API.BaseURL)
	}
}

func TestLoadConfigMissingDSN(t *testing.T) {
	_, err := loadConfig(writeTempConfig(t, `
sub2api:
  admin_api_key: "test-key"
`))
	if err == nil {
		t.Fatal("expected error for missing database.dsn, got nil")
	}
	if !strings.Contains(err.Error(), "database.dsn") {
		t.Errorf("error %q does not mention database.dsn", err.Error())
	}
}

func TestLoadConfigMissingAdminAPIKey(t *testing.T) {
	_, err := loadConfig(writeTempConfig(t, `
database:
  dsn: "postgres://sub2api:pass@postgres:5432/sub2api?sslmode=disable"
`))
	if err == nil {
		t.Fatal("expected error for missing sub2api.admin_api_key, got nil")
	}
	if !strings.Contains(err.Error(), "admin_api_key") {
		t.Errorf("error %q does not mention admin_api_key", err.Error())
	}
}

func TestLoadConfigInvalidPort(t *testing.T) {
	for _, port := range []string{"abc", "-1", "0", "70000", "65536"} {
		_, err := loadConfig(writeTempConfig(t, `
server:
  port: "`+port+`"
database:
  dsn: "postgres://sub2api:pass@postgres:5432/sub2api?sslmode=disable"
sub2api:
  admin_api_key: "test-key"
`))
		if err == nil {
			t.Errorf("port %q: expected error, got nil", port)
			continue
		}
		if !strings.Contains(err.Error(), "server.port") {
			t.Errorf("port %q: error %q does not mention server.port", port, err.Error())
		}
	}
}

func TestValidateConfigNil(t *testing.T) {
	if err := validateConfig(nil); err == nil {
		t.Fatal("validateConfig(nil) should error")
	}
}

func TestValidateConfigNonNumericPortDirect(t *testing.T) {
	cfg := &Config{}
	cfg.Database.DSN = "postgres://x"
	cfg.Sub2API.BaseURL = "http://sub2api:8080"
	cfg.Sub2API.AdminAPIKey = "k"
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = "not-a-number"
	if err := validateConfig(cfg); err == nil {
		t.Fatal("validateConfig should reject non-numeric port")
	}
}

func TestValidateConfigRejectsInvalidBaseURL(t *testing.T) {
	cfg := &Config{}
	cfg.Database.DSN = "postgres://x"
	cfg.Sub2API.BaseURL = "sub2api:8080"
	cfg.Sub2API.AdminAPIKey = "k"
	cfg.Server.Host = "127.0.0.1"
	cfg.Server.Port = "8090"
	if err := validateConfig(cfg); err == nil || !strings.Contains(err.Error(), "sub2api.base_url") {
		t.Fatalf("validateConfig invalid base URL error = %v", err)
	}
}
