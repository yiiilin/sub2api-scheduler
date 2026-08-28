package main

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

func yamlUnmarshal(data []byte, v any) error {
	return yaml.Unmarshal(data, v)
}

// Config 服务配置
type Config struct {
	Server struct {
		Host string `yaml:"host"` // 监听地址，默认 127.0.0.1（仅本机可访问）
		Port string `yaml:"port"`
	} `yaml:"server"`
	Database struct {
		DSN string `yaml:"dsn"`
	} `yaml:"database"`
	Sub2API struct {
		BaseURL     string `yaml:"base_url"`      // sub2api 内部 API 地址，用于 iframe token 鉴权
		AdminAPIKey string `yaml:"admin_api_key"` // 管理 API Key（服务端操作调度用，不依赖用户 token）
	} `yaml:"sub2api"`
	// 旧版 config.yaml rules 已废弃：规则现在存 DB（switch_rules 表），页面可创建/编辑
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := yamlUnmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if cfg.Server.Host == "" {
		cfg.Server.Host = "127.0.0.1"
	}
	cfg.Server.Host = strings.TrimSpace(cfg.Server.Host)
	if cfg.Server.Port == "" {
		cfg.Server.Port = "8090"
	}
	cfg.Server.Port = strings.TrimSpace(cfg.Server.Port)
	if cfg.Sub2API.BaseURL == "" {
		cfg.Sub2API.BaseURL = "http://sub2api:8080"
	}
	cfg.Sub2API.BaseURL = strings.TrimSpace(cfg.Sub2API.BaseURL)
	cfg.Database.DSN = strings.TrimSpace(cfg.Database.DSN)
	cfg.Sub2API.AdminAPIKey = strings.TrimSpace(cfg.Sub2API.AdminAPIKey)
	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}
	log.Printf("[config] loaded (rules are managed via DB switch_rules table)")
	return &cfg, nil
}

// validateConfig 启动期 fail-fast：缺必填项或端口非法时直接报错，
// 避免熔断器/管理 API 因空 key 静默失效、HTTP 因非法端口无法启动。
func validateConfig(cfg *Config) error {
	if cfg == nil {
		return fmt.Errorf("config: empty config")
	}
	if strings.TrimSpace(cfg.Database.DSN) == "" {
		return fmt.Errorf("config: database.dsn is required")
	}
	if strings.TrimSpace(cfg.Sub2API.AdminAPIKey) == "" {
		return fmt.Errorf("config: sub2api.admin_api_key is required")
	}
	baseURL, err := url.ParseRequestURI(strings.TrimSpace(cfg.Sub2API.BaseURL))
	if err != nil || baseURL.Host == "" || (baseURL.Scheme != "http" && baseURL.Scheme != "https") {
		return fmt.Errorf("config: sub2api.base_url %q must be an absolute http(s) URL", cfg.Sub2API.BaseURL)
	}
	if _, err := strconv.Atoi(cfg.Server.Port); err != nil {
		return fmt.Errorf("config: server.port %q is not a valid port: %v", cfg.Server.Port, err)
	}
	if port, _ := strconv.Atoi(cfg.Server.Port); port < 1 || port > 65535 {
		return fmt.Errorf("config: server.port %q out of range (1-65535)", cfg.Server.Port)
	}
	if cfg.Server.Host == "" {
		return fmt.Errorf("config: server.host is required")
	}
	return nil
}
