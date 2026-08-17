package main

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

func yamlUnmarshal(data []byte, v any) error {
	return yaml.Unmarshal(data, v)
}

// Config 服务配置
type Config struct {
	Server struct {
		Port string `yaml:"port"`
	} `yaml:"server"`
	Database struct {
		DSN string `yaml:"dsn"`
	} `yaml:"database"`
	Sub2API struct {
		BaseURL     string `yaml:"base_url"`      // sub2api 内部 API 地址，用于 iframe token 鉴权
		AdminAPIKey string `yaml:"admin_api_key"` // 管理 API Key（服务端操作调度用，不依赖用户 token）
	} `yaml:"sub2api"`
	Redis struct {
		Addr     string `yaml:"addr"`     // 如 redis:6379
		Password string `yaml:"password"` // sub2api redis 密码（清 sched:acc 缓存用）
	} `yaml:"redis"`
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
	if cfg.Server.Port == "" {
		cfg.Server.Port = "8090"
	}
	if cfg.Sub2API.BaseURL == "" {
		cfg.Sub2API.BaseURL = "http://sub2api:8080"
	}
	if cfg.Redis.Addr == "" {
		cfg.Redis.Addr = "redis:6379"
	}
	log.Printf("[config] loaded (rules are managed via DB switch_rules table)")
	return &cfg, nil
}
