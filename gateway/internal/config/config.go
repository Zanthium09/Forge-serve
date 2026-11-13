// Package config loads the gateway's YAML configuration (ADR-011).
//
// The file describes models and their backends; secrets never live here,
// they come from environment variables so the file can be committed and
// reviewed.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Kind names the protocol/probe family of a backend.
type Kind string

var validKinds = map[Kind]bool{
	"vllm": true, "sglang": true, "tgi": true, "ollama": true, "openai": true, "forge": true,
}

type Config struct {
	Listen       string        `yaml:"listen"`
	AuthCacheTTL time.Duration `yaml:"auth_cache_ttl"`
	MaxBodyBytes int64         `yaml:"max_body_bytes"`
	Defaults     UserDefaults  `yaml:"defaults"`
	Models       []Model       `yaml:"models"`
}

// UserDefaults apply to new users. Zero budgets/TPM mean unlimited.
type UserDefaults struct {
	RPMBurst           int     `yaml:"rpm_burst"`
	RPMRefillPerSec    float64 `yaml:"rpm_refill_per_sec"`
	TokensPerMinute    int     `yaml:"tokens_per_minute"`
	DailyTokenBudget   int64   `yaml:"daily_token_budget"`
	MonthlyTokenBudget int64   `yaml:"monthly_token_budget"`
	Priority           int     `yaml:"priority"`
}

type Model struct {
	Name             string    `yaml:"name"`
	UpstreamModel    string    `yaml:"upstream_model"`
	Restricted       bool      `yaml:"restricted"`
	DefaultMaxTokens int       `yaml:"default_max_tokens"`
	Fallback         string    `yaml:"fallback"`
	Backends         []Backend `yaml:"backends"`
}

type Backend struct {
	URL              string `yaml:"url"`
	Kind             Kind   `yaml:"kind"`
	MaxWaiting       int    `yaml:"max_waiting"`
	MaxInflight      int    `yaml:"max_inflight"`
	SupportsPriority bool   `yaml:"supports_priority"`
}

// Upstream is the model name sent to the engine.
func (m Model) Upstream() string {
	if m.UpstreamModel != "" {
		return m.UpstreamModel
	}
	return m.Name
}

// Load reads, defaults and validates a config file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// FromWorkerURLs is the WORKER_URLS shorthand: one model served by our own
// workers, so pre-config deployments keep working.
func FromWorkerURLs(urls []string, model string) *Config {
	c := &Config{Models: []Model{{Name: model}}}
	for _, u := range urls {
		c.Models[0].Backends = append(c.Models[0].Backends,
			Backend{URL: strings.TrimSpace(u), Kind: "forge"})
	}
	c.applyDefaults()
	return c
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.AuthCacheTTL == 0 {
		c.AuthCacheTTL = 30 * time.Second
	}
	if c.MaxBodyBytes == 0 {
		c.MaxBodyBytes = 1 << 20
	}
	if c.Defaults.RPMBurst == 0 {
		c.Defaults.RPMBurst = 60
	}
	if c.Defaults.RPMRefillPerSec == 0 {
		c.Defaults.RPMRefillPerSec = 10
	}
	for i := range c.Models {
		m := &c.Models[i]
		if m.DefaultMaxTokens == 0 {
			m.DefaultMaxTokens = 256
		}
		for j := range m.Backends {
			if m.Backends[j].MaxInflight == 0 {
				m.Backends[j].MaxInflight = 64
			}
		}
	}
}

// Validate rejects configs that would route nowhere or loop.
func (c *Config) Validate() error {
	if len(c.Models) == 0 {
		return fmt.Errorf("config: no models configured")
	}
	names := map[string]bool{}
	for _, m := range c.Models {
		if names[m.Name] {
			return fmt.Errorf("config: duplicate model %q", m.Name)
		}
		names[m.Name] = true
	}
	for _, m := range c.Models {
		if m.Name == "" {
			return fmt.Errorf("config: model with empty name")
		}
		if len(m.Backends) == 0 {
			return fmt.Errorf("config: model %q has no backends", m.Name)
		}
		for _, b := range m.Backends {
			if b.URL == "" {
				return fmt.Errorf("config: model %q has a backend with empty url", m.Name)
			}
			if !validKinds[b.Kind] {
				return fmt.Errorf("config: model %q has backend of unknown kind %q", m.Name, b.Kind)
			}
		}
		if m.Fallback == m.Name {
			return fmt.Errorf("config: model %q falls back to itself", m.Name)
		}
		if m.Fallback != "" && !names[m.Fallback] {
			return fmt.Errorf("config: model %q falls back to unknown model %q", m.Name, m.Fallback)
		}
	}
	return nil
}
