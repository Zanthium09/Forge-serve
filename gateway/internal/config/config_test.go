package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "forge.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const minimal = `
models:
  - name: big
    backends:
      - { url: "http://gpu1:8000", kind: vllm }
`

func TestLoadAppliesDefaults(t *testing.T) {
	c, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":8080" || c.AuthCacheTTL != 30*time.Second || c.MaxBodyBytes != 1048576 {
		t.Fatalf("server defaults = %q %v %d", c.Listen, c.AuthCacheTTL, c.MaxBodyBytes)
	}
	m := c.Models[0]
	if m.DefaultMaxTokens != 256 || m.Backends[0].MaxInflight != 64 {
		t.Fatalf("model defaults = %d %d", m.DefaultMaxTokens, m.Backends[0].MaxInflight)
	}
	if c.Defaults.RPMBurst != 60 || c.Defaults.RPMRefillPerSec != 10 {
		t.Fatalf("user defaults = %+v", c.Defaults)
	}
}

func TestLoadParsesDurationAndFallback(t *testing.T) {
	c, err := Load(writeConfig(t, `
auth_cache_ttl: 45s
models:
  - name: big
    fallback: small
    backends: [{ url: "http://gpu1:8000", kind: vllm }]
  - name: small
    backends: [{ url: "http://gpu2:8000", kind: vllm }]
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.AuthCacheTTL != 45*time.Second {
		t.Fatalf("ttl = %v", c.AuthCacheTTL)
	}
	if c.Models[0].Fallback != "small" {
		t.Fatalf("fallback = %q", c.Models[0].Fallback)
	}
}

func TestValidateRejects(t *testing.T) {
	cases := map[string]string{
		"duplicate name": `
models:
  - { name: m, backends: [{ url: "http://a", kind: vllm }] }
  - { name: m, backends: [{ url: "http://b", kind: vllm }] }`,
		"no backends": `
models:
  - { name: m, backends: [] }`,
		"unknown kind": `
models:
  - { name: m, backends: [{ url: "http://a", kind: foo }] }`,
		"empty url": `
models:
  - { name: m, backends: [{ url: "", kind: vllm }] }`,
		"unknown fallback": `
models:
  - { name: m, fallback: nope, backends: [{ url: "http://a", kind: vllm }] }`,
		"self fallback": `
models:
  - { name: m, fallback: m, backends: [{ url: "http://a", kind: vllm }] }`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(writeConfig(t, body))
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), `"m"`) {
				t.Fatalf("error %q does not name the model", err)
			}
		})
	}
}

func TestFromWorkerURLs(t *testing.T) {
	c := FromWorkerURLs([]string{"http://a:9000", " http://b:9000"}, "qwen2.5-1.5b")
	if len(c.Models) != 1 || c.Models[0].Name != "qwen2.5-1.5b" {
		t.Fatalf("models = %+v", c.Models)
	}
	bs := c.Models[0].Backends
	if len(bs) != 2 || bs[1].URL != "http://b:9000" || bs[0].Kind != "forge" {
		t.Fatalf("backends = %+v", bs)
	}
	if c.Listen != ":8080" || bs[0].MaxInflight != 64 {
		t.Fatal("defaults not applied")
	}
}

func TestUpstream(t *testing.T) {
	if (Model{Name: "a"}).Upstream() != "a" {
		t.Fatal("Upstream without upstream_model should be the name")
	}
	if (Model{Name: "a", UpstreamModel: "x"}).Upstream() != "x" {
		t.Fatal("Upstream should prefer upstream_model")
	}
}

// Every config shipped in deploy/ must load and validate.
func TestDeployConfigsLoad(t *testing.T) {
	files, _ := filepath.Glob("../../../deploy/forge*.yaml")
	if len(files) < 2 {
		t.Fatalf("expected deploy configs, found %v", files)
	}
	for _, f := range files {
		if _, err := Load(f); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
}
