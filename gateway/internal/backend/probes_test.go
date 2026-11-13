package backend

import (
	"context"
	"testing"

	"github.com/Zanthium09/forge-serve/gateway/internal/backend/backendtest"
)

const vllmMetrics = `# HELP vllm:num_requests_waiting Number of requests waiting.
# TYPE vllm:num_requests_waiting gauge
vllm:num_requests_waiting{model_name="m"} 3
vllm:num_requests_waiting{model_name="n"} 2
vllm:num_requests_running{model_name="m"} 7.0
vllm:gpu_cache_usage_perc{model_name="m"} 0.4
`

func TestParseVLLMMetrics(t *testing.T) {
	l, err := ParseVLLMMetrics(vllmMetrics)
	if err != nil {
		t.Fatal(err)
	}
	if l != (Load{Waiting: 5, Running: 7, Known: true}) {
		t.Fatalf("load = %+v", l)
	}
}

func TestParseVLLMMetricsMissing(t *testing.T) {
	if _, err := ParseVLLMMetrics("process_cpu_seconds_total 12\n"); err == nil {
		t.Fatal("expected error when vllm metrics are absent")
	}
}

func TestVLLMLoadFromServer(t *testing.T) {
	f := &backendtest.Fake{Metrics: vllmMetrics}
	srv := f.Server(t)
	b, err := New("vllm", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	l, err := b.Load(context.Background())
	if err != nil || !l.Known || l.Waiting != 5 {
		t.Fatalf("load = %+v err = %v", l, err)
	}
}

func TestNewByKind(t *testing.T) {
	if b, err := New("vllm", "http://x"); err != nil {
		t.Fatal(err)
	} else if _, ok := b.(*VLLM); !ok {
		t.Fatalf("vllm kind built %T", b)
	}
	if b, err := New("ollama", "http://x"); err != nil {
		t.Fatal(err)
	} else if _, ok := b.(*OpenAI); !ok {
		t.Fatalf("ollama kind built %T", b)
	}
	if _, err := New("x", "http://x"); err == nil {
		t.Fatal("unknown kind should error")
	}
}
