package backend

import (
	"fmt"

	"github.com/Zanthium09/forge-serve/gateway/internal/config"
)

// New builds a backend for a configured kind. sglang and tgi use the
// health-only probe until their metric names are verified (plan decision).
func New(kind config.Kind, url string) (Backend, error) {
	switch kind {
	case "vllm":
		return &VLLM{NewOpenAI(url)}, nil
	case "sglang", "tgi", "ollama", "openai":
		return NewOpenAI(url), nil
	case "forge":
		return NewForge(url), nil
	default:
		return nil, fmt.Errorf("backend: unknown kind %q", kind)
	}
}
