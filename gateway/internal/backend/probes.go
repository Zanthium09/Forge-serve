package backend

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// VLLM is an OpenAI forwarder whose Load reads vLLM's own queue gauges, so
// admission can use the engine's real waiting count (ADR-010).
type VLLM struct{ *OpenAI }

func (v *VLLM) Load(ctx context.Context) (Load, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.base+"/metrics", nil)
	if err != nil {
		return Load{}, err
	}
	resp, err := v.hc.Do(req)
	if err != nil {
		return Load{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return Load{}, fmt.Errorf("vllm /metrics returned %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return Load{}, err
	}
	return ParseVLLMMetrics(string(b))
}

// ParseVLLMMetrics sums the waiting/running gauges across label sets (one
// set per served model). A plain line scan is enough for two gauges; the
// Prometheus text parser would be a dependency for nothing.
func ParseVLLMMetrics(text string) (Load, error) {
	var l Load
	found := false
	sc := bufio.NewScanner(strings.NewReader(text))
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		name := line
		if i := strings.IndexAny(line, "{ "); i >= 0 {
			name = line[:i]
		}
		var dst *int
		switch name {
		case "vllm:num_requests_waiting":
			dst = &l.Waiting
		case "vllm:num_requests_running":
			dst = &l.Running
		default:
			continue
		}
		fields := strings.Fields(line)
		v, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		if err != nil {
			continue
		}
		*dst += int(v)
		found = true
	}
	if !found {
		return Load{}, fmt.Errorf("vllm queue metrics not found")
	}
	l.Known = true
	return l, nil
}
