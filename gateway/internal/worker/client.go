// Package worker is the gateway-side client for the inference worker's
// NDJSON streaming protocol (ADR-002).
package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

type Client struct {
	baseURL string
	hc      *http.Client
}

// New returns a client for a worker at baseURL (no trailing slash).
// No timeout on the http.Client: generation legitimately takes minutes;
// cancellation comes from ctx, not a wall clock.
func New(baseURL string) *Client {
	return &Client{baseURL: baseURL, hc: &http.Client{}}
}

type Request struct {
	Messages    []Message `json:"messages"`
	Temperature float64   `json:"temperature"`
	MaxTokens   int       `json:"max_tokens"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Event struct {
	Token            string `json:"token"`
	Done             bool   `json:"done"`
	PromptTokens     int    `json:"prompt_tokens"`
	CompletionTokens int    `json:"completion_tokens"`
	FinishReason     string `json:"finish_reason"`
}

// Generate POSTs req and calls fn once per NDJSON event, in order.
// fn returning false stops reading (used when the client disconnects).
// Cancelling ctx aborts the HTTP request, which stops the worker's
// generation — the cancellation path from client to model.
func (c *Client) Generate(ctx context.Context, req Request, fn func(Event) bool) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/generate", bytes.NewReader(body))
	if err != nil {
		return err
	}
	hreq.Header.Set("Content-Type", "application/json")

	resp, err := c.hc.Do(hreq)
	if err != nil {
		return fmt.Errorf("worker request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("worker returned %d", resp.StatusCode)
	}

	sc := bufio.NewScanner(resp.Body)
	// Default scanner cap is 64KB/line; tokens are tiny but be safe.
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var ev Event
		if err := json.Unmarshal(line, &ev); err != nil {
			return fmt.Errorf("bad worker event %q: %w", line, err)
		}
		if !fn(ev) {
			return nil
		}
		if ev.Done {
			return nil
		}
	}
	return sc.Err()
}
