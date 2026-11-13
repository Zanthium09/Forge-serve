package backend

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// maxErrorBody bounds how much of a backend error reply is kept.
const maxErrorBody = 1 << 20

// OpenAI forwards to any OpenAI-compatible engine (vLLM, SGLang, TGI,
// Ollama, llama.cpp server). Its Load is health-only.
type OpenAI struct {
	base string
	// No total timeout: generation legitimately takes minutes, and
	// cancellation comes from the request context (ADR-002).
	hc *http.Client
}

func NewOpenAI(baseURL string) *OpenAI {
	return &OpenAI{base: strings.TrimRight(baseURL, "/"), hc: &http.Client{}}
}

type usageJSON struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
}

func (o *OpenAI) Forward(ctx context.Context, r *ProxyRequest, sink Sink) (Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		o.base+r.Endpoint.Path(), bytes.NewReader(r.Body))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := o.hc.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("backend request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusTooManyRequests {
		return Result{}, ErrOverloaded
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return Result{}, &StatusError{Code: resp.StatusCode, Body: b}
	}
	if r.Stream {
		return relaySSE(resp.Body, r.StripUsageChunk, sink)
	}
	return o.deliverJSON(resp.Body, sink)
}

// sseChunk is the part of a stream chunk the gateway reads; everything else
// is relayed byte-for-byte.
type sseChunk struct {
	Choices []struct {
		Delta struct {
			Content   string          `json:"content"`
			ToolCalls json.RawMessage `json:"tool_calls"`
		} `json:"delta"`
		Text string `json:"text"`
	} `json:"choices"`
	Usage *usageJSON `json:"usage"`
}

func (c *sseChunk) hasContent() bool {
	for _, ch := range c.Choices {
		if ch.Delta.Content != "" || ch.Text != "" || len(ch.Delta.ToolCalls) > 0 {
			return true
		}
	}
	return false
}

// relaySSE streams the backend's events to the client. Events before the
// first content chunk are held back (spec §6.3): until real content has
// been written, nothing has reached the client and a failure is retryable.
func relaySSE(body io.Reader, stripUsage bool, sink Sink) (Result, error) {
	var res Result
	var held [][]byte
	started, sawUsage, sawDone := false, false, false

	start := func() error {
		if started {
			return nil
		}
		started = true
		sink.Start("text/event-stream")
		for _, h := range held {
			if err := sink.Write(h); err != nil {
				return err
			}
		}
		held = nil
		return nil
	}

	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(line[len("data:"):])
		ev := append(append([]byte("data: "), payload...), "\n\n"...)

		if string(payload) == "[DONE]" {
			sawDone = true
			if err := start(); err != nil {
				return res, err
			}
			if err := sink.Write(ev); err != nil {
				return res, err
			}
			break
		}

		var c sseChunk
		_ = json.Unmarshal(payload, &c)
		content := c.hasContent()
		if c.Usage != nil {
			sawUsage = true
			res.Usage.PromptTokens = c.Usage.PromptTokens
			res.Usage.CompletionTokens = c.Usage.CompletionTokens
			if stripUsage && !content {
				continue
			}
		}
		if !content && !started {
			held = append(held, ev)
			continue
		}
		if err := start(); err != nil {
			return res, err
		}
		if err := sink.Write(ev); err != nil {
			return res, err
		}
		if content {
			res.Sent++
		}
	}
	if err := sc.Err(); err != nil {
		return res, fmt.Errorf("backend stream: %w", err)
	}
	if !sawDone {
		// A stream that stops without [DONE] is a dead engine, not a
		// finished answer.
		return res, fmt.Errorf("backend stream ended without [DONE]")
	}
	if !sawUsage {
		res.Usage = Usage{CompletionTokens: res.Sent, Estimated: true}
	}
	return res, nil
}

// deliverJSON buffers the whole reply before the client sees any of it, so
// a failure mid-read is still retryable.
func (o *OpenAI) deliverJSON(body io.Reader, sink Sink) (Result, error) {
	b, err := io.ReadAll(body)
	if err != nil {
		return Result{}, fmt.Errorf("read backend reply: %w", err)
	}
	var parsed struct {
		Usage *usageJSON `json:"usage"`
	}
	_ = json.Unmarshal(b, &parsed)
	u := Usage{Estimated: true}
	if parsed.Usage != nil {
		u = Usage{PromptTokens: parsed.Usage.PromptTokens, CompletionTokens: parsed.Usage.CompletionTokens}
	}
	sink.Start("application/json")
	if err := sink.Write(b); err != nil {
		return Result{}, err
	}
	return Result{Sent: 1, Usage: u}, nil
}

// Load is health-only: any 200 from /v1/models means alive.
func (o *OpenAI) Load(ctx context.Context) (Load, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.base+"/v1/models", nil)
	if err != nil {
		return Load{}, err
	}
	resp, err := o.hc.Do(req)
	if err != nil {
		return Load{}, err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Load{}, fmt.Errorf("backend /v1/models returned %d", resp.StatusCode)
	}
	return Load{}, nil
}
