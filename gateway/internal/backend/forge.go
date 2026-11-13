package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/worker"
)

// Forge adapts our own continuous-batching worker (NDJSON, ADR-002) to the
// OpenAI wire format, so the MVP engine stays a supported backend. It only
// implements chat.
type Forge struct{ c *worker.Client }

func NewForge(baseURL string) *Forge { return &Forge{c: worker.New(baseURL)} }

type forgeChunk struct {
	ID      string        `json:"id"`
	Object  string        `json:"object"`
	Created int64         `json:"created"`
	Model   string        `json:"model"`
	Choices []forgeChoice `json:"choices"`
	Usage   *forgeUsage   `json:"usage,omitempty"`
}

type forgeChoice struct {
	Index        int        `json:"index"`
	Delta        *forgeText `json:"delta,omitempty"`
	Message      *forgeMsg  `json:"message,omitempty"`
	FinishReason *string    `json:"finish_reason"`
}

type forgeText struct {
	Content string `json:"content,omitempty"`
}

type forgeMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type forgeUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

func (f *Forge) Forward(ctx context.Context, r *ProxyRequest, sink Sink) (Result, error) {
	if r.Endpoint != EndpointChat {
		return Result{}, ErrUnsupported
	}
	var in struct {
		Model       string           `json:"model"`
		Messages    []worker.Message `json:"messages"`
		Temperature *float64         `json:"temperature"`
		MaxTokens   *int             `json:"max_tokens"`
		Stream      struct {
			IncludeUsage bool `json:"include_usage"`
		} `json:"stream_options"`
	}
	if err := json.Unmarshal(r.Body, &in); err != nil {
		return Result{}, &StatusError{Code: 400, Body: []byte(`{"error":{"message":"invalid request body"}}`)}
	}
	req := worker.Request{Messages: in.Messages, Temperature: 0.7, MaxTokens: 256}
	if in.Temperature != nil {
		req.Temperature = *in.Temperature
	}
	if in.MaxTokens != nil {
		req.MaxTokens = *in.MaxTokens
	}
	id := fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
	base := forgeChunk{ID: id, Created: time.Now().Unix(), Model: in.Model}

	if !r.Stream {
		return f.aggregate(ctx, req, base, sink)
	}

	var res Result
	var writeErr error
	started := false
	emit := func(c forgeChunk) bool {
		b, _ := json.Marshal(c)
		if !started {
			started = true
			sink.Start("text/event-stream")
		}
		if err := sink.Write([]byte("data: " + string(b) + "\n\n")); err != nil {
			writeErr = err
			return false
		}
		return true
	}
	base.Object = "chat.completion.chunk"
	err := f.c.Generate(ctx, req, func(ev worker.Event) bool {
		c := base
		if !ev.Done {
			c.Choices = []forgeChoice{{Delta: &forgeText{Content: ev.Token}}}
			if !emit(c) {
				return false
			}
			res.Sent++
			return true
		}
		res.Usage = Usage{PromptTokens: ev.PromptTokens, CompletionTokens: ev.CompletionTokens}
		fr := ev.FinishReason
		c.Choices = []forgeChoice{{Delta: &forgeText{}, FinishReason: &fr}}
		if !emit(c) {
			return false
		}
		if in.Stream.IncludeUsage && !r.StripUsageChunk {
			u := base
			u.Choices = []forgeChoice{}
			u.Usage = &forgeUsage{ev.PromptTokens, ev.CompletionTokens, ev.PromptTokens + ev.CompletionTokens}
			if !emit(u) {
				return false
			}
		}
		if err := sink.Write([]byte("data: [DONE]\n\n")); err != nil {
			writeErr = err
			return false
		}
		return true
	})
	if writeErr != nil {
		return res, writeErr
	}
	return res, translate(err)
}

// aggregate buffers the whole answer, so any failure stays retryable.
func (f *Forge) aggregate(ctx context.Context, req worker.Request, base forgeChunk, sink Sink) (Result, error) {
	var sb strings.Builder
	var final worker.Event
	err := f.c.Generate(ctx, req, func(ev worker.Event) bool {
		if ev.Done {
			final = ev
		} else {
			sb.WriteString(ev.Token)
		}
		return true
	})
	if err != nil {
		return Result{}, translate(err)
	}
	fr := final.FinishReason
	base.Object = "chat.completion"
	base.Choices = []forgeChoice{{Message: &forgeMsg{Role: "assistant", Content: sb.String()}, FinishReason: &fr}}
	base.Usage = &forgeUsage{final.PromptTokens, final.CompletionTokens, final.PromptTokens + final.CompletionTokens}
	b, _ := json.Marshal(base)
	sink.Start("application/json")
	if err := sink.Write(b); err != nil {
		return Result{}, err
	}
	return Result{Sent: 1, Usage: Usage{PromptTokens: final.PromptTokens, CompletionTokens: final.CompletionTokens}}, nil
}

func translate(err error) error {
	if errors.Is(err, worker.ErrOverloaded) {
		return ErrOverloaded
	}
	return err
}

func (f *Forge) Load(ctx context.Context) (Load, error) {
	st, err := f.c.Stats(ctx)
	if err != nil {
		return Load{}, err
	}
	return Load{Waiting: st.QueueDepth, Running: st.ActiveSeqs, Known: true}, nil
}
