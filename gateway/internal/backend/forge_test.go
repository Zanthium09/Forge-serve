package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

var forgeHappy = []string{
	`{"token": "Hello", "done": false}`,
	`{"token": " world", "done": false}`,
	`{"done": true, "prompt_tokens": 5, "completion_tokens": 2, "finish_reason": "stop"}`,
}

// forgeWorker speaks our worker's NDJSON protocol (ADR-002).
func forgeWorker(t *testing.T, status int, lines []string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stats" {
			_, _ = fmt.Fprint(w, `{"queue_depth":2,"active_seqs":5,"max_batch":16}`)
			return
		}
		if status != 0 {
			w.WriteHeader(status)
			return
		}
		for _, l := range lines {
			_, _ = fmt.Fprintln(w, l)
		}
	}))
	t.Cleanup(s.Close)
	return s
}

const forgeChat = `{"model":"m","messages":[{"role":"user","content":"hi"}]}`

func TestForgeStreamTranslatesToOpenAIChunks(t *testing.T) {
	s := forgeWorker(t, 0, forgeHappy)
	sink := &recSink{}
	res, err := NewForge(s.URL).Forward(context.Background(),
		&ProxyRequest{Endpoint: EndpointChat, Body: []byte(forgeChat), Stream: true}, sink)
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(sink.events, "")
	for _, want := range []string{`"content":"Hello"`, `"content":" world"`, `"finish_reason":"stop"`, "data: [DONE]"} {
		if !strings.Contains(all, want) {
			t.Fatalf("stream missing %q:\n%s", want, all)
		}
	}
	if sink.contentType != "text/event-stream" || res.Sent != 2 || res.Usage != (Usage{PromptTokens: 5, CompletionTokens: 2}) {
		t.Fatalf("ct = %q res = %+v", sink.contentType, res)
	}
}

func TestForgeNonStream(t *testing.T) {
	s := forgeWorker(t, 0, forgeHappy)
	sink := &recSink{}
	if _, err := NewForge(s.URL).Forward(context.Background(),
		&ProxyRequest{Endpoint: EndpointChat, Body: []byte(forgeChat)}, sink); err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Choices []struct {
			Message struct{ Content string } `json:"message"`
		} `json:"choices"`
		Usage struct {
			TotalTokens int `json:"total_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal([]byte(sink.events[0]), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Choices[0].Message.Content != "Hello world" || resp.Usage.TotalTokens != 7 {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestForgeRejectsEmbeddings(t *testing.T) {
	s := forgeWorker(t, 0, forgeHappy)
	sink := &recSink{}
	_, err := NewForge(s.URL).Forward(context.Background(),
		&ProxyRequest{Endpoint: EndpointEmbeddings, Body: []byte(`{"input":"a"}`)}, sink)
	if !errors.Is(err, ErrUnsupported) || sink.started {
		t.Fatalf("err = %v started = %v", err, sink.started)
	}
}

func TestForgeOverloaded(t *testing.T) {
	s := forgeWorker(t, http.StatusTooManyRequests, nil)
	_, err := NewForge(s.URL).Forward(context.Background(),
		&ProxyRequest{Endpoint: EndpointChat, Body: []byte(forgeChat), Stream: true}, &recSink{})
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("err = %v", err)
	}
}

func TestForgeLoad(t *testing.T) {
	s := forgeWorker(t, 0, nil)
	l, err := NewForge(s.URL).Load(context.Background())
	if err != nil || l != (Load{Waiting: 2, Running: 5, Known: true}) {
		t.Fatalf("load = %+v err = %v", l, err)
	}
}

func TestNewForgeKind(t *testing.T) {
	b, err := New("forge", "http://x")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := b.(*Forge); !ok {
		t.Fatalf("forge kind built %T", b)
	}
}
