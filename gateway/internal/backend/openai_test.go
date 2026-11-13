package backend

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Zanthium09/forge-serve/gateway/internal/backend/backendtest"
)

// recSink records what would reach the client.
type recSink struct {
	started     bool
	contentType string
	events      []string
	failWrites  bool
}

func (s *recSink) Start(ct string) { s.started, s.contentType = true, ct }
func (s *recSink) Write(p []byte) error {
	if s.failWrites {
		return errors.New("client gone")
	}
	s.events = append(s.events, string(p))
	return nil
}

func TestOpenAINonStreamDeliversBodyAndUsage(t *testing.T) {
	f := &backendtest.Fake{}
	srv := f.Server(t)
	body := []byte(`{"model":"m","messages":[]}`)
	sink := &recSink{}
	res, err := NewOpenAI(srv.URL).Forward(context.Background(),
		&ProxyRequest{Endpoint: EndpointChat, Body: body}, sink)
	if err != nil {
		t.Fatal(err)
	}
	if sink.contentType != "application/json" || len(sink.events) != 1 || sink.events[0] != backendtest.DefaultBody {
		t.Fatalf("sink = %+v", sink)
	}
	if res.Sent != 1 || res.Usage != (Usage{PromptTokens: 5, CompletionTokens: 2}) {
		t.Fatalf("res = %+v", res)
	}
	if string(f.LastBody()) != string(body) || f.LastPath() != "/v1/chat/completions" {
		t.Fatalf("backend saw %q at %q", f.LastBody(), f.LastPath())
	}
}

func TestOpenAIEmbeddingsUsesPromptTokens(t *testing.T) {
	f := &backendtest.Fake{Body: `{"object":"list","data":[],"usage":{"prompt_tokens":7,"total_tokens":7}}`}
	srv := f.Server(t)
	res, err := NewOpenAI(srv.URL).Forward(context.Background(),
		&ProxyRequest{Endpoint: EndpointEmbeddings, Body: []byte(`{"input":"a"}`)}, &recSink{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Usage.PromptTokens != 7 || res.Usage.CompletionTokens != 0 {
		t.Fatalf("usage = %+v", res.Usage)
	}
	if f.LastPath() != "/v1/embeddings" {
		t.Fatalf("path = %q", f.LastPath())
	}
}

func TestOpenAI4xxIsStatusErrorAndNothingWritten(t *testing.T) {
	f := &backendtest.Fake{Status: 400, Body: `{"error":{"message":"too long"}}`}
	srv := f.Server(t)
	sink := &recSink{}
	_, err := NewOpenAI(srv.URL).Forward(context.Background(),
		&ProxyRequest{Endpoint: EndpointChat, Body: []byte(`{}`)}, sink)
	var se *StatusError
	if !errors.As(err, &se) || se.Code != 400 || !strings.Contains(string(se.Body), "too long") {
		t.Fatalf("err = %v", err)
	}
	if sink.started {
		t.Fatal("sink must not start on a backend error")
	}
}

func TestOpenAI429IsErrOverloaded(t *testing.T) {
	f := &backendtest.Fake{Status: 429}
	srv := f.Server(t)
	_, err := NewOpenAI(srv.URL).Forward(context.Background(),
		&ProxyRequest{Endpoint: EndpointChat, Body: []byte(`{}`)}, &recSink{})
	if !errors.Is(err, ErrOverloaded) {
		t.Fatalf("err = %v, want ErrOverloaded", err)
	}
}

func TestOpenAIConnectionRefusedIsRetryable(t *testing.T) {
	f := &backendtest.Fake{}
	srv := f.Server(t)
	srv.Close()
	res, err := NewOpenAI(srv.URL).Forward(context.Background(),
		&ProxyRequest{Endpoint: EndpointChat, Body: []byte(`{}`)}, &recSink{})
	var se *StatusError
	if err == nil || errors.As(err, &se) || errors.Is(err, ErrOverloaded) || res.Sent != 0 {
		t.Fatalf("err = %v res = %+v", err, res)
	}
}

func TestOpenAILoadHealthOnly(t *testing.T) {
	f := &backendtest.Fake{}
	srv := f.Server(t)
	l, err := NewOpenAI(srv.URL).Load(context.Background())
	if err != nil || l.Known {
		t.Fatalf("load = %+v err = %v", l, err)
	}
	srv.Close()
	if _, err := NewOpenAI(srv.URL).Load(context.Background()); err == nil {
		t.Fatal("expected error from a closed server")
	}
}
