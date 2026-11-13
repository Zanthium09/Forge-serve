package backend

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/backend/backendtest"
)

func ev(payload string) string { return "data: " + payload + "\n\n" }

func stream(t *testing.T, f *backendtest.Fake, ep Endpoint, body string, strip bool) (*recSink, Result, error) {
	t.Helper()
	srv := f.Server(t)
	sink := &recSink{}
	res, err := NewOpenAI(srv.URL).Forward(context.Background(),
		&ProxyRequest{Endpoint: ep, Body: []byte(body), Stream: true, StripUsageChunk: strip}, sink)
	return sink, res, err
}

const streamBody = `{"stream":true}`
const streamUsageBody = `{"stream":true,"stream_options":{"include_usage":true}}`

func TestStreamHoldsRoleChunkUntilContent(t *testing.T) {
	f := &backendtest.Fake{Chunks: []string{backendtest.RoleChunk(), backendtest.ChatChunk("Hi"), backendtest.ChatChunk("!")}}
	sink, res, err := stream(t, f, EndpointChat, streamBody, false)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{ev(backendtest.RoleChunk()), ev(backendtest.ChatChunk("Hi")), ev(backendtest.ChatChunk("!")), ev("[DONE]")}
	if strings.Join(sink.events, "|") != strings.Join(want, "|") {
		t.Fatalf("events = %q", sink.events)
	}
	if res.Sent != 2 || sink.contentType != "text/event-stream" {
		t.Fatalf("sent = %d ct = %q", res.Sent, sink.contentType)
	}
}

func TestStreamDiesBeforeContentWritesNothing(t *testing.T) {
	f := &backendtest.Fake{Chunks: []string{backendtest.RoleChunk(), backendtest.ChatChunk("x")}, DieAfter: 1}
	sink, res, err := stream(t, f, EndpointChat, streamBody, false)
	if err == nil || sink.started || res.Sent != 0 {
		t.Fatalf("err = %v started = %v sent = %d", err, sink.started, res.Sent)
	}
}

func TestStreamDiesAfterContentReportsSent(t *testing.T) {
	f := &backendtest.Fake{Chunks: []string{backendtest.ChatChunk("a"), backendtest.ChatChunk("b"), backendtest.ChatChunk("c")}, DieAfter: 2}
	_, res, err := stream(t, f, EndpointChat, streamBody, false)
	if err == nil || res.Sent != 2 {
		t.Fatalf("err = %v sent = %d", err, res.Sent)
	}
}

func TestStreamStripsInjectedUsageChunk(t *testing.T) {
	f := &backendtest.Fake{Chunks: []string{backendtest.ChatChunk("a"), backendtest.ChatChunk("b")}}
	sink, res, err := stream(t, f, EndpointChat, streamUsageBody, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range sink.events {
		if strings.Contains(e, `"usage"`) {
			t.Fatalf("usage chunk leaked to client: %q", e)
		}
	}
	if res.Usage != (Usage{PromptTokens: 5, CompletionTokens: 2}) {
		t.Fatalf("usage = %+v", res.Usage)
	}
}

func TestStreamKeepsUsageChunkWhenClientAskedForIt(t *testing.T) {
	f := &backendtest.Fake{Chunks: []string{backendtest.ChatChunk("a")}}
	sink, _, err := stream(t, f, EndpointChat, streamUsageBody, false)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range sink.events {
		if strings.Contains(e, `"usage"`) {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("usage events = %d, want 1", n)
	}
}

func TestStreamWithoutUsageIsEstimated(t *testing.T) {
	f := &backendtest.Fake{OmitUsage: true, Chunks: []string{backendtest.ChatChunk("a"), backendtest.ChatChunk("b"), backendtest.ChatChunk("c")}}
	_, res, err := stream(t, f, EndpointChat, streamUsageBody, true)
	if err != nil {
		t.Fatal(err)
	}
	if res.Usage != (Usage{CompletionTokens: 3, Estimated: true}) {
		t.Fatalf("usage = %+v", res.Usage)
	}
}

func TestStreamCompletionsTextCountsAsContent(t *testing.T) {
	f := &backendtest.Fake{Chunks: []string{`{"choices":[{"text":"ok"}]}`}}
	_, res, err := stream(t, f, EndpointCompletions, streamBody, false)
	if err != nil || res.Sent != 1 {
		t.Fatalf("err = %v sent = %d", err, res.Sent)
	}
}

func TestStreamClientCancelStopsBackend(t *testing.T) {
	chunks := make([]string, 20)
	for i := range chunks {
		chunks[i] = backendtest.ChatChunk("x")
	}
	f := &backendtest.Fake{Chunks: chunks, Delay: 50 * time.Millisecond}
	srv := f.Server(t)
	ctx, cancel := context.WithCancel(context.Background())
	sink := &cancelSink{cancel: cancel}
	start := time.Now()
	_, err := NewOpenAI(srv.URL).Forward(ctx,
		&ProxyRequest{Endpoint: EndpointChat, Body: []byte(streamBody), Stream: true}, sink)
	if err == nil {
		t.Fatal("expected an error after cancellation")
	}
	if d := time.Since(start); d > 400*time.Millisecond {
		t.Fatalf("Forward took %v after cancel", d)
	}
}

// cancelSink cancels the request context on the first write, like a
// client that disconnects after seeing one token.
type cancelSink struct{ cancel func() }

func (s *cancelSink) Start(string) {}
func (s *cancelSink) Write([]byte) error {
	s.cancel()
	return nil
}
