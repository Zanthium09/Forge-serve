package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Zanthium09/forge-serve/gateway/internal/backend"
)

func TestParseRequiresObjectAndField(t *testing.T) {
	bad := []struct {
		ep   backend.Endpoint
		body string
	}{
		{backend.EndpointChat, `[]`},
		{backend.EndpointChat, `"x"`},
		{backend.EndpointChat, `{}`},
		{backend.EndpointChat, `{"prompt":"a"}`},
		{backend.EndpointEmbeddings, `{"messages":[]}`},
		{backend.EndpointChat, `{not json`},
	}
	for _, c := range bad {
		if _, err := parseRequest(c.ep, []byte(c.body)); err == nil {
			t.Fatalf("%s %s: expected error", c.ep, c.body)
		}
	}
	p, err := parseRequest(backend.EndpointChat, []byte(`{"model":"m","messages":[],"stream":true,"max_tokens":9}`))
	if err != nil || p.Model != "m" || !p.Stream || p.MaxTokens != 9 {
		t.Fatalf("p = %+v err = %v", p, err)
	}
	if _, err := parseRequest(backend.EndpointCompletions, []byte(`{"prompt":"a"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := parseRequest(backend.EndpointEmbeddings, []byte(`{"input":"a"}`)); err != nil {
		t.Fatal(err)
	}
}

func decode(t *testing.T, b []byte) map[string]json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestBuildPassesUnknownFieldsThrough(t *testing.T) {
	in := `{"model":"a","messages":[],"top_p":0.3,"tools":[{"type":"function"}],"seed":7}`
	p, err := parseRequest(backend.EndpointChat, []byte(in))
	if err != nil {
		t.Fatal(err)
	}
	out, injected, err := p.build("up/a", nil)
	if err != nil || injected {
		t.Fatalf("injected = %v err = %v", injected, err)
	}
	m := decode(t, out)
	if string(m["model"]) != `"up/a"` || string(m["top_p"]) != `0.3` ||
		string(m["tools"]) != `[{"type":"function"}]` || string(m["seed"]) != `7` {
		t.Fatalf("out = %s", out)
	}
}

func TestBuildInjectsUsageOnlyForStream(t *testing.T) {
	p, _ := parseRequest(backend.EndpointChat, []byte(`{"messages":[],"stream":true,"stream_options":{"x":1}}`))
	out, injected, _ := p.build("m", nil)
	so := decode(t, decode(t, out)["stream_options"])
	if !injected || string(so["include_usage"]) != "true" || string(so["x"]) != "1" {
		t.Fatalf("out = %s injected = %v", out, injected)
	}

	p, _ = parseRequest(backend.EndpointChat, []byte(`{"messages":[],"stream":true,"stream_options":{"include_usage":true}}`))
	if _, injected, _ = p.build("m", nil); injected {
		t.Fatal("client already asked for usage; nothing to inject")
	}

	p, _ = parseRequest(backend.EndpointChat, []byte(`{"messages":[]}`))
	out, injected, _ = p.build("m", nil)
	if _, ok := decode(t, out)["stream_options"]; ok || injected {
		t.Fatalf("non-stream must not get stream_options: %s", out)
	}
}

func TestBuildStripsClientPriority(t *testing.T) {
	p, _ := parseRequest(backend.EndpointChat, []byte(`{"messages":[],"priority":1}`))
	out, _, _ := p.build("m", nil)
	if _, ok := decode(t, out)["priority"]; ok {
		t.Fatalf("client priority must be stripped: %s", out)
	}
	five := 5
	out, _, _ = p.build("m", &five)
	if string(decode(t, out)["priority"]) != "5" {
		t.Fatalf("priority not set: %s", out)
	}
}

func TestReadBodyTooLarge(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(strings.Repeat("a", 2<<20)))
	_, err := readBody(httptest.NewRecorder(), r, 1<<20)
	var mbe *http.MaxBytesError
	if !errors.As(err, &mbe) {
		t.Fatalf("err = %v, want *http.MaxBytesError", err)
	}
}

func TestResolveModel(t *testing.T) {
	if m, err := resolveModel("", []string{"m"}); err != nil || m != "m" {
		t.Fatalf("single model: %q %v", m, err)
	}
	if _, err := resolveModel("", []string{"a", "b"}); err == nil {
		t.Fatal("no model with several configured must error")
	}
	if m, _ := resolveModel("x", []string{"a"}); m != "x" {
		t.Fatalf("explicit model changed to %q", m)
	}
}

func TestWriteErrorShape(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, 404, "invalid_request_error", "model_not_found", "no such model")
	var body struct {
		Error struct{ Message, Type, Code string } `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if rec.Code != 404 || body.Error.Type != "invalid_request_error" || body.Error.Code != "model_not_found" ||
		rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("got %d %+v", rec.Code, body)
	}
}
