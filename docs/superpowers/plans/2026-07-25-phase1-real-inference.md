# Phase 1 — Real Inference, Single Worker: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Real model tokens stream from a Python llama.cpp worker through the Go gateway to any OpenAI client, with API-key auth (Postgres) and first Prometheus metrics.

**Architecture:** New `worker/` FastAPI service (llama-cpp-python high-level API, sequential, lock-guarded) streams NDJSON to the gateway. Gateway grows: OpenAI schema validation, worker client, SSE relay + non-stream aggregation, SHA-256 key auth with 5-min cache backed by Postgres, `/metrics`. Fake worker + fake store in unit tests; real model only in local exit checks.

**Tech Stack:** Go 1.22 (+ `jackc/pgx/v5`, `prometheus/client_golang`), Python 3.11 + FastAPI + uvicorn + llama-cpp-python (Docker only), Postgres 16, docker-compose.

## Global Constraints (from blueprint + spec)

- Worker is sequential this phase (lock). Batch engine is Phase 2 — do not build it now.
- Worker container is NOT exposed to the host; only `worker:9000` on the compose network.
- Dev API key (local only): `sk-forge-dev-key`, seeded via `deploy/postgres-init.sql`. Postgres init scripts run only on first volume creation — use `docker compose down -v` to re-seed.
- `/healthz` and `/metrics` unauthenticated; `/v1/*` requires Bearer key.
- CI: lint + unit tests only; never loads the model.
- Windows: use `curl.exe` in PowerShell; `go` needs `$env:PATH += ";C:\Program Files\Go\bin"` in fresh shells.
- Conventional commits; contexts propagated; *why* comments on non-obvious choices.

**Phase 1 exit criteria:**
1. Real tokens stream through composed stack (`curl.exe -N` with Bearer key).
2. OpenAI Python SDK works unmodified: stream and non-stream.
3. 10 concurrent requests all complete without deadlock.

---

### Task 1: Branch + worker service files

**Files:**
- Create: `worker/server.py`, `worker/config.py`, `worker/requirements.txt`, `worker/Dockerfile`

**Interfaces:**
- Produces: `POST /generate` accepting `{"messages":[{"role","content"}],"temperature":float,"max_tokens":int}`, streaming NDJSON lines `{"token":str,"done":false}` then `{"done":true,"prompt_tokens":int,"completion_tokens":int,"finish_reason":str}`; `GET /healthz` → `{"status":"ok"}`. Task 2's compose and Task 4's Go client consume exactly these shapes.

- [ ] **Step 1: Branch**

```bash
git checkout -b phase-1-real-inference
```

- [ ] **Step 2: Write `worker/config.py`**

```python
"""Worker configuration from environment (12-factor: config lives in env)."""

import os

MODEL_PATH = os.environ.get("MODEL_PATH", "/models/qwen2.5-1.5b-instruct-q4_k_m.gguf")
N_CTX = int(os.environ.get("N_CTX", "4096"))
# 0 = let llama.cpp pick (all physical cores).
N_THREADS = int(os.environ.get("N_THREADS", "0")) or None
```

- [ ] **Step 3: Write `worker/server.py`**

```python
"""Forge-Serve inference worker, Phase 1: sequential generation.

Streams NDJSON (one JSON object per line) over chunked HTTP — see ADR-002.
Deliberately simple: this is the baseline the Phase 2 continuous-batching
engine is benchmarked against.
"""

import json
import threading

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from llama_cpp import Llama
from pydantic import BaseModel

import config

app = FastAPI()
llm = Llama(
    model_path=config.MODEL_PATH,
    n_ctx=config.N_CTX,
    n_threads=config.N_THREADS,
    verbose=False,
)
# llama.cpp contexts are not thread-safe, and Phase 1 is sequential by
# design: one generation at a time; concurrent requests queue on this lock.
gen_lock = threading.Lock()


class Message(BaseModel):
    role: str
    content: str


class GenerateRequest(BaseModel):
    messages: list[Message]
    temperature: float = 0.7
    max_tokens: int = 256


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.post("/generate")
def generate(req: GenerateRequest) -> StreamingResponse:
    def stream():
        with gen_lock:
            # Approximation: the high-level API hides the templated prompt,
            # so we tokenize raw message content. Exact counts arrive in
            # Phase 2 when we own the decode loop.
            prompt_tokens = len(llm.tokenize(
                "".join(m.content for m in req.messages).encode("utf-8")))
            completion_tokens = 0
            finish_reason = "stop"
            for chunk in llm.create_chat_completion(
                messages=[m.model_dump() for m in req.messages],
                temperature=req.temperature,
                max_tokens=req.max_tokens,
                stream=True,
            ):
                choice = chunk["choices"][0]
                if choice.get("finish_reason"):
                    finish_reason = choice["finish_reason"]
                content = choice["delta"].get("content")
                if content:
                    completion_tokens += 1
                    yield json.dumps({"token": content, "done": False}) + "\n"
            yield json.dumps({
                "done": True,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "finish_reason": finish_reason,
            }) + "\n"

    return StreamingResponse(stream(), media_type="application/x-ndjson")
```

- [ ] **Step 4: Write `worker/requirements.txt`**

```
fastapi>=0.110
uvicorn>=0.29
llama-cpp-python>=0.2.90
```

- [ ] **Step 5: Write `worker/Dockerfile`**

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
# Prebuilt CPU wheels from the maintainer's index avoid a 10-minute C++
# compile. If this index ever 404s: apt-get install build-essential cmake,
# drop the extra-index-url, and let pip compile from source.
RUN pip install --no-cache-dir -r requirements.txt \
    --extra-index-url https://abetlen.github.io/llama-cpp-python/whl/cpu
COPY server.py config.py ./
EXPOSE 9000
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "9000"]
```

- [ ] **Step 6: Commit**

```bash
git add worker/
git commit -m "feat(worker): sequential llama.cpp inference service streaming NDJSON"
```

---

### Task 2: Compose wiring + Postgres seed

**Files:**
- Create: `deploy/postgres-init.sql`
- Modify: `docker-compose.yml`

**Interfaces:**
- Produces: `worker` service healthy at `worker:9000` (internal only); gateway env `WORKER_URL`, `DATABASE_URL`; Postgres seeded with tenant `dev` and key `sk-forge-dev-key` (hash in `api_keys.key_hash` as hex). Tasks 5–7 consume these.

- [ ] **Step 1: Write `deploy/postgres-init.sql`**

```sql
-- Runs once, on first boot of a fresh pgdata volume
-- (docker-entrypoint-initdb.d). To re-run: docker compose down -v.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE tenants (
    id   BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE api_keys (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  BIGINT NOT NULL REFERENCES tenants(id),
    -- SHA-256 hex of the raw key. Raw keys are never stored: a leaked DB
    -- yields nothing usable.
    key_hash   TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO tenants (name) VALUES ('dev');

-- Dev-only key for local use: sk-forge-dev-key
INSERT INTO api_keys (tenant_id, key_hash)
SELECT id, encode(digest('sk-forge-dev-key', 'sha256'), 'hex')
FROM tenants WHERE name = 'dev';
```

- [ ] **Step 2: Update `docker-compose.yml`** — replace the whole file:

```yaml
services:
  gateway:
    build: ./gateway
    ports:
      - "8080:8080"
    environment:
      WORKER_URL: http://worker:9000
      DATABASE_URL: postgres://forge:forge@postgres:5432/forge
    depends_on:
      worker:
        condition: service_healthy
      postgres:
        condition: service_started
      redis:
        condition: service_started

  worker:
    build: ./worker
    # No ports: — the gateway is the only public door; the worker is
    # reachable solely as worker:9000 on the compose network.
    volumes:
      - ./models:/models:ro
    environment:
      MODEL_PATH: /models/qwen2.5-1.5b-instruct-q4_k_m.gguf
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:9000/healthz')"]
      interval: 10s
      timeout: 5s
      retries: 12
      start_period: 60s

  # Unused until Phase 4 — present so the quickstart shape never changes.
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: forge
      POSTGRES_PASSWORD: forge
      POSTGRES_DB: forge
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./deploy/postgres-init.sql:/docker-entrypoint-initdb.d/init.sql:ro

volumes:
  pgdata:
```

- [ ] **Step 3: Build the worker image now (it's the slow one) and smoke-test it**

Run: `docker compose build worker`
Expected: image builds; pip resolves llama-cpp-python from the CPU wheel index without compiling.

Run: `docker compose up -d worker && docker compose logs -f worker` (Ctrl+C the logs once you see uvicorn ready; first start loads the 1.1 GB model, allow ~30–60 s)
Expected: `Uvicorn running on http://0.0.0.0:9000`.

Run: `docker compose exec worker python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:9000/healthz').read())"`
Expected: `b'{"status":"ok"}'`.

- [ ] **Step 4: Real generation smoke test (worker only, via exec since no host port)**

```bash
docker compose exec worker python -c "import json,urllib.request; req=urllib.request.Request('http://localhost:9000/generate', data=json.dumps({'messages':[{'role':'user','content':'Say hi in five words'}],'max_tokens':20}).encode(), headers={'Content-Type':'application/json'}); [print(line.decode().strip()) for line in urllib.request.urlopen(req)]"
```
Expected: several `{"token": "...", "done": false}` lines of real model output, final line `{"done": true, ...}`.

- [ ] **Step 5: Recreate Postgres with seed** (destroys Phase 0's empty pgdata — nothing of value in it)

Run: `docker compose down -v && docker compose up -d postgres && sleep 5 && docker compose exec postgres psql -U forge -d forge -c "SELECT t.name, k.key_hash FROM api_keys k JOIN tenants t ON t.id=k.tenant_id;"`
Expected: one row, tenant `dev`, 64-char hex hash.

- [ ] **Step 6: Commit**

```bash
git add deploy/ docker-compose.yml
git commit -m "feat: worker service in compose, postgres key seed"
```

---

### Task 3: Gateway OpenAI schema + validation (TDD)

**Files:**
- Create: `gateway/internal/api/schema.go`
- Test: `gateway/internal/api/schema_test.go`

**Interfaces:**
- Produces: `ChatRequest{Model string; Messages []ChatMessage; Temperature *float64; MaxTokens *int; Stream bool}`, `ChatMessage{Role, Content string}`, method `(r *ChatRequest) Validate() error`, plus non-stream response types `ChatResponse{ID,Object,Created,Model string/int64; Choices []RespChoice; Usage Usage}`, `RespChoice{Index int; Message ChatMessage; FinishReason string}`, `Usage{PromptTokens, CompletionTokens, TotalTokens int}`. Task 5 consumes all of these.

- [ ] **Step 1: Write failing tests** — `gateway/internal/api/schema_test.go`

```go
package api

import "testing"

func TestValidateAcceptsMinimalRequest(t *testing.T) {
	r := ChatRequest{Messages: []ChatMessage{{Role: "user", Content: "hi"}}}
	if err := r.Validate(); err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}
}

func TestValidateRejectsBadRequests(t *testing.T) {
	temp := 3.5
	neg := -1
	cases := []struct {
		name string
		req  ChatRequest
	}{
		{"no messages", ChatRequest{}},
		{"empty role", ChatRequest{Messages: []ChatMessage{{Content: "hi"}}}},
		{"temperature out of range", ChatRequest{
			Messages: []ChatMessage{{Role: "user", Content: "hi"}}, Temperature: &temp}},
		{"negative max_tokens", ChatRequest{
			Messages: []ChatMessage{{Role: "user", Content: "hi"}}, MaxTokens: &neg}},
	}
	for _, c := range cases {
		if err := c.req.Validate(); err == nil {
			t.Errorf("%s: expected error, got nil", c.name)
		}
	}
}
```

- [ ] **Step 2: Run tests, expect FAIL** (`undefined: ChatRequest`)

Run: `cd gateway && go test ./internal/api/`

- [ ] **Step 3: Write `gateway/internal/api/schema.go`**

```go
package api

import "errors"

// OpenAI /v1/chat/completions request — the fields Phase 1 honors.
// Unknown JSON fields are ignored by encoding/json, which matches
// OpenAI's own be-liberal-in-what-you-accept behavior.
type ChatRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Temperature *float64      `json:"temperature"`
	MaxTokens   *int          `json:"max_tokens"`
	Stream      bool          `json:"stream"`
}

type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// Validate enforces the trust boundary: everything past here assumes a
// well-formed request.
func (r *ChatRequest) Validate() error {
	if len(r.Messages) == 0 {
		return errors.New("messages: required, non-empty")
	}
	for _, m := range r.Messages {
		if m.Role == "" {
			return errors.New("messages[].role: required")
		}
	}
	if r.Temperature != nil && (*r.Temperature < 0 || *r.Temperature > 2) {
		return errors.New("temperature: must be in [0, 2]")
	}
	if r.MaxTokens != nil && *r.MaxTokens <= 0 {
		return errors.New("max_tokens: must be positive")
	}
	return nil
}

// Non-stream response (object "chat.completion").
type ChatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []RespChoice `json:"choices"`
	Usage   Usage        `json:"usage"`
}

type RespChoice struct {
	Index        int         `json:"index"`
	Message      ChatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"`
}

type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}
```

- [ ] **Step 4: Run tests, expect PASS**, **commit**

```bash
git add gateway/internal/api/schema.go gateway/internal/api/schema_test.go
git commit -m "feat(gateway): openai chat request schema and validation"
```

---

### Task 4: Worker client in Go (TDD, fake NDJSON server)

**Files:**
- Create: `gateway/internal/worker/client.go`
- Test: `gateway/internal/worker/client_test.go`

**Interfaces:**
- Consumes: worker NDJSON contract from Task 1.
- Produces: `worker.New(baseURL string) *Client`; `(c *Client) Generate(ctx context.Context, req Request, fn func(Event) bool) error` — fn is called per event, return false to stop early; `Request{Messages []Message; Temperature float64; MaxTokens int}`, `Message{Role, Content string}`, `Event{Token string; Done bool; PromptTokens, CompletionTokens int; FinishReason string}`. Task 5 consumes these.

- [ ] **Step 1: Write failing test** — `gateway/internal/worker/client_test.go`

```go
package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func fakeWorker(t *testing.T, lines []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/generate" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, l := range lines {
			fmt.Fprintln(w, l)
		}
	}))
}

func TestGenerateStreamsEvents(t *testing.T) {
	srv := fakeWorker(t, []string{
		`{"token": "Hello", "done": false}`,
		`{"token": " world", "done": false}`,
		`{"done": true, "prompt_tokens": 3, "completion_tokens": 2, "finish_reason": "stop"}`,
	})
	defer srv.Close()

	var tokens []string
	var final Event
	err := New(srv.URL).Generate(context.Background(),
		Request{Messages: []Message{{Role: "user", Content: "hi"}}},
		func(ev Event) bool {
			if ev.Done {
				final = ev
			} else {
				tokens = append(tokens, ev.Token)
			}
			return true
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 || tokens[0] != "Hello" {
		t.Fatalf("tokens = %v", tokens)
	}
	if !final.Done || final.PromptTokens != 3 || final.CompletionTokens != 2 {
		t.Fatalf("final = %+v", final)
	}
}

func TestGenerateStopsWhenCallbackReturnsFalse(t *testing.T) {
	srv := fakeWorker(t, []string{
		`{"token": "a", "done": false}`,
		`{"token": "b", "done": false}`,
		`{"done": true}`,
	})
	defer srv.Close()

	calls := 0
	err := New(srv.URL).Generate(context.Background(), Request{}, func(ev Event) bool {
		calls++
		return false // stop after first event
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestGenerateReturnsErrorOnNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	err := New(srv.URL).Generate(context.Background(), Request{}, func(Event) bool { return true })
	if err == nil {
		t.Fatal("expected error on 500, got nil")
	}
}
```

- [ ] **Step 2: Run, expect FAIL** (`undefined: New`)

Run: `cd gateway && go test ./internal/worker/`

- [ ] **Step 3: Write `gateway/internal/worker/client.go`**

```go
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
```

- [ ] **Step 4: Run, expect PASS**, **commit**

```bash
git add gateway/internal/worker/
git commit -m "feat(gateway): ndjson streaming client for the inference worker"
```

---

### Task 5: Real chat handler — SSE relay + non-stream (TDD)

**Files:**
- Create: `gateway/internal/api/chat.go`
- Delete: `gateway/internal/api/chat_stub.go` (keep the `chunk`/`choice`/`delta` types — they move to `chat.go`)
- Modify: `gateway/internal/api/server.go` (NewMux gains dependencies)
- Test: modify `gateway/internal/api/chat_stub_test.go` → rename content to `gateway/internal/api/chat_test.go`

**Interfaces:**
- Consumes: `worker.Client` (Task 4), schema types (Task 3).
- Produces: `NewMux(w *worker.Client) *http.ServeMux` (Task 7 wires auth+metrics around it; Task 6 changes signature again — final form there). SSE stream: `chunk` events then `data: [DONE]`; non-stream: `ChatResponse` JSON. Handler exits on client disconnect (Phase 0 guarantee preserved).

- [ ] **Step 1: Rewrite tests** — delete `chat_stub_test.go`, create `gateway/internal/api/chat_test.go`

```go
package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/worker"
)

// fakeWorker speaks the NDJSON protocol with a configurable per-line delay.
func fakeWorker(t *testing.T, delay time.Duration, lines []string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f := w.(http.Flusher)
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, l := range lines {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(delay):
			}
			fmt.Fprintln(w, l)
			f.Flush()
		}
	}))
}

var happyLines = []string{
	`{"token": "Hello", "done": false}`,
	`{"token": " world", "done": false}`,
	`{"done": true, "prompt_tokens": 5, "completion_tokens": 2, "finish_reason": "stop"}`,
}

func newTestMux(t *testing.T, workerURL string) http.Handler {
	t.Helper()
	return NewMux(worker.New(workerURL))
}

func TestChatStreamRelaysSSE(t *testing.T) {
	fw := fakeWorker(t, 0, happyLines)
	defer fw.Close()
	srv := httptest.NewServer(newTestMux(t, fw.URL))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}

	var contents []string
	sawDone := false
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			sawDone = true
			break
		}
		var c chunk
		if err := json.Unmarshal([]byte(payload), &c); err != nil {
			t.Fatalf("bad chunk %q: %v", payload, err)
		}
		if s := c.Choices[0].Delta.Content; s != "" {
			contents = append(contents, s)
		}
	}
	got := strings.Join(contents, "")
	if got != "Hello world" || !sawDone {
		t.Fatalf("got %q sawDone=%v", got, sawDone)
	}
}

func TestChatNonStreamAggregates(t *testing.T) {
	fw := fakeWorker(t, 0, happyLines)
	defer fw.Close()
	srv := httptest.NewServer(newTestMux(t, fw.URL))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var cr ChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		t.Fatal(err)
	}
	if cr.Object != "chat.completion" {
		t.Fatalf("object = %q", cr.Object)
	}
	if cr.Choices[0].Message.Content != "Hello world" {
		t.Fatalf("content = %q", cr.Choices[0].Message.Content)
	}
	if cr.Usage.TotalTokens != 7 {
		t.Fatalf("usage = %+v", cr.Usage)
	}
}

func TestChatRejectsInvalidRequest(t *testing.T) {
	srv := httptest.NewServer(newTestMux(t, "http://unused.invalid"))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestChatClientDisconnectCancelsHandlerAndWorker(t *testing.T) {
	// Slow fake worker: 50ms per line, so the client can bail mid-stream.
	fw := fakeWorker(t, 50*time.Millisecond, happyLines)
	defer fw.Close()

	handlerDone := make(chan struct{})
	mux := newTestMux(t, fw.URL)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
		close(handlerDone)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if _, err := resp.Body.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	cancel()

	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit after client disconnect")
	}
}

func TestHealthzStillWorks(t *testing.T) {
	srv := httptest.NewServer(newTestMux(t, "http://unused.invalid"))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
```

Also delete the old `TestHealthz` from `server_test.go` (superseded by `TestHealthzStillWorks`): delete file `gateway/internal/api/server_test.go`.

- [ ] **Step 2: Run, expect FAIL** (`NewMux` signature, `undefined` symbols)

- [ ] **Step 3: Delete `chat_stub.go`, write `gateway/internal/api/chat.go`**

```go
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/worker"
)

// SSE chunk types mirror OpenAI's chat.completion.chunk schema.
type chunk struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []choice `json:"choices"`
}

type choice struct {
	Index        int     `json:"index"`
	Delta        delta   `json:"delta"`
	FinishReason *string `json:"finish_reason"`
}

type delta struct {
	Content string `json:"content,omitempty"`
}

const (
	defaultTemperature = 0.7
	defaultMaxTokens   = 256
)

type chatHandler struct {
	worker *worker.Client
}

func (h *chatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	wreq := worker.Request{
		Temperature: defaultTemperature,
		MaxTokens:   defaultMaxTokens,
	}
	if req.Temperature != nil {
		wreq.Temperature = *req.Temperature
	}
	if req.MaxTokens != nil {
		wreq.MaxTokens = *req.MaxTokens
	}
	for _, m := range req.Messages {
		wreq.Messages = append(wreq.Messages, worker.Message{Role: m.Role, Content: m.Content})
	}

	id := fmt.Sprintf("chatcmpl-%d", time.Now().UnixNano())
	if req.Stream {
		h.stream(w, r, id, req.Model, wreq)
	} else {
		h.aggregate(w, r, id, req.Model, wreq)
	}
}

// stream relays worker tokens as SSE. Client disconnect cancels r.Context(),
// which aborts the worker request — generation stops end to end.
func (h *chatHandler) stream(w http.ResponseWriter, r *http.Request, id, model string, wreq worker.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	sent := 0
	err := h.worker.Generate(r.Context(), wreq, func(ev worker.Event) bool {
		c := chunk{ID: id, Object: "chat.completion.chunk",
			Created: time.Now().Unix(), Model: model}
		if ev.Done {
			fr := ev.FinishReason
			c.Choices = []choice{{FinishReason: &fr}}
		} else {
			c.Choices = []choice{{Delta: delta{Content: ev.Token}}}
			sent++
		}
		b, merr := json.Marshal(c)
		if merr != nil {
			return false
		}
		if _, werr := fmt.Fprintf(w, "data: %s\n\n", b); werr != nil {
			return false // client gone
		}
		flusher.Flush()
		return true
	})
	if err != nil {
		// Headers are already sent; an SSE error event is all we can do.
		slog.Error("worker stream failed", "err", err, "tokens_sent", sent)
		fmt.Fprintf(w, "data: {\"error\": {\"message\": \"upstream failure\"}}\n\n")
		flusher.Flush()
		return
	}
	if r.Context().Err() != nil {
		slog.Info("client disconnected, stream cancelled", "tokens_sent", sent)
		return
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}

// aggregate collects the full worker stream, then answers with one JSON body.
func (h *chatHandler) aggregate(w http.ResponseWriter, r *http.Request, id, model string, wreq worker.Request) {
	var sb strings.Builder
	var final worker.Event
	err := h.worker.Generate(r.Context(), wreq, func(ev worker.Event) bool {
		if ev.Done {
			final = ev
		} else {
			sb.WriteString(ev.Token)
		}
		return true
	})
	if err != nil {
		http.Error(w, "upstream failure", http.StatusBadGateway)
		return
	}
	resp := ChatResponse{
		ID: id, Object: "chat.completion", Created: time.Now().Unix(), Model: model,
		Choices: []RespChoice{{
			Message:      ChatMessage{Role: "assistant", Content: sb.String()},
			FinishReason: final.FinishReason,
		}},
		Usage: Usage{
			PromptTokens:     final.PromptTokens,
			CompletionTokens: final.CompletionTokens,
			TotalTokens:      final.PromptTokens + final.CompletionTokens,
		},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
```

- [ ] **Step 4: Update `gateway/internal/api/server.go`**

```go
// Package api holds the gateway's HTTP surface: health and the
// OpenAI-compatible chat completions endpoint.
package api

import (
	"net/http"

	"github.com/Zanthium09/forge-serve/gateway/internal/worker"
)

// NewMux returns the gateway's routes, wired to the given worker client.
func NewMux(w *worker.Client) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.Handle("POST /v1/chat/completions", &chatHandler{worker: w})
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}
```

- [ ] **Step 5: Fix `main.go` compile** — in `gateway/cmd/gateway/main.go` add import `"github.com/Zanthium09/forge-serve/gateway/internal/worker"` and replace the server line:

```go
	workerURL := os.Getenv("WORKER_URL")
	if workerURL == "" {
		workerURL = "http://localhost:9000"
	}
	srv := &http.Server{Addr: addr, Handler: api.NewMux(worker.New(workerURL))}
```

- [ ] **Step 6: Run all tests, expect PASS**

Run: `cd gateway && go test ./...`

- [ ] **Step 7: Commit**

```bash
git add gateway/
git commit -m "feat(gateway): real chat handler relaying worker stream, sse and non-stream modes"
```

---

### Task 6: API-key auth (TDD, fake store)

**Files:**
- Create: `gateway/internal/auth/auth.go`, `gateway/internal/store/postgres.go`
- Modify: `gateway/internal/api/server.go`, `gateway/cmd/gateway/main.go`
- Test: `gateway/internal/auth/auth_test.go`

**Interfaces:**
- Produces: `auth.ErrUnknownKey`; `auth.Store` interface `{ TenantIDByKeyHash(ctx context.Context, hashHex string) (int64, error) }`; `auth.New(s Store, ttl time.Duration) *Authenticator`; `(a *Authenticator) Middleware(next http.Handler) http.Handler` (401 unknown/missing, 503 store down, caches hits for ttl). `store.New(dsn string) (*Store, error)` implements auth.Store via Postgres. `api.NewMux` final signature: `NewMux(w *worker.Client, a *auth.Authenticator) *http.ServeMux` — auth wraps only `/v1/*`.

- [ ] **Step 1: Add dependency**

Run: `cd gateway && go get github.com/jackc/pgx/v5@latest`

- [ ] **Step 2: Write failing tests** — `gateway/internal/auth/auth_test.go`

```go
package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type fakeStore struct {
	keys  map[string]int64 // hashHex -> tenant
	calls int
	err   error
}

func (f *fakeStore) TenantIDByKeyHash(_ context.Context, h string) (int64, error) {
	f.calls++
	if f.err != nil {
		return 0, f.err
	}
	id, ok := f.keys[h]
	if !ok {
		return 0, ErrUnknownKey
	}
	return id, nil
}

// sha256 hex of "sk-test" — precomputed so the test states its expectation.
const skTestHash = "c8ee27467bdcf14e5a9e57ffa2fc7ea1d7f77e5a1c6cadd25f4dccb6bb0b0dc7"

func do(t *testing.T, a *Authenticator, key string) int {
	t.Helper()
	h := a.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Code
}

func TestMissingKeyIs401(t *testing.T) {
	a := New(&fakeStore{}, time.Minute)
	if code := do(t, a, ""); code != http.StatusUnauthorized {
		t.Fatalf("code = %d", code)
	}
}

func TestUnknownKeyIs401(t *testing.T) {
	a := New(&fakeStore{keys: map[string]int64{}}, time.Minute)
	if code := do(t, a, "sk-wrong"); code != http.StatusUnauthorized {
		t.Fatalf("code = %d", code)
	}
}

func TestValidKeyPassesAndIsCached(t *testing.T) {
	fs := &fakeStore{keys: map[string]int64{skTestHash: 1}}
	a := New(fs, time.Minute)
	if code := do(t, a, "sk-test"); code != http.StatusOK {
		t.Fatalf("first call code = %d", code)
	}
	if code := do(t, a, "sk-test"); code != http.StatusOK {
		t.Fatalf("second call code = %d", code)
	}
	if fs.calls != 1 {
		t.Fatalf("store calls = %d, want 1 (second hit must come from cache)", fs.calls)
	}
}

func TestStoreErrorIs503(t *testing.T) {
	a := New(&fakeStore{err: errors.New("db down")}, time.Minute)
	if code := do(t, a, "sk-test"); code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", code)
	}
}
```

Verify the constant first: `python -c "import hashlib; print(hashlib.sha256(b'sk-test').hexdigest())"` — paste the real output into `skTestHash` if it differs.

- [ ] **Step 3: Run, expect FAIL**, then write `gateway/internal/auth/auth.go`

```go
// Package auth authenticates API keys: SHA-256 the presented key, look the
// hex hash up in the store, cache hits briefly. Raw keys never touch disk
// or logs — same rationale as password hashing.
package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"
)

var ErrUnknownKey = errors.New("unknown api key")

type Store interface {
	TenantIDByKeyHash(ctx context.Context, hashHex string) (int64, error)
}

type cacheEntry struct {
	tenantID int64
	expires  time.Time
}

type Authenticator struct {
	store Store
	ttl   time.Duration
	mu    sync.Mutex
	cache map[string]cacheEntry
}

func New(s Store, ttl time.Duration) *Authenticator {
	return &Authenticator{store: s, ttl: ttl, cache: map[string]cacheEntry{}}
}

// Middleware guards /v1/* routes. Cache is keyed by hash so a raw key is
// never retained in memory longer than the request.
func (a *Authenticator) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key, ok := bearer(r)
		if !ok {
			http.Error(w, `{"error":{"message":"missing bearer token"}}`, http.StatusUnauthorized)
			return
		}
		sum := sha256.Sum256([]byte(key))
		hash := hex.EncodeToString(sum[:])

		a.mu.Lock()
		e, hit := a.cache[hash]
		a.mu.Unlock()
		if hit && time.Now().Before(e.expires) {
			next.ServeHTTP(w, r)
			return
		}

		tenantID, err := a.store.TenantIDByKeyHash(r.Context(), hash)
		switch {
		case errors.Is(err, ErrUnknownKey):
			http.Error(w, `{"error":{"message":"invalid api key"}}`, http.StatusUnauthorized)
			return
		case err != nil:
			http.Error(w, `{"error":{"message":"auth backend unavailable"}}`, http.StatusServiceUnavailable)
			return
		}

		a.mu.Lock()
		a.cache[hash] = cacheEntry{tenantID: tenantID, expires: time.Now().Add(a.ttl)}
		a.mu.Unlock()
		next.ServeHTTP(w, r)
	})
}

func bearer(r *http.Request) (string, bool) {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) || len(h) == len(prefix) {
		return "", false
	}
	return strings.TrimSpace(h[len(prefix):]), true
}
```

- [ ] **Step 4: Run auth tests, expect PASS**

- [ ] **Step 5: Write `gateway/internal/store/postgres.go`**

```go
// Package store is the gateway's Postgres access layer.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql driver "pgx"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
)

type Store struct {
	db *sql.DB
}

// New opens and pings the database. Fail fast at boot: a gateway that
// cannot authenticate anyone should not report healthy.
func New(dsn string) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) TenantIDByKeyHash(ctx context.Context, hashHex string) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx,
		`SELECT tenant_id FROM api_keys WHERE key_hash = $1`, hashHex).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, auth.ErrUnknownKey
	}
	return id, err
}
```

- [ ] **Step 6: Wire auth into the mux** — `gateway/internal/api/server.go` becomes:

```go
package api

import (
	"net/http"

	"github.com/Zanthium09/forge-serve/gateway/internal/auth"
	"github.com/Zanthium09/forge-serve/gateway/internal/worker"
)

// NewMux returns the gateway's routes. Auth wraps only /v1/* — health
// (and later metrics) stay open for probes and scrapers.
func NewMux(w *worker.Client, a *auth.Authenticator) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.Handle("POST /v1/chat/completions",
		a.Middleware(&chatHandler{worker: w}))
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}
```

Update `chat_test.go`'s `newTestMux` to build a pass-through authenticator backed by an allow-all fake store:

```go
type allowAllStore struct{}

func (allowAllStore) TenantIDByKeyHash(_ context.Context, _ string) (int64, error) {
	return 1, nil
}

func newTestMux(t *testing.T, workerURL string) http.Handler {
	t.Helper()
	return NewMux(worker.New(workerURL), auth.New(allowAllStore{}, time.Minute))
}
```

and add `Authorization: Bearer sk-test` headers to every request in `chat_test.go` (http.Post → build requests with header; for `http.Post` calls switch to `http.NewRequest` + `Do`).

- [ ] **Step 7: Wire `main.go`** — read `DATABASE_URL`, construct store and authenticator:

```go
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		slog.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	st, err := store.New(dsn)
	if err != nil {
		slog.Error("postgres unavailable", "err", err)
		os.Exit(1)
	}
	authn := auth.New(st, 5*time.Minute)
	srv := &http.Server{Addr: addr, Handler: api.NewMux(worker.New(workerURL), authn)}
```

- [ ] **Step 8: Run all tests (`go test ./...`), expect PASS; `go mod tidy`; commit**

```bash
git add gateway/
git commit -m "feat(gateway): sha-256 api-key auth with postgres store and ttl cache"
```

---

### Task 7: Prometheus metrics

**Files:**
- Create: `gateway/internal/metrics/metrics.go`
- Modify: `gateway/internal/api/server.go`, `gateway/internal/api/chat.go`
- Test: `gateway/internal/api/metrics_smoke_test.go`

**Interfaces:**
- Produces: `GET /metrics` (unauthenticated); metrics `forge_requests_total{code}`, `forge_ttft_seconds`, `forge_tokens_per_second`. `metrics.ObserveTTFT(d time.Duration)`, `metrics.SetTokensPerSecond(v float64)`, `metrics.IncRequest(code int)` used from `chat.go`.

- [ ] **Step 1: Add dependency**

Run: `cd gateway && go get github.com/prometheus/client_golang@latest`

- [ ] **Step 2: Write `gateway/internal/metrics/metrics.go`**

```go
// Package metrics holds the gateway's Prometheus instruments.
// Package-level promauto vars: simple, and there is exactly one gateway
// process — no need for injected registries yet.
package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "forge_requests_total",
		Help: "Chat completion requests by HTTP status code.",
	}, []string{"code"})

	// Buckets 50ms..~200s: TTFT on CPU spans stub-fast to queued-slow.
	ttftSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "forge_ttft_seconds",
		Help:    "Time to first token.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 12),
	})

	tokensPerSecond = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "forge_tokens_per_second",
		Help: "Completion tokens/sec of the most recently finished request.",
	})
)

func IncRequest(code int)             { requestsTotal.WithLabelValues(strconv.Itoa(code)).Inc() }
func ObserveTTFT(d time.Duration)     { ttftSeconds.Observe(d.Seconds()) }
func SetTokensPerSecond(v float64)    { tokensPerSecond.Set(v) }
```

- [ ] **Step 3: Expose `/metrics`** — in `server.go` add route (import `"github.com/prometheus/client_golang/prometheus/promhttp"`):

```go
	mux.Handle("GET /metrics", promhttp.Handler())
```

- [ ] **Step 4: Instrument `chat.go`** — in `stream`: record `start := time.Now()` at top of `ServeHTTP`; observe TTFT at the first non-done event (`if sent == 0 { metrics.ObserveTTFT(time.Since(start)) }` before incrementing); after `[DONE]` compute `metrics.SetTokensPerSecond(float64(sent)/time.Since(start).Seconds())`; call `metrics.IncRequest(200)` on success paths, `metrics.IncRequest(400)` in validation failures, `metrics.IncRequest(502)` in aggregate upstream failure. Mirror TTFT/tokens-per-second in `aggregate` using `final.CompletionTokens`.

- [ ] **Step 5: Smoke test** — `gateway/internal/api/metrics_smoke_test.go`

```go
package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMetricsEndpointExposesForgeMetrics(t *testing.T) {
	srv := httptest.NewServer(newTestMux(t, "http://unused.invalid"))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "forge_ttft_seconds") {
		t.Fatal("forge_ttft_seconds not exposed")
	}
}
```

- [ ] **Step 6: `go test ./...` PASS, `go mod tidy`, commit**

```bash
git add gateway/
git commit -m "feat(gateway): prometheus metrics (requests, ttft histogram, tokens/sec)"
```

---

### Task 8: ADRs + CI update

**Files:**
- Create: `docs/adr/001-sse-for-client-edge.md`, `docs/adr/002-ndjson-worker-protocol.md`
- Modify: `.github/workflows/ci.yml`

- [ ] **Step 1: Write `docs/adr/001-sse-for-client-edge.md`**

```markdown
# ADR-001: SSE for the client edge

Status: accepted (Phase 1, 2026-07-25)

## Context
Clients need live token streams. Candidates: SSE, WebSockets, gRPC streaming.

## Decision
Server-Sent Events on /v1/chat/completions.

## Rationale
- OpenAI compatibility is a hard requirement, and OpenAI streams SSE — any
  deviation breaks unmodified SDKs.
- Traffic is one-directional after the request (server → client). WebSockets'
  bidirectionality buys nothing and costs an upgrade handshake and stateful
  connection handling.
- SSE is plain HTTP: proxies, load balancers, curl all work untouched.
- gRPC streaming would force every browser/script client through a proxy.

## Consequences
Client → server mid-stream signals are limited to connection close (which we
use as cancellation). Acceptable: it is the OpenAI semantic too.
```

- [ ] **Step 2: Write `docs/adr/002-ndjson-worker-protocol.md`**

```markdown
# ADR-002: NDJSON over chunked HTTP for gateway ↔ worker streaming

Status: accepted (Phase 1, 2026-07-25)

## Context
The worker streams tokens to the gateway. Candidates: SSE, NDJSON over
chunked HTTP/1.1, gRPC server streaming.

## Decision
NDJSON (one JSON object per line) over HTTP/1.1 chunked transfer.

## Rationale
- Internal hop between two services we own: no browsers, no reconnection
  semantics, no proxies — SSE's framing (`data:` prefixes, event ids) is
  dead weight here.
- NDJSON is one json.dumps per line in Python and one json.Unmarshal per
  line in Go; trivially debuggable with curl.
- gRPC adds codegen, TLS/ALPN considerations and a heavier debug loop for
  no measurable win at Phase 1 scale. Revisit only with a measured need
  (per blueprint §5).

## Consequences
One stream per request (no multiplexing) — fine, the gateway opens one
worker request per client request. Final NDJSON line carries usage counts.
```

- [ ] **Step 3: Update CI** — replace `.github/workflows/ci.yml`:

```yaml
name: ci
on:
  push:
    branches: [main]
  pull_request:

jobs:
  gateway:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: "1.22"
          cache-dependency-path: gateway/go.sum
      - uses: golangci/golangci-lint-action@v6
        with:
          working-directory: gateway
      - name: Unit tests
        run: go test ./...
        working-directory: gateway

  worker:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Lint (ruff)
        run: |
          pip install ruff
          ruff check worker/
```

- [ ] **Step 4: Commit**

```bash
git add docs/adr/ .github/workflows/ci.yml
git commit -m "docs: adr-001 sse edge, adr-002 ndjson worker protocol; ci: worker ruff job, go cache"
```

---

### Task 9: End-to-end exit checks (real model, local)

**Files:** none (verification only; SDK script goes to scratchpad, not the repo).

- [ ] **Step 1: Full stack up**

Run: `docker compose down && docker compose up --build -d && docker compose ps`
Expected: gateway, worker (healthy), redis, postgres all running. Worker takes ~30–60s to become healthy (model load); gateway waits for it.

- [ ] **Step 2: Exit check 1 — real tokens via curl, auth enforced**

Run (should be 401): `curl.exe -s -o NUL -w "%{http_code}" -X POST http://localhost:8080/v1/chat/completions -H "Content-Type: application/json" -d "{\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}"`
Expected: `401`.

Run (real stream):
```bash
curl.exe -N -X POST http://localhost:8080/v1/chat/completions -H "Content-Type: application/json" -H "Authorization: Bearer sk-forge-dev-key" -d "{\"stream\":true,\"max_tokens\":40,\"messages\":[{\"role\":\"user\",\"content\":\"Write one sentence about servers.\"}]}"
```
Expected: real model tokens in `chat.completion.chunk` SSE events, then `data: [DONE]`.

- [ ] **Step 3: Exit check 2 — OpenAI SDK unmodified**

Write to scratchpad `sdk_check.py`:

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8080/v1", api_key="sk-forge-dev-key")

print("--- non-stream ---")
r = client.chat.completions.create(
    model="qwen2.5-1.5b", max_tokens=30,
    messages=[{"role": "user", "content": "Say hello in exactly three words."}])
print(r.choices[0].message.content)
print("usage:", r.usage)

print("--- stream ---")
s = client.chat.completions.create(
    model="qwen2.5-1.5b", max_tokens=30, stream=True,
    messages=[{"role": "user", "content": "Count from 1 to 5."}])
for chunk in s:
    if chunk.choices and chunk.choices[0].delta.content:
        print(chunk.choices[0].delta.content, end="", flush=True)
print()
```

Run: `pip install openai` (once) then `python sdk_check.py`
Expected: both sections print real model output; usage shows non-zero token counts.

- [ ] **Step 4: Exit check 3 — concurrency 10, no deadlock**

Run (Git Bash):
```bash
for i in $(seq 1 10); do curl -s -o /dev/null -w "req$i: %{http_code} in %{time_total}s\n" -X POST http://localhost:8080/v1/chat/completions -H "Content-Type: application/json" -H "Authorization: Bearer sk-forge-dev-key" -d '{"max_tokens":15,"messages":[{"role":"user","content":"hi"}]}' & done; wait
```
Expected: ten lines, all `200`. Times climb roughly linearly (sequential worker queues them) — slow is fine, stuck is not.

- [ ] **Step 5: Check metrics have data**

Run: `curl.exe -s http://localhost:8080/metrics | findstr forge_`
Expected: `forge_requests_total{code="200"} 1x`, `forge_ttft_seconds_count > 0`, `forge_tokens_per_second > 0`.

---

### Task 10: Finish the branch

- [ ] **Step 1:** Update `CLAUDE.md`: `Current phase:` → `**Phase 2 — Continuous batching** (Phase 1 completed <date>; exit criteria verified: real SSE end-to-end, OpenAI SDK unmodified both modes, concurrency-10 clean)`.
- [ ] **Step 2:** `git add CLAUDE.md && git commit -m "docs: mark phase 1 complete"`
- [ ] **Step 3:** Merge and push: `git checkout main && git merge phase-1-real-inference && git push && git branch -d phase-1-real-inference`
- [ ] **Step 4:** Verify CI green (both jobs) via the GitHub API or Actions tab.
