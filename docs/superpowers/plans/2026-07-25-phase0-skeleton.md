# Phase 0 — Skeleton Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Working repo skeleton: Go gateway streaming fake SSE tokens with client-disconnect cancellation, docker-compose with Redis+Postgres, model download target, CI running lint+test.

**Architecture:** Single Go module (`gateway/`) using only the standard library (`net/http`, Go 1.22 method-pattern routing, `log/slog`). The stub `/v1/chat/completions` handler streams OpenAI-format `chat.completion.chunk` SSE events from a ticker loop that selects on `r.Context().Done()` — the same cancellation shape the real proxy will use in Phase 1. Redis/Postgres run in compose but are unused until Phases 1/4.

**Tech Stack:** Go 1.22+ (stdlib only, zero dependencies), Docker + docker-compose, GitHub Actions, GNU Make.

## Global Constraints (from FORGE_SERVE_BLUEPRINT.md)

- Go 1.22+, standard project layout, no global mutable state, contexts propagated, graceful shutdown on SIGTERM.
- Stub streams fake tokens at **20 tok/s** (50ms interval).
- CI runs **lint + unit tests only** — never inference or benchmarks.
- Every non-obvious choice gets a *why* comment.
- Commits: conventional-commit style (`feat(gateway): ...`).
- Dev is native Windows: in PowerShell use `curl.exe` (plain `curl` aliases Invoke-WebRequest); `make` may be absent locally — every Makefile target's underlying command is listed so it can be run directly.
- Model: Qwen2.5-1.5B-Instruct GGUF Q4_K_M (not gated on Hugging Face, unlike Llama 3.2 — no login needed for `make model`).

**Phase 0 exit criteria (must all pass at the end):**
1. `curl.exe -N` shows streamed fake tokens through the composed stack.
2. Client disconnect cancels the handler goroutine, verified via gateway logs.
3. CI green on GitHub (lint + test).

---

### Task 1: Toolchain verification

**Files:** none (environment check only).

**Interfaces:**
- Consumes: nothing.
- Produces: verified `go` ≥1.22 and `docker` with compose on PATH — every later task assumes both.

- [ ] **Step 1: Check Go**

Run: `go version`
Expected: `go version go1.22` or higher.
If missing: `winget install GoLang.Go` then reopen the terminal.

- [ ] **Step 2: Check Docker Desktop + compose**

Run: `docker compose version`
Expected: `Docker Compose version v2.x`.
If missing: install Docker Desktop from https://www.docker.com/products/docker-desktop/ (it enables WSL2 automatically; needs virtualization on in BIOS), start it, re-run.

- [ ] **Step 3: Check docker daemon is running**

Run: `docker ps`
Expected: table header (possibly empty list), not an error. If "cannot connect", start Docker Desktop and wait for the whale icon to settle.

---

### Task 2: Repo scaffold

**Files:**
- Create: `.gitignore`, `README.md`, `Makefile`

**Interfaces:**
- Consumes: nothing.
- Produces: `make run|test|lint|model|up` targets used by all later tasks; `models/` ignored by git.

- [ ] **Step 1: Write `.gitignore`**

```gitignore
models/
bin/
*.exe
```

- [ ] **Step 2: Write minimal `README.md`** (recruiter version comes in Phase 4)

````markdown
# Forge-Serve

Self-hosted, distributed LLM inference gateway for open-weight models — an
OpenAI-compatible streaming API with continuous batching, routing, rate
limiting, failover, and observability. Built from scratch; see
[FORGE_SERVE_BLUEPRINT.md](FORGE_SERVE_BLUEPRINT.md).

**Status: Phase 0 — skeleton.**

## Quickstart

```bash
docker compose up --build
```

Then in another terminal:

```bash
curl.exe -N -X POST http://localhost:8080/v1/chat/completions -H "Content-Type: application/json" -d "{\"stream\":true}"
```

You should see fake tokens stream at 20 tok/s (real model arrives in Phase 1).
````

- [ ] **Step 3: Write `Makefile`**

```makefile
# Every target's command is runnable directly if make is not installed locally.
MODEL_URL  = https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct-GGUF/resolve/main/qwen2.5-1.5b-instruct-q4_k_m.gguf
MODEL_FILE = models/qwen2.5-1.5b-instruct-q4_k_m.gguf

.PHONY: run test lint model up

run:            ## run gateway locally
	cd gateway && go run ./cmd/gateway

test:           ## unit tests
	cd gateway && go test ./...

lint:           ## requires golangci-lint (CI always runs it)
	cd gateway && golangci-lint run

model: $(MODEL_FILE)  ## download quantized GGUF (~1.1 GB, not gated)

$(MODEL_FILE):
	mkdir -p models
	curl -L --fail -o $(MODEL_FILE) $(MODEL_URL)

up:             ## full stack
	docker compose up --build
```

- [ ] **Step 4: Commit**

```bash
git add .gitignore README.md Makefile
git commit -m "chore: repo scaffold (gitignore, readme stub, makefile)"
```

---

### Task 3: Gateway module with /healthz

**Files:**
- Create: `gateway/go.mod`, `gateway/cmd/gateway/main.go`, `gateway/internal/api/server.go`
- Test: `gateway/internal/api/server_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `api.NewMux() *http.ServeMux` — Task 4 registers nothing new, it edits `server.go`'s chat handler; Task 5's Dockerfile builds `./cmd/gateway`.

- [ ] **Step 1: Create module**

Run (from repo root): `cd gateway && go mod init github.com/Zanthium09/forge-serve/gateway`
Expected: creates `gateway/go.mod` with `go 1.2x`.

- [ ] **Step 2: Write the failing test** — `gateway/internal/api/server_test.go`

```go
package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthz(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	NewMux().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d, want 200", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Fatalf("got body %q, want \"ok\"", rec.Body.String())
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd gateway && go test ./...`
Expected: FAIL — `undefined: NewMux`.

- [ ] **Step 4: Write `gateway/internal/api/server.go`**

```go
// Package api holds the gateway's HTTP surface: health, and the
// OpenAI-compatible chat completions endpoint (stubbed in Phase 0).
package api

import "net/http"

// NewMux returns the gateway's routes. Go 1.22 method patterns replace a
// router dependency — stdlib is enough at this size.
func NewMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	return mux
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("ok"))
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd gateway && go test ./...`
Expected: `ok  github.com/Zanthium09/forge-serve/gateway/internal/api`.

- [ ] **Step 6: Write `gateway/cmd/gateway/main.go`** (with SIGTERM graceful shutdown per blueprint conventions)

```go
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Zanthium09/forge-serve/gateway/internal/api"
)

func main() {
	addr := ":8080"
	if v := os.Getenv("GATEWAY_ADDR"); v != "" {
		addr = v
	}
	srv := &http.Server{Addr: addr, Handler: api.NewMux()}

	// Graceful shutdown: SIGTERM/interrupt lets in-flight streams drain
	// instead of cutting client connections mid-token.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("gateway listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server failed", "err", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 7: Build + quick manual check**

Run: `cd gateway && go build ./... && go run ./cmd/gateway` then in a second terminal `curl.exe http://localhost:8080/healthz`
Expected: `ok`. Stop the server with Ctrl+C.

- [ ] **Step 8: Commit**

```bash
git add gateway/
git commit -m "feat(gateway): module skeleton with /healthz and graceful shutdown"
```

---

### Task 4: Stub SSE chat completions with cancellation

**Files:**
- Modify: `gateway/internal/api/server.go`
- Create: `gateway/internal/api/chat_stub.go`
- Test: `gateway/internal/api/chat_stub_test.go`

**Interfaces:**
- Consumes: `NewMux()` from Task 3 (adds one route).
- Produces: `POST /v1/chat/completions` streaming `chat.completion.chunk` SSE events then `data: [DONE]`; logs `"client disconnected"` with `tokens_sent` on cancel. Phase 1 replaces the body of `handleChatCompletions` with a real proxy — the SSE/cancellation shape stays.

- [ ] **Step 1: Write the failing tests** — `gateway/internal/api/chat_stub_test.go`

```go
package api

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestChatCompletionsStreamsSSE(t *testing.T) {
	srv := httptest.NewServer(NewMux())
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("got Content-Type %q, want text/event-stream", ct)
	}

	var chunks int
	var sawDone bool
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		if !strings.HasPrefix(line, "data: ") {
			t.Fatalf("non-SSE line: %q", line)
		}
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			sawDone = true
			break
		}
		var c chunk
		if err := json.Unmarshal([]byte(payload), &c); err != nil {
			t.Fatalf("bad chunk JSON %q: %v", payload, err)
		}
		if c.Object != "chat.completion.chunk" {
			t.Fatalf("got object %q, want chat.completion.chunk", c.Object)
		}
		chunks++
	}
	if chunks == 0 || !sawDone {
		t.Fatalf("chunks=%d sawDone=%v, want >0 and true", chunks, sawDone)
	}
}

func TestChatCompletionsClientDisconnectCancelsHandler(t *testing.T) {
	handlerDone := make(chan struct{})
	// Wrap the mux so we can observe the handler goroutine actually return —
	// this is the Phase 0 exit criterion "disconnect cancels the goroutine".
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		NewMux().ServeHTTP(w, r)
		close(handlerDone)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		srv.URL+"/v1/chat/completions", strings.NewReader(`{"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	// Read one byte so we know streaming started, then drop the connection.
	if _, err := resp.Body.Read(make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	cancel()

	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not exit within 2s of client disconnect")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd gateway && go test ./...`
Expected: FAIL — `undefined: chunk` (and 404s from the missing route).

- [ ] **Step 3: Write `gateway/internal/api/chat_stub.go`**

```go
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// Minimal mirror of OpenAI's chat.completion.chunk schema — only the fields
// clients need to render a stream. Full schema arrives in Phase 1.
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

var loremWords = strings.Fields(
	"lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod " +
		"tempor incididunt ut labore et dolore magna aliqua")

// handleChatCompletions is the Phase 0 stub: it proves the SSE pipe and the
// cancellation path before any model exists. The select-on-context shape here
// is exactly what the Phase 1 proxy and Phase 2 batch eviction will reuse.
func handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	// 50ms/token = 20 tok/s per blueprint Phase 0 spec.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for i, word := range loremWords {
		select {
		case <-r.Context().Done():
			// Client went away: stop generating immediately. In Phase 2 this
			// same signal evicts the sequence from a live batch.
			slog.Info("client disconnected, stream cancelled", "tokens_sent", i)
			return
		case <-ticker.C:
		}
		c := chunk{
			ID:      "chatcmpl-stub",
			Object:  "chat.completion.chunk",
			Created: time.Now().Unix(),
			Model:   "stub",
			Choices: []choice{{Delta: delta{Content: word + " "}}},
		}
		b, err := json.Marshal(c)
		if err != nil {
			slog.Error("marshal chunk", "err", err)
			return
		}
		fmt.Fprintf(w, "data: %s\n\n", b)
		flusher.Flush()
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
	flusher.Flush()
}
```

- [ ] **Step 4: Register the route** — in `gateway/internal/api/server.go`, add inside `NewMux` after the healthz line:

```go
	mux.HandleFunc("POST /v1/chat/completions", handleChatCompletions)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd gateway && go test ./...`
Expected: PASS (streaming test takes ~1s — 20 words at 20 tok/s).

- [ ] **Step 6: Commit**

```bash
git add gateway/internal/api/
git commit -m "feat(gateway): stub SSE chat completions with client-disconnect cancellation"
```

---

### Task 5: Dockerfile + docker-compose

**Files:**
- Create: `gateway/Dockerfile`, `docker-compose.yml`

**Interfaces:**
- Consumes: `gateway/cmd/gateway` binary from Task 3/4.
- Produces: `docker compose up --build` runs gateway (:8080) + Redis (:6379) + Postgres (:5432). Phase 1 adds the worker service; Phase 1/4 start actually using Redis/Postgres.

- [ ] **Step 1: Write `gateway/Dockerfile`**

```dockerfile
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /gateway ./cmd/gateway

FROM alpine:3.20
COPY --from=build /gateway /gateway
EXPOSE 8080
ENTRYPOINT ["/gateway"]
```

- [ ] **Step 2: Write `docker-compose.yml`** (repo root)

```yaml
services:
  gateway:
    build: ./gateway
    ports:
      - "8080:8080"
    depends_on:
      - redis
      - postgres

  # Unused until Phase 1 (auth) / Phase 4 (rate limiting) — present now so
  # the one-command quickstart shape is fixed from day one.
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

volumes:
  pgdata:
```

- [ ] **Step 3: Build and run the stack**

Run: `docker compose up --build` (leave it running)
Expected: three services start; gateway logs `gateway listening addr=:8080`.

- [ ] **Step 4: Verify from a second terminal**

Run: `curl.exe http://localhost:8080/healthz`
Expected: `ok`.

- [ ] **Step 5: Stop the stack** (Ctrl+C), **commit**

```bash
git add gateway/Dockerfile docker-compose.yml
git commit -m "feat: dockerize gateway; compose with redis and postgres"
```

---

### Task 6: Model download target

**Files:** none new (Makefile target from Task 2; creates untracked `models/qwen2.5-1.5b-instruct-q4_k_m.gguf`).

**Interfaces:**
- Consumes: `make model` target from Task 2.
- Produces: local GGUF file Phase 1's worker will load.

- [ ] **Step 1: Run the download** (~1.1 GB; if `make` is absent locally, run the two commands directly)

Run: `make model`
Or directly:
```bash
mkdir -p models
curl -L --fail -o models/qwen2.5-1.5b-instruct-q4_k_m.gguf https://huggingface.co/Qwen/Qwen2.5-1.5B-Instruct-GGUF/resolve/main/qwen2.5-1.5b-instruct-q4_k_m.gguf
```
Expected: file exists, size ≈ 1.1 GB. No commit — `models/` is gitignored.

- [ ] **Step 2: Verify git ignores it**

Run: `git status --short`
Expected: no `models/` entry.

---

### Task 7: CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `gateway/` module and its tests.
- Produces: green check on GitHub for every push/PR — lint + unit tests only, per blueprint (no benchmarks in CI, ever).

- [ ] **Step 1: Write `.github/workflows/ci.yml`**

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
          cache: false # no go.sum yet (zero deps); enable when deps appear
      - uses: golangci/golangci-lint-action@v6
        with:
          working-directory: gateway
      - name: Unit tests
        run: go test ./...
        working-directory: gateway
```

- [ ] **Step 2: Commit and push**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: lint and unit tests on push/PR"
git push -u origin main
```

- [ ] **Step 3: Verify CI is green**

Run: `gh run watch` (or check the Actions tab on GitHub)
Expected: `ci` workflow passes. If lint flags anything, fix it, commit, push again.

---

### Task 8: Exit criteria verification (manual, through the composed stack)

**Files:** none.

**Interfaces:**
- Consumes: everything above.
- Produces: Phase 0 sign-off; Phase 1 may begin.

- [ ] **Step 1: Start the full stack**

Run: `docker compose up --build`

- [ ] **Step 2: Exit criterion 1 — streamed fake tokens**

Run (second terminal):
```bash
curl.exe -N -X POST http://localhost:8080/v1/chat/completions -H "Content-Type: application/json" -d "{\"stream\":true}"
```
Expected: `data: {...chat.completion.chunk...}` lines appearing ~20/s, ending with `data: [DONE]`.

- [ ] **Step 3: Exit criterion 2 — disconnect cancels the goroutine**

Run the same curl again, press **Ctrl+C after ~3 tokens**, then:
```bash
docker compose logs gateway --tail 5
```
Expected: a log line `client disconnected, stream cancelled tokens_sent=N` with N < 20.

- [ ] **Step 4: Exit criterion 3 — CI green** (verified in Task 7; re-confirm the badge/Actions tab shows passing on the latest commit).

- [ ] **Step 5: Record completion** — update `CLAUDE.md` line `Current phase:` to `**Phase 1 — Real inference, single worker** (Phase 0 complete <date>)`, commit:

```bash
git add CLAUDE.md
git commit -m "docs: mark phase 0 complete"
git push
```
