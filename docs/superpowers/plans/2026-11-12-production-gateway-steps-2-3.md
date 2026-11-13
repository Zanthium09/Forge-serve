# Production Gateway (Steps 2–3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the gateway into an engine-agnostic, multi-model, pass-through proxy (Step 2), then give it per-person identity in its own database with request/TPM limits, token budgets, grants, priority, an admin API and a CLI (Step 3).

**Architecture:** The gateway forwards the client's original OpenAI body to a per-model pool of backends behind one `Backend` interface (OpenAI-compatible forwarder + our NDJSON worker adapter). A YAML config describes models and backends. Identity lives in the gateway's own Postgres schema (7 tables, embedded migrations); Redis holds request buckets, TPM buckets and budget counters, all degrading to local/skip when Redis is down.

**Tech Stack:** Go 1.25 (`gateway/` module `github.com/Zanthium09/forge-serve/gateway`), Postgres 16 via `pgx/v5/stdlib`, Redis via `go-redis/v9`, Prometheus `client_golang`, new: `gopkg.in/yaml.v3`.

**Spec:** `docs/superpowers/specs/2026-11-10-production-gateway-design.md` (approved 2026-11-12). Section references below (§n) point there.

## Global Constraints

- Only new dependency: `gopkg.in/yaml.v3`. Everything else stdlib or already in `go.mod`.
- Never log, store or put in metrics any request/response body text or raw API key. No `user` label on any Prometheus metric.
- Key format: `sk-forge-` + base62 of 32 random bytes; stored as lowercase SHA-256 hex of the full raw key; `key_prefix` = first 8 chars after `sk-forge-`.
- Defaults: `listen ":8080"`, `auth_cache_ttl 30s`, `max_body_bytes 1048576`, backend `max_inflight 64`, `max_waiting 0` (= not used), model `default_max_tokens 256`, user `rpm_burst 60`, `rpm_refill_per_sec 10`, `priority 0`.
- Health: poll every 2 s, unhealthy after 3 consecutive misses, healthy after 1 success (unchanged, ADR-004).
- Retry rule (ADR-005): retry once on another backend in the same pool only if 0 content chunks reached the client, the client is still connected, and the failure is not a backend 4xx / `ErrOverloaded` / `ErrUnsupported`.
- Error bodies use OpenAI shape `{"error":{"message":"…","type":"…","code":"…"}}`:

  | Situation | Status | type | code |
  |---|---|---|---|
  | bad JSON / missing field / no model with >1 configured | 400 | `invalid_request_error` | `invalid_request` |
  | body > `max_body_bytes` | 413 | `invalid_request_error` | `body_too_large` |
  | missing/unknown/revoked/expired key, disabled user | 401 | `authentication_error` | `invalid_api_key` |
  | restricted model, no grant / non-admin on `/admin` | 403 | `permission_error` | `model_not_allowed` / `admin_required` |
  | unknown model | 404 | `invalid_request_error` | `model_not_found` |
  | endpoint unsupported by backend kind | 400 | `invalid_request_error` | `unsupported_endpoint` |
  | request rate / TPM exceeded | 429 + `Retry-After` | `rate_limit_error` | `rate_limited` / `tokens_per_minute` |
  | budget reached | 429 | `insufficient_quota` | `budget_exceeded` |
  | pool full | 429 + `Retry-After: 1` | `server_error` | `overloaded` |
  | no healthy backend | 503 + `Retry-After: 5` | `server_error` | `no_healthy_backend` |
  | mid-stream failure | SSE event `data: {"error":{"message":"upstream failure","type":"worker_lost"}}` | | |
- Redis keys: request bucket `forge:rl:u:<userID>`; TPM bucket `forge:tpm:u:<userID>`; budgets `forge:tok:u:<id>:d:<YYYY-MM-DD>`, `forge:tok:u:<id>:m:<YYYY-MM>`, `forge:tok:t:<id>:m:<YYYY-MM>` (UTC).
- Every task leaves `go build ./... && go test ./...` green. When a task changes a type used by another package, it updates those callers in the same task (zero values are fine until a later task fills them).
- Commit style: conventional commits. Work on branch `design/production-gateway` (or a feature branch from it).
- Test commands run from `gateway/`. DB-backed tests need `FORGE_TEST_DATABASE_URL` (skip when unset); real-Redis tests need `FORGE_TEST_REDIS_URL` (skip when unset). Local: `docker compose up -d postgres redis`, then `FORGE_TEST_DATABASE_URL='postgres://forge:forge@localhost:5432/forge?sslmode=disable' FORGE_TEST_REDIS_URL='redis://localhost:6379' go test ./...`.

## Review Focus

1. **Backend 4xx** (context too long, bad parameter): client must receive the backend's status and body unchanged; no retry, no failover, no budget/TPM charge. → test in Task 8.
2. **Client disconnects mid-stream:** backend request cancelled, backend in-flight slot released, delivered tokens recorded as `estimated` usage, no retry. → tests in Task 8 (release) and Task 15 (usage).
3. **Request without `model`:** one model configured → served by it (existing `loadgen` and `chaos_test.sh` send no model); several configured → 400. → test in Task 6.
4. **Oversized or non-object body** (`[]`, `"x"`, 2 MiB): rejected 413/400 before any backend call. → test in Task 6.
5. **Two gateway copies start together on a fresh DB:** migrations apply exactly once. → test in Task 10.

---

## Decisions made in this plan (not in the spec)

- `Forward` returns `Result{Sent, Usage}` instead of spec's `(sent int, err)`, so usage travels with the count.
- Response `model` field is passed through from the engine unchanged (it names the served upstream model); the client-facing served name goes in header `X-Forge-Model-Fallback` only when fallback happened. Avoids re-marshalling every chunk.
- `sglang` and `tgi` kinds are accepted but use the health-only probe until Step 8 verifies their metric names; `ollama` and `openai` are health-only by spec.
- Migration `001_identity.sql` **drops** the legacy `tenants`/`api_keys`/`usage_events` tables instead of migrating rows (overrides the spec's builder's call in §7.1): only dev data exists today; dev keys are re-created by `FORGE_DEV_SEED=1` (Task 17).
- Model omitted + exactly one model configured → that model (keeps loadgen/chaos scripts working).
- Backend `api_key_env` (§6.8) is left to Step 4 per the spec's build order.

---

## Task 0: Decision records

**Files:**
- Create: `docs/adr/007-pass-through-proxy.md` … `docs/adr/015-gateway-owns-identity.md` (one per spec §12 item, 007–015)
- Modify: `FORGE_SERVE_BLUEPRINT.md` §13 (append amendment entry dated 2026-11-12 pointing at the spec)
- Modify: `CLAUDE.md` (current phase line: "Production gateway, Steps 2–3, plan `docs/superpowers/plans/2026-11-12-production-gateway-steps-2-3.md`")

- [ ] **Step 1:** Write each ADR in the existing format (`Status`, `Context`, `Decision`, `Rationale`, `Consequences`), content taken from the spec section each one names (§6.2, §7.2/ADR-008, §7.3, §6.4, §6.5, §7.2 TPM, §7.5, §6.7, §7.7).
- [ ] **Step 2:** Commit: `docs: ADR-007..015 and blueprint amendment for production gateway`.

---

# Milestone A — Step 2: proxy, backends, pools

## Task 1: Config file

**Files:**
- Create: `gateway/internal/config/config.go`, `gateway/internal/config/config_test.go`, `deploy/forge.dev.yaml`
- Modify: `gateway/go.mod`, `gateway/go.sum` (`go get gopkg.in/yaml.v3`)

**Interfaces:**
- Produces:
  ```go
  type Kind string // "vllm" | "sglang" | "tgi" | "ollama" | "openai" | "forge"
  type Config struct {
      Listen       string        `yaml:"listen"`
      AuthCacheTTL time.Duration `yaml:"auth_cache_ttl"`
      MaxBodyBytes int64         `yaml:"max_body_bytes"`
      Defaults     UserDefaults  `yaml:"defaults"`
      Models       []Model       `yaml:"models"`
  }
  type UserDefaults struct {
      RPMBurst int `yaml:"rpm_burst"`; RPMRefillPerSec float64 `yaml:"rpm_refill_per_sec"`
      TokensPerMinute int `yaml:"tokens_per_minute"`               // 0 = unlimited
      DailyTokenBudget, MonthlyTokenBudget int64 `yaml:"…"`          // 0 = unlimited
      Priority int `yaml:"priority"`
  }
  type Model struct {
      Name string `yaml:"name"`; UpstreamModel string `yaml:"upstream_model"`
      Restricted bool `yaml:"restricted"`; DefaultMaxTokens int `yaml:"default_max_tokens"`
      Fallback string `yaml:"fallback"`; Backends []Backend `yaml:"backends"`
  }
  type Backend struct {
      URL string `yaml:"url"`; Kind Kind `yaml:"kind"`
      MaxWaiting int `yaml:"max_waiting"`; MaxInflight int `yaml:"max_inflight"`
      SupportsPriority bool `yaml:"supports_priority"`
  }
  func Load(path string) (*Config, error)            // read + unmarshal + applyDefaults + Validate
  func FromWorkerURLs(urls []string, model string) *Config // shim: one forge model
  func (c *Config) Validate() error
  func (m Model) Upstream() string                    // UpstreamModel, else Name
  ```

- [ ] **Step 1: Write failing tests** in `config_test.go`:
  ```go
  func TestLoadAppliesDefaults(t *testing.T)       // minimal file: 1 model, 1 vllm backend
  // assert: c.Listen==":8080", c.AuthCacheTTL==30*time.Second, c.MaxBodyBytes==1048576,
  //         c.Models[0].DefaultMaxTokens==256, c.Models[0].Backends[0].MaxInflight==64,
  //         c.Defaults.RPMBurst==60, c.Defaults.RPMRefillPerSec==10
  func TestLoadParsesDurationAndFallback(t *testing.T) // auth_cache_ttl: 45s; fallback: small
  // assert: AuthCacheTTL==45*time.Second, Models[0].Fallback=="small"
  func TestValidateRejects(t *testing.T) // table: duplicate model name; model with 0 backends;
  // unknown kind "foo"; empty url; fallback to unknown model; fallback to itself
  // assert: each returns non-nil error mentioning the model name
  func TestFromWorkerURLs(t *testing.T) // ["http://a:9000"," http://b:9000"], "qwen2.5-1.5b"
  // assert: 1 model "qwen2.5-1.5b", 2 backends kind "forge", urls trimmed, defaults applied
  func TestUpstream(t *testing.T) // Model{Name:"a"}.Upstream()=="a"; with UpstreamModel "x" -> "x"
  ```
- [ ] **Step 2:** Run `go test ./internal/config/ -v` → FAIL (package missing).
- [ ] **Step 3:** Implement. `yaml.v3` decodes `"45s"` into `time.Duration`; if the test shows it does not, add a `Duration` wrapper type with `UnmarshalYAML`. Write `deploy/forge.dev.yaml` mirroring today's compose (model `qwen2.5-1.5b`, kind `forge`, `http://worker1:9000`, `http://worker2:9000`).
- [ ] **Step 4:** Run `go test ./internal/config/ -v` → PASS.
- [ ] **Step 5:** Commit `feat(config): yaml config for models and backends`.

## Task 2: Backend contract and non-streaming OpenAI forwarder

**Files:**
- Create: `gateway/internal/backend/backend.go`, `gateway/internal/backend/openai.go`, `gateway/internal/backend/openai_test.go`, `gateway/internal/backend/backendtest/fake.go`

**Interfaces:**
- Produces (`package backend`):
  ```go
  type Endpoint string
  const (EndpointChat Endpoint = "chat"; EndpointCompletions = "completions"; EndpointEmbeddings = "embeddings")
  func (e Endpoint) Path() string // "/v1/chat/completions", "/v1/completions", "/v1/embeddings"

  type ProxyRequest struct {
      Endpoint        Endpoint
      Body            []byte // final body, already edited by the gateway
      Stream          bool
      StripUsageChunk bool   // gateway injected include_usage; hide that chunk from the client
  }
  type Usage struct { PromptTokens, CompletionTokens int; Estimated bool }
  type Result struct { Sent int; Usage Usage } // Sent = content chunks (stream) or 1 for a delivered JSON body

  // Sink is the client side. Start is called at most once, immediately before the
  // first Write; nothing may reach Sink before the first content chunk (stream)
  // or before the full successful body (non-stream).
  type Sink interface { Start(contentType string); Write(p []byte) error }

  type Load struct { Waiting, Running int; Known bool }
  type Backend interface {
      Forward(ctx context.Context, r *ProxyRequest, sink Sink) (Result, error)
      Load(ctx context.Context) (Load, error)
  }
  var ErrOverloaded  = errors.New("backend overloaded")             // backend said 429
  var ErrUnsupported = errors.New("endpoint not supported by backend")
  type StatusError struct { Code int; Body []byte }                 // backend non-2xx, non-429
  func (e *StatusError) Error() string

  func NewOpenAI(baseURL string) *OpenAI    // Forward + health-only Load (GET /v1/models)
  ```
- Produces (`package backendtest`, test helper used by Tasks 2–8, 15):
  ```go
  type Fake struct {
      Chunks      []string      // SSE data payloads (without "data: "); "[DONE]" appended automatically
      Delay       time.Duration // before each chunk
      DieAfter    int           // -1 never; n = close connection after n chunks
      Status      int           // non-zero: reply with this status and Body before streaming
      Body        string        // JSON body for non-stream / error replies
      OmitUsage   bool          // never send a usage chunk even if include_usage requested
      Metrics     string        // served at /metrics
      LastBody    atomic.Value  // []byte of the last request body received
      Requests    atomic.Int64
  }
  func (f *Fake) Server(t *testing.T) *httptest.Server // closed via t.Cleanup
  func ChatChunk(content string) string   // `{"choices":[{"index":0,"delta":{"content":"<c>"}}]}`
  func RoleChunk() string                  // `{"choices":[{"index":0,"delta":{"role":"assistant"}}]}`
  func UsageChunk(prompt, completion int) string // `{"choices":[],"usage":{"prompt_tokens":p,"completion_tokens":c}}`
  ```
  When the request body has `stream_options.include_usage: true` and `OmitUsage` is false, the fake appends `UsageChunk(5, len(content chunks))` before `[DONE]`. Non-stream replies use `Body` (default: a chat.completion with `"usage":{"prompt_tokens":5,"completion_tokens":2}`).

- [ ] **Step 1: Write failing tests** (non-stream only):
  ```go
  func TestOpenAINonStreamDeliversBodyAndUsage(t *testing.T)
  // fake default Body; Forward(EndpointChat, Stream:false) into a recording sink
  // assert: err==nil, sink.contentType=="application/json", sink body == fake Body bytes,
  //         res.Sent==1, res.Usage=={5,2,false}, fake.LastBody == request Body
  func TestOpenAIEmbeddingsUsesPromptTokens(t *testing.T)
  // Body `{"object":"list","data":[],"usage":{"prompt_tokens":7,"total_tokens":7}}`
  // assert: res.Usage.PromptTokens==7, CompletionTokens==0, path hit == "/v1/embeddings"
  func TestOpenAI4xxIsStatusErrorAndNothingWritten(t *testing.T)
  // Status 400, Body `{"error":{"message":"too long"}}`
  // assert: errors.As(err,&se), se.Code==400, string(se.Body) contains "too long", sink never started
  func TestOpenAI429IsErrOverloaded(t *testing.T)   // Status 429 → errors.Is(err, ErrOverloaded)
  func TestOpenAIConnectionRefusedIsRetryable(t *testing.T)
  // closed server → err != nil, not *StatusError, not ErrOverloaded, res.Sent==0
  func TestOpenAILoadHealthOnly(t *testing.T)
  // /v1/models 200 → Load{Known:false}, nil; server closed → err != nil
  ```
- [ ] **Step 2:** Run `go test ./internal/backend/ -run OpenAI -v` → FAIL.
- [ ] **Step 3:** Implement `backend.go`, `backendtest/fake.go`, and the non-stream path of `OpenAI.Forward`: POST `Body` to `baseURL+Endpoint.Path()`, read the whole body, decode only `usage`, then `sink.Start("application/json")` + one `Write`. `http.Client` without total timeout (ADR-002).
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat(backend): backend contract and non-streaming openai forwarder`.

## Task 3: Streaming relay in the OpenAI forwarder

**Files:**
- Modify: `gateway/internal/backend/openai.go`
- Test: `gateway/internal/backend/openai_stream_test.go`

**Interfaces:**
- Consumes: Task 2 types and `backendtest.Fake`.
- Produces: `OpenAI.Forward` with `Stream:true` writes SSE events to the sink as `"data: <payload>\n\n"`, one `Write` per event.

Relay rules (spec §6.3):
- Lines without content (role-only delta, empty delta) are **held** until the first content-bearing chunk, then flushed in order right after `sink.Start("text/event-stream")`.
- A chunk is content-bearing if any `choices[i].delta.content` (chat) or `choices[i].text` (completions) is non-empty, or `delta.tool_calls` is present.
- A chunk with a `usage` object is parsed into `Result.Usage`; when `StripUsageChunk` is true it is not written.
- `data: [DONE]` is written through; stream ends.
- Stream ends without any usage chunk → `Usage{CompletionTokens: Sent, Estimated: true}`.
- `Sent` counts content-bearing chunks written.

- [ ] **Step 1: Write failing tests:**
  ```go
  func TestStreamHoldsRoleChunkUntilContent(t *testing.T)
  // Chunks: RoleChunk(), ChatChunk("Hi"), ChatChunk("!")
  // assert: sink events == ["data: "+RoleChunk(), "data: "+ChatChunk("Hi"), "data: "+ChatChunk("!"), "data: [DONE]"]
  //         (each followed by "\n\n"), res.Sent==2, contentType=="text/event-stream"
  func TestStreamDiesBeforeContentWritesNothing(t *testing.T)
  // Chunks: RoleChunk(), ChatChunk("x"); DieAfter:1 → err!=nil, sink never started, res.Sent==0
  func TestStreamDiesAfterContentReportsSent(t *testing.T)
  // Chunks: ChatChunk("a"), ChatChunk("b"), ChatChunk("c"); DieAfter:2 → err!=nil, res.Sent==2
  func TestStreamStripsInjectedUsageChunk(t *testing.T)
  // request body has include_usage:true, StripUsageChunk:true
  // assert: no written event contains `"usage"`, res.Usage=={5,2,false}
  func TestStreamKeepsUsageChunkWhenClientAskedForIt(t *testing.T)
  // StripUsageChunk:false → one written event contains `"usage"`
  func TestStreamWithoutUsageIsEstimated(t *testing.T)
  // OmitUsage:true, 3 content chunks → res.Usage=={0,3,true}
  func TestStreamCompletionsTextCountsAsContent(t *testing.T)
  // Chunks: `{"choices":[{"text":"ok"}]}` with EndpointCompletions → res.Sent==1
  func TestStreamClientCancelStopsBackend(t *testing.T)
  // Delay 50ms, 20 chunks; cancel ctx after first Write → Forward returns within 200ms, err != nil
  ```
- [ ] **Step 2:** Run `go test ./internal/backend/ -run Stream -v` → FAIL.
- [ ] **Step 3:** Implement with `bufio.Scanner` (buffer max 1 MiB) over `data:` lines; parse each payload into a minimal struct (`choices[].delta.content`, `choices[].delta.tool_calls`, `choices[].text`, `usage`).
- [ ] **Step 4:** Run `go test ./internal/backend/ -v` → PASS.
- [ ] **Step 5:** Commit `feat(backend): sse relay with held role chunks and usage capture`.

## Task 4: Load probes and backend construction by kind

**Files:**
- Create: `gateway/internal/backend/probes.go`, `gateway/internal/backend/probes_test.go`, `gateway/internal/backend/new.go`

**Interfaces:**
- Consumes: `config.Kind`.
- Produces:
  ```go
  func ParseVLLMMetrics(text string) (Load, error)
  // sums samples of vllm:num_requests_waiting and vllm:num_requests_running across label sets;
  // Known=true; error if neither metric is present
  type VLLM struct{ *OpenAI }             // Load = GET /metrics + ParseVLLMMetrics
  func New(kind config.Kind, url string) (Backend, error)
  // vllm→*VLLM; sglang|tgi|ollama|openai→*OpenAI; forge→NewForge (Task 5); unknown→error
  ```

- [ ] **Step 1: Write failing tests:**
  ```go
  func TestParseVLLMMetrics(t *testing.T)
  // text contains comments, `vllm:num_requests_waiting{model_name="m"} 3`,
  // `vllm:num_requests_waiting{model_name="n"} 2`, `vllm:num_requests_running{model_name="m"} 7.0`
  // assert: Load{Waiting:5, Running:7, Known:true}
  func TestParseVLLMMetricsMissing(t *testing.T) // only unrelated metrics → error
  func TestVLLMLoadFromServer(t *testing.T)       // Fake.Metrics set → Known:true, Waiting==3
  func TestNewByKind(t *testing.T)                // "vllm"→*VLLM, "ollama"→*OpenAI, "x"→error
  ```
- [ ] **Step 2:** Run `go test ./internal/backend/ -run 'VLLM|NewByKind' -v` → FAIL.
- [ ] **Step 3:** Implement with a line scanner (no new dependency): metric name = text before `{` or space; value = last field parsed with `strconv.ParseFloat`.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat(backend): vllm load probe and backend construction by kind`.

## Task 5: Forge worker adapter

**Files:**
- Create: `gateway/internal/backend/forge.go`, `gateway/internal/backend/forge_test.go`
- Modify: `gateway/internal/backend/new.go` (wire `forge`)

**Interfaces:**
- Consumes: `worker.Client` (`gateway/internal/worker/client.go`, unchanged), Task 2 types.
- Produces: `func NewForge(baseURL string) *Forge`. `Forward`: non-chat endpoint → `ErrUnsupported` (sink untouched). Chat: decode `messages`, `temperature`, `max_tokens` from `Body` into `worker.Request` (defaults 0.7 / 256), call `Generate`, emit OpenAI `chat.completion.chunk` SSE events (the `chunk`/`choice`/`delta` structs move here from `api/chat.go`) ending with a finish-reason chunk, a usage chunk only if `Body` asked for `include_usage` and `StripUsageChunk` is false, and `data: [DONE]`. Non-stream: one `chat.completion` JSON with `usage`. `worker.ErrOverloaded` → `ErrOverloaded`. `Load` = `Stats` → `Load{Waiting: QueueDepth, Running: ActiveSeqs, Known: true}`.

- [ ] **Step 1: Write failing tests** (NDJSON fake worker: copy `fakeWorker`/`happyLines` pattern from `internal/api/chat_test.go` into `forge_test.go`):
  ```go
  func TestForgeStreamTranslatesToOpenAIChunks(t *testing.T)
  // happyLines → content events "Hello", " world", a chunk with finish_reason "stop", "data: [DONE]";
  // res.Sent==2, res.Usage=={5,2,false}
  func TestForgeNonStream(t *testing.T)
  // body JSON has choices[0].message.content=="Hello world", usage.total_tokens==7
  func TestForgeRejectsEmbeddings(t *testing.T)   // errors.Is(err, ErrUnsupported), sink not started
  func TestForgeOverloaded(t *testing.T)          // worker 429 → errors.Is(err, ErrOverloaded)
  func TestForgeLoad(t *testing.T)                // /stats {"queue_depth":2,"active_seqs":5} → {2,5,true}
  ```
- [ ] **Step 2:** Run `go test ./internal/backend/ -run Forge -v` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat(backend): forge worker adapter behind backend interface`.

## Task 6: Request parsing and body edits

**Files:**
- Create: `gateway/internal/api/body.go`, `gateway/internal/api/body_test.go`, `gateway/internal/api/errors.go`

**Interfaces:**
- Produces:
  ```go
  type parsedRequest struct {
      Endpoint  backend.Endpoint
      Model     string // "" if absent
      Stream    bool
      MaxTokens int    // 0 if absent
      fields    map[string]json.RawMessage
  }
  func readBody(w http.ResponseWriter, r *http.Request, max int64) ([]byte, error) // http.MaxBytesReader; *http.MaxBytesError → caller sends 413
  func parseRequest(ep backend.Endpoint, body []byte) (*parsedRequest, error)
  // must be a JSON object; chat requires "messages", completions "prompt", embeddings "input"
  func (p *parsedRequest) build(upstreamModel string, priority *int) (body []byte, injectedUsage bool, err error)
  // sets "model"=upstreamModel; always deletes client "priority", then sets it when priority != nil;
  // when Stream and stream_options.include_usage is not true: sets it true (keeping other
  // stream_options keys) and returns injectedUsage=true; every other field byte-for-byte unchanged
  func resolveModel(requested string, models []string) (string, error) // ""+1 model → that; ""+many → error
  func writeError(w http.ResponseWriter, status int, typ, code, msg string) // OpenAI error shape
  ```

- [ ] **Step 1: Write failing tests:**
  ```go
  func TestParseRequiresObjectAndField(t *testing.T)
  // `[]`, `"x"`, `{}` for chat, `{"prompt":"a"}` for chat → error; `{"messages":[]}` chat → ok
  func TestBuildPassesUnknownFieldsThrough(t *testing.T)
  // in: {"model":"a","messages":[],"top_p":0.3,"tools":[{"type":"function"}],"seed":7}
  // build("up/a", nil) → decoded map: model=="up/a"; top_p, tools, seed equal to input RawMessage
  func TestBuildInjectsUsageOnlyForStream(t *testing.T)
  // stream:true, stream_options:{"x":1} → include_usage true, x still 1, injected==true
  // stream:true, include_usage:true already → injected==false; stream:false → no stream_options added
  func TestBuildStripsClientPriority(t *testing.T)
  // client priority 1: build(...,nil) → no "priority"; build(...,&five) → priority==5
  func TestReadBodyTooLarge(t *testing.T) // 2 MiB body, max 1 MiB → *http.MaxBytesError
  func TestResolveModel(t *testing.T)
  // ("",["m"])→"m"; ("",["a","b"])→error; ("x",["a"])→"x" (existence checked later)
  ```
- [ ] **Step 2:** Run `go test ./internal/api/ -run 'Parse|Build|ReadBody|ResolveModel' -v` → FAIL.
- [ ] **Step 3:** Implement with `map[string]json.RawMessage` (field order may change; values may not).
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat(api): pass-through request parsing with permitted body edits`.

## Task 7: Per-model pools, admission and fallback lookup

**Files:**
- Modify: `gateway/internal/pool/pool.go`, `gateway/internal/pool/pool_test.go`
- Create: `gateway/internal/pool/fleet.go`, `gateway/internal/pool/fleet_test.go`

**Interfaces:**
- Consumes: `backend.Backend`, `backend.Load`, `config.Config`, `backend.New`.
- Produces:
  ```go
  // Worker: Client *worker.Client is replaced by Backend backend.Backend.
  func NewWorker(url string, b backend.Backend, maxWaiting, maxInflight int, supportsPriority bool) *Worker
  func (w *Worker) SupportsPriority() bool
  func (w *Worker) Acquire()   // in-flight +1
  func (w *Worker) Release()   // in-flight -1
  func (w *Worker) Admissible() bool // healthy && inflight < maxInflight && !(load.Known && maxWaiting > 0 && waiting >= maxWaiting)
  func New(ws []*Worker) *Pool
  var ErrPoolFull = errors.New("all backends at capacity")
  func (p *Pool) Pick(exclude *Worker) (*Worker, error)
  // least outstanding among Admissible (excluding exclude);
  // ErrNoHealthyWorker if no healthy candidate; ErrPoolFull if healthy ones exist but none admissible
  // observe(st, err) now takes (backend.Load, error); Snapshot sums Waiting/Running.

  type Model struct {
      Name, Upstream string; Restricted bool; DefaultMaxTokens int
      Fallback string // "" = none
      Pool *Pool
  }
  type Fleet struct{ /* models by name, in config order */ }
  func NewFleet(c *config.Config) (*Fleet, error) // builds backends via backend.New
  func (f *Fleet) Model(name string) (*Model, bool)
  func (f *Fleet) Names() []string
  func (f *Fleet) Start(ctx context.Context, interval time.Duration) // polls every pool
  func (f *Fleet) Snapshot() Snapshot                             // summed over all pools
  func (f *Fleet) AnyHealthy() bool
  ```

- [ ] **Step 1: Update existing pool tests** to construct workers with a fake `backend.Backend` (local struct in the test) instead of URLs, keeping every current assertion. Add:
  ```go
  func TestPickPoolFullWhenAllAtMaxInflight(t *testing.T)
  // 2 healthy workers, maxInflight 1, both Acquire()d → ErrPoolFull; Release one → that worker
  func TestPickPoolFullWhenEngineQueueFull(t *testing.T)
  // observe(Load{Waiting:4,Known:true}) with maxWaiting 4 → not admissible; Known:false ignores waiting
  func TestPickNoHealthyIsDistinctFromFull(t *testing.T) // all unhealthy → ErrNoHealthyWorker
  func TestFleetBuildsPoolsFromConfig(t *testing.T)
  // config with models "big" (2 vllm backends, fallback "small") and "small" (1 ollama)
  // assert: Names()==["big","small"], Model("big").Pool.Size()==2, Fallback=="small", Model("x") not ok
  ```
- [ ] **Step 2:** Run `go test ./internal/pool/ -v` → FAIL.
- [ ] **Step 3:** Implement. In-flight is an `atomic.Int64`. Keep `MarkHealthyForTest`.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat(pool): per-model pools with in-flight and queue admission`.

## Task 8: Proxy handler, endpoints and wiring

**Files:**
- Create: `gateway/internal/api/proxy.go`, `gateway/internal/api/proxy_test.go`, `gateway/internal/api/models.go`
- Delete: `gateway/internal/api/chat.go`, `gateway/internal/api/schema.go`, `gateway/internal/api/schema_test.go`
- Modify: `gateway/internal/api/server.go` (`Deps.Pool` → `Deps.Fleet *pool.Fleet`, add `Deps.MaxBodyBytes int64`, `Deps.Ready func(context.Context) error`), `gateway/internal/api/chat_test.go` and `failover_test.go` (port to `backendtest.Fake` + fleet; keep every existing scenario), `gateway/cmd/gateway/main.go`, `docker-compose.yml`

**Interfaces:**
- Consumes: Tasks 1–7.
- Produces: routes `POST /v1/chat/completions`, `POST /v1/completions`, `POST /v1/embeddings`, `GET /v1/models`, `GET /readyz` (200 when `Ready` returns nil and `Fleet.AnyHealthy()`, else 503). Every proxied response carries `X-Request-Id` (16 random bytes, hex). `main.go`: `FORGE_CONFIG` set → `config.Load`; else `config.FromWorkerURLs(WORKER_URLS, WORKER_MODEL default "qwen2.5-1.5b")`. Compose: gateway mounts `deploy/forge.dev.yaml` and sets `FORGE_CONFIG`.

Handler flow per request (spec §6.1, identity steps added in Task 15):
1. `readBody` (413) → `parseRequest` (400) → `resolveModel` against `Fleet.Names()` (400) → `Fleet.Model` (404).
2. `pick := model.Pool.Pick(nil)`. On `ErrPoolFull`/`ErrNoHealthyWorker`: if `model.Fallback != ""` and header `X-Forge-No-Fallback` != `"1"`, switch to the fallback model once (its own `Fallback` ignored) and set `X-Forge-Model-Fallback: <fallback name>`; else 429 overloaded / 503.
3. Attempt: `w.Acquire(); defer w.Release()`; `AddOutstanding(maxTokens or DefaultMaxTokens)` with the existing release-remainder logic from `dispatch`; `body := p.build(model.Upstream, nil)`; `Forward`.
4. Failure handling per Global Constraints retry rule; `*backend.StatusError` → write `se.Code` + `se.Body` verbatim; `ErrUnsupported` → 400 `unsupported_endpoint`; `ErrOverloaded` → 429 overloaded; mid-stream → SSE `worker_lost` event.
5. Metrics: `ObserveTTFT` at `Sink.Start`, `IncRequest(code)`, `IncFailover` on retry, `SetTokensPerSecond` as today.

- [ ] **Step 1: Write failing tests** (`proxy_test.go`, using `backendtest.Fake` and a fleet built from a config literal):
  ```go
  func TestProxyForwardsUnknownFields(t *testing.T)      // fake.LastBody has top_p 0.3 and upstream model name
  func TestProxyRoutesByModel(t *testing.T)              // models "a"→fakeA, "b"→fakeB; model "b" hits only fakeB
  func TestProxyUnknownModel404(t *testing.T)            // body model "zzz" → 404, code "model_not_found"
  func TestProxyBackend4xxPassedThroughNoRetry(t *testing.T)
  // pool [fake400, fakeOK]; fake400 Status 400 Body `{"error":{"message":"ctx too long"}}`
  // assert: client 400, body contains "ctx too long", fakeOK.Requests==0
  func TestProxyFallbackWhenPoolFull(t *testing.T)
  // "big" maxInflight 1 with one held request, fallback "small" → 200 from small,
  // header X-Forge-Model-Fallback=="small"
  func TestProxyNoFallbackHeaderGives429(t *testing.T)   // same + X-Forge-No-Fallback: 1 → 429, Retry-After "1"
  func TestProxyFallbackIsOneHop(t *testing.T)            // big→small→tiny, big and small full → 429, tiny untouched
  func TestProxyEmbeddingsOnForgeIs400(t *testing.T)      // code "unsupported_endpoint"
  func TestModelsListsConfiguredModels(t *testing.T)      // GET /v1/models → object "list", ids ["a","b"]
  func TestReadyz(t *testing.T)                           // Ready nil + healthy → 200; Ready error → 503
  func TestProxySetsRequestID(t *testing.T)               // X-Request-Id is 32 hex chars
  func TestDisconnectReleasesInflight(t *testing.T)
  // Review Focus 2: slow stream, client cancels after first chunk → within 500ms the worker's
  // in-flight is 0, Outstanding()==0, fake saw its request context cancelled, no second backend called
  ```
  Ported tests keep their names and assertions (`TestChatStreamRelaysSSE`, `TestChatNonStreamAggregates`, `TestChatClientDisconnectCancelsHandlerAndWorker`, `TestFailoverRetriesOnAnotherWorkerWhenNoTokensSent`, `TestNonStreamFailsOverToo`, `TestMidStreamFailureIsNotRetriedAndSendsErrorEvent`, `TestNoHealthyWorkerReturns503`, overload 429 tests).
- [ ] **Step 2:** Run `go test ./internal/api/ -v` → FAIL.
- [ ] **Step 3:** Implement `proxy.go` (handler + `httpSink`), `models.go`, rewire `server.go` and `main.go`.
- [ ] **Step 4:** Run `go test ./... && go vet ./...` → PASS; `golangci-lint run` clean.
- [ ] **Step 5:** Commit `feat(api): multi-model pass-through proxy with admission and fallback`.

## Task 9: Layer-2 verification on the laptop (two Ollama)

**Files:**
- Create: `deploy/compose.test.yaml`, `deploy/forge.test.yaml`
- Modify: `benchmarks/chaos_test.sh` (env `MODEL` added to the request body when set; `VICTIM` already configurable)

`compose.test.yaml`: services `gateway` (build `../gateway`, `FORGE_CONFIG=/etc/forge/forge.yaml`), `ollama1`, `ollama2` (`ollama/ollama`, named volumes for models), `postgres` (mounts `deploy/postgres-init.sql` for the dev keys, like today), `redis`, `prometheus`, `grafana`. `forge.test.yaml`: model `small` (`upstream_model: qwen2.5:0.5b`) on both Ollamas; model `solo` (`upstream_model: qwen2.5:0.5b`) on `ollama1` only, `fallback: small`. Task 10 removes the init-sql mount from this file too and Task 17 adds `FORGE_DEV_SEED: "1"` to it; between those tasks the test stack has no keys, which is expected.

- [ ] **Step 1:** `docker compose -f deploy/compose.test.yaml up -d --build`, then `docker compose -f deploy/compose.test.yaml exec ollama1 ollama pull qwen2.5:0.5b` (same for `ollama2`).
- [ ] **Step 2:** Verify with the OpenAI Python SDK pointed at `http://localhost:8080/v1` (streamed reply for `small` and `solo`; `top_p` accepted). Expected: tokens stream, no errors.
- [ ] **Step 3:** `MODEL=small VICTIM=deploy-ollama2-1 bash benchmarks/chaos_test.sh` → `CHAOS TEST: PASS`.
- [ ] **Step 4:** Stop `ollama1`; request `solo` → 200 with `X-Forge-Model-Fallback: small`.
- [ ] **Step 5:** Commit `test: layer-2 compose stack with two ollama backends` (include results summary in the commit body).

---

# Milestone B — Step 3: identity in the gateway's own database

## Task 10: Migrations and the identity schema

**Files:**
- Create: `gateway/internal/store/migrations/001_identity.sql`, `gateway/internal/store/migrate.go`, `gateway/internal/store/migrate_test.go`, `gateway/internal/store/storetest/db.go`
- Delete: `deploy/postgres-init.sql`
- Modify: `docker-compose.yml` and `deploy/compose.test.yaml` (remove the init-sql mount), `.github/workflows/ci.yml` (gateway job: `services: postgres:16-alpine` + `redis:7-alpine`, env `FORGE_TEST_DATABASE_URL`, `FORGE_TEST_REDIS_URL`), `gateway/cmd/gateway/main.go` (call `Migrate` before serving)

**Interfaces:**
- Produces:
  ```go
  func (s *Store) Migrate(ctx context.Context) error
  // under pg_advisory_lock(727301): apply embedded migrations/*.sql in numeric order,
  // each in its own transaction, recording version in schema_migrations; idempotent
  func (s *Store) Ping(ctx context.Context) error
  func (s *Store) DB() *sql.DB                  // for tests and admin queries in this package only
  // package storetest:
  func Open(t *testing.T) *store.Store
  // skips when FORGE_TEST_DATABASE_URL unset; creates schema t_<random>, opens the store with
  // search_path=t_<random> in the DSN, runs Migrate, drops the schema in t.Cleanup
  ```
- `001_identity.sql` = `DROP TABLE IF EXISTS usage_events, api_keys, tenants;` followed by the spec §7.1 SQL **verbatim, without** the `schema_migrations` table (the runner creates that one itself before applying anything).

- [ ] **Step 1: Write failing tests:**
  ```go
  func TestMigrateCreatesSchema(t *testing.T)
  // after Open: tables teams, users, api_keys, model_grants, usage_events, admin_audit,
  // schema_migrations exist; schema_migrations has version 1
  func TestMigrateIsIdempotent(t *testing.T)            // Migrate twice → nil, still one row
  func TestMigrateConcurrentCopies(t *testing.T)
  // two Store values on the same schema, Migrate in parallel → both nil, exactly one version row
  func TestEmailUniqueCaseInsensitive(t *testing.T)
  // insert users 'A@x.com' then 'a@x.com' → second fails with unique violation
  func TestModelGrantNeedsExactlyOneTarget(t *testing.T) // both user_id and team_id → check violation
  ```
- [ ] **Step 2:** Start Postgres, run `go test ./internal/store/ -v` → FAIL.
- [ ] **Step 3:** Implement (`//go:embed migrations/*.sql`).
- [ ] **Step 4:** Run → PASS. Confirm `go test ./...` without the env var skips (not fails) these tests.
- [ ] **Step 5:** Commit `feat(store): embedded migrations and identity schema`.

## Task 11: Store queries for identity and usage

**Files:**
- Modify: `gateway/internal/store/postgres.go` (usage writer), `gateway/internal/api/server.go` (`api.UsageEvent` gets the new fields), `gateway/internal/api/proxy.go` (fill what Identity has today; ids stay 0 until Task 12), `gateway/cmd/gateway/main.go` (`usageAdapter`, `StartBackground`)
- Create: `gateway/internal/store/identity.go`, `gateway/internal/store/identity_test.go`, `gateway/internal/store/usage_test.go`

Note: after Task 10 the running gateway cannot authenticate until Task 12 (the legacy key query is gone). Builds and tests stay green; do not deploy between Tasks 10 and 12.

**Interfaces:**
- Consumes: Task 10 schema, `storetest.Open`.
- Produces:
  ```go
  type KeyRecord struct {
      KeyID, UserID, TeamID int64      // TeamID 0 = none
      KeyPrefix, Role string
      CanViewUsage, Active, Revoked bool
      ExpiresAt *time.Time
      RPMBurst int; RPMRefillPerSec float64
      TokensPerMinute int                         // 0 = unlimited
      DailyBudget, MonthlyBudget, TeamMonthlyCap int64 // 0 = unlimited
      Priority *int                               // COALESCE(user, team); nil = config default
      Grants []string                             // models granted to the user or their team
  }
  func (s *Store) LookupKey(ctx context.Context, hashHex string) (KeyRecord, error) // auth.ErrUnknownKey if absent
  func (s *Store) MarkKeyUsed(id int64)  // non-blocking enqueue; writer goroutine batches
  // UPDATE api_keys SET last_used_at=now() WHERE id = ANY($1) every 60 s
  type UsageEvent struct {
      UserID, TeamID, KeyID int64; Model, RequestedModel, Endpoint string
      PromptTokens, CompletionTokens int; Estimated bool
  }
  func (s *Store) RecordUsage(ev UsageEvent)          // unchanged semantics (drop when queue full)
  func (s *Store) StartBackground(ctx context.Context) // usage writer + last-used writer
  func (s *Store) TokensUsed(ctx context.Context, scope string, id int64, since time.Time) (int64, error)
  // scope "user" | "team": SUM(prompt+completion) since
  type ModelUsage struct { Model string; Requests, PromptTokens, CompletionTokens int64 }
  type UsageReport struct { Requests, PromptTokens, CompletionTokens, TotalTokens int64; ByModel []ModelUsage }
  func (s *Store) UsageForUser(ctx context.Context, userID int64, since time.Time) (UsageReport, error)
  ```
  `TenantIDByKeyHash` and `KeyRecord.TenantID` are removed.

- [ ] **Step 1: Write failing tests** (insert fixtures with plain SQL in the test):
  ```go
  func TestLookupKeyJoinsUserTeamAndGrants(t *testing.T)
  // team T(priority 3, cap 1000), user U(team T, tpm 500, priority NULL), key K, grant model "big" to T
  // assert: rec.UserID, rec.TeamID, rec.TeamMonthlyCap==1000, *rec.Priority==3,
  //         rec.TokensPerMinute==500, rec.Grants==["big"], rec.Active, !rec.Revoked
  func TestLookupKeyUserPriorityWins(t *testing.T)   // user priority 1, team 3 → 1
  func TestLookupKeyUnknown(t *testing.T)            // errors.Is(err, auth.ErrUnknownKey)
  func TestLookupKeyReportsRevokedAndDisabled(t *testing.T) // revoked_at set → Revoked; status disabled → !Active
  func TestUsageWriterAndReports(t *testing.T)
  // RecordUsage two events (models a,b), wait for flush (StartBackground with ctx cancel flushes);
  // TokensUsed("user",U,epoch)==sum; UsageForUser ByModel has 2 entries
  func TestMarkKeyUsedSetsLastUsed(t *testing.T)    // after flush last_used_at not null
  ```
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement. Flush interval for last-used is a package var so the test can shorten it.
- [ ] **Step 4:** Run `go test ./internal/store/ -v` → PASS.
- [ ] **Step 5:** Commit `feat(store): identity lookup and per-user usage queries`.

## Task 12: Authentication per person

**Files:**
- Modify: `gateway/internal/auth/auth.go`, `gateway/internal/auth/auth_test.go`, `gateway/internal/store/adapter.go` (`LookupKeyForAuth` converts `store.KeyRecord` → `auth.KeyRecord`), `gateway/internal/api/*_test.go` (fake auth store to the new interface), `gateway/internal/api/middleware.go` (rate-limit key `"u:<UserID>"`, plan from `RPMBurst`/`RPMRefillPerSec`), `gateway/cmd/gateway/main.go` (`auth.New` call)
- Create: `gateway/internal/auth/keys.go`, `gateway/internal/auth/keys_test.go`

**Interfaces:**
- Produces:
  ```go
  type KeyRecord struct { /* same fields as store.KeyRecord */ }
  type Store interface { LookupKeyForAuth(ctx context.Context, hashHex string) (KeyRecord, error) }
  type Identity struct {
      KeyID, UserID, TeamID int64; KeyHash, KeyPrefix, Role string; CanViewUsage bool
      RPMBurst int; RPMRefillPerSec float64; TokensPerMinute int
      DailyBudget, MonthlyBudget, TeamMonthlyCap int64
      Priority int                // effective, config default applied
      Grants map[string]bool
  }
  func (id Identity) CanUse(model string, restricted bool) bool // !restricted || Grants[model]
  func New(s Store, ttl time.Duration, defaultPriority int, touch func(keyID int64)) *Authenticator
  func (a *Authenticator) Middleware(next http.Handler) http.Handler
  // 401 invalid_api_key when: no bearer, unknown, Revoked, ExpiresAt <= now, !Active
  // touch(keyID) called at most once per key per minute
  func (a *Authenticator) Evict(keyHash string)
  func (a *Authenticator) EvictUser(userID int64)
  func (a *Authenticator) RequireAdmin(next http.Handler) http.Handler // 403 admin_required
  func NewKey() (raw, hashHex, prefix string, err error)
  func HashKey(raw string) string
  ```
  `PlanStore`, `TenantIDByKeyHash`, `Identity.Burst/RefillPerSec/TenantID` are removed.

- [ ] **Step 1: Write failing tests** (rewrite the fake store to the new interface, keep the existing 401/cache/503 tests):
  ```go
  func TestRevokedExpiredDisabledAre401(t *testing.T) // three records → each 401
  func TestCacheTTLThenRevocationTakesEffect(t *testing.T)
  // ttl 50ms; first call 200; store now returns Revoked; immediate call 200 (cached); after 60ms → 401
  func TestEvictUserDropsCachedKeys(t *testing.T)  // EvictUser(U) → next call hits store again
  func TestEffectivePriorityDefault(t *testing.T)  // record Priority nil, default 2 → Identity.Priority==2
  func TestRequireAdmin(t *testing.T)              // role member → 403; admin → 200
  func TestTouchAtMostOncePerMinute(t *testing.T)  // 3 requests → touch called once
  func TestNewKeyFormat(t *testing.T)
  // raw matches ^sk-forge-[0-9A-Za-z]{43}$ (32 bytes base62 → 43 chars, left-padded with '0');
  // hashHex==HashKey(raw); prefix==raw[9:17]; two calls differ
  ```
- [ ] **Step 2:** Run `go test ./internal/auth/ -v` → FAIL.
- [ ] **Step 3:** Implement. Base62 via `math/big` on the 32 bytes, left-padded to 43 characters.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat(auth): per-person identity, revocation and admin guard`.

## Task 13: Per-user request limit and tokens-per-minute limit

**Files:**
- Modify: `gateway/internal/ratelimit/redis.go`, `gateway/internal/ratelimit/ratelimit_test.go`
- Create: `gateway/internal/ratelimit/tpm.go`, `gateway/internal/ratelimit/tpm_test.go`, `gateway/internal/ratelimit/redis_test.go`

**Interfaces:**
- Produces:
  ```go
  type TPMBackend interface {
      TPMCheck(ctx context.Context, key string, perMinute int, now time.Time) (ok bool, waitSec int, err error)
      TPMDebit(ctx context.Context, key string, tokens, perMinute int, now time.Time) error
  }
  // *RedisBackend implements TPMBackend via two Lua scripts on key forge:tpm:u:<id>
  type TPMLimiter struct{ /* backend + local fallback buckets */ }
  func NewTPM(b TPMBackend) *TPMLimiter
  func (l *TPMLimiter) Check(ctx context.Context, userID int64, perMinute int, now time.Time) (ok bool, retryAfter int)
  func (l *TPMLimiter) Debit(ctx context.Context, userID int64, tokens, perMinute int, now time.Time)
  func (l *TPMLimiter) UsingFallback() bool
  ```
  Bucket math (both Redis and local): capacity `C = perMinute`, refill `C/60` per second capped at `C`, starts full; Check allows iff tokens `> 0` after refill; `retryAfter = floor(-tokens / (C/60)) + 1` when not allowed; Debit subtracts with no lower bound. Redis error → local bucket (same as ADR-006). `perMinute <= 0` → always allowed, nothing stored. Request bucket key becomes `forge:rl:u:<userID>` (caller passes `"u:<id>"`; the script prefix stays `forge:rl:`).

- [ ] **Step 1: Write failing tests:**
  ```go
  func TestTPMAllowsWhilePositiveThenBlocks(t *testing.T)
  // perMinute 60, fake backend: Check ok; Debit 100 (goes to -40); Check → !ok, retryAfter==41
  func TestTPMRefills(t *testing.T)        // after Debit 100 at t0, Check at t0+41s → ok
  func TestTPMUnlimited(t *testing.T)      // perMinute 0 → ok, backend never called
  func TestTPMFallsBackLocally(t *testing.T) // backend errors → decisions still enforced, UsingFallback()
  func TestRedisScriptsAgainstRealRedis(t *testing.T)
  // skip without FORGE_TEST_REDIS_URL; request bucket Burst 2 → allow, allow, deny;
  // TPM same scenario as the first test → identical results
  ```
- [ ] **Step 2:** Run `go test ./internal/ratelimit/ -v` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run (with and without `FORGE_TEST_REDIS_URL`) → PASS.
- [ ] **Step 5:** Commit `feat(ratelimit): per-user tokens-per-minute limit`.

## Task 14: Token budgets

**Files:**
- Create: `gateway/internal/budget/budget.go`, `gateway/internal/budget/budget_test.go`, `gateway/internal/budget/redis.go`
- Modify: `gateway/internal/metrics/metrics.go` (add `forge_budget_check_skipped_total`, `IncBudgetSkipped()`)

**Interfaces:**
- Produces:
  ```go
  type Counter interface {
      Get(ctx context.Context, key string) (val int64, exists bool, err error)
      SetNX(ctx context.Context, key string, val int64, ttl time.Duration) error
      AddIfExists(ctx context.Context, key string, n int64) error // Lua: INCRBY only when key exists
  }
  type Source interface { TokensUsed(ctx context.Context, scope string, id int64, since time.Time) (int64, error) }
  type Limits struct { UserID, TeamID int64; Daily, Monthly, TeamMonthly int64 } // 0 = unlimited / no team
  func New(c Counter, src Source) *Budgets
  func (b *Budgets) Check(ctx context.Context, l Limits, now time.Time) bool
  // for each non-zero limit: key missing → rebuild from Source since window start (UTC day / month),
  // SetNX with TTL = window end - now + 1h; reject when used >= limit.
  // Any Counter or Source error → IncBudgetSkipped(), return true
  func (b *Budgets) Add(ctx context.Context, l Limits, tokens int64, now time.Time) // AddIfExists on all three keys
  func NewRedisCounter(rdb *redis.Client) Counter
  ```

- [ ] **Step 1: Write failing tests** (map-backed fake Counter, fake Source):
  ```go
  func TestBudgetRebuildsFromPostgresOnMiss(t *testing.T)
  // Source returns 900 for user today; Daily 1000 → Check true; Add 150 → Check false
  func TestBudgetTeamCapApplies(t *testing.T)   // user unlimited, team cap 100, team used 100 → false
  func TestBudgetUnlimitedSkipsCounters(t *testing.T) // all zero → true, Counter untouched
  func TestBudgetRedisDownAllowsAndCounts(t *testing.T) // Counter errors → true, skip metric +1
  func TestBudgetKeysAndTTL(t *testing.T)
  // now 2026-11-12T10:00Z, user 7, team 3 → keys forge:tok:u:7:d:2026-11-12,
  // forge:tok:u:7:m:2026-11, forge:tok:t:3:m:2026-11; daily TTL == 15h
  ```
- [ ] **Step 2:** Run `go test ./internal/budget/ -v` → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat(budget): daily/monthly token budgets with postgres rebuild`.

## Task 15: Identity in the request path

**Files:**
- Modify: `gateway/internal/api/proxy.go`, `gateway/internal/api/middleware.go`, `gateway/internal/api/models.go`, `gateway/internal/api/server.go` (`Deps` gains `TPM *ratelimit.TPMLimiter`, `Budgets *budget.Budgets`), `gateway/cmd/gateway/main.go`
- Test: `gateway/internal/api/identity_test.go`

**Interfaces:**
- Consumes: Tasks 8, 11–14.
- Produces: middleware order `auth → limits → proxy`. `limits` = request bucket (`"u:<id>"`, plan from Identity) → TPM `Check` → budget `Check`. Proxy additions: grant check after model resolution (403 `model_not_allowed`); fallback only when `CanUse(fallback)`; priority passed to `build` only when the picked worker `SupportsPriority()`; after the attempt, tokens = prompt + completion (or `Sent` estimated on mid-stream failure or client disconnect), then `TPM.Debit`, `Budgets.Add`, `RecordUsage` with served `Model`, `RequestedModel` set only on fallback, `Endpoint`, `KeyID`, `TeamID`. Backend 4xx → nothing charged or recorded. `/v1/models` lists only usable models. `GET /v1/usage` → `UsageForUser` (`days` query param as today).

- [ ] **Step 1: Write failing tests** (fake auth store, fake TPM/budget backends):
  ```go
  func TestRestrictedModelNeedsGrant(t *testing.T)    // restricted "big", no grant → 403; grant → 200
  func TestModelsFilteredByGrants(t *testing.T)       // ids exclude "big" without grant
  func TestFallbackRespectsGrants(t *testing.T)       // big full, fallback restricted & not granted → 429
  func TestPriorityOnlyToSupportingBackend(t *testing.T)
  // Identity.Priority 4; backend supports_priority → fake.LastBody priority==4; other → no priority;
  // client sent priority 0 → never forwarded as 0 to the non-supporting backend
  func TestTPMBlocksAfterBigRequest(t *testing.T)     // tpm 60, response usage 100 tokens → next request 429 tokens_per_minute
  func TestBudgetExceededIs429InsufficientQuota(t *testing.T)
  func TestUsageRecordedAgainstServedModel(t *testing.T) // fallback → event.Model=="small", RequestedModel=="big"
  func TestClientDisconnectRecordsEstimatedUsage(t *testing.T) // Review Focus 2: Estimated true, CompletionTokens==Sent
  func TestBackend4xxChargesNothing(t *testing.T)     // Review Focus 1: no Debit, no Add, no RecordUsage
  ```
- [ ] **Step 2:** Run `go test ./internal/api/ -v` → FAIL.
- [ ] **Step 3:** Implement; wire Redis TPM backend, budget counter and store into `main.go`; `auth.New(..., cfg.AuthCacheTTL, cfg.Defaults.Priority, st.MarkKeyUsed)`.
- [ ] **Step 4:** Run `go test ./... && go vet ./...` → PASS.
- [ ] **Step 5:** Commit `feat(api): per-person limits, budgets, grants and priority in the request path`.

## Task 16: Admin API

**Files:**
- Create: `gateway/internal/admin/admin.go` (routes + handlers), `gateway/internal/admin/admin_test.go`, `gateway/internal/store/admin.go` (queries), `gateway/internal/store/admin_test.go`
- Modify: `gateway/internal/api/server.go` (mount `/admin/v1/` behind `Auth.Middleware` + `Auth.RequireAdmin`)

**Interfaces:**
- Consumes: `store.Store`, `auth.NewKey`, `auth.Authenticator.Evict/EvictUser`.
- Produces routes (JSON in/out, `limit` default 100 max 1000, `offset` default 0 on list endpoints; every write inserts `admin_audit` with `actor_user_id` = caller, `action` like `user.create`, `target` like `user:42`, never a raw key):

  | Method + path | Body / query | Result |
  |---|---|---|
  | `POST /admin/v1/users` | `email, display_name, team_id?, role?, can_view_usage?, rpm_burst?, rpm_refill_per_sec?, tokens_per_minute?, daily_token_budget?, monthly_token_budget?, priority?` | 201 user; omitted limits from `cfg.Defaults` |
  | `GET /admin/v1/users` | `team_id, status` | list |
  | `GET /admin/v1/users/{id}` | | user |
  | `PATCH /admin/v1/users/{id}` | any create field except email (`null` clears a nullable field) | user; `EvictUser` |
  | `POST /admin/v1/users/{id}/disable` / `enable` | | user; disable → `EvictUser` |
  | `GET /admin/v1/users/inactive` | `days` (default 30) | users with no usage in N days |
  | `POST /admin/v1/teams`, `GET /admin/v1/teams`, `PATCH /admin/v1/teams/{id}` | `name, monthly_token_cap?, priority?` | team |
  | `DELETE /admin/v1/teams/{id}` | | 204; 409 if it has users |
  | `POST /admin/v1/users/{id}/keys` | `label, expires_at?` | 201 `{id, key, key_prefix, label, expires_at}` — `key` only here |
  | `GET /admin/v1/users/{id}/keys` | | `[{id, key_prefix, label, created_at, expires_at, revoked_at, last_used_at}]` |
  | `POST /admin/v1/keys/{id}/revoke` | | key; `Evict(hash)` |
  | `POST /admin/v1/grants`, `GET /admin/v1/grants`, `DELETE /admin/v1/grants/{id}` | `model, user_id \| team_id` | grant; unknown model name → 400 |
  | `GET /admin/v1/usage` and `/admin/v1/usage.csv` | `group_by=user\|team\|model`, `from`, `to` (RFC 3339, default last 30 days) | rows `{key, requests, prompt_tokens, completion_tokens, total_tokens}`; `group_by=user` needs `can_view_usage` else 403 |
  | `GET /admin/v1/audit` | `limit, offset` | newest first |

- [ ] **Step 1: Write failing tests** (DB-backed via `storetest.Open`, admin identity injected by a test auth store):
  ```go
  func TestCreateUserAppliesDefaults(t *testing.T)      // rpm_burst==60, priority NULL, audit row "user.create"
  func TestCreateUserDuplicateEmail409(t *testing.T)     // "a@x.com" then "A@X.com" → 409
  func TestCreateKeyReturnsRawOnce(t *testing.T)
  // POST → key matches sk-forge-…; GET list has no "key" field and no hash; audit detail has no "sk-forge-"
  func TestRevokeKeyEvictsCache(t *testing.T)            // spy authenticator records Evict(hash)
  func TestDisableUserEvictsAndBlocks(t *testing.T)      // spy records EvictUser; lookup shows !Active
  func TestDeleteTeamWithUsers409(t *testing.T)
  func TestGrantUnknownModel400(t *testing.T)
  func TestUsageByUserNeedsCanViewUsage(t *testing.T)    // can_view_usage false → 403; group_by=model → 200
  func TestUsageCSV(t *testing.T)                        // header "key,requests,prompt_tokens,completion_tokens,total_tokens"
  func TestInactiveUsers(t *testing.T)                   // user with usage 40 days ago listed for days=30
  func TestNonAdminGets403(t *testing.T)
  ```
- [ ] **Step 2:** Run `go test ./internal/admin/ ./internal/store/ -v` → FAIL.
- [ ] **Step 3:** Implement with Go 1.22+ `ServeMux` patterns (`POST /admin/v1/users/{id}/disable`).
- [ ] **Step 4:** Run → PASS.
- [ ] **Step 5:** Commit `feat(admin): admin api for users, teams, keys, grants, usage and audit`.

## Task 17: CLI bootstrap, user import and dev seed

**Files:**
- Create: `gateway/cmd/gateway/cli.go`, `gateway/cmd/gateway/cli_test.go`
- Modify: `gateway/cmd/gateway/main.go` (dispatch on `os.Args[1]`), `docker-compose.yml` (`FORGE_DEV_SEED: "1"` on gateway), `README.md` (quickstart: dev keys unchanged; how to bootstrap an admin), `deploy/forge.test.yaml` users note

**Interfaces:**
- Produces:
  ```
  gateway                         # serve (default)
  gateway admin bootstrap --email <e>   # fails if any admin exists; creates admin + key; prints key once
  gateway users import <file.csv>       # header email,display_name,team; upsert by lower(email);
                                        # creates missing teams; prints "created N, updated M"; audited (actor NULL)
  ```
  ```go
  func runBootstrap(ctx context.Context, st *store.Store, email string, out io.Writer) error
  func runImport(ctx context.Context, st *store.Store, r io.Reader, out io.Writer) error
  func devSeed(ctx context.Context, st *store.Store) error
  // FORGE_DEV_SEED=1 only: ensure users dev@forge.local (rpm 60/10) and demo@forge.local (rpm 5/1)
  // with keys whose raw values are sk-forge-dev-key / sk-forge-demo-key; idempotent
  ```

- [ ] **Step 1: Write failing tests** (DB-backed):
  ```go
  func TestBootstrapOnce(t *testing.T)       // first → output contains "sk-forge-"; second → error "admin already exists"
  func TestImportUpserts(t *testing.T)
  // CSV with 2 rows (team "ml"), then same file with a changed display_name
  // → "created 2, updated 0" then "created 0, updated 2"; team "ml" exists once
  func TestImportRejectsBadHeader(t *testing.T)
  func TestDevSeedIdempotent(t *testing.T)   // twice → still 2 users; HashKey("sk-forge-dev-key") resolves
  ```
- [ ] **Step 2:** Run `go test ./cmd/gateway/ -v` → FAIL.
- [ ] **Step 3:** Implement with `flag.NewFlagSet` per subcommand and `encoding/csv`.
- [ ] **Step 4:** Run `go test ./...` → PASS; `docker compose up --build` → playground works with `sk-forge-dev-key`; `make ratelimit-test` passes (demo key 5 burst, 1/s).
- [ ] **Step 5:** Commit `feat(cli): admin bootstrap, csv user import and dev seed`.

## Task 18: Layer-3 verification against real vLLM

**Files:**
- Create: `deploy/forge.vllm.yaml` (one model on one vLLM, `supports_priority: true`, `max_waiting: 32`)
- Modify: `docs/superpowers/specs/2026-11-10-production-gateway-design.md` §1.1 or §14 only if a finding contradicts the spec

Run vLLM (laptop CPU build with `Qwen/Qwen2.5-0.5B-Instruct`, or a rented GPU with the production vLLM version), started with `--scheduling-policy priority`.

- [ ] **Step 1:** `curl <vllm>/metrics | grep -E 'vllm:num_requests_(waiting|running)'` → both present. If names differ, fix `ParseVLLMMetrics` and its test first.
- [ ] **Step 2:** Gateway with `deploy/forge.vllm.yaml`: streamed request → `usage_events` row with non-estimated tokens (proves `include_usage` injection).
- [ ] **Step 3:** User priority 0 vs 10 under concurrent load: confirm which value vLLM serves first and that it accepts the field; record the observed direction in the spec §7.5.
- [ ] **Step 4:** Restart vLLM **without** `--scheduling-policy priority`, send a request with `supports_priority: true` still configured → record the behaviour (accepted or 400) in spec §7.5; set the sample config accordingly.
- [ ] **Step 5:** Commit `test: layer-3 verification against vllm` with findings in the body.

---

## Spec coverage map

| Spec | Task |
|---|---|
| §6.1 flow, §6.2 pass-through, §6.6 endpoints | 6, 8, 15 |
| §6.3 relay | 2, 3, 5 |
| §6.4 backends, probes, admission | 4, 5, 7 |
| §6.5 config | 1 |
| §6.7 fallback | 7, 8, 15 |
| §7.1 schema | 10 |
| §7.2 request + TPM limits | 13, 15 |
| §7.3 budgets | 14, 15 |
| §7.4 revocation (≤ 30 s) | 12, 16 |
| §7.5 priority | 6, 15, 18 |
| §7.6 admin API, bootstrap | 16, 17 |
| §7.7 user import, inactive users | 16, 17 |
| §10.1 layers 2–3 | 9, 18 |
| §12 ADRs | 0 |
| §8 hardening, §9 visibility, backend `api_key_env` | Steps 4–5 (separate plan) |
