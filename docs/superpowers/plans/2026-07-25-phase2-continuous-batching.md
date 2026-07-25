# Phase 2 — Continuous Batching: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Continuous batching engine in the worker (low-level llama.cpp), backpressure end to end, a Go loadgen, and a committed sequential-vs-batched benchmark report with a ≥3× target at concurrency 16.

**Architecture:** `worker/engine.py` is a scheduler (admit FCFS → shared decode step → evict) driving a `ModelBackend` protocol; pytest proves the scheduler with a scripted fake backend; `worker/backend_llama.py` implements it with low-level `llama_batch`/`llama_decode`/KV ops; `server.py` selects `ENGINE=sequential|batched`. Gateway maps worker 429 → client 429 + Retry-After and polls worker `/stats` into gauges. `loadgen/` measures; `benchmarks/` records.

**Tech Stack:** Python 3.11 (stdlib threading/queue + numpy + llama-cpp-python pinned), Go 1.26 (loadgen: stdlib only), pandas+matplotlib for plots (local only), pytest in CI.

## Global Constraints

- Engine logic must not import llama_cpp — only `backend_llama.py` may (CI runs scheduler tests without the model).
- Python code stays 3.9-compatible syntax-wise (owner's local python is 3.9; CI uses 3.11): `from __future__ import annotations`, no match statements.
- Crown-jewel readability: explicit state enum, named invariants in comments, small files.
- Sequential engine path must keep working unchanged (`ENGINE=sequential`).
- Benchmarks run locally only; numbers come from committed CSVs; warm-up excluded; hardware disclosed.
- Windows: `curl.exe --%` in PowerShell; Git Bash for `&`-loops; `$env:PATH += ";C:\Program Files\Go\bin"` in fresh shells.

**Phase 2 exit criteria:** measured multiplier at conc=16 (target ≥3×); TTFT p99 at conc 10/50 in report; cancellation evicts from a live batch (pytest + live observation).

---

### Task 1: Branch, version pin, CI pytest job

**Files:**
- Modify: `worker/requirements.txt`, `.github/workflows/ci.yml`

- [ ] **Step 1:** `git checkout -b phase-2-continuous-batching`
- [ ] **Step 2:** Discover the installed llama-cpp-python version (the image already built with it):

Run: `docker compose exec worker pip show llama-cpp-python | head -2`
Pin exactly that version in `worker/requirements.txt` (replace the `>=` line with `llama-cpp-python==<found>`), and add `numpy` (it is already a transitive dep; declare it since we now import it).

- [ ] **Step 3:** Add pytest job to `.github/workflows/ci.yml` after the `worker` job:

```yaml
  worker-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - name: Scheduler unit tests (no model)
        run: |
          pip install pytest
          python -m pytest worker/tests -q
```

- [ ] **Step 4:** Commit: `git add -A && git commit -m "chore(worker): pin llama-cpp-python, add pytest ci job"`

---

### Task 2: ChatML formatting (TDD)

**Files:**
- Create: `worker/chatml.py`
- Test: `worker/tests/test_chatml.py` (and empty `worker/tests/__init__.py`)

**Interfaces:**
- Produces: `format_chatml(messages: list[dict]) -> str` — messages have `role`/`content`; Task 5 calls it with the request's messages.

- [ ] **Step 1: Failing test** — `worker/tests/test_chatml.py`

```python
from chatml import format_chatml


def test_format_single_user_message():
    got = format_chatml([{"role": "user", "content": "hi"}])
    assert got == "<|im_start|>user\nhi<|im_end|>\n<|im_start|>assistant\n"


def test_format_system_and_user():
    got = format_chatml([
        {"role": "system", "content": "be brief"},
        {"role": "user", "content": "hi"},
    ])
    assert got == (
        "<|im_start|>system\nbe brief<|im_end|>\n"
        "<|im_start|>user\nhi<|im_end|>\n"
        "<|im_start|>assistant\n"
    )
```

Run (from `worker/`): `python -m pytest tests/test_chatml.py -q` → FAIL (no module).

- [ ] **Step 2: Implement** — `worker/chatml.py`

```python
"""ChatML prompt formatting (Qwen2.5's template).

Hand-rolled because Phase 2 owns the decode loop and therefore the prompt.
Safe to hardcode: the served model is pinned in config/compose (ADR-003).
"""
from __future__ import annotations


def format_chatml(messages: list[dict]) -> str:
    parts = []
    for m in messages:
        parts.append("<|im_start|>%s\n%s<|im_end|>\n" % (m["role"], m["content"]))
    parts.append("<|im_start|>assistant\n")
    return "".join(parts)
```

- [ ] **Step 3:** Test passes → commit `feat(worker): chatml prompt formatting`.

---

### Task 3: Engine scheduler (TDD — the crown jewel's logic)

**Files:**
- Create: `worker/engine.py`
- Test: `worker/tests/test_engine.py`

**Interfaces:**
- Produces (consumed by Tasks 4–5):
  - `State` enum: WAITING/PREFILL/DECODING/FINISHED/CANCELLED
  - `Sequence(prompt, max_tokens, temperature)` with `.out` (queue of str pieces, `None` sentinel = stream end), `.cancelled` (threading.Event), `.finish_reason`, `.prompt_tokens`, `.seq_id`
  - `ModelBackend` protocol: `prefill(seq_id, prompt) -> tuple[int, str | None]` (prompt token count, first sampled piece or None if EOS-at-once); `step(seqs) -> list[str | None]` (next piece per DECODING seq, None = EOS); `release(seq_id) -> None`
  - `Engine(backend, max_batch, max_queue)`: `.submit(seq)` (raises `QueueFullError`), `.step_once()` (one admit→step→evict iteration, for tests), `.start()/.stop()` (decode-loop thread), `.stats() -> dict`

- [ ] **Step 1: Failing tests** — `worker/tests/test_engine.py`

```python
import threading

import pytest

from engine import Engine, QueueFullError, Sequence, State


class FakeBackend:
    """Scripted backend: script maps prompt -> list of pieces; None = EOS."""

    def __init__(self, script=None):
        self.script = dict(script or {})
        self.prefilled = []
        self.released = []

    def prefill(self, seq_id, prompt):
        self.prefilled.append((seq_id, prompt))
        pieces = self.script.get(prompt)
        first = pieces.pop(0) if pieces else "t"
        return (3, first)

    def step(self, seqs):
        out = []
        for s in seqs:
            pieces = self.script.get(s.prompt)
            out.append(pieces.pop(0) if pieces else "t")
        return out

    def release(self, seq_id):
        self.released.append(seq_id)


def drain(seq):
    got = []
    while True:
        p = seq.out.get(timeout=1)
        if p is None:
            return got
        got.append(p)


def test_single_sequence_runs_to_eos():
    be = FakeBackend({"p": ["a", "b", None]})
    e = Engine(be, max_batch=2, max_queue=8)
    s = Sequence(prompt="p", max_tokens=10)
    e.submit(s)
    for _ in range(5):
        e.step_once()
    assert drain(s) == ["a", "b"]
    assert s.state is State.FINISHED
    assert s.finish_reason == "stop"
    assert s.prompt_tokens == 3
    assert be.released == [s.seq_id]


def test_max_tokens_caps_generation():
    be = FakeBackend()  # infinite "t"s
    e = Engine(be, max_batch=1, max_queue=8)
    s = Sequence(prompt="p", max_tokens=3)
    e.submit(s)
    for _ in range(6):
        e.step_once()
    assert drain(s) == ["t", "t", "t"]
    assert s.finish_reason == "length"


def test_two_sequences_share_a_step():
    be = FakeBackend()
    e = Engine(be, max_batch=2, max_queue=8)
    a, b = Sequence(prompt="a", max_tokens=2), Sequence(prompt="b", max_tokens=2)
    e.submit(a)
    e.submit(b)
    e.step_once()  # both admitted (prefill emits first piece), then one shared step
    # after one iteration both have first piece + one stepped piece
    assert a.out.qsize() >= 1 and b.out.qsize() >= 1


def test_fcfs_admission_and_slot_reuse():
    be = FakeBackend({"a": ["x", None]})
    e = Engine(be, max_batch=1, max_queue=8)
    a, b = Sequence(prompt="a", max_tokens=5), Sequence(prompt="b", max_tokens=1)
    e.submit(a)
    e.submit(b)
    e.step_once()          # a admitted (b waits: no slot)
    assert [p for p, _ in [(x[1], 0) for x in be.prefilled]] == ["a"]
    for _ in range(3):
        e.step_once()      # a finishes, slot freed, b admitted into same slot
    assert be.prefilled[1][1] == "b"
    assert be.prefilled[0][0] == be.prefilled[1][0]  # same slot id reused


def test_cancel_mid_decode_evicts_and_frees_slot():
    be = FakeBackend()
    e = Engine(be, max_batch=1, max_queue=8)
    s = Sequence(prompt="p", max_tokens=100)
    e.submit(s)
    e.step_once()
    s.cancelled.set()
    e.step_once()
    assert s.state is State.CANCELLED
    assert be.released == [s.seq_id]
    # sentinel delivered so the HTTP generator unblocks
    drained = drain(s)
    assert isinstance(drained, list)
    # freed slot admits the next request
    n = Sequence(prompt="n", max_tokens=1)
    e.submit(n)
    e.step_once()
    assert be.prefilled[-1][1] == "n"


def test_cancel_while_waiting_never_prefills():
    be = FakeBackend()
    e = Engine(be, max_batch=1, max_queue=8)
    a = Sequence(prompt="a", max_tokens=100)
    b = Sequence(prompt="b", max_tokens=1)
    e.submit(a)
    e.submit(b)
    b.cancelled.set()
    for _ in range(4):
        e.step_once()
    assert all(p != "b" for _, p in be.prefilled)
    assert b.state is State.CANCELLED


def test_queue_full_raises():
    e = Engine(FakeBackend(), max_batch=1, max_queue=2)
    e.submit(Sequence(prompt="a"))
    e.submit(Sequence(prompt="b"))
    with pytest.raises(QueueFullError):
        e.submit(Sequence(prompt="c"))


def test_stats_reports_queue_and_active():
    be = FakeBackend()
    e = Engine(be, max_batch=1, max_queue=8)
    e.submit(Sequence(prompt="a", max_tokens=5))
    e.submit(Sequence(prompt="b", max_tokens=5))
    e.step_once()
    st = e.stats()
    assert st["active_seqs"] == 1
    assert st["queue_depth"] == 1


def test_immediate_eos_at_prefill():
    be = FakeBackend({"p": [None]})
    e = Engine(be, max_batch=1, max_queue=8)
    s = Sequence(prompt="p", max_tokens=5)
    e.submit(s)
    e.step_once()
    assert drain(s) == []
    assert s.state is State.FINISHED
    assert s.finish_reason == "stop"


def test_background_thread_runs():
    be = FakeBackend({"p": ["a", None]})
    e = Engine(be, max_batch=1, max_queue=8)
    e.start()
    try:
        s = Sequence(prompt="p", max_tokens=5)
        e.submit(s)
        assert drain(s) == ["a"]
    finally:
        e.stop()
```

Run: `python -m pytest tests/test_engine.py -q` → FAIL (`No module named engine`).

- [ ] **Step 2: Implement** — `worker/engine.py`

```python
"""Continuous batching engine — the scheduler half of the crown jewel.

One decode-loop thread owns the model. HTTP handlers submit Sequence
objects and read pieces from seq.out; the loop admits at iteration
boundaries, shares one decode step across all active sequences, and
evicts finished/cancelled sequences immediately.

State machine:  WAITING -> PREFILL -> DECODING -> FINISHED
                              \\------------\\--> CANCELLED

Invariants (checked by tests):
- I1: a slot id is in free_slots XOR owned by exactly one seq in active.
- I2: admission order is FCFS (waiting is a FIFO queue).
- I3: every submitted sequence eventually gets the None sentinel on .out
      (FINISHED, CANCELLED, or cancelled-while-waiting).
- I4: backend.release() is called exactly once per admitted sequence.

This module must not import llama_cpp — the ModelBackend protocol keeps
scheduler logic testable in CI without the model (see backend_llama.py).
"""
from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class State(Enum):
    WAITING = auto()
    PREFILL = auto()
    DECODING = auto()
    FINISHED = auto()
    CANCELLED = auto()


class QueueFullError(Exception):
    """Raised by submit() when the bounded waiting queue is full (429)."""


@dataclass
class Sequence:
    prompt: str = ""
    max_tokens: int = 256
    temperature: float = 0.7
    seq_id: int = -1
    state: State = State.WAITING
    n_generated: int = 0
    prompt_tokens: int = 0
    finish_reason: str = ""
    out: "queue.Queue[Optional[str]]" = field(default_factory=queue.Queue)
    cancelled: threading.Event = field(default_factory=threading.Event)


class Engine:
    def __init__(self, backend, max_batch: int = 8, max_queue: int = 32):
        self.backend = backend
        self.max_batch = max_batch
        self.waiting: "queue.Queue[Sequence]" = queue.Queue(maxsize=max_queue)
        self.active: list[Sequence] = []
        self.free_slots = list(range(max_batch))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    # -- public API (called from HTTP handler threads) --------------------

    def submit(self, seq: Sequence) -> None:
        try:
            self.waiting.put_nowait(seq)
        except queue.Full:
            raise QueueFullError()

    def stats(self) -> dict:
        return {
            "queue_depth": self.waiting.qsize(),
            "active_seqs": len(self.active),
            "max_batch": self.max_batch,
        }

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    # -- decode loop ------------------------------------------------------

    def _run(self) -> None:
        while not self._stop.is_set():
            if not self.active and self.waiting.empty():
                time.sleep(0.005)  # idle; nothing to admit or step
                continue
            self.step_once()

    def step_once(self) -> None:
        """One iteration: admit -> step -> evict. Public for unit tests."""
        self._admit()
        self._decode_step()
        self._evict()

    def _admit(self) -> None:
        while self.free_slots and not self.waiting.empty():
            seq = self.waiting.get_nowait()
            if seq.cancelled.is_set():
                # Cancelled before ever touching the model: no slot consumed.
                seq.state = State.CANCELLED
                seq.out.put(None)
                continue
            seq.seq_id = self.free_slots.pop(0)
            seq.state = State.PREFILL
            n_prompt, first = self.backend.prefill(seq.seq_id, seq.prompt)
            seq.prompt_tokens = n_prompt
            if first is None:
                # Model emitted EOS on the very first sample.
                seq.state = State.FINISHED
                seq.finish_reason = "stop"
            else:
                seq.out.put(first)
                seq.n_generated = 1
                if seq.n_generated >= seq.max_tokens:
                    seq.state = State.FINISHED
                    seq.finish_reason = "length"
                else:
                    seq.state = State.DECODING
            self.active.append(seq)

    def _decode_step(self) -> None:
        seqs = [
            s for s in self.active
            if s.state is State.DECODING and not s.cancelled.is_set()
        ]
        if not seqs:
            return
        pieces = self.backend.step(seqs)
        for s, piece in zip(seqs, pieces):
            if piece is None:
                s.state = State.FINISHED
                s.finish_reason = "stop"
                continue
            s.n_generated += 1
            s.out.put(piece)
            if s.n_generated >= s.max_tokens:
                s.state = State.FINISHED
                s.finish_reason = "length"

    def _evict(self) -> None:
        keep: list[Sequence] = []
        for s in self.active:
            if s.cancelled.is_set() and s.state is State.DECODING:
                s.state = State.CANCELLED
            if s.state in (State.FINISHED, State.CANCELLED):
                self.backend.release(s.seq_id)
                self.free_slots.append(s.seq_id)
                s.out.put(None)  # I3: unblock the HTTP generator
            else:
                keep.append(s)
        self.active = keep
```

- [ ] **Step 3:** All engine tests pass. Run the full worker suite: `python -m pytest tests -q`. Commit: `feat(worker): continuous batching scheduler with fcfs admission, eviction, cancellation`.

---

### Task 4: Real llama.cpp backend (low-level)

**Files:**
- Create: `worker/backend_llama.py`, `worker/kvcache.py`
- Verify: probe + smoke scripts run **inside the container** (scratchpad, not committed)

**Interfaces:**
- Consumes: `ModelBackend` protocol from Task 3.
- Produces: `LlamaBackend(model_path, n_ctx, max_batch, n_threads=None)` implementing prefill/step/release; `kvcache.SlotPositions` tracking per-slot next position.

- [ ] **Step 1: Probe the installed low-level API** (names churn between versions). Run inside the container:

```bash
docker compose exec worker python -c "import llama_cpp, inspect; names=[n for n in dir(llama_cpp) if n.startswith('llama_')]; print('\n'.join(n for n in names if any(k in n for k in ('batch','decode','tokenize','logits','kv','memory','token_','vocab','model','context','sampler'))))"
```

Record which of these exist (adapt the reference code below to the real names): `llama_load_model_from_file` vs `llama_model_load_from_file`; `llama_new_context_with_model` vs `llama_init_from_model`; `llama_kv_cache_seq_rm` vs `llama_kv_self_seq_rm` vs `llama_memory_seq_rm`; `llama_token_is_eog(model,...)` vs vocab-based; `llama_tokenize(model,...)` vs `llama_tokenize(vocab,...)`; `llama_token_to_piece` signature.

- [ ] **Step 2: Write `worker/kvcache.py`**

```python
"""Per-slot KV bookkeeping: each slot (seq id) tracks the next write
position in its logical KV sequence. Slot lifecycle is owned by the
engine; this module only tracks positions and produces clear ranges.
"""
from __future__ import annotations


class SlotPositions:
    def __init__(self) -> None:
        self._pos: dict[int, int] = {}

    def begin(self, slot: int, n_prompt: int) -> None:
        self._pos[slot] = n_prompt

    def next(self, slot: int) -> int:
        return self._pos[slot]

    def advance(self, slot: int) -> None:
        self._pos[slot] += 1

    def end(self, slot: int) -> None:
        self._pos.pop(slot, None)
```

- [ ] **Step 3: Write `worker/backend_llama.py`** (reference implementation — adapt symbol names to the probe results):

```python
"""Real ModelBackend: drives llama.cpp's low-level API.

Why low-level: continuous batching needs one llama_decode per iteration
covering many sequences, per-sequence KV clearing, and sampling from
per-sequence logits — none of which the high-level API exposes.

Sampling is manual (numpy softmax over logits with temperature; argmax at
temperature 0). top_p is deferred until a measured need (spec).
"""
from __future__ import annotations

import ctypes

import numpy as np

import llama_cpp
from kvcache import SlotPositions


class LlamaBackend:
    def __init__(self, model_path: str, n_ctx: int, max_batch: int,
                 n_threads=None) -> None:
        llama_cpp.llama_backend_init()
        mparams = llama_cpp.llama_model_default_params()
        self.model = llama_cpp.llama_load_model_from_file(
            model_path.encode("utf-8"), mparams)
        cparams = llama_cpp.llama_context_default_params()
        cparams.n_ctx = n_ctx
        cparams.n_batch = 2048
        cparams.n_seq_max = max_batch  # KV cache shared by up to max_batch seqs
        if n_threads:
            cparams.n_threads = n_threads
            cparams.n_threads_batch = n_threads
        self.ctx = llama_cpp.llama_new_context_with_model(self.model, cparams)
        self.batch = llama_cpp.llama_batch_init(2048, 0, max_batch)
        self.pos = SlotPositions()
        self.cur_token: dict[int, int] = {}   # slot -> last sampled token
        self.utf8_buf: dict[int, bytes] = {}  # slot -> undecoded byte tail
        self.n_vocab = llama_cpp.llama_n_vocab(self.model)
        self.rng = np.random.default_rng()

    # -- ModelBackend ----------------------------------------------------

    def prefill(self, seq_id: int, prompt: str):
        toks = self._tokenize(prompt)
        n = len(toks)
        # Feed the prompt in n_batch-sized chunks; logits only for the
        # final position (that's where the first output token comes from).
        i = 0
        while i < n:
            chunk = toks[i:i + 2048]
            last_chunk = (i + len(chunk) == n)
            self._fill_batch_prompt(seq_id, chunk, start_pos=i,
                                    want_logits_last=last_chunk)
            if llama_cpp.llama_decode(self.ctx, self.batch) != 0:
                raise RuntimeError("llama_decode failed during prefill")
            i += len(chunk)
        self.pos.begin(seq_id, n)
        self.utf8_buf[seq_id] = b""
        tok = self._sample(self.batch.n_tokens - 1, temperature=0.7)
        if self._is_eog(tok):
            return (n, None)
        self.cur_token[seq_id] = tok
        return (n, self._piece(seq_id, tok))

    def step(self, seqs):
        # One shared decode: newest token of every sequence, one batch.
        self.batch.n_tokens = len(seqs)
        for i, s in enumerate(seqs):
            self.batch.token[i] = self.cur_token[s.seq_id]
            self.batch.pos[i] = self.pos.next(s.seq_id)
            self.batch.n_seq_id[i] = 1
            self.batch.seq_id[i][0] = s.seq_id
            self.batch.logits[i] = True
        if llama_cpp.llama_decode(self.ctx, self.batch) != 0:
            raise RuntimeError("llama_decode failed during step")
        out = []
        for i, s in enumerate(seqs):
            self.pos.advance(s.seq_id)
            tok = self._sample(i, temperature=s.temperature)
            if self._is_eog(tok):
                out.append(None)
            else:
                self.cur_token[s.seq_id] = tok
                out.append(self._piece(s.seq_id, tok))
        return out

    def release(self, seq_id: int) -> None:
        # Free this sequence's KV range so the slot can be reused.
        llama_cpp.llama_kv_cache_seq_rm(self.ctx, seq_id, 0, -1)
        self.pos.end(seq_id)
        self.cur_token.pop(seq_id, None)
        self.utf8_buf.pop(seq_id, None)

    # -- internals -------------------------------------------------------

    def _fill_batch_prompt(self, seq_id, toks, start_pos, want_logits_last):
        self.batch.n_tokens = len(toks)
        for i, t in enumerate(toks):
            self.batch.token[i] = t
            self.batch.pos[i] = start_pos + i
            self.batch.n_seq_id[i] = 1
            self.batch.seq_id[i][0] = seq_id
            self.batch.logits[i] = bool(want_logits_last and i == len(toks) - 1)

    def _tokenize(self, text: str):
        b = text.encode("utf-8")
        n_max = len(b) + 32
        arr = (llama_cpp.llama_token * n_max)()
        n = llama_cpp.llama_tokenize(
            self.model, b, len(b), arr, n_max, True, True)
        if n < 0:
            raise RuntimeError("tokenize buffer too small")
        return list(arr[:n])

    def _sample(self, logit_idx: int, temperature: float) -> int:
        ptr = llama_cpp.llama_get_logits_ith(self.ctx, logit_idx)
        logits = np.ctypeslib.as_array(ptr, shape=(self.n_vocab,)).copy()
        if temperature <= 0:
            return int(logits.argmax())
        logits /= temperature
        logits -= logits.max()
        p = np.exp(logits)
        p /= p.sum()
        return int(self.rng.choice(self.n_vocab, p=p))

    def _is_eog(self, tok: int) -> bool:
        return bool(llama_cpp.llama_token_is_eog(self.model, tok))

    def _piece(self, seq_id: int, tok: int) -> str:
        buf = ctypes.create_string_buffer(64)
        n = llama_cpp.llama_token_to_piece(self.model, tok, buf, 64, 0, False)
        raw = self.utf8_buf[seq_id] + buf.raw[:n]
        try:
            text = raw.decode("utf-8")
            self.utf8_buf[seq_id] = b""
            return text
        except UnicodeDecodeError:
            # Partial multi-byte char: hold bytes until the next piece.
            self.utf8_buf[seq_id] = raw
            return ""
```

**Protocol note (binding):** `prefill` takes the request's temperature: the
signature everywhere is `prefill(seq_id, prompt, temperature) -> (n, first)`.
Engine calls `self.backend.prefill(seq.seq_id, seq.prompt, seq.temperature)`;
the FakeBackend in Task 3 uses `def prefill(self, seq_id, prompt, temperature=0.7)`;
`LlamaBackend.prefill` passes it to `_sample`. (The reference code above shows
`temperature=0.7` hardcoded — correct it when implementing.)

- [ ] **Step 4: In-container smoke test** (script to scratchpad, run via `docker compose exec`): build a `LlamaBackend`, prefill two prompts on slots 0 and 1, run 20 shared steps, print both outputs — verifies parallel generation produces coherent text for both. Expected: two readable sentences.

- [ ] **Step 5:** Commit `feat(worker): low-level llama.cpp backend (shared decode, kv mgmt, manual sampling)`.

---

### Task 5: Wire the engine into the worker

**Files:**
- Modify: `worker/server.py`, `worker/config.py`, `docker-compose.yml`

**Interfaces:**
- Produces: `ENGINE=batched|sequential` switch; `GET /stats` → `{"queue_depth":n,"active_seqs":n,"max_batch":n}` (zeros + `"engine":"sequential"` in sequential mode); `/generate` unchanged NDJSON contract; 429 JSON `{"error":"overloaded"}` when queue full.

- [ ] **Step 1:** `worker/config.py` add:

```python
ENGINE = os.environ.get("ENGINE", "batched")
MAX_BATCH = int(os.environ.get("MAX_BATCH", "8"))
MAX_QUEUE = int(os.environ.get("MAX_QUEUE", "32"))
```

- [ ] **Step 2:** `worker/server.py` — keep the sequential path exactly as-is behind `if config.ENGINE == "sequential"`; batched path:

```python
# module init (batched mode)
from backend_llama import LlamaBackend
from chatml import format_chatml
from engine import Engine, QueueFullError, Sequence

backend = LlamaBackend(config.MODEL_PATH, config.N_CTX, config.MAX_BATCH,
                       config.N_THREADS)
engine = Engine(backend, config.MAX_BATCH, config.MAX_QUEUE)
engine.start()


@app.post("/generate")
def generate(req: GenerateRequest):
    seq = Sequence(prompt=format_chatml([m.model_dump() for m in req.messages]),
                   max_tokens=req.max_tokens, temperature=req.temperature)
    try:
        engine.submit(seq)
    except QueueFullError:
        return JSONResponse({"error": "overloaded"}, status_code=429)

    def stream():
        try:
            n = 0
            while True:
                piece = seq.out.get()
                if piece is None:
                    break
                if piece == "":
                    continue  # partial utf-8 held back
                n += 1
                yield json.dumps({"token": piece, "done": False}) + "\n"
            yield json.dumps({"done": True, "prompt_tokens": seq.prompt_tokens,
                              "completion_tokens": seq.n_generated,
                              "finish_reason": seq.finish_reason or "stop"}) + "\n"
        finally:
            # Client gone (GeneratorExit lands at a yield) or normal end:
            # either way, mark cancelled so the engine evicts if still live.
            seq.cancelled.set()

    return StreamingResponse(stream(), media_type="application/x-ndjson")


@app.get("/stats")
def stats():
    return engine.stats()
```

(Structure the file so sequential/batched share the FastAPI app and models; only `/generate` + `/stats` bodies differ by mode.)

- [ ] **Step 3:** compose worker env: add `ENGINE: ${ENGINE:-batched}` — shell var switches engines: `ENGINE=sequential docker compose up -d worker`.
- [ ] **Step 4:** Rebuild, end-to-end checks: healthy worker; authed streaming curl through the gateway returns coherent text; **live cancellation** — start a long stream, Ctrl+C after a few tokens, then `curl /stats` inside the network shows `active_seqs` back at 0 (eviction observed).
- [ ] **Step 5:** Concurrency sanity: re-run the 10-parallel-curl loop — expect roughly *flat* completion times instead of Phase 1's staircase.
- [ ] **Step 6:** Commit `feat(worker): batched engine wired into server with stats and backpressure`.

---

### Task 6: Gateway — 429 passthrough + stats gauges (TDD)

**Files:**
- Modify: `gateway/internal/worker/client.go` (typed `ErrOverloaded`; `Stats` method), `gateway/internal/api/chat.go` (defer SSE headers until first event; map overload → 429 + Retry-After), `gateway/internal/metrics/metrics.go` (2 gauges), `gateway/cmd/gateway/main.go` (stats poller goroutine)
- Test: `gateway/internal/api/chat_test.go` (new case), `gateway/internal/worker/client_test.go` (429 → ErrOverloaded)

Key changes:

```go
// client.go
var ErrOverloaded = errors.New("worker overloaded")
// in Generate, after Do():
if resp.StatusCode == http.StatusTooManyRequests { return ErrOverloaded }

// Stats fetches /stats (used by the metrics poller).
type StatsResponse struct {
	QueueDepth int `json:"queue_depth"`
	ActiveSeqs int `json:"active_seqs"`
	MaxBatch   int `json:"max_batch"`
}
func (c *Client) Stats(ctx context.Context) (StatsResponse, error) { ... plain GET+decode ... }
```

```go
// chat.go stream(): do NOT set SSE headers up front; set them inside the
// callback on the first event (headers are mutable until first write).
// After Generate: if errors.Is(err, worker.ErrOverloaded) && sent == 0 {
//     w.Header().Set("Retry-After", "1")
//     metrics.IncRequest(http.StatusTooManyRequests)
//     http.Error(w, `{"error":{"message":"server overloaded, retry"}}`,
//         http.StatusTooManyRequests)
//     return
// }
// aggregate(): same mapping, no header subtlety.
```

```go
// metrics.go additions
workerQueueDepth = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "forge_worker_queue_depth", Help: "Worker waiting-queue depth."})
batchOccupancy = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "forge_batch_occupancy", Help: "Active sequences in the batch."})
func SetWorkerStats(queue, active int) {
	workerQueueDepth.Set(float64(queue)); batchOccupancy.Set(float64(active))
}
```

```go
// main.go: poller goroutine (2s ticker, context-cancelled on shutdown)
go func() {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if st, err := wc.Stats(context.Background()); err == nil {
				metrics.SetWorkerStats(st.QueueDepth, st.ActiveSeqs)
			}
		}
	}
}()
```

Tests: fake worker returning 429 → client gets 429 + Retry-After header (stream and non-stream); existing tests still green (header-deferral must not break the SSE test).

- [ ] Commit `feat(gateway): overload passthrough with retry-after, worker stats gauges`.

---

### Task 7: Loadgen

**Files:**
- Create: `loadgen/go.mod` (module `github.com/Zanthium09/forge-serve/loadgen`, go 1.22), `loadgen/main.go`

CLI: `loadgen -url http://localhost:8080 -key sk-forge-dev-key -mode closed -concurrency 16 -duration 120s -max-tokens 64 -prompt-words 32 -warmup 15s -out benchmarks/raw/run.csv`

Behavior: each virtual request = streaming chat completion; measure TTFT (first content chunk), total, count chunks. Closed mode: `-concurrency` goroutines looping until deadline. Open mode: ticker at `1/rate` firing request goroutines. CSV columns: `id,mode,start_unix_ms,ttft_ms,total_ms,tokens,tpot_ms,status`. Rows inside `-warmup` are tagged `warmup=1` (analysis drops them). Print a stdout summary at end (requests, errors, throughput tok/s, TTFT p50/p95/p99 — computed by sorting).

(~250 lines, stdlib only: flag, net/http, bufio, encoding/csv, sort, sync, time. Full code left to implementation; the NDJSON→SSE parsing mirrors `chat_test.go`'s scanner loop.)

- [ ] Self-check: `go vet ./...`; a 20s smoke run against the live stack produces a CSV with sane TTFTs and a summary. Commit `feat(loadgen): open/closed-loop generator with per-request csv`.

---

### Task 8: Benchmarks

**Files:**
- Create: `benchmarks/run_bench.sh` (Git Bash), `benchmarks/plot.py`, `benchmarks/report.md`; raw CSVs under `benchmarks/raw/`
- Modify: `Makefile` (`bench` target), `.gitignore` (do NOT ignore benchmarks/raw)

`run_bench.sh` outline: for ENGINE in sequential batched → `ENGINE=$ENGINE docker compose up -d worker && wait-for-healthy` → closed-loop conc 1, 8, 16 (120s each, 15s warmup) → open-loop rates giving conc≈10 and ≈50 for TTFT percentiles → CSVs named `{engine}_{mode}_{param}.csv`. 3 repetitions of the conc=16 runs; median reported.

`plot.py` (pandas+matplotlib, run locally): throughput bar chart (seq vs batched by concurrency), TTFT CDF/percentile table, tokens/sec-vs-batch-size curve. Outputs PNG into `benchmarks/`.

`report.md`: headline multiplier, tables, methodology (open vs closed loop explained, warmup exclusion, 3-run median, hardware: Ultra 9 185H / 32 GB / Windows 11 + Docker Desktop WSL2, model: Qwen2.5-1.5B Q4_K_M, exact commit hash).

- [ ] Run the full matrix; verify ≥3× or report honest number; commit CSVs + plots + report.

---

### Task 9: ADR-003, docs, finish

**Files:**
- Create: `docs/adr/003-fcfs-bounded-queue-scheduling.md`
- Modify: `README.md` (benchmark table + headline from report), `CLAUDE.md` (phase status), `FORGE_SERVE_BLUEPRINT.md` (nothing unless numbers force an amendment)

ADR-003 content: FCFS admission + bounded queue + immediate 429; why not SJF (needs length prediction, starvation risk) yet; aging deferred to Phase 6 priorities; ChatML hardcoding note; sampling simplification (temperature-only) note.

- [ ] Exit-criteria checklist against the report; update CLAUDE.md to Phase 3; merge to main; push; CI green (now 3 jobs); delete branch.
