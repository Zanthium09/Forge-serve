"""Continuous batching engine - the scheduler half of the crown jewel.

One decode-loop thread owns the model. HTTP handlers submit Sequence
objects and read pieces from seq.out; the loop admits at iteration
boundaries, shares one decode step across all active sequences, and
evicts finished/cancelled sequences immediately.

State machine:  WAITING -> PREFILL -> DECODING -> FINISHED
                              |            |
                              +------------+--> CANCELLED

Invariants (each covered by tests/test_engine.py):
- I1: a slot id is in free_slots XOR owned by exactly one seq in active.
- I2: admission order is FCFS (waiting is a FIFO queue).
- I3: every submitted sequence eventually gets the None sentinel on .out
      (FINISHED, CANCELLED, or cancelled-while-waiting).
- I4: backend.release() is called exactly once per admitted sequence.

This module must not import llama_cpp - the ModelBackend protocol keeps
scheduler logic testable in CI without the model (see backend_llama.py).
"""
from __future__ import annotations

import queue
import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto


class State(Enum):
    WAITING = auto()
    PREFILL = auto()
    DECODING = auto()
    FINISHED = auto()
    CANCELLED = auto()


class QueueFullError(Exception):
    """Raised by submit() when the bounded waiting queue is full (-> 429)."""


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
    out: queue.Queue[str | None] = field(default_factory=queue.Queue)
    cancelled: threading.Event = field(default_factory=threading.Event)
    # Optional wakeup hook set by the HTTP layer. Without it readers poll,
    # and polling readers take the GIL from this decode loop: measured
    # 40.2 -> 43.2 tok/s at 8 concurrent streams when polling was replaced
    # by this callback.
    notify: Callable[[], None] | None = None

    def emit(self, piece: str | None) -> None:
        """Single place that hands a piece (or the end sentinel) to the
        reader, so the wakeup can never be forgotten."""
        self.out.put(piece)
        if self.notify is not None:
            self.notify()


class Engine:
    """Scheduler: admit (FCFS) -> shared decode step -> immediate evict."""

    def __init__(self, backend, max_batch: int = 8, max_queue: int = 32):
        self.backend = backend
        self.max_batch = max_batch
        self.max_queue = max_queue
        # deque + lock instead of queue.Queue: the loop must be able to
        # sweep cancelled waiters out of the middle of the line (I3) -
        # a plain Queue only supports popping the head.
        self.waiting: deque[Sequence] = deque()
        self._wlock = threading.Lock()
        self.active: list[Sequence] = []
        self.free_slots = list(range(max_batch))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    # -- public API (called from HTTP handler threads) --------------------

    def submit(self, seq: Sequence) -> None:
        with self._wlock:
            if len(self.waiting) >= self.max_queue:
                raise QueueFullError()
            self.waiting.append(seq)

    def stats(self) -> dict:
        with self._wlock:
            depth = len(self.waiting)
        return {
            "queue_depth": depth,
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
            with self._wlock:
                idle = not self.active and not self.waiting
            if idle:
                time.sleep(0.005)  # idle: nothing to admit or step
                continue
            self.step_once()

    def step_once(self) -> None:
        """One iteration: sweep -> admit -> step -> evict. Public for tests."""
        self._sweep_cancelled_waiters()
        self._admit()
        self._decode_step()
        self._evict()

    def _sweep_cancelled_waiters(self) -> None:
        # I3 even under a permanently full batch: cancelled waiters must be
        # resolved now, not when a slot eventually frees.
        with self._wlock:
            gone = [s for s in self.waiting if s.cancelled.is_set()]
            if gone:
                self.waiting = deque(
                    s for s in self.waiting if not s.cancelled.is_set())
        for s in gone:
            s.state = State.CANCELLED
            s.emit(None)

    def _pop_waiter(self) -> Sequence | None:
        with self._wlock:
            if not self.waiting:
                return None
            return self.waiting.popleft()

    def _admit(self) -> None:
        while self.free_slots:
            seq = self._pop_waiter()
            if seq is None:
                return
            if seq.cancelled.is_set():
                # Cancelled before touching the model: no slot consumed.
                seq.state = State.CANCELLED
                seq.emit(None)
                continue
            seq.seq_id = self.free_slots.pop(0)
            seq.state = State.PREFILL
            n_prompt, first = self.backend.prefill(
                seq.seq_id, seq.prompt, seq.temperature)
            seq.prompt_tokens = n_prompt
            if first is None:
                # Model emitted EOS on the very first sample.
                seq.state = State.FINISHED
                seq.finish_reason = "stop"
            else:
                seq.emit(first)
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
        # THE shared step: one backend call advances every sequence by one
        # token - this amortized weight-read is the whole speedup.
        pieces = self.backend.step(seqs)
        for s, piece in zip(seqs, pieces):
            if piece is None:
                s.state = State.FINISHED
                s.finish_reason = "stop"
                continue
            s.n_generated += 1
            s.emit(piece)
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
                s.emit(None)  # I3: unblock the HTTP generator
            else:
                keep.append(s)
        self.active = keep
