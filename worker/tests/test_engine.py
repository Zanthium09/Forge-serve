import pytest
from engine import Engine, QueueFullError, Sequence, State


class FakeBackend:
    """Scripted backend: script maps prompt -> list of pieces; None = EOS."""

    def __init__(self, script=None):
        self.script = dict(script or {})
        self.prefilled = []
        self.released = []

    def prefill(self, seq_id, prompt, temperature=0.7):
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
    e.step_once()  # both admitted (prefill emits first piece) + one shared step
    assert a.out.qsize() >= 1 and b.out.qsize() >= 1


def test_fcfs_admission_and_slot_reuse():
    be = FakeBackend({"a": ["x", None]})
    e = Engine(be, max_batch=1, max_queue=8)
    a, b = Sequence(prompt="a", max_tokens=5), Sequence(prompt="b", max_tokens=1)
    e.submit(a)
    e.submit(b)
    e.step_once()  # a admitted; b must wait (no free slot)
    assert [p for _, p in be.prefilled] == ["a"]
    for _ in range(3):
        e.step_once()  # a hits EOS, slot freed, b admitted into the same slot
    assert be.prefilled[1][1] == "b"
    assert be.prefilled[0][0] == be.prefilled[1][0]  # slot id reused


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
    drain(s)  # sentinel delivered so the HTTP generator unblocks
    n = Sequence(prompt="n", max_tokens=1)
    e.submit(n)
    e.step_once()
    assert be.prefilled[-1][1] == "n"  # freed slot admits the next request


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
