"""Measure llama_decode wall time vs batch size (the premise of batching).

If decode(N seqs) costs ~= decode(1 seq), batching is nearly free and
throughput scales with N. If it costs N x, the hardware was already
saturated and batching buys nothing.
"""
import sys
import time

sys.path.insert(0, "/app")
import llama_cpp  # noqa: E402

from backend_llama import LlamaBackend  # noqa: E402

be = LlamaBackend("/models/qwen2.5-1.5b-instruct-q4_k_m.gguf",
                  n_ctx=8192, max_batch=16)

PROMPT = "Explain distributed systems. " * 8


class FakeSeq:
    def __init__(self, seq_id):
        self.seq_id = seq_id
        self.temperature = 0.0


print(f"{'batch':>6} {'ms/step':>10} {'ms/token':>10} {'tok/s':>10}")
for n in (1, 2, 4, 8, 16):
    # fresh slots for this measurement
    for sid in range(16):
        llama_cpp.llama_memory_seq_rm(be.mem, sid, 0, -1)
    be.pos = type(be.pos)()
    be.cur_token = {}
    be.utf8_buf = {}

    seqs = []
    for sid in range(n):
        be.prefill(sid, PROMPT, 0.0)
        seqs.append(FakeSeq(sid))

    be.step(seqs)  # warm

    t0 = time.perf_counter()
    STEPS = 12
    for _ in range(STEPS):
        be.step(seqs)
    dt = (time.perf_counter() - t0) / STEPS
    print(f"{n:>6} {dt * 1000:>10.1f} {dt * 1000 / n:>10.1f} {n / dt:>10.2f}")
