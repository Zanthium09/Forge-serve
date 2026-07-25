"""Worker configuration from environment (12-factor: config lives in env)."""

import os

MODEL_PATH = os.environ.get("MODEL_PATH", "/models/qwen2.5-1.5b-instruct-q4_k_m.gguf")
# Total KV cache, shared by all batch slots: each concurrent sequence gets
# roughly N_CTX / MAX_BATCH positions. 16384/16 = 1024 tokens per request.
N_CTX = int(os.environ.get("N_CTX", "16384"))
# 0 = let llama.cpp pick (all physical cores).
N_THREADS = int(os.environ.get("N_THREADS", "0")) or None

# Phase 2: engine selection + batching knobs.
ENGINE = os.environ.get("ENGINE", "batched")
# 16 slots: measured decode scaling (benchmarks/raw/decode_scaling.txt)
# shows batch 16 at 64 tok/s vs batch 8 at 58.6, and twice the slot
# turnover, which is what actually drives queue wait down.
MAX_BATCH = int(os.environ.get("MAX_BATCH", "16"))
# Queue depth is a LATENCY budget, not a buffer size: a slot frees roughly
# every (request duration / MAX_BATCH), so a queue of N costs the last
# waiter about N * that interval before its first token. Sized so p99 TTFT
# stays inside the 5s SLO; anything deeper is a promise we cannot keep, so
# it is rejected at admission instead (ADR-003).
MAX_QUEUE = int(os.environ.get("MAX_QUEUE", "8"))
