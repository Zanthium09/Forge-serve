"""Worker configuration from environment (12-factor: config lives in env)."""

import os

MODEL_PATH = os.environ.get("MODEL_PATH", "/models/qwen2.5-1.5b-instruct-q4_k_m.gguf")
N_CTX = int(os.environ.get("N_CTX", "4096"))
# 0 = let llama.cpp pick (all physical cores).
N_THREADS = int(os.environ.get("N_THREADS", "0")) or None

# Phase 2: engine selection + batching knobs.
ENGINE = os.environ.get("ENGINE", "batched")
MAX_BATCH = int(os.environ.get("MAX_BATCH", "8"))
MAX_QUEUE = int(os.environ.get("MAX_QUEUE", "32"))
