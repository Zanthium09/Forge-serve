"""Forge-Serve inference worker.

Two engines behind one NDJSON contract (ADR-002), selected by ENGINE env:
- "batched" (default): continuous batching via engine.Engine - many
  sequences share each decode step (Phase 2 crown jewel).
- "sequential": the Phase 1 lock-guarded path, kept as the benchmark
  baseline so A/B runs use identical infrastructure.
"""

import asyncio
import json
import queue
import threading

import config
from fastapi import FastAPI
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

app = FastAPI()


class Message(BaseModel):
    role: str
    content: str


class GenerateRequest(BaseModel):
    messages: list[Message]
    temperature: float = 0.7
    max_tokens: int = 256


if config.ENGINE == "batched":
    from backend_llama import LlamaBackend
    from chatml import format_chatml
    from engine import Engine, QueueFullError, Sequence

    backend = LlamaBackend(config.MODEL_PATH, config.N_CTX,
                           config.MAX_BATCH, config.N_THREADS)
    engine = Engine(backend, config.MAX_BATCH, config.MAX_QUEUE)
    engine.start()

    @app.post("/generate")
    async def generate(req: GenerateRequest) -> StreamingResponse:
        seq = Sequence(
            prompt=format_chatml([m.model_dump() for m in req.messages]),
            max_tokens=req.max_tokens,
            temperature=req.temperature,
        )
        # Event-driven handoff from the decode loop; measured 40.2 -> 43.2
        # tok/s at 8 concurrent streams versus a 10ms polling reader.
        loop = asyncio.get_running_loop()
        ready = asyncio.Event()
        seq.notify = lambda: loop.call_soon_threadsafe(ready.set)

        try:
            engine.submit(seq)
        except QueueFullError:
            # Fail fast at admission - never accept-then-drop (blueprint).
            return JSONResponse({"error": "overloaded"}, status_code=429)

        # ASYNC generator, deliberately: Starlette signals a client
        # disconnect by cancelling this task, and asyncio cancellation can
        # only land on an await. A sync generator would sit blocked in
        # seq.out.get() inside a worker thread, never run its finally, and
        # keep burning decode steps for a client that already left
        # (observed: 360 wasted tokens before this was made async).
        async def stream():
            try:
                while True:
                    try:
                        piece = seq.out.get_nowait()
                    except queue.Empty:
                        ready.clear()
                        try:
                            # Timeout is a safety net, not the mechanism: it
                            # covers a notify landing between the failed
                            # get_nowait above and this clear().
                            await asyncio.wait_for(ready.wait(), timeout=0.25)
                        except TimeoutError:
                            pass
                        continue
                    if piece is None:
                        break
                    if piece == "":
                        continue  # partial utf-8 held back by the backend
                    yield json.dumps({"token": piece, "done": False}) + "\n"
                yield json.dumps({
                    "done": True,
                    "prompt_tokens": seq.prompt_tokens,
                    "completion_tokens": seq.n_generated,
                    "finish_reason": seq.finish_reason or "stop",
                }) + "\n"
            finally:
                # Normal end or cancellation: either way mark cancelled so
                # the engine evicts a still-live sequence at the next
                # iteration boundary and frees its slot.
                seq.cancelled.set()

        return StreamingResponse(stream(), media_type="application/x-ndjson")

    @app.get("/stats")
    def stats() -> dict:
        return engine.stats()

else:
    from llama_cpp import Llama

    llm = Llama(
        model_path=config.MODEL_PATH,
        n_ctx=config.N_CTX,
        n_threads=config.N_THREADS,
        verbose=False,
    )
    # llama.cpp contexts are not thread-safe; the sequential baseline
    # serves one generation at a time - others queue on this lock.
    gen_lock = threading.Lock()

    @app.post("/generate")
    def generate(req: GenerateRequest) -> StreamingResponse:  # type: ignore[no-redef]
        def stream():
            with gen_lock:
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

    @app.get("/stats")
    def stats() -> dict:  # type: ignore[no-redef]
        return {"queue_depth": 0, "active_seqs": 0, "max_batch": 1,
                "engine": "sequential"}


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}
