"""Forge-Serve inference worker, Phase 1: sequential generation.

Streams NDJSON (one JSON object per line) over chunked HTTP - see ADR-002.
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
