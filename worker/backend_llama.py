"""Real ModelBackend: drives llama.cpp's low-level API (pinned 0.3.34).

Why low-level: continuous batching needs one llama_decode per iteration
covering many sequences, per-sequence KV clearing, and sampling from
per-sequence logits - none of which the high-level API exposes.

API notes for this pinned version (verified by probing argtypes):
- tokenize/token_to_piece/is_eog take a *vocab* pointer, not the model.
- KV freeing goes through the memory API: llama_get_memory + llama_memory_seq_rm.
- llama_load_model_from_file / llama_new_context_with_model are dead stubs
  here; the live names are llama_model_load_from_file / llama_init_from_model.

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
        self.model = llama_cpp.llama_model_load_from_file(
            model_path.encode("utf-8"), mparams)
        if not self.model:
            raise RuntimeError(f"failed to load model: {model_path}")
        self.vocab = llama_cpp.llama_model_get_vocab(self.model)
        cparams = llama_cpp.llama_context_default_params()
        cparams.n_ctx = n_ctx
        cparams.n_batch = 2048
        # One KV cache shared by up to max_batch concurrent sequences.
        cparams.n_seq_max = max_batch
        if n_threads:
            cparams.n_threads = n_threads
            cparams.n_threads_batch = n_threads
        self.ctx = llama_cpp.llama_init_from_model(self.model, cparams)
        if not self.ctx:
            raise RuntimeError("failed to create llama context")
        self.mem = llama_cpp.llama_get_memory(self.ctx)
        self.batch = llama_cpp.llama_batch_init(2048, 0, max_batch)
        self.n_vocab = llama_cpp.llama_vocab_n_tokens(self.vocab)
        self.pos = SlotPositions()
        self.cur_token: dict[int, int] = {}   # slot -> last sampled token
        self.utf8_buf: dict[int, bytes] = {}  # slot -> undecoded byte tail
        self.rng = np.random.default_rng()

    # -- ModelBackend ----------------------------------------------------

    def prefill(self, seq_id: int, prompt: str, temperature: float = 0.7):
        toks = self._tokenize(prompt)
        n = len(toks)
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
        tok = self._sample(self.batch.n_tokens - 1, temperature)
        if self._is_eog(tok):
            return (n, None)
        self.cur_token[seq_id] = tok
        return (n, self._piece(seq_id, tok))

    def step(self, seqs):
        # One shared decode: the newest token of every sequence, one batch.
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
            tok = self._sample(i, s.temperature)
            if self._is_eog(tok):
                out.append(None)
            else:
                self.cur_token[s.seq_id] = tok
                out.append(self._piece(s.seq_id, tok))
        return out

    def release(self, seq_id: int) -> None:
        # Free this sequence's whole KV range so the slot can be reused.
        llama_cpp.llama_memory_seq_rm(self.mem, seq_id, 0, -1)
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
        arr = (ctypes.c_int * n_max)()
        n = llama_cpp.llama_tokenize(
            self.vocab, b, len(b), arr, n_max, True, True)
        if n < 0:
            raise RuntimeError("tokenize buffer too small")
        return list(arr[:n])

    def _sample(self, logit_idx: int, temperature: float) -> int:
        ptr = llama_cpp.llama_get_logits_ith(self.ctx, logit_idx)
        logits = np.ctypeslib.as_array(ptr, shape=(self.n_vocab,)).astype(
            np.float64, copy=True)
        if temperature <= 0:
            return int(logits.argmax())
        logits /= temperature
        logits -= logits.max()
        p = np.exp(logits)
        p /= p.sum()
        return int(self.rng.choice(self.n_vocab, p=p))

    def _is_eog(self, tok: int) -> bool:
        return bool(llama_cpp.llama_vocab_is_eog(self.vocab, tok))

    def _piece(self, seq_id: int, tok: int) -> str:
        buf = ctypes.create_string_buffer(64)
        n = llama_cpp.llama_token_to_piece(self.vocab, tok, buf, 64, 0, False)
        raw = self.utf8_buf[seq_id] + buf.raw[:n]
        try:
            text = raw.decode("utf-8")
            self.utf8_buf[seq_id] = b""
            return text
        except UnicodeDecodeError:
            # Partial multi-byte char: hold bytes until the next piece.
            self.utf8_buf[seq_id] = raw
            return ""
