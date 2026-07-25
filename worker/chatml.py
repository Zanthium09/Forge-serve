"""ChatML prompt formatting (Qwen2.5's template).

Hand-rolled because Phase 2 owns the decode loop and therefore the prompt.
Safe to hardcode: the served model is pinned in config/compose (ADR-003).
"""
from __future__ import annotations


def format_chatml(messages: list) -> str:
    parts = []
    for m in messages:
        parts.append("<|im_start|>%s\n%s<|im_end|>\n" % (m["role"], m["content"]))
    parts.append("<|im_start|>assistant\n")
    return "".join(parts)
