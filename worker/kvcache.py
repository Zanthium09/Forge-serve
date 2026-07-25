"""Per-slot KV bookkeeping: each slot (seq id) tracks the next write
position in its logical KV sequence. Slot lifecycle is owned by the
engine; this module only tracks positions.
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
