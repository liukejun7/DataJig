from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class PhashMatch(Generic[T]):
    distance: int
    hash_value: str
    items: tuple[T, ...]


@dataclass(slots=True)
class _Node(Generic[T]):
    hash_value: str
    numeric_value: int
    items: list[T] = field(default_factory=list)
    children: dict[int, _Node[T]] = field(default_factory=dict)


class PhashIndex(Generic[T]):
    """BK-tree index for exact Hamming-radius queries over hexadecimal pHashes."""

    def __init__(self) -> None:
        self._root: _Node[T] | None = None
        self.last_query_distance_calls = 0

    def add(self, hash_value: str, item: T) -> None:
        numeric = _numeric_hash(hash_value)
        if self._root is None:
            self._root = _Node(hash_value, numeric, [item])
            return
        node = self._root
        while True:
            distance = (node.numeric_value ^ numeric).bit_count()
            if distance == 0:
                node.items.append(item)
                return
            child = node.children.get(distance)
            if child is None:
                node.children[distance] = _Node(hash_value, numeric, [item])
                return
            node = child

    def query(self, hash_value: str, threshold: int) -> tuple[PhashMatch[T], ...]:
        if threshold < 0:
            raise ValueError("threshold must be non-negative")
        numeric = _numeric_hash(hash_value)
        self.last_query_distance_calls = 0
        if self._root is None:
            return ()
        matches: list[PhashMatch[T]] = []
        stack = [self._root]
        while stack:
            node = stack.pop()
            self.last_query_distance_calls += 1
            distance = (node.numeric_value ^ numeric).bit_count()
            if distance <= threshold:
                matches.append(
                    PhashMatch(distance, node.hash_value, tuple(node.items))
                )
            lower = distance - threshold
            upper = distance + threshold
            for edge in sorted(node.children, reverse=True):
                if lower <= edge <= upper:
                    stack.append(node.children[edge])
        matches.sort(key=lambda item: (item.distance, item.hash_value))
        return tuple(matches)


def _numeric_hash(hash_value: str) -> int:
    try:
        return int(hash_value, 16)
    except ValueError as exc:
        raise ValueError(f"invalid hexadecimal perceptual hash: {hash_value!r}") from exc

