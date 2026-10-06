from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath


class LayoutError(ValueError):
    """Raised when a relative path does not follow the ImageFolder contract."""


@dataclass(frozen=True, slots=True)
class ParsedPath:
    split: str
    label: str
    logical_path: PurePosixPath
    supported: bool


class ImageFolderLayout:
    supported_suffixes = frozenset({".jpg", ".jpeg", ".png", ".webp"})
    accepted_splits = frozenset({"train", "val", "test"})

    def parse(self, relative_path: PurePosixPath) -> ParsedPath:
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise LayoutError(f"path must stay relative to the snapshot: {relative_path}")
        parts = relative_path.parts
        if len(parts) < 3:
            raise LayoutError(f"expected <split>/<label>/<file>: {relative_path}")
        split, label, *logical_parts = parts
        if split not in self.accepted_splits:
            accepted = ", ".join(sorted(self.accepted_splits))
            raise LayoutError(f"unsupported split {split!r}; expected one of {accepted}")
        if not label or not logical_parts or not logical_parts[-1]:
            raise LayoutError(f"path must include a label and file: {relative_path}")
        logical_path = PurePosixPath(*logical_parts)
        return ParsedPath(
            split=split,
            label=label,
            logical_path=logical_path,
            supported=logical_path.suffix.lower() in self.supported_suffixes,
        )

