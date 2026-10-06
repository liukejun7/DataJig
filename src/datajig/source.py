from __future__ import annotations

import os
import stat
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol


class SnapshotRootError(ValueError):
    """Raised when a snapshot root cannot be enumerated."""


class SnapshotSecurityError(ValueError):
    """Raised when an entry crosses the source boundary."""


@dataclass(frozen=True, slots=True)
class SnapshotEntry:
    relative_path: PurePosixPath
    absolute_path: Path
    size: int
    mtime_ns: int
    device: int = 0
    inode: int = 0
    ctime_ns: int = 0


class SnapshotSource(Protocol):
    def entries(self) -> Iterator[SnapshotEntry]: ...

    def open(self, entry: SnapshotEntry) -> BinaryIO: ...


class DirectorySource:
    def __init__(self, root: str | Path) -> None:
        supplied = Path(root).expanduser()
        if not supplied.exists():
            raise SnapshotRootError(f"snapshot root does not exist: {supplied}")
        if not supplied.is_dir():
            raise SnapshotRootError(f"snapshot root is not a directory: {supplied}")
        self.root = supplied.resolve()

    def entries(self) -> Iterator[SnapshotEntry]:
        yield from self._entries_below(self.root)

    def _entries_below(self, directory: Path) -> Iterator[SnapshotEntry]:
        for path in sorted(directory.iterdir(), key=lambda item: item.name):
            if path.is_symlink():
                target = self._safe_target(path)
                if target.is_dir():
                    continue
                if not target.is_file():
                    continue
                yield self._entry(path)
            elif path.is_dir():
                yield from self._entries_below(path)
            elif path.is_file():
                yield self._entry(path)

    def _entry(self, path: Path) -> SnapshotEntry:
        stat = path.stat()
        relative = PurePosixPath(path.relative_to(self.root).as_posix())
        return SnapshotEntry(
            relative,
            path,
            stat.st_size,
            stat.st_mtime_ns,
            stat.st_dev,
            stat.st_ino,
            stat.st_ctime_ns,
        )

    def _safe_target(self, path: Path) -> Path:
        try:
            target = path.resolve(strict=True)
        except OSError as exc:
            raise SnapshotSecurityError(f"cannot resolve snapshot entry: {path}") from exc
        if not target.is_relative_to(self.root):
            raise SnapshotSecurityError(f"snapshot entry escapes snapshot root: {path}")
        return target

    def open(self, entry: SnapshotEntry) -> BinaryIO:
        expected = self.root.joinpath(*entry.relative_path.parts)
        if entry.relative_path.is_absolute() or ".." in entry.relative_path.parts:
            raise SnapshotSecurityError(f"invalid snapshot-relative path: {entry.relative_path}")
        if entry.absolute_path.absolute() != expected:
            raise SnapshotSecurityError(f"entry does not belong to snapshot: {entry.relative_path}")
        target = self._safe_target(expected)
        return self._open_resolved_target(target)

    def _open_resolved_target(self, target: Path) -> BinaryIO:
        relative = target.relative_to(self.root)
        if os.name != "posix" or not hasattr(os, "O_NOFOLLOW"):
            before = os.stat(target, follow_symlinks=False)
            if not stat.S_ISREG(before.st_mode):
                raise SnapshotSecurityError(f"snapshot entry is not a regular file: {target}")
            descriptor = os.open(target, os.O_RDONLY)
            try:
                opened = os.fstat(descriptor)
                after = os.stat(target, follow_symlinks=False)
                if (
                    not stat.S_ISREG(after.st_mode)
                    or (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
                    or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
                    or not target.resolve(strict=True).is_relative_to(self.root)
                ):
                    raise SnapshotSecurityError(
                        f"snapshot entry changed while opening: {target}"
                    )
                return os.fdopen(descriptor, "rb")
            except Exception:
                os.close(descriptor)
                raise
        directory_fd = os.open(
            self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        )
        try:
            for part in relative.parts[:-1]:
                next_fd = os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=directory_fd,
                )
                os.close(directory_fd)
                directory_fd = next_fd
            file_fd = os.open(
                relative.parts[-1],
                os.O_RDONLY | os.O_NOFOLLOW,
                dir_fd=directory_fd,
            )
            return os.fdopen(file_fd, "rb")
        finally:
            os.close(directory_fd)
