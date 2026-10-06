from __future__ import annotations

import os
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from pathlib import Path

from blake3 import blake3

from datajig.manifest import ManifestEntry, SnapshotManifest
from datajig.source import DirectorySource, SnapshotEntry


class SnapshotChangedError(RuntimeError):
    """Raised when a source changes while its snapshot is being built."""


def create_snapshot(
    root: str | Path,
    *,
    output: str | Path | None = None,
    workers: int = 1,
) -> SnapshotManifest:
    if workers < 1:
        raise ValueError("workers must be at least 1")
    source = DirectorySource(root)
    destination = _safe_destination(output, source.root) if output is not None else None
    entries = list(source.entries())
    _reject_destination_alias(destination, entries)
    if workers == 1:
        manifest_entries = tuple(_hash_entry(source, item) for item in entries)
    else:
        completed: list[ManifestEntry] = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            pending: dict[Future[ManifestEntry], SnapshotEntry] = {
                executor.submit(_hash_entry, source, item): item for item in entries
            }
            for future in as_completed(pending):
                completed.append(future.result())
        manifest_entries = tuple(completed)
    final_entries = list(source.entries())
    if [_entry_identity(item) for item in entries] != [
        _entry_identity(item) for item in final_entries
    ]:
        raise SnapshotChangedError("dataset membership or metadata changed during snapshot")
    manifest = SnapshotManifest.create(manifest_entries)
    if destination is not None:
        manifest.save(destination)
    return manifest


def _hash_entry(source: DirectorySource, entry: SnapshotEntry) -> ManifestEntry:
    digest = blake3()
    size = 0
    with source.open(entry) as stream:
        before = os.fstat(stream.fileno())
        if _stat_identity(before) != _entry_stat_identity(entry):
            raise SnapshotChangedError(
                f"dataset entry changed before hashing: {entry.relative_path}"
            )
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
        after = os.fstat(stream.fileno())
    if _stat_identity(before) != _stat_identity(after) or size != before.st_size:
        raise SnapshotChangedError(
            f"dataset entry changed while hashing: {entry.relative_path}"
        )
    return ManifestEntry(entry.relative_path.as_posix(), size, digest.hexdigest())


def _safe_destination(path: str | Path, source_root: Path) -> Path:
    destination = Path(path).expanduser().resolve(strict=False)
    if destination == source_root or destination.is_relative_to(source_root):
        raise ValueError("snapshot output must stay outside the dataset source")
    return destination


def _reject_destination_alias(
    destination: Path | None, entries: list[SnapshotEntry]
) -> None:
    if destination is None or not destination.exists():
        return
    destination_stat = destination.stat()
    identity = (destination_stat.st_dev, destination_stat.st_ino)
    if any((entry.device, entry.inode) == identity for entry in entries):
        raise ValueError("snapshot output aliases a dataset source file")


def _entry_identity(entry: SnapshotEntry) -> tuple[object, ...]:
    return (
        entry.relative_path.as_posix(),
        entry.size,
        entry.mtime_ns,
        entry.ctime_ns,
        entry.device,
        entry.inode,
    )


def _entry_stat_identity(entry: SnapshotEntry) -> tuple[int, ...]:
    return (entry.device, entry.inode, entry.size, entry.mtime_ns, entry.ctime_ns)


def _stat_identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
