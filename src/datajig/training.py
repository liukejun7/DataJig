from __future__ import annotations

import json
import os
import stat
import tempfile
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import blake3

from datajig.native import NativeBackendUnavailableError, run_native

_COPY_BYTES = 1024 * 1024
_SPOOL_MEMORY_BYTES = 8 * 1024 * 1024


class BundleVerificationError(RuntimeError):
    """Raised before unverified training records can be exposed."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class VerifiedShard:
    relative_path: str
    split: str
    index: int
    records: int
    bytes: int
    shard_id: str


@dataclass(frozen=True)
class VerifiedBundle:
    manifest: Path
    root: Path
    bundle_id: str
    revision_id: str
    state_id: str
    assurance: str
    id_field: str
    records: int
    bytes: int
    splits: tuple[str, ...]
    _shards: tuple[VerifiedShard, ...]
    _root_identity: tuple[int, int]

    def iter_records(self, split: str | None = None) -> Iterator[dict[str, Any]]:
        """Yield verified JSON objects in the bundle's deterministic order."""

        yield from self._iter_records(split, worker_index=0, worker_count=1)

    def _iter_records(
        self,
        split: str | None,
        *,
        worker_index: int,
        worker_count: int,
    ) -> Iterator[dict[str, Any]]:
        if split is not None and split not in self.splits:
            raise BundleVerificationError("SPLIT_NOT_FOUND", "training split was not found")
        if worker_count < 1 or not 0 <= worker_index < worker_count:
            raise ValueError("worker index and count are invalid")
        selected = self._selected_shards(split)
        for position, shard in enumerate(selected):
            if position % worker_count != worker_index:
                continue
            yield from self._iter_shard(shard)

    def _selected_shards(self, split: str | None) -> tuple[VerifiedShard, ...]:
        return tuple(
            shard for shard in self._shards if split is None or shard.split == split
        )

    def _iter_shard(self, shard: VerifiedShard) -> Iterator[dict[str, Any]]:
        if shard not in self._shards:
            raise BundleVerificationError(
                "INVALID_SHARD", "training shard is not part of this verified bundle"
            )
        yield from _read_verified_shard(self, shard)


def open_bundle(
    manifest: str | Path,
    *,
    expected_bundle_id: str | None = None,
    expected_revision_id: str | None = None,
    require_assurance: str | None = None,
    split: str | None = None,
) -> VerifiedBundle:
    """Verify a DataJig bundle natively and return a race-aware local reader."""

    _require_safe_local_open()
    manifest_path = Path(manifest).expanduser()
    parent = manifest_path.parent if manifest_path.parent != Path("") else Path(".")
    try:
        root = parent.resolve(strict=True)
    except OSError as exc:
        raise BundleVerificationError(
            "BUNDLE_IO_ERROR", "cannot resolve training bundle directory"
        ) from exc
    pinned_manifest = root / manifest_path.name
    args = [
        "export-info",
        str(pinned_manifest),
        "--verify",
        "--consumer-plan",
    ]
    if expected_bundle_id is not None:
        args.extend(("--expect-bundle", expected_bundle_id))
    if expected_revision_id is not None:
        args.extend(("--expect-revision", expected_revision_id))
    if require_assurance is not None:
        args.extend(("--require-assurance", require_assurance))
    if split is not None:
        args.extend(("--split", split))
    try:
        completed = run_native(args)
    except NativeBackendUnavailableError as exc:
        raise BundleVerificationError("NATIVE_BACKEND_UNAVAILABLE", str(exc)) from exc
    if completed.returncode != 0:
        raise _native_failure(completed.stderr)
    try:
        document = json.loads(completed.stdout)
        plan = document["artifact"]["consumer_plan"]
        bundle = _bundle_from_plan(pinned_manifest, root, plan)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise BundleVerificationError(
            "INVALID_CONSUMER_PLAN", "native backend returned an invalid consumer plan"
        ) from exc
    if split is not None and bundle.splits != (split,):
        raise BundleVerificationError(
            "INVALID_CONSUMER_PLAN", "native consumer split selection does not match"
        )
    return bundle


def _bundle_from_plan(
    manifest: Path, root: Path, raw: Mapping[str, Any]
) -> VerifiedBundle:
    if (
        raw.get("schema_version") != 1
        or raw.get("verified") is not True
        or raw.get("integrity_scope") != "manifest-and-all-shards-v1"
        or raw.get("format") != "jsonl"
        or raw.get("record_encoding") != "source-json-lf-v1"
    ):
        raise ValueError("unsupported consumer plan")
    source = raw["source"]
    if not isinstance(source, dict):
        raise TypeError("consumer source is invalid")
    raw_splits = raw["splits"]
    if not isinstance(raw_splits, list):
        raise TypeError("consumer splits are invalid")
    split_names: list[str] = []
    shards: list[VerifiedShard] = []
    for raw_split in raw_splits:
        if not isinstance(raw_split, dict) or not isinstance(raw_split.get("name"), str):
            raise TypeError("consumer split is invalid")
        name = raw_split["name"]
        split_names.append(name)
        raw_shards = raw_split.get("shards")
        if not isinstance(raw_shards, list):
            raise TypeError("consumer shards are invalid")
        for raw_shard in raw_shards:
            if not isinstance(raw_shard, dict):
                raise TypeError("consumer shard is invalid")
            shard = VerifiedShard(
                relative_path=_required_string(raw_shard, "relative_path"),
                split=_required_string(raw_shard, "split"),
                index=_required_int(raw_shard, "index"),
                records=_required_positive_int(raw_shard, "records"),
                bytes=_required_positive_int(raw_shard, "bytes"),
                shard_id=_required_string(raw_shard, "shard_id"),
            )
            if shard.split != name or not _valid_relative_shard(shard):
                raise ValueError("consumer shard path is invalid")
            shards.append(shard)
    root_stat = root.stat()
    if not stat.S_ISDIR(root_stat.st_mode):
        raise ValueError("consumer root is not a directory")
    return VerifiedBundle(
        manifest=manifest,
        root=root,
        bundle_id=_required_string(raw, "bundle_id"),
        revision_id=_required_string(source, "revision_id"),
        state_id=_required_string(source, "state_id"),
        assurance=_required_string(source, "assurance"),
        id_field=_required_string(source, "id_field"),
        records=_required_nonnegative_int(raw, "records"),
        bytes=_required_nonnegative_int(raw, "bytes"),
        splits=tuple(split_names),
        _shards=tuple(shards),
        _root_identity=(root_stat.st_dev, root_stat.st_ino),
    )


def _read_verified_shard(
    bundle: VerifiedBundle, shard: VerifiedShard
) -> Iterator[dict[str, Any]]:
    parts = PurePosixPath(shard.relative_path).parts
    if len(parts) != 2:
        raise BundleVerificationError("SHARD_CHANGED", "training shard path is invalid")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY
    nofollow = os.O_NOFOLLOW
    root_fd = split_fd = shard_fd = None
    try:
        root_fd = os.open(bundle.root, directory_flags | nofollow)
        root_stat = os.fstat(root_fd)
        if (root_stat.st_dev, root_stat.st_ino) != bundle._root_identity:
            raise BundleVerificationError(
                "SHARD_CHANGED", "training bundle directory changed after verification"
            )
        split_fd = os.open(parts[0], directory_flags | nofollow, dir_fd=root_fd)
        shard_fd = os.open(parts[1], os.O_RDONLY | nofollow, dir_fd=split_fd)
        metadata = os.fstat(shard_fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size != shard.bytes:
            raise BundleVerificationError(
                "SHARD_CHANGED", "training shard changed after verification"
            )
        with os.fdopen(shard_fd, "rb", closefd=True) as source:
            shard_fd = None
            with tempfile.SpooledTemporaryFile(max_size=_SPOOL_MEMORY_BYTES, mode="w+b") as spool:
                digest = blake3.blake3()
                digest.update(b"datajig-training-shard-v1\0")
                copied = 0
                while True:
                    chunk = source.read(_COPY_BYTES)
                    if not chunk:
                        break
                    copied += len(chunk)
                    if copied > shard.bytes:
                        raise BundleVerificationError(
                            "SHARD_CHANGED", "training shard grew after verification"
                        )
                    digest.update(chunk)
                    spool.write(chunk)
                actual_id = f"shard_{digest.hexdigest()}"
                if copied != shard.bytes or actual_id != shard.shard_id:
                    raise BundleVerificationError(
                        "SHARD_CHANGED", "training shard changed after verification"
                    )
                spool.seek(0)
                yielded = 0
                for line in spool:
                    if not line.endswith(b"\n") or line.endswith(b"\r\n"):
                        raise BundleVerificationError(
                            "INVALID_SHARD", "training shard framing is invalid"
                        )
                    try:
                        value = json.loads(line[:-1])
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise BundleVerificationError(
                            "INVALID_SHARD", "training shard contains invalid JSON"
                        ) from exc
                    if not isinstance(value, dict):
                        raise BundleVerificationError(
                            "INVALID_SHARD", "training shard record is not an object"
                        )
                    yielded += 1
                    yield value
                if yielded != shard.records:
                    raise BundleVerificationError(
                        "SHARD_CHANGED", "training shard record count changed"
                    )
    except BundleVerificationError:
        raise
    except OSError as exc:
        raise BundleVerificationError(
            "SHARD_CHANGED", "cannot open verified training shard"
        ) from exc
    finally:
        for descriptor in (shard_fd, split_fd, root_fd):
            if descriptor is not None:
                os.close(descriptor)


def _native_failure(stderr: str) -> BundleVerificationError:
    try:
        error = json.loads(stderr)["error"]
        code = error["code"]
        message = error["message"]
        if not isinstance(code, str) or not isinstance(message, str):
            raise TypeError
    except (KeyError, TypeError, json.JSONDecodeError):
        return BundleVerificationError(
            "BUNDLE_VERIFICATION_FAILED", "native bundle verification failed"
        )
    return BundleVerificationError(code, message)


def _require_safe_local_open() -> None:
    if (
        not hasattr(os, "O_DIRECTORY")
        or not hasattr(os, "O_NOFOLLOW")
        or os.O_DIRECTORY == 0
        or os.O_NOFOLLOW == 0
        or os.open not in os.supports_dir_fd
    ):
        raise BundleVerificationError(
            "UNSUPPORTED_PLATFORM",
            "verified bundle loading requires safe local no-follow file access",
        )


def _required_string(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise TypeError(f"{key} is invalid")
    return item


def _required_int(value: Mapping[str, Any], key: str) -> int:
    item = value.get(key)
    if type(item) is not int or item < 0:
        raise TypeError(f"{key} is invalid")
    return item


def _required_positive_int(value: Mapping[str, Any], key: str) -> int:
    item = _required_int(value, key)
    if item == 0:
        raise TypeError(f"{key} is invalid")
    return item


def _required_nonnegative_int(value: Mapping[str, Any], key: str) -> int:
    return _required_int(value, key)


def _valid_relative_shard(shard: VerifiedShard) -> bool:
    path = PurePosixPath(shard.relative_path)
    return (
        not path.is_absolute()
        and path.parts == (shard.split, f"part-{shard.index:05}.jsonl")
        and all(part not in {"", ".", ".."} for part in path.parts)
    )
