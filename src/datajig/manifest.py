from __future__ import annotations

import json
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import cast

from blake3 import blake3

MANIFEST_SCHEMA_VERSION = 1
MAX_MANIFEST_BYTES = 128 * 1024 * 1024
MAX_MANIFEST_ENTRIES = 250_000
MAX_PATH_BYTES = 4_096
MAX_FILE_SIZE = 2**64 - 1
_DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
_IDENTITY_PREFIX = b"datajig-snapshot-v1\0"


class ManifestSchemaError(ValueError):
    """Raised when a snapshot manifest violates schema or identity rules."""


@dataclass(frozen=True, slots=True)
class ManifestEntry:
    path: str
    size: int
    content_hash: str

    def __post_init__(self) -> None:
        canonical = PurePosixPath(self.path)
        if (
            not self.path
            or "\\" in self.path
            or canonical.is_absolute()
            or canonical.as_posix() != self.path
            or any(part in {"", ".", ".."} for part in canonical.parts)
        ):
            raise ValueError(f"manifest path must be canonical and relative: {self.path!r}")
        try:
            path_bytes = self.path.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise ValueError("manifest path must be valid UTF-8") from exc
        if len(path_bytes) > MAX_PATH_BYTES:
            raise ValueError(f"manifest path exceeds {MAX_PATH_BYTES} UTF-8 bytes")
        if self.size < 0 or self.size > MAX_FILE_SIZE:
            raise ValueError("manifest entry size must fit an unsigned 64-bit integer")
        if _DIGEST_PATTERN.fullmatch(self.content_hash) is None:
            raise ValueError("manifest content_hash must be 64 lowercase hex characters")

    def to_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "size": self.size,
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True, slots=True)
class SnapshotManifest:
    snapshot_id: str
    entries: tuple[ManifestEntry, ...]
    schema_version: int = MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != MANIFEST_SCHEMA_VERSION:
            raise ValueError(f"unsupported manifest schema {self.schema_version}")
        paths = tuple(item.path for item in self.entries)
        if paths != tuple(sorted(paths, key=lambda path: path.encode("utf-8"))):
            raise ValueError("manifest entries must be sorted by path")
        if len(paths) != len(set(paths)):
            raise ValueError("manifest entry paths must be unique")
        expected = _snapshot_id(self.entries, self.schema_version)
        if self.snapshot_id != expected:
            raise ValueError("snapshot_id does not match manifest entries")

    @classmethod
    def create(cls, entries: tuple[ManifestEntry, ...]) -> SnapshotManifest:
        if len(entries) > MAX_MANIFEST_ENTRIES:
            raise ValueError(f"manifest exceeds {MAX_MANIFEST_ENTRIES} entries")
        ordered = tuple(sorted(entries, key=lambda item: item.path.encode("utf-8")))
        paths = [item.path for item in ordered]
        if len(paths) != len(set(paths)):
            raise ValueError("manifest entry paths must be unique")
        return cls(_snapshot_id(ordered, MANIFEST_SCHEMA_VERSION), ordered)

    @property
    def total_files(self) -> int:
        return len(self.entries)

    @property
    def total_bytes(self) -> int:
        return sum(item.size for item in self.entries)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "snapshot_id": self.snapshot_id,
            "summary": {
                "total_files": self.total_files,
                "total_bytes": self.total_bytes,
            },
            "entries": [item.to_dict() for item in self.entries],
        }

    def save(self, path: str | Path) -> None:
        destination = Path(path)
        payload = manifest_to_json(self).encode("utf-8")
        if len(payload) > MAX_MANIFEST_BYTES:
            raise ManifestSchemaError(
                f"manifest is too large (max {MAX_MANIFEST_BYTES} bytes)"
            )
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=destination.parent, prefix=f".{destination.name}."
        )
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, destination)
            if os.name == "posix":
                directory_fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)


def manifest_to_json(manifest: SnapshotManifest, indent: int | None = 2) -> str:
    return json.dumps(
        manifest.to_dict(),
        ensure_ascii=False,
        indent=indent,
        sort_keys=True,
        separators=(",", ":") if indent is None else None,
    )


def manifest_from_json(payload: str) -> SnapshotManifest:
    try:
        encoded_size = len(payload.encode("utf-8"))
    except UnicodeEncodeError as exc:
        raise ManifestSchemaError("manifest must contain valid UTF-8 text") from exc
    if encoded_size > MAX_MANIFEST_BYTES:
        raise ManifestSchemaError(f"manifest is too large (max {MAX_MANIFEST_BYTES} bytes)")
    try:
        decoded: object = json.loads(payload, object_pairs_hook=_unique_object)
    except ManifestSchemaError:
        raise
    except json.JSONDecodeError as exc:
        raise ManifestSchemaError(f"invalid manifest JSON: {exc.msg}") from exc
    except RecursionError as exc:
        raise ManifestSchemaError("invalid manifest JSON: nesting is too deep") from exc
    except ValueError as exc:
        raise ManifestSchemaError("invalid manifest JSON value") from exc
    try:
        root = _mapping(decoded, "manifest")
        if set(root) != {"schema_version", "snapshot_id", "summary", "entries"}:
            raise ManifestSchemaError("manifest has unknown or missing fields")
        schema_version = _integer(root["schema_version"], "schema_version")
        snapshot_id = _string(root["snapshot_id"], "snapshot_id")
        raw_entries = _sequence(root["entries"], "entries")
        if len(raw_entries) > MAX_MANIFEST_ENTRIES:
            raise ManifestSchemaError(
                f"manifest exceeds {MAX_MANIFEST_ENTRIES} entries"
            )
        entries = tuple(_entry(item) for item in raw_entries)
        manifest = SnapshotManifest(snapshot_id, entries, schema_version)
        summary = _mapping(root["summary"], "summary")
        if set(summary) != {"total_files", "total_bytes"}:
            raise ManifestSchemaError("summary has unknown or missing fields")
        if _integer(summary["total_files"], "summary.total_files") != manifest.total_files:
            raise ManifestSchemaError("summary.total_files does not match entries")
        if _integer(summary["total_bytes"], "summary.total_bytes") != manifest.total_bytes:
            raise ManifestSchemaError("summary.total_bytes does not match entries")
        return manifest
    except ManifestSchemaError:
        raise
    except (KeyError, TypeError, ValueError, RecursionError) as exc:
        raise ManifestSchemaError(f"invalid manifest ({type(exc).__name__})") from exc


def load_manifest(path: str | Path) -> SnapshotManifest:
    try:
        source = Path(path)
        if source.stat().st_size > MAX_MANIFEST_BYTES:
            raise ManifestSchemaError(
                f"manifest is too large (max {MAX_MANIFEST_BYTES} bytes)"
            )
        return manifest_from_json(source.read_text(encoding="utf-8"))
    except UnicodeError as exc:
        raise ManifestSchemaError("manifest must be UTF-8 JSON") from exc


def _snapshot_id(entries: tuple[ManifestEntry, ...], schema_version: int) -> str:
    if schema_version != MANIFEST_SCHEMA_VERSION:
        raise ValueError(f"unsupported manifest schema {schema_version}")
    identity = bytearray(_IDENTITY_PREFIX)
    for item in entries:
        path = item.path.encode("utf-8")
        identity.extend(len(path).to_bytes(4, "big"))
        identity.extend(path)
        identity.extend(item.size.to_bytes(8, "big"))
        identity.extend(bytes.fromhex(item.content_hash))
    return f"snap_{blake3(identity).hexdigest()}"


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ManifestSchemaError(f"duplicate JSON member {key!r}")
        result[key] = value
    return result


def _entry(value: object) -> ManifestEntry:
    data = _mapping(value, "entry")
    if set(data) != {"path", "size", "content_hash"}:
        raise ManifestSchemaError("entry has unknown or missing fields")
    return ManifestEntry(
        _string(data["path"], "entry.path"),
        _integer(data["size"], "entry.size"),
        _string(data["content_hash"], "entry.content_hash"),
    )


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ManifestSchemaError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _sequence(value: object, name: str) -> list[object]:
    if not isinstance(value, list):
        raise ManifestSchemaError(f"{name} must be an array")
    return cast(list[object], value)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ManifestSchemaError(f"{name} must be a string")
    return value


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ManifestSchemaError(f"{name} must be an integer")
    return value
