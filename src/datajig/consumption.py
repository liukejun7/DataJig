from __future__ import annotations

import fcntl
import json
import os
import stat
import threading
from collections.abc import Iterator, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import blake3

from datajig.native import NativeBackendUnavailableError, run_native
from datajig.training import VerifiedBundle, VerifiedShard, _bundle_from_plan

_CLAIM = "all_verified_split_records_crossed_adapter_boundary_at_least_once"
_MAX_PLAN_BYTES = 4 * 1024 * 1024
_MAX_STATE_BYTES = 1024 * 1024


class ConsumptionStateError(RuntimeError):
    """Raised when consumption evidence cannot be safely created or verified."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class _Runtime:
    plan_id: str
    run_id: str
    consumer: str
    manifest: Path
    run_dir: Path
    plan_path: Path
    bundle_id: str
    source: Mapping[str, Any]
    split: str
    records: int
    bytes: int
    shards: tuple[VerifiedShard, ...]


class ConsumptionRun:
    """One accepted adapter-boundary consumption run."""

    def __init__(
        self, runtime: _Runtime, bundle: VerifiedBundle | None, plan_bytes: bytes
    ) -> None:
        self._runtime = runtime
        self._bundle = bundle
        self._plan_bytes = plan_bytes
        self._initialize_state()
        self._run_identity = _directory_identity(self._runtime.run_dir)
        self._shards_identity = _directory_identity(self._runtime.run_dir / "shards")

    @property
    def consumer(self) -> str:
        return self._runtime.consumer

    @property
    def split(self) -> str:
        return self._runtime.split

    @property
    def receipt(self) -> Path | None:
        path = self._runtime.run_dir / "datajig.consumed.json"
        return path if path.is_file() else None

    def iter_records(self) -> Iterator[dict[str, Any]]:
        if self._runtime.consumer != "python":
            raise ValueError(
                f"{self._runtime.consumer} consumption plans must use their named adapter"
            )
        yield from self._iter_records(worker_index=0, worker_count=1)

    def _iter_records(
        self, *, worker_index: int, worker_count: int
    ) -> Iterator[dict[str, Any]]:
        if worker_count < 1 or not 0 <= worker_index < worker_count:
            raise ValueError("worker index and count are invalid")
        self._require_same_state()
        if self._bundle is None:
            raise ConsumptionStateError(
                "CONSUMPTION_ALREADY_COMPLETED",
                "completed training consumption evidence cannot be iterated without live inputs",
            )
        for position, shard in enumerate(self._runtime.shards):
            if position % worker_count != worker_index:
                continue
            yield from self._bundle._iter_shard(shard)
            self._publish_marker(shard)
        self._try_finalize()

    def _initialize_state(self) -> None:
        run_dir = self._runtime.run_dir
        try:
            run_dir.mkdir(mode=0o700)
            (run_dir / "shards").mkdir(mode=0o700)
            _publish_new(run_dir / "plan.json", self._plan_bytes)
            _publish_new(run_dir / ".lock", b"")
        except FileExistsError:
            _require_directory(run_dir)
            if _read_regular(run_dir / "plan.json", _MAX_PLAN_BYTES) != self._plan_bytes:
                raise ConsumptionStateError(
                    "CONSUMPTION_STATE_CONFLICT",
                    "training consumption run state does not match its accepted plan",
                ) from None
            shards = run_dir / "shards"
            _require_directory(shards)
            _read_regular(run_dir / ".lock", 0)
        except OSError as exc:
            raise ConsumptionStateError(
                "CONSUMPTION_IO_ERROR", "cannot initialize training consumption state"
            ) from exc

    def _publish_marker(self, shard: VerifiedShard) -> None:
        self._require_same_state()
        lock_descriptor = os.open(
            self._runtime.run_dir / ".lock", os.O_RDWR | os.O_NOFOLLOW
        )
        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
            self._require_same_state()
            self._publish_marker_locked(shard)
        finally:
            os.close(lock_descriptor)

    def _publish_marker_locked(self, shard: VerifiedShard) -> None:
        marker = {
            "namespace": "datajig",
            "kind": "training_consumption_shard_marker",
            "schema_version": 1,
            "consumption_plan_id": self._runtime.plan_id,
            "split": shard.split,
            "index": shard.index,
            "records": shard.records,
            "shard_id": shard.shard_id,
        }
        payload = _canonical_json(marker)
        path = self._runtime.run_dir / "shards" / f"{shard.shard_id}.json"
        try:
            _publish_new(path, payload)
        except FileExistsError:
            if _read_regular(path, _MAX_STATE_BYTES) != payload:
                raise ConsumptionStateError(
                    "CONSUMPTION_STATE_CONFLICT",
                    "training consumption shard marker conflicts with the accepted plan",
                ) from None

    def _try_finalize(self) -> None:
        self._require_same_state()
        lock_descriptor = os.open(
            self._runtime.run_dir / ".lock", os.O_RDWR | os.O_NOFOLLOW
        )
        try:
            fcntl.flock(lock_descriptor, fcntl.LOCK_EX)
            self._finalize_locked()
        finally:
            os.close(lock_descriptor)

    def _finalize_locked(self) -> None:
        self._validate_root_entries(complete_allowed=True)
        markers = self._runtime.run_dir / "shards"
        expected = {f"{shard.shard_id}.json" for shard in self._runtime.shards}
        try:
            actual = {path.name for path in markers.iterdir()}
        except OSError as exc:
            raise ConsumptionStateError(
                "CONSUMPTION_IO_ERROR", "cannot inspect training consumption markers"
            ) from exc
        if not expected.issubset(actual):
            return
        if actual != expected:
            raise ConsumptionStateError(
                "CONSUMPTION_STATE_CONFLICT",
                "training consumption run contains unexpected shard state",
            )
        for shard in self._runtime.shards:
            expected_marker = _canonical_json(
                {
                    "namespace": "datajig",
                    "kind": "training_consumption_shard_marker",
                    "schema_version": 1,
                    "consumption_plan_id": self._runtime.plan_id,
                    "split": shard.split,
                    "index": shard.index,
                    "records": shard.records,
                    "shard_id": shard.shard_id,
                }
            )
            actual_marker = _read_regular(
                markers / f"{shard.shard_id}.json", _MAX_STATE_BYTES
            )
            if actual_marker != expected_marker:
                raise ConsumptionStateError(
                    "CONSUMPTION_STATE_CONFLICT",
                    "training consumption shard marker is invalid",
                )
        completion = self._completion_bytes()
        _publish_idempotent(self._runtime.run_dir / "completion.json", completion)
        _publish_idempotent(
            self._runtime.run_dir / "datajig.consumed.json", self._receipt_bytes()
        )

    def _completion_bytes(self) -> bytes:
        return _canonical_json(
            {
                "namespace": "datajig",
                "kind": "training_consumption_completion",
                "schema_version": 1,
                "consumption_plan_id": self._runtime.plan_id,
                "records": self._runtime.records,
                "shards": len(self._runtime.shards),
            }
        )

    def _receipt_bytes(self) -> bytes:
        receipt_identity = {
            "consumption_plan_id": self._runtime.plan_id,
            "claim": _CLAIM,
        }
        digest = blake3.blake3()
        digest.update(b"datajig-training-consumption-receipt-v1\0")
        digest.update(_canonical_json(receipt_identity))
        return _canonical_json(
            {
                "namespace": "datajig",
                "kind": "training_consumption_receipt",
                "schema_version": 1,
                "consumption_receipt_id": f"consumed_{digest.hexdigest()}",
                "consumption_plan_id": self._runtime.plan_id,
                "run_id": self._runtime.run_id,
                "consumer": self._runtime.consumer,
                "bundle_id": self._runtime.bundle_id,
                "split": self._runtime.split,
                "records": self._runtime.records,
                "shards": len(self._runtime.shards),
                "claim": _CLAIM,
                "plan": str(self._runtime.run_dir / "plan.json"),
                "run_dir": str(self._runtime.run_dir),
            }
        )

    def _validate_root_entries(self, *, complete_allowed: bool) -> None:
        self._require_same_state()
        allowed = {".lock", "plan.json", "shards"}
        if complete_allowed:
            allowed.update({"completion.json", "datajig.consumed.json"})
        actual = {path.name for path in self._runtime.run_dir.iterdir()}
        if not actual.issubset(allowed):
            raise ConsumptionStateError(
                "CONSUMPTION_STATE_CONFLICT",
                "training consumption run contains unexpected state",
            )

    def _require_same_state(self) -> None:
        if (
            _directory_identity(self._runtime.run_dir) != self._run_identity
            or _directory_identity(self._runtime.run_dir / "shards")
            != self._shards_identity
        ):
            raise ConsumptionStateError(
                "CONSUMPTION_STATE_CONFLICT",
                "training consumption run directory changed after it was opened",
            )

    def _validate_completed(self) -> None:
        self._validate_root_entries(complete_allowed=True)
        self._try_finalize()
        if _read_regular(
            self._runtime.run_dir / "completion.json", _MAX_STATE_BYTES
        ) != self._completion_bytes() or _read_regular(
            self._runtime.run_dir / "datajig.consumed.json", _MAX_STATE_BYTES
        ) != self._receipt_bytes():
            raise ConsumptionStateError(
                "CONSUMPTION_STATE_CONFLICT",
                "completed training consumption state does not match its plan",
            )


def open_consumption(plan: str | Path, *, accept_plan: str) -> ConsumptionRun:
    """Verify and open one accepted training consumption plan."""

    plan_path = Path(plan).expanduser()
    plan_bytes = _read_regular(plan_path, _MAX_PLAN_BYTES)
    local_runtime = _runtime_from_plan(plan_bytes, plan_path, accept_plan)
    completion_path = local_runtime.run_dir / "completion.json"
    receipt_path = local_runtime.run_dir / "datajig.consumed.json"
    if _entry_exists(completion_path) or _entry_exists(receipt_path):
        run = ConsumptionRun(local_runtime, None, plan_bytes)
        run._validate_completed()
        return run
    try:
        completed = run_native(
            ["consume-info", str(plan_path), "--verify", "--accept-plan", accept_plan]
        )
    except NativeBackendUnavailableError as exc:
        raise ConsumptionStateError("NATIVE_BACKEND_UNAVAILABLE", str(exc)) from exc
    if completed.returncode != 0:
        raise _native_failure(completed.stderr)
    try:
        document = json.loads(completed.stdout)
        runtime = _parse_runtime(document["artifact"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ConsumptionStateError(
            "INVALID_CONSUMPTION_PLAN",
            "native backend returned an invalid training consumption plan",
        ) from exc
    if runtime.plan_id != accept_plan:
        raise ConsumptionStateError(
            "CONSUMPTION_NOT_AUTHORIZED",
            "native backend returned a different training consumption plan",
        )
    root = runtime.manifest.parent.resolve(strict=True)
    bundle = _bundle_from_plan(
        runtime.manifest,
        root,
        {
            "schema_version": 1,
            "verified": True,
            "integrity_scope": "manifest-and-all-shards-v1",
            "bundle_id": runtime.bundle_id,
            "format": "jsonl",
            "record_encoding": "source-json-lf-v1",
            "source": runtime.source,
            "records": runtime.records,
            "bytes": runtime.bytes,
            "splits": [
                {
                    "name": runtime.split,
                    "records": runtime.records,
                    "bytes": runtime.bytes,
                    "shards": [
                        {
                            "relative_path": shard.relative_path,
                            "split": shard.split,
                            "index": shard.index,
                            "records": shard.records,
                            "bytes": shard.bytes,
                            "shard_id": shard.shard_id,
                        }
                        for shard in runtime.shards
                    ],
                }
            ],
        },
    )
    return ConsumptionRun(runtime, bundle, plan_bytes)


def _parse_runtime(raw: Mapping[str, Any]) -> _Runtime:
    required = {
        "schema_version",
        "verified",
        "consumption_plan_id",
        "run_id",
        "consumer",
        "manifest_path",
        "run_dir",
        "plan_path",
        "bundle_id",
        "source",
        "split",
        "records",
        "bytes",
        "shards",
    }
    if set(raw) != required or raw["schema_version"] != 1 or raw["verified"] is not True:
        raise ValueError("unsupported runtime")
    raw_shards = raw["shards"]
    if not isinstance(raw_shards, list) or not raw_shards:
        raise TypeError("invalid shards")
    shards = tuple(
        VerifiedShard(
            relative_path=_string(item, "relative_path"),
            split=_string(item, "split"),
            index=_integer(item, "index"),
            records=_integer(item, "records", positive=True),
            bytes=_integer(item, "bytes", positive=True),
            shard_id=_string(item, "shard_id"),
        )
        for item in raw_shards
        if isinstance(item, Mapping)
    )
    if len(shards) != len(raw_shards):
        raise TypeError("invalid shard")
    source = raw["source"]
    if not isinstance(source, Mapping):
        raise TypeError("invalid source")
    return _Runtime(
        plan_id=_string(raw, "consumption_plan_id"),
        run_id=_string(raw, "run_id"),
        consumer=_string(raw, "consumer"),
        manifest=Path(_string(raw, "manifest_path")),
        run_dir=Path(_string(raw, "run_dir")),
        plan_path=Path(_string(raw, "plan_path")),
        bundle_id=_string(raw, "bundle_id"),
        source=source,
        split=_string(raw, "split"),
        records=_integer(raw, "records", positive=True),
        bytes=_integer(raw, "bytes", positive=True),
        shards=shards,
    )


def _runtime_from_plan(plan_bytes: bytes, plan_path: Path, accept_plan: str) -> _Runtime:
    try:
        raw = json.loads(plan_bytes, object_pairs_hook=_strict_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ConsumptionStateError(
            "INVALID_CONSUMPTION_PLAN", "training consumption plan is invalid"
        ) from exc
    expected_keys = {
        "namespace",
        "kind",
        "schema_version",
        "consumption_plan_id",
        "run_id",
        "consumer",
        "manifest_path",
        "run_dir",
        "bundle_id",
        "source",
        "split",
        "records",
        "bytes",
        "shards",
    }
    if (
        not isinstance(raw, dict)
        or set(raw) != expected_keys
        or raw.get("namespace") != "datajig"
        or raw.get("kind") != "training_consumption_plan"
        or raw.get("schema_version") != 1
    ):
        raise ConsumptionStateError(
            "INVALID_CONSUMPTION_PLAN", "training consumption plan schema is invalid"
        )
    try:
        plan_id = _string(raw, "consumption_plan_id")
    except TypeError as exc:
        raise ConsumptionStateError(
            "INVALID_CONSUMPTION_PLAN", "training consumption plan schema is invalid"
        ) from exc
    if plan_id != accept_plan:
        raise ConsumptionStateError(
            "CONSUMPTION_NOT_AUTHORIZED",
            "accepted training consumption plan identity does not match",
        )
    identity = {
        "namespace": raw["namespace"],
        "kind": raw["kind"],
        "schema_version": raw["schema_version"],
        "run_id": raw["run_id"],
        "consumer": raw["consumer"],
        "manifest_path": raw["manifest_path"],
        "run_dir": raw["run_dir"],
        "bundle_id": raw["bundle_id"],
        "source": raw["source"],
        "split": raw["split"],
        "records": raw["records"],
        "bytes": raw["bytes"],
        "shards": raw["shards"],
    }
    digest = blake3.blake3()
    digest.update(b"datajig-training-consumption-plan-v1\0")
    digest.update(
        json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode()
    )
    if plan_id != f"consume_{digest.hexdigest()}":
        raise ConsumptionStateError(
            "INVALID_CONSUMPTION_PLAN", "training consumption plan identity is invalid"
        )
    runtime_raw = dict(raw)
    runtime_raw.pop("namespace")
    runtime_raw.pop("kind")
    runtime_raw["verified"] = True
    runtime_raw["plan_path"] = str(plan_path.resolve(strict=True))
    try:
        return _parse_runtime(runtime_raw)
    except (TypeError, ValueError) as exc:
        raise ConsumptionStateError(
            "INVALID_CONSUMPTION_PLAN", "training consumption plan schema is invalid"
        ) from exc


def _publish_idempotent(path: Path, payload: bytes) -> None:
    try:
        _publish_new(path, payload)
    except FileExistsError:
        if _read_regular(path, _MAX_STATE_BYTES) != payload:
            raise ConsumptionStateError(
                "CONSUMPTION_STATE_CONFLICT",
                "training consumption completion state conflicts with its plan",
            ) from None


def _publish_new(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    descriptor = None
    try:
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = None
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path, follow_symlinks=False)
        temporary.unlink()
    except Exception:
        if descriptor is not None:
            os.close(descriptor)
        with suppress(FileNotFoundError):
            temporary.unlink()
        raise


def _read_regular(path: Path, maximum: int) -> bytes:
    flags = os.O_RDONLY | os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > maximum:
            raise ConsumptionStateError(
                "CONSUMPTION_STATE_CONFLICT", "training consumption state is not regular"
            )
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            payload = stream.read(maximum + 1)
            if len(payload) > maximum:
                raise ConsumptionStateError(
                    "CONSUMPTION_STATE_CONFLICT",
                    "training consumption state exceeds its size limit",
                )
            return payload
    except ConsumptionStateError:
        raise
    except OSError as exc:
        raise ConsumptionStateError(
            "CONSUMPTION_IO_ERROR", "cannot read training consumption state"
        ) from exc


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        + b"\n"
    )


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON member")
        value[key] = item
    return value


def _require_directory(path: Path) -> None:
    _directory_identity(path)


def _directory_identity(path: Path) -> tuple[int, int]:
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise ConsumptionStateError(
            "CONSUMPTION_STATE_CONFLICT",
            "training consumption directory state is invalid",
        ) from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise ConsumptionStateError(
            "CONSUMPTION_STATE_CONFLICT",
            "training consumption directory state is invalid",
        )
    return metadata.st_dev, metadata.st_ino


def _entry_exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ConsumptionStateError(
            "CONSUMPTION_STATE_CONFLICT",
            "training consumption state cannot be inspected",
        ) from exc
    return True


def _string(value: Mapping[str, Any], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise TypeError(key)
    return item


def _integer(value: Mapping[str, Any], key: str, *, positive: bool = False) -> int:
    item = value.get(key)
    if type(item) is not int or item < (1 if positive else 0):
        raise TypeError(key)
    return item


def _native_failure(stderr: str) -> ConsumptionStateError:
    try:
        error = json.loads(stderr)["error"]
        return ConsumptionStateError(_string(error, "code"), _string(error, "message"))
    except (KeyError, TypeError, json.JSONDecodeError):
        return ConsumptionStateError(
            "CONSUMPTION_VERIFICATION_FAILED",
            "native training consumption verification failed",
        )
