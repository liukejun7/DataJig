from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from collections.abc import Mapping
from contextlib import suppress
from pathlib import Path

import blake3

from datajig.native import run_native
from datajig.pipeline import (
    PipelineError,
    apply_pipeline,
    create_pipeline_plan,
    garbage_collect_pipelines,
    read_pipeline_artifact,
)

_RUN_RECEIPT_KEYS = frozenset(
    {
        "namespace",
        "kind",
        "run_receipt_schema_version",
        "status",
        "intent_id",
        "plan_id",
        "attempt_id",
        "source_content_id",
        "pipeline_id",
        "pipeline_receipt_id",
        "revision_id",
        "bundle_id",
        "consumption_plan_ids",
        "output",
        "run_receipt_id",
    }
)


def execute_accepted_run(
    response: Mapping[str, object], root: Path, *, allow_recovery: bool = False
) -> dict[str, object]:
    artifact = _mapping(response.get("artifact"), "run response artifact")
    if response.get("kind") != "run_plan_accepted" or response.get("decision") != "ready":
        return dict(response)
    intent_id = _text(artifact, "intent_id")
    plan_id = _text(artifact, "plan_id")
    attempt_id = _text(artifact, "attempt_id")
    ast = _mapping(artifact.get("canonical_ast"), "canonical_ast")
    binding = _mapping(artifact.get("binding"), "binding")
    root = root.resolve(strict=True)
    attempt = root / ".datajig" / "runs" / intent_id / "attempts" / attempt_id
    workspace = root / ".datajig" / "runs" / intent_id / "workspace"
    if not attempt.is_dir() or not workspace.is_dir():
        raise PipelineError("RUN_STATE_CORRUPT", "Accepted run workspace is missing")

    expected_source = _text(binding, "source_content_id")
    source_path = _text(_mapping(ast.get("source"), "source"), "path")
    _verify_source_binding(root, source_path, expected_source)
    prepared = _prepare_run_source(root, attempt, ast)
    _verify_source_binding(root, source_path, expected_source)
    pipeline_plan_path = attempt / "pipeline-plan.json"
    if pipeline_plan_path.exists():
        pipeline_plan = read_pipeline_artifact(pipeline_plan_path, verify=True)
    else:
        config = _pipeline_config(root, workspace, prepared, ast, artifact, binding)
        config_path = _write_temporary_config(root, config)
        try:
            pipeline_plan = create_pipeline_plan(config_path, pipeline_plan_path)
        finally:
            config_path.unlink(missing_ok=True)
    pipeline_id = _text(pipeline_plan, "pipeline_id")
    try:
        receipt, decision = apply_pipeline(pipeline_plan_path, pipeline_id, resume=False)
    except PipelineError as exc:
        if not allow_recovery or exc.code != "PIPELINE_RECOVERY_REQUIRED":
            raise
        receipt, decision = apply_pipeline(pipeline_plan_path, pipeline_id, resume=True)
    run_receipt = _build_run_receipt(artifact, binding, receipt)
    run_receipt_path = (
        root
        / ".datajig"
        / "runs"
        / intent_id
        / "receipts"
        / f"{attempt_id}.json"
    )
    _persist_run_receipt(run_receipt_path, run_receipt)
    return {
        "agent_api_version": 1,
        "backend": "python-pipeline",
        "kind": "run_completed",
        "decision": decision,
        "next_actions": [],
        "artifact": {
            **receipt,
            "intent_id": intent_id,
            "plan_id": plan_id,
            "attempt_id": attempt_id,
            "execution_status": "committed",
            "run_receipt_id": run_receipt["run_receipt_id"],
            "run_receipt_path": str(run_receipt_path),
        },
    }


def _build_run_receipt(
    run_plan: Mapping[str, object],
    binding: Mapping[str, object],
    pipeline_receipt: Mapping[str, object],
) -> dict[str, object]:
    raw_consumption = pipeline_receipt.get("consumption_plans")
    consumption_plan_ids = (
        [
            item["plan_id"]
            for item in raw_consumption
            if isinstance(item, dict) and isinstance(item.get("plan_id"), str)
        ]
        if isinstance(raw_consumption, list)
        else []
    )
    basis: dict[str, object] = {
        "namespace": "datajig",
        "kind": "run_receipt",
        "run_receipt_schema_version": 1,
        "status": "committed",
        "intent_id": _text(run_plan, "intent_id"),
        "plan_id": _text(run_plan, "plan_id"),
        "attempt_id": _text(run_plan, "attempt_id"),
        "source_content_id": _text(binding, "source_content_id"),
        "pipeline_id": _text(pipeline_receipt, "pipeline_id"),
        "pipeline_receipt_id": _text(pipeline_receipt, "pipeline_receipt_id"),
        "revision_id": _text(pipeline_receipt, "final_revision"),
        "bundle_id": _text(pipeline_receipt, "bundle_id"),
        "consumption_plan_ids": consumption_plan_ids,
        "output": _text(binding, "output"),
    }
    receipt_id = _run_receipt_identity(basis)
    return {**basis, "run_receipt_id": receipt_id}


def run_lineage(
    artifact_or_id: str, *, state: Path | None = None
) -> dict[str, object] | None:
    candidate = Path(artifact_or_id).expanduser()
    if candidate.is_file():
        document = _load_json_object(candidate, "RUN_RECEIPT_TAMPERED")
        if document.get("kind") != "run_receipt":
            return None
    elif artifact_or_id.startswith("runrcpt_") and state is not None:
        receipt_path = _find_run_receipt(state, artifact_or_id)
        document = _load_json_object(receipt_path, "RUN_RECEIPT_TAMPERED")
    else:
        return None
    receipt = _verify_run_receipt(document)
    return {
        "lineage_schema_version": 1,
        "pipeline_id": receipt["pipeline_id"],
        "run_receipt_id": receipt["run_receipt_id"],
        "nodes": [
            {"kind": "source", "id": receipt["source_content_id"]},
            {"kind": "intent", "id": receipt["intent_id"]},
            {"kind": "plan", "id": receipt["plan_id"]},
            {"kind": "attempt", "id": receipt["attempt_id"]},
            {"kind": "revision", "id": receipt["revision_id"]},
            {"kind": "bundle", "id": receipt["bundle_id"]},
            {"kind": "consumption", "ids": receipt["consumption_plan_ids"]},
            {"kind": "run_receipt", "id": receipt["run_receipt_id"]},
        ],
        "complete": True,
    }


def _verify_run_receipt(document: Mapping[str, object]) -> dict[str, object]:
    if set(document) != _RUN_RECEIPT_KEYS:
        raise PipelineError("RUN_RECEIPT_TAMPERED", "Run receipt has an invalid shape")
    constants = {
        "namespace": "datajig",
        "kind": "run_receipt",
        "run_receipt_schema_version": 1,
        "status": "committed",
    }
    if any(document.get(key) != value for key, value in constants.items()):
        raise PipelineError("RUN_RECEIPT_TAMPERED", "Run receipt contract is invalid")
    prefixes = {
        "intent_id": "intent",
        "plan_id": "plan",
        "attempt_id": "attempt",
        "source_content_id": "source",
        "pipeline_id": "pipe",
        "pipeline_receipt_id": "piped",
        "revision_id": "rev",
        "bundle_id": "bundle",
        "run_receipt_id": "runrcpt",
    }
    for key, prefix in prefixes.items():
        value = document.get(key)
        if not isinstance(value, str) or re.fullmatch(f"{prefix}_[0-9a-f]{{64}}", value) is None:
            raise PipelineError("RUN_RECEIPT_TAMPERED", f"Run receipt {key} is invalid")
    consumption = document.get("consumption_plan_ids")
    if not isinstance(consumption, list):
        raise PipelineError(
            "RUN_RECEIPT_TAMPERED", "Run receipt consumption_plan_ids are invalid"
        )
    consumption_ids: list[str] = []
    for value in consumption:
        if (
            not isinstance(value, str)
            or re.fullmatch(r"consume_[0-9a-f]{64}", value) is None
        ):
            raise PipelineError(
                "RUN_RECEIPT_TAMPERED", "Run receipt consumption_plan_ids are invalid"
            )
        consumption_ids.append(value)
    if len(consumption_ids) > 64 or len(set(consumption_ids)) != len(consumption_ids):
        raise PipelineError(
            "RUN_RECEIPT_TAMPERED", "Run receipt consumption_plan_ids are invalid"
        )
    output = document.get("output")
    if not isinstance(output, str) or not output:
        raise PipelineError("RUN_RECEIPT_TAMPERED", "Run receipt output is invalid")
    basis = {key: value for key, value in document.items() if key != "run_receipt_id"}
    if document["run_receipt_id"] != _run_receipt_identity(basis):
        raise PipelineError("RUN_RECEIPT_TAMPERED", "Run receipt identity does not verify")
    return dict(document)


def _run_receipt_identity(basis: Mapping[str, object]) -> str:
    encoded = json.dumps(
        basis, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return "runrcpt_" + blake3.blake3(
        b"datajig-run-receipt-v1\0" + encoded
    ).hexdigest()


def _find_run_receipt(state: Path, receipt_id: str) -> Path:
    root = _find_datajig_root(state)
    if root is None:
        raise PipelineError("LINEAGE_NOT_FOUND", "Cannot locate the DataJig run index")
    matches = [
        root / _text(entry, "path")
        for entry in _load_or_rebuild_run_index(root)
        if entry.get("run_receipt_id") == receipt_id
    ]
    if len(matches) != 1:
        raise PipelineError(
            "LINEAGE_NOT_FOUND", f"Found {len(matches)} matching run receipts"
        )
    return matches[0]


def _persist_run_receipt(path: Path, receipt: Mapping[str, object]) -> None:
    payload = (
        json.dumps(
            receipt,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor: int | None = None
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            descriptor = None
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path, follow_symlinks=False)
        except FileExistsError:
            flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
            try:
                existing_descriptor = os.open(path, flags)
                try:
                    metadata = os.fstat(existing_descriptor)
                    existing = os.read(existing_descriptor, len(payload) + 1)
                finally:
                    os.close(existing_descriptor)
            except OSError as exc:
                raise PipelineError(
                    "RUN_RECEIPT_CONFLICT",
                    "Persisted run receipt cannot be safely verified",
                ) from exc
            if not stat.S_ISREG(metadata.st_mode) or existing != payload:
                raise PipelineError(
                    "RUN_RECEIPT_CONFLICT",
                    "Persisted run receipt conflicts with the completed execution",
                ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        with suppress(FileNotFoundError):
            temporary.unlink()
    try:
        _refresh_run_index(path.parents[3])
    except OSError as exc:
        raise PipelineError(
            "RUN_INDEX_WRITE_FAILED",
            "Run committed, but its derived status index could not be updated; resume to retry",
        ) from exc


def run_status(state: Path) -> dict[str, object] | None:
    datajig = _find_datajig_root(state)
    if datajig is None:
        return None
    entries = _load_or_rebuild_run_index(datajig)
    recent: dict[str, object] | None = None
    if entries:
        receipt_path = datajig / _text(entries[0], "path")
        receipt = _verify_run_receipt(
            _load_json_object(receipt_path, "RUN_RECEIPT_TAMPERED")
        )
        recent = {
            key: receipt[key]
            for key in (
                "run_receipt_id",
                "intent_id",
                "plan_id",
                "attempt_id",
                "revision_id",
                "bundle_id",
                "status",
            )
        }
    recoverable: list[dict[str, str]] = []
    for attempt in sorted((datajig / "runs").glob("*/attempts/attempt_*")):
        intent = attempt.parents[1].name
        receipt_path = datajig / "runs" / intent / "receipts" / f"{attempt.name}.json"
        if receipt_path.is_file():
            continue
        if (attempt / "pipeline-plan.json").is_file():
            plan = _load_json_object(attempt / "plan.json", "RUN_STATE_CORRUPT")
            recoverable.append(
                {
                    "intent_id": intent,
                    "plan_id": _text(plan, "plan_id"),
                    "attempt_id": attempt.name,
                }
            )
        if len(recoverable) == 64:
            break
    gc = garbage_collect_pipelines(state, dry_run=True)
    raw_pending_gc = gc.get("completed_attempts")
    pending_gc = raw_pending_gc if isinstance(raw_pending_gc, int) else 0
    return {
        "recent_run": recent,
        "recoverable_runs": recoverable,
        "pending_run_gc": pending_gc,
    }


def _find_datajig_root(path: Path) -> Path | None:
    resolved = path.expanduser().resolve(strict=True)
    for candidate in (resolved, *resolved.parents):
        if candidate.name == ".datajig" and (candidate / "runs").is_dir():
            return candidate
    return None


def _refresh_run_index(datajig: Path) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    candidates = sorted(
        (datajig / "runs").glob("*/receipts/*.json"),
        key=lambda path: path.stat().st_mtime_ns,
        reverse=True,
    )
    for path in candidates:
        try:
            receipt = _verify_run_receipt(
                _load_json_object(path, "RUN_RECEIPT_TAMPERED")
            )
        except (OSError, PipelineError):
            continue
        entries.append(
            {
                "path": path.relative_to(datajig).as_posix(),
                "run_receipt_id": receipt["run_receipt_id"],
                "intent_id": receipt["intent_id"],
                "attempt_id": receipt["attempt_id"],
            }
        )
        if len(entries) == 64:
            break
    meta = datajig / "meta"
    meta.mkdir(parents=True, exist_ok=True)
    index_path = meta / "run-index.json"
    payload = {
        "namespace": "datajig",
        "kind": "run_index",
        "run_index_schema_version": 1,
        "entries": entries,
    }
    temporary = index_path.with_name(f".{index_path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, index_path)
    return entries


def _load_or_rebuild_run_index(datajig: Path) -> list[dict[str, object]]:
    index_path = datajig / "meta" / "run-index.json"
    try:
        index = _load_json_object(index_path, "RUN_INDEX_CORRUPT")
        if (
            index.get("namespace") != "datajig"
            or index.get("kind") != "run_index"
            or index.get("run_index_schema_version") != 1
        ):
            raise PipelineError("RUN_INDEX_CORRUPT", "Run index contract is invalid")
        raw_entries = index.get("entries")
        if not isinstance(raw_entries, list) or len(raw_entries) > 64:
            raise PipelineError("RUN_INDEX_CORRUPT", "Run index entries are invalid")
        entries: list[dict[str, object]] = []
        for entry in raw_entries:
            if not isinstance(entry, dict) or set(entry) != {
                "path",
                "run_receipt_id",
                "intent_id",
                "attempt_id",
            }:
                raise PipelineError("RUN_INDEX_CORRUPT", "Run index entry is invalid")
            relative = Path(_text(entry, "path"))
            if relative.is_absolute() or ".." in relative.parts:
                raise PipelineError("RUN_INDEX_CORRUPT", "Run index path is invalid")
            receipt_path = datajig / relative
            receipt = _verify_run_receipt(
                _load_json_object(receipt_path, "RUN_INDEX_CORRUPT")
            )
            if receipt.get("run_receipt_id") != entry.get("run_receipt_id"):
                raise PipelineError("RUN_INDEX_CORRUPT", "Run index identity is stale")
            entries.append(dict(entry))
        indexed_paths = [str(entry["path"]) for entry in entries]
        current_paths = [
            path.relative_to(datajig).as_posix()
            for path in sorted(
                (datajig / "runs").glob("*/receipts/*.json"),
                key=lambda path: path.stat().st_mtime_ns,
                reverse=True,
            )[:64]
        ]
        if indexed_paths != current_paths:
            raise PipelineError("RUN_INDEX_CORRUPT", "Run index membership is stale")
        return entries
    except (OSError, PipelineError):
        return _refresh_run_index(datajig)


def _prepare_run_source(root: Path, attempt: Path, ast: Mapping[str, object]) -> Path:
    source = _mapping(ast.get("source"), "source")
    prepare = _mapping(ast.get("prepare"), "prepare")
    source_path = _resolve_source(root, _text(source, "path"))
    source_format = source.get("format")
    if source_format not in {"csv", "jsonl", "parquet"}:
        raise PipelineError("RUN_PLAN_CORRUPT", "Run source format is not bound")
    source_id = _mapping(source.get("source_id_field"), "source.source_id_field")
    id_field = _text(source_id, "field")
    generated = prepare.get("generated_source_id")
    if generated is not None and not isinstance(generated, str):
        raise PipelineError("RUN_PLAN_CORRUPT", "generated_source_id must be a string or null")
    steps = prepare.get("steps")
    if not isinstance(steps, list):
        raise PipelineError("RUN_PLAN_CORRUPT", "prepare.steps must be an array")

    recipe_path = attempt / "prepare-recipe.json"
    output_path = attempt / "prepared.jsonl"
    plan_path = attempt / "prepare-plan.json"
    recipe = {
        "namespace": "datajig",
        "kind": "prepare",
        "schema_version": 1,
        "source": {"format": source_format},
        "output": {"format": "jsonl"},
        "id_field": id_field,
        "generated_source_id": generated,
        "steps": steps,
    }
    _write_or_verify_json(recipe_path, recipe, "RUN_STATE_CORRUPT")
    if plan_path.exists():
        planned = _load_json_object(plan_path, "RUN_STATE_CORRUPT")
    else:
        planned = _native_artifact(
            [
                "prepare-plan",
                str(source_path),
                "--recipe",
                str(recipe_path),
                "--output",
                str(output_path),
                "--plan",
                str(plan_path),
            ]
        )
    _native_artifact(
        ["prepare-apply", str(plan_path), "--accept-plan", _text(planned, "plan_id")]
    )
    return output_path


def _pipeline_config(
    root: Path,
    workspace: Path,
    prepared: Path,
    ast: Mapping[str, object],
    artifact: Mapping[str, object],
    binding: Mapping[str, object],
) -> dict[str, object]:
    transform = _mapping(ast.get("transform"), "transform")
    export = _mapping(ast.get("export"), "export")
    intent_id = _text(artifact, "intent_id")
    target_dataset = workspace / "dataset.jsonl"
    target_state = workspace / "state"
    mode = "update" if target_dataset.is_file() and target_state.is_dir() else "create"
    splits = _mapping(export.get("splits"), "export.splits")
    split_values = [
        f"{name}={_positive_integer(splits, name)}" for name in ("train", "val", "test")
    ]
    bound_consumption = binding.get("consumption")
    if bound_consumption is None:
        consumers: list[dict[str, str]] = []
    else:
        requested = _mapping(bound_consumption, "binding.consumption")
        consumers = [
            {
                "consumer": _text(requested, "consumer"),
                "split": "train",
                "run_id": _text(requested, "run_id"),
            }
        ]
    return {
        "schema_version": 1,
        "pipeline": {"name": f"run-{intent_id[7:23]}", "provider": "duckdb"},
        "target": {
            "dataset": _logical_path(root, target_dataset),
            "state": _logical_path(root, target_state),
            "mode": mode,
        },
        "delivery": {"output": _text(binding, "output")},
        "inputs": [
            {"alias": "source", "path": _logical_path(root, prepared), "format": "jsonl"}
        ],
        "transform": {
            "sql": _transform_sql(transform),
            "id_field": _text(export, "id_field"),
            "params": [],
        },
        "export": {"split": split_values},
        "consumption_plan": consumers,
        "run": {
            "intent_id": _text(artifact, "intent_id"),
            "plan_id": _text(artifact, "plan_id"),
            "attempt_id": _text(artifact, "attempt_id"),
        },
    }


def _verify_source_binding(root: Path, logical: str, expected: str) -> None:
    observed = _fingerprint_run_source(root, logical)
    if observed != expected:
        raise PipelineError(
            "SOURCE_CHANGED",
            "Run source changed after plan acceptance; create and accept a new run plan",
            expected_source_content_id=expected,
            actual_source_content_id=observed,
        )


def _fingerprint_run_source(root: Path, logical: str) -> str:
    relative = Path(logical)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise PipelineError("RUN_PATH_INVALID", "Run source must be a normalized relative path")
    source = root / relative
    try:
        metadata = source.lstat()
    except OSError as exc:
        raise PipelineError("SOURCE_LOAD_FAILED", f"Cannot inspect run source: {exc}") from exc
    if source.is_symlink() or not (source.is_file() or source.is_dir()):
        raise PipelineError("SOURCE_LOAD_FAILED", "Run source must be a direct file or directory")
    if metadata.st_mode == 0:
        raise PipelineError("SOURCE_LOAD_FAILED", "Run source metadata is invalid")
    if source.is_file():
        files = [(".", source)]
    else:
        files = []
        for candidate in source.rglob("*"):
            candidate_metadata = candidate.lstat()
            if candidate.is_symlink():
                raise PipelineError("SOURCE_LOAD_FAILED", "Run source contains a symbolic link")
            if candidate.is_file():
                files.append((candidate.relative_to(source).as_posix(), candidate))
            elif not candidate.is_dir():
                raise PipelineError("SOURCE_LOAD_FAILED", "Run source contains a special file")
            if candidate_metadata.st_mode == 0:
                raise PipelineError("SOURCE_LOAD_FAILED", "Run source metadata is invalid")
        files.sort(key=lambda item: item[0])
    if not files:
        raise PipelineError("SOURCE_LOAD_FAILED", "Run source contains no files")
    digest = blake3.blake3(b"datajig-run-source-v1\0")
    for name, path in files:
        encoded_name = name.encode("utf-8")
        digest.update(len(encoded_name).to_bytes(8, "little"))
        digest.update(encoded_name)
        size = path.stat().st_size
        digest.update(size.to_bytes(8, "little"))
        with path.open("rb") as stream:
            while chunk := stream.read(64 * 1024):
                digest.update(chunk)
    return f"source_{digest.hexdigest()}"


def _transform_sql(transform: Mapping[str, object]) -> str:
    kind = transform.get("kind")
    if kind == "sql":
        return _text(transform, "sql")
    if kind != "aggregate":
        raise PipelineError("RUN_PLAN_CORRUPT", "Unsupported run transform kind")
    by = _text(transform, "by")
    raw_aggregations = transform.get("aggregations")
    if not isinstance(raw_aggregations, list) or not raw_aggregations:
        raise PipelineError("RUN_PLAN_CORRUPT", "Aggregate transform has no aggregations")
    expressions = [_quote_identifier(by)]
    for raw in raw_aggregations:
        aggregation = _mapping(raw, "transform.aggregations[]")
        function = _text(aggregation, "function").upper()
        if function not in {"COUNT", "SUM", "AVG", "MIN", "MAX"}:
            raise PipelineError("RUN_PLAN_CORRUPT", "Unsupported aggregate function")
        expressions.append(
            f"{function}({_quote_identifier(_text(aggregation, 'field'))}) "
            f"AS {_quote_identifier(_text(aggregation, 'alias'))}"
        )
    return (
        f"SELECT {', '.join(expressions)} FROM source "
        f"GROUP BY {_quote_identifier(by)} ORDER BY {_quote_identifier(by)}"
    )


def _native_artifact(arguments: list[str]) -> dict[str, object]:
    completed = run_native(arguments)
    serialized = completed.stdout if completed.returncode == 0 else completed.stderr
    try:
        payload = json.loads(serialized)
    except (TypeError, json.JSONDecodeError) as exc:
        raise PipelineError(
            "RUN_EXECUTION_FAILED", "Native run step returned invalid JSON"
        ) from exc
    if completed.returncode != 0:
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            raise PipelineError(
                str(error.get("code", "RUN_EXECUTION_FAILED")),
                str(error.get("message", "Native run step failed")),
            )
        raise PipelineError("RUN_EXECUTION_FAILED", "Native run step failed")
    if not isinstance(payload, dict) or not isinstance(payload.get("artifact"), dict):
        raise PipelineError("RUN_EXECUTION_FAILED", "Native run step omitted its artifact")
    artifact = payload["artifact"]
    assert isinstance(artifact, dict)
    return {str(key): value for key, value in artifact.items()}


def _resolve_source(root: Path, value: str) -> Path:
    path = Path(value)
    candidate = path if path.is_absolute() else root / path
    try:
        return candidate.resolve(strict=True)
    except OSError as exc:
        raise PipelineError("SOURCE_LOAD_FAILED", f"Cannot resolve run source: {exc}") from exc


def _logical_path(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise PipelineError(
            "RUN_PATH_INVALID", "Run intermediates must stay under the run root"
        ) from exc


def _write_temporary_config(root: Path, value: Mapping[str, object]) -> Path:
    descriptor, name = tempfile.mkstemp(prefix=".datajig-run-config-", suffix=".json", dir=root)
    path = Path(name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        path.unlink(missing_ok=True)
        raise
    return path


def _write_or_verify_json(path: Path, value: Mapping[str, object], code: str) -> None:
    if path.exists():
        if _load_json_object(path, code) != value:
            raise PipelineError(code, f"Run artifact changed: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    try:
        with path.open("x", encoding="utf-8") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        if _load_json_object(path, code) != value:
            raise PipelineError(code, f"Run artifact changed: {path}") from exc


def _load_json_object(path: Path, code: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PipelineError(code, f"Cannot read run artifact: {path}") from exc
    if not isinstance(value, dict):
        raise PipelineError(code, f"Run artifact is not an object: {path}")
    return value


def _mapping(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise PipelineError("RUN_PLAN_CORRUPT", f"{context} must be an object")
    return value


def _text(value: Mapping[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise PipelineError("RUN_PLAN_CORRUPT", f"{key} must be a non-empty string")
    return item


def _positive_integer(value: Mapping[str, object], key: str) -> int:
    item = value.get(key)
    if not isinstance(item, int) or isinstance(item, bool) or item <= 0:
        raise PipelineError("RUN_PLAN_CORRUPT", f"{key} must be a positive integer")
    return item


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
