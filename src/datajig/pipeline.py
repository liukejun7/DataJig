from __future__ import annotations

import json
import math
import os
import re
import shutil
import stat
import sys
import tempfile
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager, suppress
from pathlib import Path, PurePosixPath
from typing import NoReturn, cast

import blake3
import yaml
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode
from yaml.tokens import AliasToken, AnchorToken, TagToken

from datajig.hashing import compute_blake3
from datajig.providers.duckdb import ProviderError, _probe

PIPELINE_PLAN_SCHEMA_VERSION = 1
_ALIAS = re.compile(r"[a-z][a-z0-9_]{0,63}\Z")
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_RUN_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")
_TOP_KEYS = frozenset(
    {
        "schema_version",
        "pipeline",
        "target",
        "delivery",
        "inputs",
        "transform",
        "validate",
        "export",
        "consumption_plan",
        "run",
    }
)


class PipelineError(Exception):
    def __init__(self, code: str, message: str, **details: object) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details


class _StrictLoader(yaml.SafeLoader):
    pass


def _construct_mapping(
    loader: _StrictLoader, node: MappingNode, deep: bool = False
) -> dict[object, object]:
    loader.flatten_mapping(node)
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise ConstructorError(None, None, f"duplicate YAML key: {key!r}", key_node.start_mark)
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _identity(prefix: str, domain: bytes, value: object) -> str:
    digest = blake3.blake3(domain + bytes((0,)) + _canonical(value)).hexdigest()
    return f"{prefix}_{digest}"


def _object(value: object, keys: frozenset[str], context: str) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        _fail(f"{context} must be a mapping")
    unknown = sorted(set(value) - keys)
    if unknown:
        _fail(
            f"{context} contains unknown field(s): {', '.join(unknown)}; "
            f"allowed fields: {', '.join(sorted(keys))}"
        )
    return value


def _required_text(data: Mapping[str, object], key: str, context: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        _fail(f"{context}.{key} must be a non-empty string")
    if any(ord(character) < 32 and character not in "\n\r\t" for character in value):
        _fail(f"{context}.{key} contains control characters")
    return value


def _required_identity(
    data: Mapping[str, object], key: str, prefix: str, context: str
) -> str:
    value = _required_text(data, key, context)
    if re.fullmatch(rf"{re.escape(prefix)}_[0-9a-f]{{64}}", value) is None:
        _fail(f"{context}.{key} must be an exact {prefix}_ content identity")
    return value


def _positive_int(data: Mapping[str, object], key: str, context: str, default: int) -> int:
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        _fail(f"{context}.{key} must be a positive integer")
    return value


def _logical_path(value: str, context: str) -> str:
    if "\\" in value:
        _fail(f"{context} must use portable '/' separators")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or value in {"", "."}
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        _fail(f"{context} must be a normalized relative path without '..'")
    return path.as_posix()


def _validate_scalar(value: object, context: str) -> object:
    if value is None or isinstance(value, str | bool | int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    _fail(f"{context} must contain only finite JSON-compatible scalar values")


def _validate_parameter(value: object, context: str) -> object:
    if value is None or isinstance(value, str | bool | int):
        return value
    _fail(f"{context} must be a string, boolean, integer, or null")


def _normalize_json(value: object, context: str) -> object:
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            _fail(f"{context} keys must be strings")
        return {key: _normalize_json(value[key], f"{context}.{key}") for key in sorted(value)}
    if isinstance(value, list):
        return [_normalize_json(item, f"{context}[]") for item in value]
    return _validate_scalar(value, context)


def _fail(message: str) -> NoReturn:
    raise PipelineError("INVALID_PIPELINE_CONFIG", message)


def _load_yaml(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise PipelineError("INVALID_PIPELINE_CONFIG", f"Cannot read pipeline YAML: {exc}") from exc
    if len(raw) > 1024 * 1024:
        _fail("Pipeline YAML exceeds the 1 MiB limit")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PipelineError("INVALID_PIPELINE_CONFIG", "Pipeline YAML must be UTF-8") from exc
    try:
        for token in yaml.scan(text):
            if isinstance(token, (AliasToken, AnchorToken, TagToken)):
                _fail("YAML anchors, aliases, and tags are forbidden")
        root = yaml.compose(text, Loader=_StrictLoader)
        if root is not None:
            _reject_non_json_yaml_nodes(root)
        payload = yaml.load(text, Loader=_StrictLoader)
    except PipelineError:
        raise
    except (yaml.YAMLError, ValueError, TypeError) as exc:
        raise PipelineError("INVALID_PIPELINE_CONFIG", f"Invalid YAML: {exc}") from exc
    if not isinstance(payload, dict):
        _fail("YAML root must be a mapping")
    return _object(payload, _TOP_KEYS, "pipeline config")


def _reject_non_json_yaml_nodes(node: Node) -> None:
    if node.tag == "tag:yaml.org,2002:timestamp":
        _fail("YAML implicit timestamps are forbidden; quote the value as a string")
    allowed = {
        "tag:yaml.org,2002:map",
        "tag:yaml.org,2002:seq",
        "tag:yaml.org,2002:str",
        "tag:yaml.org,2002:int",
        "tag:yaml.org,2002:float",
        "tag:yaml.org,2002:bool",
        "tag:yaml.org,2002:null",
    }
    if node.tag not in allowed:
        _fail(f"YAML tag {node.tag!r} is forbidden")
    if isinstance(node, MappingNode):
        for key, value in node.value:
            _reject_non_json_yaml_nodes(key)
            _reject_non_json_yaml_nodes(value)
    elif isinstance(node, SequenceNode):
        for item in node.value:
            _reject_non_json_yaml_nodes(item)
    elif not isinstance(node, ScalarNode):
        _fail("YAML contains an unsupported node")


def create_pipeline_plan(config_path: Path, plan_path: Path) -> dict[str, object]:
    try:
        config_path = config_path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise PipelineError(
            "INVALID_PIPELINE_CONFIG", f"Cannot resolve pipeline config: {exc}"
        ) from exc
    plan_path = plan_path.expanduser().resolve()
    data = _load_yaml(config_path)
    if data.get("schema_version") != 1:
        _fail("schema_version must be exactly 1")

    pipeline = _object(data.get("pipeline"), frozenset({"name", "provider"}), "pipeline")
    name = _required_text(pipeline, "name", "pipeline")
    if _NAME.fullmatch(name) is None:
        _fail("pipeline.name must be 1..128 portable identifier characters")
    provider = _required_text(pipeline, "provider", "pipeline")
    if provider != "duckdb":
        _fail("pipeline.provider must be duckdb in DataJig 0.8")

    target = _object(data.get("target"), frozenset({"dataset", "state", "mode"}), "target")
    target_normalized = {
        "dataset": _logical_path(_required_text(target, "dataset", "target"), "target.dataset"),
        "state": _logical_path(_required_text(target, "state", "target"), "target.state"),
        "mode": _required_text(target, "mode", "target"),
    }
    if target_normalized["mode"] not in {"create", "update"}:
        _fail("target.mode is required and must be create or update")

    delivery = _object(data.get("delivery"), frozenset({"output"}), "delivery")
    delivery_normalized = {
        "output": _logical_path(_required_text(delivery, "output", "delivery"), "delivery.output")
    }
    for context, logical in (
        ("target.dataset", str(target_normalized["dataset"])),
        ("target.state", str(target_normalized["state"])),
        ("delivery.output", str(delivery_normalized["output"])),
    ):
        _validate_path_components(config_path.parent, logical, context, planning=True)

    raw_inputs = data.get("inputs")
    if not isinstance(raw_inputs, list) or not 1 <= len(raw_inputs) <= 16:
        _fail("inputs must contain 1..16 local single-file inputs")
    inputs: list[dict[str, object]] = []
    aliases: set[str] = set()
    for index, raw_input in enumerate(raw_inputs):
        item = _object(raw_input, frozenset({"alias", "path", "format"}), f"inputs[{index}]")
        alias = _required_text(item, "alias", f"inputs[{index}]")
        if _ALIAS.fullmatch(alias) is None or alias in aliases:
            _fail("input aliases must be unique lowercase SQL identifiers")
        aliases.add(alias)
        logical = _logical_path(
            _required_text(item, "path", f"inputs[{index}]"), f"inputs[{index}].path"
        )
        source_format = _required_text(item, "format", f"inputs[{index}]")
        if source_format not in {"csv", "parquet", "jsonl"}:
            _fail("input format must be csv, parquet, or jsonl")
        live = config_path.parent.joinpath(*PurePosixPath(logical).parts)
        try:
            metadata = live.lstat()
        except OSError as exc:
            raise PipelineError(
                "INVALID_PIPELINE_CONFIG",
                f"Input {alias!r} must be an available regular file: {exc}",
            ) from exc
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            _fail(f"Input {alias!r} must be a regular file and not a symbolic link")
        with live.open("rb") as stream:
            digest = compute_blake3(stream)
        inputs.append(
            {
                "alias": alias,
                "path": logical,
                "format": source_format,
                "bytes": metadata.st_size,
                "content_id": f"blob_{digest}",
            }
        )
    inputs.sort(key=lambda item: str(item["alias"]))

    transform = _object(
        data.get("transform"), frozenset({"sql", "id_field", "params"}), "transform"
    )
    sql = _required_text(transform, "sql", "transform")
    if len(sql.encode("utf-8")) > 65536:
        _fail("transform.sql exceeds 64 KiB")
    id_field = _required_text(transform, "id_field", "transform")
    params = transform.get("params", [])
    if not isinstance(params, list) or len(params) > 256:
        _fail("transform.params must be an array of at most 256 scalar values")
    params = [
        _validate_parameter(value, f"transform.params[{index}]")
        for index, value in enumerate(params)
    ]
    transform_normalized = {"sql": sql, "id_field": id_field, "params": params}

    validate = _object(data.get("validate", {}), frozenset({"quality_policy"}), "validate")
    quality = _normalize_quality_policy(validate.get("quality_policy"), config_path.parent)

    export = _object(
        data.get("export"),
        frozenset({"split", "max_shard_records", "max_shard_bytes", "seed"}),
        "export",
    )
    splits = export.get("split")
    if (
        not isinstance(splits, list)
        or not splits
        or any(not isinstance(item, str) for item in splits)
    ):
        _fail("export.split must be a non-empty list such as [train=7, val=2, test=1]")
    normalized_splits = _normalize_splits(splits)
    export_normalized: dict[str, object] = {
        "split": normalized_splits,
        "max_shard_records": _positive_int(export, "max_shard_records", "export", 10000),
        "max_shard_bytes": _positive_int(export, "max_shard_bytes", "export", 268435456),
        "seed": _required_text(export, "seed", "export") if "seed" in export else "datajig-v1",
    }

    raw_consumption = data.get("consumption_plan")
    if not isinstance(raw_consumption, list):
        _fail("consumption_plan must be an array; use [] when no consumer is requested")
    split_names = {item.split("=", 1)[0] for item in normalized_splits}
    consumption: list[dict[str, str]] = []
    for index, raw_consumer in enumerate(raw_consumption):
        consumer = _object(
            raw_consumer,
            frozenset({"consumer", "split", "run_id", "run_dir"}),
            f"consumption_plan[{index}]",
        )
        consumer_name = _required_text(consumer, "consumer", f"consumption_plan[{index}]")
        split = _required_text(consumer, "split", f"consumption_plan[{index}]")
        run_id = _required_text(consumer, "run_id", f"consumption_plan[{index}]")
        if consumer_name not in {"python", "pytorch", "huggingface"}:
            _fail("consumer must be python, pytorch, or huggingface")
        if split not in split_names:
            _fail(f"consumption split {split!r} is not exported")
        if _RUN_ID.fullmatch(run_id) is None:
            _fail("run_id must be 1..128 characters from [A-Za-z0-9._:-]")
        normalized_consumer = {
            "consumer": consumer_name,
            "split": split,
            "run_id": run_id,
        }
        if "run_dir" in consumer:
            run_dir = _logical_path(
                _required_text(consumer, "run_dir", f"consumption_plan[{index}]"),
                f"consumption_plan[{index}].run_dir",
            )
            _validate_path_components(
                config_path.parent,
                run_dir,
                f"consumption_plan[{index}].run_dir",
                planning=True,
            )
            normalized_consumer["run_dir"] = run_dir
        consumption.append(normalized_consumer)
    consumption.sort(
        key=lambda item: (
            item["split"],
            item["consumer"],
            item["run_id"],
            item.get("run_dir", ""),
        )
    )

    run_binding: dict[str, str] | None = None
    if data.get("run") is not None:
        run = _object(
            data.get("run"), frozenset({"intent_id", "plan_id", "attempt_id"}), "run"
        )
        run_binding = {
            "intent_id": _required_identity(run, "intent_id", "intent", "run"),
            "plan_id": _required_identity(run, "plan_id", "plan", "run"),
            "attempt_id": _required_identity(run, "attempt_id", "attempt", "run"),
        }

    base_revision = _base_revision(config_path.parent, target_normalized)
    try:
        provider_identity = _probe()
    except (ImportError, ModuleNotFoundError) as exc:
        raise PipelineError(
            "PROVIDER_UNAVAILABLE",
            "DuckDB pipeline support is not installed",
            remediation="pip install 'datajig[duckdb]'",
        ) from exc
    except ProviderError as exc:
        raise PipelineError(exc.code, exc.message, remediation=exc.remediation) from exc
    bundle_spec = {
        "inputs": inputs,
        "transform": transform_normalized,
        "quality_policy": quality,
        "export": export_normalized,
        "provider": provider_identity,
    }
    bundle_spec_id = _identity("bundlespec", b"datajig-pipeline-bundle-spec-v1", bundle_spec)
    authorization = {
        "pipeline": {"name": name, "provider": provider},
        "target": target_normalized,
        "delivery": delivery_normalized,
        "base_revision": base_revision,
        "bundle_spec_id": bundle_spec_id,
        "consumption_plan": consumption,
    }
    if run_binding is not None:
        authorization["run"] = run_binding
    pipeline_id = _identity("pipe", b"datajig-pipeline-plan-v1", authorization)
    artifact: dict[str, object] = {
        "namespace": "datajig",
        "pipeline_plan_schema_version": PIPELINE_PLAN_SCHEMA_VERSION,
        "pipeline_id": pipeline_id,
        "bundle_spec_id": bundle_spec_id,
        "pipeline": {"name": name, "provider": provider},
        "target": target_normalized,
        "delivery": delivery_normalized,
        "inputs": inputs,
        "transform": transform_normalized,
        "quality_policy": quality,
        "export": export_normalized,
        "consumption_plan": consumption,
        "provider_identity": provider_identity,
        "base_revision": base_revision,
        "bindings": {"config_base": os.path.relpath(config_path.parent, plan_path.parent)},
    }
    if run_binding is not None:
        artifact["run"] = run_binding
    planned_paths = _execution_paths(artifact, plan_path)
    if planned_paths["delivery"].exists() or planned_paths["delivery"].is_symlink():
        if target_normalized["mode"] != "update":
            _fail("create requires delivery.output to be absent")
        _verify_replacement_delivery(
            artifact,
            planned_paths,
            planned_paths["delivery"],
            expected_head=base_revision,
        )
        _probe_atomic_exchange(planned_paths["delivery"].parent)
    if plan_path.exists() or plan_path.is_symlink():
        raise PipelineError("OUTPUT_EXISTS", "Pipeline plan destination already exists")
    _write_new_json(plan_path, artifact)
    return artifact


def _normalize_quality_policy(value: object, config_base: Path) -> dict[str, object] | None:
    if value is None:
        return None
    wrapper = _object(value, frozenset({"inline", "file"}), "validate.quality_policy")
    if ("inline" in wrapper) == ("file" in wrapper):
        _fail("quality_policy requires exactly one of inline or file")
    if "inline" in wrapper:
        content = _normalize_json(wrapper["inline"], "quality_policy.inline")
    else:
        logical = _logical_path(
            _required_text(wrapper, "file", "quality_policy"), "quality_policy.file"
        )
        path = config_base.joinpath(*PurePosixPath(logical).parts)
        try:
            metadata = path.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                _fail("quality_policy.file must be a regular file and not a symbolic link")
            content = _normalize_json(
                json.loads(path.read_text(encoding="utf-8")), "quality_policy.file"
            )
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise PipelineError(
                "INVALID_PIPELINE_CONFIG", f"Invalid quality policy file: {exc}"
            ) from exc
    return {
        "content": content,
        "content_id": _identity("policy", b"datajig-pipeline-policy-v1", content),
    }


def _normalize_splits(values: Sequence[str]) -> list[str]:
    parsed: list[tuple[str, int]] = []
    seen: set[str] = set()
    for item in values:
        if item.count("=") != 1:
            _fail("split must use NAME=WEIGHT, for example train=7")
        name, raw_weight = item.split("=", 1)
        if _ALIAS.fullmatch(name) is None or name in seen:
            _fail("split names must be unique lowercase identifiers")
        try:
            weight = int(raw_weight)
        except ValueError:
            _fail("split weights must be positive integers")
        if weight <= 0:
            _fail("split weights must be positive integers")
        seen.add(name)
        parsed.append((name, weight))
    return [f"{name}={weight}" for name, weight in sorted(parsed)]


def _base_revision(config_base: Path, target: Mapping[str, object]) -> str | None:
    mode = target["mode"]
    dataset = config_base.joinpath(*PurePosixPath(str(target["dataset"])).parts)
    state = config_base.joinpath(*PurePosixPath(str(target["state"])).parts)
    if mode == "create":
        if dataset.exists() or dataset.is_symlink():
            _fail("create requires target.dataset to be absent")
        if state.exists() and (state.is_symlink() or not state.is_dir() or any(state.iterdir())):
            _fail("create requires target.state to be absent or a safe empty directory")
        return None
    if not dataset.is_file() or not state.is_dir():
        _fail("update requires target.dataset and target.state to exist")
    from datajig.native import run_native

    completed = run_native(["status", "--state", str(state)])
    if completed.returncode != 0:
        raise PipelineError(
            "INVALID_PIPELINE_CONFIG", "update target is not a valid DataJig workspace"
        )
    try:
        payload = json.loads(completed.stdout)
        artifact = payload["artifact"]
        bound = Path(artifact["dataset_path"]).resolve(strict=True)
        revision = artifact["head_revision_id"]
    except (KeyError, TypeError, json.JSONDecodeError, OSError) as exc:
        raise PipelineError(
            "INVALID_PIPELINE_CONFIG", "update workspace status is invalid"
        ) from exc
    if bound != dataset.resolve(strict=True) or not isinstance(revision, str):
        _fail("update state is not bound to target.dataset")
    return revision


def _write_new_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor: int | None = None
    temporary: Path | None = None
    try:
        descriptor, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        temporary = Path(name)
        with os.fdopen(descriptor, "w", encoding="utf-8", closefd=True) as stream:
            descriptor = None
            json.dump(
                payload,
                stream,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path)
        _sync_directory(path.parent)
    except FileExistsError as exc:
        raise PipelineError("OUTPUT_EXISTS", f"Output already exists: {path}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rename_directory_noreplace(source: Path, destination: Path) -> None:
    import ctypes
    import errno

    libc = ctypes.CDLL(None, use_errno=True)
    source_bytes = os.fsencode(source)
    destination_bytes = os.fsencode(destination)
    try:
        if sys.platform.startswith("linux"):
            rename = libc.renameat2
            rename.argtypes = (
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_uint,
            )
            rename.restype = ctypes.c_int
            result = rename(-100, source_bytes, -100, destination_bytes, 1)
        elif sys.platform == "darwin":
            rename = libc.renamex_np
            rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
            rename.restype = ctypes.c_int
            result = rename(source_bytes, destination_bytes, 4)
        else:
            raise OSError(errno.ENOTSUP, "atomic no-replace rename is unsupported")
    except AttributeError as exc:
        raise OSError(errno.ENOTSUP, "atomic no-replace rename is unavailable") from exc
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), destination)
    _sync_directory(destination.parent)


def _exchange_directories(left: Path, right: Path) -> None:
    import ctypes
    import errno

    libc = ctypes.CDLL(None, use_errno=True)
    left_bytes = os.fsencode(left)
    right_bytes = os.fsencode(right)
    try:
        if sys.platform.startswith("linux"):
            rename = libc.renameat2
            rename.argtypes = (
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_int,
                ctypes.c_char_p,
                ctypes.c_uint,
            )
            rename.restype = ctypes.c_int
            result = rename(-100, left_bytes, -100, right_bytes, 2)
        elif sys.platform == "darwin":
            rename = libc.renamex_np
            rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
            rename.restype = ctypes.c_int
            result = rename(left_bytes, right_bytes, 2)
        else:
            raise OSError(errno.ENOTSUP, "atomic directory exchange is unsupported")
    except AttributeError as exc:
        raise OSError(errno.ENOTSUP, "atomic directory exchange is unavailable") from exc
    if result != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), right)
    _sync_directory(left.parent)
    if right.parent != left.parent:
        _sync_directory(right.parent)


def _probe_atomic_exchange(parent: Path) -> None:
    left = Path(tempfile.mkdtemp(prefix=".datajig-exchange-probe-a-", dir=parent))
    right = Path(tempfile.mkdtemp(prefix=".datajig-exchange-probe-b-", dir=parent))
    try:
        (left / "left").write_text("left", encoding="utf-8")
        (right / "right").write_text("right", encoding="utf-8")
        _exchange_directories(left, right)
        if not (left / "right").is_file() or not (right / "left").is_file():
            raise OSError("atomic directory exchange did not swap both entries")
        _exchange_directories(left, right)
        if not (left / "left").is_file() or not (right / "right").is_file():
            raise OSError("atomic directory exchange could not be reversed")
    except OSError as exc:
        raise PipelineError(
            "ATOMIC_EXCHANGE_UNSUPPORTED",
            "The delivery filesystem does not support atomic directory exchange; "
            "use a new delivery directory or a compatible local filesystem",
            delivery_parent=str(parent),
        ) from exc
    finally:
        shutil.rmtree(left, ignore_errors=True)
        shutil.rmtree(right, ignore_errors=True)


def read_pipeline_artifact(path: Path, verify: bool = False) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PipelineError(
            "INVALID_PIPELINE_ARTIFACT", f"Cannot read pipeline artifact: {exc}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("pipeline_plan_schema_version") != 1:
        raise PipelineError("INVALID_PIPELINE_ARTIFACT", "Unsupported pipeline artifact")
    if verify:
        allowed = {
            "namespace",
            "pipeline_plan_schema_version",
            "pipeline_id",
            "bundle_spec_id",
            "pipeline",
            "target",
            "delivery",
            "inputs",
            "transform",
            "quality_policy",
            "export",
            "consumption_plan",
            "provider_identity",
            "base_revision",
            "bindings",
            "run",
        }
        unknown = sorted(set(payload) - allowed)
        if unknown:
            raise PipelineError(
                "INVALID_PIPELINE_ARTIFACT",
                f"Pipeline plan contains unknown field(s): {', '.join(unknown)}",
            )
        expected_bundle = payload.get("bundle_spec_id")
        bundle_spec = {
            "inputs": payload.get("inputs"),
            "transform": payload.get("transform"),
            "quality_policy": payload.get("quality_policy"),
            "export": payload.get("export"),
            "provider": payload.get("provider_identity"),
        }
        actual_bundle = _identity("bundlespec", b"datajig-pipeline-bundle-spec-v1", bundle_spec)
        if expected_bundle != actual_bundle:
            raise PipelineError(
                "PIPELINE_PLAN_TAMPERED",
                "Pipeline data preparation no longer matches its bundle identity",
                expected_id=expected_bundle,
                actual_id=actual_bundle,
            )
        expected = payload.get("pipeline_id")
        actual = calculate_pipeline_id(payload)
        if expected != actual:
            raise PipelineError(
                "PIPELINE_PLAN_TAMPERED",
                "Pipeline plan content no longer matches its accepted identity",
                expected_id=expected,
                actual_id=actual,
            )
    return payload


def calculate_pipeline_id(plan: Mapping[str, object]) -> str:
    authorization = {
        "pipeline": plan.get("pipeline"),
        "target": plan.get("target"),
        "delivery": plan.get("delivery"),
        "base_revision": plan.get("base_revision"),
        "bundle_spec_id": plan.get("bundle_spec_id"),
        "consumption_plan": plan.get("consumption_plan"),
    }
    if plan.get("run") is not None:
        authorization["run"] = plan.get("run")
    return _identity("pipe", b"datajig-pipeline-plan-v1", authorization)


def read_pipeline_document(path: Path, *, verify: bool = False) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PipelineError(
            "INVALID_PIPELINE_ARTIFACT", f"Cannot read pipeline artifact: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise PipelineError("INVALID_PIPELINE_ARTIFACT", "Pipeline artifact must be an object")
    if payload.get("pipeline_plan_schema_version") == 1:
        return read_pipeline_artifact(path, verify=verify)
    if payload.get("pipeline_receipt_schema_version") == 1:
        if verify:
            _verify_pipeline_receipt(payload)
        return payload
    raise PipelineError("INVALID_PIPELINE_ARTIFACT", "Unsupported pipeline artifact")


def _verify_pipeline_receipt(receipt: Mapping[str, object]) -> None:
    expected = receipt.get("pipeline_receipt_id")
    basis = {key: value for key, value in receipt.items() if key != "pipeline_receipt_id"}
    actual = _identity("piped", b"datajig-pipeline-receipt-v1", basis)
    if expected != actual or receipt.get("status") != "committed":
        raise PipelineError(
            "PIPELINE_RECEIPT_TAMPERED",
            "Pipeline receipt content no longer matches its success identity",
            expected_id=expected,
            actual_id=actual,
        )


def render_pipeline_markdown(artifact: Mapping[str, object]) -> str:
    pipeline_id = artifact.get("pipeline_id")
    if not isinstance(pipeline_id, str):
        raise PipelineError("INVALID_PIPELINE_ARTIFACT", "Pipeline identity is missing")
    lines = [f"# DataJig Pipeline `{pipeline_id}`", ""]
    if artifact.get("pipeline_receipt_schema_version") == 1:
        lines.extend(
            (
                f"- Receipt: `{artifact.get('pipeline_receipt_id')}`",
                f"- Status: `{artifact.get('status')}`",
                f"- Revision: `{artifact.get('final_revision')}`",
                f"- Bundle: `{artifact.get('bundle_id')}`",
            )
        )
    else:
        pipeline = artifact.get("pipeline")
        target = artifact.get("target")
        if not isinstance(pipeline, dict) or not isinstance(target, dict):
            raise PipelineError("INVALID_PIPELINE_ARTIFACT", "Pipeline plan shape is invalid")
        lines.extend(
            (
                f"- Name: `{pipeline.get('name')}`",
                f"- Mode: `{target.get('mode')}`",
                f"- Bundle specification: `{artifact.get('bundle_spec_id')}`",
            )
        )
    return "\n".join(lines) + "\n"


def pipeline_lineage(artifact_or_id: str, *, state: Path | None = None) -> dict[str, object]:
    candidate = Path(artifact_or_id).expanduser()
    if candidate.is_file():
        document = read_pipeline_document(candidate, verify=True)
    elif state is not None:
        state = state.expanduser().resolve(strict=True)
        receipt_path = _find_pipeline_receipt(state, artifact_or_id)
        document = read_pipeline_document(receipt_path, verify=True)
    else:
        raise PipelineError(
            "LINEAGE_NOT_FOUND",
            "Pass a pipeline plan/receipt path, or an artifact ID together with --state",
        )
    pipeline_id = _plan_text(document, "pipeline_id")
    if document.get("pipeline_receipt_schema_version") == 1:
        consumption = document.get("consumption_plans")
        plan_ids = (
            [item.get("plan_id") for item in consumption if isinstance(item, dict)]
            if isinstance(consumption, list)
            else []
        )
        source_ids = document.get("source_content_ids")
        nodes = [
            {
                "kind": "source",
                "ids": source_ids if isinstance(source_ids, list) else [],
            },
            {"kind": "transform", "id": document.get("transform_receipt_id")},
            {"kind": "revision", "id": document.get("final_revision")},
            {"kind": "bundle", "id": document.get("bundle_id")},
            {"kind": "consumption_plan", "ids": plan_ids},
        ]
    else:
        inputs = document.get("inputs")
        content_ids = (
            [item.get("content_id") for item in inputs if isinstance(item, dict)]
            if isinstance(inputs, list)
            else []
        )
        nodes = [
            {"kind": "source", "ids": content_ids},
            {"kind": "transform", "status": "planned"},
            {"kind": "revision", "status": "pending"},
            {"kind": "bundle", "id": document.get("bundle_spec_id"), "status": "planned"},
            {"kind": "consumption_plan", "status": "planned"},
        ]
    return {
        "lineage_schema_version": 1,
        "pipeline_id": pipeline_id,
        "nodes": nodes,
        "complete": document.get("pipeline_receipt_schema_version") == 1,
    }


def _find_pipeline_receipt(state: Path, artifact_id: str) -> Path:
    matches: list[Path] = []
    for receipt_path in sorted((state / "pipelines").glob("pipe_*/receipt.json")):
        try:
            candidate = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        if not isinstance(candidate, dict):
            continue
        identities: set[str] = {
            value
            for key in (
                "pipeline_id",
                "pipeline_receipt_id",
                "final_revision",
                "bundle_id",
                "transform_plan_id",
                "transform_receipt_id",
                "change_id",
                "changeset_id",
                "report_id",
            )
            if isinstance((value := candidate.get(key)), str)
        }
        source_ids = candidate.get("source_content_ids")
        if isinstance(source_ids, list):
            identities.update(value for value in source_ids if isinstance(value, str))
        consumption = candidate.get("consumption_plans")
        if isinstance(consumption, list):
            identities.update(
                item["plan_id"]
                for item in consumption
                if isinstance(item, dict) and isinstance(item.get("plan_id"), str)
            )
        if artifact_id in identities:
            matches.append(receipt_path)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        raise PipelineError(
            "LINEAGE_AMBIGUOUS",
            "The artifact identity is present in more than one pipeline receipt",
            artifact_id=artifact_id,
        )
    raise PipelineError(
        "LINEAGE_NOT_FOUND",
        "No committed pipeline receipt contains the requested artifact identity",
        artifact_id=artifact_id,
    )


def garbage_collect_pipelines(state: Path, *, dry_run: bool) -> dict[str, object]:
    state = state.expanduser().resolve(strict=True)
    roots = (state / "pipelines", state.parent / ".datajig-pipeline")
    recoverable: list[Path] = []
    completed: list[Path] = []
    for root in roots:
        if not root.is_dir():
            continue
        for journal_path in root.glob("*/attempts/*/journal.json"):
            journal = _load_journal(journal_path)
            if journal is None:
                continue
            attempt = journal_path.parent
            if journal.get("phase") == "completed":
                completed.append(attempt)
            else:
                recoverable.append(attempt)
    if recoverable and not dry_run:
        raise PipelineError(
            "PIPELINE_GC_REQUIRES_RESUME",
            "Recoverable executions are retained because cleanup could strand HEAD or live data",
            remediation="Resume each reported pipeline with pipeline apply --resume",
            attempts=[str(path) for path in recoverable[:20]],
        )
    removed = 0
    if not dry_run:
        for attempt in completed:
            shutil.rmtree(attempt)
            removed += 1
    return {
        "pipeline_gc_schema_version": 1,
        "state": str(state),
        "dry_run": dry_run,
        "recoverable_attempts": len(recoverable),
        "completed_attempts": len(completed),
        "removed_attempts": removed,
        "attempts": [str(path) for path in (recoverable + completed)[:100]],
    }


def envelope(artifact: Mapping[str, object], next_actions: Sequence[str] = ()) -> dict[str, object]:
    return {
        "agent_api_version": 1,
        "artifact": dict(artifact),
        "next_actions": list(next_actions),
    }


def apply_pipeline(
    plan_path: Path, accepted_pipeline_id: str, *, resume: bool = False
) -> tuple[dict[str, object], str]:
    try:
        plan_path = plan_path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise PipelineError(
            "INVALID_PIPELINE_ARTIFACT", f"Cannot resolve pipeline plan: {exc}"
        ) from exc
    plan = read_pipeline_artifact(plan_path, verify=True)
    pipeline_id = _plan_text(plan, "pipeline_id")
    if accepted_pipeline_id != pipeline_id:
        raise PipelineError(
            "PIPELINE_ACCEPTANCE_MISMATCH",
            "Accepted pipeline identity does not match the plan",
            expected_id=pipeline_id,
            actual_id=accepted_pipeline_id,
        )
    paths = _execution_paths(plan, plan_path)
    existing = _committed_delivery(paths["delivery"], pipeline_id, allow_other=True)
    if existing is None and (paths["delivery"].exists() or paths["delivery"].is_symlink()):
        target_mode = _mapping_text(_plan_mapping(plan, "target"), "mode", "target")
        if not resume and target_mode == "update":
            _verify_replacement_delivery(
                plan,
                paths,
                paths["delivery"],
                expected_head=plan.get("base_revision"),
            )
            _probe_atomic_exchange(paths["delivery"].parent)
        elif not resume:
            raise PipelineError(
                "DELIVERY_CONFLICT",
                "DataJig refuses to replace an existing delivery not owned by this pipeline",
            )

    control_root = _control_root(plan, paths["state"], pipeline_id)
    control_root.mkdir(parents=True, exist_ok=True)
    attempt_id = _identity(
        "attempt",
        b"datajig-pipeline-attempt-v1",
        {
            "pipeline_id": pipeline_id,
            "state": str(paths["state"]),
            "delivery": str(paths["delivery"]),
        },
    )
    attempt = control_root / "attempts" / attempt_id
    journal_path = attempt / "journal.json"
    with ExitStack() as stack:
        stack.enter_context(_advisory_lock(control_root / "pipeline.lock", "PIPELINE_BUSY"))
        workspace_lock = (
            paths["state"] / ".pipeline.workspace.lock"
            if paths["state"].is_dir()
            else paths["state"].parent / f".datajig-workspace-{_short_path_id(paths['state'])}.lock"
        )
        stack.enter_context(_advisory_lock(workspace_lock, "WORKSPACE_BUSY"))

        existing = _committed_delivery(paths["delivery"], pipeline_id, allow_other=True)
        if existing is not None:
            _persist_success(paths["state"], pipeline_id, existing)
            recovered_journal = _load_journal(journal_path)
            if recovered_journal is not None:
                _cleanup_completed_attempt(attempt, _journal_artifacts(recovered_journal))
            return existing, "already_applied"
        journal = _load_journal(journal_path)
        if journal is not None and journal.get("phase") != "completed" and not resume:
            raise PipelineError(
                "PIPELINE_RECOVERY_REQUIRED",
                "A recoverable execution already exists; ordinary reruns never "
                "continue it silently",
                phase=journal.get("phase"),
                remediation=(
                    f"datajig pipeline apply {plan_path} --accept-plan {pipeline_id} --resume"
                ),
            )
        if journal is None:
            if resume:
                raise PipelineError("PIPELINE_NOT_RECOVERABLE", "No recoverable execution exists")
            attempt.mkdir(parents=True, exist_ok=False)
            journal = {
                "pipeline_journal_schema_version": 1,
                "attempt_id": attempt_id,
                "pipeline_id": pipeline_id,
                "phase": "planned",
                "created_unix_ns": time.time_ns(),
                "artifacts": {},
            }
            _save_journal(journal_path, journal)

        phase = _journal_phase(journal)
        artifacts = _journal_artifacts(journal)
        if phase == "planned":
            _verify_live_inputs(plan, paths["config_base"])
            _execute_transform(plan, paths, attempt, artifacts)
            _advance_journal(journal_path, journal, "transformed")
            phase = "transformed"
        if phase == "transformed":
            _commit_workspace_head(plan, paths, attempt, artifacts, journal_path, journal)
            _advance_journal(journal_path, journal, "revision_prepared")
            phase = "revision_prepared"
            if os.environ.get("DATAJIG_PIPELINE_FAILPOINT") == "after_detached_prepare":
                raise PipelineError(
                    "PIPELINE_INTERRUPTED",
                    "Pipeline stopped after preparing its detached revision",
                    phase=phase,
                    retryable=True,
                )
        if phase == "revision_prepared":
            _prepare_delivery(plan, paths, attempt, artifacts)
            _advance_journal(journal_path, journal, "prepared")
            phase = "prepared"
        if phase == "prepared":
            _commit_prepared_head(plan, paths, artifacts)
            _advance_journal(journal_path, journal, "head_committed")
            phase = "head_committed"
            if os.environ.get("DATAJIG_PIPELINE_FAILPOINT") == "after_head_commit":
                raise PipelineError(
                    "PIPELINE_INTERRUPTED",
                    "Pipeline stopped at the requested recovery boundary",
                    phase=phase,
                    retryable=True,
                )
        if phase in {"head_committed", "delivery_exchanged"}:
            receipt = _publish_delivery(
                plan, paths, attempt, artifacts, journal_path, journal, phase
            )
            _advance_journal(journal_path, journal, "delivery_committed")
            phase = "delivery_committed"
        else:
            recovered_receipt = _committed_delivery(paths["delivery"], pipeline_id)
            if recovered_receipt is None:
                raise PipelineError(
                    "PIPELINE_RECOVERY_CORRUPT", "Committed delivery marker is missing"
                )
            receipt = recovered_receipt
        if phase == "delivery_committed":
            _persist_success(paths["state"], pipeline_id, receipt)
            _advance_journal(journal_path, journal, "completed")
            _cleanup_completed_attempt(attempt, artifacts)
        return receipt, "applied"


def _cleanup_completed_attempt(attempt: Path, artifacts: Mapping[str, object]) -> None:
    transform_receipt = artifacts.get("transform_receipt")
    if isinstance(transform_receipt, str):
        receipt_path = Path(transform_receipt)
        if receipt_path.name.endswith(".datajig.transform.json"):
            with suppress(OSError):
                receipt_path.unlink(missing_ok=True)
    with suppress(OSError):
        shutil.rmtree(attempt)


def _execution_paths(plan: Mapping[str, object], plan_path: Path) -> dict[str, Path]:
    bindings = _plan_mapping(plan, "bindings")
    config_base_value = _mapping_text(bindings, "config_base", "bindings")
    config_base = (plan_path.parent / config_base_value).resolve(strict=True)
    target = _plan_mapping(plan, "target")
    delivery = _plan_mapping(plan, "delivery")
    return {
        "config_base": config_base,
        "dataset": _resolve_logical(config_base, _mapping_text(target, "dataset", "target")),
        "state": _resolve_logical(config_base, _mapping_text(target, "state", "target")),
        "delivery": _resolve_logical(config_base, _mapping_text(delivery, "output", "delivery")),
    }


def _resolve_logical(base: Path, logical: str) -> Path:
    normalized = _logical_path(logical, "plan path")
    _validate_path_components(base, normalized, "plan path", planning=False)
    return base.joinpath(*PurePosixPath(normalized).parts)


def _validate_path_components(base: Path, logical: str, context: str, *, planning: bool) -> None:
    current = base
    parts = PurePosixPath(logical).parts
    for index, part in enumerate(parts):
        current = current / part
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            break
        except OSError as exc:
            code = "INVALID_PIPELINE_CONFIG" if planning else "PIPELINE_PATH_UNSAFE"
            raise PipelineError(code, f"Cannot inspect {context}: {exc}") from exc
        if stat.S_ISLNK(metadata.st_mode):
            code = "INVALID_PIPELINE_CONFIG" if planning else "PIPELINE_PATH_UNSAFE"
            raise PipelineError(code, f"{context} crosses a symbolic link at {current}")
        if index < len(parts) - 1 and not stat.S_ISDIR(metadata.st_mode):
            code = "INVALID_PIPELINE_CONFIG" if planning else "PIPELINE_PATH_UNSAFE"
            raise PipelineError(code, f"{context} crosses a non-directory path at {current}")


def _control_root(plan: Mapping[str, object], state: Path, pipeline_id: str) -> Path:
    mode = _mapping_text(_plan_mapping(plan, "target"), "mode", "target")
    if mode == "create":
        return state.parent / ".datajig-pipeline" / pipeline_id
    return state / "pipelines" / pipeline_id


def _short_path_id(path: Path) -> str:
    return blake3.blake3(str(path).encode("utf-8")).hexdigest()[:16]


@contextmanager
def _advisory_lock(path: Path, error_code: str) -> Iterator[None]:
    import fcntl

    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0), 0o600)
    stream = os.fdopen(descriptor, "r+")
    try:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PipelineError(
                error_code, "Another pipeline process holds the required OS lock"
            ) from exc
        stream.seek(0)
        stream.truncate()
        stream.write(json.dumps({"pid": os.getpid(), "started_unix_ns": time.time_ns()}))
        stream.flush()
        yield
    finally:
        stream.close()


def _load_journal(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PipelineError(
            "PIPELINE_RECOVERY_CORRUPT", f"Execution journal is unreadable: {exc}"
        ) from exc
    if not isinstance(payload, dict) or payload.get("pipeline_journal_schema_version") != 1:
        raise PipelineError("PIPELINE_RECOVERY_CORRUPT", "Execution journal schema is invalid")
    return payload


def _save_journal(path: Path, journal: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("x", encoding="utf-8") as stream:
        json.dump(journal, stream, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    _sync_directory(path.parent)


def _advance_journal(path: Path, journal: dict[str, object], phase: str) -> None:
    journal["phase"] = phase
    journal["updated_unix_ns"] = time.time_ns()
    _save_journal(path, journal)


def _journal_phase(journal: Mapping[str, object]) -> str:
    phase = journal.get("phase")
    if phase not in {
        "planned",
        "transformed",
        "revision_prepared",
        "head_committed",
        "prepared",
        "delivery_exchanged",
        "delivery_committed",
        "completed",
    }:
        raise PipelineError("PIPELINE_RECOVERY_CORRUPT", "Execution journal phase is invalid")
    return phase


def _journal_artifacts(journal: dict[str, object]) -> dict[str, object]:
    artifacts = journal.get("artifacts")
    if not isinstance(artifacts, dict):
        raise PipelineError("PIPELINE_RECOVERY_CORRUPT", "Execution journal artifacts are invalid")
    return artifacts


def _verify_live_inputs(plan: Mapping[str, object], config_base: Path) -> None:
    inputs = plan.get("inputs")
    if not isinstance(inputs, list):
        raise PipelineError("INVALID_PIPELINE_ARTIFACT", "Pipeline inputs are invalid")
    for raw in inputs:
        if not isinstance(raw, dict):
            raise PipelineError("INVALID_PIPELINE_ARTIFACT", "Pipeline input is invalid")
        alias = _mapping_text(raw, "alias", "input")
        source = _resolve_logical(config_base, _mapping_text(raw, "path", "input"))
        try:
            metadata = source.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
                raise OSError("not a direct regular file")
            with source.open("rb") as stream:
                actual = f"blob_{compute_blake3(stream)}"
        except OSError as exc:
            raise PipelineError(
                "PIPELINE_INPUT_DRIFT",
                f"Pipeline input {alias!r} is unavailable",
                input_alias=alias,
            ) from exc
        if actual != raw.get("content_id") or metadata.st_size != raw.get("bytes"):
            raise PipelineError(
                "PIPELINE_INPUT_DRIFT",
                f"Pipeline input {alias!r} changed after planning",
                input_alias=alias,
                expected_content_id=raw.get("content_id"),
                actual_content_id=actual,
            )


def _execute_transform(
    plan: Mapping[str, object],
    paths: Mapping[str, Path],
    attempt: Path,
    artifacts: dict[str, object],
) -> None:
    if "transform_receipt" not in artifacts:
        _verify_live_pipeline_provider(plan)
    staging = attempt / "staging"
    staging.mkdir(parents=True, exist_ok=True)
    mode = _mapping_text(_plan_mapping(plan, "target"), "mode", "target")
    output = paths["dataset"] if mode == "create" else staging / "candidate.jsonl"
    output.parent.mkdir(parents=True, exist_ok=True)
    transform_plan = staging / "transform-plan.json"
    if "transform_receipt" in artifacts:
        if not output.is_file() or not Path(str(artifacts["transform_receipt"])).is_file():
            raise PipelineError(
                "PIPELINE_RECOVERY_CORRUPT", "Transform recovery artifacts are missing"
            )
        return
    inputs = plan.get("inputs")
    transform = _plan_mapping(plan, "transform")
    assert isinstance(inputs, list)
    arguments = ["transform-plan"]
    for raw in inputs:
        assert isinstance(raw, dict)
        alias = _mapping_text(raw, "alias", "input")
        source = _resolve_logical(paths["config_base"], _mapping_text(raw, "path", "input"))
        arguments.extend(("--input", f"{alias}={source}"))
    arguments.extend(("--sql", _mapping_text(transform, "sql", "transform")))
    parameters = transform.get("params", [])
    if parameters:
        params_path = staging / "transform-params.json"
        with params_path.open("x", encoding="utf-8") as stream:
            json.dump(parameters, stream, ensure_ascii=False, separators=(",", ":"))
            stream.write("\n")
        arguments.extend(("--params-file", str(params_path)))
    arguments.extend(
        (
            "--id-field",
            _mapping_text(transform, "id_field", "transform"),
            "--output",
            str(output),
            "--plan",
            str(transform_plan),
        )
    )
    planned = _run_native_artifact(arguments)
    applied = _run_native_artifact(
        [
            "transform-apply",
            str(transform_plan),
            "--accept-plan",
            _mapping_text(planned, "plan_id", "transform plan"),
        ]
    )
    artifacts.update(
        {
            "transform_plan_id": planned["plan_id"],
            "transform_receipt_id": applied["receipt_id"],
            "transform_receipt": applied["receipt"],
            "transformed_output": applied["output"],
            "transformed_content_id": applied["output_content_id"],
        }
    )


def _verify_live_pipeline_provider(plan: Mapping[str, object]) -> None:
    expected = _plan_mapping(plan, "provider_identity")
    try:
        actual = _probe()
    except (ImportError, ModuleNotFoundError) as exc:
        raise PipelineError(
            "PROVIDER_UNAVAILABLE",
            "DuckDB pipeline support is not installed",
            remediation="pip install 'datajig[duckdb]'",
        ) from exc
    except ProviderError as exc:
        raise PipelineError(exc.code, exc.message, remediation=exc.remediation) from exc
    if dict(actual) != dict(expected):
        raise PipelineError(
            "PIPELINE_PROVIDER_DRIFT",
            "The transform provider changed after this pipeline plan was accepted",
            expected_provider_id=expected.get("provider_id"),
            actual_provider_id=actual.get("provider_id"),
            remediation="create and accept a new pipeline plan with the active provider",
        )


def _commit_workspace_head(
    plan: Mapping[str, object],
    paths: Mapping[str, Path],
    attempt: Path,
    artifacts: dict[str, object],
    journal_path: Path,
    journal: dict[str, object],
) -> None:
    mode = _mapping_text(_plan_mapping(plan, "target"), "mode", "target")
    transform = _plan_mapping(plan, "transform")
    if mode == "create":
        if paths["state"].is_dir() and (paths["state"] / "refs.json").is_file():
            status = _run_native_artifact(["status", "--state", str(paths["state"])])
            artifacts.setdefault("revision_id", status["head_revision_id"])
            artifacts.setdefault("base_revision", None)
            return
        quality = plan.get("quality_policy")
        arguments = [
            "init",
            str(paths["dataset"]),
            "--id-field",
            _mapping_text(transform, "id_field", "transform"),
            "--state",
            str(paths["state"]),
            "--source-receipt",
            str(artifacts["transform_receipt"]),
        ]
        if isinstance(quality, dict) and quality.get("content") not in ({}, None):
            policy_path = attempt / "staging" / "quality-policy.json"
            _write_new_json(policy_path, cast(Mapping[str, object], quality["content"]))
            arguments.extend(("--policy", str(policy_path)))
        initialized = _run_native_artifact(arguments)
        artifacts["revision_id"] = initialized["head_revision_id"]
        artifacts["base_revision"] = None
        return

    base = plan.get("base_revision")
    logged = _run_native_artifact(["log", "--state", str(paths["state"]), "--limit", "1"])
    revisions = logged.get("revisions")
    if not isinstance(revisions, list) or not revisions or not isinstance(revisions[0], dict):
        raise PipelineError("PIPELINE_RECOVERY_CORRUPT", "Workspace revision log is invalid")
    current_head = revisions[0].get("revision_id")
    if isinstance(artifacts.get("revision_id"), str) and current_head == base:
        return
    if current_head != base:
        if current_head == artifacts.get("revision_id"):
            return
        if isinstance(artifacts.get("changeset_id"), str):
            provenance = revisions[0].get("provenance")
            if isinstance(provenance, dict) and provenance.get("changeset_id") == artifacts.get(
                "changeset_id"
            ):
                artifacts["revision_id"] = current_head
                artifacts["base_revision"] = base
                _save_journal(journal_path, journal)
                return
        raise PipelineError(
            "PIPELINE_HEAD_DRIFT",
            "Workspace HEAD changed after pipeline planning",
            expected_revision=base,
            actual_revision=current_head,
        )
    if isinstance(artifacts.get("change_id"), str) and not isinstance(
        artifacts.get("changeset_id"), str
    ):
        status = {
            "head_revision_id": current_head,
            "clean": False,
            "change_id": artifacts["change_id"],
        }
    else:
        status_args = ["status", "--state", str(paths["state"])]
        if isinstance(artifacts.get("changeset_id"), str):
            status_args.extend(
                (
                    "--change",
                    str(artifacts["change_id"]),
                    "--changeset",
                    str(artifacts["changeset_id"]),
                )
            )
        status = _run_native_artifact(status_args)
    candidate = Path(str(artifacts["transformed_output"]))
    if status.get("clean") is True and candidate.read_bytes() == paths["dataset"].read_bytes():
        artifacts["revision_id"] = base
        artifacts["base_revision"] = base
        artifacts["no_op"] = True
        _save_journal(journal_path, journal)
        return
    change_id = artifacts.get("change_id") or status.get("change_id")
    if not isinstance(change_id, str):
        begin = _run_native_artifact(
            [
                "changeset-begin",
                "--state",
                str(paths["state"]),
                "--intent",
                f"Apply pipeline {_plan_text(plan, 'pipeline_id')}",
                "--task-id",
                _plan_text(plan, "pipeline_id"),
            ]
        )
        change_id = _mapping_text(begin, "change_id", "changeset begin")
    artifacts["change_id"] = change_id
    _save_journal(journal_path, journal)
    backup = attempt / "staging" / "dataset.before.jsonl"
    if not backup.exists():
        shutil.copyfile(paths["dataset"], backup)
    if candidate.read_bytes() != paths["dataset"].read_bytes():
        replacement = paths["dataset"].with_name(f".{paths['dataset'].name}.{os.getpid()}.pipeline")
        shutil.copyfile(candidate, replacement)
        os.replace(replacement, paths["dataset"])
    artifacts["dataset_replaced"] = True
    _save_journal(journal_path, journal)
    if os.environ.get("DATAJIG_PIPELINE_FAILPOINT") == "after_dataset_replace":
        raise PipelineError(
            "PIPELINE_INTERRUPTED",
            "Pipeline stopped at the requested recovery boundary",
            phase="dataset_replaced",
            retryable=True,
        )
    changeset_id = artifacts.get("changeset_id") or status.get("changeset_id")
    if not isinstance(changeset_id, str):
        staged = _run_native_artifact(
            ["changeset-stage", "--state", str(paths["state"]), "--change", change_id]
        )
        changeset_id = _mapping_text(staged, "changeset_id", "changeset stage")
    artifacts["changeset_id"] = changeset_id
    _save_journal(journal_path, journal)
    checked = _run_native_artifact(
        [
            "check",
            "--state",
            str(paths["state"]),
            "--change",
            change_id,
            "--changeset",
            changeset_id,
        ]
    )
    if checked.get("status") != "pass":
        raise PipelineError("PIPELINE_QUALITY_FAILED", "Pipeline candidate did not pass validation")
    seal_args = [
        "seal",
        "--detached",
        "--source-receipt",
        str(artifacts["transform_receipt"]),
        "--state",
        str(paths["state"]),
        "--message",
        f"Apply pipeline {_plan_text(plan, 'pipeline_id')}",
        "--change",
        change_id,
        "--changeset",
        changeset_id,
    ]
    report_id = checked.get("report_content_id")
    artifacts["report_id"] = report_id
    _save_journal(journal_path, journal)
    if isinstance(report_id, str) and (
        plan.get("quality_policy") is not None or checked.get("findings") != 0
    ):
        seal_args.extend(("--accept-report", report_id))
    sealed = _run_native_artifact(seal_args)
    artifacts.update(
        {
            "base_revision": base,
            "revision_id": sealed["revision_id"],
            "change_id": change_id,
            "changeset_id": changeset_id,
            "report_id": report_id,
        }
    )
    _save_journal(journal_path, journal)


def _commit_prepared_head(
    plan: Mapping[str, object], paths: Mapping[str, Path], artifacts: Mapping[str, object]
) -> None:
    target = _plan_mapping(plan, "target")
    mode = _mapping_text(target, "mode", "target")
    final_revision = artifacts.get("revision_id")
    if not isinstance(final_revision, str):
        raise PipelineError("PIPELINE_RECOVERY_CORRUPT", "Prepared revision identity is missing")
    if mode == "update" and not artifacts.get("no_op", False):
        base = artifacts.get("base_revision")
        if not isinstance(base, str):
            raise PipelineError("PIPELINE_RECOVERY_CORRUPT", "Base revision identity is missing")
        _run_native_artifact(
            [
                "seal",
                "--state",
                str(paths["state"]),
                "--commit-revision",
                final_revision,
                "--expected-head",
                base,
            ]
        )
        return
    current = _run_native_artifact(["log", "--state", str(paths["state"]), "--limit", "1"])
    revisions = current.get("revisions")
    if (
        not isinstance(revisions, list)
        or not revisions
        or not isinstance(revisions[0], dict)
        or revisions[0].get("revision_id") != final_revision
    ):
        raise PipelineError(
            "PIPELINE_HEAD_DRIFT", "Workspace HEAD does not match the prepared pipeline revision"
        )


def _prepare_delivery(
    plan: Mapping[str, object],
    paths: Mapping[str, Path],
    attempt: Path,
    artifacts: dict[str, object],
) -> None:
    delivery_stage = paths["delivery"].parent / (
        f".datajig-delivery-{_plan_text(plan, 'pipeline_id')}-{attempt.name}"
    )
    if artifacts.get("delivery_stage"):
        if not delivery_stage.is_dir():
            raise PipelineError(
                "PIPELINE_RECOVERY_CORRUPT", "Delivery staging directory is missing"
            )
        return
    if delivery_stage.exists() or delivery_stage.is_symlink():
        raise PipelineError("PIPELINE_RECOVERY_CORRUPT", "Unexpected delivery staging collision")
    delivery_stage.parent.mkdir(parents=True, exist_ok=True)
    delivery_stage.mkdir()
    bundle = delivery_stage / "bundle"
    export = _plan_mapping(plan, "export")
    arguments = [
        "export",
        "--state",
        str(paths["state"]),
        "--output",
        str(bundle),
        "--revision",
        str(artifacts["revision_id"]),
        "--seed",
        _mapping_text(export, "seed", "export"),
        "--max-shard-records",
        str(export["max_shard_records"]),
        "--max-shard-bytes",
        str(export["max_shard_bytes"]),
    ]
    target = _plan_mapping(plan, "target")
    if _mapping_text(target, "mode", "target") == "update" and not artifacts.get("no_op", False):
        arguments.append("--allow-detached")
    splits = export.get("split")
    assert isinstance(splits, list)
    for split in splits:
        arguments.extend(("--split", str(split)))
    exported = _run_native_artifact(arguments)
    consumption_results = _create_consumption_plans(
        plan, delivery_stage, paths["config_base"], paths["delivery"]
    )
    artifacts.update(
        {
            "delivery_stage": str(delivery_stage),
            "bundle_id": exported["bundle_id"],
            "bundle_manifest": "bundle/datajig.bundle.json",
            "consumption_plans": consumption_results,
        }
    )
    if paths["delivery"].exists() or paths["delivery"].is_symlink():
        previous = _verify_replacement_delivery(
            plan,
            paths,
            paths["delivery"],
            expected_head=plan.get("base_revision"),
        )
        backup = paths["delivery"].parent / (
            f".{paths['delivery'].name}.datajig-backup-{previous['pipeline_receipt_id']}"
        )
        if backup.exists() or backup.is_symlink():
            raise PipelineError(
                "DELIVERY_CONFLICT", "The deterministic delivery backup path already exists"
            )
        artifacts.update(
            {
                "replaced_pipeline_id": previous["pipeline_id"],
                "delivery_backup": str(backup),
            }
        )
    receipt = _build_delivery_receipt(plan, paths, artifacts)
    artifacts["pipeline_receipt"] = receipt
    _write_new_json(delivery_stage / "pipeline-receipt.json", receipt)


def _publish_delivery(
    plan: Mapping[str, object],
    paths: Mapping[str, Path],
    attempt: Path,
    artifacts: dict[str, object],
    journal_path: Path,
    journal: dict[str, object],
    phase: str,
) -> dict[str, object]:
    del attempt
    delivery_stage = Path(str(artifacts["delivery_stage"]))
    raw_receipt = artifacts.get("pipeline_receipt")
    if not isinstance(raw_receipt, dict):
        raise PipelineError("PIPELINE_RECOVERY_CORRUPT", "Pipeline receipt is missing")
    receipt = dict(raw_receipt)
    backup_value = artifacts.get("delivery_backup")
    if isinstance(backup_value, str):
        backup = Path(backup_value)
        if phase == "head_committed":
            _verify_replacement_delivery(
                plan,
                paths,
                paths["delivery"],
                expected_head=artifacts.get("revision_id"),
            )
            try:
                _exchange_directories(delivery_stage, paths["delivery"])
            except OSError as exc:
                if exc.errno in {22, 38, 45, 95}:
                    raise PipelineError(
                        "ATOMIC_EXCHANGE_UNSUPPORTED",
                        "The delivery filesystem does not support atomic directory exchange; "
                        "choose a new delivery directory or a compatible local filesystem",
                        delivery=str(paths["delivery"]),
                    ) from exc
                raise
            _advance_journal(journal_path, journal, "delivery_exchanged")
            phase = "delivery_exchanged"
            if os.environ.get("DATAJIG_PIPELINE_FAILPOINT") == "after_delivery_exchange":
                raise PipelineError(
                    "PIPELINE_INTERRUPTED",
                    "Pipeline stopped after atomic delivery exchange; recovery must finish forward",
                    phase=phase,
                    retryable=True,
                )
        if phase != "delivery_exchanged":
            raise PipelineError("PIPELINE_RECOVERY_CORRUPT", "Delivery exchange phase is invalid")
        _verify_pending_delivery(paths["delivery"], receipt)
        if delivery_stage.exists() or delivery_stage.is_symlink():
            _verify_replacement_delivery(
                plan,
                paths,
                delivery_stage,
                expected_head=artifacts.get("revision_id"),
                expected_delivery=paths["delivery"],
            )
            try:
                _rename_directory_noreplace(delivery_stage, backup)
            except FileExistsError as exc:
                raise PipelineError(
                    "PIPELINE_RECOVERY_CORRUPT", "Delivery backup appeared during recovery"
                ) from exc
        elif backup.is_dir() and not backup.is_symlink():
            _verify_replacement_delivery(
                plan,
                paths,
                backup,
                expected_head=artifacts.get("revision_id"),
                expected_delivery=paths["delivery"],
            )
        else:
            raise PipelineError(
                "PIPELINE_RECOVERY_CORRUPT", "Exchanged delivery backup is missing"
            )
    elif not paths["delivery"].exists():
        try:
            _rename_directory_noreplace(delivery_stage, paths["delivery"])
        except FileExistsError as exc:
            raise PipelineError(
                "DELIVERY_CONFLICT", "Delivery target appeared during atomic publication"
            ) from exc
        except OSError as exc:
            if exc.errno in {22, 38, 45, 95}:
                raise PipelineError(
                    "ATOMIC_PUBLISH_UNSUPPORTED",
                    "The delivery filesystem does not support atomic no-replace publication; "
                    "choose a local filesystem such as /tmp",
                    delivery=str(paths["delivery"]),
                ) from exc
            raise
        if os.environ.get("DATAJIG_PIPELINE_FAILPOINT") == "after_delivery_rename":
            raise PipelineError(
                "PIPELINE_INTERRUPTED",
                "Pipeline stopped after delivery rename but before commit marker publication",
                phase="delivery_renamed",
                retryable=True,
            )
    elif delivery_stage.exists():
        raise PipelineError("DELIVERY_CONFLICT", "Both staged and target delivery exist")
    if not paths["delivery"].is_dir() or paths["delivery"].is_symlink():
        raise PipelineError("DELIVERY_CONFLICT", "Incomplete delivery target is invalid")
    receipt_path = paths["delivery"] / "pipeline-receipt.json"
    _verify_json_file(receipt_path, receipt, "Published pipeline receipt")
    marker = paths["delivery"] / ".datajig-commit.json"
    if not marker.exists():
        _write_new_json(marker, receipt)
    if os.environ.get("DATAJIG_PIPELINE_FAILPOINT") == "after_delivery_marker":
        raise PipelineError(
            "PIPELINE_INTERRUPTED",
            "Pipeline stopped after publishing its commit marker",
            phase="delivery_marker_published",
            retryable=True,
        )
    return receipt


def _build_delivery_receipt(
    plan: Mapping[str, object], paths: Mapping[str, Path], artifacts: Mapping[str, object]
) -> dict[str, object]:
    raw_inputs = plan.get("inputs")
    source_content_ids = (
        [item["content_id"] for item in raw_inputs if isinstance(item, dict)]
        if isinstance(raw_inputs, list)
        else []
    )
    receipt_basis: dict[str, object] = {
        "namespace": "datajig",
        "pipeline_receipt_schema_version": 1,
        "pipeline_id": _plan_text(plan, "pipeline_id"),
        "status": "committed",
        "base_revision": artifacts.get("base_revision"),
        "final_revision": artifacts["revision_id"],
        "transform_plan_id": artifacts["transform_plan_id"],
        "transform_receipt_id": artifacts["transform_receipt_id"],
        "source_content_ids": source_content_ids,
        "change_id": artifacts.get("change_id"),
        "changeset_id": artifacts.get("changeset_id"),
        "report_id": artifacts.get("report_id"),
        "bundle_id": artifacts["bundle_id"],
        "bundle_manifest": artifacts["bundle_manifest"],
        "consumption_plans": artifacts["consumption_plans"],
        "delivery": str(paths["delivery"].resolve()),
        "no_op": bool(artifacts.get("no_op", False)),
    }
    if plan.get("run") is not None:
        receipt_basis["run"] = plan.get("run")
    if isinstance(artifacts.get("delivery_backup"), str):
        receipt_basis["delivery_backup"] = artifacts["delivery_backup"]
    receipt_id = _identity("piped", b"datajig-pipeline-receipt-v1", receipt_basis)
    return {**receipt_basis, "pipeline_receipt_id": receipt_id}


def _create_consumption_plans(
    plan: Mapping[str, object],
    delivery: Path,
    config_base: Path,
    published_delivery: Path,
) -> list[dict[str, object]]:
    manifest = delivery / "bundle" / "datajig.bundle.json"
    results: list[dict[str, object]] = []
    raw_consumption = plan.get("consumption_plan")
    assert isinstance(raw_consumption, list)
    for index, raw in enumerate(raw_consumption):
        assert isinstance(raw, dict)
        requested_run_dir = raw.get("run_dir")
        output = (
            _resolve_logical(config_base, requested_run_dir)
            if isinstance(requested_run_dir, str)
            else delivery / "runs" / f"{index:03d}"
        )
        plan_output = delivery / "consumption" / f"{index:03d}.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        plan_output.parent.mkdir(parents=True, exist_ok=True)
        if plan_output.exists():
            try:
                candidate = json.loads(plan_output.read_text(encoding="utf-8"))
                plan_id = candidate["consumption_plan_id"]
                if not isinstance(plan_id, str):
                    raise TypeError
            except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
                raise PipelineError(
                    "PIPELINE_RECOVERY_CORRUPT", "Partial consumption plan is invalid"
                ) from exc
            consumed = _run_native_artifact(
                ["consume-info", str(plan_output), "--verify", "--accept-plan", plan_id]
            )
        else:
            consumed = _run_native_artifact(
                [
                    "consume-plan",
                    str(manifest),
                    "--split",
                    _mapping_text(raw, "split", "consumption plan"),
                    "--consumer",
                    _mapping_text(raw, "consumer", "consumption plan"),
                    "--run-id",
                    _mapping_text(raw, "run_id", "consumption plan"),
                    "--output",
                    str(output),
                    "--plan",
                    str(plan_output),
                    "--published-manifest",
                    str(published_delivery / "bundle" / "datajig.bundle.json"),
                ]
            )
        results.append(
            {
                "consumer": raw["consumer"],
                "split": raw["split"],
                "run_id": raw["run_id"],
                "plan_id": consumed["consumption_plan_id"],
                "plan": str(plan_output.relative_to(delivery)),
            }
        )
    return results


def _committed_delivery(
    path: Path, pipeline_id: str, *, allow_other: bool = False
) -> dict[str, object] | None:
    marker = path / ".datajig-commit.json"
    if not marker.is_file():
        return None
    receipt = _read_owned_delivery(path, expected_delivery=path)
    if receipt.get("pipeline_id") != pipeline_id:
        if allow_other:
            return None
        raise PipelineError("DELIVERY_CONFLICT", "Existing delivery belongs to another pipeline")
    return receipt


def _read_owned_delivery(path: Path, *, expected_delivery: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_dir():
        raise PipelineError("DELIVERY_CONFLICT", "Existing delivery is not a direct directory")
    marker = path / ".datajig-commit.json"
    receipt_path = path / "pipeline-receipt.json"
    for candidate, label in ((marker, "marker"), (receipt_path, "receipt")):
        try:
            metadata = candidate.lstat()
        except OSError as exc:
            raise PipelineError(
                "DELIVERY_CONFLICT", f"Existing delivery {label} is missing"
            ) from exc
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise PipelineError(
                "DELIVERY_CONFLICT", f"Existing delivery {label} is not a direct regular file"
            )
    try:
        marker_payload = json.loads(marker.read_text(encoding="utf-8"))
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PipelineError("DELIVERY_CONFLICT", "Existing delivery metadata is invalid") from exc
    if not isinstance(marker_payload, dict) or not isinstance(receipt, dict):
        raise PipelineError("DELIVERY_CONFLICT", "Existing delivery metadata is invalid")
    if marker_payload != receipt:
        raise PipelineError("DELIVERY_CONFLICT", "Existing delivery marker and receipt differ")
    try:
        _verify_pipeline_receipt(receipt)
    except PipelineError as exc:
        raise PipelineError(
            "DELIVERY_CONFLICT", "Existing delivery marker failed verification"
        ) from exc
    try:
        expected_path = str(expected_delivery.resolve(strict=True))
    except OSError as exc:
        raise PipelineError(
            "DELIVERY_CONFLICT", "Existing delivery path cannot be resolved"
        ) from exc
    if receipt.get("delivery") != expected_path:
        raise PipelineError("DELIVERY_CONFLICT", "Existing delivery receipt path is not exact")
    return receipt


def _verify_replacement_delivery(
    plan: Mapping[str, object],
    paths: Mapping[str, Path],
    candidate: Path,
    *,
    expected_head: object,
    expected_delivery: Path | None = None,
) -> dict[str, object]:
    if not isinstance(expected_head, str):
        raise PipelineError("DELIVERY_CONFLICT", "Expected workspace HEAD is unavailable")
    receipt = _read_owned_delivery(
        candidate, expected_delivery=expected_delivery or paths["delivery"]
    )
    base_revision = plan.get("base_revision")
    if not isinstance(base_revision, str) or receipt.get("final_revision") != base_revision:
        raise PipelineError(
            "DELIVERY_CONFLICT",
            "Existing delivery is not the direct predecessor of this update",
        )
    status = _run_native_artifact(["status", "--state", str(paths["state"])])
    if status.get("head_revision_id") != expected_head:
        raise PipelineError(
            "PIPELINE_HEAD_DRIFT", "Workspace HEAD changed before delivery exchange"
        )
    manifest_relative = receipt.get("bundle_manifest")
    if manifest_relative != "bundle/datajig.bundle.json":
        raise PipelineError("DELIVERY_CONFLICT", "Existing delivery manifest path is invalid")
    manifest = candidate / "bundle" / "datajig.bundle.json"
    verified = _run_native_artifact(
        [
            "export-info",
            str(manifest),
            "--verify",
            "--expect-bundle",
            str(receipt.get("bundle_id")),
            "--expect-revision",
            str(receipt.get("final_revision")),
        ]
    )
    if (
        verified.get("bundle_id") != receipt.get("bundle_id")
        or verified.get("source_revision_id") != receipt.get("final_revision")
    ):
        raise PipelineError("DELIVERY_CONFLICT", "Existing delivery bundle identity is invalid")
    try:
        manifest_payload = json.loads(manifest.read_text(encoding="utf-8"))
        source = manifest_payload["source"]
        manifest_dataset_id = source["dataset_id"]
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise PipelineError(
            "DELIVERY_CONFLICT", "Existing delivery bundle source is invalid"
        ) from exc
    if manifest_dataset_id != status.get("dataset_id"):
        raise PipelineError(
            "DELIVERY_CONFLICT", "Existing delivery belongs to another dataset workspace"
        )
    return receipt


def _verify_pending_delivery(path: Path, expected: Mapping[str, object]) -> None:
    if path.is_symlink() or not path.is_dir():
        raise PipelineError("PIPELINE_RECOVERY_CORRUPT", "Exchanged delivery is not a directory")
    if (path / ".datajig-commit.json").exists():
        raise PipelineError(
            "PIPELINE_RECOVERY_CORRUPT", "Exchanged delivery marker appeared before recovery"
        )
    _verify_json_file(path / "pipeline-receipt.json", expected, "Exchanged pipeline receipt")


def _verify_json_file(path: Path, expected: Mapping[str, object], label: str) -> None:
    try:
        metadata = path.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise OSError("not a direct regular file")
        actual = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PipelineError("PIPELINE_RECOVERY_CORRUPT", f"{label} is invalid") from exc
    if actual != dict(expected):
        raise PipelineError("PIPELINE_RECOVERY_CORRUPT", f"{label} does not match its journal")


def _persist_success(state: Path, pipeline_id: str, receipt: Mapping[str, object]) -> None:
    root = state / "pipelines" / pipeline_id
    root.mkdir(parents=True, exist_ok=True)
    target = root / "receipt.json"
    if target.exists():
        existing = json.loads(target.read_text(encoding="utf-8"))
        if existing != receipt:
            raise PipelineError("PIPELINE_RECOVERY_CORRUPT", "Stored success receipt conflicts")
        return
    _write_new_json(target, receipt)


def _run_native_artifact(arguments: Sequence[str]) -> dict[str, object]:
    from datajig.native import (
        NativeBackendUnavailableError,
        TransformProviderUnavailableError,
        run_native,
    )

    try:
        completed = run_native(arguments)
    except TransformProviderUnavailableError as exc:
        raise PipelineError(exc.code, str(exc), remediation=exc.remediation) from exc
    except NativeBackendUnavailableError as exc:
        raise PipelineError("NATIVE_BACKEND_UNAVAILABLE", str(exc)) from exc
    serialized = completed.stdout if completed.returncode == 0 else completed.stderr
    try:
        payload = json.loads(serialized)
    except json.JSONDecodeError as exc:
        raise PipelineError(
            "NATIVE_PROTOCOL_ERROR", "Native command returned invalid JSON"
        ) from exc
    if completed.returncode != 0:
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            raise PipelineError(
                str(error.get("code", "PIPELINE_STEP_FAILED")),
                str(error.get("message", "Pipeline step failed")),
                step=arguments[0],
            )
        raise PipelineError("PIPELINE_STEP_FAILED", f"Pipeline step {arguments[0]} failed")
    artifact = payload.get("artifact") if isinstance(payload, dict) else None
    if not isinstance(artifact, dict):
        raise PipelineError("NATIVE_PROTOCOL_ERROR", "Native command omitted its artifact")
    return artifact


def _plan_mapping(plan: Mapping[str, object], key: str) -> dict[str, object]:
    value = plan.get(key)
    if not isinstance(value, dict):
        raise PipelineError("INVALID_PIPELINE_ARTIFACT", f"Pipeline plan field {key} is invalid")
    return value


def _plan_text(plan: Mapping[str, object], key: str) -> str:
    value = plan.get(key)
    if not isinstance(value, str):
        raise PipelineError("INVALID_PIPELINE_ARTIFACT", f"Pipeline plan field {key} is invalid")
    return value


def _mapping_text(data: Mapping[str, object], key: str, context: str) -> str:
    value = data.get(key)
    if not isinstance(value, str):
        raise PipelineError("INVALID_PIPELINE_ARTIFACT", f"{context}.{key} is invalid")
    return value
