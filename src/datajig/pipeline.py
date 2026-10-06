from __future__ import annotations

import json
import math
import os
import re
import stat
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import NoReturn

import blake3
import yaml
from yaml.constructor import ConstructorError
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode
from yaml.tokens import AliasToken, AnchorToken, TagToken

from datajig.hashing import compute_blake3
from datajig.providers.duckdb import _probe

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
        _fail(f"{context} contains unknown field(s): {', '.join(unknown)}")
    return value


def _required_text(data: Mapping[str, object], key: str, context: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        _fail(f"{context}.{key} must be a non-empty string")
    if any(ord(character) < 32 and character not in "\n\r\t" for character in value):
        _fail(f"{context}.{key} contains control characters")
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
    config_path = config_path.expanduser().resolve(strict=True)
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
        _validate_scalar(value, f"transform.params[{index}]") for index, value in enumerate(params)
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
    if not isinstance(raw_consumption, list) or not raw_consumption:
        _fail("consumption_plan must contain at least one consumer binding")
    split_names = {item.split("=", 1)[0] for item in normalized_splits}
    consumption: list[dict[str, str]] = []
    for index, raw_consumer in enumerate(raw_consumption):
        consumer = _object(
            raw_consumer, frozenset({"consumer", "split", "run_id"}), f"consumption_plan[{index}]"
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
        consumption.append({"consumer": consumer_name, "split": split, "run_id": run_id})
    consumption.sort(key=lambda item: (item["split"], item["consumer"], item["run_id"]))

    base_revision = _base_revision(config_path.parent, target_normalized)
    provider_identity = _probe()
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
    except FileExistsError as exc:
        raise PipelineError("OUTPUT_EXISTS", f"Output already exists: {path}") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary is not None:
            temporary.unlink(missing_ok=True)


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
    return _identity("pipe", b"datajig-pipeline-plan-v1", authorization)


def envelope(artifact: Mapping[str, object], next_actions: Sequence[str] = ()) -> dict[str, object]:
    return {
        "agent_api_version": 1,
        "artifact": dict(artifact),
        "next_actions": list(next_actions),
    }
