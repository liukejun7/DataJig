from __future__ import annotations

import json
import math
from typing import cast

from datajig.models import (
    DistributionDelta,
    EvidenceValue,
    Finding,
    PolicyResult,
    ReviewReport,
    ReviewStatus,
    SampleMatch,
    SampleRecord,
    Severity,
)


class JsonSchemaError(ValueError):
    """Raised when a JSON report does not match the supported schema."""


def report_to_json(report: ReviewReport, indent: int | None = 2) -> str:
    return json.dumps(
        report.to_dict(),
        indent=indent,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":") if indent is None else None,
    )


def report_from_json(payload: str) -> ReviewReport:
    try:
        decoded: object = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise JsonSchemaError(f"invalid JSON: {exc.msg}") from exc
    except RecursionError as exc:
        raise JsonSchemaError("invalid JSON: nesting is too deep") from exc
    try:
        return _decode_report(decoded)
    except JsonSchemaError:
        raise
    except (OverflowError, TypeError, ValueError, RecursionError) as exc:
        raise JsonSchemaError(
            f"invalid report value ({type(exc).__name__})"
        ) from exc


def _decode_report(decoded: object) -> ReviewReport:
    root = _mapping(decoded, "report")
    if root.get("namespace") != "datajig":
        raise JsonSchemaError("unsupported report namespace")
    schema_version = _integer(root.get("schema_version"), "schema_version")
    if schema_version != 1:
        raise JsonSchemaError(f"unsupported schema version {schema_version}")
    samples = tuple(_sample(_mapping(item, "sample")) for item in _sequence(root.get("samples")))
    findings = tuple(
        _finding(_mapping(item, "finding")) for item in _sequence(root.get("findings"))
    )
    matches = tuple(_match(_mapping(item, "match")) for item in _sequence(root.get("matches")))
    distributions = tuple(
        _distribution(_mapping(item, "distribution"))
        for item in _sequence(root.get("distributions"))
    )
    policy_data = _mapping(root.get("policy"), "policy")
    effective = _mapping(policy_data.get("effective_policy"), "effective_policy")
    policy = PolicyResult(
        ReviewStatus(_string(policy_data.get("status"), "policy.status")),
        _string_tuple(policy_data.get("failures"), "policy.failures"),
        _string_tuple(policy_data.get("warnings"), "policy.warnings"),
        tuple(sorted((key, _evidence_value(value)) for key, value in effective.items())),
    )
    metadata = _mapping(root.get("metadata"), "metadata")
    return ReviewReport(
        baseline=_string(root.get("baseline"), "baseline"),
        candidate=_string(root.get("candidate"), "candidate"),
        samples=samples,
        findings=findings,
        matches=matches,
        distributions=distributions,
        policy=policy,
        complete=_boolean(root.get("complete"), "complete"),
        metadata=tuple(sorted((key, _evidence_value(value)) for key, value in metadata.items())),
        schema_version=schema_version,
    )


def _sample(value: dict[str, object]) -> SampleRecord:
    return SampleRecord(
        snapshot=_string(value.get("snapshot"), "sample.snapshot"),
        relative_path=_string(value.get("relative_path"), "sample.relative_path"),
        split=_string(value.get("split"), "sample.split"),
        label=_string(value.get("label"), "sample.label"),
        logical_path=_string(value.get("logical_path"), "sample.logical_path"),
        size=_integer(value.get("size"), "sample.size"),
        mtime_ns=_integer(value.get("mtime_ns"), "sample.mtime_ns"),
        content_hash=_string(value.get("content_hash"), "sample.content_hash"),
        perceptual_hash=_optional_string(value.get("perceptual_hash"), "sample.perceptual_hash"),
        width=_optional_integer(value.get("width"), "sample.width"),
        height=_optional_integer(value.get("height"), "sample.height"),
        media_format=_optional_string(value.get("media_format"), "sample.media_format"),
        channels=_optional_integer(value.get("channels"), "sample.channels"),
        decode_error=_optional_string(value.get("decode_error"), "sample.decode_error"),
    )


def _finding(value: dict[str, object]) -> Finding:
    evidence = _mapping(value.get("evidence"), "finding.evidence")
    return Finding(
        _string(value.get("code"), "finding.code"),
        Severity(_string(value.get("severity"), "finding.severity")),
        _string(value.get("message"), "finding.message"),
        _string_tuple(value.get("sample_ids"), "finding.sample_ids"),
        tuple(sorted((key, _evidence_value(item)) for key, item in evidence.items())),
    )


def _match(value: dict[str, object]) -> SampleMatch:
    return SampleMatch(
        _string(value.get("baseline_id"), "match.baseline_id"),
        _string(value.get("candidate_id"), "match.candidate_id"),
        _string(value.get("method"), "match.method"),
        _optional_integer(value.get("distance"), "match.distance"),
        _string(value.get("confidence"), "match.confidence"),
    )


def _distribution(value: dict[str, object]) -> DistributionDelta:
    return DistributionDelta(
        _string(value.get("dimension"), "distribution.dimension"),
        _string(value.get("key"), "distribution.key"),
        _integer(value.get("before_count"), "distribution.before_count"),
        _integer(value.get("after_count"), "distribution.after_count"),
        _number(value.get("before_proportion"), "distribution.before_proportion"),
        _number(value.get("after_proportion"), "distribution.after_proportion"),
        _optional_number(value.get("percentage_delta"), "distribution.percentage_delta"),
    )


def _mapping(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise JsonSchemaError(f"{name} must be an object")
    return cast(dict[str, object], value)


def _sequence(value: object) -> list[object]:
    if not isinstance(value, list):
        raise JsonSchemaError("expected an array")
    return cast(list[object], value)


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise JsonSchemaError(f"{name} must be a string")
    return value


def _optional_string(value: object, name: str) -> str | None:
    return None if value is None else _string(value, name)


def _integer(value: object, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise JsonSchemaError(f"{name} must be an integer")
    return value


def _optional_integer(value: object, name: str) -> int | None:
    return None if value is None else _integer(value, name)


def _number(value: object, name: str) -> float:
    if not isinstance(value, int | float) or isinstance(value, bool):
        raise JsonSchemaError(f"{name} must be a number")
    try:
        result = float(value)
    except OverflowError as exc:
        raise JsonSchemaError(f"{name} must be finite") from exc
    if not math.isfinite(result):
        raise JsonSchemaError(f"{name} must be finite")
    return result


def _optional_number(value: object, name: str) -> float | None:
    return None if value is None else _number(value, name)


def _boolean(value: object, name: str) -> bool:
    if not isinstance(value, bool):
        raise JsonSchemaError(f"{name} must be a boolean")
    return value


def _string_tuple(value: object, name: str) -> tuple[str, ...]:
    return tuple(_string(item, name) for item in _sequence(value))


def _evidence_value(value: object) -> EvidenceValue:
    if isinstance(value, float) and not math.isfinite(value):
        raise JsonSchemaError("evidence numbers must be finite")
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, list):
        result = tuple(_evidence_value(item) for item in value)
        if any(isinstance(item, tuple) for item in result):
            raise JsonSchemaError("nested evidence arrays are not supported")
        return cast(tuple[str | int | float | bool | None, ...], result)
    raise JsonSchemaError("evidence values must be scalar or scalar arrays")
