from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TypeAlias

Scalar: TypeAlias = str | int | float | bool | None
EvidenceValue: TypeAlias = Scalar | tuple[Scalar, ...]


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class ReviewStatus(StrEnum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"
    INCOMPLETE = "incomplete"


@dataclass(frozen=True, slots=True)
class Finding:
    code: str
    severity: Severity
    message: str
    sample_ids: tuple[str, ...] = ()
    evidence: tuple[tuple[str, EvidenceValue], ...] = ()


@dataclass(frozen=True, slots=True)
class SampleRecord:
    snapshot: str
    relative_path: str
    split: str
    label: str
    logical_path: str
    size: int
    mtime_ns: int
    content_hash: str
    perceptual_hash: str | None
    width: int | None
    height: int | None
    media_format: str | None
    channels: int | None
    decode_error: str | None = None


@dataclass(frozen=True, slots=True)
class SampleMatch:
    baseline_id: str
    candidate_id: str
    method: str
    distance: int | None
    confidence: str

    def __post_init__(self) -> None:
        if self.distance is not None and self.distance < 0:
            raise ValueError("distance must be non-negative")


@dataclass(frozen=True, slots=True)
class DistributionDelta:
    dimension: str
    key: str
    before_count: int
    after_count: int
    before_proportion: float
    after_proportion: float
    percentage_delta: float | None


@dataclass(frozen=True, slots=True)
class PolicyResult:
    status: ReviewStatus
    failures: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    effective_policy: tuple[tuple[str, EvidenceValue], ...] = ()


@dataclass(frozen=True, slots=True)
class ReviewReport:
    baseline: str
    candidate: str
    samples: tuple[SampleRecord, ...]
    findings: tuple[Finding, ...]
    matches: tuple[SampleMatch, ...]
    distributions: tuple[DistributionDelta, ...]
    policy: PolicyResult
    complete: bool = True
    metadata: tuple[tuple[str, EvidenceValue], ...] = ()
    schema_version: int = 1

    def to_dict(self) -> dict[str, object]:
        findings = indexed_findings(self)
        samples = sorted(self.samples, key=lambda item: item.relative_path)
        matches = sorted(
            self.matches,
            key=lambda item: (item.baseline_id, item.candidate_id, item.method),
        )
        distributions = sorted(
            self.distributions,
            key=lambda item: (item.dimension, item.key),
        )
        return {
            "namespace": "datajig",
            "schema_version": self.schema_version,
            "baseline": self.baseline,
            "candidate": self.candidate,
            "complete": self.complete,
            "metadata": dict(sorted(self.metadata)),
            "samples": [
                {
                    "snapshot": item.snapshot,
                    "relative_path": item.relative_path,
                    "split": item.split,
                    "label": item.label,
                    "logical_path": item.logical_path,
                    "size": item.size,
                    "mtime_ns": item.mtime_ns,
                    "content_hash": item.content_hash,
                    "perceptual_hash": item.perceptual_hash,
                    "width": item.width,
                    "height": item.height,
                    "media_format": item.media_format,
                    "channels": item.channels,
                    "decode_error": item.decode_error,
                }
                for item in samples
            ],
            "findings": [
                {
                    "id": finding_id,
                    "code": item.code,
                    "severity": item.severity.value,
                    "message": item.message,
                    "sample_ids": sorted(item.sample_ids),
                    "evidence": dict(sorted(item.evidence)),
                }
                for finding_id, item in findings
            ],
            "matches": [
                {
                    "baseline_id": item.baseline_id,
                    "candidate_id": item.candidate_id,
                    "method": item.method,
                    "distance": item.distance,
                    "confidence": item.confidence,
                }
                for item in matches
            ],
            "distributions": [
                {
                    "dimension": item.dimension,
                    "key": item.key,
                    "before_count": item.before_count,
                    "after_count": item.after_count,
                    "before_proportion": item.before_proportion,
                    "after_proportion": item.after_proportion,
                    "percentage_delta": item.percentage_delta,
                }
                for item in distributions
            ],
            "policy": {
                "status": self.policy.status.value,
                "failures": sorted(self.policy.failures),
                "warnings": sorted(self.policy.warnings),
                "effective_policy": dict(sorted(self.policy.effective_policy)),
            },
        }

    def show(self) -> str:
        from datajig.reporting.html import render_html

        return render_html(self)

    def save_html(self, path: str | Path) -> None:
        from datajig.reporting.html import save_html

        save_html(self, path)

    def save_json(self, path: str | Path) -> None:
        from datajig.reporting.json import report_to_json

        Path(path).write_text(report_to_json(self), encoding="utf-8")


def indexed_findings(report: ReviewReport) -> tuple[tuple[str, Finding], ...]:
    """Return findings in canonical order with deterministic, unique identifiers."""
    severity_rank = {Severity.ERROR: 0, Severity.WARNING: 1, Severity.INFO: 2}
    findings = sorted(
        report.findings,
        key=lambda item: (
            severity_rank[item.severity],
            item.code,
            tuple(sorted(item.sample_ids)),
            item.message,
            _finding_fingerprint(item),
        ),
    )
    occurrences: dict[str, int] = {}
    indexed: list[tuple[str, Finding]] = []
    for finding in findings:
        fingerprint = _finding_fingerprint(finding)
        occurrence = occurrences.get(fingerprint, 0) + 1
        occurrences[fingerprint] = occurrence
        indexed.append((f"fnd_{fingerprint[:20]}_{occurrence:04d}", finding))
    return tuple(indexed)


def _finding_fingerprint(finding: Finding) -> str:
    canonical = {
        "code": finding.code,
        "severity": finding.severity.value,
        "message": finding.message,
        "sample_ids": sorted(finding.sample_ids),
        "evidence": dict(sorted(finding.evidence)),
    }
    payload = json.dumps(
        canonical,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(b"datajig-finding-v1\0" + payload).hexdigest()
