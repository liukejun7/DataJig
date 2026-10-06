from __future__ import annotations

import json
from collections import Counter
from importlib.metadata import PackageNotFoundError, version

from datajig.models import Finding, ReviewReport, Severity, indexed_findings

AGENT_API_VERSION = 1
DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200
MAX_COMPACT_FINDINGS = 20
MAX_COMPACT_CODES = 50
MAX_COMPACT_SAMPLE_IDS = 5
MAX_COMPACT_EVIDENCE_ITEMS = 8
MAX_COMPACT_SEQUENCE_ITEMS = 5
MAX_COMPACT_TEXT_CHARS = 240
MAX_COMPACT_JSON_CHARS = 50_000


class FindingNotFoundError(LookupError):
    """Raised when a report does not contain a requested finding ID."""


def capabilities() -> dict[str, object]:
    return {
        "agent_api_version": AGENT_API_VERSION,
        "kind": "capabilities",
        "tool": {"name": "datajig", "version": _tool_version()},
        "report_schema_versions": [1],
        "identity_namespace": "datajig-v1",
        "commands": [
            "capabilities",
            "compare",
            "explain",
            "finding",
            "findings",
            "snapshot",
            "snapshot-diff",
            "snapshot-info",
        ],
        "limits": {
            "default_page_size": DEFAULT_PAGE_SIZE,
            "max_page_size": MAX_PAGE_SIZE,
            "default_compact_findings": 10,
            "max_compact_findings": MAX_COMPACT_FINDINGS,
            "max_compact_characters": MAX_COMPACT_JSON_CHARS,
        },
        "features": {
            "compact_output": True,
            "deterministic_finding_ids": True,
            "filtered_pagination": True,
            "native_manifest_backend": True,
            "native_manifest_fallback": False,
            "read_only_queries": True,
            "snapshot_manifests": True,
        },
    }


def list_findings(
    report: ReviewReport,
    *,
    severities: tuple[Severity, ...] = (),
    codes: tuple[str, ...] = (),
    offset: int = 0,
    limit: int = DEFAULT_PAGE_SIZE,
) -> dict[str, object]:
    _validate_page(offset, limit)
    severity_filter = set(severities)
    code_filter = set(codes)
    filtered = [
        (finding_id, finding)
        for finding_id, finding in indexed_findings(report)
        if (not severity_filter or finding.severity in severity_filter)
        and (not code_filter or finding.code in code_filter)
    ]
    page = filtered[offset : offset + limit]
    return {
        "agent_api_version": AGENT_API_VERSION,
        "kind": "finding_page",
        "report": _report_descriptor(report),
        "filters": {
            "severities": sorted(item.value for item in severity_filter),
            "codes": sorted(code_filter),
        },
        "page": {
            "offset": offset,
            "limit": limit,
            "returned": len(page),
            "total": len(filtered),
            "has_more": offset + len(page) < len(filtered),
        },
        "findings": [
            _finding_payload(report, finding_id, finding)
            for finding_id, finding in page
        ],
    }


def get_finding(report: ReviewReport, finding_id: str) -> dict[str, object]:
    for current_id, finding in indexed_findings(report):
        if current_id == finding_id:
            return {
                "agent_api_version": AGENT_API_VERSION,
                "kind": "finding",
                "report": _report_descriptor(report),
                "finding": _finding_payload(report, current_id, finding),
            }
    raise FindingNotFoundError(f"finding {finding_id!r} was not found")


def compact_summary(report: ReviewReport, *, limit: int = 10) -> dict[str, object]:
    if limit < 0 or limit > MAX_COMPACT_FINDINGS:
        raise ValueError(f"limit must be between 0 and {MAX_COMPACT_FINDINGS}")
    indexed = indexed_findings(report)
    severity_counts = Counter(finding.severity.value for _, finding in indexed)
    code_counts = Counter(finding.code for _, finding in indexed)
    selected = indexed[:limit]
    compact_codes = _compact_code_counts(code_counts)
    compact_findings = [
        _compact_finding_payload(report, finding_id, finding)
        for finding_id, finding in selected
    ]
    result: dict[str, object] = {
        "agent_api_version": AGENT_API_VERSION,
        "kind": "compact_summary",
        "report": _compact_report_descriptor(report),
        "counts": {
            "total": len(indexed),
            "by_severity": {
                severity.value: severity_counts.get(severity.value, 0)
                for severity in sorted(Severity, key=lambda item: item.value)
            },
            "by_code": compact_codes,
        },
        "code_kinds_total": len(code_counts),
        "code_kinds_truncated": len(code_counts) > len(compact_codes),
        "findings": compact_findings,
        "truncated": len(indexed) > len(compact_findings),
        "budget_truncated": False,
    }
    while compact_findings and _json_size(result) > MAX_COMPACT_JSON_CHARS:
        compact_findings.pop()
        result["truncated"] = True
        result["budget_truncated"] = True
    return result


def _finding_payload(
    report: ReviewReport,
    finding_id: str,
    finding: Finding,
) -> dict[str, object]:
    configured = dict(report.policy.effective_policy).get(
        finding.code, finding.severity.value
    )
    effective_severity = (
        configured if isinstance(configured, str) else finding.severity.value
    )
    return {
        "id": finding_id,
        "code": finding.code,
        "severity": finding.severity.value,
        "effective_severity": effective_severity,
        "message": finding.message,
        "sample_ids": sorted(finding.sample_ids),
        "evidence": dict(sorted(finding.evidence)),
    }


def _report_descriptor(report: ReviewReport) -> dict[str, object]:
    return {
        "schema_version": report.schema_version,
        "baseline": report.baseline,
        "candidate": report.candidate,
        "complete": report.complete,
        "status": report.policy.status.value,
    }


def _compact_report_descriptor(report: ReviewReport) -> dict[str, object]:
    descriptor = _report_descriptor(report)
    descriptor["baseline"] = _compact_text(report.baseline)
    descriptor["candidate"] = _compact_text(report.candidate)
    return descriptor


def _compact_finding_payload(
    report: ReviewReport,
    finding_id: str,
    finding: Finding,
) -> dict[str, object]:
    configured = dict(report.policy.effective_policy).get(
        finding.code, finding.severity.value
    )
    effective_severity = (
        configured if isinstance(configured, str) else finding.severity.value
    )
    sample_ids = sorted(finding.sample_ids)
    evidence_items = sorted(finding.evidence)[:MAX_COMPACT_EVIDENCE_ITEMS]
    return {
        "id": finding_id,
        "code": _compact_text(finding.code),
        "severity": finding.severity.value,
        "effective_severity": effective_severity,
        "message": _compact_text(finding.message),
        "message_truncated": len(finding.message) > MAX_COMPACT_TEXT_CHARS,
        "sample_ids": [
            _compact_text(item) for item in sample_ids[:MAX_COMPACT_SAMPLE_IDS]
        ],
        "sample_ids_total": len(sample_ids),
        "sample_ids_truncated": len(sample_ids) > MAX_COMPACT_SAMPLE_IDS,
        "evidence": {
            _compact_text(key): _compact_evidence_value(value)
            for key, value in evidence_items
        },
        "evidence_total": len(finding.evidence),
        "evidence_truncated": len(finding.evidence) > len(evidence_items),
    }


def _compact_code_counts(counts: Counter[str]) -> dict[str, int]:
    compact: dict[str, int] = {}
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    for code, count in ordered[:MAX_COMPACT_CODES]:
        key = _compact_text(code)
        compact[key] = compact.get(key, 0) + count
    return dict(sorted(compact.items()))


def _compact_evidence_value(value: object) -> object:
    if isinstance(value, tuple):
        return {
            "items": [
                _compact_evidence_value(item)
                for item in value[:MAX_COMPACT_SEQUENCE_ITEMS]
            ],
            "total": len(value),
            "truncated": len(value) > MAX_COMPACT_SEQUENCE_ITEMS,
        }
    if isinstance(value, str):
        return _compact_text(value)
    return value


def _compact_text(value: str) -> str:
    if len(value) <= MAX_COMPACT_TEXT_CHARS:
        return value
    return f"{value[: MAX_COMPACT_TEXT_CHARS - 1]}…"


def _json_size(value: object) -> int:
    return len(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    )


def _validate_page(offset: int, limit: int) -> None:
    if offset < 0:
        raise ValueError("offset must be non-negative")
    if limit < 1 or limit > MAX_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_PAGE_SIZE}")


def _tool_version() -> str:
    try:
        return version("datajig")
    except PackageNotFoundError:
        return "0+unknown"
