from __future__ import annotations

from datajig.matching import MatchResult
from datajig.models import Finding, Severity


def analyze_changes(result: MatchResult) -> tuple[Finding, ...]:
    before_by_path = {item.relative_path: item for item in result.baseline}
    after_by_path = {item.relative_path: item for item in result.candidate}
    findings: list[Finding] = list(result.findings)

    for match in result.matches:
        if match.confidence == "ambiguous":
            continue
        before = before_by_path[match.baseline_id]
        after = after_by_path[match.candidate_id]
        sample_ids = (before.relative_path, after.relative_path)
        if before.split != after.split:
            findings.append(_finding("SPLIT_CHANGED", "sample changed split", sample_ids))
        if before.label != after.label:
            findings.append(_finding("LABEL_CHANGED", "sample changed label", sample_ids))
        if before.logical_path != after.logical_path:
            findings.append(_finding("SAMPLE_MOVED", "sample path changed", sample_ids))
        if before.content_hash != after.content_hash:
            code = (
                "PROBABLE_REENCODE"
                if match.distance is not None and match.distance <= result.phash_threshold
                else "CONTENT_CHANGED"
            )
            message = (
                "sample bytes changed but perceptual content is similar"
                if code == "PROBABLE_REENCODE"
                else "sample content changed"
            )
            findings.append(_finding(code, message, sample_ids, distance=match.distance))

    for item in result.unmatched_candidate:
        findings.append(
            _finding("SAMPLE_ADDED", "sample was added", (item.relative_path,))
        )
    for item in result.unmatched_baseline:
        findings.append(
            _finding("SAMPLE_REMOVED", "sample was removed", (item.relative_path,))
        )
    findings.sort(key=lambda item: (item.code, item.sample_ids))
    return tuple(findings)


def _finding(
    code: str,
    message: str,
    sample_ids: tuple[str, ...],
    *,
    distance: int | None = None,
) -> Finding:
    evidence = () if distance is None else (("perceptual_distance", distance),)
    return Finding(code, Severity.INFO, message, sample_ids, evidence)
