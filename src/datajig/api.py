from __future__ import annotations

from pathlib import Path

from datajig.analyzers.changes import analyze_changes
from datajig.analyzers.distribution import (
    BUCKET_BOUNDARIES,
    compare_distributions,
    summarize_distribution,
)
from datajig.analyzers.leakage import analyze_leakage
from datajig.cache import AnalysisCache
from datajig.inventory import scan_snapshot
from datajig.layout import ImageFolderLayout
from datajig.matching import MatchResult, match_samples
from datajig.models import (
    DistributionDelta,
    Finding,
    ReviewReport,
    Severity,
)
from datajig.policy import PolicyConfig, evaluate_policy
from datajig.source import DirectorySource


def compare(
    baseline: str | Path,
    candidate: str | Path,
    *,
    layout: str = "imagefolder",
    cache: AnalysisCache | str | Path | None = None,
    policy: PolicyConfig | None = None,
    workers: int = 1,
    phash_threshold: int = 6,
) -> ReviewReport:
    if layout != "imagefolder":
        raise ValueError("layout must be 'imagefolder'")
    if workers < 1:
        raise ValueError("workers must be at least 1")
    if phash_threshold < 0:
        raise ValueError("phash_threshold must be non-negative")
    baseline_source = DirectorySource(baseline)
    candidate_source = DirectorySource(candidate)
    if isinstance(cache, str | Path):
        validate_destination(cache, baseline_source.root, candidate_source.root)
    layout_parser = ImageFolderLayout()
    analysis_cache, owns_cache = _resolve_cache(cache)
    try:
        baseline_inventory = scan_snapshot(
            baseline_source, layout_parser, analysis_cache, workers=workers
        )
        candidate_inventory = scan_snapshot(
            candidate_source, layout_parser, analysis_cache, workers=workers
        )
    finally:
        if owns_cache:
            analysis_cache.close()

    findings: list[Finding] = [
        *_root_layout_findings(baseline_source, "baseline"),
        *_root_layout_findings(candidate_source, "candidate"),
        *baseline_inventory.findings,
        *candidate_inventory.findings,
    ]
    complete = not any(
        item.code == "PROBE_INTERNAL_ERROR" for item in findings
    )
    if any(item.code == "INVALID_LAYOUT" for item in findings):
        findings.sort(key=lambda item: (item.code, item.sample_ids, item.message))
        effective_policy = policy or PolicyConfig()
        return ReviewReport(
            baseline=str(baseline_source.root),
            candidate=str(candidate_source.root),
            samples=baseline_inventory.records + candidate_inventory.records,
            findings=tuple(findings),
            matches=(),
            distributions=(),
            policy=evaluate_policy(tuple(findings), complete, effective_policy),
            complete=complete,
            metadata=(
                ("baseline_unsupported", len(baseline_inventory.unsupported_paths)),
                ("candidate_unsupported", len(candidate_inventory.unsupported_paths)),
                ("layout", layout),
                ("phash_threshold", phash_threshold),
                ("distribution_bucket_boundaries", BUCKET_BOUNDARIES),
            ),
        )

    matching_complete = True
    try:
        match_result = match_samples(
            baseline_inventory.records,
            candidate_inventory.records,
            phash_threshold,
        )
    except Exception as exc:
        matching_complete = False
        complete = False
        findings.append(_analyzer_error("matching", exc))
        match_result = MatchResult(
            baseline_inventory.records,
            candidate_inventory.records,
            (),
            baseline_inventory.records,
            candidate_inventory.records,
            phash_threshold=phash_threshold,
        )
    if matching_complete:
        try:
            findings.extend(analyze_changes(match_result))
        except Exception as exc:
            complete = False
            findings.append(_analyzer_error("changes", exc))
    try:
        findings.extend(analyze_leakage(candidate_inventory, phash_threshold))
    except Exception as exc:
        complete = False
        findings.append(_analyzer_error("leakage", exc))

    distributions: tuple[DistributionDelta, ...] = ()
    effective_policy = policy or PolicyConfig()
    try:
        before_distribution = summarize_distribution(baseline_inventory)
        after_distribution = summarize_distribution(candidate_inventory)
        distributions = compare_distributions(before_distribution, after_distribution)
        findings.extend(_distribution_findings(distributions, effective_policy))
    except Exception as exc:
        complete = False
        findings.append(_analyzer_error("distribution", exc))

    findings.sort(key=lambda item: (item.code, item.sample_ids, item.message))
    policy_result = evaluate_policy(tuple(findings), complete, effective_policy)
    return ReviewReport(
        baseline=str(baseline_source.root),
        candidate=str(candidate_source.root),
        samples=baseline_inventory.records + candidate_inventory.records,
        findings=tuple(findings),
        matches=match_result.matches,
        distributions=distributions,
        policy=policy_result,
        complete=complete,
        metadata=(
            ("baseline_unsupported", len(baseline_inventory.unsupported_paths)),
            ("candidate_unsupported", len(candidate_inventory.unsupported_paths)),
            ("layout", layout),
            ("phash_threshold", phash_threshold),
            ("distribution_bucket_boundaries", BUCKET_BOUNDARIES),
        ),
    )


def validate_destination(path: str | Path, *snapshot_roots: Path) -> Path:
    destination = Path(path).expanduser().resolve(strict=False)
    if any(destination == root or destination.is_relative_to(root) for root in snapshot_roots):
        raise ValueError(f"write destinations must stay outside dataset snapshots: {path}")
    return destination


def _resolve_cache(
    cache: AnalysisCache | str | Path | None,
) -> tuple[AnalysisCache, bool]:
    if isinstance(cache, AnalysisCache):
        return cache, False
    if cache is None:
        return AnalysisCache.disabled(), True
    return AnalysisCache.open(cache), True


def _root_layout_findings(source: DirectorySource, snapshot_name: str) -> tuple[Finding, ...]:
    has_split = any(
        path.name in ImageFolderLayout.accepted_splits
        and path.is_dir()
        and not path.is_symlink()
        for path in source.root.iterdir()
    )
    if has_split:
        return ()
    return (
        Finding(
            "INVALID_LAYOUT",
            Severity.ERROR,
            f"{snapshot_name} snapshot must contain train, val, or test",
        ),
    )


def _analyzer_error(name: str, exc: Exception) -> Finding:
    return Finding(
        "ANALYZER_INTERNAL_ERROR",
        Severity.ERROR,
        f"{name} analyzer failed: {type(exc).__name__}: {exc}",
        evidence=(("analyzer", name),),
    )


def _distribution_findings(
    deltas: tuple[DistributionDelta, ...], policy: PolicyConfig
) -> tuple[Finding, ...]:
    findings: list[Finding] = []
    media_dimensions = {"width", "height", "aspect_ratio", "format", "channels"}
    for delta in deltas:
        absolute_change = abs(delta.after_proportion - delta.before_proportion)
        if delta.dimension == "label" and absolute_change >= policy.label_delta_threshold:
            code = "LABEL_DISTRIBUTION_CHANGED"
        elif (
            delta.dimension in media_dimensions
            and absolute_change >= policy.media_delta_threshold
        ):
            code = "MEDIA_DISTRIBUTION_CHANGED"
        else:
            continue
        findings.append(
            Finding(
                code,
                Severity.WARNING,
                f"{delta.dimension} distribution changed for {delta.key}",
                evidence=(
                    ("dimension", delta.dimension),
                    ("key", delta.key),
                    ("before_proportion", delta.before_proportion),
                    ("after_proportion", delta.after_proportion),
                ),
            )
        )
    return tuple(findings)
