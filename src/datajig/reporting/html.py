from __future__ import annotations

import base64
from io import BytesIO
from pathlib import Path
from typing import Any, cast

from jinja2 import Environment, FileSystemLoader
from PIL import Image

from datajig.models import Finding, ReviewReport, SampleRecord, Severity

_TEMPLATE_DIRECTORY = Path(__file__).parent / "templates"
_ENVIRONMENT = Environment(
    loader=FileSystemLoader(_TEMPLATE_DIRECTORY),
    autoescape=True,
)


def render_html(
    report: ReviewReport,
    *,
    embed_thumbnails: bool = True,
    max_bytes: int = 10_000_000,
) -> str:
    if max_bytes < 0:
        raise ValueError("max_bytes must be non-negative")
    template = _ENVIRONMENT.get_template("review.html.j2")
    view = _view_model(report)
    thumbnails: dict[str, str] = {}
    html = template.render(**view, thumbnails=thumbnails)
    if not embed_thumbnails:
        return html
    thumbnail_samples = cast(dict[str, SampleRecord], view["thumbnail_samples"])
    for sample_id, sample in thumbnail_samples.items():
        thumbnail = _thumbnail_data_uri(sample)
        if thumbnail is None:
            continue
        trial = dict(thumbnails)
        trial[sample_id] = thumbnail
        trial_html = template.render(**view, thumbnails=trial)
        if len(trial_html.encode("utf-8")) > max_bytes:
            continue
        thumbnails = trial
        html = trial_html
    return html


def save_html(
    report: ReviewReport,
    path: str | Path,
    **options: Any,
) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_html(report, **options), encoding="utf-8")


def _view_model(report: ReviewReport) -> dict[str, object]:
    effective_values = dict(report.policy.effective_policy)

    def effective_severity(finding: Finding) -> Severity:
        value = effective_values.get(finding.code, finding.severity.value)
        return Severity(value) if isinstance(value, str) else finding.severity

    sorted_findings = sorted(
        report.findings,
        key=lambda item: (
            {Severity.ERROR: 0, Severity.WARNING: 1, Severity.INFO: 2}[
                effective_severity(item)
            ],
            item.code,
            item.sample_ids,
        ),
    )
    rendered_findings = [
        {
            "code": item.code,
            "message": item.message,
            "sample_ids": item.sample_ids,
            "severity": effective_severity(item).value,
        }
        for item in sorted_findings
    ]
    headline = {
        "added": sum(item.code == "SAMPLE_ADDED" for item in report.findings),
        "removed": sum(item.code == "SAMPLE_REMOVED" for item in report.findings),
        "leakage": sum("LEAKAGE" in item.code for item in report.findings),
        "exact_matches": sum(item.confidence == "exact" for item in report.matches),
        "probable_matches": sum(item.confidence == "probable" for item in report.matches),
    }
    representative_ids: list[str] = []
    for finding in sorted_findings:
        for sample_id in finding.sample_ids:
            if sample_id not in representative_ids:
                representative_ids.append(sample_id)
            if len(representative_ids) >= 12:
                break
        if len(representative_ids) >= 12:
            break
    baseline_samples = {
        item.relative_path: item for item in report.samples if item.snapshot == report.baseline
    }
    candidate_samples = {
        item.relative_path: item for item in report.samples if item.snapshot == report.candidate
    }
    samples = {**baseline_samples, **candidate_samples}
    representatives = [samples[item] for item in representative_ids if item in samples]
    representative_pairs: list[dict[str, object]] = []
    thumbnail_samples: dict[str, SampleRecord] = {
        item.relative_path: item for item in representatives
    }
    for match in report.matches:
        before = baseline_samples.get(match.baseline_id)
        after = candidate_samples.get(match.candidate_id)
        if before is None or after is None:
            continue
        before_key = f"before:{match.baseline_id}"
        after_key = f"after:{match.candidate_id}"
        thumbnail_samples[before_key] = before
        thumbnail_samples[after_key] = after
        representative_pairs.append(
            {
                "before": before,
                "after": after,
                "before_key": before_key,
                "after_key": after_key,
                "method": match.method,
                "confidence": match.confidence,
            }
        )
        if len(representative_pairs) >= 12:
            break
    return {
        "status": report.policy.status.value.upper(),
        "status_class": report.policy.status.value,
        "complete": report.complete,
        "baseline_name": Path(report.baseline).name or "baseline",
        "candidate_name": Path(report.candidate).name or "candidate",
        "headline": headline,
        "policy_failures": report.policy.failures,
        "policy_warnings": report.policy.warnings,
        "risk_findings": [item for item in rendered_findings if item["severity"] != "info"],
        "findings": rendered_findings,
        "distributions": sorted(
            report.distributions, key=lambda item: (item.dimension, item.key)
        ),
        "representatives": representatives,
        "representative_ids": representative_ids,
        "representative_pairs": representative_pairs,
        "thumbnail_samples": thumbnail_samples,
    }


def _thumbnail_data_uri(sample: SampleRecord) -> str | None:
    root = Path(sample.snapshot).expanduser().resolve()
    path = root.joinpath(*Path(sample.relative_path).parts)
    try:
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root) or not resolved.is_file():
            return None
        with Image.open(resolved) as image:
            image.load()
            image.thumbnail((240, 180), Image.Resampling.LANCZOS)
            converted = image.convert("RGB")
            stream = BytesIO()
            converted.save(stream, format="JPEG", quality=75, optimize=True)
    except (OSError, ValueError):
        return None
    encoded = base64.b64encode(stream.getvalue()).decode("ascii")
    return f"data:image/jpeg;base64,{encoded}"
