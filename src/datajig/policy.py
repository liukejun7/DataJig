from __future__ import annotations

from dataclasses import dataclass

from datajig.models import Finding, PolicyResult, ReviewStatus, Severity

DEFAULT_SEVERITIES: tuple[tuple[str, Severity], ...] = (
    ("AMBIGUOUS_MATCH", Severity.WARNING),
    ("CROSS_SPLIT_EXACT_LEAKAGE", Severity.ERROR),
    ("CROSS_SPLIT_NEAR_LEAKAGE", Severity.WARNING),
    ("IMAGE_DECODE_FAILED", Severity.ERROR),
    ("INVALID_LAYOUT", Severity.ERROR),
    ("LABEL_DISTRIBUTION_CHANGED", Severity.WARNING),
    ("MEDIA_DISTRIBUTION_CHANGED", Severity.WARNING),
    ("NEAR_DUPLICATE_GROUP", Severity.WARNING),
)


@dataclass(frozen=True, slots=True)
class PolicyConfig:
    overrides: tuple[tuple[str, Severity], ...] = ()
    label_delta_threshold: float = 0.1
    media_delta_threshold: float = 0.1

    def __post_init__(self) -> None:
        for name, value in (
            ("label_delta_threshold", self.label_delta_threshold),
            ("media_delta_threshold", self.media_delta_threshold),
        ):
            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be between 0 and 1")

    def effective_severities(self) -> tuple[tuple[str, Severity], ...]:
        values = dict(DEFAULT_SEVERITIES)
        values.update(self.overrides)
        return tuple(sorted(values.items()))


def evaluate_policy(
    findings: tuple[Finding, ...],
    complete: bool,
    config: PolicyConfig,
) -> PolicyResult:
    configured = dict(config.effective_severities())
    failures: set[str] = set()
    warnings: set[str] = set()
    for finding in findings:
        severity = configured.get(finding.code, finding.severity)
        if severity is Severity.ERROR:
            failures.add(finding.code)
        elif severity is Severity.WARNING:
            warnings.add(finding.code)
    if not complete:
        status = ReviewStatus.INCOMPLETE
    elif failures:
        status = ReviewStatus.FAIL
    elif warnings:
        status = ReviewStatus.WARN
    else:
        status = ReviewStatus.PASS
    effective = (
        *((code, severity.value) for code, severity in sorted(configured.items())),
        ("label_delta_threshold", config.label_delta_threshold),
        ("media_delta_threshold", config.media_delta_threshold),
    )
    return PolicyResult(status, tuple(sorted(failures)), tuple(sorted(warnings)), effective)
