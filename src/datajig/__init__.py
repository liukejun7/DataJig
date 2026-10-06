from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datajig.api import compare
    from datajig.consumption import ConsumptionRun, ConsumptionStateError, open_consumption
    from datajig.models import (
        DistributionDelta,
        Finding,
        PolicyResult,
        ReviewReport,
        ReviewStatus,
        SampleMatch,
        SampleRecord,
        Severity,
    )
    from datajig.policy import PolicyConfig
    from datajig.training import BundleVerificationError, VerifiedBundle, open_bundle

__all__ = [
    "BundleVerificationError",
    "ConsumptionRun",
    "ConsumptionStateError",
    "DistributionDelta",
    "Finding",
    "PolicyConfig",
    "PolicyResult",
    "ReviewReport",
    "ReviewStatus",
    "SampleMatch",
    "SampleRecord",
    "Severity",
    "VerifiedBundle",
    "compare",
    "open_bundle",
    "open_consumption",
]

_MODEL_EXPORTS = frozenset(__all__) - {
    "BundleVerificationError",
    "ConsumptionRun",
    "ConsumptionStateError",
    "PolicyConfig",
    "VerifiedBundle",
    "compare",
    "open_bundle",
    "open_consumption",
}


def __getattr__(name: str) -> Any:
    if name == "compare":
        return import_module("datajig.api").compare
    if name == "PolicyConfig":
        return import_module("datajig.policy").PolicyConfig
    if name in {"BundleVerificationError", "VerifiedBundle", "open_bundle"}:
        return getattr(import_module("datajig.training"), name)
    if name in {"ConsumptionRun", "ConsumptionStateError", "open_consumption"}:
        return getattr(import_module("datajig.consumption"), name)
    if name in _MODEL_EXPORTS:
        return getattr(import_module("datajig.models"), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
