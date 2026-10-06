from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

from datajig.inventory import InventoryResult
from datajig.models import DistributionDelta, SampleRecord

SIZE_BUCKET_BOUNDARIES = (256, 512, 1024)
ASPECT_RATIO_BOUNDARIES = (0.8, 1.25)
BUCKET_BOUNDARIES = (
    "size: <256, 256-511, 512-1023, >=1024, unknown",
    "aspect_ratio: portrait <0.8, square 0.8-1.25, landscape >1.25, unknown",
)


@dataclass(frozen=True, slots=True)
class DistributionBucket:
    dimension: str
    key: str
    count: int
    proportion: float


@dataclass(frozen=True, slots=True)
class DistributionSummary:
    total: int
    buckets: tuple[DistributionBucket, ...]
    bucket_boundaries: tuple[str, ...] = BUCKET_BOUNDARIES


def summarize_distribution(inventory: InventoryResult) -> DistributionSummary:
    total = len(inventory.records)
    counts: Counter[tuple[str, str]] = Counter()
    for record in inventory.records:
        for dimension, key in _dimensions(record):
            counts[(dimension, key)] += 1
    buckets = tuple(
        DistributionBucket(
            dimension,
            key,
            count,
            count / total if total else 0.0,
        )
        for (dimension, key), count in sorted(counts.items())
    )
    return DistributionSummary(total, buckets)


def compare_distributions(
    before: DistributionSummary, after: DistributionSummary
) -> tuple[DistributionDelta, ...]:
    before_map = {(item.dimension, item.key): item for item in before.buckets}
    after_map = {(item.dimension, item.key): item for item in after.buckets}
    deltas: list[DistributionDelta] = []
    for dimension, key in sorted(before_map.keys() | after_map.keys()):
        before_item = before_map.get((dimension, key))
        after_item = after_map.get((dimension, key))
        before_count = before_item.count if before_item else 0
        after_count = after_item.count if after_item else 0
        before_proportion = before_item.proportion if before_item else 0.0
        after_proportion = after_item.proportion if after_item else 0.0
        percentage_delta = (
            ((after_proportion - before_proportion) / before_proportion) * 100
            if before_proportion
            else None
        )
        deltas.append(
            DistributionDelta(
                dimension,
                key,
                before_count,
                after_count,
                before_proportion,
                after_proportion,
                percentage_delta,
            )
        )
    return tuple(deltas)


def _dimensions(record: SampleRecord) -> tuple[tuple[str, str], ...]:
    return (
        ("split", record.split),
        ("label", record.label),
        ("width", _size_bucket(record.width)),
        ("height", _size_bucket(record.height)),
        ("aspect_ratio", _aspect_bucket(record.width, record.height)),
        ("format", record.media_format or "unknown"),
        ("channels", str(record.channels) if record.channels is not None else "unknown"),
    )


def _size_bucket(value: int | None) -> str:
    if value is None:
        return "unknown"
    if value < SIZE_BUCKET_BOUNDARIES[0]:
        return "<256"
    if value < SIZE_BUCKET_BOUNDARIES[1]:
        return "256-511"
    if value < SIZE_BUCKET_BOUNDARIES[2]:
        return "512-1023"
    return ">=1024"


def _aspect_bucket(width: int | None, height: int | None) -> str:
    if width is None or height is None or height == 0:
        return "unknown"
    ratio = width / height
    if ratio < ASPECT_RATIO_BOUNDARIES[0]:
        return "portrait"
    if ratio <= ASPECT_RATIO_BOUNDARIES[1]:
        return "square"
    return "landscape"
