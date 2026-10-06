from __future__ import annotations

from collections import defaultdict

from datajig.inventory import InventoryResult
from datajig.models import Finding, SampleRecord, Severity
from datajig.phash_index import PhashIndex


def analyze_leakage(
    inventory: InventoryResult, threshold: int
) -> tuple[Finding, ...]:
    if threshold < 0:
        raise ValueError("threshold must be non-negative")
    findings: list[Finding] = []
    by_digest: dict[str, list[SampleRecord]] = defaultdict(list)
    for record in inventory.records:
        by_digest[record.content_hash].append(record)
    for digest in sorted(by_digest):
        group = sorted(by_digest[digest], key=lambda item: item.relative_path)
        if len(group) < 2:
            continue
        sample_ids = tuple(item.relative_path for item in group)
        exact_evidence = (("group_size", len(group)), ("content_hash", digest))
        findings.append(
            Finding(
                "EXACT_DUPLICATE_GROUP",
                Severity.WARNING,
                "identical image bytes appear more than once",
                sample_ids,
                exact_evidence,
            )
        )
        if len({item.split for item in group}) > 1:
            findings.append(
                Finding(
                    "CROSS_SPLIT_EXACT_LEAKAGE",
                    Severity.ERROR,
                    "identical image bytes appear across splits",
                    sample_ids,
                    exact_evidence,
                )
            )

    for group in _near_components(inventory.records, threshold):
        sample_ids = tuple(item.relative_path for item in group)
        near_evidence = (("group_size", len(group)), ("threshold", threshold))
        findings.append(
            Finding(
                "NEAR_DUPLICATE_GROUP",
                Severity.WARNING,
                "perceptually similar images form a duplicate candidate group",
                sample_ids,
                near_evidence,
            )
        )
        if len({item.split for item in group}) > 1:
            findings.append(
                Finding(
                    "CROSS_SPLIT_NEAR_LEAKAGE",
                    Severity.WARNING,
                    "perceptually similar images appear across splits",
                    sample_ids,
                    near_evidence,
                )
            )
    findings.sort(key=lambda item: (item.code, item.sample_ids))
    return tuple(findings)


def _near_components(
    records: tuple[SampleRecord, ...], threshold: int
) -> list[list[SampleRecord]]:
    records = tuple(sorted(records, key=lambda item: item.relative_path))
    groups: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    for index, record in enumerate(records):
        if record.perceptual_hash is not None:
            groups[record.perceptual_hash][record.content_hash].append(index)

    parents = list(range(len(records)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parents[right_root] = left_root

    def connect(left_group: list[int], right_group: list[int]) -> None:
        for index in left_group[1:]:
            union(left_group[0], index)
        for index in right_group[1:]:
            union(right_group[0], index)
        union(left_group[0], right_group[0])

    phash_index: PhashIndex[str] = PhashIndex()
    for hash_value in sorted(groups):
        digest_groups = groups[hash_value]
        digest_values = sorted(digest_groups)
        if len(digest_values) > 1:
            anchor = digest_groups[digest_values[0]]
            for other_digest in digest_values[1:]:
                connect(anchor, digest_groups[other_digest])
        for near_match in phash_index.query(hash_value, threshold):
            for other_hash in near_match.items:
                combined = [*digest_groups.items(), *groups[other_hash].items()]
                anchor_digest, anchor_group = combined[0]
                for other_digest, other_group in combined[1:]:
                    if anchor_digest != other_digest:
                        connect(anchor_group, other_group)
        phash_index.add(hash_value, hash_value)

    connected: dict[int, list[int]] = defaultdict(list)
    for record_index in range(len(records)):
        connected[find(record_index)].append(record_index)
    return [
        [records[index] for index in indexes]
        for _, indexes in sorted(connected.items())
        if len(indexes) > 1
    ]
