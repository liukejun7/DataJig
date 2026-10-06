from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

import numpy as np
from scipy.optimize import linear_sum_assignment  # type: ignore[import-untyped]

from datajig.models import Finding, SampleMatch, SampleRecord, Severity
from datajig.phash_index import PhashIndex


@dataclass(frozen=True, slots=True)
class MatchResult:
    baseline: tuple[SampleRecord, ...]
    candidate: tuple[SampleRecord, ...]
    matches: tuple[SampleMatch, ...]
    unmatched_baseline: tuple[SampleRecord, ...]
    unmatched_candidate: tuple[SampleRecord, ...]
    findings: tuple[Finding, ...] = ()
    phash_threshold: int = 6


def perceptual_distance(left: str | None, right: str | None) -> int | None:
    if left is None or right is None or len(left) != len(right):
        return None
    try:
        return (int(left, 16) ^ int(right, 16)).bit_count()
    except ValueError:
        return None


def match_samples(
    baseline: tuple[SampleRecord, ...],
    candidate: tuple[SampleRecord, ...],
    phash_threshold: int,
) -> MatchResult:
    if phash_threshold < 0:
        raise ValueError("phash_threshold must be non-negative")
    baseline_sorted = tuple(sorted(baseline, key=lambda item: item.relative_path))
    candidate_sorted = tuple(sorted(candidate, key=lambda item: item.relative_path))
    candidate_by_path = {item.relative_path: item for item in candidate_sorted}
    used_candidate: set[str] = set()
    used_baseline: set[str] = set()
    matches: list[SampleMatch] = []

    for before in baseline_sorted:
        after = candidate_by_path.get(before.relative_path)
        if after is None:
            continue
        matches.append(
            SampleMatch(
                before.relative_path,
                after.relative_path,
                "path",
                perceptual_distance(before.perceptual_hash, after.perceptual_hash),
                "exact" if before.content_hash == after.content_hash else "probable",
            )
        )
        used_baseline.add(before.relative_path)
        used_candidate.add(after.relative_path)

    remaining_before = [
        item for item in baseline_sorted if item.relative_path not in used_baseline
    ]
    remaining_after = [
        item for item in candidate_sorted if item.relative_path not in used_candidate
    ]
    before_by_digest: dict[str, list[SampleRecord]] = defaultdict(list)
    after_by_digest: dict[str, list[SampleRecord]] = defaultdict(list)
    for item in remaining_before:
        before_by_digest[item.content_hash].append(item)
    for item in remaining_after:
        after_by_digest[item.content_hash].append(item)

    for digest in sorted(before_by_digest.keys() & after_by_digest.keys()):
        before_group = sorted(before_by_digest[digest], key=lambda item: item.relative_path)
        after_group = sorted(after_by_digest[digest], key=lambda item: item.relative_path)
        for before, after in zip(before_group, after_group, strict=False):
            matches.append(
                SampleMatch(
                    before.relative_path,
                    after.relative_path,
                    "content",
                    0,
                    "exact",
                )
            )
            used_baseline.add(before.relative_path)
            used_candidate.add(after.relative_path)

    remaining_before = [
        item for item in baseline_sorted if item.relative_path not in used_baseline
    ]
    remaining_after = [
        item for item in candidate_sorted if item.relative_path not in used_candidate
    ]
    perceptual_matches, ambiguous_findings = _match_perceptual_components(
        remaining_before, remaining_after, phash_threshold
    )
    matches.extend(perceptual_matches)
    used_baseline.update(item.baseline_id for item in perceptual_matches)
    used_candidate.update(item.candidate_id for item in perceptual_matches)

    matches.sort(key=lambda item: (item.baseline_id, item.candidate_id, item.method))
    return MatchResult(
        baseline_sorted,
        candidate_sorted,
        tuple(matches),
        tuple(item for item in baseline_sorted if item.relative_path not in used_baseline),
        tuple(item for item in candidate_sorted if item.relative_path not in used_candidate),
        ambiguous_findings,
        phash_threshold=phash_threshold,
    )


def _match_perceptual_components(
    baseline: list[SampleRecord],
    candidate: list[SampleRecord],
    threshold: int,
) -> tuple[list[SampleMatch], tuple[Finding, ...]]:
    baseline_hashes = {
        item.perceptual_hash for item in baseline if item.perceptual_hash is not None
    }
    candidate_hashes = {
        item.perceptual_hash for item in candidate if item.perceptual_hash is not None
    }
    if (
        len(baseline_hashes) == 1
        and baseline_hashes == candidate_hashes
        and all(item.perceptual_hash is not None for item in baseline)
        and all(item.perceptual_hash is not None for item in candidate)
    ):
        fast_pairs = list(zip(baseline, candidate, strict=False))
        ambiguous = len(fast_pairs) > 1
        fast_matches = [
            SampleMatch(
                before.relative_path,
                after.relative_path,
                "perceptual",
                0,
                "ambiguous" if ambiguous else "probable",
            )
            for before, after in fast_pairs
        ]
        if not ambiguous:
            return fast_matches, ()
        sample_ids = tuple(
            sorted(
                [item.relative_path for item in baseline]
                + [item.relative_path for item in candidate]
            )
        )
        return fast_matches, (
            Finding(
                "AMBIGUOUS_MATCH",
                Severity.WARNING,
                "multiple perceptual assignments have the same minimum cost",
                sample_ids,
                (("minimum_distance", 0),),
            ),
        )
    distances: dict[tuple[int, int], int] = {}
    before_neighbors: dict[int, set[int]] = defaultdict(set)
    after_neighbors: dict[int, set[int]] = defaultdict(set)
    candidate_index: PhashIndex[int] = PhashIndex()
    for after_index, after in enumerate(candidate):
        if after.perceptual_hash is not None:
            candidate_index.add(after.perceptual_hash, after_index)
    for before_index, before in enumerate(baseline):
        if before.perceptual_hash is None:
            continue
        for candidate_match in candidate_index.query(before.perceptual_hash, threshold):
            for after_index in candidate_match.items:
                distances[(before_index, after_index)] = candidate_match.distance
                before_neighbors[before_index].add(after_index)
                after_neighbors[after_index].add(before_index)

    matches: list[SampleMatch] = []
    findings: list[Finding] = []
    visited_before: set[int] = set()
    for start in sorted(before_neighbors, key=lambda index: baseline[index].relative_path):
        if start in visited_before:
            continue
        component_before: set[int] = set()
        component_after: set[int] = set()
        stack: list[tuple[str, int]] = [("before", start)]
        while stack:
            side, index = stack.pop()
            if side == "before":
                if index in component_before:
                    continue
                component_before.add(index)
                stack.extend(("after", other) for other in before_neighbors[index])
            else:
                if index in component_after:
                    continue
                component_after.add(index)
                stack.extend(("before", other) for other in after_neighbors[index])
        visited_before.update(component_before)
        before_indexes = sorted(component_before, key=lambda index: baseline[index].relative_path)
        after_indexes = sorted(component_after, key=lambda index: candidate[index].relative_path)
        component_distances = {
            distances[(before_index, after_index)]
            for before_index in before_indexes
            for after_index in after_indexes
            if (before_index, after_index) in distances
        }
        complete_edge_count = len(before_indexes) * len(after_indexes)
        actual_edge_count = sum(
            (before_index, after_index) in distances
            for before_index in before_indexes
            for after_index in after_indexes
        )
        if len(component_distances) == 1 and actual_edge_count == complete_edge_count:
            selected = list(zip(before_indexes, after_indexes, strict=False))
            primary_distance = sum(distances[pair] for pair in selected)
            ambiguous = len(selected) > 1
        else:
            selected = _solve_component(before_indexes, after_indexes, distances, tie_break=True)
            primary_count = len(selected)
            primary_distance = sum(distances[pair] for pair in selected)
            ambiguous = False
            for pair in selected:
                alternative = _solve_component(
                    before_indexes,
                    after_indexes,
                    distances,
                    forbidden=pair,
                    tie_break=False,
                )
                if len(alternative) == primary_count and sum(
                    distances[item] for item in alternative
                ) == primary_distance:
                    ambiguous = True
                    break
        if ambiguous:
            sample_ids = tuple(
                sorted(
                    [baseline[index].relative_path for index in before_indexes]
                    + [candidate[index].relative_path for index in after_indexes]
                )
            )
            findings.append(
                Finding(
                    "AMBIGUOUS_MATCH",
                    Severity.WARNING,
                    "multiple perceptual assignments have the same minimum cost",
                    sample_ids,
                    (("minimum_distance", primary_distance),),
                )
            )
        for before_index, after_index in selected:
            matches.append(
                SampleMatch(
                    baseline[before_index].relative_path,
                    candidate[after_index].relative_path,
                    "perceptual",
                    distances[(before_index, after_index)],
                    "ambiguous" if ambiguous else "probable",
                )
            )
    matches.sort(key=lambda item: (item.baseline_id, item.candidate_id))
    findings.sort(key=lambda item: item.sample_ids)
    return matches, tuple(findings)


def _solve_component(
    before_indexes: list[int],
    after_indexes: list[int],
    distances: dict[tuple[int, int], int],
    *,
    forbidden: tuple[int, int] | None = None,
    tie_break: bool,
) -> list[tuple[int, int]]:
    before_count = len(before_indexes)
    after_count = len(after_indexes)
    size = before_count + after_count
    unmatched_cost = float(10_000 * (max(before_count, after_count) + 1))
    invalid_cost = unmatched_cost * (size + 1)
    costs = np.full((size, size), invalid_cost, dtype=np.float64)
    edge_count = max(1, before_count * after_count)
    match_count = max(1, min(before_count, after_count))
    for row, before_index in enumerate(before_indexes):
        costs[row, after_count + row] = unmatched_cost
        for column, after_index in enumerate(after_indexes):
            pair = (before_index, after_index)
            if pair == forbidden or pair not in distances:
                continue
            tie = (row * after_count + column) / ((edge_count + 1) * (match_count + 1))
            costs[row, column] = distances[pair] + (tie if tie_break else 0.0)
    for column in range(after_count):
        costs[before_count + column, column] = unmatched_cost
        costs[before_count + column, after_count:] = 0.0

    rows, columns = linear_sum_assignment(costs)
    selected: list[tuple[int, int]] = []
    for row, column in zip(rows.tolist(), columns.tolist(), strict=True):
        if row >= before_count or column >= after_count:
            continue
        pair = (before_indexes[row], after_indexes[column])
        if pair in distances and pair != forbidden:
            selected.append(pair)
    selected.sort()
    return selected
