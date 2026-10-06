from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass

from blake3 import blake3

from datajig.manifest import ManifestEntry, SnapshotManifest

MAX_DIFF_PAGE_SIZE = 200
MAX_DIFF_JSON_CHARS = 50_000


@dataclass(frozen=True, slots=True)
class ManifestChange:
    change_id: str
    kind: str
    before_path: str | None
    after_path: str | None
    before_hash: str | None
    after_hash: str | None
    before_size: int | None
    after_size: int | None

    def to_dict(self) -> dict[str, object]:
        return {
            "id": self.change_id,
            "kind": self.kind,
            "before_path": self.before_path,
            "after_path": self.after_path,
            "before_hash": self.before_hash,
            "after_hash": self.after_hash,
            "before_size": self.before_size,
            "after_size": self.after_size,
        }


@dataclass(frozen=True, slots=True)
class ManifestDiff:
    before_snapshot_id: str
    after_snapshot_id: str
    unchanged: int
    changes: tuple[ManifestChange, ...]


def diff_manifests(before: SnapshotManifest, after: SnapshotManifest) -> ManifestDiff:
    before_by_path = {item.path: item for item in before.entries}
    after_by_path = {item.path: item for item in after.entries}
    common_paths = sorted(before_by_path.keys() & after_by_path.keys())
    unchanged = 0
    modified: list[ManifestChange] = []
    for path in common_paths:
        old = before_by_path[path]
        new = after_by_path[path]
        if old.content_hash == new.content_hash and old.size == new.size:
            unchanged += 1
        else:
            modified.append(_change("modified", old, new))

    removed = [
        before_by_path[path] for path in sorted(before_by_path.keys() - after_by_path.keys())
    ]
    added = [
        after_by_path[path] for path in sorted(after_by_path.keys() - before_by_path.keys())
    ]
    removed_groups = _by_identity(removed)
    added_groups = _by_identity(added)
    renamed: list[ManifestChange] = []
    paired_removed: set[str] = set()
    paired_added: set[str] = set()
    for identity in sorted(removed_groups.keys() & added_groups.keys()):
        old_group = removed_groups[identity]
        new_group = added_groups[identity]
        for old, new in zip(old_group, new_group, strict=False):
            renamed.append(_change("renamed", old, new))
            paired_removed.add(old.path)
            paired_added.add(new.path)
    renamed.sort(key=lambda item: (item.before_path or "", item.after_path or ""))

    remaining_removed = [
        _change("removed", item, None)
        for item in removed
        if item.path not in paired_removed
    ]
    remaining_added = [
        _change("added", None, item) for item in added if item.path not in paired_added
    ]
    changes = tuple(modified + renamed + remaining_removed + remaining_added)
    return ManifestDiff(before.snapshot_id, after.snapshot_id, unchanged, changes)


def manifest_diff_page(
    result: ManifestDiff,
    *,
    offset: int = 0,
    limit: int = 50,
) -> dict[str, object]:
    if offset < 0:
        raise ValueError("offset must be non-negative")
    if limit < 1 or limit > MAX_DIFF_PAGE_SIZE:
        raise ValueError(f"limit must be between 1 and {MAX_DIFF_PAGE_SIZE}")
    selected = result.changes[offset : offset + limit]
    counts = Counter(change.kind for change in result.changes)
    changes = [change.to_dict() for change in selected]
    payload: dict[str, object] = {
        "agent_api_version": 1,
        "kind": "manifest_diff",
        "before_snapshot_id": result.before_snapshot_id,
        "after_snapshot_id": result.after_snapshot_id,
        "summary": {
            "unchanged": result.unchanged,
            "modified": counts["modified"],
            "renamed": counts["renamed"],
            "removed": counts["removed"],
            "added": counts["added"],
        },
        "page": {
            "offset": offset,
            "limit": limit,
            "returned": len(changes),
            "total": len(result.changes),
            "has_more": offset + len(changes) < len(result.changes),
        },
        "changes": changes,
        "budget_truncated": False,
    }
    while changes and _json_size(payload) > MAX_DIFF_JSON_CHARS:
        changes.pop()
        page = payload["page"]
        assert isinstance(page, dict)
        page["returned"] = len(changes)
        page["has_more"] = offset + len(changes) < len(result.changes)
        payload["budget_truncated"] = True
    return payload


def _by_identity(
    entries: list[ManifestEntry],
) -> dict[tuple[str, int], list[ManifestEntry]]:
    groups: dict[tuple[str, int], list[ManifestEntry]] = defaultdict(list)
    for item in entries:
        groups[(item.content_hash, item.size)].append(item)
    return groups


def _change(
    kind: str,
    before: ManifestEntry | None,
    after: ManifestEntry | None,
) -> ManifestChange:
    before_path = before.path if before else None
    after_path = after.path if after else None
    before_hash = before.content_hash if before else None
    after_hash = after.content_hash if after else None
    before_size = before.size if before else None
    after_size = after.size if after else None
    payload = {
        "kind": kind,
        "before_path": before_path,
        "after_path": after_path,
        "before_hash": before_hash,
        "after_hash": after_hash,
        "before_size": before_size,
        "after_size": after_size,
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    digest = blake3(b"datajig-manifest-change-v1\0" + canonical).hexdigest()
    return ManifestChange(
        f"chg_{digest}",
        kind,
        before_path,
        after_path,
        before_hash,
        after_hash,
        before_size,
        after_size,
    )


def _json_size(value: object) -> int:
    return len(json.dumps(value, sort_keys=True, separators=(",", ":")))
