use crate::{ManifestEntry, SnapshotManifest};
use anyhow::{Result, bail};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

pub const MAX_DIFF_PAGE_SIZE: usize = 200;
pub const MAX_DIFF_JSON_CHARS: usize = 50_000;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ManifestChange {
    #[serde(rename = "id")]
    change_id: String,
    kind: String,
    before_path: Option<String>,
    after_path: Option<String>,
    before_hash: Option<String>,
    after_hash: Option<String>,
    before_size: Option<u64>,
    after_size: Option<u64>,
}

impl ManifestChange {
    pub fn change_id(&self) -> &str {
        &self.change_id
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn before_path(&self) -> Option<&str> {
        self.before_path.as_deref()
    }

    pub fn after_path(&self) -> Option<&str> {
        self.after_path.as_deref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManifestDiff {
    before_snapshot_id: String,
    after_snapshot_id: String,
    unchanged: usize,
    changes: Vec<ManifestChange>,
}

impl ManifestDiff {
    pub fn unchanged(&self) -> usize {
        self.unchanged
    }

    pub fn changes(&self) -> &[ManifestChange] {
        &self.changes
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ManifestDiffSummary {
    pub unchanged: usize,
    pub modified: usize,
    pub renamed: usize,
    pub removed: usize,
    pub added: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ManifestDiffPageInfo {
    pub offset: usize,
    pub limit: usize,
    pub returned: usize,
    pub total: usize,
    pub has_more: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ManifestDiffPage {
    pub agent_api_version: u8,
    pub kind: &'static str,
    pub before_snapshot_id: String,
    pub after_snapshot_id: String,
    pub summary: ManifestDiffSummary,
    pub page: ManifestDiffPageInfo,
    pub changes: Vec<ManifestChange>,
    pub budget_truncated: bool,
}

pub fn diff_manifests(before: &SnapshotManifest, after: &SnapshotManifest) -> ManifestDiff {
    let before_by_path: BTreeMap<_, _> = before
        .entries()
        .iter()
        .map(|entry| (entry.path(), entry))
        .collect();
    let after_by_path: BTreeMap<_, _> = after
        .entries()
        .iter()
        .map(|entry| (entry.path(), entry))
        .collect();
    let before_paths: BTreeSet<_> = before_by_path.keys().copied().collect();
    let after_paths: BTreeSet<_> = after_by_path.keys().copied().collect();

    let mut unchanged = 0;
    let mut modified = Vec::new();
    for path in before_paths.intersection(&after_paths) {
        let old = before_by_path[path];
        let new = after_by_path[path];
        if same_identity(old, new) {
            unchanged += 1;
        } else {
            modified.push(change("modified", Some(old), Some(new)));
        }
    }

    let removed: Vec<_> = before_paths
        .difference(&after_paths)
        .map(|path| before_by_path[path])
        .collect();
    let added: Vec<_> = after_paths
        .difference(&before_paths)
        .map(|path| after_by_path[path])
        .collect();
    let removed_groups = by_identity(&removed);
    let added_groups = by_identity(&added);
    let mut renamed = Vec::new();
    let mut paired_removed = BTreeSet::new();
    let mut paired_added = BTreeSet::new();
    for identity in removed_groups
        .keys()
        .filter(|key| added_groups.contains_key(*key))
    {
        for (old, new) in removed_groups[identity]
            .iter()
            .zip(added_groups[identity].iter())
        {
            renamed.push(change("renamed", Some(old), Some(new)));
            paired_removed.insert(old.path());
            paired_added.insert(new.path());
        }
    }
    renamed.sort_by(|left, right| {
        (left.before_path.as_deref(), left.after_path.as_deref())
            .cmp(&(right.before_path.as_deref(), right.after_path.as_deref()))
    });

    let remaining_removed = removed
        .into_iter()
        .filter(|entry| !paired_removed.contains(entry.path()))
        .map(|entry| change("removed", Some(entry), None));
    let remaining_added = added
        .into_iter()
        .filter(|entry| !paired_added.contains(entry.path()))
        .map(|entry| change("added", None, Some(entry)));
    let changes = modified
        .into_iter()
        .chain(renamed)
        .chain(remaining_removed)
        .chain(remaining_added)
        .collect();
    ManifestDiff {
        before_snapshot_id: before.snapshot_id().to_owned(),
        after_snapshot_id: after.snapshot_id().to_owned(),
        unchanged,
        changes,
    }
}

pub fn manifest_diff_page(
    result: &ManifestDiff,
    offset: i64,
    limit: i64,
) -> Result<ManifestDiffPage> {
    if offset < 0 {
        bail!("offset must be non-negative");
    }
    if !(1..=MAX_DIFF_PAGE_SIZE as i64).contains(&limit) {
        bail!("limit must be between 1 and {MAX_DIFF_PAGE_SIZE}");
    }
    let offset = usize::try_from(offset)?;
    let limit = usize::try_from(limit)?;
    let end = offset.saturating_add(limit).min(result.changes.len());
    let mut changes = result.changes.get(offset..end).unwrap_or_default().to_vec();
    let mut page = ManifestDiffPage {
        agent_api_version: 1,
        kind: "manifest_diff",
        before_snapshot_id: result.before_snapshot_id.clone(),
        after_snapshot_id: result.after_snapshot_id.clone(),
        summary: summary(result),
        page: ManifestDiffPageInfo {
            offset,
            limit,
            returned: changes.len(),
            total: result.changes.len(),
            has_more: offset.saturating_add(changes.len()) < result.changes.len(),
        },
        changes: Vec::new(),
        budget_truncated: false,
    };
    page.changes.append(&mut changes);
    while !page.changes.is_empty() && serde_json::to_string(&page)?.len() > MAX_DIFF_JSON_CHARS {
        page.changes.pop();
        page.page.returned = page.changes.len();
        page.page.has_more = offset.saturating_add(page.changes.len()) < result.changes.len();
        page.budget_truncated = true;
    }
    Ok(page)
}

fn same_identity(left: &ManifestEntry, right: &ManifestEntry) -> bool {
    left.content_hash() == right.content_hash() && left.size() == right.size()
}

fn by_identity<'a>(
    entries: &[&'a ManifestEntry],
) -> BTreeMap<(String, u64), Vec<&'a ManifestEntry>> {
    let mut groups: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for entry in entries {
        groups
            .entry((entry.content_hash().to_owned(), entry.size()))
            .or_default()
            .push(*entry);
    }
    groups
}

fn summary(result: &ManifestDiff) -> ManifestDiffSummary {
    let mut summary = ManifestDiffSummary {
        unchanged: result.unchanged,
        modified: 0,
        renamed: 0,
        removed: 0,
        added: 0,
    };
    for change in &result.changes {
        match change.kind.as_str() {
            "modified" => summary.modified += 1,
            "renamed" => summary.renamed += 1,
            "removed" => summary.removed += 1,
            "added" => summary.added += 1,
            _ => unreachable!("change kinds are constructed internally"),
        }
    }
    summary
}

fn change(
    kind: &str,
    before: Option<&ManifestEntry>,
    after: Option<&ManifestEntry>,
) -> ManifestChange {
    let before_path = before.map(|entry| entry.path().to_owned());
    let after_path = after.map(|entry| entry.path().to_owned());
    let before_hash = before.map(|entry| entry.content_hash().to_owned());
    let after_hash = after.map(|entry| entry.content_hash().to_owned());
    let before_size = before.map(ManifestEntry::size);
    let after_size = after.map(ManifestEntry::size);
    let identity = ChangeIdentity {
        after_hash: after_hash.as_deref(),
        after_path: after_path.as_deref(),
        after_size,
        before_hash: before_hash.as_deref(),
        before_path: before_path.as_deref(),
        before_size,
        kind,
    };
    let serialized =
        serde_json::to_string(&identity).expect("change identity is always serializable");
    let canonical = ascii_escaped_json(&serialized);
    let change_id = crate::identity::blake3_content_id(
        "chg",
        b"datajig-manifest-change-v1\0",
        canonical.as_bytes(),
    );
    ManifestChange {
        change_id,
        kind: kind.to_owned(),
        before_path,
        after_path,
        before_hash,
        after_hash,
        before_size,
        after_size,
    }
}

#[derive(Serialize)]
struct ChangeIdentity<'a> {
    after_hash: Option<&'a str>,
    after_path: Option<&'a str>,
    after_size: Option<u64>,
    before_hash: Option<&'a str>,
    before_path: Option<&'a str>,
    before_size: Option<u64>,
    kind: &'a str,
}

fn ascii_escaped_json(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_ascii() {
            escaped.push(character);
        } else {
            let mut code_units = [0_u16; 2];
            for code_unit in character.encode_utf16(&mut code_units) {
                use std::fmt::Write as _;
                write!(escaped, "\\u{code_unit:04x}").expect("writing to a String cannot fail");
            }
        }
    }
    escaped
}
