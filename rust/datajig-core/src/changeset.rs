use crate::DatasetInventory;
use crate::revision::validate_content_id;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const CHANGESET_SCHEMA_VERSION: u8 = 1;
pub const RECORD_CHANGESET_SCHEMA_VERSION: u8 = 2;
pub const MAX_CHANGESET_BYTES: usize = 128 * 1024;
const MAX_CHANGESET_STRING_BYTES: usize = 4_096;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeDeclaration {
    schema_version: u8,
    dataset_id: String,
    adapter: String,
    base_revision_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_inventory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_state_id: Option<String>,
    intent: String,
    external_task_id: String,
    actor_kind: String,
    tool_version: String,
    change_id: String,
}

#[derive(Serialize)]
struct DeclarationIdentityV1<'a> {
    schema_version: u8,
    dataset_id: &'a str,
    adapter: &'a str,
    base_revision_id: &'a str,
    base_inventory_id: &'a str,
    intent: &'a str,
    external_task_id: &'a str,
    actor_kind: &'a str,
    tool_version: &'a str,
}

#[derive(Serialize)]
struct DeclarationIdentityV2<'a> {
    schema_version: u8,
    dataset_id: &'a str,
    adapter: &'a str,
    base_revision_id: &'a str,
    base_state_id: &'a str,
    intent: &'a str,
    external_task_id: &'a str,
    actor_kind: &'a str,
    tool_version: &'a str,
}

impl ChangeDeclaration {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dataset_id: String,
        adapter: String,
        base_revision_id: String,
        base_inventory_id: String,
        intent: String,
        external_task_id: String,
        actor_kind: String,
        tool_version: String,
    ) -> Result<Self> {
        let mut value = Self {
            schema_version: CHANGESET_SCHEMA_VERSION,
            dataset_id,
            adapter,
            base_revision_id,
            base_inventory_id: Some(base_inventory_id),
            base_state_id: None,
            intent,
            external_task_id,
            actor_kind,
            tool_version,
            change_id: String::new(),
        };
        value.validate_fields()?;
        value.change_id = value.compute_id()?;
        Ok(value)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_for_state(
        dataset_id: String,
        adapter: String,
        base_revision_id: String,
        base_state_id: String,
        intent: String,
        external_task_id: String,
        actor_kind: String,
        tool_version: String,
    ) -> Result<Self> {
        let mut value = Self {
            schema_version: RECORD_CHANGESET_SCHEMA_VERSION,
            dataset_id,
            adapter,
            base_revision_id,
            base_inventory_id: None,
            base_state_id: Some(base_state_id),
            intent,
            external_task_id,
            actor_kind,
            tool_version,
            change_id: String::new(),
        };
        value.validate_fields()?;
        value.change_id = value.compute_id()?;
        Ok(value)
    }

    pub fn from_json(payload: &str) -> Result<Self> {
        ensure_size(payload)?;
        let value: Self =
            serde_json::from_str(payload).context("invalid change declaration JSON")?;
        value.validate_fields()?;
        if value.change_id != value.compute_id()? {
            bail!("change_id does not match declaration content");
        }
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        let payload = serde_json::to_string_pretty(self)?;
        ensure_size(&payload)?;
        Ok(payload)
    }

    pub fn change_id(&self) -> &str {
        &self.change_id
    }
    pub fn dataset_id(&self) -> &str {
        &self.dataset_id
    }
    pub fn adapter(&self) -> &str {
        &self.adapter
    }
    pub fn base_revision_id(&self) -> &str {
        &self.base_revision_id
    }
    pub fn base_inventory_id(&self) -> &str {
        self.base_inventory_id
            .as_deref()
            .expect("base_inventory_id is only available on imagefolder declarations")
    }
    pub fn base_state_id(&self) -> &str {
        self.base_state_id
            .as_deref()
            .or(self.base_inventory_id.as_deref())
            .expect("validated declarations contain a base state identity")
    }
    pub fn intent(&self) -> &str {
        &self.intent
    }
    pub fn external_task_id(&self) -> &str {
        &self.external_task_id
    }
    pub fn actor_kind(&self) -> &str {
        &self.actor_kind
    }

    fn validate_fields(&self) -> Result<()> {
        validate_content_id(&self.dataset_id, "ds", "dataset_id")?;
        validate_content_id(&self.base_revision_id, "rev", "base_revision_id")?;
        match self.schema_version {
            CHANGESET_SCHEMA_VERSION => {
                if self.adapter != "imagefolder" || self.base_state_id.is_some() {
                    bail!("schema-1 change declaration descriptor is invalid");
                }
                validate_content_id(
                    self.base_inventory_id
                        .as_deref()
                        .context("schema-1 declaration is missing base_inventory_id")?,
                    "inventory",
                    "base_inventory_id",
                )?;
            }
            RECORD_CHANGESET_SCHEMA_VERSION => {
                if self.adapter != "jsonl" || self.base_inventory_id.is_some() {
                    bail!("schema-2 change declaration descriptor is invalid");
                }
                validate_content_id(
                    self.base_state_id
                        .as_deref()
                        .context("schema-2 declaration is missing base_state_id")?,
                    "recordstate",
                    "base_state_id",
                )?;
            }
            _ => bail!(
                "unsupported change declaration schema {}",
                self.schema_version
            ),
        }
        for (value, name) in [
            (&self.adapter, "adapter"),
            (&self.intent, "intent"),
            (&self.external_task_id, "external_task_id"),
            (&self.actor_kind, "actor_kind"),
            (&self.tool_version, "tool_version"),
        ] {
            validate_string(value, name)?;
        }
        Ok(())
    }

    fn compute_id(&self) -> Result<String> {
        match self.schema_version {
            CHANGESET_SCHEMA_VERSION => content_id(
                "datajig-change-declaration-v1\0",
                "chg",
                &DeclarationIdentityV1 {
                    schema_version: self.schema_version,
                    dataset_id: &self.dataset_id,
                    adapter: &self.adapter,
                    base_revision_id: &self.base_revision_id,
                    base_inventory_id: self
                        .base_inventory_id
                        .as_deref()
                        .expect("validated declaration"),
                    intent: &self.intent,
                    external_task_id: &self.external_task_id,
                    actor_kind: &self.actor_kind,
                    tool_version: &self.tool_version,
                },
            ),
            RECORD_CHANGESET_SCHEMA_VERSION => content_id(
                "datajig-change-declaration-v2\0",
                "chg",
                &DeclarationIdentityV2 {
                    schema_version: self.schema_version,
                    dataset_id: &self.dataset_id,
                    adapter: &self.adapter,
                    base_revision_id: &self.base_revision_id,
                    base_state_id: self
                        .base_state_id
                        .as_deref()
                        .expect("validated declaration"),
                    intent: &self.intent,
                    external_task_id: &self.external_task_id,
                    actor_kind: &self.actor_kind,
                    tool_version: &self.tool_version,
                },
            ),
            _ => unreachable!("validated declaration schema"),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChangesetSummary {
    pub added: usize,
    pub removed: usize,
    pub replaced: usize,
    pub moved: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordChangesetSummary {
    pub added: usize,
    pub removed: usize,
    pub modified: usize,
    pub moved: usize,
    pub unchanged: usize,
    pub byte_only_changed: bool,
    pub semantic_diff_available: bool,
}

impl RecordChangesetSummary {
    pub fn new(
        added: usize,
        removed: usize,
        modified: usize,
        moved: usize,
        unchanged: usize,
        byte_only_changed: bool,
    ) -> Result<Self> {
        let value = Self {
            added,
            removed,
            modified,
            moved,
            unchanged,
            byte_only_changed,
            semantic_diff_available: true,
        };
        if value.total_records() == 0 && !value.byte_only_changed {
            bail!("record changeset cannot be empty");
        }
        Ok(value)
    }

    pub fn unavailable() -> Self {
        Self {
            added: 0,
            removed: 0,
            modified: 0,
            moved: 0,
            unchanged: 0,
            byte_only_changed: false,
            semantic_diff_available: false,
        }
    }

    pub fn total_changes(&self) -> usize {
        self.added
            .saturating_add(self.removed)
            .saturating_add(self.modified)
            .saturating_add(self.moved)
    }

    pub fn total_records(&self) -> usize {
        self.total_changes().saturating_add(self.unchanged)
    }
}

impl ChangesetSummary {
    pub fn derive(before: &DatasetInventory, after: &DatasetInventory) -> Self {
        let full_file_coverage =
            before.coverage() == "all_files_v2" && after.coverage() == "all_files_v2";
        let before_records = if full_file_coverage {
            before
                .files()
                .iter()
                .map(|file| (file.relative_path(), (file.content_hash(), file.size())))
                .collect::<BTreeMap<_, _>>()
        } else {
            before
                .records()
                .iter()
                .map(|record| {
                    (
                        record.relative_path.as_str(),
                        (record.content_hash.as_str(), record.size),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let after_records = if full_file_coverage {
            after
                .files()
                .iter()
                .map(|file| (file.relative_path(), (file.content_hash(), file.size())))
                .collect::<BTreeMap<_, _>>()
        } else {
            after
                .records()
                .iter()
                .map(|record| {
                    (
                        record.relative_path.as_str(),
                        (record.content_hash.as_str(), record.size),
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let all_paths = before_records
            .keys()
            .chain(after_records.keys())
            .copied()
            .collect::<BTreeSet<_>>();
        let mut replaced = 0;
        let mut removed_records = Vec::new();
        let mut added_records = Vec::new();
        for path in all_paths {
            match (before_records.get(path), after_records.get(path)) {
                (Some(left), Some(right)) if left != right => replaced += 1,
                (Some(_), Some(_)) => {}
                (Some(value), None) => removed_records.push(*value),
                (None, Some(value)) => added_records.push(*value),
                (None, None) => unreachable!(),
            }
        }
        removed_records.sort_unstable();
        added_records.sort_unstable();
        let mut moved = 0;
        let mut left = 0;
        let mut right = 0;
        while left < removed_records.len() && right < added_records.len() {
            match removed_records[left].cmp(&added_records[right]) {
                std::cmp::Ordering::Less => left += 1,
                std::cmp::Ordering::Greater => right += 1,
                std::cmp::Ordering::Equal => {
                    moved += 1;
                    left += 1;
                    right += 1;
                }
            }
        }
        let before_unsupported = if full_file_coverage {
            BTreeSet::new()
        } else {
            before
                .unsupported_paths()
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
        };
        let after_unsupported = if full_file_coverage {
            BTreeSet::new()
        } else {
            after
                .unsupported_paths()
                .iter()
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
        };
        Self {
            added: added_records.len() - moved
                + after_unsupported.difference(&before_unsupported).count(),
            removed: removed_records.len() - moved
                + before_unsupported.difference(&after_unsupported).count(),
            replaced,
            moved,
        }
    }

    pub fn total(&self) -> usize {
        self.added
            .saturating_add(self.removed)
            .saturating_add(self.replaced)
            .saturating_add(self.moved)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetChangeset {
    schema_version: u8,
    change_id: String,
    dataset_id: String,
    adapter: String,
    diff_algorithm: String,
    base_revision_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_inventory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate_inventory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_state_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate_state_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<ChangesetSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    record_summary: Option<RecordChangesetSummary>,
    coverage: String,
    changeset_id: String,
}

#[derive(Serialize)]
struct ChangesetIdentity<'a> {
    schema_version: u8,
    change_id: &'a str,
    dataset_id: &'a str,
    adapter: &'a str,
    diff_algorithm: &'a str,
    base_revision_id: &'a str,
    base_inventory_id: &'a str,
    candidate_inventory_id: &'a str,
    summary: &'a ChangesetSummary,
    coverage: &'a str,
}

#[derive(Serialize)]
struct RecordChangesetIdentity<'a> {
    schema_version: u8,
    change_id: &'a str,
    dataset_id: &'a str,
    adapter: &'a str,
    diff_algorithm: &'a str,
    base_revision_id: &'a str,
    base_state_id: &'a str,
    candidate_state_id: &'a str,
    record_summary: &'a RecordChangesetSummary,
    coverage: &'a str,
}

impl DatasetChangeset {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        change_id: String,
        dataset_id: String,
        adapter: String,
        base_revision_id: String,
        base_inventory_id: String,
        candidate_inventory_id: String,
        summary: ChangesetSummary,
        coverage: String,
    ) -> Result<Self> {
        let diff_algorithm = match coverage.as_str() {
            "supported_media_v1" => "imagefolder-exact-v1",
            "all_files_v2" => "dataset-files-exact-v2",
            _ => bail!("unsupported changeset coverage {coverage}"),
        };
        let mut value = Self {
            schema_version: CHANGESET_SCHEMA_VERSION,
            change_id,
            dataset_id,
            adapter,
            diff_algorithm: diff_algorithm.into(),
            base_revision_id,
            base_inventory_id: Some(base_inventory_id),
            candidate_inventory_id: Some(candidate_inventory_id),
            base_state_id: None,
            candidate_state_id: None,
            summary: Some(summary),
            record_summary: None,
            coverage,
            changeset_id: String::new(),
        };
        value.validate_fields()?;
        value.changeset_id = value.compute_id()?;
        Ok(value)
    }

    pub fn new_record(
        change_id: String,
        dataset_id: String,
        base_revision_id: String,
        base_state_id: String,
        candidate_state_id: String,
        record_summary: RecordChangesetSummary,
    ) -> Result<Self> {
        let mut value = Self {
            schema_version: RECORD_CHANGESET_SCHEMA_VERSION,
            change_id,
            dataset_id,
            adapter: "jsonl".into(),
            diff_algorithm: "jsonl-records-v1".into(),
            base_revision_id,
            base_inventory_id: None,
            candidate_inventory_id: None,
            base_state_id: Some(base_state_id),
            candidate_state_id: Some(candidate_state_id),
            summary: None,
            record_summary: Some(record_summary),
            coverage: "records_all_v1".into(),
            changeset_id: String::new(),
        };
        value.validate_fields()?;
        value.changeset_id = value.compute_id()?;
        Ok(value)
    }

    pub fn from_json(payload: &str) -> Result<Self> {
        ensure_size(payload)?;
        let value: Self =
            serde_json::from_str(payload).context("invalid dataset changeset JSON")?;
        value.validate_fields()?;
        if value.changeset_id != value.compute_id()? {
            bail!("changeset_id does not match changeset content");
        }
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        let payload = serde_json::to_string_pretty(self)?;
        ensure_size(&payload)?;
        Ok(payload)
    }
    pub fn changeset_id(&self) -> &str {
        &self.changeset_id
    }
    pub fn change_id(&self) -> &str {
        &self.change_id
    }
    pub fn dataset_id(&self) -> &str {
        &self.dataset_id
    }
    pub fn adapter(&self) -> &str {
        &self.adapter
    }
    pub fn base_revision_id(&self) -> &str {
        &self.base_revision_id
    }
    pub fn base_inventory_id(&self) -> &str {
        self.base_inventory_id
            .as_deref()
            .expect("base_inventory_id is only available on imagefolder changesets")
    }
    pub fn candidate_inventory_id(&self) -> &str {
        self.candidate_inventory_id
            .as_deref()
            .expect("candidate_inventory_id is only available on imagefolder changesets")
    }
    pub fn base_state_id(&self) -> &str {
        self.base_state_id
            .as_deref()
            .or(self.base_inventory_id.as_deref())
            .expect("validated changesets contain a base state identity")
    }
    pub fn candidate_state_id(&self) -> &str {
        self.candidate_state_id
            .as_deref()
            .or(self.candidate_inventory_id.as_deref())
            .expect("validated changesets contain a candidate state identity")
    }
    pub fn summary(&self) -> &ChangesetSummary {
        self.summary
            .as_ref()
            .expect("summary is only available on imagefolder changesets")
    }
    pub fn record_summary(&self) -> Option<&RecordChangesetSummary> {
        self.record_summary.as_ref()
    }
    pub fn coverage(&self) -> &str {
        &self.coverage
    }

    fn validate_fields(&self) -> Result<()> {
        validate_content_id(&self.change_id, "chg", "change_id")?;
        validate_content_id(&self.dataset_id, "ds", "dataset_id")?;
        validate_content_id(&self.base_revision_id, "rev", "base_revision_id")?;
        match self.schema_version {
            CHANGESET_SCHEMA_VERSION => {
                let descriptor_valid = matches!(
                    (self.coverage.as_str(), self.diff_algorithm.as_str()),
                    ("supported_media_v1", "imagefolder-exact-v1")
                        | ("all_files_v2", "dataset-files-exact-v2")
                );
                if self.adapter != "imagefolder"
                    || !descriptor_valid
                    || self.base_state_id.is_some()
                    || self.candidate_state_id.is_some()
                    || self.record_summary.is_some()
                {
                    bail!("schema-1 changeset descriptor is invalid");
                }
                validate_content_id(
                    self.base_inventory_id
                        .as_deref()
                        .context("schema-1 changeset is missing base_inventory_id")?,
                    "inventory",
                    "base_inventory_id",
                )?;
                validate_content_id(
                    self.candidate_inventory_id
                        .as_deref()
                        .context("schema-1 changeset is missing candidate_inventory_id")?,
                    "inventory",
                    "candidate_inventory_id",
                )?;
                if self
                    .summary
                    .as_ref()
                    .context("schema-1 changeset is missing summary")?
                    .total()
                    == 0
                {
                    bail!("changeset cannot be empty");
                }
            }
            RECORD_CHANGESET_SCHEMA_VERSION => {
                if self.adapter != "jsonl"
                    || self.diff_algorithm != "jsonl-records-v1"
                    || self.coverage != "records_all_v1"
                    || self.base_inventory_id.is_some()
                    || self.candidate_inventory_id.is_some()
                    || self.summary.is_some()
                {
                    bail!("schema-2 changeset descriptor is invalid");
                }
                validate_content_id(
                    self.base_state_id
                        .as_deref()
                        .context("schema-2 changeset is missing base_state_id")?,
                    "recordstate",
                    "base_state_id",
                )?;
                validate_content_id(
                    self.candidate_state_id
                        .as_deref()
                        .context("schema-2 changeset is missing candidate_state_id")?,
                    "recordstate",
                    "candidate_state_id",
                )?;
                let summary = self
                    .record_summary
                    .as_ref()
                    .context("schema-2 changeset is missing record_summary")?;
                if summary.semantic_diff_available
                    && summary.total_records() == 0
                    && !summary.byte_only_changed
                {
                    bail!("record changeset cannot be empty");
                }
            }
            _ => bail!("unsupported changeset schema {}", self.schema_version),
        }
        Ok(())
    }

    fn compute_id(&self) -> Result<String> {
        match self.schema_version {
            CHANGESET_SCHEMA_VERSION => content_id(
                "datajig-changeset-v1\0",
                "changeset",
                &ChangesetIdentity {
                    schema_version: self.schema_version,
                    change_id: &self.change_id,
                    dataset_id: &self.dataset_id,
                    adapter: &self.adapter,
                    diff_algorithm: &self.diff_algorithm,
                    base_revision_id: &self.base_revision_id,
                    base_inventory_id: self
                        .base_inventory_id
                        .as_deref()
                        .expect("validated changeset"),
                    candidate_inventory_id: self
                        .candidate_inventory_id
                        .as_deref()
                        .expect("validated changeset"),
                    summary: self.summary.as_ref().expect("validated changeset"),
                    coverage: &self.coverage,
                },
            ),
            RECORD_CHANGESET_SCHEMA_VERSION => content_id(
                "datajig-changeset-v2\0",
                "changeset",
                &RecordChangesetIdentity {
                    schema_version: self.schema_version,
                    change_id: &self.change_id,
                    dataset_id: &self.dataset_id,
                    adapter: &self.adapter,
                    diff_algorithm: &self.diff_algorithm,
                    base_revision_id: &self.base_revision_id,
                    base_state_id: self.base_state_id.as_deref().expect("validated changeset"),
                    candidate_state_id: self
                        .candidate_state_id
                        .as_deref()
                        .expect("validated changeset"),
                    record_summary: self.record_summary.as_ref().expect("validated changeset"),
                    coverage: &self.coverage,
                },
            ),
            _ => unreachable!("validated changeset schema"),
        }
    }
}

fn content_id(prefix: &str, kind: &str, value: &impl Serialize) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(prefix.as_bytes());
    serde_json::to_writer(&mut hasher, value)?;
    Ok(format!("{kind}_{}", hasher.finalize().to_hex()))
}
fn validate_string(value: &str, name: &str) -> Result<()> {
    if value.is_empty() || value.len() > MAX_CHANGESET_STRING_BYTES {
        bail!("{name} must contain 1 to {MAX_CHANGESET_STRING_BYTES} UTF-8 bytes");
    }
    Ok(())
}
fn ensure_size(payload: &str) -> Result<()> {
    if payload.len() > MAX_CHANGESET_BYTES {
        bail!("changeset object exceeds {MAX_CHANGESET_BYTES} bytes");
    }
    Ok(())
}
