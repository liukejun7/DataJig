use crate::io::rename_noreplace_is_unsupported;
use crate::jsonl_inspect::{MAX_JSONL_LINE_BYTES, canonical_record_digest, jsonl_record_id_digest};
use crate::jsonl_view::{JsonlSubsetRecipe, JsonlSubsetViewBinding, JsonlViewRecordFact};
use crate::revision::validate_content_id;
use crate::{ConcurrentModificationError, InvalidArgumentError, InvalidRecipeError};
use anyhow::{Context, Result, bail};
use cap_primitives::fs::FollowSymlinks;
use cap_std::ambient_authority;
use cap_std::fs::{Dir, OpenOptions as CapOpenOptions};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const TRAINING_BUNDLE_SCHEMA_VERSION: u8 = 1;
pub const TRAINING_CONSUMER_PLAN_SCHEMA_VERSION: u8 = 1;
pub const MAX_TRAINING_SOURCE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
pub const MAX_TRAINING_BUNDLE_BYTES: u64 =
    MAX_TRAINING_SOURCE_BYTES + crate::MAX_JSONL_DIFF_RECORDS as u64;
pub const MAX_TRAINING_BUNDLE_MANIFEST_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_TRAINING_SPLITS: usize = 16;
pub const MAX_TRAINING_SHARDS: usize = 4_096;
pub const MAX_TRAINING_SEED_BYTES: usize = 256;
pub const MIN_TRAINING_SHARD_RECORDS: usize = 1;
pub const MAX_TRAINING_SHARD_RECORDS: usize = crate::MAX_JSONL_DIFF_RECORDS;
pub const MIN_TRAINING_SHARD_BYTES: u64 = 1;
pub const MAX_TRAINING_SHARD_BYTES: u64 = 1024 * 1024 * 1024;
pub const DEFAULT_TRAINING_SHARD_RECORDS: usize = 10_000;
pub const DEFAULT_TRAINING_SHARD_BYTES: u64 = 256 * 1024 * 1024;
const TRAINING_SPLIT_WEIGHT_TOTAL: u32 = 10_000;

fn invalid_training_split(message: impl AsRef<str>) -> InvalidArgumentError {
    InvalidArgumentError::new(format!(
        "{}; use NAME=WEIGHT with positive relative integers, for example --split train=7 --split val=2 --split test=1",
        message.as_ref()
    ))
    .with_remediation(
        "Provide deterministic positive relative split weights.",
        "export",
        vec![
            "--split".into(),
            "train=7".into(),
            "--split".into(),
            "val=2".into(),
            "--split".into(),
            "test=1".into(),
        ],
    )
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrainingSplitConfig {
    name: String,
    weight: u32,
}

impl TrainingSplitConfig {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn weight(&self) -> u32 {
        self.weight
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrainingExportConfig {
    format: String,
    record_encoding: String,
    split_algorithm: String,
    order_algorithm: String,
    seed: String,
    splits: Vec<TrainingSplitConfig>,
    max_shard_records: usize,
    max_shard_bytes: u64,
}

impl TrainingExportConfig {
    pub fn new(
        seed: String,
        split_specs: &[String],
        max_shard_records: usize,
        max_shard_bytes: u64,
    ) -> Result<Self> {
        if seed.is_empty() || seed.len() > MAX_TRAINING_SEED_BYTES {
            return Err(InvalidArgumentError::new(format!(
                "export seed must contain 1 to {MAX_TRAINING_SEED_BYTES} UTF-8 bytes"
            ))
            .into());
        }
        if split_specs.is_empty() || split_specs.len() > MAX_TRAINING_SPLITS {
            return Err(invalid_training_split(format!(
                "export requires 1 to {MAX_TRAINING_SPLITS} splits"
            ))
            .into());
        }
        if !(MIN_TRAINING_SHARD_RECORDS..=MAX_TRAINING_SHARD_RECORDS).contains(&max_shard_records) {
            return Err(InvalidArgumentError::new(format!(
                "max shard records must be between {MIN_TRAINING_SHARD_RECORDS} and {MAX_TRAINING_SHARD_RECORDS}"
            ))
            .into());
        }
        if !(MIN_TRAINING_SHARD_BYTES..=MAX_TRAINING_SHARD_BYTES).contains(&max_shard_bytes) {
            return Err(InvalidArgumentError::new(format!(
                "max shard bytes must be between {MIN_TRAINING_SHARD_BYTES} and {MAX_TRAINING_SHARD_BYTES}"
            ))
            .into());
        }
        let mut splits = Vec::with_capacity(split_specs.len());
        let mut names = BTreeSet::new();
        let mut total = 0u64;
        for spec in split_specs {
            let (name, weight) = spec.split_once('=').ok_or_else(|| {
                invalid_training_split("split must use the form safe-name=weight")
            })?;
            if !valid_split_name(name) {
                return Err(invalid_training_split(format!(
                    "invalid training split name {name:?}"
                ))
                .into());
            }
            if !names.insert(name.to_owned()) {
                return Err(invalid_training_split(format!(
                    "duplicate training split name {name:?}"
                ))
                .into());
            }
            let weight = weight.parse::<u32>().map_err(|_| {
                invalid_training_split("training split weight must be a positive integer")
            })?;
            if weight == 0 {
                return Err(
                    invalid_training_split("training split weight must be positive").into(),
                );
            }
            total = total.checked_add(u64::from(weight)).ok_or_else(|| {
                invalid_training_split("training split weights overflow their limit")
            })?;
            splits.push(TrainingSplitConfig {
                name: name.into(),
                weight,
            });
        }
        splits.sort_by(|left, right| left.name.cmp(&right.name));
        let mut normalized_total = 0u32;
        let mut remainders = Vec::with_capacity(splits.len());
        for (index, split) in splits.iter_mut().enumerate() {
            let scaled = u64::from(split.weight) * u64::from(TRAINING_SPLIT_WEIGHT_TOTAL);
            split.weight = (scaled / total) as u32;
            normalized_total += split.weight;
            remainders.push((scaled % total, index));
        }
        remainders.sort_by(
            |(left_remainder, left_index), (right_remainder, right_index)| {
                right_remainder
                    .cmp(left_remainder)
                    .then_with(|| splits[*left_index].name.cmp(&splits[*right_index].name))
            },
        );
        for (_, index) in remainders
            .into_iter()
            .take((TRAINING_SPLIT_WEIGHT_TOTAL - normalized_total) as usize)
        {
            splits[index].weight += 1;
        }
        if let Some(split) = splits.iter().find(|split| split.weight == 0) {
            return Err(invalid_training_split(format!(
                "training split {:?} is too small after normalization",
                split.name
            ))
            .into());
        }
        Ok(Self {
            format: "jsonl".into(),
            record_encoding: "source-json-lf-v1".into(),
            split_algorithm: "typed-id-hash-v1".into(),
            order_algorithm: "split-key-v1".into(),
            seed,
            splits,
            max_shard_records,
            max_shard_bytes,
        })
    }

    pub fn seed(&self) -> &str {
        &self.seed
    }

    pub fn splits(&self) -> &[TrainingSplitConfig] {
        &self.splits
    }

    fn validate(&self) -> Result<()> {
        let specs = self
            .splits
            .iter()
            .map(|split| format!("{}={}", split.name, split.weight))
            .collect::<Vec<_>>();
        if Self::new(
            self.seed.clone(),
            &specs,
            self.max_shard_records,
            self.max_shard_bytes,
        )? != *self
        {
            bail!("training export configuration is not canonical");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrainingSourceBinding {
    dataset_id: String,
    revision_id: String,
    state_id: String,
    dataset_content_id: String,
    id_field: String,
    assurance: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    quality_policy_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    accepted_report_id: Option<String>,
}

impl TrainingSourceBinding {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        dataset_id: String,
        revision_id: String,
        state_id: String,
        dataset_content_id: String,
        id_field: String,
        quality_policy_id: Option<String>,
        accepted_report_id: Option<String>,
    ) -> Result<Self> {
        let assurance = if quality_policy_id.is_some() {
            "quality_policy"
        } else {
            "structural"
        };
        let value = Self {
            dataset_id,
            revision_id,
            state_id,
            dataset_content_id,
            id_field,
            assurance: assurance.into(),
            quality_policy_id,
            accepted_report_id,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<()> {
        validate_content_id(&self.dataset_id, "ds", "training source dataset_id")?;
        validate_content_id(&self.revision_id, "rev", "training source revision_id")?;
        validate_content_id(&self.state_id, "recordstate", "training source state_id")?;
        validate_content_id(
            &self.dataset_content_id,
            "records",
            "training source dataset_content_id",
        )?;
        if self.id_field.is_empty() || self.id_field.len() > crate::MAX_JSONL_FIELD_NAME_BYTES {
            bail!("training source id_field is invalid");
        }
        match (&self.quality_policy_id, self.assurance.as_str()) {
            (Some(policy), "quality_policy") => {
                validate_content_id(policy, "policy", "training source quality_policy_id")?;
            }
            (None, "structural") => {}
            _ => bail!("training source assurance does not match its quality policy"),
        }
        if let Some(report) = &self.accepted_report_id {
            validate_content_id(report, "review", "training source accepted_report_id")?;
        }
        Ok(())
    }

    pub fn revision_id(&self) -> &str {
        &self.revision_id
    }

    pub fn state_id(&self) -> &str {
        &self.state_id
    }

    pub fn assurance(&self) -> &str {
        &self.assurance
    }

    pub(crate) fn dataset_content_id(&self) -> &str {
        &self.dataset_content_id
    }

    pub(crate) fn id_field(&self) -> &str {
        &self.id_field
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrainingShard {
    path: String,
    split: String,
    index: usize,
    records: usize,
    bytes: u64,
    shard_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrainingSplitSummary {
    records: usize,
    bytes: u64,
    shards: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TrainingBundleManifest {
    namespace: String,
    kind: String,
    schema_version: u8,
    source: TrainingSourceBinding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    view: Option<JsonlSubsetViewBinding>,
    config: TrainingExportConfig,
    records: usize,
    bytes: u64,
    splits: BTreeMap<String, TrainingSplitSummary>,
    shards: Vec<TrainingShard>,
    bundle_id: String,
}

impl TrainingBundleManifest {
    pub(crate) fn new(
        source: TrainingSourceBinding,
        view: Option<JsonlSubsetViewBinding>,
        config: TrainingExportConfig,
        shards: Vec<TrainingShard>,
    ) -> Result<Self> {
        let mut splits = config
            .splits
            .iter()
            .map(|split| {
                (
                    split.name.clone(),
                    TrainingSplitSummary {
                        records: 0,
                        bytes: 0,
                        shards: 0,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut records = 0usize;
        let mut bytes = 0u64;
        for shard in &shards {
            let summary = splits
                .get_mut(&shard.split)
                .context("training shard names an unknown split")?;
            summary.records = summary
                .records
                .checked_add(shard.records)
                .context("record count overflow")?;
            summary.bytes = summary
                .bytes
                .checked_add(shard.bytes)
                .context("byte count overflow")?;
            summary.shards += 1;
            records = records
                .checked_add(shard.records)
                .context("record count overflow")?;
            bytes = bytes
                .checked_add(shard.bytes)
                .context("byte count overflow")?;
        }
        let mut value = Self {
            namespace: crate::identity::ARTIFACT_NAMESPACE.into(),
            kind: "training_bundle".into(),
            schema_version: TRAINING_BUNDLE_SCHEMA_VERSION,
            source,
            view,
            config,
            records,
            bytes,
            splits,
            shards,
            bundle_id: String::new(),
        };
        value.bundle_id = value.compute_id()?;
        value.validate()?;
        Ok(value)
    }

    fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_TRAINING_BUNDLE_MANIFEST_BYTES {
            bail!("training bundle manifest exceeds {MAX_TRAINING_BUNDLE_MANIFEST_BYTES} bytes");
        }
        crate::strict_json::reject_duplicate_json_members(payload)
            .context("invalid training bundle manifest")?;
        let value: Self =
            serde_json::from_str(payload).context("invalid training bundle manifest")?;
        value.validate()?;
        if value.bundle_id != value.compute_id()? {
            bail!("training bundle identity does not match its manifest");
        }
        Ok(value)
    }

    pub(crate) fn to_json(&self) -> Result<String> {
        let payload = serde_json::to_string_pretty(self)?;
        if payload.len() > MAX_TRAINING_BUNDLE_MANIFEST_BYTES {
            bail!("training bundle manifest exceeds {MAX_TRAINING_BUNDLE_MANIFEST_BYTES} bytes");
        }
        Ok(payload)
    }

    fn compute_id(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Identity<'a> {
            namespace: &'a str,
            kind: &'a str,
            schema_version: u8,
            source: &'a TrainingSourceBinding,
            #[serde(skip_serializing_if = "Option::is_none")]
            view: Option<&'a JsonlSubsetViewBinding>,
            config: &'a TrainingExportConfig,
            records: usize,
            bytes: u64,
            splits: &'a BTreeMap<String, TrainingSplitSummary>,
            shards: &'a [TrainingShard],
        }
        let identity = Identity {
            namespace: &self.namespace,
            kind: &self.kind,
            schema_version: self.schema_version,
            source: &self.source,
            view: self.view.as_ref(),
            config: &self.config,
            records: self.records,
            bytes: self.bytes,
            splits: &self.splits,
            shards: &self.shards,
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"datajig-training-bundle-v1\0");
        serde_json::to_writer(&mut hasher, &identity)?;
        Ok(format!("bundle_{}", hasher.finalize().to_hex()))
    }

    fn validate(&self) -> Result<()> {
        if self.namespace != crate::identity::ARTIFACT_NAMESPACE
            || self.kind != "training_bundle"
            || self.schema_version != TRAINING_BUNDLE_SCHEMA_VERSION
        {
            bail!("unsupported training bundle manifest identity or schema");
        }
        self.source.validate()?;
        if let Some(view) = &self.view {
            view.validate()?;
            if view.selected_records() == 0 || view.selected_records() != self.records {
                bail!("training bundle subset view does not match its record count");
            }
        }
        self.config.validate()?;
        validate_content_id(&self.bundle_id, "bundle", "training bundle_id")?;
        if self.records > crate::MAX_JSONL_DIFF_RECORDS
            || self.bytes > MAX_TRAINING_BUNDLE_BYTES
            || self.shards.len() > MAX_TRAINING_SHARDS
        {
            bail!("training bundle exceeds its record or shard limit");
        }
        let expected_splits = self
            .config
            .splits
            .iter()
            .map(|split| split.name.as_str())
            .collect::<BTreeSet<_>>();
        if self
            .splits
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            != expected_splits
        {
            bail!("training bundle split summaries do not match configuration");
        }
        let mut paths = BTreeSet::new();
        let mut records = 0usize;
        let mut bytes = 0u64;
        let mut summaries = self
            .splits
            .keys()
            .map(|name| {
                (
                    name.clone(),
                    TrainingSplitSummary {
                        records: 0,
                        bytes: 0,
                        shards: 0,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let split_positions = self
            .config
            .splits
            .iter()
            .enumerate()
            .map(|(index, split)| (split.name.as_str(), index))
            .collect::<BTreeMap<_, _>>();
        let mut previous_position = None;
        let mut next_indices = BTreeMap::<&str, usize>::new();
        for shard in &self.shards {
            let split_position = *split_positions
                .get(shard.split.as_str())
                .context("training shard names an unknown split")?;
            let expected_index = next_indices.entry(shard.split.as_str()).or_default();
            if !valid_shard_path(&shard.path, &shard.split, shard.index)
                || !paths.insert(shard.path.as_str())
                || previous_position.is_some_and(|position| split_position < position)
                || shard.index != *expected_index
                || shard.records == 0
                || shard.records > self.config.max_shard_records
                || shard.bytes == 0
                || (shard.bytes > self.config.max_shard_bytes && shard.records != 1)
            {
                bail!("training bundle contains an invalid shard descriptor");
            }
            *expected_index += 1;
            previous_position = Some(split_position);
            validate_content_id(&shard.shard_id, "shard", "training shard_id")?;
            let summary = summaries
                .get_mut(&shard.split)
                .context("training shard names an unknown split")?;
            summary.records += shard.records;
            summary.bytes += shard.bytes;
            summary.shards += 1;
            records += shard.records;
            bytes += shard.bytes;
        }
        if records != self.records || bytes != self.bytes || summaries != self.splits {
            bail!("training bundle aggregate summaries do not match its shards");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct TrainingBundleArtifact {
    pub bundle_id: String,
    pub manifest: String,
    pub source_revision_id: String,
    pub source_state_id: String,
    pub assurance: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipe_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_id: Option<String>,
    pub records: usize,
    pub bytes: u64,
    pub splits: BTreeMap<String, TrainingSplitSummary>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TrainingBundleInfo {
    pub bundle_id: String,
    pub schema_version: u8,
    pub source_revision_id: String,
    pub source_state_id: String,
    pub assurance: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recipe_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub view_id: Option<String>,
    pub records: usize,
    pub bytes: u64,
    pub splits: BTreeMap<String, TrainingSplitSummary>,
    pub verified: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consumer_plan: Option<TrainingConsumerPlan>,
}

pub fn inspect_training_bundle(manifest: &Path, verify: bool) -> Result<TrainingBundleInfo> {
    inspect_training_bundle_with_consumer(manifest, verify, false, None, None, None, None)
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrainingConsumerShard {
    pub relative_path: String,
    pub split: String,
    pub index: usize,
    pub records: usize,
    pub bytes: u64,
    pub shard_id: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrainingConsumerSplit {
    pub name: String,
    pub records: usize,
    pub bytes: u64,
    pub shards: Vec<TrainingConsumerShard>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrainingConsumerPlan {
    pub schema_version: u8,
    pub verified: bool,
    pub integrity_scope: &'static str,
    pub bundle_id: String,
    pub format: String,
    pub record_encoding: String,
    pub source: TrainingSourceBinding,
    pub records: usize,
    pub bytes: u64,
    pub splits: Vec<TrainingConsumerSplit>,
}

#[allow(clippy::too_many_arguments)]
pub fn inspect_training_bundle_with_consumer(
    manifest: &Path,
    verify: bool,
    consumer_plan: bool,
    expected_bundle_id: Option<&str>,
    expected_revision_id: Option<&str>,
    required_assurance: Option<&str>,
    selected_split: Option<&str>,
) -> Result<TrainingBundleInfo> {
    if (consumer_plan
        || expected_bundle_id.is_some()
        || expected_revision_id.is_some()
        || required_assurance.is_some()
        || selected_split.is_some())
        && !verify
    {
        return Err(InvalidArgumentError::new(
            "training consumer plans and identity pins require --verify",
        )
        .into());
    }
    if selected_split.is_some() && !consumer_plan {
        return Err(InvalidArgumentError::new("--split requires --consumer-plan").into());
    }
    let manifest_name = manifest
        .file_name()
        .context("training bundle manifest must name a file")?;
    let root_path = manifest
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .context("cannot resolve training bundle directory")?;
    let root = Dir::open_ambient_dir(&root_path, ambient_authority())
        .context("cannot open training bundle directory")?;
    let payload = load_bounded_regular_at(
        &root,
        Path::new(manifest_name),
        MAX_TRAINING_BUNDLE_MANIFEST_BYTES as u64,
        "training bundle manifest",
    )?;
    let text =
        std::str::from_utf8(&payload).context("training bundle manifest is not valid UTF-8")?;
    let value = TrainingBundleManifest::from_json(text)?;
    if verify {
        if manifest_name != "datajig.bundle.json" {
            bail!("verified training bundle manifest must be named datajig.bundle.json");
        }
        verify_bundle_files(&root, &value, &payload)?;
    }
    if expected_bundle_id.is_some_and(|expected| expected != value.bundle_id) {
        return Err(InvalidArgumentError::new(
            "training bundle does not match the expected bundle ID",
        )
        .into());
    }
    if expected_revision_id.is_some_and(|expected| expected != value.source.revision_id) {
        return Err(InvalidArgumentError::new(
            "training bundle does not match the expected source revision",
        )
        .into());
    }
    if let Some(required) = required_assurance {
        if !matches!(required, "structural" | "quality_policy") {
            return Err(InvalidArgumentError::new(
                "required training assurance must be structural or quality_policy",
            )
            .into());
        }
        if required == "quality_policy" && value.source.assurance != "quality_policy" {
            return Err(InvalidArgumentError::new(
                "training bundle does not meet the required assurance",
            )
            .into());
        }
    }
    let plan = consumer_plan
        .then(|| build_consumer_plan(&value, selected_split))
        .transpose()?;
    Ok(TrainingBundleInfo {
        bundle_id: value.bundle_id,
        schema_version: value.schema_version,
        source_revision_id: value.source.revision_id,
        source_state_id: value.source.state_id,
        assurance: value.source.assurance,
        recipe_id: value.view.as_ref().map(|view| view.recipe_id().into()),
        view_id: value.view.as_ref().map(|view| view.view_id().into()),
        records: value.records,
        bytes: value.bytes,
        splits: value.splits,
        verified: verify,
        consumer_plan: plan,
    })
}

fn build_consumer_plan(
    manifest: &TrainingBundleManifest,
    selected_split: Option<&str>,
) -> Result<TrainingConsumerPlan> {
    if selected_split.is_some_and(|name| !manifest.splits.contains_key(name)) {
        return Err(InvalidArgumentError::new("training bundle split was not found").into());
    }
    let mut splits = Vec::new();
    let mut records = 0usize;
    let mut bytes = 0u64;
    for (name, summary) in &manifest.splits {
        if selected_split.is_some_and(|selected| selected != name) {
            continue;
        }
        let shards = manifest
            .shards
            .iter()
            .filter(|shard| shard.split == *name)
            .map(|shard| TrainingConsumerShard {
                relative_path: shard.path.clone(),
                split: shard.split.clone(),
                index: shard.index,
                records: shard.records,
                bytes: shard.bytes,
                shard_id: shard.shard_id.clone(),
            })
            .collect();
        records = records
            .checked_add(summary.records)
            .context("training consumer record count overflow")?;
        bytes = bytes
            .checked_add(summary.bytes)
            .context("training consumer byte count overflow")?;
        splits.push(TrainingConsumerSplit {
            name: name.clone(),
            records: summary.records,
            bytes: summary.bytes,
            shards,
        });
    }
    Ok(TrainingConsumerPlan {
        schema_version: TRAINING_CONSUMER_PLAN_SCHEMA_VERSION,
        verified: true,
        integrity_scope: "manifest-and-all-shards-v1",
        bundle_id: manifest.bundle_id.clone(),
        format: manifest.config.format.clone(),
        record_encoding: manifest.config.record_encoding.clone(),
        source: manifest.source.clone(),
        records,
        bytes,
        splits,
    })
}

#[derive(Clone)]
struct TrainingRecordRef {
    offset: u64,
    length: usize,
    rid: [u8; 32],
    key: [u8; 32],
    split: usize,
}

struct OwnedTempDirectory {
    parent: Dir,
    name: PathBuf,
    identity: DirectoryIdentity,
    armed: bool,
}

impl Drop for OwnedTempDirectory {
    fn drop(&mut self) {
        if self.armed {
            match open_direct_directory_at(&self.parent, &self.name) {
                Ok(current)
                    if directory_identity(&current).ok().as_ref() == Some(&self.identity) =>
                {
                    let _ = self.parent.remove_dir_all(&self.name);
                }
                _ => {}
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceFingerprint {
    identity: SourceIdentity,
    len: u64,
    modified: Option<SystemTime>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectoryIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(not(unix))]
    created: Option<SystemTime>,
}

#[cfg(unix)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceIdentity {
    device: u64,
    inode: u64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[cfg(not(unix))]
#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceIdentity {
    created: Option<SystemTime>,
}

pub(crate) fn materialize_training_bundle(
    source: &Path,
    output: &Path,
    binding: &TrainingSourceBinding,
    view: Option<&JsonlSubsetViewBinding>,
    config: &TrainingExportConfig,
) -> Result<TrainingBundleArtifact> {
    binding.validate()?;
    config.validate()?;
    let source_name = source
        .file_name()
        .context("training export source must name a file")?;
    let source_parent_path = source
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .context("cannot resolve training export source parent")?;
    let source_path = source_parent_path.join(source_name);
    let source_parent = Dir::open_ambient_dir(&source_parent_path, ambient_authority())
        .context("cannot open training export source parent")?;
    let source_parent_identity = directory_identity(&source_parent)?;
    let mut source_file = open_direct_regular_at(
        &source_parent,
        Path::new(source_name),
        MAX_TRAINING_SOURCE_BYTES,
        "training export source",
    )
    .map_err(|_| {
        ConcurrentModificationError::new("training export source path changed before export")
    })?;
    let initial_source_fingerprint = source_fingerprint(&source_file).map_err(|_| {
        ConcurrentModificationError::new("training export source changed before export")
    })?;
    let output_name = output
        .file_name()
        .context("training export output must name a new directory")?;
    let output_parent = output
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .context("cannot resolve training export output parent")?;
    let output = output_parent.join(output_name);
    let output_parent_dir = Dir::open_ambient_dir(&output_parent, ambient_authority())
        .context("cannot open training export output parent")?;
    let output_parent_identity = directory_identity(&output_parent_dir)?;
    match output_parent_dir.symlink_metadata(output_name) {
        Ok(_) => {
            return Err(InvalidArgumentError::new("training export output already exists").into());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("cannot inspect training export output"),
    }
    if source_path.starts_with(&output) || output.starts_with(&source_path) {
        return Err(InvalidArgumentError::new("training export output overlaps its source").into());
    }
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before Unix epoch")?
        .as_nanos();
    let temp_name = PathBuf::from(format!(
        ".datajig-export-{}-{}-{nonce}",
        std::process::id(),
        output_name.to_string_lossy()
    ));
    create_private_directory_at(&output_parent_dir, &temp_name)
        .context("cannot create private training export directory")?;
    let container_dir = open_direct_directory_at(&output_parent_dir, &temp_name)
        .context("cannot open private training export directory")?;
    validate_private_directory_owner(&container_dir)?;
    let container_identity = directory_identity(&container_dir)?;
    create_private_directory_at(&container_dir, Path::new("bundle"))
        .context("cannot create private training bundle staging directory")?;
    let temp_dir = open_direct_directory_at(&container_dir, Path::new("bundle"))
        .context("cannot open private training bundle staging directory")?;
    validate_private_directory_owner(&temp_dir)?;
    let mut owned = OwnedTempDirectory {
        parent: output_parent_dir.try_clone()?,
        name: temp_name.clone(),
        identity: container_identity.clone(),
        armed: true,
    };
    let (snapshot, dataset_content_id) = copy_source_snapshot(&mut source_file, &temp_dir)?;
    if source_fingerprint(&source_file).map_err(|_| {
        ConcurrentModificationError::new(
            "training export source changed while it was being snapshotted",
        )
    })? != initial_source_fingerprint
        || dataset_content_id != binding.dataset_content_id()
    {
        return Err(ConcurrentModificationError::new(
            "training export source changed while it was being snapshotted",
        )
        .into());
    }
    let mut records = index_training_records(&snapshot, binding, view, config)?;
    records.sort_unstable_by(|left, right| {
        left.split
            .cmp(&right.split)
            .then_with(|| left.key.cmp(&right.key))
            .then_with(|| left.rid.cmp(&right.rid))
    });
    let shards = write_training_shards(&snapshot, &temp_dir, &records, config)?;
    let manifest =
        TrainingBundleManifest::new(binding.clone(), view.cloned(), config.clone(), shards)?;
    write_private_file_at(
        &temp_dir,
        Path::new("datajig.bundle.json"),
        manifest.to_json()?.as_bytes(),
    )?;
    temp_dir
        .remove_file(".source.jsonl")
        .context("cannot remove private training source snapshot")?;
    let current_source_parent = Dir::open_ambient_dir(&source_parent_path, ambient_authority())
        .map_err(|_| {
            ConcurrentModificationError::new(
                "training export source parent changed while the bundle was being created",
            )
        })?;
    if directory_identity(&current_source_parent).map_err(|_| {
        ConcurrentModificationError::new(
            "training export source parent changed while the bundle was being created",
        )
    })? != source_parent_identity
    {
        return Err(ConcurrentModificationError::new(
            "training export source parent changed while the bundle was being created",
        )
        .into());
    }
    let final_source = open_direct_regular_at(
        &current_source_parent,
        Path::new(source_name),
        MAX_TRAINING_SOURCE_BYTES,
        "training export source",
    )
    .map_err(|_| {
        ConcurrentModificationError::new(
            "training export source path changed while the bundle was being created",
        )
    })?;
    if source_fingerprint(&final_source).map_err(|_| {
        ConcurrentModificationError::new(
            "training export source changed while the bundle was being created",
        )
    })? != initial_source_fingerprint
    {
        return Err(ConcurrentModificationError::new(
            "training export source changed while the bundle was being created",
        )
        .into());
    }
    sync_directory_handle(&temp_dir)?;
    let current_output_parent = Dir::open_ambient_dir(&output_parent, ambient_authority())
        .context("training export output parent changed before publication")?;
    if directory_identity(&current_output_parent)? != output_parent_identity {
        bail!("training export output parent changed before publication");
    }
    publish_noreplace(
        &container_dir,
        Path::new("bundle"),
        &output_parent_dir,
        Path::new(output_name),
    )
    .map_err(|error| atomic_training_publish_error(&output, error))?;
    owned.armed = false;
    match open_direct_directory_at(&output_parent_dir, &temp_name) {
        Ok(current_container)
            if directory_identity(&current_container).ok().as_ref()
                == Some(&container_identity) =>
        {
            let _ = output_parent_dir.remove_dir(&temp_name);
        }
        _ => {}
    }
    Ok(TrainingBundleArtifact {
        bundle_id: manifest.bundle_id,
        manifest: output
            .join("datajig.bundle.json")
            .to_string_lossy()
            .into_owned(),
        source_revision_id: manifest.source.revision_id,
        source_state_id: manifest.source.state_id,
        assurance: manifest.source.assurance,
        recipe_id: manifest
            .view
            .as_ref()
            .map(|view| view.recipe_id().to_owned()),
        view_id: manifest.view.as_ref().map(|view| view.view_id().to_owned()),
        records: manifest.records,
        bytes: manifest.bytes,
        splits: manifest.splits,
    })
}

pub(crate) fn inspect_subset_view(
    source: &Path,
    binding: &TrainingSourceBinding,
    recipe: &JsonlSubsetRecipe,
) -> Result<JsonlSubsetViewBinding> {
    binding.validate()?;
    recipe.validate()?;
    let source_name = source
        .file_name()
        .context("subset view source must name a file")?;
    let source_parent_path = source
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .context("cannot resolve subset view source parent")?;
    let source_parent = Dir::open_ambient_dir(&source_parent_path, ambient_authority())
        .context("cannot open subset view source parent")?;
    let parent_identity = directory_identity(&source_parent)?;
    let source_file = open_direct_regular_at(
        &source_parent,
        Path::new(source_name),
        MAX_TRAINING_SOURCE_BYTES,
        "subset view source",
    )
    .map_err(|_| ConcurrentModificationError::new("subset view source path changed"))?;
    let source_identity = source_fingerprint(&source_file)
        .map_err(|_| ConcurrentModificationError::new("subset view source changed"))?;
    let mut reader = BufReader::new(source_file.try_clone()?);
    let mut dataset_hasher = blake3::Hasher::new();
    dataset_hasher.update(b"datajig-jsonl-recordset-v1\0");
    let mut seen = BTreeSet::new();
    let mut facts = Vec::new();
    let mut source_records = 0usize;
    let mut line = Vec::new();
    loop {
        line.clear();
        let read = Read::by_ref(&mut reader)
            .take((MAX_JSONL_LINE_BYTES + 2) as u64)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        dataset_hasher.update(&line);
        let mut content = line.as_slice();
        if content.ends_with(b"\n") {
            content = &content[..content.len() - 1];
        }
        if content.ends_with(b"\r") {
            content = &content[..content.len() - 1];
        }
        if content.len() > MAX_JSONL_LINE_BYTES {
            return Err(ConcurrentModificationError::new(
                "subset view source changed after structural validation",
            )
            .into());
        }
        let text = std::str::from_utf8(content).map_err(|_| {
            ConcurrentModificationError::new(
                "subset view source changed after structural validation",
            )
        })?;
        if text.trim().is_empty() {
            continue;
        }
        source_records = source_records
            .checked_add(1)
            .context("record count overflow")?;
        if source_records > crate::MAX_JSONL_DIFF_RECORDS {
            bail!("subset view source exceeds the record limit");
        }
        let value: serde_json::Value = serde_json::from_str(text).map_err(|_| {
            ConcurrentModificationError::new(
                "subset view source changed after structural validation",
            )
        })?;
        let object = value.as_object().ok_or_else(|| {
            ConcurrentModificationError::new(
                "subset view source changed after structural validation",
            )
        })?;
        let rid = object
            .get(binding.id_field())
            .and_then(jsonl_record_id_digest)
            .ok_or_else(|| {
                ConcurrentModificationError::new(
                    "subset view source changed after structural validation",
                )
            })?;
        if !seen.insert(rid) {
            return Err(ConcurrentModificationError::new(
                "subset view source changed after structural validation",
            )
            .into());
        }
        if recipe.matches(object) && recipe.includes_sample(rid) {
            facts.push(JsonlViewRecordFact::new(
                rid,
                canonical_record_digest(&value),
            ));
        }
    }
    let dataset_content_id = format!("records_{}", dataset_hasher.finalize().to_hex());
    if dataset_content_id != binding.dataset_content_id()
        || source_fingerprint(&source_file)
            .map_err(|_| ConcurrentModificationError::new("subset view source changed"))?
            != source_identity
    {
        return Err(ConcurrentModificationError::new(
            "subset view source changed while it was inspected",
        )
        .into());
    }
    let current_parent = Dir::open_ambient_dir(&source_parent_path, ambient_authority())
        .map_err(|_| ConcurrentModificationError::new("subset view source parent changed"))?;
    if directory_identity(&current_parent)
        .map_err(|_| ConcurrentModificationError::new("subset view source parent changed"))?
        != parent_identity
    {
        return Err(ConcurrentModificationError::new(
            "subset view source parent changed while it was inspected",
        )
        .into());
    }
    let final_source = open_direct_regular_at(
        &current_parent,
        Path::new(source_name),
        MAX_TRAINING_SOURCE_BYTES,
        "subset view source",
    )
    .map_err(|_| ConcurrentModificationError::new("subset view source path changed"))?;
    if source_fingerprint(&final_source)
        .map_err(|_| ConcurrentModificationError::new("subset view source changed"))?
        != source_identity
    {
        return Err(ConcurrentModificationError::new(
            "subset view source changed while it was inspected",
        )
        .into());
    }
    JsonlSubsetViewBinding::new(binding, recipe.clone(), source_records, facts)
}

pub(crate) fn load_subset_recipe_file(path: &Path) -> Result<JsonlSubsetRecipe> {
    let name = path
        .file_name()
        .ok_or_else(|| InvalidRecipeError::new("subset recipe must name a file"))?;
    let parent_path = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .map_err(|_| InvalidRecipeError::new("cannot resolve subset recipe parent"))?;
    let parent = Dir::open_ambient_dir(&parent_path, ambient_authority())
        .map_err(|_| InvalidRecipeError::new("cannot open subset recipe parent"))?;
    let parent_identity = directory_identity(&parent)
        .map_err(|_| InvalidRecipeError::new("cannot inspect subset recipe parent"))?;
    let file = open_direct_regular_at(
        &parent,
        Path::new(name),
        crate::MAX_JSONL_SUBSET_RECIPE_BYTES as u64,
        "subset recipe",
    )
    .map_err(|_| InvalidRecipeError::new("subset recipe must be a bounded direct regular file"))?;
    let identity = source_fingerprint(&file)
        .map_err(|_| InvalidRecipeError::new("cannot inspect subset recipe"))?;
    let mut payload = Vec::with_capacity(identity.len as usize);
    Read::by_ref(&mut &file)
        .take(crate::MAX_JSONL_SUBSET_RECIPE_BYTES as u64 + 1)
        .read_to_end(&mut payload)
        .map_err(|_| InvalidRecipeError::new("cannot read subset recipe"))?;
    if payload.len() > crate::MAX_JSONL_SUBSET_RECIPE_BYTES
        || source_fingerprint(&file)
            .map_err(|_| InvalidRecipeError::new("cannot inspect subset recipe"))?
            != identity
    {
        return Err(InvalidRecipeError::new("subset recipe changed while being read").into());
    }
    let current_parent = Dir::open_ambient_dir(&parent_path, ambient_authority())
        .map_err(|_| InvalidRecipeError::new("subset recipe parent changed"))?;
    let final_file = open_direct_regular_at(
        &current_parent,
        Path::new(name),
        crate::MAX_JSONL_SUBSET_RECIPE_BYTES as u64,
        "subset recipe",
    )
    .map_err(|_| InvalidRecipeError::new("subset recipe path changed"))?;
    if directory_identity(&current_parent)
        .map_err(|_| InvalidRecipeError::new("subset recipe parent changed"))?
        != parent_identity
        || source_fingerprint(&final_file)
            .map_err(|_| InvalidRecipeError::new("subset recipe path changed"))?
            != identity
    {
        return Err(InvalidRecipeError::new("subset recipe changed while being read").into());
    }
    let text = std::str::from_utf8(&payload)
        .map_err(|_| InvalidRecipeError::new("subset recipe must be UTF-8 JSON"))?;
    JsonlSubsetRecipe::from_json(text)
        .map_err(|_| InvalidRecipeError::new("subset recipe is invalid").into())
}

fn copy_source_snapshot(input: &mut File, root: &Dir) -> Result<(File, String)> {
    input.seek(SeekFrom::Start(0))?;
    let mut output = create_private_file_at(root, Path::new(".source.jsonl"), true)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-jsonl-recordset-v1\0");
    let mut copied = 0u64;
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = input
            .read(&mut buffer)
            .context("cannot snapshot training export source")?;
        if read == 0 {
            break;
        }
        copied = copied
            .checked_add(read as u64)
            .context("training export source byte count overflow")?;
        if copied > MAX_TRAINING_SOURCE_BYTES {
            return Err(ConcurrentModificationError::new(
                "training export source grew beyond its byte limit while being snapshotted",
            )
            .into());
        }
        output.write_all(&buffer[..read])?;
        hasher.update(&buffer[..read]);
    }
    output
        .sync_all()
        .context("cannot sync training source snapshot")?;
    output.seek(SeekFrom::Start(0))?;
    Ok((output, format!("records_{}", hasher.finalize().to_hex())))
}

fn index_training_records(
    snapshot: &File,
    binding: &TrainingSourceBinding,
    view: Option<&JsonlSubsetViewBinding>,
    config: &TrainingExportConfig,
) -> Result<Vec<TrainingRecordRef>> {
    let mut file = snapshot.try_clone()?;
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(file);
    let mut records = Vec::new();
    let mut source_records = 0usize;
    let mut view_facts = Vec::new();
    let mut line = Vec::new();
    let mut offset = 0u64;
    loop {
        line.clear();
        let read = Read::by_ref(&mut reader)
            .take((MAX_JSONL_LINE_BYTES + 2) as u64)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        let record_offset = offset;
        offset = offset
            .checked_add(read as u64)
            .context("training source offset overflow")?;
        let mut content = line.as_slice();
        if content.ends_with(b"\n") {
            content = &content[..content.len() - 1];
        }
        if content.ends_with(b"\r") {
            content = &content[..content.len() - 1];
        }
        let text = std::str::from_utf8(content)
            .context("training source changed after structural validation")?;
        if text.trim().is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(text)
            .context("training source changed after structural validation")?;
        let object = value
            .as_object()
            .context("training source record is not an object")?;
        source_records = source_records
            .checked_add(1)
            .context("training source record count overflow")?;
        let rid = object
            .get(binding.id_field())
            .and_then(jsonl_record_id_digest)
            .context("training source record has an invalid identity")?;
        if let Some(view) = view {
            if !view.recipe().matches(object) || !view.recipe().includes_sample(rid) {
                continue;
            }
            view_facts.push(JsonlViewRecordFact::new(
                rid,
                canonical_record_digest(&value),
            ));
        }
        let key = training_record_key(config.seed.as_bytes(), rid);
        records.push(TrainingRecordRef {
            offset: record_offset,
            length: content.len(),
            rid,
            key,
            split: choose_split(&key, &config.splits),
        });
        if records.len() > crate::MAX_JSONL_DIFF_RECORDS {
            bail!("training export exceeds the record limit");
        }
    }
    if let Some(expected) = view {
        let actual = JsonlSubsetViewBinding::new(
            binding,
            expected.recipe().clone(),
            source_records,
            view_facts,
        )?;
        if actual != *expected {
            return Err(ConcurrentModificationError::new(
                "subset view changed while the training bundle was created",
            )
            .into());
        }
    }
    Ok(records)
}

fn training_record_key(seed: &[u8], rid: [u8; 32]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-training-split-v1\0");
    hasher.update(&(seed.len() as u64).to_be_bytes());
    hasher.update(seed);
    hasher.update(&rid);
    *hasher.finalize().as_bytes()
}

fn choose_split(key: &[u8; 32], splits: &[TrainingSplitConfig]) -> usize {
    let value = u64::from_be_bytes(key[..8].try_into().expect("eight-byte hash prefix"));
    let slot = ((u128::from(value) * u128::from(TRAINING_SPLIT_WEIGHT_TOTAL)) >> 64) as u32;
    let mut cumulative = 0u32;
    for (index, split) in splits.iter().enumerate() {
        cumulative += split.weight;
        if slot < cumulative {
            return index;
        }
    }
    unreachable!("validated split weights cover every slot")
}

fn write_training_shards(
    snapshot: &File,
    root: &Dir,
    records: &[TrainingRecordRef],
    config: &TrainingExportConfig,
) -> Result<Vec<TrainingShard>> {
    let mut source = snapshot.try_clone()?;
    let mut shards = Vec::new();
    for (split_index, split) in config.splits.iter().enumerate() {
        let split_records = records
            .iter()
            .filter(|record| record.split == split_index)
            .collect::<Vec<_>>();
        if split_records.is_empty() {
            continue;
        }
        create_private_directory_at(root, Path::new(&split.name))?;
        let split_dir = root.open_dir(&split.name)?;
        let mut position = 0usize;
        let mut shard_index = 0usize;
        while position < split_records.len() {
            if shards.len() >= MAX_TRAINING_SHARDS {
                bail!("training export exceeds {MAX_TRAINING_SHARDS} shards");
            }
            let relative = format!("{}/part-{shard_index:05}.jsonl", split.name);
            let shard_name = format!("part-{shard_index:05}.jsonl");
            let file = create_private_file_at(&split_dir, Path::new(&shard_name), false)?;
            let mut writer = BufWriter::new(file);
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"datajig-training-shard-v1\0");
            let mut shard_records = 0usize;
            let mut shard_bytes = 0u64;
            while position < split_records.len() {
                let record = split_records[position];
                let record_bytes = record.length as u64 + 1;
                if shard_records > 0
                    && (shard_records == config.max_shard_records
                        || shard_bytes + record_bytes > config.max_shard_bytes)
                {
                    break;
                }
                let mut payload = vec![0u8; record.length];
                source.seek(SeekFrom::Start(record.offset))?;
                source.read_exact(&mut payload)?;
                writer.write_all(&payload)?;
                writer.write_all(b"\n")?;
                hasher.update(&payload);
                hasher.update(b"\n");
                shard_records += 1;
                shard_bytes += record_bytes;
                position += 1;
            }
            writer.flush()?;
            writer.get_ref().sync_all()?;
            shards.push(TrainingShard {
                path: relative,
                split: split.name.clone(),
                index: shard_index,
                records: shard_records,
                bytes: shard_bytes,
                shard_id: format!("shard_{}", hasher.finalize().to_hex()),
            });
            shard_index += 1;
        }
        sync_directory_handle(&split_dir)?;
    }
    Ok(shards)
}

fn nofollow_options(read: bool, write: bool, create_new: bool) -> CapOpenOptions {
    let mut options = CapOpenOptions::new();
    options
        .read(read)
        .write(write)
        .create_new(create_new)
        ._cap_fs_ext_follow(FollowSymlinks::No)
        ._cap_fs_ext_nonblock(true);
    #[cfg(unix)]
    {
        use cap_std::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn create_private_directory_at(root: &Dir, path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use cap_std::fs::{DirBuilder, DirBuilderExt};
        let mut builder = DirBuilder::new();
        builder.mode(0o700);
        root.create_dir_with(path, &builder)?;
    }
    #[cfg(not(unix))]
    root.create_dir(path)?;
    Ok(())
}

fn create_private_file_at(root: &Dir, path: &Path, read: bool) -> Result<File> {
    let file = root.open_with(path, &nofollow_options(read, true, true))?;
    Ok(file.into_std())
}

fn write_private_file_at(root: &Dir, path: &Path, payload: &[u8]) -> Result<()> {
    let mut file = create_private_file_at(root, path, false)?;
    file.write_all(payload)?;
    file.sync_all()?;
    Ok(())
}

fn sync_directory_handle(directory: &Dir) -> Result<()> {
    let mut options = nofollow_options(true, false, false);
    options._cap_fs_ext_maybe_dir(true);
    directory.open_with(".", &options)?.into_std().sync_all()?;
    Ok(())
}

fn open_direct_regular_at(root: &Dir, path: &Path, limit: u64, artifact: &str) -> Result<File> {
    let file = root
        .open_with(path, &nofollow_options(true, false, false))
        .with_context(|| format!("cannot open {artifact}"))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("cannot inspect {artifact}"))?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err(InvalidArgumentError::new(format!(
            "{artifact} exceeds its byte limit or is not a direct regular file"
        ))
        .into());
    }
    Ok(file.into_std())
}

fn open_direct_directory_at(root: &Dir, path: &Path) -> Result<Dir> {
    let mut options = nofollow_options(true, false, false);
    options._cap_fs_ext_maybe_dir(true);
    let file = root.open_with(path, &options)?.into_std();
    if !file.metadata()?.is_dir() {
        bail!("filesystem object is not a direct directory");
    }
    Ok(Dir::from_std_file(file))
}

fn source_fingerprint(file: &File) -> Result<SourceFingerprint> {
    let metadata = file
        .metadata()
        .context("cannot inspect training export source")?;
    if !metadata.is_file() {
        return Err(ConcurrentModificationError::new(
            "training export source is no longer a regular file",
        )
        .into());
    }
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        SourceIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        }
    };
    #[cfg(not(unix))]
    let identity = SourceIdentity {
        created: metadata.created().ok(),
    };
    Ok(SourceFingerprint {
        identity,
        len: metadata.len(),
        modified: metadata.modified().ok(),
    })
}

fn directory_identity(directory: &Dir) -> Result<DirectoryIdentity> {
    let metadata = directory
        .try_clone()?
        .into_std_file()
        .metadata()
        .context("cannot inspect pinned directory")?;
    if !metadata.is_dir() {
        bail!("pinned filesystem object is no longer a directory");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(DirectoryIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        Ok(DirectoryIdentity {
            created: metadata.created().ok(),
        })
    }
}

#[cfg(unix)]
fn validate_private_directory_owner(directory: &Dir) -> Result<()> {
    use std::os::unix::fs::MetadataExt;
    let metadata = directory
        .try_clone()?
        .into_std_file()
        .metadata()
        .context("cannot inspect private training export directory")?;
    if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o077 != 0 {
        bail!("private training export directory has unsafe ownership or permissions");
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_private_directory_owner(_directory: &Dir) -> Result<()> {
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
fn publish_noreplace(
    from_parent: &Dir,
    from: &Path,
    to_parent: &Dir,
    to: &Path,
) -> std::io::Result<()> {
    rustix::fs::renameat_with(
        from_parent,
        from,
        to_parent,
        to,
        rustix::fs::RenameFlags::NOREPLACE,
    )?;
    Ok(())
}

fn atomic_training_publish_error(output: &Path, error: std::io::Error) -> anyhow::Error {
    if rename_noreplace_is_unsupported(&error) {
        anyhow::anyhow!(
            "filesystem containing {} does not support atomic no-replace publication; no partial bundle was published; choose an output on a local filesystem such as /tmp and move it after verification: {error}",
            output.display()
        )
    } else {
        anyhow::Error::new(error).context("cannot atomically publish training bundle")
    }
}

#[cfg(windows)]
fn publish_noreplace(
    from_parent: &Dir,
    from: &Path,
    to_parent: &Dir,
    to: &Path,
) -> std::io::Result<()> {
    from_parent.rename(from, to_parent, to)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    windows
)))]
fn publish_noreplace(
    _from_parent: &Dir,
    _from: &Path,
    _to_parent: &Dir,
    _to: &Path,
) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "atomic no-replace publication is unsupported on this platform",
    ))
}

fn verify_bundle_files(
    root: &Dir,
    manifest: &TrainingBundleManifest,
    manifest_payload: &[u8],
) -> Result<()> {
    let expected = manifest
        .shards
        .iter()
        .map(|shard| (shard.path.as_str(), shard))
        .collect::<BTreeMap<_, _>>();
    let expected_directories = manifest
        .shards
        .iter()
        .map(|shard| shard.split.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen_paths = BTreeSet::new();
    let mut seen_directories = BTreeSet::new();
    let mut saw_manifest = false;
    let mut root_entries = 0usize;
    let mut verified_bytes = 0u64;
    let mut verification = BundleVerification {
        seen: BTreeSet::new(),
        facts: Vec::new(),
        ranges: BTreeMap::new(),
    };
    for entry in root
        .entries()
        .context("cannot read training bundle directory")?
    {
        let entry = entry?;
        let name = entry.file_name();
        if name == "datajig.bundle.json" {
            let mut file = open_entry_direct(&entry, false, "training bundle manifest")?;
            let mut current = Vec::with_capacity(manifest_payload.len());
            Read::by_ref(&mut file)
                .take(MAX_TRAINING_BUNDLE_MANIFEST_BYTES as u64 + 1)
                .read_to_end(&mut current)?;
            if current != manifest_payload {
                bail!("training bundle manifest changed during verification");
            }
            saw_manifest = true;
            continue;
        }
        let split = name.to_str().context("training bundle path is not UTF-8")?;
        if !expected_directories.contains(split) || !seen_directories.insert(split.to_owned()) {
            bail!("training bundle contains an unexpected directory");
        }
        root_entries += 1;
        if root_entries > MAX_TRAINING_SPLITS {
            bail!("training bundle contains too many directories");
        }
        let split_dir = open_entry_directory_direct(&entry, "training split directory")?;
        let expected_split_shards = manifest
            .shards
            .iter()
            .filter(|shard| shard.split == split)
            .count();
        let mut split_entries = 0usize;
        for shard_entry in split_dir.entries()? {
            let shard_entry = shard_entry?;
            let shard_name = shard_entry.file_name();
            let shard_name = shard_name
                .to_str()
                .context("training shard path is not UTF-8")?;
            split_entries += 1;
            if split_entries > expected_split_shards || split_entries > MAX_TRAINING_SHARDS {
                bail!("training bundle contains unexpected shard files");
            }
            let relative = format!("{split}/{shard_name}");
            let shard = expected
                .get(relative.as_str())
                .context("training bundle contains an unexpected shard")?;
            if !seen_paths.insert(relative) {
                bail!("training bundle contains duplicate shard paths");
            }
            let file = open_entry_direct(&shard_entry, false, "training shard")?;
            verify_shard_stream(
                file,
                shard,
                &mut verified_bytes,
                &manifest.source,
                manifest.view.as_ref(),
                &manifest.config,
                &mut verification,
            )?;
        }
    }
    if !saw_manifest
        || seen_paths.len() != expected.len()
        || seen_directories.len() != expected_directories.len()
        || verified_bytes != manifest.bytes
    {
        bail!("training bundle files do not match its manifest");
    }
    let mut previous_by_split = BTreeMap::<&str, TrainingRecordOrder>::new();
    for shard in &manifest.shards {
        let (first, last) = verification
            .ranges
            .get(&(shard.split.clone(), shard.index))
            .context("training bundle is missing a verified shard order range")?;
        if previous_by_split
            .get(shard.split.as_str())
            .is_some_and(|previous| previous >= first)
        {
            bail!("training bundle records are not in deterministic order");
        }
        previous_by_split.insert(&shard.split, *last);
    }
    if let Some(expected) = manifest.view.as_ref() {
        let actual = JsonlSubsetViewBinding::new(
            &manifest.source,
            expected.recipe().clone(),
            expected.source_records(),
            verification.facts,
        )?;
        if actual != *expected {
            bail!("training bundle records do not match its subset view");
        }
    }
    Ok(())
}

type TrainingRecordOrder = ([u8; 32], [u8; 32]);

struct BundleVerification {
    seen: BTreeSet<[u8; 32]>,
    facts: Vec<JsonlViewRecordFact>,
    ranges: BTreeMap<(String, usize), (TrainingRecordOrder, TrainingRecordOrder)>,
}

fn load_bounded_regular_at(root: &Dir, path: &Path, limit: u64, artifact: &str) -> Result<Vec<u8>> {
    let file = open_direct_regular_at(root, path, limit, artifact)?;
    let metadata = file
        .metadata()
        .with_context(|| format!("cannot inspect {artifact}"))?;
    if !metadata.is_file() || metadata.len() > limit {
        bail!("{artifact} exceeds its byte limit or is not regular");
    }
    let mut payload = Vec::with_capacity(metadata.len() as usize);
    file.take(limit + 1).read_to_end(&mut payload)?;
    if payload.len() as u64 > limit {
        bail!("{artifact} exceeds its byte limit");
    }
    Ok(payload)
}

fn open_entry_direct(
    entry: &cap_std::fs::DirEntry,
    maybe_directory: bool,
    artifact: &str,
) -> Result<File> {
    let mut options = nofollow_options(true, false, false);
    options._cap_fs_ext_maybe_dir(maybe_directory);
    let file = entry
        .open_with(&options)
        .with_context(|| format!("cannot open {artifact}"))?
        .into_std();
    let metadata = file
        .metadata()
        .with_context(|| format!("cannot inspect {artifact}"))?;
    if (!maybe_directory && !metadata.is_file()) || (maybe_directory && !metadata.is_dir()) {
        bail!("{artifact} must be a direct regular filesystem object");
    }
    Ok(file)
}

fn open_entry_directory_direct(entry: &cap_std::fs::DirEntry, artifact: &str) -> Result<Dir> {
    Ok(Dir::from_std_file(open_entry_direct(
        entry, true, artifact,
    )?))
}

fn verify_shard_stream(
    file: File,
    shard: &TrainingShard,
    verified_bytes: &mut u64,
    source: &TrainingSourceBinding,
    view: Option<&JsonlSubsetViewBinding>,
    config: &TrainingExportConfig,
    verification: &mut BundleVerification,
) -> Result<()> {
    let metadata = file.metadata().context("cannot inspect training shard")?;
    if metadata.len() != shard.bytes || shard.bytes > MAX_TRAINING_SHARD_BYTES {
        bail!("training shard does not match its manifest descriptor");
    }
    *verified_bytes = verified_bytes
        .checked_add(shard.bytes)
        .context("training bundle byte count overflow")?;
    if *verified_bytes > MAX_TRAINING_BUNDLE_BYTES {
        bail!("training bundle exceeds its aggregate byte limit");
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-training-shard-v1\0");
    let mut records = 0usize;
    let mut bytes = 0u64;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    let mut first_order = None;
    let mut previous_order = None;
    loop {
        line.clear();
        let read = Read::by_ref(&mut reader)
            .take((MAX_JSONL_LINE_BYTES + 2) as u64)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        bytes += read as u64;
        if bytes > shard.bytes {
            bail!("training shard exceeds its declared byte count");
        }
        if line.len() > MAX_JSONL_LINE_BYTES + 1 || !line.ends_with(b"\n") {
            bail!("training shard contains an invalid JSONL record");
        }
        hasher.update(&line);
        records += 1;
        if records > shard.records {
            bail!("training shard exceeds its declared record count");
        }
        let content = &line[..line.len() - 1];
        if content.ends_with(b"\r") {
            bail!("training shard does not use canonical LF JSONL framing");
        }
        let text = std::str::from_utf8(content)
            .context("training shard contains non-UTF-8 record data")?;
        let value: serde_json::Value =
            serde_json::from_str(text).context("training shard contains invalid JSON")?;
        let object = value
            .as_object()
            .context("training shard record is not an object")?;
        let rid = object
            .get(source.id_field())
            .and_then(jsonl_record_id_digest)
            .context("training shard record has an invalid identity")?;
        if !verification.seen.insert(rid) {
            bail!("training bundle contains duplicate record identifiers");
        }
        let key = training_record_key(config.seed.as_bytes(), rid);
        let split_index = choose_split(&key, &config.splits);
        if config.splits[split_index].name != shard.split {
            bail!("training shard record is assigned to the wrong split");
        }
        let order = (key, rid);
        if previous_order.is_some_and(|previous| previous >= order) {
            bail!("training shard records are not in deterministic order");
        }
        first_order.get_or_insert(order);
        previous_order = Some(order);
        if let Some(view) = view {
            if !view.recipe().matches(object) || !view.recipe().includes_sample(rid) {
                bail!("training shard record does not match its subset recipe");
            }
            verification.facts.push(JsonlViewRecordFact::new(
                rid,
                canonical_record_digest(&value),
            ));
        }
    }
    let shard_id = format!("shard_{}", hasher.finalize().to_hex());
    if bytes != shard.bytes || records != shard.records || shard_id != shard.shard_id {
        bail!("training shard does not match its manifest descriptor");
    }
    let range = (
        first_order.context("training shard contains no records")?,
        previous_order.context("training shard contains no records")?,
    );
    if verification
        .ranges
        .insert((shard.split.clone(), shard.index), range)
        .is_some()
    {
        bail!("training bundle contains duplicate shard order ranges");
    }
    Ok(())
}

fn valid_split_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn valid_shard_path(path: &str, split: &str, index: usize) -> bool {
    let expected = format!("{split}/part-{index:05}.jsonl");
    path == expected
        && Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_split_normalization_uses_a_stable_largest_remainder() {
        let config = TrainingExportConfig::new(
            "seed".into(),
            &["c=1".into(), "a=1".into(), "b=1".into()],
            1,
            1,
        )
        .unwrap();

        assert_eq!(
            vec![("a", 3334), ("b", 3333), ("c", 3333)],
            config
                .splits
                .iter()
                .map(|split| (split.name.as_str(), split.weight))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn normalization_distributes_remainders_before_rejecting_zero_allocations() {
        let config = TrainingExportConfig::new(
            "seed".into(),
            &["tiny=1".into(), "large=10000".into()],
            1,
            1,
        )
        .unwrap();

        assert_eq!(
            vec![("large", 9999), ("tiny", 1)],
            config
                .splits
                .iter()
                .map(|split| (split.name.as_str(), split.weight))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn unsupported_atomic_directory_publication_explains_the_safe_recovery() {
        let error = atomic_training_publish_error(
            Path::new("/project/bundle"),
            std::io::Error::from_raw_os_error(22),
        );
        let message = format!("{error:#}");

        assert!(message.contains("/project/bundle"));
        assert!(message.contains("does not support atomic no-replace publication"));
        assert!(message.contains("/tmp"));
        assert!(message.contains("no partial bundle was published"));
    }
}
