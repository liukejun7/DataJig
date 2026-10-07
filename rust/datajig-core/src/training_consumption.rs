use crate::InvalidArgumentError;
use crate::identity::{ARTIFACT_NAMESPACE, blake3_content_id};
use crate::io::save_new_file_atomically;
use crate::strict_json::reject_duplicate_json_members;
use crate::training_bundle::{
    TrainingConsumerPlan, TrainingConsumerShard, TrainingSourceBinding,
    inspect_training_bundle_with_consumer,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

pub const TRAINING_CONSUMPTION_PLAN_SCHEMA_VERSION: u8 = 1;
pub const TRAINING_CONSUMPTION_RECEIPT_SCHEMA_VERSION: u8 = 1;
pub const TRAINING_CONSUMPTION_CLAIM: &str =
    "all_verified_split_records_crossed_adapter_boundary_at_least_once";
const MAX_CONSUMPTION_PLAN_BYTES: usize = 4 * 1024 * 1024;
const MAX_RUN_ID_BYTES: usize = 256;

#[derive(Debug)]
pub struct ConsumptionNotAuthorizedError;

impl fmt::Display for ConsumptionNotAuthorizedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("accepted training consumption plan identity does not match")
    }
}

impl Error for ConsumptionNotAuthorizedError {}

#[derive(Debug)]
pub struct StaleConsumptionInputError;

impl fmt::Display for StaleConsumptionInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("training consumption bundle or split changed after planning")
    }
}

impl Error for StaleConsumptionInputError {}

#[derive(Clone, Debug, Serialize)]
pub struct TrainingConsumptionPlanArtifact {
    pub schema_version: u8,
    pub consumption_plan_id: String,
    pub run_id: String,
    pub consumer: String,
    pub bundle_id: String,
    pub split: String,
    pub records: usize,
    pub bytes: u64,
    pub shards: usize,
    pub plan: String,
    pub run_dir: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct TrainingConsumptionRuntime {
    pub schema_version: u8,
    pub verified: bool,
    pub consumption_plan_id: String,
    pub run_id: String,
    pub consumer: String,
    pub manifest_path: String,
    pub run_dir: String,
    pub plan_path: String,
    pub bundle_id: String,
    pub source: TrainingSourceBinding,
    pub split: String,
    pub records: usize,
    pub bytes: u64,
    pub shards: Vec<TrainingConsumerShard>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct TrainingConsumptionPlan {
    namespace: String,
    kind: String,
    schema_version: u8,
    consumption_plan_id: String,
    run_id: String,
    consumer: String,
    manifest_path: String,
    run_dir: String,
    bundle_id: String,
    source: TrainingSourceBinding,
    split: String,
    records: usize,
    bytes: u64,
    shards: Vec<TrainingConsumerShard>,
}

#[derive(Serialize)]
struct PlanIdentity<'a> {
    namespace: &'a str,
    kind: &'a str,
    schema_version: u8,
    run_id: &'a str,
    consumer: &'a str,
    manifest_path: &'a str,
    run_dir: &'a str,
    bundle_id: &'a str,
    source: &'a TrainingSourceBinding,
    split: &'a str,
    records: usize,
    bytes: u64,
    shards: &'a [TrainingConsumerShard],
}

impl TrainingConsumptionPlan {
    fn new(
        run_id: String,
        consumer: String,
        manifest_path: String,
        run_dir: String,
        bundle: TrainingConsumerPlan,
        split: String,
    ) -> Result<Self> {
        let selected = bundle
            .splits
            .into_iter()
            .next()
            .context("training consumption requires one selected split")?;
        if selected.name != split
            || selected.records == 0
            || selected.shards.is_empty()
            || bundle.records != selected.records
            || bundle.bytes != selected.bytes
        {
            return Err(InvalidArgumentError::new(
                "training consumption split must contain verified records",
            )
            .into());
        }
        let mut value = Self {
            namespace: ARTIFACT_NAMESPACE.into(),
            kind: "training_consumption_plan".into(),
            schema_version: TRAINING_CONSUMPTION_PLAN_SCHEMA_VERSION,
            consumption_plan_id: String::new(),
            run_id,
            consumer,
            manifest_path,
            run_dir,
            bundle_id: bundle.bundle_id,
            source: bundle.source,
            split,
            records: selected.records,
            bytes: selected.bytes,
            shards: selected.shards,
        };
        value.validate_fields()?;
        value.consumption_plan_id = value.compute_id()?;
        Ok(value)
    }

    fn identity(&self) -> PlanIdentity<'_> {
        PlanIdentity {
            namespace: &self.namespace,
            kind: &self.kind,
            schema_version: self.schema_version,
            run_id: &self.run_id,
            consumer: &self.consumer,
            manifest_path: &self.manifest_path,
            run_dir: &self.run_dir,
            bundle_id: &self.bundle_id,
            source: &self.source,
            split: &self.split,
            records: self.records,
            bytes: self.bytes,
            shards: &self.shards,
        }
    }

    fn compute_id(&self) -> Result<String> {
        Ok(blake3_content_id(
            "consume",
            b"datajig-training-consumption-plan-v1\0",
            &serde_json::to_vec(&self.identity())?,
        ))
    }

    fn validate(&self) -> Result<()> {
        self.validate_fields()?;
        if self.consumption_plan_id != self.compute_id()? {
            bail!("training consumption plan identity does not match its content");
        }
        Ok(())
    }

    fn validate_fields(&self) -> Result<()> {
        if self.namespace != ARTIFACT_NAMESPACE
            || self.kind != "training_consumption_plan"
            || self.schema_version != TRAINING_CONSUMPTION_PLAN_SCHEMA_VERSION
        {
            bail!("unsupported training consumption plan identity or schema");
        }
        validate_run_id(&self.run_id)?;
        validate_consumer(&self.consumer)?;
        if self.records == 0 || self.bytes == 0 || self.shards.is_empty() {
            bail!("training consumption plan must contain records and shards");
        }
        if self.shards.iter().any(|shard| shard.split != self.split) {
            bail!("training consumption plan contains a shard from another split");
        }
        Ok(())
    }

    fn runtime(&self, plan_path: &Path) -> TrainingConsumptionRuntime {
        TrainingConsumptionRuntime {
            schema_version: self.schema_version,
            verified: true,
            consumption_plan_id: self.consumption_plan_id.clone(),
            run_id: self.run_id.clone(),
            consumer: self.consumer.clone(),
            manifest_path: self.manifest_path.clone(),
            run_dir: self.run_dir.clone(),
            plan_path: plan_path.to_string_lossy().into_owned(),
            bundle_id: self.bundle_id.clone(),
            source: self.source.clone(),
            split: self.split.clone(),
            records: self.records,
            bytes: self.bytes,
            shards: self.shards.clone(),
        }
    }
}

pub fn plan_training_consumption(
    manifest: &Path,
    split: &str,
    consumer: &str,
    run_id: &str,
    run_dir: &Path,
    plan_path: &Path,
) -> Result<TrainingConsumptionPlanArtifact> {
    plan_training_consumption_for_publication(
        manifest, split, consumer, run_id, run_dir, plan_path, None,
    )
}

pub fn plan_training_consumption_for_publication(
    manifest: &Path,
    split: &str,
    consumer: &str,
    run_id: &str,
    run_dir: &Path,
    plan_path: &Path,
    published_manifest: Option<&Path>,
) -> Result<TrainingConsumptionPlanArtifact> {
    validate_run_id(run_id)?;
    validate_consumer(consumer)?;
    let info =
        inspect_training_bundle_with_consumer(manifest, true, true, None, None, None, Some(split))?;
    let manifest_path = canonical_regular_file(manifest, "training bundle manifest")?;
    let bound_manifest_path = match published_manifest {
        Some(path) => validate_planned_absolute_path(path, "published training bundle manifest")?,
        None => manifest_path.clone(),
    };
    let run_dir = resolve_new_path(run_dir, "training consumption run")?;
    let plan_path = resolve_new_path(plan_path, "training consumption plan")?;
    if run_dir == plan_path || run_dir == bound_manifest_path || plan_path == bound_manifest_path {
        return Err(InvalidArgumentError::new(
            "training consumption manifest, run, and plan paths must be distinct",
        )
        .into());
    }
    if fs::symlink_metadata(&run_dir).is_ok() {
        return Err(InvalidArgumentError::new(
            "training consumption run directory must not already exist",
        )
        .into());
    }
    if fs::symlink_metadata(&plan_path).is_ok() {
        return Err(
            InvalidArgumentError::new("training consumption plan must name a new file").into(),
        );
    }
    let consumer_plan = info
        .consumer_plan
        .context("verified training bundle did not produce a consumer plan")?;
    let plan = TrainingConsumptionPlan::new(
        run_id.into(),
        consumer.into(),
        bound_manifest_path.to_string_lossy().into_owned(),
        run_dir.to_string_lossy().into_owned(),
        consumer_plan,
        split.into(),
    )?;
    let payload = serde_json::to_vec_pretty(&plan)?;
    if payload.len() > MAX_CONSUMPTION_PLAN_BYTES {
        bail!("training consumption plan exceeds {MAX_CONSUMPTION_PLAN_BYTES} bytes");
    }
    save_new_file_atomically(&plan_path, &payload, "training consumption plan")?;
    Ok(TrainingConsumptionPlanArtifact {
        schema_version: plan.schema_version,
        consumption_plan_id: plan.consumption_plan_id,
        run_id: plan.run_id,
        consumer: plan.consumer,
        bundle_id: plan.bundle_id,
        split: plan.split,
        records: plan.records,
        bytes: plan.bytes,
        shards: plan.shards.len(),
        plan: plan_path.to_string_lossy().into_owned(),
        run_dir: plan.run_dir,
    })
}

fn validate_planned_absolute_path(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(InvalidArgumentError::new(format!(
            "{label} must be an absolute normalized path without '..'"
        ))
        .into());
    }
    Ok(path.to_path_buf())
}

pub fn inspect_training_consumption(
    plan_path: &Path,
    accept_plan: &str,
) -> Result<TrainingConsumptionRuntime> {
    let plan_path = canonical_regular_file(plan_path, "training consumption plan")?;
    let plan = load_plan(&plan_path)?;
    if plan.consumption_plan_id != accept_plan {
        return Err(ConsumptionNotAuthorizedError.into());
    }
    let live = inspect_training_bundle_with_consumer(
        Path::new(&plan.manifest_path),
        true,
        true,
        Some(&plan.bundle_id),
        Some(plan.source.revision_id()),
        Some(plan.source.assurance()),
        Some(&plan.split),
    )
    .map_err(|_| anyhow::Error::new(StaleConsumptionInputError))?;
    let live_plan = live
        .consumer_plan
        .ok_or_else(|| anyhow::Error::new(StaleConsumptionInputError))?;
    let expected = TrainingConsumptionPlan::new(
        plan.run_id.clone(),
        plan.consumer.clone(),
        plan.manifest_path.clone(),
        plan.run_dir.clone(),
        live_plan,
        plan.split.clone(),
    )
    .map_err(|_| anyhow::Error::new(StaleConsumptionInputError))?;
    if expected.consumption_plan_id != plan.consumption_plan_id {
        return Err(StaleConsumptionInputError.into());
    }
    Ok(plan.runtime(&plan_path))
}

fn load_plan(path: &Path) -> Result<TrainingConsumptionPlan> {
    let payload = read_bounded(
        path,
        MAX_CONSUMPTION_PLAN_BYTES,
        "training consumption plan",
    )?;
    let text =
        std::str::from_utf8(&payload).context("training consumption plan is not valid UTF-8")?;
    reject_duplicate_json_members(text).context("invalid training consumption plan")?;
    let plan: TrainingConsumptionPlan =
        serde_json::from_str(text).context("invalid training consumption plan")?;
    plan.validate()?;
    Ok(plan)
}

fn validate_consumer(consumer: &str) -> Result<()> {
    if !matches!(consumer, "python" | "pytorch" | "huggingface") {
        return Err(InvalidArgumentError::new(
            "training consumption consumer must be python, pytorch, or huggingface",
        )
        .into());
    }
    Ok(())
}

fn validate_run_id(run_id: &str) -> Result<()> {
    if run_id.is_empty() || run_id.len() > MAX_RUN_ID_BYTES || run_id.chars().any(char::is_control)
    {
        return Err(InvalidArgumentError::new(format!(
            "training consumption run ID must contain 1 to {MAX_RUN_ID_BYTES} non-control UTF-8 bytes"
        ))
        .into());
    }
    Ok(())
}

fn read_bounded(path: &Path, maximum: usize, label: &str) -> Result<Vec<u8>> {
    let file = File::open(path).with_context(|| format!("cannot open {label}"))?;
    let metadata = file.metadata()?;
    if metadata.len() > maximum as u64 {
        bail!("{label} exceeds {maximum} bytes");
    }
    let mut payload = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum as u64 + 1).read_to_end(&mut payload)?;
    if payload.len() > maximum {
        bail!("{label} exceeds {maximum} bytes");
    }
    Ok(payload)
}

fn canonical_regular_file(path: &Path, label: &str) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(path).with_context(|| format!("cannot inspect {label}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(InvalidArgumentError::new(format!("{label} must be a regular file")).into());
    }
    path.canonicalize()
        .with_context(|| format!("cannot resolve {label}"))
}

fn resolve_new_path(path: &Path, label: &str) -> Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| format!("cannot create {label} parent"))?;
    let parent = parent
        .canonicalize()
        .with_context(|| format!("cannot resolve {label} parent"))?;
    let name = path
        .file_name()
        .ok_or_else(|| InvalidArgumentError::new(format!("{label} has no file name")))?;
    Ok(parent.join(name))
}
