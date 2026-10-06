use crate::inventory::{scan_inventory, scan_inventory_for_schema};
use crate::io::{save_file_atomically, save_new_file_atomically};
use crate::jsonl_patch::prepare_jsonl_patch;
use crate::jsonl_review::{create_jsonl_review, jsonl_review_payload};
use crate::jsonl_state::build_jsonl_record_state_with_byte_limit;
use crate::remediation::create_remediation_plan_from_payload;
use crate::review::{create_review_for_schema, create_review_with_metadata};
use crate::training_bundle::{
    MAX_TRAINING_SOURCE_BYTES, inspect_subset_view, load_subset_recipe_file,
    materialize_training_bundle,
};
use crate::{
    ChangeDeclaration, ChangesetSummary, ConcurrentModificationError, DatasetChangeset,
    DatasetRevision, EmptyViewError, FindingNotFoundError, InvalidArgumentError,
    JsonlPatchPreviewArtifact, JsonlPatchRequest, JsonlQualityEvaluation, JsonlQualityPolicy,
    JsonlRecordLocation, JsonlRecordStateBundle, JsonlStructuralLocation, MAX_INVENTORY_THREADS,
    MAX_PAGE_SIZE, MAX_PHASH_THRESHOLD, MAX_REPORT_BYTES, PatchConflictError,
    PatchNotAuthorizedError, RecordChangesetSummary, RemediationPlanArtifact, ReviewReport,
    RevisionProvenance, TrainingBundleArtifact, TrainingExportConfig, TrainingSourceBinding,
    TransformInputSpec, TransformLimits, TransformSourceFormat, UndoNotFoundError,
    UnstagedChangesError, WorkspaceBusyError, WorkspaceStore, build_jsonl_record_state,
    changed_record_ids, create_remediation_plan, diff_jsonl_record_states, evaluate_jsonl_quality,
    stage_transform_sources,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
#[cfg(unix)]
use std::fs::{File, OpenOptions};
#[cfg(unix)]
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEFAULT_WORKSPACE_STATE: &str = ".datajig";
pub const MAX_WORKSPACE_BYTES: usize = 64 * 1024;
pub const MAX_REVISION_PAGE_SIZE: usize = 200;
pub const MAX_REVISION_TRAVERSAL: usize = 100_000;
const WORKSPACE_SCHEMA_VERSION: u8 = 2;
const WORKSPACE_FILE: &str = "workspace.json";
const LATEST_REPORT_FILE: &str = "latest.review.json";
const LATEST_PLAN_FILE: &str = "latest.plan.json";
const PATCH_TRANSACTION_SCHEMA_VERSION: u8 = 1;
const MAX_PATCH_JOURNAL_BYTES: usize = 256 * 1024;

pub fn check_subset_view(state: &Path, recipe_path: &Path) -> Result<ViewCheckArtifact> {
    let recipe = load_subset_recipe_file(recipe_path)?;
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter != "jsonl" {
        return Err(
            InvalidArgumentError::new("subset views require a keyed JSONL workspace").into(),
        );
    }
    let (dataset_id, head, baseline) = resolve_record_head(&state_dir, &workspace)?;
    if baseline.state().finding_count() != 0 {
        bail!("sealed JSONL HEAD contains structural findings");
    }
    let live = scan_live_record_state_with_byte_limit(&workspace, MAX_TRAINING_SOURCE_BYTES)?;
    if live.state().record_state_id() != baseline.state().record_state_id()
        || live.state().dataset_content_id() != baseline.state().dataset_content_id()
    {
        return Err(UnstagedChangesError::new(
            "live JSONL dataset differs from sealed HEAD; seal it before checking a view",
        )
        .into());
    }
    let binding = TrainingSourceBinding::new(
        dataset_id.clone(),
        head.revision_id().into(),
        baseline.state().record_state_id().into(),
        baseline.state().dataset_content_id().into(),
        workspace
            .id_field()
            .expect("validated JSONL workspace")
            .into(),
        workspace.quality_policy_id().map(str::to_owned),
        head.accepted_report_id().map(str::to_owned),
    )?;
    let view = inspect_subset_view(Path::new(workspace.dataset_path()), &binding, &recipe)?;
    Ok(ViewCheckArtifact {
        dataset_id,
        revision_id: head.revision_id().into(),
        state_id: baseline.state().record_state_id().into(),
        assurance: binding.assurance().into(),
        recipe_id: view.recipe_id().into(),
        view_id: view.view_id().into(),
        source_records: view.source_records(),
        selected_records: view.selected_records(),
        exportable: view.selected_records() != 0,
        state_dir: path_text(&state_dir, "state directory")?,
    })
}

pub fn export_training_bundle(
    state: &Path,
    output: &Path,
    seed: String,
    split_specs: &[String],
    max_shard_records: usize,
    max_shard_bytes: u64,
) -> Result<TrainingBundleArtifact> {
    export_training_bundle_with_view(
        state,
        output,
        None,
        seed,
        split_specs,
        max_shard_records,
        max_shard_bytes,
    )
}

pub fn export_training_bundle_with_view(
    state: &Path,
    output: &Path,
    view_path: Option<&Path>,
    seed: String,
    split_specs: &[String],
    max_shard_records: usize,
    max_shard_bytes: u64,
) -> Result<TrainingBundleArtifact> {
    export_training_bundle_with_view_at_revision(
        state,
        output,
        view_path,
        None,
        seed,
        split_specs,
        max_shard_records,
        max_shard_bytes,
    )
}

#[allow(clippy::too_many_arguments)]
pub fn export_training_bundle_with_view_at_revision(
    state: &Path,
    output: &Path,
    view_path: Option<&Path>,
    revision_id: Option<&str>,
    seed: String,
    split_specs: &[String],
    max_shard_records: usize,
    max_shard_bytes: u64,
) -> Result<TrainingBundleArtifact> {
    let config = TrainingExportConfig::new(seed, split_specs, max_shard_records, max_shard_bytes)?;
    let recipe = view_path.map(load_subset_recipe_file).transpose()?;
    let (state_dir, workspace) = if revision_id.is_some() {
        load_workspace_without_live_dataset(state)?
    } else {
        load_workspace_compatible(state)?
    };
    if workspace.adapter != "jsonl" {
        return Err(
            InvalidArgumentError::new("training export requires a keyed JSONL workspace").into(),
        );
    }
    let output_parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(output_parent).context("cannot create training export output parent")?;
    let output_parent = output_parent
        .canonicalize()
        .context("cannot resolve training export output parent")?;
    let output_name = output
        .file_name()
        .context("training export output must name a new directory")?;
    let resolved_output = output_parent.join(output_name);
    if resolved_output.starts_with(&state_dir) || state_dir.starts_with(&resolved_output) {
        return Err(InvalidArgumentError::new(
            "training export output must stay outside the workspace state directory",
        )
        .into());
    }
    let (dataset_id, head, baseline, source) = if let Some(revision_id) = revision_id {
        let store = WorkspaceStore::open(&state_dir)?;
        let (revision, bundle) =
            resolve_reachable_record_revision(&store, &workspace, revision_id)?;
        let source = store
            .verified_jsonl_blob_path(revision.dataset_content_id(), bundle.state().byte_count())?;
        (workspace.dataset_id.clone(), revision, bundle, source)
    } else {
        let (dataset_id, head, baseline) = resolve_record_head(&state_dir, &workspace)?;
        let live = scan_live_record_state_with_byte_limit(&workspace, MAX_TRAINING_SOURCE_BYTES)?;
        if live.state().record_state_id() != baseline.state().record_state_id()
            || live.state().dataset_content_id() != baseline.state().dataset_content_id()
        {
            return Err(UnstagedChangesError::new(
                "live JSONL dataset differs from sealed HEAD; seal it before export",
            )
            .into());
        }
        (
            dataset_id,
            head,
            baseline,
            PathBuf::from(workspace.dataset_path()),
        )
    };
    if baseline.state().finding_count() != 0 {
        bail!("sealed JSONL HEAD contains structural findings");
    }
    let binding = TrainingSourceBinding::new(
        dataset_id,
        head.revision_id().into(),
        baseline.state().record_state_id().into(),
        baseline.state().dataset_content_id().into(),
        workspace
            .id_field()
            .expect("validated JSONL workspace")
            .into(),
        workspace.quality_policy_id().map(str::to_owned),
        head.accepted_report_id().map(str::to_owned),
    )?;
    let view = recipe
        .as_ref()
        .map(|recipe| inspect_subset_view(&source, &binding, recipe))
        .transpose()?;
    if view
        .as_ref()
        .is_some_and(|view| view.selected_records() == 0)
    {
        return Err(EmptyViewError::new(
            "subset view selects no records; no training bundle was created",
        )
        .into());
    }
    materialize_training_bundle(&source, &resolved_output, &binding, view.as_ref(), &config)
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkspaceArtifact {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline_inventory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline_state_id: Option<String>,
    pub adapter: String,
    pub dataset_coverage: String,
    pub dataset_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_path: Option<String>,
    pub head_revision_id: String,
    pub state_dir: String,
    pub workspace_schema_version: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality_policy_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ViewCheckArtifact {
    pub dataset_id: String,
    pub revision_id: String,
    pub state_id: String,
    pub assurance: String,
    pub recipe_id: String,
    pub view_id: String,
    pub source_records: usize,
    pub selected_records: usize,
    pub exportable: bool,
    pub state_dir: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkspaceCheckArtifact {
    pub decision: String,
    pub findings: usize,
    pub report_content_id: String,
    pub report_path: String,
    pub status: String,
    pub dataset_coverage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changeset_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkspaceStatusArtifact {
    pub changed_files: usize,
    pub clean: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_inventory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub current_state_id: Option<String>,
    pub dataset_id: String,
    pub dataset_coverage: String,
    pub decision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_inventory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head_state_id: Option<String>,
    pub head_revision_id: String,
    pub state_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changeset_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staged_inventory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staged_state_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changed_records: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_findings: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unstaged_changes: Option<bool>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RevisionPage {
    pub has_more: bool,
    pub limit: usize,
    pub offset: usize,
    pub returned: usize,
    pub revisions: Vec<DatasetRevision>,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkspaceSealArtifact {
    pub accepted_report_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline_inventory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub baseline_state_id: Option<String>,
    pub dataset_id: String,
    pub dataset_coverage: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_path: Option<String>,
    pub parent_revision_id: String,
    pub revision_id: String,
    pub state_dir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changeset_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct MaterializeArtifact {
    pub schema_version: u8,
    pub status: String,
    pub adapter: &'static str,
    pub revision_id: String,
    pub state_id: String,
    pub dataset_content_id: String,
    pub id_field: String,
    pub bytes: u64,
    pub records: usize,
    pub output: String,
    pub content_verified: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct WorkspacePlanArtifact {
    #[serde(flatten)]
    pub plan: RemediationPlanArtifact,
    pub decision: String,
    pub state_dir: String,
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum FindingLocation {
    Record(JsonlRecordLocation),
    Structural(JsonlStructuralLocation),
}

#[derive(Clone, Debug, Serialize)]
pub struct FindingLocationPage {
    pub offset: usize,
    pub limit: usize,
    pub returned: usize,
    pub total: usize,
    pub has_more: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct FindingLocationsArtifact {
    pub report_content_id: String,
    pub finding_id: String,
    pub finding_code: String,
    pub change_id: String,
    pub changeset_id: String,
    pub candidate_state_id: String,
    pub candidate_dataset_content_id: String,
    pub evidence_truncated: bool,
    pub page: FindingLocationPage,
    pub locations: Vec<FindingLocation>,
}

#[derive(Clone, Debug, Serialize)]
pub struct JsonlPatchDraftArtifact {
    pub schema_version: u8,
    pub request_content_id: String,
    pub report_content_id: String,
    pub finding_id: String,
    pub candidate_state_id: String,
    pub output: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceConfig {
    namespace: String,
    adapter: String,
    dataset_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    dataset_root: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dataset_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    adapter_config: Option<JsonlAdapterConfig>,
    refs: String,
    schema_version: u8,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct JsonlAdapterConfig {
    id_field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    quality_policy_id: Option<String>,
}

impl WorkspaceConfig {
    fn validate(&self) -> Result<()> {
        if self.namespace != crate::identity::ARTIFACT_NAMESPACE {
            bail!("unsupported workspace namespace {}", self.namespace);
        }
        validate_dataset_id(&self.dataset_id)?;
        match self.schema_version {
            WORKSPACE_SCHEMA_VERSION => {
                if self.adapter != "imagefolder"
                    || self.dataset_path.is_some()
                    || self.adapter_config.is_some()
                {
                    bail!("schema-2 workspace descriptor is invalid");
                }
                let root = self
                    .dataset_root
                    .as_deref()
                    .context("schema-2 workspace is missing dataset_root")?;
                if root.is_empty() || root.len() > 4_096 {
                    bail!("workspace dataset root is invalid");
                }
            }
            3 => {
                if self.adapter != "jsonl" || self.dataset_root.is_some() {
                    bail!("schema-3 workspace descriptor is invalid");
                }
                let path = self
                    .dataset_path
                    .as_deref()
                    .context("schema-3 workspace is missing dataset_path")?;
                if path.is_empty() || path.len() > 4_096 {
                    bail!("workspace dataset path is invalid");
                }
                let config = self
                    .adapter_config
                    .as_ref()
                    .context("schema-3 workspace is missing adapter_config")?;
                if config.id_field.is_empty() || config.id_field.len() > 4_096 {
                    bail!("workspace JSONL id_field is invalid");
                }
                if config.quality_policy_id.is_some() {
                    bail!("schema-3 workspace cannot contain a quality policy");
                }
            }
            4 => {
                if self.adapter != "jsonl" || self.dataset_root.is_some() {
                    bail!("schema-4 workspace descriptor is invalid");
                }
                let path = self
                    .dataset_path
                    .as_deref()
                    .context("schema-4 workspace is missing dataset_path")?;
                if path.is_empty() || path.len() > 4_096 {
                    bail!("workspace dataset path is invalid");
                }
                let config = self
                    .adapter_config
                    .as_ref()
                    .context("schema-4 workspace is missing adapter_config")?;
                if config.id_field.is_empty() || config.id_field.len() > 4_096 {
                    bail!("workspace JSONL id_field is invalid");
                }
                crate::revision::validate_content_id(
                    config
                        .quality_policy_id
                        .as_deref()
                        .context("schema-4 workspace is missing quality_policy_id")?,
                    "policy",
                    "quality_policy_id",
                )?;
            }
            _ => bail!("unsupported workspace schema {}", self.schema_version),
        }
        if self.refs != "refs.json" {
            bail!("workspace refs path is invalid");
        }
        Ok(())
    }

    fn dataset_path(&self) -> &str {
        self.dataset_path
            .as_deref()
            .or(self.dataset_root.as_deref())
            .expect("validated workspace has a dataset path")
    }

    fn id_field(&self) -> Option<&str> {
        self.adapter_config
            .as_ref()
            .map(|config| config.id_field.as_str())
    }

    fn quality_policy_id(&self) -> Option<&str> {
        self.adapter_config
            .as_ref()
            .and_then(|config| config.quality_policy_id.as_deref())
    }
}

pub fn initialize_workspace(
    dataset: &Path,
    state: &Path,
    threads: usize,
) -> Result<WorkspaceArtifact> {
    validate_threads(threads)?;
    let dataset = dataset
        .canonicalize()
        .context("cannot resolve workspace dataset")?;
    if !dataset.is_dir() {
        return Err(InvalidArgumentError::new("workspace dataset is not a directory").into());
    }
    let state = safe_state_directory(state, &dataset)?;
    let state_dir = path_text(&state, "state directory")?;
    if state.join(WORKSPACE_FILE).exists() {
        return Err(InvalidArgumentError::new("workspace is already initialized").into());
    }
    fs::create_dir_all(&state).context("cannot create workspace state directory")?;
    let inventory = scan_inventory(&dataset, threads)?;
    let baseline_inventory_id = inventory.content_id()?;
    save_file_atomically(&state.join(".gitignore"), b"*\n", "workspace ignore file")?;
    let store = WorkspaceStore::open(&state)?;
    store.publish_inventory(&inventory)?;
    let revision = if state.join("refs.json").exists() {
        let refs = store.load_refs()?;
        let existing = store.load_revision(refs.head())?;
        if existing.parent().is_some()
            || existing.adapter() != "imagefolder"
            || existing.inventory_id() != baseline_inventory_id
        {
            bail!("incomplete workspace initialization does not match the current dataset");
        }
        existing
    } else {
        let provenance = RevisionProvenance::new(
            "system".into(),
            None,
            env!("CARGO_PKG_VERSION").into(),
            "Initialize dataset workspace".into(),
        )?;
        let created = DatasetRevision::new(
            None,
            "imagefolder".into(),
            baseline_inventory_id.clone(),
            baseline_inventory_id.clone(),
            None,
            provenance,
            now_unix_ns()?,
        )?;
        store.publish_revision(&created)?;
        store.compare_and_swap_head(None, created.revision_id())?;
        created
    };
    let dataset_id = dataset_id("imagefolder", &baseline_inventory_id);
    let workspace = WorkspaceConfig {
        namespace: crate::identity::ARTIFACT_NAMESPACE.into(),
        adapter: "imagefolder".into(),
        dataset_id: dataset_id.clone(),
        dataset_root: Some(path_text(&dataset, "dataset root")?),
        dataset_path: None,
        adapter_config: None,
        refs: "refs.json".into(),
        schema_version: WORKSPACE_SCHEMA_VERSION,
    };
    let payload = serde_json::to_vec_pretty(&workspace)?;
    save_file_atomically(
        &state.join(WORKSPACE_FILE),
        &payload,
        "workspace configuration",
    )?;
    Ok(WorkspaceArtifact {
        baseline_inventory_id: Some(baseline_inventory_id),
        baseline_state_id: None,
        adapter: "imagefolder".into(),
        dataset_coverage: inventory.coverage().into(),
        dataset_id,
        dataset_root: Some(workspace.dataset_path().into()),
        dataset_path: None,
        head_revision_id: revision.revision_id().to_owned(),
        state_dir,
        workspace_schema_version: WORKSPACE_SCHEMA_VERSION,
        quality_policy_id: None,
    })
}

pub fn initialize_jsonl_workspace(
    dataset: &Path,
    state: &Path,
    id_field: &str,
) -> Result<WorkspaceArtifact> {
    initialize_jsonl_workspace_with_policy(dataset, state, id_field, None)
}

pub fn initialize_jsonl_workspace_with_policy(
    dataset: &Path,
    state: &Path,
    id_field: &str,
    policy_path: Option<&Path>,
) -> Result<WorkspaceArtifact> {
    initialize_jsonl_workspace_with_receipt(dataset, state, id_field, policy_path, None)
}

struct JsonlDatasetSnapshot {
    root: PathBuf,
    path: PathBuf,
}

impl JsonlDatasetSnapshot {
    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for JsonlDatasetSnapshot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn snapshot_jsonl_dataset(dataset: &Path) -> Result<JsonlDatasetSnapshot> {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    let parent = dataset
        .parent()
        .context("workspace JSONL dataset has no parent directory")?;
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let mut root = None;
    for attempt in 0..32 {
        let candidate = parent.join(format!(
            ".datajig-snapshot-{}-{stamp}-{attempt}",
            std::process::id()
        ));
        match fs::create_dir(&candidate) {
            Ok(()) => {
                #[cfg(unix)]
                if let Err(error) =
                    fs::set_permissions(&candidate, fs::Permissions::from_mode(0o700))
                {
                    let _ = fs::remove_dir(&candidate);
                    return Err(error).context("cannot secure private dataset snapshot");
                }
                root = Some(candidate);
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("cannot create private dataset snapshot"),
        }
    }
    let root = root.context("cannot allocate private dataset snapshot")?;
    let staged = stage_transform_sources(
        &[TransformInputSpec::new(
            "dataset".into(),
            dataset.to_owned(),
            TransformSourceFormat::Jsonl,
        )?],
        &root,
        &TransformLimits::v1(),
    );
    match staged {
        Ok(staged) => Ok(JsonlDatasetSnapshot {
            path: staged[0].staged_path().to_owned(),
            root,
        }),
        Err(error) => {
            let _ = fs::remove_dir_all(root);
            Err(error).context("cannot snapshot transform output for workspace initialization")
        }
    }
}

pub fn initialize_jsonl_workspace_with_receipt(
    dataset: &Path,
    state: &Path,
    id_field: &str,
    policy_path: Option<&Path>,
    source_receipt: Option<&Path>,
) -> Result<WorkspaceArtifact> {
    let dataset = dataset
        .canonicalize()
        .context("cannot resolve workspace JSONL dataset")?;
    if !dataset.is_file() {
        return Err(InvalidArgumentError::new("JSONL workspace dataset is not a file").into());
    }
    let snapshot = source_receipt
        .map(|_| snapshot_jsonl_dataset(&dataset))
        .transpose()?;
    let inspection_dataset = snapshot
        .as_ref()
        .map_or(dataset.as_path(), JsonlDatasetSnapshot::path);
    let transform_lineage = source_receipt
        .map(|receipt| {
            crate::transform_workflow::verify_transform_receipt_snapshot(
                receipt,
                &dataset,
                inspection_dataset,
                id_field,
            )
            .and_then(|verified| crate::TransformLineage::from_verified_receipt(&verified))
        })
        .transpose()?;
    let state = safe_state_directory(state, &dataset)?;
    let state_dir = path_text(&state, "state directory")?;
    if state.join(WORKSPACE_FILE).exists() {
        return Err(InvalidArgumentError::new("workspace is already initialized").into());
    }
    let built = build_jsonl_record_state(inspection_dataset, id_field)?;
    if built.state().finding_count() != 0 {
        return Err(InvalidArgumentError::new(
            "JSONL workspace baseline must pass inspection without findings",
        )
        .into());
    }
    let policy = policy_path
        .map(crate::JsonlQualityPolicy::from_path)
        .transpose()?;
    if let Some(policy) = &policy {
        let gated_ids = (policy.mode() == "changed_only").then(BTreeSet::new);
        let evaluation = evaluate_jsonl_quality(
            inspection_dataset,
            id_field,
            built.state().dataset_content_id(),
            policy,
            gated_ids.as_ref(),
        )?;
        if policy.mode() == "full" && evaluation.gated_violations() != 0 {
            return Err(InvalidArgumentError::new(
                "JSONL workspace baseline fails its full quality policy",
            )
            .into());
        }
    }
    fs::create_dir_all(&state).context("cannot create workspace state directory")?;
    save_file_atomically(&state.join(".gitignore"), b"*\n", "workspace ignore file")?;
    let store = WorkspaceStore::open(&state)?;
    let baseline_state_id = store.publish_record_state_bundle(&built.bundle())?;
    let quality_policy_id = policy
        .as_ref()
        .map(|policy| store.publish_jsonl_quality_policy(policy))
        .transpose()?;
    store.publish_jsonl_blob(inspection_dataset, built.state().dataset_content_id())?;
    let revision = if state.join("refs.json").exists() {
        let refs = store.load_refs()?;
        let existing = store.load_revision(refs.head())?;
        if existing.parent().is_some()
            || existing.adapter() != "jsonl"
            || existing.state_id() != baseline_state_id
            || existing.dataset_content_id() != built.state().dataset_content_id()
            || existing.transform_lineage() != transform_lineage.as_ref()
        {
            bail!("incomplete JSONL workspace initialization does not match the current dataset");
        }
        existing
    } else {
        let provenance = RevisionProvenance::new(
            "system".into(),
            None,
            env!("CARGO_PKG_VERSION").into(),
            "Initialize JSONL dataset workspace".into(),
        )?;
        let created = DatasetRevision::new_record_with_lineage(
            None,
            baseline_state_id.clone(),
            built.state().dataset_content_id().into(),
            None,
            provenance,
            transform_lineage,
            now_unix_ns()?,
        )?;
        store.publish_revision(&created)?;
        let final_built = build_jsonl_record_state(&dataset, id_field)?;
        if final_built.state().record_state_id() != built.state().record_state_id() {
            return Err(ConcurrentModificationError::new(
                "workspace JSONL dataset changed while it was being initialized",
            )
            .into());
        }
        store.compare_and_swap_head(None, created.revision_id())?;
        created
    };
    let dataset_id = quality_policy_id.as_deref().map_or_else(
        || record_dataset_id(id_field, &baseline_state_id),
        |policy_id| record_dataset_id_with_policy(id_field, &baseline_state_id, policy_id),
    );
    let workspace = WorkspaceConfig {
        namespace: crate::identity::ARTIFACT_NAMESPACE.into(),
        adapter: "jsonl".into(),
        dataset_id: dataset_id.clone(),
        dataset_root: None,
        dataset_path: Some(path_text(&dataset, "dataset path")?),
        adapter_config: Some(JsonlAdapterConfig {
            id_field: id_field.into(),
            quality_policy_id: quality_policy_id.clone(),
        }),
        refs: "refs.json".into(),
        schema_version: if quality_policy_id.is_some() { 4 } else { 3 },
    };
    workspace.validate()?;
    save_file_atomically(
        &state.join(WORKSPACE_FILE),
        &serde_json::to_vec_pretty(&workspace)?,
        "workspace configuration",
    )?;
    Ok(WorkspaceArtifact {
        baseline_inventory_id: None,
        baseline_state_id: Some(baseline_state_id),
        adapter: "jsonl".into(),
        dataset_coverage: "records_all_v1".into(),
        dataset_id,
        dataset_root: None,
        dataset_path: Some(workspace.dataset_path().into()),
        head_revision_id: revision.revision_id().into(),
        state_dir,
        workspace_schema_version: workspace.schema_version,
        quality_policy_id,
    })
}

pub fn check_workspace(
    state: &Path,
    threads: usize,
    phash_threshold: usize,
) -> Result<WorkspaceCheckArtifact> {
    validate_threads(threads)?;
    if phash_threshold > MAX_PHASH_THRESHOLD {
        return Err(InvalidArgumentError::new(format!(
            "phash threshold must be between 0 and {MAX_PHASH_THRESHOLD}"
        ))
        .into());
    }
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter == "jsonl" {
        let mut error =
            InvalidArgumentError::new("JSONL workspace check requires --change and --changeset");
        if let Ok((change_id, changeset_id)) =
            resolve_changeset_selectors_in_workspace(&state_dir, &workspace, "@active", "@latest")
        {
            error = InvalidArgumentError::new(format!(
                "JSONL workspace check requires --change and --changeset; the unique active binding is --change {change_id} --changeset {changeset_id}"
            ))
            .with_remediation(
                "Use the unique active change and latest staged changeset.",
                "check",
                vec![
                    "--state".into(),
                    path_text(&state_dir, "state directory")?,
                    "--change".into(),
                    "@active".into(),
                    "--changeset".into(),
                    "@latest".into(),
                ],
            );
        }
        return Err(error.into());
    }
    let (_dataset_id, head, baseline_inventory) = resolve_head(&state_dir, &workspace)?;
    let baseline = WorkspaceStore::open(&state_dir)?.inventory_path(head.inventory_id())?;
    let report = state_dir.join(LATEST_REPORT_FILE);
    let artifact = create_review_for_schema(
        &format!("inventory:{}", baseline.to_string_lossy()),
        &format!("path:{}", workspace.dataset_path()),
        &report,
        threads,
        phash_threshold,
        baseline_inventory.schema_version(),
    )?;
    let decision = match artifact.status.as_str() {
        "pass" => "seal",
        "warn" => "inspect",
        "fail" => "fix",
        _ => "retry",
    };
    Ok(WorkspaceCheckArtifact {
        change_id: None,
        changeset_id: None,
        decision: decision.into(),
        findings: artifact.findings,
        report_content_id: artifact.content_id,
        report_path: artifact.output,
        status: artifact.status,
        dataset_coverage: baseline_inventory.coverage().into(),
    })
}

pub fn status_workspace(state: &Path, threads: usize) -> Result<WorkspaceStatusArtifact> {
    validate_threads(threads)?;
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter == "jsonl" {
        return status_jsonl_workspace(&state_dir, &workspace);
    }
    let (dataset_id, head, baseline) = resolve_head(&state_dir, &workspace)?;
    let current = scan_inventory_for_schema(
        Path::new(workspace.dataset_path()),
        threads,
        baseline.schema_version(),
    )?;
    let head_inventory_id = baseline.content_id()?;
    let current_inventory_id = current.content_id()?;
    let clean = head_inventory_id == current_inventory_id;
    let changed_files = if clean {
        0
    } else {
        count_changed_files(&baseline, &current).max(1)
    };
    Ok(WorkspaceStatusArtifact {
        changed_files,
        clean,
        current_inventory_id: Some(current_inventory_id),
        current_state_id: None,
        dataset_id,
        dataset_coverage: baseline.coverage().into(),
        decision: if clean { "clean" } else { "review" }.into(),
        head_inventory_id: Some(head_inventory_id),
        head_state_id: None,
        head_revision_id: head.revision_id().to_owned(),
        state_dir: path_text(&state_dir, "state directory")?,
        change_id: None,
        changeset_id: None,
        staged_inventory_id: None,
        staged_state_id: None,
        changed_records: None,
        validation_findings: None,
        unstaged_changes: None,
    })
}

pub(crate) fn workspace_dataset_path(state: &Path) -> Result<PathBuf> {
    let (_, workspace) = load_workspace_compatible(state)?;
    Ok(PathBuf::from(workspace.dataset_path()))
}

pub fn begin_changeset(
    state: &Path,
    threads: usize,
    intent: &str,
    external_task_id: &str,
    actor_kind: &str,
) -> Result<ChangeDeclaration> {
    validate_threads(threads)?;
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter == "jsonl" {
        return begin_jsonl_changeset(&state_dir, &workspace, intent, external_task_id, actor_kind);
    }
    let (dataset_id, head, baseline) = resolve_head(&state_dir, &workspace)?;
    let current = scan_inventory_for_schema(
        Path::new(workspace.dataset_path()),
        threads,
        baseline.schema_version(),
    )?;
    if current.content_id()? != baseline.content_id()? {
        return Err(InvalidArgumentError::new(
            "changeset begin requires a clean workspace; seal or restore current edits first",
        )
        .into());
    }
    let change = ChangeDeclaration::new(
        dataset_id,
        workspace.adapter.clone(),
        head.revision_id().into(),
        head.inventory_id().into(),
        intent.into(),
        external_task_id.into(),
        actor_kind.into(),
        env!("CARGO_PKG_VERSION").into(),
    )?;
    WorkspaceStore::open(&state_dir)?.publish_change_declaration(&change)?;
    Ok(change)
}

pub fn stage_changeset(state: &Path, threads: usize, change_id: &str) -> Result<DatasetChangeset> {
    validate_threads(threads)?;
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter == "jsonl" {
        return stage_jsonl_changeset(&state_dir, &workspace, change_id);
    }
    let store = WorkspaceStore::open(&state_dir)?;
    let change = store.load_change_declaration(change_id)?;
    let (dataset_id, head, baseline) = resolve_head(&state_dir, &workspace)?;
    validate_change_anchor(&change, &workspace, &dataset_id, &head, &baseline)?;
    let candidate = scan_inventory_for_schema(
        Path::new(workspace.dataset_path()),
        threads,
        baseline.schema_version(),
    )?;
    let candidate_id = candidate.content_id()?;
    let summary = ChangesetSummary::derive(&baseline, &candidate);
    if summary.total() == 0 || candidate_id == change.base_inventory_id() {
        return Err(InvalidArgumentError::new("cannot stage an empty dataset changeset").into());
    }
    store.publish_inventory(&candidate)?;
    let changeset = DatasetChangeset::new(
        change.change_id().into(),
        dataset_id,
        workspace.adapter.clone(),
        head.revision_id().into(),
        head.inventory_id().into(),
        candidate_id,
        summary,
        baseline.coverage().into(),
    )?;
    store.publish_changeset(&changeset)?;
    Ok(changeset)
}

pub fn resolve_changeset_selectors(
    state: &Path,
    change_selector: &str,
    changeset_selector: &str,
) -> Result<(String, String)> {
    if !is_context_selector(change_selector) && !is_context_selector(changeset_selector) {
        return Ok((change_selector.into(), changeset_selector.into()));
    }
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    resolve_changeset_selectors_in_workspace(
        &state_dir,
        &workspace,
        change_selector,
        changeset_selector,
    )
}

pub fn resolve_optional_changeset_context(
    state: &Path,
    change_selector: Option<&str>,
    changeset_selector: Option<&str>,
) -> Result<Option<(String, String)>> {
    match (change_selector, changeset_selector) {
        (Some(change), Some(changeset)) => {
            resolve_changeset_selectors(state, change, changeset).map(Some)
        }
        (None, None) => {
            let (state_dir, workspace) = load_workspace_compatible(state)?;
            if workspace.adapter != "jsonl" {
                return Ok(None);
            }
            resolve_changeset_selectors_in_workspace(&state_dir, &workspace, "@active", "@latest")
                .map(Some)
        }
        _ => Err(InvalidArgumentError::new(
            "--change and --changeset must be provided together or both omitted",
        )
        .into()),
    }
}

fn resolve_changeset_selectors_in_workspace(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
    change_selector: &str,
    changeset_selector: &str,
) -> Result<(String, String)> {
    let store = WorkspaceStore::open(state_dir)?;
    let head = store.load_refs()?.head().to_owned();
    let change = if is_context_selector(change_selector) {
        let candidates = store
            .list_change_declarations()?
            .into_iter()
            .filter(|candidate| {
                candidate.dataset_id() == workspace.dataset_id
                    && candidate.adapter() == workspace.adapter
                    && candidate.base_revision_id() == head
            })
            .collect::<Vec<_>>();
        if candidates.len() != 1 {
            return Err(InvalidArgumentError::new(format!(
                "cannot resolve --change {change_selector}: found {} compatible active change declarations; pass one explicit chg_... ID",
                candidates.len()
            ))
            .into());
        }
        candidates.into_iter().next().expect("one candidate")
    } else {
        store.load_change_declaration(change_selector)?
    };
    let changeset = if is_context_selector(changeset_selector) {
        let candidates = store
            .list_changesets()?
            .into_iter()
            .filter(|candidate| {
                candidate.change_id() == change.change_id()
                    && candidate.dataset_id() == workspace.dataset_id
                    && candidate.adapter() == workspace.adapter
                    && candidate.base_revision_id() == head
            })
            .collect::<Vec<_>>();
        if candidates.len() != 1 {
            return Err(InvalidArgumentError::new(format!(
                "cannot resolve --changeset {changeset_selector}: found {} compatible staged changesets for {}; pass one explicit changeset_... ID",
                candidates.len(),
                change.change_id()
            ))
            .into());
        }
        candidates.into_iter().next().expect("one candidate")
    } else {
        store.load_changeset(changeset_selector)?
    };
    Ok((
        change.change_id().to_owned(),
        changeset.changeset_id().to_owned(),
    ))
}

fn is_context_selector(value: &str) -> bool {
    matches!(value, "@active" | "@latest")
}

struct ChangesetContext {
    state_dir: PathBuf,
    workspace: WorkspaceConfig,
    change: ChangeDeclaration,
    changeset: DatasetChangeset,
    baseline: crate::DatasetInventory,
    candidate: crate::DatasetInventory,
}

fn load_changeset_context(
    state: &Path,
    change_id: &str,
    changeset_id: &str,
) -> Result<ChangesetContext> {
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    let store = WorkspaceStore::open(&state_dir)?;
    let change = store.load_change_declaration(change_id)?;
    let changeset = store.load_changeset(changeset_id)?;
    let (dataset_id, head, baseline) = resolve_head(&state_dir, &workspace)?;
    validate_change_anchor(&change, &workspace, &dataset_id, &head, &baseline)?;
    if changeset.change_id() != change.change_id()
        || changeset.dataset_id() != dataset_id
        || changeset.adapter() != workspace.adapter
        || changeset.base_revision_id() != head.revision_id()
        || changeset.base_inventory_id() != head.inventory_id()
    {
        bail!("staged changeset does not match its declaration or workspace base");
    }
    let candidate = store.load_inventory(changeset.candidate_inventory_id())?;
    if baseline.coverage() != candidate.coverage() || changeset.coverage() != candidate.coverage() {
        bail!("staged changeset coverage does not match its inventory objects");
    }
    if ChangesetSummary::derive(&baseline, &candidate) != *changeset.summary() {
        bail!("staged changeset summary does not match its inventory objects");
    }
    Ok(ChangesetContext {
        state_dir,
        workspace,
        change,
        changeset,
        baseline,
        candidate,
    })
}

pub fn status_changeset(
    state: &Path,
    threads: usize,
    change_id: &str,
    changeset_id: &str,
) -> Result<WorkspaceStatusArtifact> {
    validate_threads(threads)?;
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter == "jsonl" {
        return status_jsonl_changeset(&state_dir, &workspace, change_id, changeset_id);
    }
    let context = load_changeset_context(state, change_id, changeset_id)?;
    let current = scan_inventory_for_schema(
        Path::new(context.workspace.dataset_path()),
        threads,
        context.candidate.schema_version(),
    )?;
    let current_inventory_id = current.content_id()?;
    let head_inventory_id = context.baseline.content_id()?;
    let staged_inventory_id = context.candidate.content_id()?;
    let unstaged_changes = current_inventory_id != staged_inventory_id;
    Ok(WorkspaceStatusArtifact {
        changed_files: count_changed_files(&context.baseline, &current),
        clean: current_inventory_id == head_inventory_id,
        current_inventory_id: Some(current_inventory_id),
        current_state_id: None,
        dataset_id: context.workspace.dataset_id,
        dataset_coverage: context.candidate.coverage().into(),
        decision: if unstaged_changes { "stage" } else { "review" }.into(),
        head_inventory_id: Some(head_inventory_id),
        head_state_id: None,
        head_revision_id: context.change.base_revision_id().into(),
        state_dir: path_text(&context.state_dir, "state directory")?,
        change_id: Some(context.change.change_id().into()),
        changeset_id: Some(context.changeset.changeset_id().into()),
        staged_inventory_id: Some(staged_inventory_id),
        staged_state_id: None,
        changed_records: None,
        validation_findings: None,
        unstaged_changes: Some(unstaged_changes),
    })
}

fn resolve_record_head(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
) -> Result<(String, DatasetRevision, JsonlRecordStateBundle)> {
    if workspace.adapter != "jsonl" {
        bail!("workspace is not a JSONL record workspace");
    }
    let store = WorkspaceStore::open(state_dir)?;
    let refs = store.load_refs()?;
    let revision = store.load_revision(refs.head())?;
    validate_workspace_identity(&store, workspace, &revision)?;
    let state = store.load_record_state_bundle(revision.state_id())?;
    Ok((workspace.dataset_id.clone(), revision, state))
}

fn status_jsonl_workspace(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
) -> Result<WorkspaceStatusArtifact> {
    let (dataset_id, head, baseline) = resolve_record_head(state_dir, workspace)?;
    let current = build_jsonl_record_state(
        Path::new(workspace.dataset_path()),
        workspace.id_field().expect("validated JSONL workspace"),
    )?
    .bundle();
    let head_state_id = baseline.state().record_state_id().to_owned();
    let current_state_id = current.state().record_state_id().to_owned();
    let clean = head_state_id == current_state_id;
    let changed_records =
        if baseline.state().finding_count() == 0 && current.state().finding_count() == 0 {
            Some(diff_jsonl_record_states(&baseline, &current)?.total_changes())
        } else {
            None
        };
    Ok(WorkspaceStatusArtifact {
        changed_files: usize::from(!clean),
        clean,
        current_inventory_id: None,
        current_state_id: Some(current_state_id),
        dataset_id,
        dataset_coverage: "records_all_v1".into(),
        decision: if clean { "clean" } else { "review" }.into(),
        head_inventory_id: None,
        head_state_id: Some(head_state_id),
        head_revision_id: head.revision_id().into(),
        state_dir: path_text(state_dir, "state directory")?,
        change_id: None,
        changeset_id: None,
        staged_inventory_id: None,
        staged_state_id: None,
        changed_records,
        validation_findings: Some(current.state().finding_count()),
        unstaged_changes: None,
    })
}

fn begin_jsonl_changeset(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
    intent: &str,
    external_task_id: &str,
    actor_kind: &str,
) -> Result<ChangeDeclaration> {
    let (dataset_id, head, baseline) = resolve_record_head(state_dir, workspace)?;
    let current = build_jsonl_record_state(
        Path::new(workspace.dataset_path()),
        workspace.id_field().expect("validated JSONL workspace"),
    )?;
    if current.state().record_state_id() != baseline.state().record_state_id() {
        return Err(InvalidArgumentError::new(
            "changeset begin requires a clean workspace; seal or restore current edits first",
        )
        .into());
    }
    let change = ChangeDeclaration::new_for_state(
        dataset_id,
        "jsonl".into(),
        head.revision_id().into(),
        head.state_id().into(),
        intent.into(),
        external_task_id.into(),
        actor_kind.into(),
        env!("CARGO_PKG_VERSION").into(),
    )?;
    WorkspaceStore::open(state_dir)?.publish_change_declaration(&change)?;
    Ok(change)
}

fn stage_jsonl_changeset(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
    change_id: &str,
) -> Result<DatasetChangeset> {
    let store = WorkspaceStore::open(state_dir)?;
    let change = store.load_change_declaration(change_id)?;
    let (dataset_id, head, baseline) = resolve_record_head(state_dir, workspace)?;
    validate_record_change_anchor(&change, workspace, &dataset_id, &head, &baseline)?;
    let candidate = build_jsonl_record_state(
        Path::new(workspace.dataset_path()),
        workspace.id_field().expect("validated JSONL workspace"),
    )?
    .bundle();
    if candidate.state().record_state_id() == baseline.state().record_state_id() {
        return Err(InvalidArgumentError::new("cannot stage an empty dataset changeset").into());
    }
    let summary = if candidate.state().finding_count() == 0 {
        let diff = diff_jsonl_record_states(&baseline, &candidate)?;
        let (added, removed, modified, moved, unchanged) = diff.summary_counts();
        RecordChangesetSummary::new(
            added,
            removed,
            modified,
            moved,
            unchanged,
            diff.byte_only_changed(),
        )?
    } else {
        RecordChangesetSummary::unavailable()
    };
    let candidate_id = store.publish_record_state_bundle(&candidate)?;
    let changeset = DatasetChangeset::new_record(
        change.change_id().into(),
        dataset_id,
        head.revision_id().into(),
        baseline.state().record_state_id().into(),
        candidate_id,
        summary,
    )?;
    store.publish_changeset(&changeset)?;
    Ok(changeset)
}

fn status_jsonl_changeset(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
    change_id: &str,
    changeset_id: &str,
) -> Result<WorkspaceStatusArtifact> {
    let store = WorkspaceStore::open(state_dir)?;
    let change = store.load_change_declaration(change_id)?;
    let changeset = store.load_changeset(changeset_id)?;
    let (dataset_id, head, baseline) = resolve_record_head(state_dir, workspace)?;
    validate_record_change_anchor(&change, workspace, &dataset_id, &head, &baseline)?;
    if changeset.change_id() != change.change_id()
        || changeset.dataset_id() != dataset_id
        || changeset.adapter() != "jsonl"
        || changeset.base_revision_id() != head.revision_id()
        || changeset.base_state_id() != baseline.state().record_state_id()
    {
        bail!("staged record changeset does not match its declaration or workspace base");
    }
    let candidate = store.load_record_state_bundle(changeset.candidate_state_id())?;
    let current = build_jsonl_record_state(
        Path::new(workspace.dataset_path()),
        workspace.id_field().expect("validated JSONL workspace"),
    )?
    .bundle();
    let current_id = current.state().record_state_id().to_owned();
    let baseline_id = baseline.state().record_state_id().to_owned();
    let candidate_id = candidate.state().record_state_id().to_owned();
    let unstaged = current_id != candidate_id;
    let changed_records =
        if baseline.state().finding_count() == 0 && current.state().finding_count() == 0 {
            Some(diff_jsonl_record_states(&baseline, &current)?.total_changes())
        } else {
            None
        };
    Ok(WorkspaceStatusArtifact {
        changed_files: usize::from(current_id != baseline_id),
        clean: current_id == baseline_id,
        current_inventory_id: None,
        current_state_id: Some(current_id),
        dataset_id,
        dataset_coverage: "records_all_v1".into(),
        decision: if unstaged { "stage" } else { "review" }.into(),
        head_inventory_id: None,
        head_state_id: Some(baseline_id),
        head_revision_id: head.revision_id().into(),
        state_dir: path_text(state_dir, "state directory")?,
        change_id: Some(change.change_id().into()),
        changeset_id: Some(changeset.changeset_id().into()),
        staged_inventory_id: None,
        staged_state_id: Some(candidate_id),
        changed_records,
        validation_findings: Some(current.state().finding_count()),
        unstaged_changes: Some(unstaged),
    })
}

fn validate_record_change_anchor(
    change: &ChangeDeclaration,
    workspace: &WorkspaceConfig,
    dataset_id: &str,
    head: &DatasetRevision,
    baseline: &JsonlRecordStateBundle,
) -> Result<()> {
    if change.dataset_id() != dataset_id
        || change.adapter() != "jsonl"
        || change.base_state_id() != baseline.state().record_state_id()
    {
        bail!("change declaration does not belong to this JSONL workspace");
    }
    if change.base_revision_id() != head.revision_id() {
        return Err(ConcurrentModificationError::new(
            "workspace HEAD changed after the change declaration was created",
        )
        .into());
    }
    if workspace.id_field() != Some(baseline.state().id_field()) {
        bail!("workspace id_field does not match its baseline record state");
    }
    Ok(())
}

struct RecordChangesetContext {
    change: ChangeDeclaration,
    changeset: DatasetChangeset,
    head: DatasetRevision,
    baseline: JsonlRecordStateBundle,
    candidate: JsonlRecordStateBundle,
}

fn load_record_changeset_context(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
    change_id: &str,
    changeset_id: &str,
) -> Result<RecordChangesetContext> {
    let store = WorkspaceStore::open(state_dir)?;
    let change = store.load_change_declaration(change_id)?;
    let changeset = store.load_changeset(changeset_id)?;
    let (dataset_id, head, baseline) = resolve_record_head(state_dir, workspace)?;
    validate_record_change_anchor(&change, workspace, &dataset_id, &head, &baseline)?;
    if changeset.change_id() != change.change_id()
        || changeset.dataset_id() != dataset_id
        || changeset.adapter() != "jsonl"
        || changeset.base_revision_id() != head.revision_id()
        || changeset.base_state_id() != baseline.state().record_state_id()
    {
        bail!("staged record changeset does not match its declaration or workspace base");
    }
    let candidate = store.load_record_state_bundle(changeset.candidate_state_id())?;
    let expected_summary = record_changeset_summary(&baseline, &candidate)?;
    if changeset.record_summary() != Some(&expected_summary) {
        bail!("staged record changeset summary does not match its state objects");
    }
    Ok(RecordChangesetContext {
        change,
        changeset,
        head,
        baseline,
        candidate,
    })
}

fn record_changeset_summary(
    baseline: &JsonlRecordStateBundle,
    candidate: &JsonlRecordStateBundle,
) -> Result<RecordChangesetSummary> {
    if candidate.state().finding_count() != 0 {
        return Ok(RecordChangesetSummary::unavailable());
    }
    let diff = diff_jsonl_record_states(baseline, candidate)?;
    let (added, removed, modified, moved, unchanged) = diff.summary_counts();
    RecordChangesetSummary::new(
        added,
        removed,
        modified,
        moved,
        unchanged,
        diff.byte_only_changed(),
    )
}

fn scan_live_record_state(workspace: &WorkspaceConfig) -> Result<JsonlRecordStateBundle> {
    let dataset = Path::new(workspace.dataset_path());
    ensure_jsonl_dataset_is_direct_file(dataset)?;
    let bundle = build_jsonl_record_state(
        dataset,
        workspace.id_field().expect("validated JSONL workspace"),
    )?
    .bundle();
    ensure_jsonl_dataset_is_direct_file(dataset)?;
    Ok(bundle)
}

fn scan_live_record_state_with_byte_limit(
    workspace: &WorkspaceConfig,
    byte_limit: u64,
) -> Result<JsonlRecordStateBundle> {
    let dataset = Path::new(workspace.dataset_path());
    ensure_jsonl_dataset_is_direct_file(dataset)?;
    let bundle = build_jsonl_record_state_with_byte_limit(
        dataset,
        workspace.id_field().expect("validated JSONL workspace"),
        byte_limit,
    )?
    .bundle();
    ensure_jsonl_dataset_is_direct_file(dataset)?;
    Ok(bundle)
}

fn ensure_jsonl_dataset_is_direct_file(dataset: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(dataset)
        .map_err(|_| ConcurrentModificationError::new("workspace JSONL dataset path changed"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(ConcurrentModificationError::new(
            "workspace JSONL dataset path must remain a direct regular file",
        )
        .into());
    }
    Ok(())
}

fn ensure_live_record_candidate(
    workspace: &WorkspaceConfig,
    context: &RecordChangesetContext,
    operation: &str,
) -> Result<JsonlRecordStateBundle> {
    let live = scan_live_record_state(workspace)?;
    if live.state().record_state_id() != context.candidate.state().record_state_id() {
        return Err(UnstagedChangesError::new(format!(
            "live JSONL dataset differs from the staged changeset; stage it again before {operation}"
        ))
        .into());
    }
    Ok(live)
}

fn check_jsonl_changeset(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
    change_id: &str,
    changeset_id: &str,
) -> Result<WorkspaceCheckArtifact> {
    let context = load_record_changeset_context(state_dir, workspace, change_id, changeset_id)?;
    ensure_live_record_candidate(workspace, &context, "check")?;
    let (policy, quality) = evaluate_record_quality(state_dir, workspace, &context)?;
    let report_path = state_dir.join(LATEST_REPORT_FILE);
    let artifact = create_jsonl_review(
        &context.baseline,
        &context.candidate,
        policy.as_ref(),
        quality.as_ref(),
        &[
            ("change_id", context.change.change_id()),
            ("changeset_id", context.changeset.changeset_id()),
        ],
        &report_path,
    )?;
    let decision = match artifact.status.as_str() {
        "pass" => "seal",
        "warn" => "inspect",
        "fail" => "fix",
        _ => "retry",
    };
    Ok(WorkspaceCheckArtifact {
        decision: decision.into(),
        findings: artifact.findings,
        report_content_id: artifact.content_id,
        report_path: artifact.output,
        status: artifact.status,
        dataset_coverage: "records_all_v1".into(),
        change_id: Some(context.change.change_id().into()),
        changeset_id: Some(context.changeset.changeset_id().into()),
    })
}

fn plan_jsonl_changeset(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
    change_id: &str,
    changeset_id: &str,
) -> Result<WorkspacePlanArtifact> {
    let context = load_record_changeset_context(state_dir, workspace, change_id, changeset_id)?;
    ensure_live_record_candidate(workspace, &context, "plan")?;
    let (policy, quality) = evaluate_record_quality(state_dir, workspace, &context)?;
    let report_payload = fs::read(state_dir.join(LATEST_REPORT_FILE))
        .context("cannot read latest workspace review")?;
    if report_payload.len() > MAX_REPORT_BYTES {
        bail!("report is too large (max {MAX_REPORT_BYTES} bytes)");
    }
    let report = ReviewReport::from_json(
        std::str::from_utf8(&report_payload)
            .context("latest workspace review is not valid UTF-8")?,
    )?;
    validate_record_report_binding(&report, &context, policy.as_ref())?;
    let expected_report = jsonl_review_payload(
        &context.baseline,
        &context.candidate,
        policy.as_ref(),
        quality.as_ref(),
        &[
            ("change_id", context.change.change_id()),
            ("changeset_id", context.changeset.changeset_id()),
        ],
    )?;
    if report_payload != expected_report {
        bail!("latest workspace review does not match the deterministic staged review");
    }
    let plan =
        create_remediation_plan_from_payload(&report_payload, &state_dir.join(LATEST_PLAN_FILE))?;
    Ok(WorkspacePlanArtifact {
        decision: plan.decision.clone(),
        plan,
        state_dir: path_text(state_dir, "state directory")?,
    })
}

fn seal_jsonl_changeset(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
    message: &str,
    accepted_report_id: Option<&str>,
    change_id: &str,
    changeset_id: &str,
) -> Result<WorkspaceSealArtifact> {
    if message.is_empty() || message.len() > 4_096 {
        return Err(InvalidArgumentError::new("message must contain 1 to 4096 UTF-8 bytes").into());
    }
    let context = load_record_changeset_context(state_dir, workspace, change_id, changeset_id)?;
    ensure_live_record_candidate(workspace, &context, "seal")?;
    let (policy, quality) = evaluate_record_quality(state_dir, workspace, &context)?;
    let report_payload = fs::read(state_dir.join(LATEST_REPORT_FILE))
        .context("cannot read latest workspace review")?;
    if report_payload.len() > MAX_REPORT_BYTES {
        bail!("report is too large (max {MAX_REPORT_BYTES} bytes)");
    }
    let report = ReviewReport::from_json(
        std::str::from_utf8(&report_payload)
            .context("latest workspace review is not valid UTF-8")?,
    )?;
    validate_record_report_binding(&report, &context, policy.as_ref())?;
    if context.candidate.state().finding_count() != 0 {
        bail!("a JSONL candidate with validation findings cannot be sealed");
    }
    let expected_report = jsonl_review_payload(
        &context.baseline,
        &context.candidate,
        policy.as_ref(),
        quality.as_ref(),
        &[
            ("change_id", context.change.change_id()),
            ("changeset_id", context.changeset.changeset_id()),
        ],
    )?;
    if report_payload != expected_report {
        bail!("latest workspace review does not match the deterministic staged review");
    }
    if report.status() != "pass" {
        bail!("latest workspace review must pass before it can be sealed");
    }
    let report_id = crate::report::report_content_id(&report_payload);
    let finding_count = report.indexed_findings()?.len();
    let accepted_report_id = match (finding_count, accepted_report_id, policy.is_some()) {
        (0, None, false) => None,
        (0, None, true) => Some(report_id.clone()),
        (_, Some(value), _) if value == report_id => Some(report_id.clone()),
        (0, Some(_), _) => bail!("accepted report does not match the latest workspace review"),
        (_, None, _) => {
            bail!("a passing review with findings requires --accept-report {report_id}")
        }
        (_, Some(_), _) => bail!("accepted report does not match the latest workspace review"),
    };
    let revision = DatasetRevision::new_record(
        Some(context.head.revision_id().into()),
        context.candidate.state().record_state_id().into(),
        context.candidate.state().dataset_content_id().into(),
        accepted_report_id.clone(),
        RevisionProvenance::new(
            "agent".into(),
            Some(context.changeset.changeset_id().into()),
            env!("CARGO_PKG_VERSION").into(),
            message.into(),
        )?,
        now_unix_ns()?,
    )?;
    let store = WorkspaceStore::open(state_dir)?;
    if accepted_report_id.is_some() {
        store.publish_review(&report_payload, &report_id)?;
    }
    store.publish_jsonl_blob(
        Path::new(workspace.dataset_path()),
        context.candidate.state().dataset_content_id(),
    )?;
    store.publish_revision(&revision)?;
    let final_live = scan_live_record_state(workspace)?;
    if final_live.state().record_state_id() != context.candidate.state().record_state_id() {
        return Err(ConcurrentModificationError::new(
            "workspace JSONL candidate changed while it was being sealed",
        )
        .into());
    }
    store.compare_and_swap_head(Some(context.head.revision_id()), revision.revision_id())?;
    Ok(WorkspaceSealArtifact {
        accepted_report_id,
        baseline_inventory_id: None,
        baseline_state_id: Some(context.candidate.state().record_state_id().into()),
        dataset_id: context.change.dataset_id().into(),
        dataset_coverage: "records_all_v1".into(),
        dataset_root: None,
        dataset_path: Some(workspace.dataset_path().into()),
        parent_revision_id: context.head.revision_id().into(),
        revision_id: revision.revision_id().into(),
        state_dir: path_text(state_dir, "state directory")?,
        change_id: Some(context.change.change_id().into()),
        changeset_id: Some(context.changeset.changeset_id().into()),
    })
}

fn validate_record_report_binding(
    report: &ReviewReport,
    context: &RecordChangesetContext,
    policy: Option<&JsonlQualityPolicy>,
) -> Result<()> {
    if report.metadata_string("change_id") != Some(context.change.change_id())
        || report.metadata_string("changeset_id") != Some(context.changeset.changeset_id())
        || report.metadata_string("base_state_id")
            != Some(context.baseline.state().record_state_id())
        || report.metadata_string("candidate_state_id")
            != Some(context.candidate.state().record_state_id())
    {
        return Err(UnstagedChangesError::new(
            "latest review is not bound to the requested staged record changeset",
        )
        .into());
    }
    match policy {
        Some(policy)
            if report.metadata_string("quality_policy_id") == Some(policy.policy_id())
                && report.metadata_string("quality_gate_mode") == Some(policy.mode()) => {}
        Some(_) => {
            return Err(UnstagedChangesError::new(
                "latest review is not bound to the workspace quality policy",
            )
            .into());
        }
        None if report.metadata_string("quality_policy_id").is_none() => {}
        None => bail!("policy-free workspace review unexpectedly names a quality policy"),
    }
    Ok(())
}

fn evaluate_record_quality(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
    context: &RecordChangesetContext,
) -> Result<(Option<JsonlQualityPolicy>, Option<JsonlQualityEvaluation>)> {
    let Some(policy_id) = workspace.quality_policy_id() else {
        return Ok((None, None));
    };
    let policy = WorkspaceStore::open(state_dir)?.load_jsonl_quality_policy(policy_id)?;
    if context.candidate.state().finding_count() != 0 {
        return Ok((Some(policy), None));
    }
    let gated_ids = if policy.mode() == "changed_only" {
        Some(changed_record_ids(&context.baseline, &context.candidate)?)
    } else {
        None
    };
    let evaluation = evaluate_jsonl_quality(
        Path::new(workspace.dataset_path()),
        workspace.id_field().expect("validated JSONL workspace"),
        context.candidate.state().dataset_content_id(),
        &policy,
        gated_ids.as_ref(),
    )?;
    Ok((Some(policy), Some(evaluation)))
}

pub fn locate_changeset_finding(
    state: &Path,
    report_path: &Path,
    finding_id: &str,
    change_id: &str,
    changeset_id: &str,
    offset: i64,
    limit: i64,
) -> Result<FindingLocationsArtifact> {
    if offset < 0 || !(1..=MAX_PAGE_SIZE).contains(&limit) {
        return Err(InvalidArgumentError::new(format!(
            "offset must be non-negative and limit must be between 1 and {MAX_PAGE_SIZE}"
        ))
        .into());
    }
    let offset = usize::try_from(offset).expect("validated non-negative offset");
    let limit = usize::try_from(limit).expect("validated positive limit");
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter != "jsonl" {
        return Err(
            InvalidArgumentError::new("finding location requires a keyed JSONL workspace").into(),
        );
    }
    let context = load_record_changeset_context(&state_dir, &workspace, change_id, changeset_id)?;
    ensure_live_record_candidate(&workspace, &context, "locating finding evidence")?;
    let (policy, quality) = evaluate_record_quality(&state_dir, &workspace, &context)?;
    let report_payload = load_direct_bounded_report(report_path)?;
    let report = ReviewReport::from_json(
        std::str::from_utf8(&report_payload)
            .map_err(|_| InvalidArgumentError::new("workspace review is invalid"))?,
    )
    .map_err(|_| InvalidArgumentError::new("workspace review is invalid"))?;
    validate_record_report_binding(&report, &context, policy.as_ref())?;
    let expected_report = jsonl_review_payload(
        &context.baseline,
        &context.candidate,
        policy.as_ref(),
        quality.as_ref(),
        &[
            ("change_id", context.change.change_id()),
            ("changeset_id", context.changeset.changeset_id()),
        ],
    )?;
    if report_payload != expected_report {
        return Err(UnstagedChangesError::new(
            "workspace review is not the deterministic review for the staged candidate",
        )
        .into());
    }
    let indexed = report.indexed_findings()?;
    let indexed = indexed
        .iter()
        .find(|item| item.id() == finding_id)
        .ok_or_else(|| FindingNotFoundError::new("finding was not found in verified report"))?;
    let finding = indexed.finding();
    let mut locations = Vec::new();
    let evidence_truncated = if finding.sample_ids().is_empty() {
        if !is_structural_finding_code(finding.code()) {
            return Err(InvalidArgumentError::new(
                "finding does not contain locatable record evidence",
            )
            .into());
        }
        locations.extend(
            context
                .candidate
                .state()
                .structural_locations(finding.code())
                .into_iter()
                .map(FindingLocation::Structural),
        );
        if locations.is_empty() {
            return Err(
                InvalidArgumentError::new("finding has no captured structural locations").into(),
            );
        }
        context.candidate.state().findings_truncated()
    } else {
        let mut record_ids = finding.sample_ids().to_vec();
        record_ids.sort_unstable();
        let sample_count = record_ids.len();
        for record_id in record_ids {
            let location = context
                .candidate
                .locate_record(&record_id)?
                .ok_or_else(|| {
                    UnstagedChangesError::new(
                        "finding evidence does not resolve in the staged candidate",
                    )
                })?;
            locations.push(FindingLocation::Record(location));
        }
        finding.evidence_bool("samples_truncated").unwrap_or(false)
            || finding
                .evidence_usize("count")
                .is_some_and(|count| count > sample_count)
    };
    let total = locations.len();
    let start = offset.min(total);
    let end = start.saturating_add(limit).min(total);
    let locations = locations[start..end].to_vec();
    let final_context = load_record_changeset_context(
        &state_dir,
        &workspace,
        context.change.change_id(),
        context.changeset.changeset_id(),
    )?;
    ensure_live_record_candidate(&workspace, &final_context, "returning finding evidence")?;
    Ok(FindingLocationsArtifact {
        report_content_id: crate::report::report_content_id(&report_payload),
        finding_id: indexed.id().into(),
        finding_code: finding.code().into(),
        change_id: context.change.change_id().into(),
        changeset_id: context.changeset.changeset_id().into(),
        candidate_state_id: context.candidate.state().record_state_id().into(),
        candidate_dataset_content_id: context.candidate.state().dataset_content_id().into(),
        evidence_truncated,
        page: FindingLocationPage {
            offset,
            limit,
            returned: locations.len(),
            total,
            has_more: end < total,
        },
        locations,
    })
}

#[allow(clippy::too_many_arguments)]
pub fn draft_jsonl_patch(
    state: &Path,
    report_path: &Path,
    finding_id: &str,
    change_id: &str,
    changeset_id: &str,
    record_id: Option<&str>,
    after: Option<serde_json::Value>,
    output: &Path,
) -> Result<JsonlPatchDraftArtifact> {
    #[cfg(not(unix))]
    {
        let _ = (
            state,
            report_path,
            finding_id,
            change_id,
            changeset_id,
            record_id,
            after,
            output,
        );
        bail!("JSONL patch drafting currently requires Unix");
    }
    #[cfg(unix)]
    {
        if output.exists() {
            return Err(InvalidArgumentError::new(
                "JSONL patch request output must name a new file",
            )
            .into());
        }
        let (state_dir, workspace) = load_workspace_compatible(state)?;
        if workspace.adapter != "jsonl" || workspace.quality_policy_id().is_none() {
            return Err(InvalidArgumentError::new(
                "JSONL patch drafting requires a keyed workspace with a pinned quality policy",
            )
            .into());
        }
        let located = locate_changeset_finding(
            &state_dir,
            report_path,
            finding_id,
            change_id,
            changeset_id,
            0,
            MAX_PAGE_SIZE,
        )?;
        if !is_patchable_quality_code(&located.finding_code) {
            return Err(InvalidArgumentError::new(
                "finding is not eligible for a JSONL field patch",
            )
            .into());
        }
        let records = located
            .locations
            .iter()
            .filter_map(|location| match location {
                FindingLocation::Record(location) => Some(location),
                FindingLocation::Structural(_) => None,
            })
            .collect::<Vec<_>>();
        let location = match record_id {
            Some(record_id) => records
                .iter()
                .copied()
                .find(|location| location.record_id == record_id)
                .ok_or_else(|| {
                    InvalidArgumentError::new(
                        "requested record is not sampled by the verified finding",
                    )
                })?,
            None if records.len() == 1 && !located.evidence_truncated => records[0],
            None => {
                return Err(InvalidArgumentError::new(
                    "finding resolves to multiple or truncated records; pass --record with a sampled rid_... identity",
                )
                .into());
            }
        };
        let report_payload = load_direct_bounded_report(report_path)?;
        let report_text = std::str::from_utf8(&report_payload)
            .map_err(|_| InvalidArgumentError::new("workspace review is invalid"))?;
        let report = ReviewReport::from_json(report_text)
            .map_err(|_| InvalidArgumentError::new("workspace review is invalid"))?;
        let indexed = report
            .indexed_findings()?
            .into_iter()
            .find(|item| item.id() == finding_id)
            .ok_or_else(|| FindingNotFoundError::new("finding was not found in verified report"))?;
        let field = indexed
            .finding()
            .evidence_string("field")
            .ok_or_else(|| InvalidArgumentError::new("finding does not name a patchable field"))?;
        let policy = WorkspaceStore::open(&state_dir)?.load_jsonl_quality_policy(
            workspace
                .quality_policy_id()
                .expect("validated JSONL patch workspace"),
        )?;
        if policy.is_unique_field(field) {
            return Err(InvalidArgumentError::new(
                "unique-field repairs are not supported by guarded patch apply",
            )
            .into());
        }
        let missing_value = serde_json::Value::Null;
        if !policy.accepts_field_value(
            field,
            after.is_some(),
            after.as_ref().unwrap_or(&missing_value),
        )? {
            return Err(InvalidArgumentError::new(
                "proposed JSONL patch does not satisfy the pinned field policy",
            )
            .into());
        }
        let physical_line = read_physical_line(Path::new(workspace.dataset_path()), location.line)?;
        let content = physical_line
            .strip_suffix(b"\r\n")
            .or_else(|| physical_line.strip_suffix(b"\n"))
            .unwrap_or(&physical_line);
        let record: serde_json::Value = serde_json::from_slice(content)
            .map_err(|_| InvalidArgumentError::new("JSONL patch target is not valid JSON"))?;
        let object = record
            .as_object()
            .ok_or_else(|| InvalidArgumentError::new("JSONL patch target is not an object"))?;
        let before = object.get(field).cloned();
        let request = JsonlPatchRequest::new_bound(
            located.report_content_id.clone(),
            located.candidate_state_id.clone(),
            finding_id.into(),
            location.record_id.clone(),
            location.record_content_id.clone(),
            before,
            after,
        )?;
        let final_location = locate_changeset_finding(
            &state_dir,
            report_path,
            finding_id,
            change_id,
            changeset_id,
            0,
            MAX_PAGE_SIZE,
        )?;
        if final_location.candidate_state_id != located.candidate_state_id
            || final_location.candidate_dataset_content_id != located.candidate_dataset_content_id
            || !final_location.locations.iter().any(|item| match item {
                FindingLocation::Record(item) => {
                    item.record_id == location.record_id
                        && item.record_content_id == location.record_content_id
                        && item.line == location.line
                }
                FindingLocation::Structural(_) => false,
            })
        {
            return Err(UnstagedChangesError::new(
                "JSONL patch target changed while drafting the request",
            )
            .into());
        }
        let payload = request.to_json()?;
        save_new_file_atomically(output, payload.as_bytes(), "JSONL patch request")?;
        let output = output
            .canonicalize()
            .context("cannot resolve JSONL patch request output")?;
        Ok(JsonlPatchDraftArtifact {
            schema_version: crate::JSONL_PATCH_SCHEMA_VERSION,
            request_content_id: crate::identity::blake3_content_id(
                "patchreq",
                b"datajig-jsonl-patch-request-v1\0",
                payload.as_bytes(),
            ),
            report_content_id: located.report_content_id,
            finding_id: finding_id.into(),
            candidate_state_id: located.candidate_state_id,
            output: path_text(&output, "JSONL patch request output")?,
        })
    }
}

pub fn preview_jsonl_patch(
    state: &Path,
    request_path: &Path,
    report_path: &Path,
    change_id: &str,
    changeset_id: &str,
) -> Result<JsonlPatchPreviewArtifact> {
    let request = JsonlPatchRequest::from_path(request_path)?;
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter != "jsonl" || workspace.quality_policy_id().is_none() {
        return Err(InvalidArgumentError::new(
            "JSONL patch preview requires a keyed workspace with a pinned quality policy",
        )
        .into());
    }
    let located = locate_changeset_finding(
        &state_dir,
        report_path,
        request.finding_id(),
        change_id,
        changeset_id,
        0,
        MAX_PAGE_SIZE,
    )?;
    if located.report_content_id != request.report_content_id()
        || located.candidate_state_id != request.candidate_state_id()
    {
        return Err(UnstagedChangesError::new(
            "JSONL patch request is not bound to the verified staged review",
        )
        .into());
    }
    if !is_patchable_quality_code(&located.finding_code) {
        return Err(InvalidArgumentError::new(
            "finding is not eligible for JSONL field patch preview",
        )
        .into());
    }
    let location = located
        .locations
        .iter()
        .find_map(|location| match location {
            FindingLocation::Record(location) if location.record_id == request.record_id() => {
                Some(location)
            }
            _ => None,
        })
        .ok_or_else(|| {
            InvalidArgumentError::new("JSONL patch record is not sampled by the verified finding")
        })?;
    if location.record_content_id != request.record_content_id() {
        return Err(InvalidArgumentError::new(
            "JSONL patch record content does not match the verified finding",
        )
        .into());
    }
    let report_payload = load_direct_bounded_report(report_path)?;
    if crate::report::report_content_id(&report_payload) != located.report_content_id {
        return Err(
            UnstagedChangesError::new("JSONL patch report changed during verification").into(),
        );
    }
    let report_text = std::str::from_utf8(&report_payload)
        .map_err(|_| InvalidArgumentError::new("workspace review is invalid"))?;
    let report = ReviewReport::from_json(report_text)
        .map_err(|_| InvalidArgumentError::new("workspace review is invalid"))?;
    let indexed = report
        .indexed_findings()?
        .into_iter()
        .find(|item| item.id() == request.finding_id())
        .ok_or_else(|| FindingNotFoundError::new("finding was not found in verified report"))?;
    if indexed.finding().code() != located.finding_code {
        return Err(
            UnstagedChangesError::new("JSONL patch finding changed during verification").into(),
        );
    }
    let field = indexed
        .finding()
        .evidence_string("field")
        .ok_or_else(|| InvalidArgumentError::new("finding does not name a patchable field"))?;
    let policy = WorkspaceStore::open(&state_dir)?.load_jsonl_quality_policy(
        workspace
            .quality_policy_id()
            .expect("validated JSONL patch workspace"),
    )?;
    let (after_present, after_value) = request.after();
    if !policy.accepts_field_value(field, after_present, after_value)? {
        return Err(InvalidArgumentError::new(
            "proposed JSONL patch does not satisfy the pinned field policy",
        )
        .into());
    }
    let preview = crate::jsonl_patch::prepare_patch_preview(
        &request,
        Path::new(workspace.dataset_path()),
        workspace.id_field().expect("validated JSONL workspace"),
        &located.finding_code,
        field,
        location.line,
        change_id,
        changeset_id,
        &located.candidate_dataset_content_id,
    )?;
    let final_location = locate_changeset_finding(
        &state_dir,
        report_path,
        request.finding_id(),
        change_id,
        changeset_id,
        0,
        MAX_PAGE_SIZE,
    )?;
    let target_unchanged = final_location.candidate_state_id == located.candidate_state_id
        && final_location.candidate_dataset_content_id == located.candidate_dataset_content_id
        && final_location.locations.iter().any(|item| match item {
            FindingLocation::Record(item) => {
                item.record_id == location.record_id
                    && item.record_content_id == location.record_content_id
                    && item.line == location.line
            }
            FindingLocation::Structural(_) => false,
        });
    if !target_unchanged {
        return Err(UnstagedChangesError::new("JSONL patch target changed during preview").into());
    }
    Ok(preview)
}

#[derive(Clone, Debug, Serialize)]
pub struct JsonlPatchApplyArtifact {
    pub schema_version: u8,
    pub apply_id: String,
    pub patch_id: String,
    pub undo_id: String,
    pub change_id: String,
    pub source_changeset_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changeset_id: Option<String>,
    pub source_state_id: String,
    pub applied_state_id: String,
    pub source_dataset_content_id: String,
    pub applied_dataset_content_id: String,
    pub source_record_content_id: String,
    pub applied_record_content_id: String,
    pub outcome: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct JsonlPatchUndoArtifact {
    pub schema_version: u8,
    pub undo_receipt_id: String,
    pub apply_id: String,
    pub patch_id: String,
    pub undo_id: String,
    pub change_id: String,
    pub changeset_id: String,
    pub restored_state_id: String,
    pub restored_dataset_content_id: String,
    pub restored_record_content_id: String,
    pub outcome: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PatchTransactionPhase {
    PreparedApply,
    Applied,
    PreparedUndo,
    Undone,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PatchTransactionJournal {
    namespace: String,
    schema_version: u8,
    phase: PatchTransactionPhase,
    apply_id: String,
    patch_id: String,
    undo_id: String,
    head_revision_id: String,
    change_id: String,
    source_changeset_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    changeset_id: Option<String>,
    source_state_id: String,
    applied_state_id: String,
    source_dataset_content_id: String,
    applied_dataset_content_id: String,
    source_record_content_id: String,
    applied_record_content_id: String,
    line: usize,
    preimage_id: String,
}

impl PatchTransactionJournal {
    fn apply_artifact(&self, outcome: &str) -> JsonlPatchApplyArtifact {
        JsonlPatchApplyArtifact {
            schema_version: PATCH_TRANSACTION_SCHEMA_VERSION,
            apply_id: self.apply_id.clone(),
            patch_id: self.patch_id.clone(),
            undo_id: self.undo_id.clone(),
            change_id: self.change_id.clone(),
            source_changeset_id: self.source_changeset_id.clone(),
            changeset_id: self.changeset_id.clone(),
            source_state_id: self.source_state_id.clone(),
            applied_state_id: self.applied_state_id.clone(),
            source_dataset_content_id: self.source_dataset_content_id.clone(),
            applied_dataset_content_id: self.applied_dataset_content_id.clone(),
            source_record_content_id: self.source_record_content_id.clone(),
            applied_record_content_id: self.applied_record_content_id.clone(),
            outcome: outcome.into(),
        }
    }

    fn undo_artifact(&self, outcome: &str) -> JsonlPatchUndoArtifact {
        JsonlPatchUndoArtifact {
            schema_version: PATCH_TRANSACTION_SCHEMA_VERSION,
            undo_receipt_id: patch_undo_receipt_id(
                &self.apply_id,
                &self.source_state_id,
                &self.source_dataset_content_id,
            ),
            apply_id: self.apply_id.clone(),
            patch_id: self.patch_id.clone(),
            undo_id: self.undo_id.clone(),
            change_id: self.change_id.clone(),
            changeset_id: self.source_changeset_id.clone(),
            restored_state_id: self.source_state_id.clone(),
            restored_dataset_content_id: self.source_dataset_content_id.clone(),
            restored_record_content_id: self.source_record_content_id.clone(),
            outcome: outcome.into(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.namespace != crate::identity::ARTIFACT_NAMESPACE
            || self.schema_version != PATCH_TRANSACTION_SCHEMA_VERSION
            || self.line == 0
        {
            bail!("unsupported patch transaction journal");
        }
        validate_patch_digest(&self.apply_id, "apply_", "apply ID")?;
        validate_patch_digest(&self.patch_id, "patch_", "patch ID")?;
        validate_patch_digest(&self.undo_id, "undo_", "undo ID")?;
        validate_patch_digest(&self.preimage_id, "preimage_", "preimage ID")?;
        Ok(())
    }
}

pub fn apply_jsonl_patch(
    state: &Path,
    request_path: &Path,
    report_path: &Path,
    change_id: &str,
    changeset_id: &str,
    accepted_patch_id: &str,
) -> Result<JsonlPatchApplyArtifact> {
    #[cfg(not(unix))]
    {
        let _ = (
            state,
            request_path,
            report_path,
            change_id,
            changeset_id,
            accepted_patch_id,
        );
        bail!("guarded JSONL patch apply currently requires Unix");
    }
    #[cfg(unix)]
    {
        validate_patch_digest(accepted_patch_id, "patch_", "accepted patch ID")?;
        let (state_dir, workspace) = load_workspace_compatible(state)?;
        if workspace.adapter != "jsonl" || workspace.quality_policy_id().is_none() {
            return Err(InvalidArgumentError::new(
                "JSONL patch apply requires a keyed workspace with a pinned quality policy",
            )
            .into());
        }
        if let Some(mut journal) = find_patch_transaction(&state_dir, accepted_patch_id)? {
            if journal.change_id != change_id || journal.source_changeset_id != changeset_id {
                return Err(PatchConflictError::new(
                    "patch transaction is bound to a different staged candidate",
                )
                .into());
            }
            let (_, current_head, _) = resolve_record_head(&state_dir, &workspace)?;
            if current_head.revision_id() != journal.head_revision_id {
                return Err(PatchConflictError::new(
                    "workspace HEAD changed after the patch transaction was prepared",
                )
                .into());
            }
            let live = scan_live_record_state(&workspace)?;
            if live.state().record_state_id() == journal.applied_state_id
                && live.state().dataset_content_id() == journal.applied_dataset_content_id
            {
                if journal.phase == PatchTransactionPhase::PreparedApply {
                    journal.phase = PatchTransactionPhase::Applied;
                    save_patch_journal(&state_dir, &journal)?;
                }
                if journal.phase == PatchTransactionPhase::Applied {
                    publish_patch_receipt(&state_dir, &journal)?;
                    return Ok(journal.apply_artifact("already_applied"));
                }
            }
            if journal.phase != PatchTransactionPhase::PreparedApply
                || live.state().record_state_id() != journal.source_state_id
                || live.state().dataset_content_id() != journal.source_dataset_content_id
            {
                return Err(PatchConflictError::new(
                    "live dataset does not match the recoverable patch transaction",
                )
                .into());
            }
        }

        let preview = preview_jsonl_patch(
            &state_dir,
            request_path,
            report_path,
            change_id,
            changeset_id,
        )?;
        if preview.patch_id != accepted_patch_id {
            return Err(PatchNotAuthorizedError.into());
        }
        let request = JsonlPatchRequest::from_path(request_path)?;
        let context =
            load_record_changeset_context(&state_dir, &workspace, change_id, changeset_id)?;
        let policy = WorkspaceStore::open(&state_dir)?.load_jsonl_quality_policy(
            workspace
                .quality_policy_id()
                .expect("validated patch apply workspace"),
        )?;
        if policy.is_unique_field(&preview.field) {
            return Err(InvalidArgumentError::new(
                "unique-field repairs are not supported by guarded patch apply",
            )
            .into());
        }
        let prepared = prepare_jsonl_patch(
            &request,
            Path::new(workspace.dataset_path()),
            workspace.id_field().expect("validated JSONL workspace"),
            &preview.finding_code,
            &preview.field,
            preview.line,
            change_id,
            changeset_id,
            &preview.source_dataset_content_id,
        )?;
        if prepared.preview.patch_id != preview.patch_id {
            return Err(ConcurrentModificationError::new(
                "patch request changed after verification",
            )
            .into());
        }
        let existing = find_patch_transaction(&state_dir, accepted_patch_id)?;
        let undo_id = existing
            .as_ref()
            .map(|journal| journal.undo_id.clone())
            .unwrap_or(generate_private_id("undo")?);
        let mut replacement = prepare_patch_replacement(
            Path::new(workspace.dataset_path()),
            preview.line,
            &prepared.preimage,
            &prepared.postimage,
            context.candidate.state().dataset_content_id(),
        )?;
        let applied = build_jsonl_record_state(
            replacement.path(),
            workspace.id_field().expect("validated JSONL workspace"),
        )?
        .bundle();
        validate_patch_delta(&context.candidate, &applied, &request, &prepared.preview)?;
        let store = WorkspaceStore::open(&state_dir)?;
        let applied_state_id = store.publish_record_state_bundle(&applied)?;
        let replacement_changeset =
            if applied.state().record_state_id() == context.baseline.state().record_state_id() {
                None
            } else {
                let summary = record_changeset_summary(&context.baseline, &applied)?;
                let changeset = DatasetChangeset::new_record(
                    context.change.change_id().into(),
                    context.changeset.dataset_id().into(),
                    context.head.revision_id().into(),
                    context.baseline.state().record_state_id().into(),
                    applied_state_id.clone(),
                    summary,
                )?;
                store.publish_changeset(&changeset)?;
                Some(changeset.changeset_id().to_owned())
            };
        let apply_id = patch_apply_id(
            accepted_patch_id,
            context.candidate.state().record_state_id(),
            applied.state().record_state_id(),
            replacement_changeset.as_deref(),
        );
        let mut journal = PatchTransactionJournal {
            namespace: crate::identity::ARTIFACT_NAMESPACE.into(),
            schema_version: PATCH_TRANSACTION_SCHEMA_VERSION,
            phase: PatchTransactionPhase::PreparedApply,
            apply_id,
            patch_id: accepted_patch_id.into(),
            undo_id,
            head_revision_id: context.head.revision_id().into(),
            change_id: change_id.into(),
            source_changeset_id: changeset_id.into(),
            changeset_id: replacement_changeset,
            source_state_id: context.candidate.state().record_state_id().into(),
            applied_state_id: applied.state().record_state_id().into(),
            source_dataset_content_id: context.candidate.state().dataset_content_id().into(),
            applied_dataset_content_id: applied.state().dataset_content_id().into(),
            source_record_content_id: preview.source_record_content_id.clone(),
            applied_record_content_id: preview.predicted_record_content_id.clone(),
            line: preview.line,
            preimage_id: patch_preimage_id(&prepared.preimage),
        };
        save_patch_preimage(&state_dir, &journal, &prepared.preimage)?;
        save_patch_journal(&state_dir, &journal)?;
        patch_test_failpoint("apply_after_prepared");
        let final_context =
            load_record_changeset_context(&state_dir, &workspace, change_id, changeset_id)?;
        let final_live = scan_live_record_state(&workspace)?;
        if final_context.head.revision_id() != journal.head_revision_id
            || final_live.state().record_state_id() != journal.source_state_id
            || final_live.state().dataset_content_id() != journal.source_dataset_content_id
        {
            return Err(ConcurrentModificationError::new(
                "JSONL patch source changed before atomic publication",
            )
            .into());
        }
        replacement.publish(&journal.applied_dataset_content_id)?;
        patch_test_failpoint("apply_after_rename");
        journal.phase = PatchTransactionPhase::Applied;
        save_patch_journal(&state_dir, &journal)?;
        publish_patch_receipt(&state_dir, &journal)?;
        Ok(journal.apply_artifact("applied"))
    }
}

pub fn undo_jsonl_patch(state: &Path, undo_id: &str) -> Result<JsonlPatchUndoArtifact> {
    #[cfg(not(unix))]
    {
        let _ = (state, undo_id);
        bail!("guarded JSONL patch undo currently requires Unix");
    }
    #[cfg(unix)]
    {
        validate_patch_digest(undo_id, "undo_", "undo ID")?;
        let (state_dir, workspace) = load_workspace_compatible(state)?;
        if workspace.adapter != "jsonl" {
            return Err(InvalidArgumentError::new(
                "JSONL patch undo requires a keyed JSONL workspace",
            )
            .into());
        }
        let mut journal = load_patch_journal(&state_dir, undo_id)?;
        let (_, head, _) = resolve_record_head(&state_dir, &workspace)?;
        if head.revision_id() != journal.head_revision_id {
            return Err(PatchConflictError::new(
                "workspace HEAD changed after the patch was applied",
            )
            .into());
        }
        let live = scan_live_record_state(&workspace)?;
        if live.state().record_state_id() == journal.source_state_id
            && live.state().dataset_content_id() == journal.source_dataset_content_id
        {
            if journal.phase != PatchTransactionPhase::Undone {
                journal.phase = PatchTransactionPhase::Undone;
                save_patch_journal(&state_dir, &journal)?;
            }
            publish_patch_undo_receipt(&state_dir, &journal)?;
            return Ok(journal.undo_artifact("already_undone"));
        }
        if !matches!(
            journal.phase,
            PatchTransactionPhase::Applied | PatchTransactionPhase::PreparedUndo
        ) || live.state().record_state_id() != journal.applied_state_id
            || live.state().dataset_content_id() != journal.applied_dataset_content_id
        {
            return Err(PatchConflictError::new(
                "live dataset changed after the patch was applied",
            )
            .into());
        }
        let preimage = load_patch_preimage(&state_dir, &journal)?;
        let current_line = read_physical_line(Path::new(workspace.dataset_path()), journal.line)?;
        let mut replacement = prepare_patch_replacement(
            Path::new(workspace.dataset_path()),
            journal.line,
            &current_line,
            &preimage,
            &journal.applied_dataset_content_id,
        )?;
        let restored = build_jsonl_record_state(
            replacement.path(),
            workspace.id_field().expect("validated JSONL workspace"),
        )?
        .bundle();
        if restored.state().record_state_id() != journal.source_state_id
            || restored.state().dataset_content_id() != journal.source_dataset_content_id
        {
            return Err(PatchConflictError::new(
                "patch undo preimage does not restore the verified source",
            )
            .into());
        }
        journal.phase = PatchTransactionPhase::PreparedUndo;
        save_patch_journal(&state_dir, &journal)?;
        patch_test_failpoint("undo_after_prepared");
        let final_live = scan_live_record_state(&workspace)?;
        let (_, final_head, _) = resolve_record_head(&state_dir, &workspace)?;
        if final_head.revision_id() != journal.head_revision_id
            || final_live.state().record_state_id() != journal.applied_state_id
            || final_live.state().dataset_content_id() != journal.applied_dataset_content_id
        {
            return Err(ConcurrentModificationError::new(
                "JSONL patch source changed before undo publication",
            )
            .into());
        }
        replacement.publish(&journal.source_dataset_content_id)?;
        patch_test_failpoint("undo_after_rename");
        journal.phase = PatchTransactionPhase::Undone;
        save_patch_journal(&state_dir, &journal)?;
        publish_patch_undo_receipt(&state_dir, &journal)?;
        Ok(journal.undo_artifact("undone"))
    }
}

#[cfg(unix)]
fn validate_patch_delta(
    before: &JsonlRecordStateBundle,
    after: &JsonlRecordStateBundle,
    request: &JsonlPatchRequest,
    preview: &JsonlPatchPreviewArtifact,
) -> Result<()> {
    let diff = diff_jsonl_record_states(before, after)?;
    let (added, removed, modified, moved, _) = diff.summary_counts();
    if (added, removed, modified, moved) != (0, 0, 1, 0) {
        return Err(PatchConflictError::new(
            "guarded patch result does not contain exactly one modified record",
        )
        .into());
    }
    let location = after
        .locate_record(request.record_id())?
        .ok_or_else(|| PatchConflictError::new("guarded patch target disappeared"))?;
    if location.line != preview.line
        || location.record_content_id != preview.predicted_record_content_id
    {
        return Err(PatchConflictError::new(
            "guarded patch result does not match its predicted record identity",
        )
        .into());
    }
    Ok(())
}

#[cfg(unix)]
#[derive(Clone)]
struct TransferableMetadata {
    mode: u32,
    uid: u32,
    gid: u32,
    atime: i64,
    atime_nsec: i64,
    mtime: i64,
    mtime_nsec: i64,
    xattrs: Vec<(std::ffi::CString, Vec<u8>)>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Eq, PartialEq)]
struct DirectFileIdentity {
    device: u64,
    inode: u64,
    size: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
}

#[cfg(unix)]
struct PreparedPatchReplacement {
    source_path: PathBuf,
    parent_path: PathBuf,
    source_name: std::ffi::OsString,
    temp_path: PathBuf,
    temp_name: std::ffi::OsString,
    temp_file: File,
    temp_identity: DirectFileIdentity,
    source_file: File,
    source_identity: DirectFileIdentity,
    parent_identity: (u64, u64),
    metadata: TransferableMetadata,
    armed: bool,
}

#[cfg(unix)]
impl PreparedPatchReplacement {
    fn path(&self) -> &Path {
        &self.temp_path
    }

    fn publish(&mut self, expected_dataset_content_id: &str) -> Result<()> {
        use std::os::unix::fs::MetadataExt;

        let source_metadata = fs::symlink_metadata(&self.source_path)
            .map_err(|_| ConcurrentModificationError::new("JSONL patch source path changed"))?;
        if source_metadata.file_type().is_symlink()
            || direct_file_identity(&source_metadata) != self.source_identity
        {
            return Err(ConcurrentModificationError::new(
                "JSONL patch source identity changed before publication",
            )
            .into());
        }
        let parent_metadata = fs::metadata(&self.parent_path)
            .map_err(|_| ConcurrentModificationError::new("JSONL patch source parent changed"))?;
        if (parent_metadata.dev(), parent_metadata.ino()) != self.parent_identity {
            return Err(ConcurrentModificationError::new(
                "JSONL patch source parent changed before publication",
            )
            .into());
        }
        let temp_metadata = fs::symlink_metadata(&self.temp_path)
            .map_err(|_| ConcurrentModificationError::new("JSONL patch replacement changed"))?;
        if temp_metadata.file_type().is_symlink()
            || temp_metadata.nlink() != 1
            || direct_file_identity(&temp_metadata) != self.temp_identity
        {
            return Err(ConcurrentModificationError::new(
                "JSONL patch replacement identity changed before publication",
            )
            .into());
        }
        apply_transferable_metadata(&self.metadata, &self.temp_file)?;
        self.temp_file.sync_all()?;
        let actual_dataset_content_id = hash_jsonl_recordset_file(&self.temp_file)?;
        if actual_dataset_content_id != expected_dataset_content_id {
            return Err(ConcurrentModificationError::new(
                "JSONL patch replacement bytes changed before publication",
            )
            .into());
        }
        let parent =
            cap_std::fs::Dir::open_ambient_dir(&self.parent_path, cap_std::ambient_authority())
                .context("cannot open guarded patch source parent")?;
        parent
            .rename(&self.temp_name, &parent, &self.source_name)
            .context("cannot atomically publish guarded JSONL patch")?;
        File::open(&self.parent_path)?.sync_all()?;
        self.armed = false;
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for PreparedPatchReplacement {
    fn drop(&mut self) {
        if self.armed {
            let _ = fs::remove_file(&self.temp_path);
        }
        let _ = fs2::FileExt::unlock(&self.source_file);
    }
}

#[cfg(unix)]
fn prepare_patch_replacement(
    source: &Path,
    target_line: usize,
    expected_line: &[u8],
    replacement_line: &[u8],
    expected_dataset_content_id: &str,
) -> Result<PreparedPatchReplacement> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    if target_line == 0 {
        return Err(InvalidArgumentError::new("JSONL patch line must be positive").into());
    }
    let parent_path = source
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .context("cannot resolve JSONL patch source parent")?;
    let source_name = source
        .file_name()
        .context("JSONL patch source must name a file")?
        .to_os_string();
    let source_path = parent_path.join(&source_name);
    let source_file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(&source_path)
        .map_err(|_| InvalidArgumentError::new("cannot open guarded JSONL patch source"))?;
    fs2::FileExt::try_lock_exclusive(&source_file).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            anyhow::Error::new(WorkspaceBusyError)
        } else {
            anyhow::Error::new(error).context("cannot lock guarded JSONL patch source")
        }
    })?;
    let source_metadata = source_file
        .metadata()
        .context("cannot inspect guarded JSONL patch source")?;
    if !source_metadata.is_file()
        || source_metadata.nlink() != 1
        || source_metadata.mode() & 0o6000 != 0
    {
        return Err(InvalidArgumentError::new(
            "guarded JSONL patch source must be a direct, singly linked regular file without set-ID bits",
        )
        .into());
    }
    cleanup_stale_patch_replacements(&parent_path, &source_name)?;
    let source_identity = direct_file_identity(&source_metadata);
    let parent_metadata = fs::metadata(&parent_path)?;
    let parent_identity = (parent_metadata.dev(), parent_metadata.ino());
    let metadata = capture_transferable_metadata(&source_file, &source_metadata)?;
    let temp_name = format!(
        ".{}.datajig-{}.tmp.jsonl",
        source_name.to_string_lossy(),
        generate_private_id("temp")?
    );
    let temp_path = parent_path.join(&temp_name);
    let mut temp_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(&temp_path)
        .context("cannot create guarded JSONL patch replacement")?;
    fs2::FileExt::try_lock_exclusive(&temp_file)
        .context("cannot lock guarded JSONL patch replacement")?;
    let mut reader = BufReader::new(source_file.try_clone()?);
    reader.seek(SeekFrom::Start(0))?;
    let mut line = Vec::new();
    let mut current = 0usize;
    let mut replaced = false;
    let mut source_hasher = blake3::Hasher::new();
    source_hasher.update(b"datajig-jsonl-recordset-v1\0");
    loop {
        line.clear();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        current = current
            .checked_add(1)
            .context("JSONL patch line count overflow")?;
        source_hasher.update(&line);
        if current == target_line {
            if line != expected_line {
                return Err(ConcurrentModificationError::new(
                    "JSONL patch target bytes changed after verification",
                )
                .into());
            }
            temp_file.write_all(replacement_line)?;
            replaced = true;
        } else {
            temp_file.write_all(&line)?;
        }
    }
    if !replaced {
        return Err(ConcurrentModificationError::new(
            "JSONL patch target line disappeared after verification",
        )
        .into());
    }
    let actual_source_id = format!("records_{}", source_hasher.finalize().to_hex());
    if actual_source_id != expected_dataset_content_id {
        return Err(ConcurrentModificationError::new(
            "JSONL patch source bytes changed after verification",
        )
        .into());
    }
    temp_file.flush()?;
    apply_transferable_metadata(&metadata, &temp_file)?;
    temp_file.sync_all()?;
    let temp_identity = direct_file_identity(
        &temp_file
            .metadata()
            .context("cannot inspect guarded patch replacement")?,
    );
    Ok(PreparedPatchReplacement {
        source_path,
        parent_path,
        source_name,
        temp_path,
        temp_name: temp_name.into(),
        temp_file,
        temp_identity,
        source_file,
        source_identity,
        parent_identity,
        metadata,
        armed: true,
    })
}

#[cfg(unix)]
fn cleanup_stale_patch_replacements(parent: &Path, source_name: &std::ffi::OsStr) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let prefix = format!(".{}.datajig-", source_name.to_string_lossy());
    let suffix = ".tmp.jsonl";
    let mut removed = false;
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(private_id) = name
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_suffix(suffix))
        else {
            continue;
        };
        if validate_patch_digest(private_id, "temp_", "patch replacement temporary ID").is_err() {
            continue;
        }
        let metadata = fs::symlink_metadata(entry.path())?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.permissions().mode() & 0o6000 != 0
        {
            bail!("stale patch replacement must be a direct, singly linked regular file");
        }
        fs::remove_file(entry.path()).context("cannot remove stale patch replacement")?;
        removed = true;
    }
    if removed {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(unix)]
fn direct_file_identity(metadata: &fs::Metadata) -> DirectFileIdentity {
    use std::os::unix::fs::MetadataExt;
    DirectFileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        size: metadata.len(),
        modified_seconds: metadata.mtime(),
        modified_nanoseconds: metadata.mtime_nsec(),
    }
}

#[cfg(unix)]
fn hash_jsonl_recordset_file(file: &File) -> Result<String> {
    let mut reader = file.try_clone()?;
    reader.seek(SeekFrom::Start(0))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-jsonl-recordset-v1\0");
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("records_{}", hasher.finalize().to_hex()))
}

#[cfg(unix)]
fn capture_transferable_metadata(
    file: &File,
    metadata: &fs::Metadata,
) -> Result<TransferableMetadata> {
    use std::os::unix::fs::MetadataExt;

    let mut names = vec![0_u8; 1024 * 1024];
    let name_bytes = rustix::fs::flistxattr(file, &mut names)
        .context("cannot list JSONL patch source extended attributes")?;
    names.truncate(name_bytes);
    let mut xattrs = Vec::new();
    for raw_name in names.split_inclusive(|byte| *byte == 0) {
        if raw_name.is_empty() || raw_name == [0] {
            continue;
        }
        if xattrs.len() >= 256 {
            bail!("JSONL patch source has too many extended attributes");
        }
        let name = std::ffi::CString::from_vec_with_nul(raw_name.to_vec())
            .context("JSONL patch source has an invalid extended attribute name")?;
        let mut value = vec![0_u8; 1024 * 1024];
        let value_bytes = rustix::fs::fgetxattr(file, name.as_c_str(), &mut value)
            .context("cannot read JSONL patch source extended attribute")?;
        value.truncate(value_bytes);
        xattrs.push((name, value));
    }
    Ok(TransferableMetadata {
        mode: metadata.mode(),
        uid: metadata.uid(),
        gid: metadata.gid(),
        atime: metadata.atime(),
        atime_nsec: metadata.atime_nsec(),
        mtime: metadata.mtime(),
        mtime_nsec: metadata.mtime_nsec(),
        xattrs,
    })
}

#[cfg(unix)]
fn apply_transferable_metadata(metadata: &TransferableMetadata, file: &File) -> Result<()> {
    use rustix::fs::{Gid, Mode, Timespec, Timestamps, Uid, XattrFlags};

    rustix::fs::fchown(
        file,
        Some(Uid::from_raw(metadata.uid)),
        Some(Gid::from_raw(metadata.gid)),
    )
    .context("cannot preserve JSONL patch source ownership")?;
    // `rustix::fs::RawMode` is `u32` on Linux and `u16` on Apple platforms.
    #[allow(clippy::useless_conversion)]
    let mode = metadata
        .mode
        .try_into()
        .context("JSONL patch source permissions exceed the platform mode range")?;
    rustix::fs::fchmod(file, Mode::from_bits_truncate(mode))
        .context("cannot preserve JSONL patch source permissions")?;
    for (name, value) in &metadata.xattrs {
        rustix::fs::fsetxattr(file, name.as_c_str(), value, XattrFlags::empty())
            .context("cannot preserve JSONL patch source extended attribute")?;
    }
    rustix::fs::futimens(
        file,
        &Timestamps {
            last_access: Timespec {
                tv_sec: metadata.atime,
                tv_nsec: metadata.atime_nsec,
            },
            last_modification: Timespec {
                tv_sec: metadata.mtime,
                tv_nsec: metadata.mtime_nsec,
            },
        },
    )
    .context("cannot preserve JSONL patch source timestamps")?;
    Ok(())
}

#[cfg(unix)]
fn read_physical_line(source: &Path, target_line: usize) -> Result<Vec<u8>> {
    use std::os::unix::fs::OpenOptionsExt;

    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(source)
        .map_err(|_| InvalidArgumentError::new("cannot open guarded JSONL patch source"))?;
    let mut reader = BufReader::new(file);
    let mut line = Vec::new();
    for current in 1..=target_line {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 {
            return Err(PatchConflictError::new("patch target line no longer exists").into());
        }
        if current == target_line {
            return Ok(line);
        }
    }
    Err(PatchConflictError::new("patch target line no longer exists").into())
}

#[cfg(unix)]
fn patch_transaction_root(state_dir: &Path) -> Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt;

    let private = state_dir.join("private");
    let root = private.join("patch-transactions");
    for directory in [&private, &root] {
        fs::create_dir_all(directory)?;
        let metadata = fs::symlink_metadata(directory)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            bail!("private patch transaction path is not a direct directory");
        }
        fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    }
    Ok(root)
}

#[cfg(unix)]
fn patch_transaction_directory(state_dir: &Path, undo_id: &str) -> Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt;

    validate_patch_digest(undo_id, "undo_", "undo ID")?;
    let directory = patch_transaction_root(state_dir)?.join(undo_id);
    fs::create_dir_all(&directory)?;
    let metadata = fs::symlink_metadata(&directory)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("patch transaction path is not a direct directory");
    }
    fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
    Ok(directory)
}

#[cfg(unix)]
fn save_patch_preimage(
    state_dir: &Path,
    journal: &PatchTransactionJournal,
    preimage: &[u8],
) -> Result<()> {
    if patch_preimage_id(preimage) != journal.preimage_id {
        bail!("patch preimage identity does not match its journal");
    }
    let path = patch_transaction_directory(state_dir, &journal.undo_id)?.join("preimage.bin");
    if path.exists() {
        let existing = load_private_patch_file(&path, crate::MAX_JSONL_LINE_BYTES + 2)?;
        if existing != preimage {
            bail!("patch preimage conflicts with an existing transaction");
        }
        return Ok(());
    }
    save_private_patch_file(&path, preimage)
}

#[cfg(unix)]
fn load_patch_preimage(state_dir: &Path, journal: &PatchTransactionJournal) -> Result<Vec<u8>> {
    let path = patch_transaction_directory(state_dir, &journal.undo_id)?.join("preimage.bin");
    let value = load_private_patch_file(&path, crate::MAX_JSONL_LINE_BYTES + 2)?;
    if patch_preimage_id(&value) != journal.preimage_id {
        bail!("patch preimage identity does not match its journal");
    }
    Ok(value)
}

#[cfg(unix)]
fn save_patch_journal(state_dir: &Path, journal: &PatchTransactionJournal) -> Result<()> {
    journal.validate()?;
    let payload = serde_json::to_vec_pretty(journal)?;
    if payload.len() > MAX_PATCH_JOURNAL_BYTES {
        bail!("patch transaction journal is too large");
    }
    let path = patch_transaction_directory(state_dir, &journal.undo_id)?.join("journal.json");
    save_private_patch_file(&path, &payload)
}

#[cfg(unix)]
fn load_patch_journal(state_dir: &Path, undo_id: &str) -> Result<PatchTransactionJournal> {
    let path = patch_transaction_root(state_dir)?
        .join(undo_id)
        .join("journal.json");
    if !path.exists() {
        return Err(UndoNotFoundError.into());
    }
    let payload = load_private_patch_file(&path, MAX_PATCH_JOURNAL_BYTES)?;
    let journal: PatchTransactionJournal =
        serde_json::from_slice(&payload).context("invalid patch transaction journal")?;
    journal.validate()?;
    if journal.undo_id != undo_id {
        bail!("patch transaction identity does not match its directory");
    }
    Ok(journal)
}

#[cfg(unix)]
fn find_patch_transaction(
    state_dir: &Path,
    patch_id: &str,
) -> Result<Option<PatchTransactionJournal>> {
    let root = patch_transaction_root(state_dir)?;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let metadata = entry.file_type()?;
        if !metadata.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if validate_patch_digest(&name, "undo_", "undo ID").is_err() {
            continue;
        }
        let journal_path = entry.path().join("journal.json");
        match fs::symlink_metadata(&journal_path) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        }
        let journal = load_patch_journal(state_dir, &name)?;
        if journal.patch_id == patch_id {
            return Ok(Some(journal));
        }
    }
    Ok(None)
}

#[cfg(unix)]
fn save_private_patch_file(path: &Path, payload: &[u8]) -> Result<()> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let parent = path.parent().context("private patch file has no parent")?;
    let name = path.file_name().context("private patch file has no name")?;
    let temp = parent.join(format!(
        ".{}.{}.tmp",
        name.to_string_lossy(),
        generate_private_id("write")?
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(&temp)?;
        file.write_all(payload)?;
        file.sync_all()?;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        fs::rename(&temp, path)?;
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

#[cfg(unix)]
fn load_private_patch_file(path: &Path, limit: usize) -> Result<Vec<u8>> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let file = OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.mode() & 0o077 != 0
        || metadata.len() > u64::try_from(limit)?
    {
        bail!("private patch transaction file is unsafe or oversized");
    }
    let mut payload = Vec::with_capacity(usize::try_from(metadata.len())?);
    file.take(u64::try_from(limit)? + 1)
        .read_to_end(&mut payload)?;
    if payload.len() > limit {
        bail!("private patch transaction file is oversized");
    }
    Ok(payload)
}

#[cfg(unix)]
fn publish_patch_receipt(state_dir: &Path, journal: &PatchTransactionJournal) -> Result<()> {
    let payload = serde_json::to_vec_pretty(&journal.apply_artifact("applied"))?;
    WorkspaceStore::open(state_dir)?.publish_patch_receipt(&journal.apply_id, &payload)
}

#[cfg(unix)]
fn publish_patch_undo_receipt(state_dir: &Path, journal: &PatchTransactionJournal) -> Result<()> {
    let artifact = journal.undo_artifact("undone");
    let payload = serde_json::to_vec_pretty(&artifact)?;
    WorkspaceStore::open(state_dir)?.publish_patch_undo_receipt(&artifact.undo_receipt_id, &payload)
}

#[cfg(unix)]
fn patch_apply_id(
    patch_id: &str,
    source_state_id: &str,
    applied_state_id: &str,
    changeset_id: Option<&str>,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-jsonl-patch-apply-v1\0");
    for value in [
        patch_id,
        source_state_id,
        applied_state_id,
        changeset_id.unwrap_or("clean"),
    ] {
        hasher.update(value.as_bytes());
        hasher.update(b"\0");
    }
    format!("apply_{}", hasher.finalize().to_hex())
}

#[cfg(unix)]
fn patch_undo_receipt_id(
    apply_id: &str,
    restored_state_id: &str,
    restored_dataset_content_id: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-jsonl-patch-undo-receipt-v1\0");
    for value in [apply_id, restored_state_id, restored_dataset_content_id] {
        hasher.update(value.as_bytes());
        hasher.update(b"\0");
    }
    format!("revert_{}", hasher.finalize().to_hex())
}

#[cfg(unix)]
fn patch_preimage_id(preimage: &[u8]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-jsonl-patch-preimage-v1\0");
    hasher.update(preimage);
    format!("preimage_{}", hasher.finalize().to_hex())
}

#[cfg(unix)]
fn generate_private_id(prefix: &str) -> Result<String> {
    let mut random = [0u8; 32];
    File::open("/dev/urandom")
        .context("cannot open operating-system random source")?
        .read_exact(&mut random)
        .context("cannot generate private patch identity")?;
    Ok(format!("{prefix}_{}", blake3::Hash::from(random).to_hex()))
}

fn validate_patch_digest(value: &str, prefix: &str, name: &str) -> Result<()> {
    let digest = value
        .strip_prefix(prefix)
        .ok_or_else(|| InvalidArgumentError::new(format!("{name} has an invalid prefix")))?;
    blake3::Hash::from_hex(digest)
        .map_err(|_| InvalidArgumentError::new(format!("{name} is invalid")))?;
    Ok(())
}

#[cfg(all(unix, debug_assertions))]
fn patch_test_failpoint(name: &str) {
    if std::env::var_os("DATAJIG_TEST_PATCH_FAILPOINT").as_deref()
        == Some(std::ffi::OsStr::new(name))
    {
        std::process::exit(86);
    }
}

#[cfg(any(not(unix), not(debug_assertions)))]
fn patch_test_failpoint(_name: &str) {}

pub fn is_patchable_quality_code(code: &str) -> bool {
    matches!(
        code,
        "JSONL_POLICY_REQUIRED"
            | "JSONL_POLICY_NULL"
            | "JSONL_POLICY_TYPE"
            | "JSONL_POLICY_ENUM"
            | "JSONL_POLICY_RANGE"
            | "JSONL_POLICY_PATTERN"
    )
}

fn is_structural_finding_code(code: &str) -> bool {
    matches!(
        code,
        "JSONL_INVALID_RECORD"
            | "JSONL_MISSING_ID"
            | "JSONL_NULL_ID"
            | "JSONL_INVALID_ID"
            | "JSONL_DUPLICATE_ID"
    )
}

#[cfg(unix)]
fn load_direct_bounded_report(path: &Path) -> Result<Vec<u8>> {
    use rustix::fs::{Mode, OFlags, open};

    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(|_| InvalidArgumentError::new("workspace review must be a direct regular file"))?;
    let file = fs::File::from(descriptor);
    let metadata = file
        .metadata()
        .context("cannot inspect open workspace review")?;
    if !metadata.is_file() {
        return Err(
            InvalidArgumentError::new("workspace review must be a direct regular file").into(),
        );
    }
    if metadata.len() > MAX_REPORT_BYTES as u64 {
        return Err(InvalidArgumentError::new(format!(
            "workspace review exceeds {MAX_REPORT_BYTES} bytes"
        ))
        .into());
    }
    let mut payload = Vec::with_capacity(usize::try_from(metadata.len())?);
    file.take(u64::try_from(MAX_REPORT_BYTES)? + 1)
        .read_to_end(&mut payload)
        .context("cannot read workspace review")?;
    if payload.len() > MAX_REPORT_BYTES {
        return Err(InvalidArgumentError::new(format!(
            "workspace review exceeds {MAX_REPORT_BYTES} bytes"
        ))
        .into());
    }
    Ok(payload)
}

#[cfg(not(unix))]
fn load_direct_bounded_report(_path: &Path) -> Result<Vec<u8>> {
    Err(InvalidArgumentError::new("finding location is only available on Unix platforms").into())
}

pub fn check_changeset(
    state: &Path,
    threads: usize,
    phash_threshold: usize,
    change_id: &str,
    changeset_id: &str,
) -> Result<WorkspaceCheckArtifact> {
    validate_threads(threads)?;
    if phash_threshold > MAX_PHASH_THRESHOLD {
        return Err(InvalidArgumentError::new(format!(
            "phash threshold must be between 0 and {MAX_PHASH_THRESHOLD}"
        ))
        .into());
    }
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter == "jsonl" {
        return check_jsonl_changeset(&state_dir, &workspace, change_id, changeset_id);
    }
    let context = load_changeset_context(state, change_id, changeset_id)?;
    let live = scan_inventory_for_schema(
        Path::new(context.workspace.dataset_path()),
        threads,
        context.candidate.schema_version(),
    )?;
    if live.content_id()? != context.changeset.candidate_inventory_id() {
        return Err(UnstagedChangesError::new(
            "live dataset differs from the staged changeset; stage it again before check",
        )
        .into());
    }
    let store = WorkspaceStore::open(&context.state_dir)?;
    let report_path = context.state_dir.join(LATEST_REPORT_FILE);
    let artifact = create_review_with_metadata(
        &format!(
            "inventory:{}",
            store
                .inventory_path(context.changeset.base_inventory_id())?
                .to_string_lossy()
        ),
        &format!(
            "inventory:{}",
            store
                .inventory_path(context.changeset.candidate_inventory_id())?
                .to_string_lossy()
        ),
        &report_path,
        threads,
        phash_threshold,
        &[
            ("change_id", context.change.change_id()),
            ("changeset_id", context.changeset.changeset_id()),
        ],
    )?;
    let decision = match artifact.status.as_str() {
        "pass" => "seal",
        "warn" => "inspect",
        "fail" => "fix",
        _ => "retry",
    };
    Ok(WorkspaceCheckArtifact {
        decision: decision.into(),
        findings: artifact.findings,
        report_content_id: artifact.content_id,
        report_path: artifact.output,
        status: artifact.status,
        dataset_coverage: context.candidate.coverage().into(),
        change_id: Some(context.change.change_id().into()),
        changeset_id: Some(context.changeset.changeset_id().into()),
    })
}

pub fn seal_changeset(
    state: &Path,
    threads: usize,
    message: &str,
    accepted_report_id: Option<&str>,
    change_id: &str,
    changeset_id: &str,
) -> Result<WorkspaceSealArtifact> {
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter == "jsonl" {
        return seal_jsonl_changeset(
            &state_dir,
            &workspace,
            message,
            accepted_report_id,
            change_id,
            changeset_id,
        );
    }
    let context = load_changeset_context(state, change_id, changeset_id)?;
    let live = scan_inventory_for_schema(
        Path::new(context.workspace.dataset_path()),
        threads,
        context.candidate.schema_version(),
    )?;
    if live.content_id()? != context.changeset.candidate_inventory_id() {
        return Err(UnstagedChangesError::new(
            "live dataset differs from the staged changeset; stage and check it again",
        )
        .into());
    }
    let mut artifact = seal_workspace_internal(
        state,
        threads,
        message,
        accepted_report_id,
        Some(context.changeset.changeset_id()),
        Some((context.change.change_id(), context.changeset.changeset_id())),
    )?;
    artifact.change_id = Some(context.change.change_id().into());
    artifact.changeset_id = Some(context.changeset.changeset_id().into());
    Ok(artifact)
}

pub fn plan_changeset(
    state: &Path,
    threads: usize,
    change_id: &str,
    changeset_id: &str,
) -> Result<WorkspacePlanArtifact> {
    validate_threads(threads)?;
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter == "jsonl" {
        return plan_jsonl_changeset(&state_dir, &workspace, change_id, changeset_id);
    }
    let context = load_changeset_context(state, change_id, changeset_id)?;
    let live = scan_inventory_for_schema(
        Path::new(context.workspace.dataset_path()),
        threads,
        context.candidate.schema_version(),
    )?;
    if live.content_id()? != context.changeset.candidate_inventory_id() {
        return Err(UnstagedChangesError::new(
            "live dataset differs from the staged changeset; stage and check it again before plan",
        )
        .into());
    }
    let report_path = context.state_dir.join(LATEST_REPORT_FILE);
    let report_payload = fs::read(&report_path).context("cannot read latest workspace review")?;
    if report_payload.len() > MAX_REPORT_BYTES {
        bail!("report is too large (max {MAX_REPORT_BYTES} bytes)");
    }
    let report_text = std::str::from_utf8(&report_payload)
        .context("latest workspace review is not valid UTF-8")?;
    let report =
        ReviewReport::from_json(report_text).context("cannot load latest workspace review")?;
    if report.metadata_string("change_id") != Some(context.change.change_id())
        || report.metadata_string("changeset_id") != Some(context.changeset.changeset_id())
        || report.metadata_string("baseline_inventory_id")
            != Some(context.changeset.base_inventory_id())
        || report.metadata_string("candidate_inventory_id")
            != Some(context.changeset.candidate_inventory_id())
    {
        return Err(UnstagedChangesError::new(
            "latest review is not bound to the requested staged changeset",
        )
        .into());
    }
    let plan = create_remediation_plan_from_payload(
        &report_payload,
        &context.state_dir.join(LATEST_PLAN_FILE),
    )?;
    Ok(WorkspacePlanArtifact {
        decision: plan.decision.clone(),
        plan,
        state_dir: path_text(&context.state_dir, "state directory")?,
    })
}

pub fn revision_log(state: &Path, offset: i64, limit: i64) -> Result<RevisionPage> {
    let offset = usize::try_from(offset)
        .map_err(|_| InvalidArgumentError::new("offset must be non-negative"))?;
    let limit = usize::try_from(limit)
        .ok()
        .filter(|value| (1..=MAX_REVISION_PAGE_SIZE).contains(value))
        .ok_or_else(|| {
            InvalidArgumentError::new(format!(
                "limit must be between 1 and {MAX_REVISION_PAGE_SIZE}"
            ))
        })?;
    let (state_dir, workspace) = load_workspace_without_live_dataset(state)?;
    let store = WorkspaceStore::open(&state_dir)?;
    let refs = store.load_refs()?;
    let head = store.load_revision(refs.head())?;
    validate_workspace_identity(&store, &workspace, &head)?;
    let mut next = Some(refs.head().to_owned());
    let mut visited = HashSet::new();
    let mut traversed = 0usize;
    let mut revisions = Vec::new();
    let required = offset.saturating_add(limit).saturating_add(1);
    while let Some(id) = next {
        if traversed >= MAX_REVISION_TRAVERSAL {
            bail!("revision history exceeds {MAX_REVISION_TRAVERSAL} entries");
        }
        if !visited.insert(id.clone()) {
            bail!("revision history contains a cycle");
        }
        let revision = store.load_revision(&id)?;
        if revision.adapter() == "imagefolder" {
            store.load_inventory(revision.inventory_id())?;
        } else if revision.adapter() == "jsonl" {
            store.load_record_state_bundle(revision.state_id())?;
        } else {
            bail!("unsupported revision adapter {}", revision.adapter());
        }
        next = revision.parent().map(str::to_owned);
        if traversed >= offset && revisions.len() < limit.saturating_add(1) {
            revisions.push(revision);
        }
        traversed += 1;
        if traversed >= required {
            break;
        }
    }
    let has_more = revisions.len() > limit;
    revisions.truncate(limit);
    Ok(RevisionPage {
        has_more,
        limit,
        offset,
        returned: revisions.len(),
        revisions,
    })
}

pub fn materialize_revision(
    state: &Path,
    revision_id: &str,
    output: &Path,
) -> Result<MaterializeArtifact> {
    let (state_dir, workspace) = load_workspace_without_live_dataset(state)?;
    if workspace.adapter != "jsonl" {
        return Err(InvalidArgumentError::new(
            "revision materialization requires a keyed JSONL workspace",
        )
        .into());
    }
    let store = WorkspaceStore::open(&state_dir)?;
    let (revision, bundle) = resolve_reachable_record_revision(&store, &workspace, revision_id)?;
    let record_state = bundle.state();
    if record_state.dataset_content_id() != revision.dataset_content_id() {
        bail!("revision record state does not match its dataset content identity");
    }
    let parent = output
        .parent()
        .context("materialized output has no parent")?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    fs::create_dir_all(parent).context("cannot create materialized output directory")?;
    let parent = parent
        .canonicalize()
        .context("cannot resolve materialized output directory")?;
    let name = output
        .file_name()
        .context("materialized output has no file name")?;
    let resolved_output = parent.join(name);
    if parent.starts_with(&state_dir) {
        return Err(InvalidArgumentError::new(
            "materialized output must be outside the workspace state directory",
        )
        .into());
    }
    let live = Path::new(workspace.dataset_path());
    if resolved_output == live {
        return Err(InvalidArgumentError::new(
            "materialized output must differ from the tracked dataset",
        )
        .into());
    }
    let result = store.materialize_jsonl_blob(
        revision.dataset_content_id(),
        record_state.byte_count(),
        &resolved_output,
    )?;
    Ok(MaterializeArtifact {
        schema_version: 1,
        status: result.status.into(),
        adapter: "jsonl",
        revision_id: revision.revision_id().into(),
        state_id: revision.state_id().into(),
        dataset_content_id: revision.dataset_content_id().into(),
        id_field: record_state.id_field().into(),
        bytes: result.bytes,
        records: record_state.record_count(),
        output: path_text(&resolved_output, "materialized output")?,
        content_verified: true,
    })
}

fn resolve_reachable_record_revision(
    store: &WorkspaceStore,
    workspace: &WorkspaceConfig,
    revision_id: &str,
) -> Result<(DatasetRevision, JsonlRecordStateBundle)> {
    let refs = store.load_refs()?;
    let head = store.load_revision(refs.head())?;
    validate_workspace_identity(store, workspace, &head)?;
    let mut current = Some(refs.head().to_owned());
    let mut seen = BTreeSet::new();
    let mut selected = None;
    for _ in 0..MAX_REVISION_TRAVERSAL {
        let Some(id) = current else { break };
        if !seen.insert(id.clone()) {
            bail!("revision history contains a cycle");
        }
        let revision = store.load_revision(&id)?;
        current = revision.parent().map(str::to_owned);
        if id == revision_id {
            selected = Some(revision);
            break;
        }
    }
    let revision = selected.ok_or_else(|| {
        InvalidArgumentError::new("revision is not reachable from this workspace HEAD")
    })?;
    if revision.adapter() != "jsonl" {
        return Err(InvalidArgumentError::new("revision is not a keyed JSONL revision").into());
    }
    let bundle = store.load_record_state_bundle(revision.state_id())?;
    if bundle.state().dataset_content_id() != revision.dataset_content_id() {
        bail!("revision record state does not match its dataset content identity");
    }
    if bundle.state().id_field() != workspace.id_field().expect("validated JSONL workspace") {
        bail!("revision record state id field does not match workspace id field");
    }
    Ok((revision, bundle))
}

pub fn seal_workspace(
    state: &Path,
    threads: usize,
    message: &str,
    accepted_report_id: Option<&str>,
) -> Result<WorkspaceSealArtifact> {
    seal_workspace_internal(state, threads, message, accepted_report_id, None, None)
}

fn seal_workspace_internal(
    state: &Path,
    threads: usize,
    message: &str,
    accepted_report_id: Option<&str>,
    revision_task_id: Option<&str>,
    report_binding: Option<(&str, &str)>,
) -> Result<WorkspaceSealArtifact> {
    validate_threads(threads)?;
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if message.is_empty() || message.len() > 4_096 {
        return Err(InvalidArgumentError::new("message must contain 1 to 4096 UTF-8 bytes").into());
    }
    let (dataset_id, parent, baseline) = resolve_head(&state_dir, &workspace)?;
    let report_path = state_dir.join(LATEST_REPORT_FILE);
    let report_payload = fs::read(&report_path).context("cannot read latest workspace review")?;
    if report_payload.len() > MAX_REPORT_BYTES {
        bail!("report is too large (max {MAX_REPORT_BYTES} bytes)");
    }
    let report_text = std::str::from_utf8(&report_payload)
        .context("latest workspace review is not valid UTF-8")?;
    let report =
        ReviewReport::from_json(report_text).context("cannot load latest workspace review")?;
    if let Some((change_id, changeset_id)) = report_binding {
        if report.metadata_string("change_id") != Some(change_id)
            || report.metadata_string("changeset_id") != Some(changeset_id)
        {
            return Err(UnstagedChangesError::new(
                "latest workspace review is not bound to the requested staged changeset",
            )
            .into());
        }
    }
    if report.status() != "pass" {
        bail!("latest workspace review must pass before it can be sealed");
    }
    let expected_baseline = report
        .metadata_string("baseline_inventory_id")
        .context("latest review is missing its baseline inventory identity")?;
    let baseline_id = baseline.content_id()?;
    if expected_baseline != baseline_id {
        bail!("workspace baseline changed after the latest review");
    }
    let dataset = Path::new(workspace.dataset_path());
    let candidate = scan_inventory_for_schema(dataset, threads, baseline.schema_version())?;
    let candidate_id = candidate.content_id()?;
    let expected_candidate = report
        .metadata_string("candidate_inventory_id")
        .context("latest review is missing its candidate inventory identity")?;
    if expected_candidate != candidate_id {
        bail!("workspace candidate changed after the latest review");
    }
    let report_id = crate::report::report_content_id(&report_payload);
    let finding_count = report.indexed_findings()?.len();
    let accepted_report_id = match (finding_count, accepted_report_id) {
        (0, None) => None,
        (_, Some(value)) if value == report_id => Some(report_id),
        (0, Some(_)) => bail!("accepted report does not match the latest workspace review"),
        (_, None) => bail!("a passing review with findings requires --accept-report {report_id}"),
        (_, Some(_)) => bail!("accepted report does not match the latest workspace review"),
    };

    let store = WorkspaceStore::open(&state_dir)?;
    store.publish_inventory(&candidate)?;
    let revision = DatasetRevision::new(
        Some(parent.revision_id().to_owned()),
        workspace.adapter.clone(),
        candidate_id.clone(),
        candidate_id.clone(),
        accepted_report_id.clone(),
        RevisionProvenance::new(
            "agent".into(),
            revision_task_id.map(str::to_owned),
            env!("CARGO_PKG_VERSION").into(),
            message.into(),
        )?,
        now_unix_ns()?,
    )?;
    store.publish_revision(&revision)?;
    let final_candidate_id =
        scan_inventory_for_schema(dataset, threads, baseline.schema_version())?.content_id()?;
    if final_candidate_id != candidate_id {
        return Err(ConcurrentModificationError::new(
            "workspace candidate changed while it was being sealed",
        )
        .into());
    }
    store.compare_and_swap_head(Some(parent.revision_id()), revision.revision_id())?;
    let committed = revision;
    Ok(WorkspaceSealArtifact {
        accepted_report_id,
        baseline_inventory_id: Some(candidate_id),
        baseline_state_id: None,
        dataset_id,
        dataset_coverage: candidate.coverage().into(),
        dataset_root: Some(workspace.dataset_path().into()),
        dataset_path: None,
        parent_revision_id: parent.revision_id().to_owned(),
        revision_id: committed.revision_id().to_owned(),
        state_dir: path_text(&state_dir, "state directory")?,
        change_id: None,
        changeset_id: None,
    })
}

pub fn plan_workspace(state: &Path) -> Result<WorkspacePlanArtifact> {
    let (state_dir, workspace) = load_workspace_compatible(state)?;
    if workspace.adapter == "jsonl" {
        return Err(InvalidArgumentError::new(
            "JSONL workspace plan requires --change and --changeset",
        )
        .into());
    }
    let report_path = state_dir.join(LATEST_REPORT_FILE);
    let plan = create_remediation_plan(&report_path, &state_dir.join(LATEST_PLAN_FILE))?;
    Ok(WorkspacePlanArtifact {
        decision: plan.decision.clone(),
        plan,
        state_dir: path_text(&state_dir, "state directory")?,
    })
}

fn load_workspace_compatible(state: &Path) -> Result<(PathBuf, WorkspaceConfig)> {
    load_workspace(state, true)
}

fn load_workspace_without_live_dataset(state: &Path) -> Result<(PathBuf, WorkspaceConfig)> {
    load_workspace(state, false)
}

fn load_workspace(state: &Path, require_live_dataset: bool) -> Result<(PathBuf, WorkspaceConfig)> {
    let state = state
        .canonicalize()
        .context("cannot resolve workspace state directory")?;
    let _ = path_text(&state, "state directory")?;
    let path = state.join(WORKSPACE_FILE);
    let metadata = fs::metadata(&path).context("cannot inspect workspace configuration")?;
    if metadata.len() > MAX_WORKSPACE_BYTES as u64 {
        bail!("workspace configuration exceeds {MAX_WORKSPACE_BYTES} bytes");
    }
    let payload = fs::read(&path).context("cannot read workspace configuration")?;
    let value: serde_json::Value =
        serde_json::from_slice(&payload).context("invalid workspace configuration")?;
    let schema = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .context("workspace schema_version must be an integer")?;
    if ![u64::from(WORKSPACE_SCHEMA_VERSION), 3, 4].contains(&schema) {
        bail!("unsupported workspace schema {schema}");
    }
    let workspace: WorkspaceConfig =
        serde_json::from_value(value).context("invalid workspace configuration")?;
    workspace.validate()?;
    if let Some(policy_id) = workspace.quality_policy_id() {
        WorkspaceStore::open(&state)?.load_jsonl_quality_policy(policy_id)?;
    }
    if !require_live_dataset {
        return Ok((state, workspace));
    }
    let dataset_path = Path::new(workspace.dataset_path());
    let dataset = if workspace.adapter == "jsonl" {
        ensure_jsonl_dataset_is_direct_file(dataset_path)?;
        dataset_path.to_path_buf()
    } else {
        dataset_path
            .canonicalize()
            .context("cannot resolve workspace dataset")?
    };
    if (workspace.adapter == "imagefolder" && !dataset.is_dir())
        || (workspace.adapter == "jsonl" && !dataset.is_file())
    {
        bail!("workspace dataset path has the wrong kind for its adapter");
    }
    if workspace.adapter == "imagefolder" && (state == dataset || state.starts_with(&dataset)) {
        bail!("workspace state must stay outside the dataset root");
    }
    Ok((state, workspace))
}

fn resolve_head(
    state_dir: &Path,
    workspace: &WorkspaceConfig,
) -> Result<(String, DatasetRevision, crate::DatasetInventory)> {
    if workspace.adapter != "imagefolder" {
        bail!("workspace is not an imagefolder workspace");
    }
    let store = WorkspaceStore::open(state_dir)?;
    let refs = store.load_refs()?;
    let revision = store.load_revision(refs.head())?;
    validate_workspace_identity(&store, workspace, &revision)?;
    let inventory = store.load_inventory(revision.inventory_id())?;
    Ok((workspace.dataset_id.clone(), revision, inventory))
}

fn validate_change_anchor(
    change: &ChangeDeclaration,
    workspace: &WorkspaceConfig,
    dataset_id: &str,
    head: &DatasetRevision,
    baseline: &crate::DatasetInventory,
) -> Result<()> {
    if change.dataset_id() != dataset_id
        || change.adapter() != workspace.adapter
        || change.base_inventory_id() != baseline.content_id()?
    {
        bail!("change declaration does not belong to this dataset workspace");
    }
    if change.base_revision_id() != head.revision_id() {
        return Err(ConcurrentModificationError::new(
            "workspace HEAD changed after the change declaration was created",
        )
        .into());
    }
    Ok(())
}

fn validate_workspace_identity(
    store: &WorkspaceStore,
    workspace: &WorkspaceConfig,
    head: &DatasetRevision,
) -> Result<()> {
    let mut current = head.clone();
    let mut visited = HashSet::new();
    for _ in 0..MAX_REVISION_TRAVERSAL {
        if current.adapter() != workspace.adapter {
            bail!("revision adapter does not match workspace adapter");
        }
        if let ("jsonl", Some(report_id)) =
            (workspace.adapter.as_str(), current.accepted_report_id())
        {
            let report = store.load_review(report_id)?;
            let expected_candidate = format!("recordstate:{}", current.state_id());
            if report.candidate() != expected_candidate || report.status() != "pass" {
                bail!("accepted JSONL review does not match its revision");
            }
            match workspace.quality_policy_id() {
                Some(policy_id)
                    if report.metadata_string("quality_policy_id") == Some(policy_id) => {}
                Some(_) => bail!("accepted JSONL review does not match the workspace policy"),
                None if report.metadata_string("quality_policy_id").is_none() => {}
                None => bail!("policy-free JSONL revision names a quality policy"),
            }
        }
        if !visited.insert(current.revision_id().to_owned()) {
            bail!("revision history contains a cycle");
        }
        let Some(parent) = current.parent() else {
            let expected = if workspace.adapter == "imagefolder" {
                store.load_inventory(current.inventory_id())?;
                dataset_id(&workspace.adapter, current.inventory_id())
            } else if workspace.adapter == "jsonl" {
                store.load_record_state_bundle(current.state_id())?;
                if let Some(policy_id) = workspace.quality_policy_id() {
                    store.load_jsonl_quality_policy(policy_id)?;
                    record_dataset_id_with_policy(
                        workspace.id_field().expect("validated JSONL workspace"),
                        current.state_id(),
                        policy_id,
                    )
                } else {
                    record_dataset_id(
                        workspace.id_field().expect("validated JSONL workspace"),
                        current.state_id(),
                    )
                }
            } else {
                bail!("unsupported workspace adapter {}", workspace.adapter);
            };
            if workspace.dataset_id != expected {
                bail!("dataset_id does not match the root revision");
            }
            return Ok(());
        };
        current = store.load_revision(parent)?;
    }
    bail!("revision history exceeds {MAX_REVISION_TRAVERSAL} entries")
}

fn count_changed_files(before: &crate::DatasetInventory, after: &crate::DatasetInventory) -> usize {
    if before.coverage() == "all_files_v2" && after.coverage() == "all_files_v2" {
        let before_files = before
            .files()
            .iter()
            .map(|file| (file.relative_path(), (file.content_hash(), file.size())))
            .collect::<HashMap<_, _>>();
        let after_files = after
            .files()
            .iter()
            .map(|file| (file.relative_path(), (file.content_hash(), file.size())))
            .collect::<HashMap<_, _>>();
        return before_files
            .keys()
            .chain(after_files.keys())
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .filter(|path| before_files.get(path) != after_files.get(path))
            .count();
    }
    let before_records = before
        .records()
        .iter()
        .map(|record| (record.relative_path.as_str(), record.content_hash.as_str()))
        .collect::<HashMap<_, _>>();
    let after_records = after
        .records()
        .iter()
        .map(|record| (record.relative_path.as_str(), record.content_hash.as_str()))
        .collect::<HashMap<_, _>>();
    let record_paths = before_records
        .keys()
        .chain(after_records.keys())
        .copied()
        .collect::<HashSet<_>>();
    let record_changes = record_paths
        .into_iter()
        .filter(|path| before_records.get(path) != after_records.get(path))
        .count();
    let before_unsupported = before
        .unsupported_paths()
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    let after_unsupported = after
        .unsupported_paths()
        .iter()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    record_changes
        + before_unsupported
            .symmetric_difference(&after_unsupported)
            .count()
}

fn dataset_id(adapter: &str, root_inventory_id: &str) -> String {
    let identity = format!("datajig-dataset-v1\0{adapter}\0{root_inventory_id}");
    format!("ds_{}", blake3::hash(identity.as_bytes()).to_hex())
}

fn record_dataset_id(id_field: &str, root_state_id: &str) -> String {
    let identity = format!("datajig-record-dataset-v1\0jsonl\0{id_field}\0{root_state_id}");
    format!("ds_{}", blake3::hash(identity.as_bytes()).to_hex())
}

fn record_dataset_id_with_policy(id_field: &str, root_state_id: &str, policy_id: &str) -> String {
    let identity =
        format!("datajig-record-dataset-v2\0jsonl\0{id_field}\0{root_state_id}\0{policy_id}");
    format!("ds_{}", blake3::hash(identity.as_bytes()).to_hex())
}

fn validate_dataset_id(value: &str) -> Result<()> {
    let Some(digest) = value.strip_prefix("ds_") else {
        bail!("dataset_id has an invalid prefix");
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("dataset_id must contain a 64-character lowercase hexadecimal digest");
    }
    Ok(())
}

fn now_unix_ns() -> Result<String> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_nanos()
        .to_string())
}

fn safe_state_directory(state: &Path, dataset: &Path) -> Result<PathBuf> {
    let absolute = lexical_normalize(std::path::absolute(state)?);
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    while !existing.exists() {
        missing.push(
            existing
                .file_name()
                .context("cannot resolve workspace state directory")?,
        );
        existing = existing
            .parent()
            .context("cannot resolve workspace state directory")?;
    }
    let mut resolved = existing
        .canonicalize()
        .context("cannot resolve workspace state directory")?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    if resolved == dataset || resolved.starts_with(dataset) {
        return Err(InvalidArgumentError::new(
            "workspace state must stay outside the dataset root",
        )
        .into());
    }
    Ok(resolved)
}

fn lexical_normalize(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

fn path_text(path: &Path, name: &str) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .with_context(|| format!("workspace {name} is not valid UTF-8"))
}

fn validate_threads(threads: usize) -> Result<()> {
    if !(1..=MAX_INVENTORY_THREADS).contains(&threads) {
        return Err(InvalidArgumentError::new(format!(
            "threads must be between 1 and {MAX_INVENTORY_THREADS}"
        ))
        .into());
    }
    Ok(())
}
