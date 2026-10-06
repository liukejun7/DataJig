#[cfg(unix)]
use anyhow::{Context, anyhow};
use anyhow::{Result, bail};
mod agent_protocol;
mod artifact_schema;
mod changeset;
mod file_inventory;
mod hf_import;
mod identity;
mod inventory;
mod io;
mod jsonl_inspect;
mod jsonl_patch;
mod jsonl_policy;
mod jsonl_review;
mod jsonl_state;
mod jsonl_view;
mod manifest;
mod manifest_diff;
mod prepare;
mod prepare_source;
mod remediation;
mod report;
mod report_query;
mod repository_integration;
mod review;
mod revision;
mod strict_json;
mod tabular_inspect;
mod tabular_source;
mod training_bundle;
mod training_consumption;
mod transform;
mod transform_output;
mod transform_provider;
mod transform_sql;
mod transform_workflow;
mod tutorial;
mod workspace;
mod workspace_store;

pub use agent_protocol::{
    AGENT_API_VERSION, AGENT_SKILL_SCHEMA_VERSION, AgentSkillArtifact, COMMAND_SCHEMA_VERSION,
    CommandDescriptor, agent_contract_id, command_catalog, command_descriptor, command_names,
    render_agent_skill, write_agent_skill,
};
pub use artifact_schema::{ARTIFACT_SCHEMA_VERSION, artifact_schema, artifact_schema_names};
#[cfg(unix)]
use cap_std::ambient_authority;
#[cfg(unix)]
use cap_std::fs::{Dir, Metadata as CapMetadata, MetadataExt as _, OpenOptions as CapOpenOptions};
pub use changeset::{
    CHANGESET_SCHEMA_VERSION, ChangeDeclaration, ChangesetSummary, DatasetChangeset,
    MAX_CHANGESET_BYTES, RECORD_CHANGESET_SCHEMA_VERSION, RecordChangesetSummary,
};
pub use file_inventory::FileRecord;
pub use hf_import::{
    HF_IMPORT_PLAN_SCHEMA_VERSION, HF_IMPORT_RECEIPT_SCHEMA_VERSION, HfHubDownloadClient,
    HfHubMetadataClient, HfImportApplyArtifact, HfImportFile, HfImportNotAuthorizedError,
    HfImportPlan, HfImportPlanArtifact, HfImportReceipt, HfImportSummary, HfImportedFile,
    MAX_HF_IMPORT_BYTES, MAX_HF_IMPORT_FILES, MAX_HF_IMPORT_PATH_BYTES,
    MAX_HF_IMPORT_PATTERN_BYTES, MAX_HF_IMPORT_PATTERNS, apply_hf_import,
    apply_hf_import_with_client, plan_hf_import, plan_hf_import_with_client,
};
pub use identity::IDENTITY_NAMESPACE;
pub use inventory::{
    DatasetInventory, InventoryFinding, InventoryRecord, InventorySummary, MAX_INVENTORY_BYTES,
    MAX_INVENTORY_THREADS, ParsedImagePath, create_inventory, load_inventory,
    parse_imagefolder_path, scan_inventory, scan_inventory_for_schema,
};
pub use io::{load_manifest, save_manifest};
pub use jsonl_inspect::{
    JSONL_INSPECTION_SCHEMA_VERSION, JsonlInspection, JsonlRecordDiff, MAX_JSONL_DIFF_CHANGES,
    MAX_JSONL_DIFF_RECORDS, MAX_JSONL_FIELD_NAME_BYTES, MAX_JSONL_FIELDS, MAX_JSONL_LINE_BYTES,
    MAX_JSONL_OUTPUT_FIELDS, MAX_JSONL_OUTPUT_FINDINGS, RECORD_DIFF_SCHEMA_VERSION,
    diff_jsonl_records, inspect_jsonl,
};
pub use jsonl_patch::{
    JSONL_PATCH_SCHEMA_VERSION, JsonlPatchPreviewArtifact, JsonlPatchRequest, MAX_JSONL_PATCH_BYTES,
};
pub use jsonl_policy::{
    JSONL_QUALITY_POLICY_SCHEMA_VERSION, JsonlQualityEvaluation, JsonlQualityPolicy,
    MAX_JSONL_POLICY_BYTES, MAX_JSONL_POLICY_FIELDS, MAX_JSONL_POLICY_PATTERN_BYTES,
    MAX_JSONL_POLICY_REGEX_FIELDS, MAX_JSONL_POLICY_SAMPLES, MAX_JSONL_POLICY_UNIQUE_FIELDS,
    evaluate_jsonl_quality,
};
pub use jsonl_state::{
    BuiltJsonlRecordState, JSONL_RECORD_PAGE_SCHEMA_VERSION, JSONL_RECORD_STATE_SCHEMA_VERSION,
    JsonlRecordLocation, JsonlRecordPage, JsonlRecordPageFact, JsonlRecordState,
    JsonlRecordStateBundle, JsonlStructuralLocation, MAX_JSONL_RECORD_PAGE_BYTES,
    MAX_JSONL_RECORD_PAGE_FACTS, MAX_JSONL_RECORD_PAGES, MAX_JSONL_RECORD_STATE_BYTES,
    build_jsonl_record_state, changed_record_ids, diff_jsonl_record_states,
};
pub use jsonl_view::{
    JSONL_SUBSET_RECIPE_SCHEMA_VERSION, JsonlSubsetRecipe, JsonlSubsetViewBinding,
    MAX_JSONL_SUBSET_CLAUSES, MAX_JSONL_SUBSET_LITERAL_BYTES, MAX_JSONL_SUBSET_RECIPE_BYTES,
    MAX_JSONL_SUBSET_SEED_BYTES, MAX_JSONL_SUBSET_VALUES,
};
pub use manifest::{
    MAX_MANIFEST_BYTES, MAX_MANIFEST_ENTRIES, MAX_PATH_BYTES, ManifestEntry, ManifestSummary,
    SnapshotManifest, estimate_manifest_size, snapshot_id,
};
pub use manifest_diff::{
    MAX_DIFF_JSON_CHARS, MAX_DIFF_PAGE_SIZE, ManifestChange, ManifestDiff, ManifestDiffPage,
    ManifestDiffPageInfo, ManifestDiffSummary, diff_manifests, manifest_diff_page,
};
pub use prepare::{
    PREPARE_RECIPE_SCHEMA_VERSION, PrepareApplyArtifact, PrepareInvalidDataError,
    PrepareNotAuthorizedError, PreparePlanArtifact, StalePrepareInputError, apply_prepare,
    plan_prepare,
};
pub use prepare_source::{MAX_LOCAL_PREPARE_SOURCE_BYTES, MAX_LOCAL_PREPARE_SOURCE_FILES};
#[cfg(unix)]
use rayon::prelude::*;
pub use remediation::{
    MAX_REMEDIATION_ACTIONS, REMEDIATION_PLAN_SCHEMA_VERSION, RemediationPlanArtifact,
    create_remediation_plan,
};
pub use report::{
    Finding, IndexedFinding, MAX_REPORT_BYTES, MAX_REPORT_ITEMS, ReviewReport, Severity,
    load_report,
};
pub use report_query::{
    DEFAULT_PAGE_SIZE, FindingNotFoundError, MAX_COMPACT_FINDINGS, MAX_COMPACT_JSON_CHARS,
    MAX_PAGE_SIZE, compact_summary, get_finding, list_findings,
};
pub use repository_integration::{
    REPOSITORY_INTEGRATION_SCHEMA_VERSION, RepositoryCheckArtifact, RepositoryComponents,
    RepositoryConflictError, RepositoryInstallArtifact, RepositoryIntegrationLock,
    RepositoryStateCheck, build_repository_integration_lock, check_repository, install_repository,
    render_repository_hook, render_repository_workflow,
};
pub use review::{MAX_PHASH_THRESHOLD, MAX_REVIEW_MATCH_CANDIDATES, ReviewArtifact, create_review};
pub use revision::{
    DatasetRevision, MAX_REVISION_BYTES, RECORD_REVISION_SCHEMA_VERSION, REVISION_SCHEMA_VERSION,
    RevisionProvenance, TRANSFORM_LINEAGE_REVISION_SCHEMA_VERSION, TransformLineage,
};
#[cfg(unix)]
use std::ffi::OsString;
#[cfg(unix)]
use std::fs;
#[cfg(unix)]
use std::io::{BufReader, Read, Write};
#[cfg(unix)]
use std::path::Component;
use std::path::Path;
use std::{error::Error, fmt};
pub use tabular_inspect::{TABULAR_INSPECTION_SCHEMA_VERSION, TabularInspection, inspect_tabular};
pub use training_bundle::{
    DEFAULT_TRAINING_SHARD_BYTES, DEFAULT_TRAINING_SHARD_RECORDS, MAX_TRAINING_BUNDLE_BYTES,
    MAX_TRAINING_BUNDLE_MANIFEST_BYTES, MAX_TRAINING_SEED_BYTES, MAX_TRAINING_SHARD_BYTES,
    MAX_TRAINING_SHARD_RECORDS, MAX_TRAINING_SHARDS, MAX_TRAINING_SOURCE_BYTES,
    MAX_TRAINING_SPLITS, MIN_TRAINING_SHARD_BYTES, MIN_TRAINING_SHARD_RECORDS,
    TRAINING_BUNDLE_SCHEMA_VERSION, TRAINING_CONSUMER_PLAN_SCHEMA_VERSION, TrainingBundleArtifact,
    TrainingBundleInfo, TrainingConsumerPlan, TrainingConsumerShard, TrainingConsumerSplit,
    TrainingExportConfig, TrainingSourceBinding, TrainingSplitConfig, inspect_training_bundle,
    inspect_training_bundle_with_consumer,
};
pub use training_consumption::{
    ConsumptionNotAuthorizedError, StaleConsumptionInputError, TRAINING_CONSUMPTION_CLAIM,
    TRAINING_CONSUMPTION_PLAN_SCHEMA_VERSION, TRAINING_CONSUMPTION_RECEIPT_SCHEMA_VERSION,
    TrainingConsumptionPlanArtifact, TrainingConsumptionRuntime, inspect_training_consumption,
    plan_training_consumption,
};
pub use transform::{
    MAX_TRANSFORM_INPUTS, MAX_TRANSFORM_OUTPUT_BYTES, MAX_TRANSFORM_OUTPUT_FIELDS,
    MAX_TRANSFORM_OUTPUT_ROWS, MAX_TRANSFORM_PARAMETER_BYTES, MAX_TRANSFORM_PARAMETERS,
    MAX_TRANSFORM_PLAN_BYTES, MAX_TRANSFORM_RECEIPT_BYTES, MAX_TRANSFORM_SOURCE_BYTES,
    MAX_TRANSFORM_SOURCE_ROWS, MAX_TRANSFORM_SQL_BYTES, TRANSFORM_PLAN_SCHEMA_VERSION,
    TRANSFORM_PROVIDER_PROTOCOL_VERSION, TRANSFORM_RECEIPT_SCHEMA_VERSION,
    TransformExecutionEvidence, TransformExpectedOutput, TransformField, TransformLimits,
    TransformPlan, TransformPlanInput, TransformProviderIdentity, TransformReceipt,
    TransformSource, TransformSourceFormat,
};
pub use transform_output::{
    ProviderObservedSchema, TransformOutputError, TransformOutputErrorKind,
    VerifiedTransformOutput, verify_transform_candidate, verify_transform_candidate_read_only,
};
pub use transform_provider::{
    ProviderExecutionSummary, ProviderRequest, StagedTransformSource, TransformInputSpec,
    TransformProviderExecutionError, TransformProviderProtocolError, TransformProviderTimeoutError,
    execute_transform_provider, probe_transform_provider, stage_transform_sources,
};
pub use transform_sql::{
    TransformOrderContract, TransformOrderError, TransformSqlParseError, TransformSqlPolicyError,
    ValidatedTransformQuery, validate_transform_query,
};
pub use transform_workflow::{
    TransformApplyArtifact, TransformDriftError, TransformInfoArtifact,
    TransformNotAuthorizedError, TransformPlanArtifact, TransformPlanRequest,
    TransformProviderUnavailableError, VerifiedTransformReceipt, apply_transform,
    apply_transform_with_optional_provider, inspect_transform, plan_transform,
    verify_transform_receipt,
};
pub use tutorial::{TutorialArtifact, run_tutorial};
#[cfg(unix)]
use walkdir::WalkDir;
pub use workspace::{
    DEFAULT_WORKSPACE_STATE, FindingLocation, FindingLocationPage, FindingLocationsArtifact,
    JsonlPatchApplyArtifact, JsonlPatchDraftArtifact, JsonlPatchUndoArtifact,
    MAX_REVISION_PAGE_SIZE, MAX_REVISION_TRAVERSAL, MAX_WORKSPACE_BYTES, MaterializeArtifact,
    RevisionPage, ViewCheckArtifact, WorkspaceArtifact, WorkspaceCheckArtifact,
    WorkspaceHeadCasArtifact, WorkspacePlanArtifact, WorkspaceSealArtifact,
    WorkspaceStatusArtifact, apply_jsonl_patch, begin_changeset, check_changeset,
    check_subset_view, check_workspace, compare_and_swap_workspace_head, draft_jsonl_patch,
    export_training_bundle, export_training_bundle_with_view,
    export_training_bundle_with_view_at_detached_revision,
    export_training_bundle_with_view_at_revision, initialize_jsonl_workspace,
    initialize_jsonl_workspace_with_policy, initialize_jsonl_workspace_with_receipt,
    initialize_workspace, is_patchable_quality_code, locate_changeset_finding,
    materialize_revision, plan_changeset, plan_workspace, preview_jsonl_patch,
    resolve_changeset_selectors, resolve_optional_changeset_context, revision_log, seal_changeset,
    seal_changeset_detached, seal_workspace, stage_changeset, status_changeset, status_workspace,
    undo_jsonl_patch,
};
pub use workspace_store::{WorkspaceLock, WorkspaceRefs, WorkspaceStore};

#[derive(Debug)]
pub struct WorkspaceBusyError;

impl fmt::Display for WorkspaceBusyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("workspace is busy with another dataset transaction")
    }
}

impl Error for WorkspaceBusyError {}

#[derive(Debug)]
pub struct OutputExistsError;

impl fmt::Display for OutputExistsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("output already exists with different or unsafe content")
    }
}

impl Error for OutputExistsError {}

#[derive(Debug)]
pub struct RevisionContentUnavailableError;

impl fmt::Display for RevisionContentUnavailableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("revision content is unavailable in this workspace")
    }
}

impl Error for RevisionContentUnavailableError {}

#[derive(Debug)]
pub struct RevisionContentCorruptError;

impl fmt::Display for RevisionContentCorruptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("stored revision content does not match its immutable identity")
    }
}

impl Error for RevisionContentCorruptError {}

#[derive(Debug)]
pub struct PatchNotAuthorizedError;

impl fmt::Display for PatchNotAuthorizedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("accepted patch identity does not match the verified repair")
    }
}

impl Error for PatchNotAuthorizedError {}

#[derive(Debug)]
pub struct PatchConflictError(String);

impl PatchConflictError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for PatchConflictError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for PatchConflictError {}

#[derive(Debug)]
pub struct UndoNotFoundError;

impl fmt::Display for UndoNotFoundError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("patch undo transaction was not found")
    }
}

impl Error for UndoNotFoundError {}

#[derive(Debug)]
pub struct InvalidArgumentError {
    message: String,
    remediation: Option<RemediationAction>,
    limit_details: Option<TransformLimitDetails>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransformLimitDetails {
    metric: &'static str,
    observed: u64,
    observed_is_lower_bound: bool,
    limit: u64,
    unit: &'static str,
}

#[derive(Clone, Debug)]
pub struct RemediationAction {
    summary: String,
    command: String,
    args: Vec<String>,
}

impl InvalidArgumentError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            remediation: None,
            limit_details: None,
        }
    }

    pub(crate) fn with_remediation(
        mut self,
        summary: impl Into<String>,
        command: impl Into<String>,
        args: Vec<String>,
    ) -> Self {
        self.remediation = Some(RemediationAction {
            summary: summary.into(),
            command: command.into(),
            args,
        });
        self
    }

    pub(crate) fn with_limit_details(
        mut self,
        metric: &'static str,
        observed: u64,
        observed_is_lower_bound: bool,
        limit: u64,
        unit: &'static str,
    ) -> Self {
        self.limit_details = Some(TransformLimitDetails::new(
            metric,
            observed,
            observed_is_lower_bound,
            limit,
            unit,
        ));
        self
    }

    pub fn remediation(&self) -> Option<&RemediationAction> {
        self.remediation.as_ref()
    }

    pub fn limit_details(&self) -> Option<&TransformLimitDetails> {
        self.limit_details.as_ref()
    }
}

impl TransformLimitDetails {
    pub(crate) fn new(
        metric: &'static str,
        observed: u64,
        observed_is_lower_bound: bool,
        limit: u64,
        unit: &'static str,
    ) -> Self {
        Self {
            metric,
            observed,
            observed_is_lower_bound,
            limit,
            unit,
        }
    }

    pub fn metric(&self) -> &str {
        self.metric
    }

    pub fn observed(&self) -> u64 {
        self.observed
    }

    pub fn observed_is_lower_bound(&self) -> bool {
        self.observed_is_lower_bound
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn unit(&self) -> &str {
        self.unit
    }
}

impl RemediationAction {
    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }
}

impl fmt::Display for InvalidArgumentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for InvalidArgumentError {}

#[derive(Debug)]
pub struct ConcurrentModificationError(String);

impl ConcurrentModificationError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for ConcurrentModificationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for ConcurrentModificationError {}

#[derive(Debug)]
pub struct UnstagedChangesError(String);

impl UnstagedChangesError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for UnstagedChangesError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for UnstagedChangesError {}

#[derive(Debug)]
pub struct InvalidRecipeError(String);

impl InvalidRecipeError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for InvalidRecipeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for InvalidRecipeError {}

#[derive(Debug)]
pub struct EmptyViewError(String);

impl EmptyViewError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for EmptyViewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for EmptyViewError {}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(unix)]
struct Candidate {
    source_path: String,
    relative_path: String,
    fingerprint: Fingerprint,
}

#[cfg(unix)]
struct OutputTarget {
    directory: Dir,
    name: OsString,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(unix)]
struct Fingerprint {
    size: u64,
    modified_ns: i128,
    created_ns: i128,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
pub fn create_snapshot(root: &Path, output: &Path, threads: usize) -> Result<SnapshotManifest> {
    if threads == 0 {
        return Err(InvalidArgumentError::new("threads must be at least 1").into());
    }
    let root = root.canonicalize().context("cannot resolve dataset root")?;
    if !root.is_dir() {
        return Err(InvalidArgumentError::new("dataset root is not a directory").into());
    }
    let destination = safe_destination(output, &root)?;
    let root_directory =
        Dir::open_ambient_dir(&root, ambient_authority()).context("cannot open dataset root")?;
    let candidates = enumerate(&root, &root_directory)?;
    reject_output_alias(&destination, &candidates)?;
    let estimated_size = estimate_manifest_size(
        candidates
            .iter()
            .map(|candidate| (candidate.relative_path.as_str(), candidate.fingerprint.size)),
    )?;
    if estimated_size > MAX_MANIFEST_BYTES {
        bail!("manifest exceeds {MAX_MANIFEST_BYTES} bytes");
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .context("cannot create hashing thread pool")?;
    let hashed: Result<Vec<ManifestEntry>> = pool.install(|| {
        candidates
            .par_iter()
            .map(|candidate| hash_candidate(&root_directory, candidate))
            .collect::<Result<Vec<_>>>()
    });
    let entries = hashed?;
    let final_candidates = enumerate(&root, &root_directory)?;
    if candidates != final_candidates {
        return Err(ConcurrentModificationError::new(
            "dataset membership or metadata changed during snapshot",
        )
        .into());
    }
    let manifest = SnapshotManifest::create(entries)?;
    let payload = manifest.to_json()?;
    if payload.len() > MAX_MANIFEST_BYTES {
        bail!("manifest exceeds {MAX_MANIFEST_BYTES} bytes");
    }
    atomic_write(&destination, payload.as_bytes())?;
    Ok(manifest)
}

#[cfg(not(unix))]
pub fn create_snapshot(_root: &Path, _output: &Path, _threads: usize) -> Result<SnapshotManifest> {
    bail!("the experimental Rust snapshot backend currently requires Unix")
}

#[cfg(unix)]
fn enumerate(root: &Path, root_directory: &Dir) -> Result<Vec<Candidate>> {
    let mut candidates = Vec::new();
    for result in WalkDir::new(root).follow_links(false) {
        let entry = result.map_err(|_| anyhow!("cannot enumerate dataset"))?;
        if entry.path() == root || entry.file_type().is_dir() {
            continue;
        }
        let path = entry.path();
        let source_path = if entry.file_type().is_symlink() {
            let target = path
                .canonicalize()
                .context("cannot resolve snapshot entry")?;
            if !target.starts_with(root) {
                return Err(
                    InvalidArgumentError::new("snapshot entry escapes dataset root").into(),
                );
            }
            if target.is_dir() {
                continue;
            }
            relative_to_posix(
                target
                    .strip_prefix(root)
                    .context("snapshot entry escapes dataset root")?,
            )?
        } else if !entry.file_type().is_file() {
            continue;
        } else {
            relative_to_posix(
                path.strip_prefix(root)
                    .context("entry is outside dataset root")?,
            )?
        };
        let relative = path
            .strip_prefix(root)
            .context("entry is outside dataset root")?;
        let relative_path = relative_to_posix(relative)?;
        if relative_path.len() > MAX_PATH_BYTES {
            bail!("manifest path exceeds {MAX_PATH_BYTES} UTF-8 bytes");
        }
        let metadata = root_directory
            .metadata(Path::new(&source_path))
            .context("cannot read snapshot entry metadata")?;
        if !metadata.is_file() {
            continue;
        }
        candidates.push(Candidate {
            source_path,
            relative_path,
            fingerprint: fingerprint_cap(&metadata)?,
        });
        if candidates.len() > MAX_MANIFEST_ENTRIES {
            bail!("manifest exceeds {MAX_MANIFEST_ENTRIES} entries");
        }
    }
    candidates.sort_by(|left, right| {
        left.relative_path
            .as_bytes()
            .cmp(right.relative_path.as_bytes())
    });
    Ok(candidates)
}

#[cfg(unix)]
fn hash_candidate(root_directory: &Dir, candidate: &Candidate) -> Result<ManifestEntry> {
    let file = root_directory
        .open(Path::new(&candidate.source_path))
        .context("cannot open snapshot entry")?
        .into_std();
    let before = fingerprint(&file.metadata()?)?;
    if before != candidate.fingerprint {
        bail!(
            "dataset entry changed before hashing: {}",
            candidate.relative_path
        );
    }
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 1024 * 1024];
    let mut size = 0_u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size = size
            .checked_add(u64::try_from(read)?)
            .ok_or_else(|| anyhow!("file size overflow"))?;
    }
    let after = fingerprint(&reader.get_ref().metadata()?)?;
    if before != after || size != before.size {
        bail!(
            "dataset entry changed while hashing: {}",
            candidate.relative_path
        );
    }
    ManifestEntry::new(
        candidate.relative_path.clone(),
        size,
        hasher.finalize().to_hex().to_string(),
    )
}

#[cfg(unix)]
fn relative_to_posix(path: &Path) -> Result<String> {
    let mut parts = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => parts.push(
                value
                    .to_str()
                    .ok_or_else(|| anyhow!("dataset path is not valid UTF-8"))?,
            ),
            _ => bail!("dataset path is not canonical and relative"),
        }
    }
    if parts.is_empty() {
        bail!("dataset path is empty");
    }
    Ok(parts.join("/"))
}

#[cfg(unix)]
fn safe_destination(output: &Path, root: &Path) -> Result<OutputTarget> {
    let output = if output.is_absolute() {
        output.to_path_buf()
    } else {
        std::env::current_dir()
            .context("cannot resolve current directory")?
            .join(output)
    };
    let parent = output
        .parent()
        .ok_or_else(|| anyhow!("snapshot output requires a parent directory"))?;
    let name = output
        .file_name()
        .ok_or_else(|| anyhow!("snapshot output requires a file name"))?;
    let mut existing = parent;
    while !existing.exists() {
        existing = existing
            .parent()
            .ok_or_else(|| anyhow!("cannot find an existing output ancestor"))?;
    }
    let existing_canonical = existing
        .canonicalize()
        .context("cannot resolve output directory")?;
    if existing_canonical.starts_with(root) {
        return Err(
            InvalidArgumentError::new("output must stay outside the dataset source").into(),
        );
    }
    let base = Dir::open_ambient_dir(&existing_canonical, ambient_authority())
        .context("cannot open output ancestor")?;
    let verified_canonical = existing_canonical
        .canonicalize()
        .context("cannot re-resolve output directory")?;
    if verified_canonical != existing_canonical || verified_canonical.starts_with(root) {
        return Err(InvalidArgumentError::new(
            "output directory changed or entered the dataset source while opening",
        )
        .into());
    }
    let opened = base
        .metadata(".")
        .context("cannot inspect opened output directory")?;
    let verified =
        fs::metadata(&verified_canonical).context("cannot inspect verified output directory")?;
    use std::os::unix::fs::MetadataExt as _;
    if opened.dev() != verified.dev() || opened.ino() != verified.ino() {
        return Err(InvalidArgumentError::new("output directory changed while opening").into());
    }
    let remainder = parent
        .strip_prefix(existing)
        .context("output directory is not below its existing ancestor")?;
    let directory = if remainder.as_os_str().is_empty() {
        base
    } else {
        base.create_dir_all(remainder)
            .context("cannot create output directory")?;
        base.open_dir(remainder)
            .context("cannot open output directory")?
    };
    Ok(OutputTarget {
        directory,
        name: name.to_os_string(),
    })
}

#[cfg(unix)]
fn reject_output_alias(destination: &OutputTarget, candidates: &[Candidate]) -> Result<()> {
    let metadata = match destination.directory.metadata(&destination.name) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error).context("cannot inspect snapshot output"),
    };
    let output_fingerprint = fingerprint_cap(&metadata)?;
    if candidates.iter().any(|candidate| {
        candidate.fingerprint.device == output_fingerprint.device
            && candidate.fingerprint.inode == output_fingerprint.inode
    }) {
        return Err(
            InvalidArgumentError::new("snapshot output aliases a dataset source file").into(),
        );
    }
    Ok(())
}

#[cfg(unix)]
fn atomic_write(destination: &OutputTarget, payload: &[u8]) -> Result<()> {
    let mut temporary = None;
    for attempt in 0..100_u32 {
        let candidate = OsString::from(format!(
            ".{}.{}.{}.tmp",
            destination.name.to_string_lossy(),
            std::process::id(),
            attempt
        ));
        let mut options = CapOpenOptions::new();
        options.write(true).create_new(true);
        match destination.directory.open_with(&candidate, &options) {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("cannot create temporary manifest"),
        }
    }
    let (temporary_path, mut file) = temporary.context("cannot allocate temporary manifest")?;
    let result = (|| -> Result<()> {
        file.write_all(payload)?;
        file.sync_all()?;
        drop(file);
        destination
            .directory
            .rename(&temporary_path, &destination.directory, &destination.name)
            .context("cannot publish manifest atomically")?;
        destination.directory.open(".")?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = destination.directory.remove_file(&temporary_path);
    }
    result
}

#[cfg(unix)]
fn fingerprint_cap(metadata: &CapMetadata) -> Result<Fingerprint> {
    let modified_ns = time_to_ns(metadata.mtime(), metadata.mtime_nsec())?;
    let created_ns = time_to_ns(metadata.ctime(), metadata.ctime_nsec())?;
    Ok(Fingerprint {
        size: metadata.len(),
        modified_ns,
        created_ns,
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(unix)]
fn fingerprint(metadata: &fs::Metadata) -> Result<Fingerprint> {
    use std::os::unix::fs::MetadataExt;
    let modified_ns = time_to_ns(metadata.mtime(), metadata.mtime_nsec())?;
    let created_ns = time_to_ns(metadata.ctime(), metadata.ctime_nsec())?;
    Ok(Fingerprint {
        size: metadata.len(),
        modified_ns,
        created_ns,
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(unix)]
fn time_to_ns(seconds: i64, nanos: i64) -> Result<i128> {
    let seconds = i128::from(seconds);
    let nanos = i128::from(nanos);
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|value| value.checked_add(nanos))
        .ok_or_else(|| anyhow!("timestamp overflow"))
}
