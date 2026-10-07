use clap::{Parser, Subcommand, ValueEnum, error::ErrorKind};
use datajig_core::{
    AGENT_API_VERSION, AuthorizationStatus, COMMAND_SCHEMA_VERSION, ConcurrentModificationError,
    ConsumptionNotAuthorizedError, DEFAULT_TRAINING_SHARD_BYTES, DEFAULT_TRAINING_SHARD_RECORDS,
    EmptyViewError, FindingNotFoundError, HfImportNotAuthorizedError, InvalidArgumentError,
    InvalidRecipeError, MAX_COMPACT_FINDINGS, MAX_COMPACT_JSON_CHARS, MAX_DIFF_PAGE_SIZE,
    MAX_INVENTORY_THREADS, MAX_PAGE_SIZE, MAX_PHASH_THRESHOLD, MAX_REMEDIATION_ACTIONS,
    MAX_REVIEW_MATCH_CANDIDATES, MAX_REVISION_PAGE_SIZE, OutputExistsError, PatchConflictError,
    PatchNotAuthorizedError, PrepareInvalidDataError, PrepareNotAuthorizedError,
    RepositoryComponents, RepositoryConflictError, RevisionContentCorruptError,
    RevisionContentUnavailableError, RunConsumptionBinding, RunControlError, RunDslError,
    RunPlanArtifact, RunPlanBinding, Severity, StaleConsumptionInputError, StalePrepareInputError,
    TrainingExportConfig, TransformDriftError, TransformInputSpec, TransformNotAuthorizedError,
    TransformOutputError, TransformOutputErrorKind, TransformProviderExecutionError,
    TransformProviderProtocolError, TransformProviderTimeoutError, TransformSourceFormat,
    UndoNotFoundError, UnstagedChangesError, WorkspaceBusyError, WorkspaceLock, WorkspaceStore,
    agent_contract_id, apply_hf_import, apply_jsonl_patch, apply_prepare, artifact_schema,
    artifact_schema_names, begin_changeset, check_changeset, check_repository, check_subset_view,
    check_workspace, classify_run_output, command_catalog, command_descriptor, command_names,
    compact_summary, compare_and_swap_workspace_head, compile_task, create_inventory,
    create_review, create_run_plan, create_snapshot, diff_jsonl_records, diff_manifests,
    draft_jsonl_patch, export_training_bundle_with_view_at_detached_revision,
    export_training_bundle_with_view_at_revision, fingerprint_run_source, get_finding,
    initialize_jsonl_workspace_with_receipt, initialize_workspace, inspect_jsonl, inspect_tabular,
    inspect_training_bundle_with_consumer, inspect_training_consumption, inspect_transform,
    install_repository, is_patchable_quality_code, list_findings, load_manifest, load_report,
    load_run_plan_for_resume, locate_changeset_finding, manifest_diff_page, materialize_revision,
    persist_run_plan, plan_changeset, plan_hf_import, plan_prepare, plan_training_consumption,
    plan_transform, plan_workspace, preview_jsonl_patch, resolve_change_selector,
    resolve_optional_changeset_context, resolve_run_source_format, revision_log, run_tutorial,
    seal_changeset, seal_changeset_detached, seal_workspace, stage_changeset, status_changeset,
    status_workspace, undo_jsonl_patch, write_agent_skill,
};
use serde_json::{Value, json};
use std::io::Read;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(
    name = "datajig-core",
    version,
    about = "Machine-readable dataset review core",
    after_help = "Lifecycle:\n  prepare/import -> transform -> version -> review/seal -> export -> consume"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a version-matched Agent Skill for the installed CLI.
    AgentSkill {
        /// Destination SKILL.md path.
        #[arg(long)]
        output: PathBuf,
    },
    /// Install or safely upgrade DataJig's repository-local Agent and CI contract.
    RepositoryInstall {
        /// Git worktree or a directory inside it.
        #[arg(long, default_value = ".")]
        root: PathBuf,
        /// Repository-relative DataJig workspace state path; repeat to bind multiple states.
        #[arg(long = "state")]
        states: Vec<PathBuf>,
        /// Do not install or activate the repository-managed pre-commit hook.
        #[arg(long)]
        no_hook: bool,
        /// Do not install the repository-managed GitHub Actions workflow.
        #[arg(long)]
        no_github_actions: bool,
    },
    /// Verify the pinned repository integration and bound workspace readiness.
    RepositoryCheck {
        /// Git worktree or a directory inside it.
        #[arg(long, default_value = ".")]
        root: PathBuf,
        /// Skip the clone-local core.hooksPath assertion in CI.
        #[arg(long)]
        ci: bool,
    },
    /// Return a machine-readable input artifact contract.
    #[command(
        after_help = "Available artifacts:\n  jsonl-field-patch\n  jsonl-quality-policy\n  pipeline-config\n  prepare-recipe\n  repository-integration\n  subset-view\n  training-consumption-plan\n  training-consumption-receipt\n  transform-plan\n  transform-receipt"
    )]
    ArtifactSchema {
        /// Optional artifact name; omit to list supported schemas.
        artifact: Option<String>,
    },
    /// Describe the native backend and its stable protocol limits.
    Capabilities,
    /// Return the versioned machine contract for all commands or one command.
    Describe {
        /// Optional command name to describe.
        command: Option<String>,
    },
    /// Create and run a complete verified JSONL-to-training example.
    Tutorial {
        /// New directory that will contain the example dataset, workspace, and bundle.
        output: PathBuf,
    },
    /// Compile, authorize, and persist a deterministic data-to-training run plan.
    #[command(
        after_help = "Examples:\n  datajig run 'from data/rows.csv source-id-field id export id-field id'\n  datajig run '整理 data/rows.csv' --strict\n  datajig run --resume <ATTEMPT_ID> --accept-plan <PLAN_ID>"
    )]
    Run {
        /// DataJig DSL v1 task or one supported deterministic phrase.
        #[arg(
            value_name = "TASK",
            required_unless_present = "resume",
            conflicts_with = "resume"
        )]
        task: Option<String>,
        /// Stop after persisting the immutable plan and require exact acceptance.
        #[arg(long)]
        strict: bool,
        /// Resume one exact persisted attempt.
        #[arg(long, value_name = "ATTEMPT_ID", requires = "accept_plan")]
        resume: Option<String>,
        /// Exact plan identity accepted for --resume.
        #[arg(long, value_name = "PLAN_ID", requires = "resume")]
        accept_plan: Option<String>,
        /// Authorize this invocation's dangerous operations; never overrides fatal checks.
        #[arg(long)]
        yes: bool,
        /// Relative public delivery directory.
        #[arg(long, default_value = "bundle")]
        output: PathBuf,
        /// Bind an optional training consumption plan to this run.
        #[arg(long)]
        consume: bool,
        /// Consumption adapter: python, pytorch, or huggingface.
        #[arg(long, requires = "consume")]
        consumer: Option<String>,
        /// Public external training-run identity.
        #[arg(long, requires = "consume")]
        run_id: Option<String>,
        /// Consumer run-state directory included in consumption identity.
        #[arg(long, requires = "consume")]
        run_dir: Option<PathBuf>,
    },
    /// Initialize a project-local last-good baseline for a dataset.
    #[command(
        after_help = "Quality policy schema and example:\n  datajig artifact-schema jsonl-quality-policy"
    )]
    Init {
        /// ImageFolder directory or JSONL file to track.
        dataset: PathBuf,
        /// Stable record key for a JSONL workspace.
        #[arg(long)]
        id_field: Option<String>,
        /// Immutable JSONL quality policy to pin to this workspace.
        #[arg(long)]
        policy: Option<PathBuf>,
        /// Verified transform receipt that exactly binds this JSONL source.
        #[arg(long)]
        source_receipt: Option<PathBuf>,
        /// Project-local state directory kept outside the dataset.
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        /// Number of media inventory threads.
        #[arg(long, visible_alias = "workers", default_value_t = 1)]
        threads: usize,
    },
    /// Export a clean sealed JSONL revision as deterministic training shards.
    #[command(
        after_help = "Example:\n  datajig export --state .datajig --output bundle --split train=7 --split val=2 --split test=1"
    )]
    Export {
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        #[arg(long)]
        output: PathBuf,
        /// Optional sealed subset recipe to export.
        #[arg(long)]
        view: Option<PathBuf>,
        /// Reachable immutable revision to export; defaults to clean live HEAD.
        #[arg(long)]
        revision: Option<String>,
        /// Internal pipeline authorization for exporting a detached direct child of HEAD.
        #[arg(long, hide = true, requires = "revision")]
        allow_detached: bool,
        #[arg(long, default_value = "datajig-v1")]
        seed: String,
        /// Repeatable NAME=WEIGHT split; each WEIGHT is a positive relative integer weight.
        #[arg(long = "split", required = true, value_name = "NAME=WEIGHT")]
        splits: Vec<String>,
        /// Record target per shard. Final shards may contain fewer records.
        #[arg(long, default_value_t = DEFAULT_TRAINING_SHARD_RECORDS)]
        max_shard_records: usize,
        /// Soft byte target; a larger single record occupies its own shard.
        #[arg(long, default_value_t = DEFAULT_TRAINING_SHARD_BYTES)]
        max_shard_bytes: u64,
    },
    /// Read and optionally verify a portable training bundle manifest.
    ExportInfo {
        manifest: PathBuf,
        #[arg(long)]
        verify: bool,
        /// Return a bounded verified shard plan for framework consumers.
        #[arg(long, requires = "verify")]
        consumer_plan: bool,
        /// Require this exact bundle identity.
        #[arg(long, requires = "verify")]
        expect_bundle: Option<String>,
        /// Require this exact sealed source revision.
        #[arg(long, requires = "verify")]
        expect_revision: Option<String>,
        /// Minimum accepted assurance: structural or quality_policy.
        #[arg(long, requires = "verify")]
        require_assurance: Option<String>,
        /// Return only one split in the consumer plan.
        #[arg(long, requires = "consumer_plan")]
        split: Option<String>,
    },
    /// Plan one proof-carrying training consumption run.
    ConsumePlan {
        /// Verified training bundle manifest.
        manifest: PathBuf,
        /// Exact non-empty split to consume.
        #[arg(long)]
        split: String,
        /// Adapter boundary: python, pytorch, or huggingface.
        #[arg(long)]
        consumer: String,
        /// Public external training-run identity.
        #[arg(long)]
        run_id: String,
        /// New local run-state directory.
        #[arg(long)]
        output: PathBuf,
        /// New consumption plan artifact.
        #[arg(long)]
        plan: PathBuf,
    },
    /// Verify one accepted training consumption plan and its live bundle.
    ConsumeInfo {
        /// Plan artifact created by consume-plan.
        plan: PathBuf,
        /// Require live manifest and shard verification.
        #[arg(long)]
        verify: bool,
        /// Exact consume_... identity returned by consume-plan.
        #[arg(long, requires = "verify")]
        accept_plan: String,
    },
    /// Resolve a deterministic subset against clean sealed JSONL HEAD.
    ViewCheck {
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        #[arg(long)]
        recipe: PathBuf,
    },
    /// Review the current dataset against the workspace's last-good baseline.
    Check {
        /// Project-local state directory created by init.
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        /// Number of media inventory threads.
        #[arg(long, visible_alias = "workers", default_value_t = 1)]
        threads: usize,
        /// Maximum perceptual-hash Hamming distance.
        #[arg(long, default_value_t = 6)]
        phash_threshold: usize,
        /// Exact chg_... ID, or @active/@latest. Omit both selectors to resolve one active pair.
        #[arg(long, requires = "changeset")]
        change: Option<String>,
        /// Exact changeset_... ID, or @latest/@active. Omit both selectors to resolve one active pair.
        #[arg(long, requires = "change")]
        changeset: Option<String>,
    },
    /// Declare an Agent task against the current clean dataset HEAD.
    ChangesetBegin {
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        #[arg(long, visible_alias = "workers", default_value_t = 1)]
        threads: usize,
        #[arg(long)]
        intent: String,
        #[arg(long)]
        task_id: String,
        #[arg(long, default_value = "agent")]
        actor_kind: String,
    },
    /// Capture the current full dataset as an immutable staged changeset.
    ChangesetStage {
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        #[arg(long, visible_alias = "workers", default_value_t = 1)]
        threads: usize,
        /// Exact chg_... ID, or @active/@latest when one declaration matches the current HEAD.
        #[arg(long)]
        change: String,
    },
    /// Turn the latest workspace review into a deterministic Agent action plan.
    ReviewPlan {
        /// Project-local state directory created by init.
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        /// Exact chg_... ID or alias. Omit both selectors to resolve one active pair.
        #[arg(long, requires = "changeset")]
        change: Option<String>,
        /// Exact changeset_... ID or alias. Omit both selectors to resolve one active pair.
        #[arg(long, requires = "change")]
        changeset: Option<String>,
    },
    /// Read bounded immutable revision history, newest first.
    Log {
        /// Project-local state directory created by init.
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        /// Zero-based revision offset.
        #[arg(long, allow_hyphen_values = true, default_value_t = 0)]
        offset: i64,
        /// Maximum revisions to return.
        #[arg(long, allow_hyphen_values = true, default_value_t = 50)]
        limit: i64,
    },
    /// Write the exact stored bytes of one immutable JSONL revision.
    Materialize {
        /// Full immutable revision identity.
        revision: String,
        /// Project-local state directory created by init.
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        /// New JSONL output path; existing differing content is never overwritten.
        #[arg(long)]
        output: PathBuf,
    },
    /// Resolve one staged JSONL finding to bounded current candidate line coordinates.
    Locate {
        /// Deterministic review report returned by check.
        report: PathBuf,
        /// Stable finding ID returned by findings.
        finding_id: String,
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        #[arg(long)]
        change: String,
        #[arg(long)]
        changeset: String,
        #[arg(long, allow_hyphen_values = true, default_value_t = 0)]
        offset: i64,
        #[arg(long, allow_hyphen_values = true, default_value_t = 50)]
        limit: i64,
    },
    /// Create a fresh evidence-bound JSONL field patch request from a finding.
    PatchDraft {
        /// Deterministic review report returned by check.
        report: PathBuf,
        /// Stable finding ID returned by findings.
        finding_id: String,
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        #[arg(long)]
        change: String,
        #[arg(long)]
        changeset: String,
        /// Optional sampled rid_... identity when a finding covers multiple records.
        #[arg(long)]
        record: Option<String>,
        /// Replacement value encoded as a JSON scalar.
        #[arg(long, conflicts_with = "remove", required_unless_present = "remove")]
        after_json: Option<String>,
        /// Remove the field instead of replacing its value.
        #[arg(
            long,
            conflicts_with = "after_json",
            required_unless_present = "after_json"
        )]
        remove: bool,
        /// Destination for the generated patch request.
        #[arg(long)]
        output: PathBuf,
    },
    /// Verify a proposed top-level JSONL field edit against exact staged finding evidence.
    PatchPreview {
        /// Bounded schema-1 JSONL field patch request.
        request: PathBuf,
        /// Deterministic review report returned by check.
        report: PathBuf,
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        #[arg(long)]
        change: String,
        #[arg(long)]
        changeset: String,
    },
    /// Atomically apply one exact JSONL field repair accepted from patch-preview.
    PatchApply {
        request: PathBuf,
        report: PathBuf,
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        #[arg(long)]
        change: String,
        #[arg(long)]
        changeset: String,
        #[arg(long)]
        accept_patch: String,
    },
    /// Restore the exact bytes replaced by one guarded JSONL patch.
    PatchUndo {
        undo_id: String,
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
    },
    /// Apply one accepted immutable Hugging Face dataset import plan.
    HfImportApply {
        /// Plan artifact created by hf-import-plan.
        plan: PathBuf,
        /// Exact hfplan_... identity returned by hf-import-plan.
        #[arg(long)]
        accept_plan: String,
    },
    /// Resolve and plan selected files from one Hugging Face dataset revision.
    HfImportPlan {
        /// Dataset repository in owner/name form.
        repository: String,
        /// Branch, tag, or commit to resolve once.
        #[arg(long, default_value = "main")]
        revision: String,
        /// Repeatable glob; overrides the default data-file extensions.
        #[arg(long = "include")]
        includes: Vec<String>,
        /// Repeatable glob applied after includes.
        #[arg(long = "ignore")]
        ignores: Vec<String>,
        /// New dataset output directory bound into the plan.
        #[arg(long)]
        output: PathBuf,
        /// New plan artifact path.
        #[arg(long)]
        plan: PathBuf,
    },
    /// Apply one exact deterministic data preparation plan.
    PrepareApply {
        /// Plan artifact created by prepare-plan.
        plan: PathBuf,
        /// Exact prep_... identity returned by prepare-plan.
        #[arg(long)]
        accept_plan: String,
    },
    /// Plan deterministic CSV/Parquet/JSONL preparation from one file or import receipt.
    #[command(
        after_help = "Discover the complete schema and canonical example:\n  datajig artifact-schema prepare-recipe\n\nMinimal recipe:\n  {\"namespace\":\"datajig\",\"kind\":\"prepare\",\"schema_version\":1,\"source\":{\"format\":\"csv\"},\"output\":{\"format\":\"jsonl\"},\"id_field\":\"id\",\"steps\":[]}"
    )]
    PreparePlan {
        /// CSV, flat Parquet, JSONL, a recursive same-format directory, or datajig.hf-import.json.
        source: PathBuf,
        /// Schema-1 recipe; run `datajig artifact-schema prepare-recipe` for fields and example.
        #[arg(long)]
        recipe: PathBuf,
        /// New JSONL output path to bind into the plan.
        #[arg(long)]
        output: PathBuf,
        /// New plan artifact path.
        #[arg(long)]
        plan: PathBuf,
    },
    /// Execute and persist an immutable plan for a bounded local SQL transform.
    #[command(
        after_help = "Example:\n  datajig transform-plan --input events=data/events.csv --sql 'SELECT id FROM events ORDER BY id' --id-field id --output prepared.jsonl --plan transform-plan.json\n\nEach --input uses ALIAS=PATH. CSV columns are strings; use explicit CAST for numeric semantics. Parameters are a JSON array of scalars. Multi-row SQL must end its top-level ORDER BY with the output ID alias. Requires: pip install 'datajig[duckdb]'."
    )]
    TransformPlan {
        /// Repeatable staged input in ALIAS=PATH form; supports CSV, Parquet, and JSONL.
        #[arg(long = "input", required = true, value_name = "ALIAS=PATH")]
        inputs: Vec<String>,
        /// Inline UTF-8 SELECT query (maximum 64 KiB).
        #[arg(
            long,
            required_unless_present = "sql_file",
            conflicts_with = "sql_file"
        )]
        sql: Option<String>,
        /// UTF-8 file containing one SELECT query (maximum 64 KiB).
        #[arg(long, required_unless_present = "sql", conflicts_with = "sql")]
        sql_file: Option<PathBuf>,
        /// Inline JSON array containing at most 256 scalar positional parameters.
        #[arg(long, conflicts_with = "params_file", value_name = "JSON_ARRAY")]
        params: Option<String>,
        /// UTF-8 file containing the JSON parameter array.
        #[arg(long, conflicts_with = "params", value_name = "PATH")]
        params_file: Option<PathBuf>,
        /// Required output record identity field.
        #[arg(long)]
        id_field: String,
        /// Future new canonical JSONL output path.
        #[arg(long)]
        output: PathBuf,
        /// New immutable transform plan artifact.
        #[arg(long)]
        plan: PathBuf,
    },
    /// Re-execute and atomically publish one accepted transform plan.
    TransformApply {
        /// Immutable transform plan artifact.
        plan: PathBuf,
        /// Exact xform_... identity returned by transform-plan.
        #[arg(long)]
        accept_plan: String,
    },
    /// Inspect and optionally verify a transform plan or receipt.
    TransformInfo {
        /// Transform plan or transform receipt artifact.
        artifact: PathBuf,
        /// Verify artifact identity and any published receipt output without executing SQL.
        #[arg(long)]
        verify: bool,
    },
    /// Promote a fresh PASS review to the workspace's last-good baseline.
    Seal {
        /// Project-local state directory created by init.
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        /// Number of media inventory threads used for freshness verification.
        #[arg(long, visible_alias = "workers", default_value_t = 1)]
        threads: usize,
        /// Human-readable reason recorded in revision provenance.
        #[arg(long, default_value = "Accept dataset revision")]
        message: String,
        /// Exact review_ID required when a passing review contains findings.
        #[arg(long)]
        accept_report: Option<String>,
        /// Exact chg_... ID or alias. Omit both selectors to resolve one active pair.
        #[arg(long, requires = "changeset")]
        change: Option<String>,
        /// Exact changeset_... ID or alias. Omit both selectors to resolve one active pair.
        #[arg(long, requires = "change")]
        changeset: Option<String>,
        /// Prepare a validated JSONL revision without advancing HEAD.
        #[arg(long, hide = true)]
        detached: bool,
        /// Verified transform receipt to bind into a detached JSONL revision.
        #[arg(long, hide = true, requires = "detached")]
        source_receipt: Option<PathBuf>,
        /// Commit an already prepared direct child revision with compare-and-swap.
        #[arg(
            long,
            hide = true,
            requires = "expected_head",
            conflicts_with = "detached"
        )]
        commit_revision: Option<String>,
        /// Exact current HEAD required by --commit-revision.
        #[arg(long, hide = true, requires = "commit_revision")]
        expected_head: Option<String>,
    },
    /// Inspect current dataset state without writing review artifacts.
    Status {
        /// Project-local state directory created by init.
        #[arg(long, default_value = ".datajig")]
        state: PathBuf,
        /// Number of media inventory threads.
        #[arg(long, visible_alias = "workers", default_value_t = 1)]
        threads: usize,
        /// Exact chg_... ID or alias. Omit both selectors to inspect one active pair.
        #[arg(long, requires = "changeset")]
        change: Option<String>,
        /// Exact changeset_... ID or alias. Omit both selectors to inspect one active pair.
        #[arg(long, requires = "change")]
        changeset: Option<String>,
    },
    /// Inspect an ImageFolder dataset and write a media inventory.
    Inventory {
        /// Dataset directory to inspect.
        root: PathBuf,
        /// Destination for the complete inventory JSON artifact.
        #[arg(long)]
        output: PathBuf,
        /// Number of hashing and media-probe threads.
        #[arg(long, visible_alias = "workers", default_value_t = 1)]
        threads: usize,
    },
    /// Inspect a JSONL, CSV, or flat Parquet dataset without exposing values.
    Inspect {
        /// JSONL, CSV, or flat Parquet file to inspect.
        source: PathBuf,
        /// Top-level field used as the record identity.
        #[arg(long, default_value = "id")]
        id_field: String,
        /// One-byte CSV delimiter; invalid for JSONL and Parquet.
        #[arg(long, default_value = ",")]
        delimiter: String,
    },
    /// Compare two JSONL datasets by stable record identity.
    RecordDiff {
        before: PathBuf,
        after: PathBuf,
        #[arg(long, default_value = "id")]
        id_field: String,
    },
    /// Compare two dataset references and write a native semantic review report.
    Review {
        /// Baseline path:<directory>, inventory:<file>, or bare directory path.
        before: String,
        /// Candidate path:<directory>, inventory:<file>, or bare directory path.
        after: String,
        /// Destination for the schema-1 review report.
        #[arg(long)]
        output: PathBuf,
        /// Number of media inventory threads for path references.
        #[arg(long, visible_alias = "workers", default_value_t = 1)]
        threads: usize,
        /// Maximum perceptual-hash Hamming distance.
        #[arg(long, default_value_t = 6)]
        phash_threshold: usize,
    },
    /// Create a portable content-addressed manifest for a directory.
    Snapshot {
        /// Directory to snapshot.
        root: PathBuf,
        /// Destination for the complete manifest JSON artifact.
        #[arg(long)]
        output: PathBuf,
        /// Number of content-hashing threads.
        #[arg(long, visible_alias = "workers", default_value_t = 1)]
        threads: usize,
    },
    /// Read a snapshot manifest summary.
    SnapshotInfo {
        /// Snapshot manifest JSON file.
        manifest: PathBuf,
    },
    /// Compare two snapshot manifests with bounded pagination.
    SnapshotDiff {
        /// Baseline snapshot manifest.
        before: PathBuf,
        /// Candidate snapshot manifest.
        after: PathBuf,
        /// Zero-based change offset.
        #[arg(long, allow_hyphen_values = true, default_value_t = 0)]
        offset: i64,
        /// Maximum changes to return.
        #[arg(long, allow_hyphen_values = true, default_value_t = 50)]
        limit: i64,
    },
    /// List findings from a saved review report.
    Findings {
        /// Review report JSON file.
        report: PathBuf,
        /// Include only these severities; may be repeated.
        #[arg(long, value_enum, action = clap::ArgAction::Append)]
        severity: Vec<SeverityArgument>,
        /// Include only these finding codes; may be repeated.
        #[arg(long, action = clap::ArgAction::Append)]
        code: Vec<String>,
        /// Zero-based finding offset.
        #[arg(long, allow_hyphen_values = true, default_value_t = 0)]
        offset: i64,
        /// Maximum findings to return.
        #[arg(long, allow_hyphen_values = true, default_value_t = 50)]
        limit: i64,
    },
    /// Get one finding by its stable identifier.
    Finding {
        /// Review report JSON file.
        report: PathBuf,
        /// Stable `fnd_...` identifier returned by `findings`.
        finding_id: String,
    },
    /// Produce a bounded review summary for an agent context window.
    Explain {
        /// Review report JSON file.
        report: PathBuf,
        /// Maximum findings to include in the summary.
        #[arg(long, allow_hyphen_values = true, default_value_t = 10)]
        limit: i64,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum SeverityArgument {
    Error,
    Warning,
    Info,
}

impl From<SeverityArgument> for Severity {
    fn from(value: SeverityArgument) -> Self {
        match value {
            SeverityArgument::Error => Self::Error,
            SeverityArgument::Warning => Self::Warning,
            SeverityArgument::Info => Self::Info,
        }
    }
}

fn main() {
    let command_name = std::env::args().nth(1);
    let argument_error_code = match command_name.as_deref() {
        Some(
            "artifact-schema" | "findings" | "finding" | "explain" | "export" | "export-info"
            | "view-check" | "inventory" | "review" | "init" | "check" | "changeset-begin"
            | "changeset-stage" | "review-plan" | "log" | "locate" | "patch-apply" | "patch-draft"
            | "patch-preview" | "patch-undo" | "hf-import-apply" | "hf-import-plan"
            | "prepare-apply" | "prepare-plan" | "repository-check" | "repository-install" | "seal"
            | "status" | "transform-apply" | "transform-info" | "transform-plan" | "run",
        ) => "INVALID_ARGUMENT",
        _ => "ARGUMENT_ERROR",
    };
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            print!("{error}");
            return;
        }
        Err(error) => exit_with_error(
            argument_error_code,
            &error.to_string(),
            2,
            command_name.as_deref(),
            None,
        ),
    };
    if let Err((code, status, error)) = run(cli) {
        exit_with_error(
            code,
            &format!("{error:#}"),
            status,
            command_name.as_deref(),
            Some(&error),
        );
    }
}

type CommandError = (&'static str, i32, anyhow::Error);

fn run(cli: Cli) -> Result<(), CommandError> {
    validate_command_arguments(&cli.command)?;
    let _workspace_lock = lock_command_workspace(&cli.command)?;
    match cli.command {
        Command::AgentSkill { output } => {
            let artifact =
                write_agent_skill(&output).map_err(|error| ("AGENT_SKILL_IO_ERROR", 2, error))?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "kind": "agent_skill_created",
                    "artifact": artifact
                })
            );
        }
        Command::RepositoryInstall {
            root,
            states,
            no_hook,
            no_github_actions,
        } => {
            let artifact = install_repository(
                &root,
                &states,
                RepositoryComponents {
                    hook: !no_hook,
                    github_actions: !no_github_actions,
                },
            )
            .map_err(repository_install_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "agent_contract_id": agent_contract_id(),
                    "backend": "rust",
                    "kind": "repository_integration_installed",
                    "decision": artifact.decision,
                    "next_actions": [{"command": "repository-check", "args": ["--root", artifact.root]}],
                    "artifact": artifact
                })
            );
        }
        Command::RepositoryCheck { root, ci } => {
            let artifact = check_repository(&root, ci).map_err(repository_install_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "agent_contract_id": agent_contract_id(),
                    "backend": "rust",
                    "kind": "repository_integration_checked",
                    "decision": artifact.decision,
                    "next_actions": [],
                    "artifact": artifact
                })
            );
        }
        Command::ArtifactSchema { artifact } => {
            let (kind, artifact) = if let Some(name) = artifact {
                let artifact = artifact_schema(&name).ok_or_else(|| {
                    (
                        "ARTIFACT_SCHEMA_NOT_FOUND",
                        4,
                        anyhow::anyhow!("artifact schema was not found"),
                    )
                })?;
                ("artifact_schema", artifact)
            } else {
                (
                    "artifact_schema_catalog",
                    json!({
                        "schema_version": datajig_core::ARTIFACT_SCHEMA_VERSION,
                        "names": artifact_schema_names()
                    }),
                )
            };
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "agent_contract_id": agent_contract_id(),
                    "backend": "rust",
                    "kind": kind,
                    "decision": "ready",
                    "next_actions": [],
                    "artifact": artifact
                })
            );
        }
        Command::Capabilities => {
            let mut limits = json!({
                "default_page_size": 50,
                "max_page_size": MAX_PAGE_SIZE,
                "default_compact_findings": 10,
                "max_compact_findings": MAX_COMPACT_FINDINGS,
                "max_compact_characters": MAX_COMPACT_JSON_CHARS,
                "max_inventory_threads": MAX_INVENTORY_THREADS,
                "max_jsonl_line_bytes": datajig_core::MAX_JSONL_LINE_BYTES,
                "max_jsonl_fields": datajig_core::MAX_JSONL_FIELDS,
                "max_jsonl_field_name_bytes": datajig_core::MAX_JSONL_FIELD_NAME_BYTES,
                "max_jsonl_output_fields": datajig_core::MAX_JSONL_OUTPUT_FIELDS,
                "max_jsonl_output_findings": datajig_core::MAX_JSONL_OUTPUT_FINDINGS,
                "max_jsonl_diff_records": datajig_core::MAX_JSONL_DIFF_RECORDS,
                "max_jsonl_diff_changes": datajig_core::MAX_JSONL_DIFF_CHANGES,
                "max_jsonl_record_page_facts": datajig_core::MAX_JSONL_RECORD_PAGE_FACTS,
                "max_jsonl_record_pages": datajig_core::MAX_JSONL_RECORD_PAGES,
                "max_jsonl_record_page_bytes": datajig_core::MAX_JSONL_RECORD_PAGE_BYTES,
                "max_jsonl_record_state_bytes": datajig_core::MAX_JSONL_RECORD_STATE_BYTES,
                "max_jsonl_patch_bytes": datajig_core::MAX_JSONL_PATCH_BYTES,
                "max_jsonl_policy_bytes": datajig_core::MAX_JSONL_POLICY_BYTES,
                "max_jsonl_policy_fields": datajig_core::MAX_JSONL_POLICY_FIELDS,
                "max_jsonl_policy_pattern_bytes": datajig_core::MAX_JSONL_POLICY_PATTERN_BYTES,
                "max_jsonl_policy_regex_fields": datajig_core::MAX_JSONL_POLICY_REGEX_FIELDS,
                "max_jsonl_policy_unique_fields": datajig_core::MAX_JSONL_POLICY_UNIQUE_FIELDS,
                "max_jsonl_policy_samples": datajig_core::MAX_JSONL_POLICY_SAMPLES,
                "max_jsonl_subset_recipe_bytes": datajig_core::MAX_JSONL_SUBSET_RECIPE_BYTES,
                "max_jsonl_subset_clauses": datajig_core::MAX_JSONL_SUBSET_CLAUSES,
                "max_jsonl_subset_values": datajig_core::MAX_JSONL_SUBSET_VALUES,
                "max_training_source_bytes": datajig_core::MAX_TRAINING_SOURCE_BYTES,
                "max_training_bundle_bytes": datajig_core::MAX_TRAINING_BUNDLE_BYTES,
                "max_training_splits": datajig_core::MAX_TRAINING_SPLITS,
                "max_training_shards": datajig_core::MAX_TRAINING_SHARDS,
                "max_training_bundle_manifest_bytes": datajig_core::MAX_TRAINING_BUNDLE_MANIFEST_BYTES,
                "min_training_shard_records": datajig_core::MIN_TRAINING_SHARD_RECORDS,
                "max_training_shard_records": datajig_core::MAX_TRAINING_SHARD_RECORDS,
                "min_training_shard_bytes": datajig_core::MIN_TRAINING_SHARD_BYTES,
                "max_training_shard_bytes": datajig_core::MAX_TRAINING_SHARD_BYTES,
                "max_phash_threshold": MAX_PHASH_THRESHOLD,
                "max_review_match_candidates": MAX_REVIEW_MATCH_CANDIDATES,
                "max_remediation_actions": MAX_REMEDIATION_ACTIONS,
                "max_revision_page_size": MAX_REVISION_PAGE_SIZE,
                "default_workspace_state": ".datajig",
            });
            let limits_object = limits
                .as_object_mut()
                .expect("capability limits are a JSON object");
            limits_object.insert(
                "max_jsonl_subset_literal_bytes".into(),
                json!(datajig_core::MAX_JSONL_SUBSET_LITERAL_BYTES),
            );
            limits_object.insert(
                "max_jsonl_subset_seed_bytes".into(),
                json!(datajig_core::MAX_JSONL_SUBSET_SEED_BYTES),
            );
            limits_object.insert(
                "max_jsonl_subset_field_bytes".into(),
                json!(datajig_core::MAX_JSONL_FIELD_NAME_BYTES),
            );
            limits_object.insert("max_prepare_recipe_bytes".into(), json!(65536));
            limits_object.insert("max_prepare_steps".into(), json!(64));
            limits_object.insert("max_prepare_rows".into(), json!(5_000_000));
            limits_object.insert(
                "max_local_prepare_source_files".into(),
                json!(datajig_core::MAX_LOCAL_PREPARE_SOURCE_FILES),
            );
            limits_object.insert(
                "max_local_prepare_source_bytes".into(),
                json!(datajig_core::MAX_LOCAL_PREPARE_SOURCE_BYTES),
            );
            limits_object.insert(
                "max_hf_import_files".into(),
                json!(datajig_core::MAX_HF_IMPORT_FILES),
            );
            limits_object.insert(
                "max_hf_import_bytes".into(),
                json!(datajig_core::MAX_HF_IMPORT_BYTES),
            );
            limits_object.insert(
                "max_hf_import_path_bytes".into(),
                json!(datajig_core::MAX_HF_IMPORT_PATH_BYTES),
            );
            limits_object.insert(
                "max_hf_import_patterns".into(),
                json!(datajig_core::MAX_HF_IMPORT_PATTERNS),
            );
            limits_object.insert(
                "max_hf_import_pattern_bytes".into(),
                json!(datajig_core::MAX_HF_IMPORT_PATTERN_BYTES),
            );
            let features = json!({
                "compact_output": true,
                "command_catalog": true,
                "deterministic_finding_ids": true,
                "filtered_pagination": true,
                "native_manifest_backend": true,
                "native_manifest_fallback": false,
                "native_inventory": true,
                "record_native_inspection": true,
                "record_diff": true,
                "jsonl_workspace": true,
                "jsonl_quality_policy": true,
                "guarded_jsonl_patch_preview": cfg!(unix),
                "evidence_bound_jsonl_patch_draft": cfg!(unix),
                "guarded_jsonl_patch_apply": cfg!(unix),
                "privacy_safe_jsonl_patch_undo": cfg!(unix),
                "verified_training_export": true,
                "verified_training_loader": true,
                "training_consumption_receipts": cfg!(unix),
                "sealed_subset_views": true,
                "deterministic_data_preparation": cfg!(unix),
                "hugging_face_revision_import": cfg!(unix),
                "native_review": true,
                "native_report_queries": true,
                "local_last_good_workflow": true,
                "immutable_dataset_revisions": true,
                "workspace_status": true,
                "revision_history": true,
                "recoverable_jsonl_revisions": true,
                "historical_revision_export": true,
                "agent_changesets": true,
                "changeset_coverage": "all_files_v2",
                "changeset_coverage_modes": ["supported_media_v1", "all_files_v2"],
                "deterministic_remediation_plans": true,
                "actionable_evidence": cfg!(unix),
                "read_only_queries": true,
                "snapshot_operations_available": cfg!(unix),
                "snapshot_manifests": true,
                "repository_managed_agent_ci": cfg!(unix),
                "agent_native_transforms": cfg!(unix),
                "deterministic_run_planning": cfg!(unix),
            });
            let mut capabilities = json!({
                "agent_api_version": AGENT_API_VERSION,
                "agent_contract_id": agent_contract_id(),
                "available": true,
                "backend": "rust",
                "identity_namespace": datajig_core::IDENTITY_NAMESPACE,
                "kind": "capabilities",
                "experimental": true,
                "platforms": ["unix"],
                "manifest_schema_versions": [1],
                "inventory_schema_versions": [1, 2],
                "jsonl_inspection_schema_versions": [1],
                "tabular_inspection_schema_versions": [1],
                "inspect_source_formats": ["jsonl", "csv", "parquet"],
                "record_diff_schema_versions": [datajig_core::RECORD_DIFF_SCHEMA_VERSION],
                "jsonl_record_state_schema_versions": [datajig_core::JSONL_RECORD_STATE_SCHEMA_VERSION],
                "jsonl_record_page_schema_versions": [datajig_core::JSONL_RECORD_PAGE_SCHEMA_VERSION],
                "jsonl_quality_policy_schema_versions": [datajig_core::JSONL_QUALITY_POLICY_SCHEMA_VERSION],
                "jsonl_patch_schema_versions": [datajig_core::JSONL_PATCH_SCHEMA_VERSION],
                "jsonl_patch_receipt_schema_versions": [1],
                "jsonl_patch_apply": {
                    "platforms": ["unix"],
                    "xattrs_required": false,
                    "xattr_policy": "preserve_when_supported",
                    "metadata_error_policy": "fail_closed_on_read_or_restore_error"
                },
                "artifact_schema_versions": [datajig_core::ARTIFACT_SCHEMA_VERSION],
                "repository_integration_schema_versions": [datajig_core::REPOSITORY_INTEGRATION_SCHEMA_VERSION],
                "prepare_recipe_schema_versions": [datajig_core::PREPARE_RECIPE_SCHEMA_VERSION],
                "prepare_source_formats": ["csv", "parquet", "jsonl"],
                "prepare_source_kinds": ["file", "directory", "hugging_face_import_receipt"],
                "hf_import_plan_schema_versions": [datajig_core::HF_IMPORT_PLAN_SCHEMA_VERSION],
                "hf_import_receipt_schema_versions": [datajig_core::HF_IMPORT_RECEIPT_SCHEMA_VERSION],
                "hf_import_repository_types": ["dataset"],
                "training_bundle_schema_versions": [datajig_core::TRAINING_BUNDLE_SCHEMA_VERSION],
                "training_consumer_plan_schema_versions": [datajig_core::TRAINING_CONSUMER_PLAN_SCHEMA_VERSION],
                "jsonl_subset_recipe_schema_versions": [datajig_core::JSONL_SUBSET_RECIPE_SCHEMA_VERSION],
                "report_schema_versions": [1],
                "workspace_schema_versions": [2, 3, 4],
                "revision_schema_versions": [1, 2, 3],
                "changeset_schema_versions": [1, 2],
                "remediation_plan_schema_versions": [1],
                "dataset_reference_schemes": ["path", "inventory"],
                "commands": command_names(),
                "tool": {"name": "datajig", "version": env!("CARGO_PKG_VERSION")},
                "limits": limits,
                "features": features
            });
            let object = capabilities
                .as_object_mut()
                .expect("capabilities document should be an object");
            object.insert(
                "run_plan_schema_versions".into(),
                json!([datajig_core::RUN_PLAN_SCHEMA_VERSION]),
            );
            object.insert("run_layout".into(), json!("stable_intent_workspace"));
            object.insert(
                "transform_plan_schema_versions".into(),
                json!([datajig_core::TRANSFORM_PLAN_SCHEMA_VERSION]),
            );
            object.insert(
                "transform_receipt_schema_versions".into(),
                json!([datajig_core::TRANSFORM_RECEIPT_SCHEMA_VERSION]),
            );
            object.insert(
                "transform_provider_protocol_versions".into(),
                json!([datajig_core::TRANSFORM_PROVIDER_PROTOCOL_VERSION]),
            );
            object.insert(
                "transform_source_formats".into(),
                json!(["csv", "parquet", "jsonl"]),
            );
            object.insert(
                "transform_output_scalar_types".into(),
                json!(["boolean", "integer", "unsigned_integer", "double", "string"]),
            );
            object.insert(
                "transform_provider".into(),
                json!({
                    "name": "duckdb", "distribution": "optional",
                    "install": "pip install 'datajig[duckdb]'", "status": "probe_required",
                    "duckdb_version": "1.5.6"
                }),
            );
            object.insert(
                "transform_limits".into(),
                json!(datajig_core::TransformLimits::v1()),
            );
            object.insert(
                "transform_ordering".into(),
                json!({"multi_row": "top_level_order_by_must_end_with_id_field"}),
            );
            object.insert(
                "training_consumption_plan_schema_versions".into(),
                json!([datajig_core::TRAINING_CONSUMPTION_PLAN_SCHEMA_VERSION]),
            );
            object.insert(
                "training_consumption_receipt_schema_versions".into(),
                json!([datajig_core::TRAINING_CONSUMPTION_RECEIPT_SCHEMA_VERSION]),
            );
            object.insert(
                "training_consumption_claim".into(),
                json!(datajig_core::TRAINING_CONSUMPTION_CLAIM),
            );
            object.insert(
                "argument_contracts".into(),
                json!({
                    "training_split": {
                        "syntax": "NAME=WEIGHT",
                        "unit": "relative_integer",
                        "weight_min": 1,
                        "normalized_weight_total": 10000,
                        "repeatable": true,
                        "example": ["train=7", "val=2", "test=1"]
                    },
                    "training_shard": {
                        "record_limit": "hard_target_final_shard_may_be_smaller",
                        "byte_limit": "soft_target_with_oversize_single_record_shards"
                    },
                    "changeset_selectors": {
                        "aliases": ["@active", "@latest"],
                        "resolution": "unique compatible candidate or fail closed",
                        "omission": "resolve the unique active pair or fail closed",
                        "stage": "resolve one declaration matching the current HEAD or fail closed",
                        "pair": "an explicit changeset binds its declaration; two aliases require one staged pair"
                    }
                }),
            );
            println!("{capabilities}");
        }
        Command::Describe { command } => {
            let payload = if let Some(name) = command {
                let descriptor = command_descriptor(&name).ok_or_else(|| {
                    (
                        "COMMAND_NOT_FOUND",
                        4,
                        anyhow::anyhow!("unknown command: {name}"),
                    )
                })?;
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "agent_contract_id": agent_contract_id(),
                    "command_schema_version": COMMAND_SCHEMA_VERSION,
                    "kind": "command_descriptor",
                    "command": descriptor,
                    "tool": {"name": "datajig", "version": env!("CARGO_PKG_VERSION")}
                })
            } else {
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "agent_contract_id": agent_contract_id(),
                    "command_schema_version": COMMAND_SCHEMA_VERSION,
                    "kind": "command_catalog",
                    "commands": command_catalog(),
                    "tool": {"name": "datajig", "version": env!("CARGO_PKG_VERSION")}
                })
            };
            println!("{payload}");
        }
        Command::Tutorial { output } => {
            let artifact =
                run_tutorial(&output).map_err(|error| (workspace_error_code(&error), 2, error))?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "tutorial_completed",
                    "decision": "ready",
                    "next_actions": [{"command": "export-info", "args": [
                        artifact.manifest_path, "--verify",
                        "--expect-bundle", artifact.bundle_id,
                        "--expect-revision", artifact.revision_id
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::Run {
            task,
            strict,
            resume,
            accept_plan,
            yes,
            output,
            consume,
            consumer,
            run_id,
            run_dir,
        } => {
            let root =
                std::env::current_dir().map_err(|error| ("RUN_PATH_INVALID", 2, error.into()))?;
            if let Some(attempt_id) = resume {
                let accepted_plan = accept_plan.as_deref().expect("clap requires --accept-plan");
                let plan = load_run_plan_for_resume(&root, &attempt_id, accepted_plan)
                    .map_err(run_control_command_error)?;
                let live_authorization = reverify_resumed_run(&root, &plan)?;
                emit_resumed_run_plan(plan, yes, live_authorization)?;
            } else {
                let task = task.as_deref().expect("clap requires a task or --resume");
                let compiled = compile_task(task).map_err(run_dsl_command_error)?;
                let compiled = resolve_run_source_format(&root, compiled)
                    .map_err(run_control_command_error)?;
                let source = PathBuf::from(&compiled.ast.source.path);
                let source_content_id =
                    fingerprint_run_source(&root, &source).map_err(run_control_command_error)?;
                let output_text = output.to_str().ok_or_else(|| {
                    (
                        "RUN_PATH_INVALID",
                        2,
                        anyhow::anyhow!("--output must be valid UTF-8"),
                    )
                })?;
                let authorization = classify_run_output(&root, &output, &compiled.intent_id)
                    .map_err(run_control_command_error)?;
                let consumption = run_consumption_binding(consume, consumer, run_id, run_dir)?;
                let binding = RunPlanBinding {
                    source_content_id,
                    engine_version: format!("datajig-run-engine-v1/{}", env!("CARGO_PKG_VERSION")),
                    output: output_text.into(),
                    consumption,
                };
                let nonce = run_attempt_nonce();
                let plan = create_run_plan(&compiled, &binding, &nonce, authorization.level, yes)
                    .map_err(run_control_command_error)?;
                let layout = persist_run_plan(&root, &plan).map_err(run_control_command_error)?;
                let strict = strict
                    || std::env::var("DATAJIG_STRICT")
                        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
                emit_new_run_plan(plan, layout.plan, strict);
            }
        }
        Command::Init {
            dataset,
            state,
            threads,
            id_field,
            policy,
            source_receipt,
        } => {
            let artifact = if dataset.is_file() || id_field.is_some() {
                let id_field = id_field.as_deref().ok_or_else(|| {
                    (
                        "INVALID_ARGUMENT",
                        2,
                        anyhow::anyhow!("JSONL workspace initialization requires --id-field"),
                    )
                })?;
                initialize_jsonl_workspace_with_receipt(
                    &dataset,
                    &state,
                    id_field,
                    policy.as_deref(),
                    source_receipt.as_deref(),
                )
            } else {
                if policy.is_some() || source_receipt.is_some() {
                    return Err((
                        "INVALID_ARGUMENT",
                        2,
                        anyhow::anyhow!(
                            "--policy and --source-receipt are only valid for JSONL workspaces"
                        ),
                    ));
                }
                initialize_workspace(&dataset, &state, threads)
            }
            .map_err(|error| (workspace_error_code(&error), 2, error))?;
            let next_actions = if artifact.adapter == "jsonl" {
                json!([{
                    "command": "changeset-begin",
                    "args": ["--state", artifact.state_dir],
                    "required_options": ["--intent", "--task-id"]
                }])
            } else {
                json!([{"command": "check", "args": ["--state", artifact.state_dir]}])
            };
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "workspace_initialized",
                    "decision": "ready",
                    "next_actions": next_actions,
                    "artifact": artifact
                })
            );
        }
        Command::Export {
            state,
            output,
            view,
            revision,
            allow_detached,
            seed,
            splits,
            max_shard_records,
            max_shard_bytes,
        } => {
            let artifact = if allow_detached {
                export_training_bundle_with_view_at_detached_revision(
                    &state,
                    &output,
                    revision.as_deref().expect("clap requires revision"),
                    seed,
                    &splits,
                    max_shard_records,
                    max_shard_bytes,
                )
            } else {
                export_training_bundle_with_view_at_revision(
                    &state,
                    &output,
                    view.as_deref(),
                    revision.as_deref(),
                    seed,
                    &splits,
                    max_shard_records,
                    max_shard_bytes,
                )
            }
            .map_err(training_export_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "training_bundle_created",
                    "decision": "verify",
                    "next_actions": [{"command": "export-info", "args": [
                        artifact.manifest, "--verify", "--expect-revision",
                        artifact.source_revision_id
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::ExportInfo {
            manifest,
            verify,
            consumer_plan,
            expect_bundle,
            expect_revision,
            require_assurance,
            split,
        } => {
            let artifact = inspect_training_bundle_with_consumer(
                &manifest,
                verify,
                consumer_plan,
                expect_bundle.as_deref(),
                expect_revision.as_deref(),
                require_assurance.as_deref(),
                split.as_deref(),
            )
            .map_err(|error| (training_bundle_error_code(&error), 2, error))?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "training_bundle_info",
                    "decision": if verify { "ready" } else { "verify" },
                    "next_actions": if verify {
                        json!([])
                    } else {
                        json!([{"command": "export-info", "args": [manifest, "--verify"]}])
                    },
                    "artifact": artifact
                })
            );
        }
        Command::ConsumePlan {
            manifest,
            split,
            consumer,
            run_id,
            output,
            plan,
        } => {
            let artifact =
                plan_training_consumption(&manifest, &split, &consumer, &run_id, &output, &plan)
                    .map_err(training_consumption_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "training_consumption_planned",
                    "decision": "consume",
                    "next_actions": [{"command": "consume-info", "args": [
                        artifact.plan, "--verify", "--accept-plan",
                        artifact.consumption_plan_id
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::ConsumeInfo {
            plan,
            verify: _,
            accept_plan,
        } => {
            let artifact = inspect_training_consumption(&plan, &accept_plan)
                .map_err(training_consumption_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "training_consumption_info",
                    "decision": "consume",
                    "next_actions": [],
                    "artifact": artifact
                })
            );
        }
        Command::ViewCheck { state, recipe } => {
            let artifact = check_subset_view(&state, &recipe)
                .map_err(|error| (subset_view_error_code(&error), 2, error))?;
            let next_actions = if artifact.exportable {
                json!([{
                    "command": "export",
                    "args": ["--state", artifact.state_dir, "--view", recipe],
                    "required_options": ["--output", "--split"]
                }])
            } else {
                json!([])
            };
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "subset_view_checked",
                    "decision": if artifact.exportable { "export" } else { "empty" },
                    "next_actions": next_actions,
                    "artifact": artifact
                })
            );
        }
        Command::ChangesetBegin {
            state,
            threads,
            intent,
            task_id,
            actor_kind,
        } => {
            let artifact = begin_changeset(&state, threads, &intent, &task_id, &actor_kind)
                .map_err(|error| (workspace_error_code(&error), 2, error))?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "change_declared",
                    "decision": "edit",
                    "next_actions": [{"command": "changeset-stage", "args": [
                        "--state", state, "--change", artifact.change_id()
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::ChangesetStage {
            state,
            threads,
            change,
        } => {
            let change = resolve_change_selector(&state, &change)
                .map_err(|error| (workspace_error_code(&error), 2, error))?;
            let artifact = stage_changeset(&state, threads, &change)
                .map_err(|error| (workspace_error_code(&error), 2, error))?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "changeset_staged",
                    "decision": "review",
                    "next_actions": [{"command": "check", "args": [
                        "--state", state,
                        "--change", artifact.change_id(),
                        "--changeset", artifact.changeset_id()
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::Check {
            state,
            threads,
            phash_threshold,
            change,
            changeset,
        } => {
            let resolved_binding =
                resolve_optional_changeset_context(&state, change.as_deref(), changeset.as_deref())
                    .map_err(|error| (workspace_error_code(&error), 2, error))?;
            let binding = resolved_binding
                .as_ref()
                .map(|(change, changeset)| (change.as_str(), changeset.as_str()));
            let artifact = match binding {
                Some((change_id, changeset_id)) => {
                    check_changeset(&state, threads, phash_threshold, change_id, changeset_id)
                }
                None => check_workspace(&state, threads, phash_threshold),
            }
            .map_err(|error| (workspace_error_code(&error), 2, error))?;
            let next_actions = if let Some((change_id, changeset_id)) = binding {
                let context_args = json!([
                    "--state",
                    state,
                    "--change",
                    change_id,
                    "--changeset",
                    changeset_id
                ]);
                if artifact.decision == "seal" && artifact.findings > 0 {
                    let seal_args = json!([
                        "--state",
                        state,
                        "--change",
                        change_id,
                        "--changeset",
                        changeset_id,
                        "--accept-report",
                        artifact.report_content_id
                    ]);
                    json!([
                        {"command": "findings", "args": [
                            artifact.report_path, "--offset", "0", "--limit", "50"
                        ]},
                        {"command": "seal", "args": seal_args}
                    ])
                } else if artifact.decision == "seal" {
                    json!([{"command": "seal", "args": context_args}])
                } else {
                    json!([
                        {"command": "review-plan", "args": context_args},
                        {"command": "findings", "args": [
                            artifact.report_path, "--offset", "0", "--limit", "50"
                        ]}
                    ])
                }
            } else if artifact.decision == "seal" && artifact.findings > 0 {
                json!([
                    {"command": "findings", "args": [
                        artifact.report_path, "--offset", "0", "--limit", "50"
                    ]},
                    {"command": "seal", "args": [
                        "--state", state,
                        "--accept-report", artifact.report_content_id
                    ]}
                ])
            } else if artifact.decision == "seal" {
                json!([{"command": "seal", "args": ["--state", state]}])
            } else {
                json!([
                    {"command": "review-plan", "args": ["--state", state]}
                ])
            };
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "workspace_checked",
                    "decision": artifact.decision,
                    "next_actions": next_actions,
                    "artifact": artifact
                })
            );
        }
        Command::ReviewPlan {
            state,
            change,
            changeset,
        } => {
            let resolved_binding =
                resolve_optional_changeset_context(&state, change.as_deref(), changeset.as_deref())
                    .map_err(|error| (workspace_error_code(&error), 2, error))?;
            let binding = resolved_binding
                .as_ref()
                .map(|(change, changeset)| (change.as_str(), changeset.as_str()));
            let artifact = match binding {
                Some((change_id, changeset_id)) => {
                    plan_changeset(&state, 1, change_id, changeset_id)
                }
                None => plan_workspace(&state),
            }
            .map_err(|error| (workspace_error_code(&error), 2, error))?;
            let next_actions = if let Some((change_id, changeset_id)) = binding {
                let context_args = json!([
                    "--state",
                    state,
                    "--change",
                    change_id,
                    "--changeset",
                    changeset_id
                ]);
                if artifact.decision == "seal" {
                    let seal_args = json!([
                        "--state",
                        state,
                        "--change",
                        change_id,
                        "--changeset",
                        changeset_id,
                        "--accept-report",
                        artifact.plan.report_content_id
                    ]);
                    json!([
                        {"command": "findings", "args": [
                            format!("{}/latest.review.json", artifact.state_dir),
                            "--offset", "0", "--limit", "50"
                        ]},
                        {"command": "seal", "args": seal_args}
                    ])
                } else {
                    json!([{"command": "findings", "args": [
                        format!("{}/latest.review.json", artifact.state_dir),
                        "--offset", "0", "--limit", "50"
                    ]}, {"command": "check", "args": context_args}])
                }
            } else if artifact.decision == "seal" {
                json!([
                    {"command": "findings", "args": [
                        format!("{}/latest.review.json", artifact.state_dir),
                        "--offset", "0", "--limit", "50"
                    ]},
                    {"command": "seal", "args": [
                        "--state", state,
                        "--accept-report", artifact.plan.report_content_id
                    ]}
                ])
            } else {
                json!([
                    {"command": "finding", "args": [
                        format!("{}/latest.review.json", artifact.state_dir),
                        "<finding_id>"
                    ]},
                    {"command": "check", "args": ["--state", state]}
                ])
            };
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "remediation_plan_created",
                    "decision": artifact.decision,
                    "next_actions": next_actions,
                    "artifact": artifact.plan
                })
            );
        }
        Command::Log {
            state,
            offset,
            limit,
        } => {
            let artifact = revision_log(&state, offset, limit)
                .map_err(|error| (workspace_error_code(&error), 2, error))?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "revision_page",
                    "decision": "inspect",
                    "next_actions": [],
                    "artifact": artifact
                })
            );
        }
        Command::Materialize {
            revision,
            state,
            output,
        } => {
            let artifact = materialize_revision(&state, &revision, &output)
                .map_err(materialize_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "revision_materialized",
                    "decision": "ready",
                    "next_actions": [{"command": "inspect", "args": [
                        artifact.output.clone(),
                        "--id-field",
                        artifact.id_field.clone()
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::Locate {
            report,
            finding_id,
            state,
            change,
            changeset,
            offset,
            limit,
        } => {
            let artifact = locate_changeset_finding(
                &state,
                &report,
                &finding_id,
                &change,
                &changeset,
                offset,
                limit,
            )
            .map_err(locate_command_error)?;
            let next_actions = if is_patchable_quality_code(&artifact.finding_code) {
                let required_options =
                    if artifact.locations.len() == 1 && !artifact.evidence_truncated {
                        json!(["--after-json|--remove", "--output"])
                    } else {
                        json!(["--record", "--after-json|--remove", "--output"])
                    };
                json!([{"command": "patch-draft", "args": [
                    report, finding_id, "--state", state,
                    "--change", change, "--changeset", changeset
                ], "required_options": required_options}])
            } else if artifact.page.has_more {
                json!([{"command": "locate", "args": [
                    report, finding_id, "--state", state,
                    "--change", change, "--changeset", changeset,
                    "--offset", artifact.page.offset.saturating_add(artifact.page.returned).to_string(),
                    "--limit", artifact.page.limit.to_string()
                ]}])
            } else {
                json!([{"command": "changeset-stage", "args": [
                    "--state", state, "--change", change
                ]}])
            };
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "finding_locations",
                    "decision": "edit",
                    "next_actions": next_actions,
                    "artifact": artifact
                })
            );
        }
        Command::PatchPreview {
            request,
            report,
            state,
            change,
            changeset,
        } => {
            let artifact = preview_jsonl_patch(&state, &request, &report, &change, &changeset)
                .map_err(patch_preview_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "jsonl_patch_preview",
                    "decision": "edit",
                    "next_actions": [{"command": "patch-apply", "args": [
                        request, report, "--state", state, "--change", change,
                        "--changeset", changeset, "--accept-patch", artifact.patch_id
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::PatchDraft {
            report,
            finding_id,
            state,
            change,
            changeset,
            record,
            after_json,
            remove: _,
            output,
        } => {
            let after = after_json
                .map(|text| {
                    let value: serde_json::Value = serde_json::from_str(&text)
                        .map_err(|error| anyhow::anyhow!("--after-json is invalid: {error}"))?;
                    if value.is_array() || value.is_object() {
                        return Err(anyhow::anyhow!("--after-json must encode a JSON scalar"));
                    }
                    Ok(value)
                })
                .transpose()
                .map_err(|error| ("INVALID_ARGUMENT", 2, error))?;
            let artifact = draft_jsonl_patch(
                &state,
                &report,
                &finding_id,
                &change,
                &changeset,
                record.as_deref(),
                after,
                &output,
            )
            .map_err(patch_preview_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "jsonl_patch_request_drafted",
                    "decision": "preview",
                    "next_actions": [{"command": "patch-preview", "args": [
                        output, report, "--state", state, "--change", change,
                        "--changeset", changeset
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::PatchApply {
            request,
            report,
            state,
            change,
            changeset,
            accept_patch,
        } => {
            let artifact = apply_jsonl_patch(
                &state,
                &request,
                &report,
                &change,
                &changeset,
                &accept_patch,
            )
            .map_err(patch_transaction_command_error)?;
            let next_actions =
                artifact
                    .changeset_id
                    .as_ref()
                    .map_or_else(Vec::new, |next_changeset| {
                        vec![json!({"command": "check", "args": [
                            "--state", state, "--change", change,
                            "--changeset", next_changeset
                        ]})]
                    });
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "jsonl_patch_applied",
                    "decision": if artifact.changeset_id.is_some() { "review" } else { "clean" },
                    "next_actions": next_actions,
                    "artifact": artifact
                })
            );
        }
        Command::HfImportPlan {
            repository,
            revision,
            includes,
            ignores,
            output,
            plan,
        } => {
            let artifact =
                plan_hf_import(&repository, &revision, &includes, &ignores, &output, &plan)
                    .map_err(hf_import_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "hugging_face_import_planned",
                    "decision": "apply",
                    "next_actions": [{"command": "hf-import-apply", "args": [
                        artifact.plan, "--accept-plan", artifact.plan_id
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::HfImportApply { plan, accept_plan } => {
            let artifact = apply_hf_import(&plan, &accept_plan).map_err(hf_import_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "hugging_face_import_applied",
                    "decision": "ready",
                    "next_actions": [{"command": "artifact-schema", "args": ["prepare-recipe"]}],
                    "artifact": artifact
                })
            );
        }
        Command::PreparePlan {
            source,
            recipe,
            output,
            plan,
        } => {
            let artifact =
                plan_prepare(&source, &recipe, &output, &plan).map_err(prepare_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "prepare_planned",
                    "decision": "apply",
                    "next_actions": [{"command": "prepare-apply", "args": [
                        artifact.plan, "--accept-plan", artifact.plan_id
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::PrepareApply { plan, accept_plan } => {
            let artifact = apply_prepare(&plan, &accept_plan).map_err(prepare_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "prepare_applied",
                    "decision": "ready",
                    "next_actions": [{"command": "init", "args": [
                        artifact.output, "--id-field", artifact.id_field
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::TransformPlan {
            inputs,
            sql,
            sql_file,
            params,
            params_file,
            id_field,
            output,
            plan,
        } => {
            let python = transform_provider_python()?;
            let inputs = parse_transform_inputs(&inputs)?;
            let parameters = load_transform_parameters(params.as_deref(), params_file.as_deref())?;
            let sql = load_transform_sql(sql, sql_file.as_deref())?;
            let artifact = plan_transform(datajig_core::TransformPlanRequest {
                inputs,
                sql,
                parameters,
                id_field,
                output_path: output,
                plan_path: plan,
                python,
            })
            .map_err(transform_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "transform_planned",
                    "decision": "apply",
                    "next_actions": [{"command": "transform-apply", "args": [
                        artifact.plan, "--accept-plan", artifact.plan_id
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::TransformApply { plan, accept_plan } => {
            let python = std::env::var_os("_DATAJIG_PROVIDER_PYTHON").map(PathBuf::from);
            let artifact = datajig_core::apply_transform_with_optional_provider(
                &plan,
                &accept_plan,
                python.as_deref(),
            )
            .map_err(transform_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "transform_applied",
                    "decision": "ready",
                    "next_actions": [{"command": "init", "args": [
                        artifact.output, "--id-field", artifact.id_field,
                        "--source-receipt", artifact.receipt
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::TransformInfo { artifact, verify } => {
            let info = inspect_transform(&artifact, verify).map_err(transform_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "transform_info",
                    "decision": "inspect",
                    "next_actions": [],
                    "artifact": info
                })
            );
        }
        Command::PatchUndo { undo_id, state } => {
            let artifact =
                undo_jsonl_patch(&state, &undo_id).map_err(patch_transaction_command_error)?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "jsonl_patch_undone",
                    "decision": "review",
                    "next_actions": [{"command": "check", "args": [
                        "--state", state, "--change", artifact.change_id,
                        "--changeset", artifact.changeset_id
                    ]}],
                    "artifact": artifact
                })
            );
        }
        Command::Seal {
            state,
            threads,
            message,
            accept_report,
            change,
            changeset,
            detached,
            source_receipt,
            commit_revision,
            expected_head,
        } => {
            if let Some(revision_id) = commit_revision {
                let artifact = compare_and_swap_workspace_head(
                    &state,
                    expected_head
                        .as_deref()
                        .expect("clap requires expected HEAD"),
                    &revision_id,
                )
                .map_err(|error| (workspace_error_code(&error), 2, error))?;
                println!(
                    "{}",
                    json!({
                        "agent_api_version": AGENT_API_VERSION,
                        "backend": "rust",
                        "kind": "workspace_head_committed",
                        "decision": "ready",
                        "next_actions": [],
                        "artifact": artifact
                    })
                );
                return Ok(());
            }
            let resolved_binding =
                resolve_optional_changeset_context(&state, change.as_deref(), changeset.as_deref())
                    .map_err(|error| (workspace_error_code(&error), 2, error))?;
            let binding = resolved_binding
                .as_ref()
                .map(|(change, changeset)| (change.as_str(), changeset.as_str()));
            let artifact = match binding {
                Some((change_id, changeset_id)) if detached => seal_changeset_detached(
                    &state,
                    &message,
                    accept_report.as_deref(),
                    change_id,
                    changeset_id,
                    source_receipt.as_deref(),
                ),
                Some((change_id, changeset_id)) => seal_changeset(
                    &state,
                    threads,
                    &message,
                    accept_report.as_deref(),
                    change_id,
                    changeset_id,
                ),
                None => seal_workspace(&state, threads, &message, accept_report.as_deref()),
            }
            .map_err(|error| (workspace_error_code(&error), 2, error))?;
            let next_actions = if artifact.baseline_state_id.is_some() {
                json!([{
                    "command": "changeset-begin",
                    "args": ["--state", artifact.state_dir],
                    "required_options": ["--intent", "--task-id"]
                }])
            } else {
                json!([{"command": "check", "args": ["--state", artifact.state_dir]}])
            };
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "dataset_sealed",
                    "decision": "revision_created",
                    "next_actions": next_actions,
                    "artifact": artifact
                })
            );
        }
        Command::Status {
            state,
            threads,
            change,
            changeset,
        } => {
            let automatic_binding = change.is_none() && changeset.is_none();
            let workspace_status = if automatic_binding {
                Some(
                    status_workspace(&state, threads)
                        .map_err(|error| (workspace_error_code(&error), 2, error))?,
                )
            } else {
                None
            };
            let resolved_binding = if workspace_status.as_ref().is_some_and(|status| status.clean) {
                None
            } else if automatic_binding {
                resolve_optional_changeset_context(&state, None, None)
                    .map_err(|error| (workspace_error_code(&error), 2, error))?
            } else {
                resolve_optional_changeset_context(&state, change.as_deref(), changeset.as_deref())
                    .map_err(|error| (workspace_error_code(&error), 2, error))?
            };
            let binding = resolved_binding
                .as_ref()
                .map(|(change, changeset)| (change.as_str(), changeset.as_str()));
            let artifact = match binding {
                Some((change_id, changeset_id)) => {
                    status_changeset(&state, threads, change_id, changeset_id)
                }
                None => Ok(workspace_status.expect("automatic status was loaded")),
            }
            .map_err(|error| (workspace_error_code(&error), 2, error))?;
            let next_actions = if let Some((change_id, changeset_id)) = binding {
                if artifact.unstaged_changes == Some(true) {
                    json!([{"command": "changeset-stage", "args": [
                        "--state", state, "--change", change_id
                    ]}])
                } else {
                    json!([{"command": "check", "args": [
                        "--state", state, "--change", change_id,
                        "--changeset", changeset_id
                    ]}])
                }
            } else if artifact.clean || artifact.current_state_id.is_some() {
                json!([])
            } else {
                json!([{"command": "check", "args": ["--state", state]}])
            };
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "workspace_status",
                    "decision": artifact.decision,
                    "next_actions": next_actions,
                    "artifact": artifact
                })
            );
        }
        Command::Inventory {
            root,
            output,
            threads,
        } => {
            let inventory = create_inventory(&root, &output, threads)
                .map_err(|error| (inventory_error_code(&error), 2, error))?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "inventory_created",
                    "inventory_schema_version": inventory.schema_version(),
                    "summary": inventory.summary(),
                    "output": output
                        .canonicalize()
                        .map_err(anyhow::Error::from)
                        .map_err(|error| ("INVENTORY_ERROR", 2, error))?
                        .to_string_lossy()
                })
            );
        }
        Command::Inspect {
            source,
            id_field,
            delimiter,
        } => match source
            .extension()
            .and_then(|value| value.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("jsonl") => {
                if delimiter != "," {
                    return Err((
                        "INVALID_ARGUMENT",
                        2,
                        anyhow::anyhow!("--delimiter is only valid for CSV"),
                    ));
                }
                let artifact = inspect_jsonl(&source, &id_field)
                    .map_err(|error| (inspect_error_code(&error), 2, error))?;
                let decision = if artifact.finding_count() == 0 {
                    "ready"
                } else {
                    "fix"
                };
                let next_actions = if decision == "ready" && cfg!(unix) {
                    let source = PathBuf::from(artifact.source());
                    let action = source.parent().and_then(|parent| {
                        let name = source.file_stem()?.to_str()?;
                        let state = parent.join(format!(".datajig-{name}"));
                        Some(json!({"command": "init", "args": [
                            source.to_string_lossy(), "--id-field", id_field,
                            "--state", state.to_string_lossy()
                        ]}))
                    });
                    action.map_or_else(|| json!([]), |action| json!([action]))
                } else {
                    json!([{"command": "inspect", "args": [
                        artifact.source(), "--id-field", id_field
                    ]}])
                };
                println!(
                    "{}",
                    json!({
                        "agent_api_version": AGENT_API_VERSION,
                        "backend": "rust",
                        "kind": "dataset_inspection",
                        "decision": decision,
                        "next_actions": next_actions,
                        "artifact": artifact
                    })
                );
            }
            Some("csv" | "parquet") => {
                let artifact = inspect_tabular(&source, &id_field, Some(&delimiter))
                    .map_err(|error| (inspect_error_code(&error), 2, error))?;
                let decision = if artifact.finding_count() == 0 {
                    "prepare"
                } else {
                    "fix"
                };
                let next_actions = if decision == "prepare" {
                    json!([{
                        "command": "prepare-plan",
                        "args": [artifact.source()],
                        "artifact_inputs": {"recipe": "artifact.recipe_template"},
                        "required_options": ["--recipe", "--output", "--plan"]
                    }])
                } else {
                    let mut args = vec![
                        json!(artifact.source()),
                        json!("--id-field"),
                        json!(id_field),
                    ];
                    if artifact.format() == "csv" && delimiter != "," {
                        args.extend([json!("--delimiter"), json!(delimiter)]);
                    }
                    json!([{"command": "inspect", "args": args}])
                };
                println!(
                    "{}",
                    json!({
                        "agent_api_version": AGENT_API_VERSION,
                        "backend": "rust",
                        "kind": "dataset_inspection",
                        "decision": decision,
                        "next_actions": next_actions,
                        "artifact": artifact
                    })
                );
            }
            _ => {
                return Err((
                    "INVALID_ARGUMENT",
                    2,
                    anyhow::anyhow!("inspect requires a .jsonl, .csv, or .parquet file"),
                ));
            }
        },
        Command::RecordDiff {
            before,
            after,
            id_field,
        } => {
            let artifact = diff_jsonl_records(&before, &after, &id_field)
                .map_err(|error| (inspect_error_code(&error), 2, error))?;
            let decision = if artifact.total_changes() == 0 && !artifact.byte_only_changed() {
                "unchanged"
            } else {
                "review"
            };
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "record_diff",
                    "decision": decision,
                    "next_actions": [],
                    "artifact": artifact
                })
            );
        }
        Command::Review {
            before,
            after,
            output,
            threads,
            phash_threshold,
        } => {
            let artifact = create_review(&before, &after, &output, threads, phash_threshold)
                .map_err(|error| (review_error_code(&error), 2, error))?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "review_created",
                    "artifact": artifact,
                    "next_actions": [
                        {"command": "explain", "args": [artifact.output, "--limit", "10"]},
                        {"command": "findings", "args": [artifact.output, "--limit", "50"]}
                    ]
                })
            );
        }
        Command::Snapshot {
            root,
            output,
            threads,
        } => {
            let manifest = create_snapshot(&root, &output, threads)
                .map_err(|error| (snapshot_error_code(&error), 2, error))?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "backend": "rust",
                    "kind": "snapshot_created",
                    "manifest_schema_version": manifest.schema_version(),
                    "snapshot_id": manifest.snapshot_id(),
                    "summary": manifest.summary(),
                    "output": output
                        .canonicalize()
                        .map_err(anyhow::Error::from)
                        .map_err(|error| ("SNAPSHOT_ERROR", 2, error))?
                        .to_string_lossy()
                })
            );
        }
        Command::SnapshotInfo { manifest } => {
            let manifest = load_manifest(&manifest)
                .map_err(|error| (manifest_error_code(&error), 2, error))?;
            println!(
                "{}",
                json!({
                    "agent_api_version": AGENT_API_VERSION,
                    "kind": "snapshot_info",
                    "manifest_schema_version": manifest.schema_version(),
                    "snapshot_id": manifest.snapshot_id(),
                    "summary": manifest.summary()
                })
            );
        }
        Command::SnapshotDiff {
            before,
            after,
            offset,
            limit,
        } => {
            if offset < 0 || !(1..=MAX_DIFF_PAGE_SIZE as i64).contains(&limit) {
                return Err((
                    "INVALID_ARGUMENT",
                    2,
                    anyhow::anyhow!(
                        "offset must be non-negative and limit must be between 1 and {MAX_DIFF_PAGE_SIZE}"
                    ),
                ));
            }
            let before =
                load_manifest(&before).map_err(|error| (manifest_error_code(&error), 2, error))?;
            let after =
                load_manifest(&after).map_err(|error| (manifest_error_code(&error), 2, error))?;
            let diff = diff_manifests(&before, &after);
            let page = manifest_diff_page(&diff, offset, limit)
                .map_err(|error| ("MANIFEST_DIFF_ERROR", 2, error))?;
            println!(
                "{}",
                serde_json::to_string(&page)
                    .map_err(anyhow::Error::from)
                    .map_err(|error| ("MANIFEST_DIFF_ERROR", 2, error))?
            );
        }
        Command::Findings {
            report,
            severity,
            code,
            offset,
            limit,
        } => {
            let report = load_report(&report).map_err(report_command_error)?;
            let severity = severity.into_iter().map(Severity::from).collect::<Vec<_>>();
            let result = list_findings(&report, &severity, &code, offset, limit)
                .map_err(|error| ("INVALID_ARGUMENT", 2, error))?;
            println!("{result}");
        }
        Command::Finding { report, finding_id } => {
            let report = load_report(&report).map_err(report_command_error)?;
            let result = get_finding(&report, &finding_id).map_err(|error| {
                if error.downcast_ref::<FindingNotFoundError>().is_some() {
                    ("FINDING_NOT_FOUND", 4, error)
                } else {
                    ("INVALID_REPORT", 2, error)
                }
            })?;
            println!("{result}");
        }
        Command::Explain { report, limit } => {
            let report = load_report(&report).map_err(report_command_error)?;
            let result =
                compact_summary(&report, limit).map_err(|error| ("INVALID_ARGUMENT", 2, error))?;
            println!("{result}");
        }
    }
    Ok(())
}

fn validate_command_arguments(command: &Command) -> Result<(), CommandError> {
    if let Command::Export {
        seed,
        splits,
        max_shard_records,
        max_shard_bytes,
        ..
    } = command
    {
        TrainingExportConfig::new(seed.clone(), splits, *max_shard_records, *max_shard_bytes)
            .map_err(training_export_command_error)?;
    }
    if let Command::Run {
        strict,
        resume,
        output,
        consume,
        consumer,
        run_id,
        run_dir,
        ..
    } = command
    {
        if resume.is_some()
            && (*strict
                || *consume
                || consumer.is_some()
                || run_id.is_some()
                || run_dir.is_some()
                || output != Path::new("bundle"))
        {
            return Err((
                "INVALID_ARGUMENT",
                2,
                anyhow::anyhow!(
                    "--resume accepts only --accept-plan and optional --yes; all execution inputs come from the immutable plan"
                ),
            ));
        }
        if *consume && (consumer.is_none() || run_id.is_none() || run_dir.is_none()) {
            return Err((
                "INVALID_ARGUMENT",
                2,
                anyhow::anyhow!(
                    "--consume requires --consumer, --run-id, and --run-dir; example: --consume --consumer pytorch --run-id training-001 --run-dir runs/training-001"
                ),
            ));
        }
    }
    Ok(())
}

fn run_consumption_binding(
    consume: bool,
    consumer: Option<String>,
    run_id: Option<String>,
    run_dir: Option<PathBuf>,
) -> Result<Option<RunConsumptionBinding>, CommandError> {
    if !consume {
        return Ok(None);
    }
    let missing = [
        ("--consumer", consumer.is_none()),
        ("--run-id", run_id.is_none()),
        ("--run-dir", run_dir.is_none()),
    ]
    .into_iter()
    .filter_map(|(name, absent)| absent.then_some(name))
    .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err((
            "INVALID_ARGUMENT",
            2,
            anyhow::anyhow!(
                "--consume requires {}; example: --consume --consumer pytorch --run-id training-001 --run-dir runs/training-001",
                missing.join(", ")
            ),
        ));
    }
    let run_dir = run_dir
        .expect("checked above")
        .to_str()
        .ok_or_else(|| {
            (
                "INVALID_ARGUMENT",
                2,
                anyhow::anyhow!("--run-dir must be valid UTF-8"),
            )
        })?
        .to_owned();
    Ok(Some(RunConsumptionBinding {
        consumer: consumer.expect("checked above"),
        run_id: run_id.expect("checked above"),
        run_dir,
    }))
}

fn run_attempt_nonce() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{}-{nanos}", std::process::id())
}

fn run_response_artifact(plan: &RunPlanArtifact, plan_path: Option<&Path>) -> Value {
    let mut artifact = serde_json::to_value(plan).expect("run plan should serialize");
    let object = artifact
        .as_object_mut()
        .expect("run plan should serialize as an object");
    object.insert("execution_status".into(), json!("planned"));
    if let Some(path) = plan_path {
        object.insert("plan_path".into(), json!(path));
    }
    artifact
}

fn emit_new_run_plan(plan: RunPlanArtifact, plan_path: PathBuf, strict: bool) {
    let requires_authorization =
        plan.authorization.status == AuthorizationStatus::ConfirmationRequired;
    let requires_acceptance = strict || requires_authorization;
    let mut args = vec![
        "--resume".to_owned(),
        plan.attempt_id.clone(),
        "--accept-plan".to_owned(),
        plan.plan_id.clone(),
    ];
    if requires_authorization {
        args.push("--yes".into());
    }
    println!(
        "{}",
        json!({
            "agent_api_version": AGENT_API_VERSION,
            "backend": "rust",
            "kind": if requires_acceptance { "run_plan_ready" } else { "run_plan_accepted" },
            "decision": if requires_acceptance { "confirmation_required" } else { "ready" },
            "next_actions": if requires_acceptance {
                json!([{"command": "run", "args": args}])
            } else {
                json!([])
            },
            "artifact": run_response_artifact(&plan, Some(&plan_path))
        })
    );
}

fn emit_resumed_run_plan(
    plan: RunPlanArtifact,
    yes: bool,
    live_level: datajig_core::AuthorizationLevel,
) -> Result<(), CommandError> {
    let authorization_level = if plan.authorization.level
        == datajig_core::AuthorizationLevel::Dangerous
        || live_level == datajig_core::AuthorizationLevel::Dangerous
    {
        datajig_core::AuthorizationLevel::Dangerous
    } else {
        datajig_core::AuthorizationLevel::Safe
    };
    let authorization = datajig_core::authorize_run(authorization_level, yes);
    if authorization.status == AuthorizationStatus::Rejected {
        return Err((
            "RUN_AUTHORIZATION_REJECTED",
            2,
            anyhow::anyhow!("fatal run authorization cannot be overridden by --yes"),
        ));
    }
    let confirmation_required = authorization.status == AuthorizationStatus::ConfirmationRequired;
    let next_actions = if confirmation_required {
        json!([{
            "command": "run",
            "args": ["--resume", plan.attempt_id, "--accept-plan", plan.plan_id, "--yes"]
        }])
    } else {
        json!([])
    };
    println!(
        "{}",
        json!({
            "agent_api_version": AGENT_API_VERSION,
            "backend": "rust",
            "kind": if confirmation_required { "run_plan_ready" } else { "run_plan_accepted" },
            "decision": if confirmation_required { "confirmation_required" } else { "ready" },
            "next_actions": next_actions,
            "artifact": run_response_artifact(&plan, None)
        })
    );
    Ok(())
}

fn reverify_resumed_run(
    root: &Path,
    plan: &RunPlanArtifact,
) -> Result<datajig_core::AuthorizationLevel, CommandError> {
    let compiled = datajig_core::canonicalize_run_task(plan.canonical_ast.clone())
        .map_err(run_dsl_command_error)?;
    resolve_run_source_format(root, compiled).map_err(run_control_command_error)?;
    let source = PathBuf::from(&plan.canonical_ast.source.path);
    let current_source =
        fingerprint_run_source(root, &source).map_err(run_control_command_error)?;
    if current_source != plan.binding.source_content_id {
        return Err((
            "SOURCE_CHANGED",
            2,
            anyhow::anyhow!(
                "run source changed after planning: expected {}, observed {}; create a new run plan",
                plan.binding.source_content_id,
                current_source
            ),
        ));
    }
    let authorization = classify_run_output(root, Path::new(&plan.binding.output), &plan.intent_id)
        .map_err(run_control_command_error)?;
    if authorization.level == datajig_core::AuthorizationLevel::Fatal {
        return Err((
            "RUN_AUTHORIZATION_REJECTED",
            2,
            anyhow::anyhow!(
                "run output eligibility changed after planning; the target is no longer safe"
            ),
        ));
    }
    Ok(authorization.level)
}

fn run_dsl_command_error(error: RunDslError) -> CommandError {
    let code = match error.code.as_str() {
        "UNSUPPORTED_TASK" => "UNSUPPORTED_TASK",
        "DSL_INTERNAL_ERROR" => "DSL_INTERNAL_ERROR",
        _ => "INVALID_ARGUMENT",
    };
    (code, 2, error.into())
}

fn run_control_command_error(error: RunControlError) -> CommandError {
    let code = match error.code.as_str() {
        "RUN_AUTHORIZATION_REJECTED" => "RUN_AUTHORIZATION_REJECTED",
        "RUN_ATTEMPT_NOT_FOUND" => "RUN_ATTEMPT_NOT_FOUND",
        "RUN_PLAN_MISMATCH" => "RUN_PLAN_MISMATCH",
        "RUN_PLAN_CORRUPT" => "RUN_PLAN_CORRUPT",
        "RUN_PLAN_CONFLICT" => "RUN_PLAN_CONFLICT",
        "RUN_STATE_CONFLICT" => "RUN_STATE_CONFLICT",
        "RUN_STATE_LIMIT_EXCEEDED" => "RUN_STATE_LIMIT_EXCEEDED",
        "SOURCE_LOAD_FAILED" => "SOURCE_LOAD_FAILED",
        "MIXED_FORMAT" => "MIXED_FORMAT",
        "SOURCE_FORMAT_MISMATCH" => "SOURCE_FORMAT_MISMATCH",
        "SOURCE_FORMAT_UNSUPPORTED" => "SOURCE_FORMAT_UNSUPPORTED",
        "RUN_PATH_INVALID" => "RUN_PATH_INVALID",
        "RUN_PLAN_READ_FAILED" => "RUN_PLAN_READ_FAILED",
        "RUN_PLAN_WRITE_FAILED" => "RUN_PLAN_WRITE_FAILED",
        _ => "INVALID_ARGUMENT",
    };
    (code, 2, error.into())
}

fn parse_transform_inputs(values: &[String]) -> Result<Vec<TransformInputSpec>, CommandError> {
    values
        .iter()
        .map(|value| {
            let (alias, path) = value.split_once('=').ok_or_else(|| {
                (
                    "INVALID_ARGUMENT",
                    2,
                    anyhow::anyhow!(
                        "--input must use ALIAS=PATH, for example --input events=data/events.csv"
                    ),
                )
            })?;
            let path = PathBuf::from(path);
            let extension = path
                .extension()
                .and_then(|value| value.to_str())
                .map(str::to_ascii_lowercase);
            let format = match extension.as_deref() {
                Some("csv") => TransformSourceFormat::Csv,
                Some("parquet" | "pq") => TransformSourceFormat::Parquet,
                Some("jsonl" | "ndjson") => TransformSourceFormat::Jsonl,
                _ => {
                    return Err((
                        "INVALID_ARGUMENT",
                        2,
                        anyhow::anyhow!(
                            "transform input paths must end in .csv, .parquet, .pq, .jsonl, or .ndjson"
                        ),
                    ));
                }
            };
            TransformInputSpec::new(alias.into(), path, format)
                .map_err(|error| ("INVALID_ARGUMENT", 2, error))
        })
        .collect()
}

fn load_transform_parameters(
    inline: Option<&str>,
    path: Option<&std::path::Path>,
) -> Result<Vec<Value>, CommandError> {
    let payload = match (inline, path) {
        (Some(value), None) => value.as_bytes().to_vec(),
        (None, Some(path)) => std::fs::read(path)
            .map_err(|error| anyhow::anyhow!("cannot read transform parameter file: {error}"))
            .map_err(|error| ("INVALID_ARGUMENT", 2, error))?,
        (None, None) => return Ok(Vec::new()),
        (Some(_), Some(_)) => {
            return Err((
                "INVALID_ARGUMENT",
                2,
                anyhow::anyhow!("--params and --params-file cannot be used together"),
            ));
        }
    };
    if payload.len() > 65_536 {
        return Err((
            "INVALID_ARGUMENT",
            2,
            anyhow::anyhow!("transform parameter JSON exceeds 65536 bytes"),
        ));
    }
    let parameters: Vec<Value> = serde_json::from_slice(&payload).map_err(|error| {
        (
            "INVALID_ARGUMENT",
            2,
            anyhow::anyhow!("transform parameters must be a JSON array: {error}"),
        )
    })?;
    if parameters.len() > 256
        || parameters
            .iter()
            .any(|value| value.is_array() || value.is_object())
    {
        return Err((
            "INVALID_ARGUMENT",
            2,
            anyhow::anyhow!("transform parameters must contain at most 256 JSON scalars"),
        ));
    }
    Ok(parameters)
}

fn load_transform_sql(inline: Option<String>, path: Option<&Path>) -> Result<String, CommandError> {
    let sql = match (inline, path) {
        (Some(sql), None) => sql,
        (None, Some(path)) => {
            let mut options = std::fs::OpenOptions::new();
            options.read(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
            }
            let mut file = options.open(path).map_err(|error| {
                (
                    "INVALID_ARGUMENT",
                    2,
                    anyhow::anyhow!("cannot open SQL file {}: {error}", path.display()),
                )
            })?;
            if !file
                .metadata()
                .map_err(anyhow::Error::from)
                .map_err(|error| ("INVALID_ARGUMENT", 2, error))?
                .is_file()
            {
                return Err((
                    "INVALID_ARGUMENT",
                    2,
                    anyhow::anyhow!("SQL file must be a regular file: {}", path.display()),
                ));
            }
            let mut bytes = Vec::new();
            file.by_ref()
                .take((datajig_core::MAX_TRANSFORM_SQL_BYTES + 1) as u64)
                .read_to_end(&mut bytes)
                .map_err(|error| {
                    (
                        "INVALID_ARGUMENT",
                        2,
                        anyhow::anyhow!("cannot read SQL file {}: {error}", path.display()),
                    )
                })?;
            String::from_utf8(bytes).map_err(|_| {
                (
                    "INVALID_ARGUMENT",
                    2,
                    anyhow::anyhow!("SQL file must contain UTF-8 text: {}", path.display()),
                )
            })?
        }
        _ => {
            return Err((
                "INVALID_ARGUMENT",
                2,
                anyhow::anyhow!(
                    "provide exactly one of --sql '<SELECT ...>' or --sql-file query.sql"
                ),
            ));
        }
    };
    if sql.trim().is_empty() {
        return Err((
            "INVALID_ARGUMENT",
            2,
            anyhow::anyhow!("SQL must not be empty; example: --sql 'SELECT * FROM source'"),
        ));
    }
    if sql.len() > datajig_core::MAX_TRANSFORM_SQL_BYTES {
        return Err((
            "INVALID_ARGUMENT",
            2,
            anyhow::anyhow!(
                "SQL exceeds the {} byte limit",
                datajig_core::MAX_TRANSFORM_SQL_BYTES
            ),
        ));
    }
    Ok(sql)
}

fn transform_provider_python() -> Result<PathBuf, CommandError> {
    std::env::var_os("_DATAJIG_PROVIDER_PYTHON")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| {
            (
                "PROVIDER_UNAVAILABLE",
                2,
                anyhow::anyhow!(
                    "the DuckDB provider interpreter is not bound; install with pip install 'datajig[duckdb]' and invoke the datajig Python entrypoint"
                ),
            )
        })
}

fn transform_command_error(error: anyhow::Error) -> CommandError {
    let code = if let Some(provider) = error.downcast_ref::<TransformProviderExecutionError>() {
        match provider.code() {
            "PROVIDER_UNAVAILABLE" => "PROVIDER_UNAVAILABLE",
            "PROVIDER_INCOMPATIBLE" | "PROVIDER_DRIFT" => "PROVIDER_INCOMPATIBLE",
            "SOURCE_DRIFT" => "TRANSFORM_DRIFT",
            "OUTPUT_LIMIT_EXCEEDED" => "OUTPUT_LIMIT_EXCEEDED",
            "SOURCE_LOAD_FAILED" => "SOURCE_LOAD_FAILED",
            "QUERY_EXECUTION_FAILED" => "TRANSFORM_QUERY_FAILED",
            "OUTPUT_SCHEMA_INVALID" => "OUTPUT_SCHEMA_INVALID",
            "INVALID_REQUEST" | "PROTOCOL_MISMATCH" => "INVALID_ARGUMENT",
            _ => "PROVIDER_EXECUTION_FAILED",
        }
    } else if error
        .downcast_ref::<TransformNotAuthorizedError>()
        .is_some()
    {
        "TRANSFORM_NOT_AUTHORIZED"
    } else if error.downcast_ref::<TransformDriftError>().is_some() {
        "TRANSFORM_DRIFT"
    } else if error
        .downcast_ref::<TransformProviderTimeoutError>()
        .is_some()
    {
        "PROVIDER_TIMEOUT"
    } else if error
        .downcast_ref::<TransformProviderProtocolError>()
        .is_some()
    {
        "PROVIDER_PROTOCOL_ERROR"
    } else if error
        .downcast_ref::<datajig_core::TransformProviderUnavailableError>()
        .is_some()
    {
        "PROVIDER_UNAVAILABLE"
    } else if error.downcast_ref::<OutputExistsError>().is_some() {
        "OUTPUT_EXISTS"
    } else if let Some(output) = error.downcast_ref::<TransformOutputError>() {
        match output.kind() {
            TransformOutputErrorKind::OutputLimit => "OUTPUT_LIMIT_EXCEEDED",
            TransformOutputErrorKind::Schema => "OUTPUT_SCHEMA_INVALID",
            _ => "TRANSFORM_OUTPUT_INVALID",
        }
    } else if error
        .downcast_ref::<InvalidArgumentError>()
        .and_then(InvalidArgumentError::limit_details)
        .is_some_and(|details| details.metric().starts_with("source_"))
    {
        "SOURCE_LIMIT_EXCEEDED"
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        "INVALID_ARGUMENT"
    } else {
        "TRANSFORM_FAILED"
    };
    (code, 2, error)
}

fn lock_command_workspace(command: &Command) -> Result<Option<WorkspaceLock>, CommandError> {
    let selection = match command {
        Command::Export { state, .. }
        | Command::ViewCheck { state, .. }
        | Command::Log { state, .. }
        | Command::Materialize { state, .. }
        | Command::Locate { state, .. }
        | Command::PatchDraft { state, .. }
        | Command::PatchPreview { state, .. }
        | Command::Status { state, .. } => Some((state, false)),
        Command::Check { state, .. }
        | Command::ChangesetBegin { state, .. }
        | Command::ChangesetStage { state, .. }
        | Command::ReviewPlan { state, .. }
        | Command::PatchApply { state, .. }
        | Command::PatchUndo { state, .. }
        | Command::Seal { state, .. } => Some((state, true)),
        _ => None,
    };
    let Some((state, exclusive)) = selection else {
        return Ok(None);
    };
    let store = WorkspaceStore::open(state).map_err(|error| {
        (
            if error.downcast_ref::<WorkspaceBusyError>().is_some() {
                "WORKSPACE_BUSY"
            } else {
                "WORKSPACE_ERROR"
            },
            2,
            error,
        )
    })?;
    let lock = if exclusive {
        store.lock_exclusive()
    } else {
        store.lock_shared()
    }
    .map_err(|error| {
        (
            if error.downcast_ref::<WorkspaceBusyError>().is_some() {
                "WORKSPACE_BUSY"
            } else {
                "WORKSPACE_ERROR"
            },
            2,
            error,
        )
    })?;
    Ok(Some(lock))
}

fn snapshot_error_code(error: &anyhow::Error) -> &'static str {
    if error
        .downcast_ref::<ConcurrentModificationError>()
        .is_some()
    {
        "STALE_INPUT"
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        "INVALID_ARGUMENT"
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        "REPORT_IO_ERROR"
    } else {
        "SNAPSHOT_ERROR"
    }
}

fn inventory_error_code(error: &anyhow::Error) -> &'static str {
    if error
        .downcast_ref::<ConcurrentModificationError>()
        .is_some()
    {
        "STALE_INPUT"
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        "INVALID_ARGUMENT"
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        "INVENTORY_IO_ERROR"
    } else {
        "INVENTORY_ERROR"
    }
}

fn inspect_error_code(error: &anyhow::Error) -> &'static str {
    if error
        .downcast_ref::<ConcurrentModificationError>()
        .is_some()
    {
        "STALE_INPUT"
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        "INVALID_ARGUMENT"
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        "INSPECTION_IO_ERROR"
    } else {
        "INSPECTION_ERROR"
    }
}

fn review_error_code(error: &anyhow::Error) -> &'static str {
    if error
        .downcast_ref::<ConcurrentModificationError>()
        .is_some()
    {
        "STALE_INPUT"
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        "INVALID_ARGUMENT"
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        "REVIEW_IO_ERROR"
    } else {
        "REVIEW_ERROR"
    }
}

fn workspace_error_code(error: &anyhow::Error) -> &'static str {
    if error.downcast_ref::<UnstagedChangesError>().is_some() {
        "UNSTAGED_CHANGES"
    } else if error
        .downcast_ref::<ConcurrentModificationError>()
        .is_some()
    {
        "STALE_INPUT"
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        "INVALID_ARGUMENT"
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        "WORKSPACE_IO_ERROR"
    } else {
        "WORKSPACE_ERROR"
    }
}

fn materialize_command_error(error: anyhow::Error) -> CommandError {
    if error.downcast_ref::<OutputExistsError>().is_some() {
        ("OUTPUT_EXISTS", 2, error)
    } else if error
        .downcast_ref::<RevisionContentUnavailableError>()
        .is_some()
    {
        ("REVISION_CONTENT_UNAVAILABLE", 4, error)
    } else if error
        .downcast_ref::<RevisionContentCorruptError>()
        .is_some()
    {
        ("REVISION_CONTENT_CORRUPT", 2, error)
    } else {
        (workspace_error_code(&error), 2, error)
    }
}

fn locate_command_error(error: anyhow::Error) -> CommandError {
    if error.downcast_ref::<FindingNotFoundError>().is_some() {
        ("FINDING_NOT_FOUND", 4, error)
    } else {
        (workspace_error_code(&error), 2, error)
    }
}

fn patch_preview_command_error(error: anyhow::Error) -> CommandError {
    if error.downcast_ref::<FindingNotFoundError>().is_some() {
        ("FINDING_NOT_FOUND", 4, error)
    } else if error.downcast_ref::<UnstagedChangesError>().is_some() {
        ("UNSTAGED_CHANGES", 2, error)
    } else if error
        .downcast_ref::<ConcurrentModificationError>()
        .is_some()
    {
        ("STALE_INPUT", 2, error)
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        ("INVALID_ARGUMENT", 2, error)
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        ("PATCH_IO_ERROR", 2, error)
    } else {
        ("PATCH_ERROR", 2, error)
    }
}

fn repository_install_command_error(error: anyhow::Error) -> CommandError {
    if error.downcast_ref::<RepositoryConflictError>().is_some() {
        ("REPOSITORY_CONFLICT", 2, error)
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        ("REPOSITORY_IO_ERROR", 2, error)
    } else {
        ("INVALID_REPOSITORY", 2, error)
    }
}

fn patch_transaction_command_error(error: anyhow::Error) -> CommandError {
    if error.downcast_ref::<PatchNotAuthorizedError>().is_some() {
        ("PATCH_NOT_AUTHORIZED", 2, error)
    } else if error.downcast_ref::<UndoNotFoundError>().is_some() {
        ("UNDO_NOT_FOUND", 4, error)
    } else if error.downcast_ref::<PatchConflictError>().is_some() {
        ("PATCH_CONFLICT", 2, error)
    } else if error.downcast_ref::<WorkspaceBusyError>().is_some() {
        ("WORKSPACE_BUSY", 2, error)
    } else if error.downcast_ref::<FindingNotFoundError>().is_some() {
        ("FINDING_NOT_FOUND", 4, error)
    } else if error.downcast_ref::<UnstagedChangesError>().is_some() {
        ("UNSTAGED_CHANGES", 2, error)
    } else if error
        .downcast_ref::<ConcurrentModificationError>()
        .is_some()
    {
        ("SOURCE_CHANGED", 2, error)
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        ("INVALID_ARGUMENT", 2, error)
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        ("PATCH_IO_ERROR", 2, error)
    } else {
        ("PATCH_ERROR", 2, error)
    }
}

fn hf_import_command_error(error: anyhow::Error) -> CommandError {
    if error.downcast_ref::<HfImportNotAuthorizedError>().is_some() {
        ("HF_IMPORT_NOT_AUTHORIZED", 2, error)
    } else if error.downcast_ref::<OutputExistsError>().is_some() {
        ("HF_IMPORT_CONFLICT", 2, error)
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        ("INVALID_ARGUMENT", 2, error)
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        ("HF_IMPORT_IO_ERROR", 2, error)
    } else {
        ("HF_IMPORT_ERROR", 2, error)
    }
}

fn prepare_command_error(error: anyhow::Error) -> CommandError {
    if error.downcast_ref::<PrepareNotAuthorizedError>().is_some() {
        ("PREPARE_NOT_AUTHORIZED", 2, error)
    } else if error.downcast_ref::<StalePrepareInputError>().is_some() {
        ("STALE_PREPARE_INPUT", 2, error)
    } else if error.downcast_ref::<PrepareInvalidDataError>().is_some() {
        ("PREPARE_INVALID_DATA", 2, error)
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        ("INVALID_ARGUMENT", 2, error)
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        ("PREPARE_IO_ERROR", 2, error)
    } else {
        ("PREPARE_ERROR", 2, error)
    }
}

fn training_consumption_command_error(error: anyhow::Error) -> CommandError {
    if error
        .downcast_ref::<ConsumptionNotAuthorizedError>()
        .is_some()
    {
        ("CONSUMPTION_NOT_AUTHORIZED", 2, error)
    } else if error.downcast_ref::<StaleConsumptionInputError>().is_some() {
        ("STALE_CONSUMPTION_INPUT", 2, error)
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        ("INVALID_ARGUMENT", 2, error)
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        ("CONSUMPTION_IO_ERROR", 2, error)
    } else {
        ("INVALID_CONSUMPTION_PLAN", 2, error)
    }
}

fn training_export_error_code(error: &anyhow::Error) -> &'static str {
    if error.downcast_ref::<EmptyViewError>().is_some() {
        "VIEW_EMPTY"
    } else if error.downcast_ref::<InvalidRecipeError>().is_some() {
        "INVALID_RECIPE"
    } else if error.downcast_ref::<UnstagedChangesError>().is_some() {
        "UNSTAGED_CHANGES"
    } else if error
        .downcast_ref::<ConcurrentModificationError>()
        .is_some()
    {
        "SOURCE_CHANGED"
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        "INVALID_ARGUMENT"
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        "EXPORT_IO_ERROR"
    } else {
        "EXPORT_ERROR"
    }
}

fn training_export_command_error(error: anyhow::Error) -> CommandError {
    if error
        .downcast_ref::<RevisionContentUnavailableError>()
        .is_some()
    {
        ("REVISION_CONTENT_UNAVAILABLE", 4, error)
    } else if error
        .downcast_ref::<RevisionContentCorruptError>()
        .is_some()
    {
        ("REVISION_CONTENT_CORRUPT", 2, error)
    } else {
        (training_export_error_code(&error), 2, error)
    }
}

fn subset_view_error_code(error: &anyhow::Error) -> &'static str {
    if error.downcast_ref::<InvalidRecipeError>().is_some() {
        "INVALID_RECIPE"
    } else if error.downcast_ref::<UnstagedChangesError>().is_some() {
        "UNSTAGED_CHANGES"
    } else if error
        .downcast_ref::<ConcurrentModificationError>()
        .is_some()
    {
        "SOURCE_CHANGED"
    } else if error.downcast_ref::<InvalidArgumentError>().is_some() {
        "INVALID_ARGUMENT"
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        "VIEW_IO_ERROR"
    } else {
        "VIEW_ERROR"
    }
}

fn training_bundle_error_code(error: &anyhow::Error) -> &'static str {
    if error.downcast_ref::<InvalidArgumentError>().is_some() {
        "INVALID_ARGUMENT"
    } else if error.downcast_ref::<std::io::Error>().is_some() {
        "BUNDLE_IO_ERROR"
    } else {
        "INVALID_BUNDLE"
    }
}

fn manifest_error_code(error: &anyhow::Error) -> &'static str {
    if error.downcast_ref::<std::io::Error>().is_some() {
        "REPORT_IO_ERROR"
    } else {
        "INVALID_MANIFEST"
    }
}

fn report_error_code(error: &anyhow::Error) -> &'static str {
    if error.downcast_ref::<std::io::Error>().is_some() {
        "REPORT_IO_ERROR"
    } else {
        "INVALID_REPORT"
    }
}

fn report_command_error(error: anyhow::Error) -> CommandError {
    let code = report_error_code(&error);
    let message = if code == "REPORT_IO_ERROR" {
        "cannot read report"
    } else {
        "report does not match schema version 1"
    };
    (code, 2, anyhow::anyhow!(message))
}

fn exit_with_error(
    code: &str,
    message: &str,
    status: i32,
    command: Option<&str>,
    source: Option<&anyhow::Error>,
) -> ! {
    const MAX_ERROR_BYTES: usize = 1_024;
    let truncated = message.len() > MAX_ERROR_BYTES;
    let limit = if truncated {
        MAX_ERROR_BYTES - '…'.len_utf8()
    } else {
        MAX_ERROR_BYTES
    };
    let mut end = message.len().min(limit);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    let mut bounded = message[..end].to_owned();
    if truncated {
        bounded.push('…');
    }
    let mut error = json!({
        "code": code,
        "message": bounded,
        "retryable": matches!(
            code,
            "STALE_INPUT" | "SOURCE_CHANGED" | "UNSTAGED_CHANGES" | "WORKSPACE_BUSY"
        )
    });
    let mut next_actions = Vec::<Value>::new();
    if let Some(run_error) = source.and_then(|error| error.downcast_ref::<RunDslError>()) {
        error["message"] = json!(run_error.message);
        error["remediation"] = json!({"summary": run_error.remediation});
        error["details"] = json!({
            "location": run_error.location,
            "position": run_error.position,
            "suggestions": run_error.suggestions
        });
        next_actions.push(json!({"command": "run", "args": ["--help"]}));
    } else if let Some(run_error) = source.and_then(|error| error.downcast_ref::<RunControlError>())
    {
        error["message"] = json!(run_error.message);
        error["remediation"] = json!({"summary": run_error.remediation});
        next_actions.push(json!({"command": "run", "args": ["--help"]}));
    } else if let Some(provider) =
        source.and_then(|error| error.downcast_ref::<TransformProviderExecutionError>())
    {
        error["message"] = json!(provider.message());
        error["remediation"] = json!({"summary": provider.remediation()});
        if let Some(command) = command.filter(|value| !value.starts_with('-')) {
            next_actions.push(json!({"command": command, "args": ["--help"]}));
        }
    } else if let Some(remediation) = source
        .and_then(|error| error.downcast_ref::<InvalidArgumentError>())
        .and_then(InvalidArgumentError::remediation)
    {
        error["remediation"] = json!({"summary": remediation.summary()});
        next_actions.push(json!({
            "command": remediation.command(),
            "args": remediation.args()
        }));
    } else if code == "INVALID_ARGUMENT" {
        if let Some(command) = command.filter(|value| !value.starts_with('-')) {
            error["remediation"] = json!({
                "summary": format!("Inspect the {command} argument contract and retry.")
            });
            next_actions.push(json!({"command": command, "args": ["--help"]}));
        }
    }
    let limit_details = source.and_then(|source| {
        source
            .downcast_ref::<InvalidArgumentError>()
            .and_then(InvalidArgumentError::limit_details)
            .cloned()
            .or_else(|| {
                source
                    .downcast_ref::<TransformOutputError>()
                    .and_then(TransformOutputError::limit_details)
                    .cloned()
            })
            .or_else(|| {
                source
                    .downcast_ref::<TransformProviderExecutionError>()
                    .and_then(TransformProviderExecutionError::limit_details)
                    .cloned()
            })
            .or_else(|| {
                source
                    .downcast_ref::<TransformProviderTimeoutError>()
                    .map(TransformProviderTimeoutError::limit_details)
            })
    });
    if let Some(details) = limit_details {
        error["details"] = json!({
            "metric": details.metric(),
            "observed": details.observed(),
            "observed_is_lower_bound": details.observed_is_lower_bound(),
            "limit": details.limit(),
            "unit": details.unit(),
        });
    }
    eprintln!(
        "{}",
        json!({
            "agent_api_version": AGENT_API_VERSION,
            "error": error,
            "next_actions": next_actions
        })
    );
    std::process::exit(status);
}
