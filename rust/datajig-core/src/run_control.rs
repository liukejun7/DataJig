use crate::identity::blake3_content_id;
use crate::io::save_new_file_atomically;
use crate::strict_json::reject_duplicate_json_members;
use crate::{CompiledRunTask, RunTaskAst, SourceFormat, canonicalize_run_task};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use walkdir::WalkDir;

pub const RUN_PLAN_SCHEMA_VERSION: u8 = 1;
const MAX_RUN_IDENTITY_VALUE_BYTES: usize = 4096;
const MAX_ATTEMPT_NONCE_BYTES: usize = 256;
const MAX_RUN_PLAN_BYTES: usize = 1024 * 1024;
const MAX_RESUME_INTENTS: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunConsumptionBinding {
    pub consumer: String,
    pub run_id: String,
    pub run_dir: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunPlanBinding {
    pub source_content_id: String,
    pub engine_version: String,
    pub output: String,
    pub consumption: Option<RunConsumptionBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunIdentities {
    pub intent_id: String,
    pub plan_id: String,
    pub attempt_id: String,
    pub consumption_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationLevel {
    Safe,
    Dangerous,
    Fatal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationStatus {
    Authorized,
    ConfirmationRequired,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunAuthorizationDecision {
    pub level: AuthorizationLevel,
    pub status: AuthorizationStatus,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunPlanArtifact {
    pub namespace: String,
    pub kind: String,
    pub schema_version: u8,
    pub intent_id: String,
    pub plan_id: String,
    pub attempt_id: String,
    pub consumption_id: Option<String>,
    pub canonical_ast: RunTaskAst,
    pub binding: RunPlanBinding,
    pub attempt_nonce: String,
    pub authorization: RunAuthorizationDecision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunLayout {
    pub workspace: PathBuf,
    pub attempt: PathBuf,
    pub plan: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RunControlError {
    pub code: String,
    pub message: String,
    pub remediation: String,
}

impl RunControlError {
    fn invalid(field: &str, message: impl Into<String>) -> Self {
        Self {
            code: "INVALID_RUN_PLAN".into(),
            message: format!("{field}: {}", message.into()),
            remediation: format!("provide a valid bounded {field}"),
        }
    }

    fn new(code: &str, message: impl Into<String>, remediation: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            remediation: remediation.into(),
        }
    }
}

impl fmt::Display for RunControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for RunControlError {}

#[derive(Serialize)]
struct ConsumptionIdentity<'a> {
    schema_version: u8,
    consumer: &'a str,
    run_id: &'a str,
    run_dir: &'a str,
}

#[derive(Serialize)]
struct PlanIdentity<'a> {
    schema_version: u8,
    intent_id: &'a str,
    source_content_id: &'a str,
    engine_version: &'a str,
    output: &'a str,
    consumption_id: Option<&'a str>,
}

#[derive(Serialize)]
struct AttemptIdentity<'a> {
    schema_version: u8,
    plan_id: &'a str,
    nonce: &'a str,
}

pub fn derive_run_identities(
    compiled: &CompiledRunTask,
    binding: &RunPlanBinding,
    attempt_nonce: &str,
) -> Result<RunIdentities, RunControlError> {
    validate_value("source_content_id", &binding.source_content_id)?;
    validate_value("engine_version", &binding.engine_version)?;
    validate_value("output", &binding.output)?;
    validate_attempt_nonce(attempt_nonce)?;

    let consumption_id = binding
        .consumption
        .as_ref()
        .map(derive_consumption_id)
        .transpose()?;
    let plan_payload = serde_json::to_vec(&PlanIdentity {
        schema_version: RUN_PLAN_SCHEMA_VERSION,
        intent_id: &compiled.intent_id,
        source_content_id: &binding.source_content_id,
        engine_version: &binding.engine_version,
        output: &binding.output,
        consumption_id: consumption_id.as_deref(),
    })
    .map_err(|error| RunControlError::invalid("plan", error.to_string()))?;
    let plan_id = blake3_content_id("plan", b"datajig-run-plan-v1\0", &plan_payload);
    let attempt_payload = serde_json::to_vec(&AttemptIdentity {
        schema_version: RUN_PLAN_SCHEMA_VERSION,
        plan_id: &plan_id,
        nonce: attempt_nonce,
    })
    .map_err(|error| RunControlError::invalid("attempt", error.to_string()))?;
    let attempt_id = blake3_content_id("attempt", b"datajig-run-attempt-v1\0", &attempt_payload);
    Ok(RunIdentities {
        intent_id: compiled.intent_id.clone(),
        plan_id,
        attempt_id,
        consumption_id,
    })
}

pub fn authorize_run(level: AuthorizationLevel, yes: bool) -> RunAuthorizationDecision {
    let status = match (level, yes) {
        (AuthorizationLevel::Safe, _) | (AuthorizationLevel::Dangerous, true) => {
            AuthorizationStatus::Authorized
        }
        (AuthorizationLevel::Dangerous, false) => AuthorizationStatus::ConfirmationRequired,
        (AuthorizationLevel::Fatal, _) => AuthorizationStatus::Rejected,
    };
    RunAuthorizationDecision { level, status }
}

pub fn create_run_plan(
    compiled: &CompiledRunTask,
    binding: &RunPlanBinding,
    attempt_nonce: &str,
    level: AuthorizationLevel,
    yes: bool,
) -> Result<RunPlanArtifact, RunControlError> {
    let identities = derive_run_identities(compiled, binding, attempt_nonce)?;
    Ok(RunPlanArtifact {
        namespace: "datajig".into(),
        kind: "run_plan".into(),
        schema_version: RUN_PLAN_SCHEMA_VERSION,
        intent_id: identities.intent_id,
        plan_id: identities.plan_id,
        attempt_id: identities.attempt_id,
        consumption_id: identities.consumption_id,
        canonical_ast: compiled.ast.clone(),
        binding: binding.clone(),
        attempt_nonce: attempt_nonce.into(),
        authorization: authorize_run(level, yes),
    })
}

impl RunPlanArtifact {
    fn verify(&self) -> Result<(), RunControlError> {
        if self.namespace != "datajig"
            || self.kind != "run_plan"
            || self.schema_version != RUN_PLAN_SCHEMA_VERSION
        {
            return Err(RunControlError::new(
                "RUN_PLAN_CORRUPT",
                "run plan has an unsupported contract identity",
                "create a new run plan with this DataJig version",
            ));
        }
        let compiled = canonicalize_run_task(self.canonical_ast.clone()).map_err(|error| {
            RunControlError::new(
                "RUN_PLAN_CORRUPT",
                format!("run plan canonical AST is invalid: {error}"),
                "create a new run plan from the original task",
            )
        })?;
        let expected = derive_run_identities(&compiled, &self.binding, &self.attempt_nonce)
            .map_err(|error| {
                RunControlError::new(
                    "RUN_PLAN_CORRUPT",
                    format!("run plan identity inputs are invalid: {error}"),
                    "create a new run plan from the original task",
                )
            })?;
        if self.intent_id != expected.intent_id
            || self.plan_id != expected.plan_id
            || self.attempt_id != expected.attempt_id
            || self.consumption_id != expected.consumption_id
        {
            return Err(RunControlError::new(
                "RUN_PLAN_CORRUPT",
                "run plan identity does not match its canonical contents",
                "discard the modified plan and create a new run attempt",
            ));
        }
        if self.authorization
            != authorize_run(
                self.authorization.level,
                self.authorization.status == AuthorizationStatus::Authorized
                    && self.authorization.level == AuthorizationLevel::Dangerous,
            )
        {
            return Err(RunControlError::new(
                "RUN_PLAN_CORRUPT",
                "run plan authorization state is inconsistent",
                "create a new run plan and authorize it explicitly",
            ));
        }
        Ok(())
    }
}

pub fn persist_run_plan(root: &Path, plan: &RunPlanArtifact) -> Result<RunLayout, RunControlError> {
    plan.verify()?;
    if plan.authorization.status == AuthorizationStatus::Rejected {
        return Err(RunControlError::new(
            "RUN_AUTHORIZATION_REJECTED",
            "fatal run preflight failed; no run state was created",
            "choose an empty output or the DataJig delivery owned by this intent",
        ));
    }
    let root = canonical_directory(root, "run root")?;
    let intent_root = root.join(".datajig/runs").join(&plan.intent_id);
    let workspace = intent_root.join("workspace");
    let attempt = intent_root.join("attempts").join(&plan.attempt_id);
    ensure_direct_directory_tree(&root, &workspace)?;
    ensure_direct_directory_tree(&root, &attempt)?;
    let plan_path = attempt.join("plan.json");
    let payload = serde_json::to_vec_pretty(plan).map_err(|error| {
        RunControlError::new(
            "RUN_PLAN_WRITE_FAILED",
            format!("cannot serialize run plan: {error}"),
            "retry with valid bounded run inputs",
        )
    })?;
    if payload.len() > MAX_RUN_PLAN_BYTES {
        return Err(RunControlError::new(
            "RUN_PLAN_WRITE_FAILED",
            format!(
                "run plan is {} bytes > limit {MAX_RUN_PLAN_BYTES}",
                payload.len()
            ),
            "reduce the task or parameter size",
        ));
    }
    publish_immutable_plan(&plan_path, &payload)?;
    Ok(RunLayout {
        workspace,
        attempt,
        plan: plan_path,
    })
}

pub fn load_run_plan_for_resume(
    root: &Path,
    attempt_id: &str,
    accepted_plan_id: &str,
) -> Result<RunPlanArtifact, RunControlError> {
    validate_prefixed_id(attempt_id, "attempt", "attempt_id")?;
    validate_prefixed_id(accepted_plan_id, "plan", "accept_plan")?;
    let root = canonical_directory(root, "run root")?;
    let runs = root.join(".datajig/runs");
    reject_symlink_components(&root, Path::new(".datajig/runs"), "run state")?;
    let entries = fs::read_dir(&runs).map_err(|error| {
        RunControlError::new(
            "RUN_ATTEMPT_NOT_FOUND",
            format!("cannot find persisted run attempts: {error}"),
            "pass an attempt_id returned by a prior strict run",
        )
    })?;
    let mut matches = Vec::new();
    for (index, entry) in entries.enumerate() {
        if index >= MAX_RESUME_INTENTS {
            return Err(RunControlError::new(
                "RUN_STATE_LIMIT_EXCEEDED",
                format!("run state contains more than {MAX_RESUME_INTENTS} intents"),
                "garbage collect old run attempts before resuming",
            ));
        }
        let entry = entry.map_err(|error| io_error("RUN_PLAN_READ_FAILED", error))?;
        let metadata = fs::symlink_metadata(entry.path())
            .map_err(|error| io_error("RUN_PLAN_READ_FAILED", error))?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            continue;
        }
        let candidate = entry
            .path()
            .join("attempts")
            .join(attempt_id)
            .join("plan.json");
        if candidate.exists() {
            matches.push(candidate);
        }
    }
    if matches.len() != 1 {
        return Err(RunControlError::new(
            "RUN_ATTEMPT_NOT_FOUND",
            format!("found {} persisted plans for {attempt_id}", matches.len()),
            "pass one exact attempt_id returned by a prior strict run",
        ));
    }
    let relative_plan = matches[0].strip_prefix(&root).map_err(|error| {
        RunControlError::new(
            "RUN_PLAN_CORRUPT",
            format!("persisted plan escaped the run root: {error}"),
            "discard the invalid run state and create a new attempt",
        )
    })?;
    reject_symlink_components(&root, relative_plan, "run plan")?;
    let plan = read_plan(&matches[0])?;
    if plan.attempt_id != attempt_id || plan.plan_id != accepted_plan_id {
        return Err(RunControlError::new(
            "RUN_PLAN_MISMATCH",
            format!(
                "attempt {} is bound to plan {}; accepted {}",
                plan.attempt_id, plan.plan_id, accepted_plan_id
            ),
            format!(
                "resume with --attempt {} --accept-plan {}",
                plan.attempt_id, plan.plan_id
            ),
        ));
    }
    Ok(plan)
}

pub fn fingerprint_run_source(root: &Path, relative: &Path) -> Result<String, RunControlError> {
    validate_relative_path(relative, "source")?;
    let root = canonical_directory(root, "source root")?;
    reject_symlink_components(&root, relative, "source")?;
    let source = root.join(relative);
    let metadata = fs::symlink_metadata(&source).map_err(|error| {
        RunControlError::new(
            "SOURCE_LOAD_FAILED",
            format!("cannot inspect source {}: {error}", relative.display()),
            "provide an existing direct file or directory",
        )
    })?;
    if metadata.file_type().is_symlink() || !(metadata.is_file() || metadata.is_dir()) {
        return Err(RunControlError::new(
            "SOURCE_LOAD_FAILED",
            "source must be a direct regular file or directory",
            "replace symbolic links and special files with direct source files",
        ));
    }
    let mut files = if metadata.is_file() {
        vec![(PathBuf::from("."), source)]
    } else {
        let mut values = Vec::new();
        for entry in WalkDir::new(&source).follow_links(false) {
            let entry = entry.map_err(|error| {
                RunControlError::new(
                    "SOURCE_LOAD_FAILED",
                    format!("cannot enumerate source: {error}"),
                    "ensure every source member is readable and is not a symbolic link",
                )
            })?;
            let entry_metadata = fs::symlink_metadata(entry.path())
                .map_err(|error| io_error("SOURCE_LOAD_FAILED", error))?;
            if entry_metadata.file_type().is_symlink() {
                return Err(RunControlError::new(
                    "SOURCE_LOAD_FAILED",
                    format!(
                        "source member {} is a symbolic link",
                        entry.path().display()
                    ),
                    "replace symbolic links with direct source files",
                ));
            }
            if entry_metadata.is_file() {
                let member = entry
                    .path()
                    .strip_prefix(&source)
                    .map_err(|error| RunControlError::invalid("source", error.to_string()))?
                    .to_path_buf();
                values.push((member, entry.path().to_path_buf()));
            } else if !entry_metadata.is_dir() {
                return Err(RunControlError::new(
                    "SOURCE_LOAD_FAILED",
                    format!(
                        "source member {} is not a regular file",
                        entry.path().display()
                    ),
                    "remove sockets, devices, and other special files from the source",
                ));
            }
        }
        values
    };
    files.sort_by(|left, right| left.0.cmp(&right.0));
    if files.is_empty() {
        return Err(RunControlError::new(
            "SOURCE_LOAD_FAILED",
            "source directory contains no files",
            "add at least one direct source file",
        ));
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-run-source-v1\0");
    for (member, path) in files {
        let name = member.to_str().ok_or_else(|| {
            RunControlError::new(
                "SOURCE_LOAD_FAILED",
                format!("source member {} is not valid UTF-8", member.display()),
                "rename source members to valid UTF-8 paths",
            )
        })?;
        hasher.update(&(name.len() as u64).to_le_bytes());
        hasher.update(name.as_bytes());
        let mut file = open_direct_source_file(&path)?;
        let size = file
            .metadata()
            .map_err(|error| io_error("SOURCE_LOAD_FAILED", error))?
            .len();
        hasher.update(&size.to_le_bytes());
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let count = file
                .read(&mut buffer)
                .map_err(|error| io_error("SOURCE_LOAD_FAILED", error))?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
    }
    Ok(format!("source_{}", hasher.finalize().to_hex()))
}

pub fn resolve_run_source_format(
    root: &Path,
    compiled: CompiledRunTask,
) -> Result<CompiledRunTask, RunControlError> {
    let relative = Path::new(&compiled.ast.source.path);
    validate_relative_path(relative, "source")?;
    let root = canonical_directory(root, "source root")?;
    reject_symlink_components(&root, relative, "source")?;
    let source = root.join(relative);
    let metadata = fs::symlink_metadata(&source).map_err(|error| {
        RunControlError::new(
            "SOURCE_LOAD_FAILED",
            format!("cannot inspect source {}: {error}", relative.display()),
            "provide an existing direct file or directory",
        )
    })?;
    if metadata.file_type().is_symlink() || !(metadata.is_file() || metadata.is_dir()) {
        return Err(RunControlError::new(
            "SOURCE_LOAD_FAILED",
            "source must be a direct regular file or directory",
            "replace symbolic links and special files with direct source files",
        ));
    }
    if metadata.is_file() {
        if compiled.ast.source.format.is_some() {
            return Ok(compiled);
        }
        let format = source_format_from_path(&source)?;
        return canonicalize_with_source_format(compiled, format);
    }

    let mut observed = std::collections::BTreeSet::new();
    let mut file_count = 0_usize;
    for entry in WalkDir::new(&source).follow_links(false) {
        let entry = entry.map_err(|error| {
            RunControlError::new(
                "SOURCE_LOAD_FAILED",
                format!("cannot enumerate source: {error}"),
                "ensure every source member is readable and is not a symbolic link",
            )
        })?;
        let entry_metadata = fs::symlink_metadata(entry.path())
            .map_err(|error| io_error("SOURCE_LOAD_FAILED", error))?;
        if entry_metadata.file_type().is_symlink() {
            return Err(RunControlError::new(
                "SOURCE_LOAD_FAILED",
                format!(
                    "source member {} is a symbolic link",
                    entry.path().display()
                ),
                "replace symbolic links with direct source files",
            ));
        }
        if entry_metadata.is_file() {
            file_count += 1;
            observed.insert(source_format_from_path(entry.path())?);
        } else if !entry_metadata.is_dir() {
            return Err(RunControlError::new(
                "SOURCE_LOAD_FAILED",
                format!(
                    "source member {} is not a regular file",
                    entry.path().display()
                ),
                "remove sockets, devices, and other special files from the source",
            ));
        }
    }
    if file_count == 0 {
        return Err(RunControlError::new(
            "SOURCE_LOAD_FAILED",
            "source directory contains no files",
            "add at least one csv, jsonl, or parquet source file",
        ));
    }
    if observed.len() != 1 {
        let formats = observed
            .iter()
            .map(source_format_name)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(RunControlError::new(
            "MIXED_FORMAT",
            format!("source directory contains mixed formats: {formats}"),
            "use one source format per directory or split the run into separate inputs",
        ));
    }
    let inferred = *observed.iter().next().expect("one observed source format");
    if let Some(declared) = compiled.ast.source.format {
        if declared != inferred {
            return Err(RunControlError::new(
                "SOURCE_FORMAT_MISMATCH",
                format!(
                    "source declares {} but directory members are {}",
                    source_format_name(&declared),
                    source_format_name(&inferred)
                ),
                "use a format matching every file in the source directory",
            ));
        }
    }
    canonicalize_with_source_format(compiled, inferred)
}

fn canonicalize_with_source_format(
    compiled: CompiledRunTask,
    format: SourceFormat,
) -> Result<CompiledRunTask, RunControlError> {
    let mut ast = compiled.ast;
    ast.source.format = Some(format);
    canonicalize_run_task(ast).map_err(|error| {
        RunControlError::new(
            "INVALID_RUN_PLAN",
            format!("cannot bind inferred source format: {error}"),
            "provide a source with one supported format",
        )
    })
}

fn source_format_from_path(path: &Path) -> Result<SourceFormat, RunControlError> {
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase);
    match extension.as_deref() {
        Some("csv") => Ok(SourceFormat::Csv),
        Some("jsonl") => Ok(SourceFormat::Jsonl),
        Some("parquet") => Ok(SourceFormat::Parquet),
        _ => Err(RunControlError::new(
            "SOURCE_FORMAT_UNSUPPORTED",
            format!(
                "source member {} must use .csv, .jsonl, or .parquet",
                path.display()
            ),
            "rename or convert the source to csv, jsonl, or parquet",
        )),
    }
}

fn source_format_name(format: &SourceFormat) -> &'static str {
    match format {
        SourceFormat::Csv => "csv",
        SourceFormat::Jsonl => "jsonl",
        SourceFormat::Parquet => "parquet",
    }
}

pub fn classify_run_output(
    root: &Path,
    relative: &Path,
    _intent_id: &str,
) -> Result<RunAuthorizationDecision, RunControlError> {
    if validate_relative_path(relative, "output").is_err() {
        return Ok(authorize_run(AuthorizationLevel::Fatal, false));
    }
    let root = canonical_directory(root, "run root")?;
    if reject_symlink_components(&root, relative, "output").is_err() {
        return Ok(authorize_run(AuthorizationLevel::Fatal, false));
    }
    let output = root.join(relative);
    if !output.exists() {
        return Ok(authorize_run(AuthorizationLevel::Safe, false));
    }
    Ok(authorize_run(AuthorizationLevel::Fatal, false))
}

fn canonical_directory(path: &Path, label: &str) -> Result<PathBuf, RunControlError> {
    let canonical = path.canonicalize().map_err(|error| {
        RunControlError::new(
            "RUN_PATH_INVALID",
            format!("cannot resolve {label}: {error}"),
            format!("provide an existing direct {label} directory"),
        )
    })?;
    let metadata =
        fs::symlink_metadata(path).map_err(|error| io_error("RUN_PATH_INVALID", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RunControlError::new(
            "RUN_PATH_INVALID",
            format!("{label} must be a direct directory"),
            "replace symbolic links with a direct directory",
        ));
    }
    Ok(canonical)
}

fn validate_relative_path(path: &Path, field: &str) -> Result<(), RunControlError> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(RunControlError::new(
            "RUN_PATH_INVALID",
            format!("{field} must be a non-empty relative path without `..`"),
            format!("provide {field} relative to the current working directory"),
        ));
    }
    Ok(())
}

fn reject_symlink_components(
    root: &Path,
    relative: &Path,
    field: &str,
) -> Result<(), RunControlError> {
    let mut current = root.to_path_buf();
    for component in relative.components() {
        if matches!(component, Component::CurDir) {
            continue;
        }
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(RunControlError::new(
                    "RUN_PATH_INVALID",
                    format!(
                        "{field} path contains a symbolic link at {}",
                        current.display()
                    ),
                    "use direct files and directories under the run root",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(io_error("RUN_PATH_INVALID", error)),
        }
    }
    Ok(())
}

fn ensure_direct_directory_tree(root: &Path, destination: &Path) -> Result<(), RunControlError> {
    let relative = destination.strip_prefix(root).map_err(|error| {
        RunControlError::new(
            "RUN_PATH_INVALID",
            format!("run state escaped its root: {error}"),
            "use the default .datajig run state directory",
        )
    })?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(RunControlError::new(
                        "RUN_STATE_CONFLICT",
                        format!(
                            "run state component {} is not a direct directory",
                            current.display()
                        ),
                        "remove or relocate the conflicting run state path",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .map_err(|error| io_error("RUN_PLAN_WRITE_FAILED", error))?;
            }
            Err(error) => return Err(io_error("RUN_PLAN_WRITE_FAILED", error)),
        }
    }
    Ok(())
}

fn publish_immutable_plan(path: &Path, payload: &[u8]) -> Result<(), RunControlError> {
    if path.exists() {
        let existing = fs::read(path).map_err(|error| io_error("RUN_PLAN_READ_FAILED", error))?;
        if existing == payload {
            return Ok(());
        }
        return Err(RunControlError::new(
            "RUN_PLAN_CONFLICT",
            "existing attempt plan has different contents",
            "resume the existing attempt or create a new attempt",
        ));
    }
    save_new_file_atomically(path, payload, "run plan").map_err(|error| {
        RunControlError::new(
            "RUN_PLAN_WRITE_FAILED",
            error.to_string(),
            "verify run state ownership and retry with a new attempt",
        )
    })
}

fn open_direct_source_file(path: &Path) -> Result<fs::File, RunControlError> {
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(path)
    };
    #[cfg(not(unix))]
    let file = fs::File::open(path);
    let file = file.map_err(|error| io_error("SOURCE_LOAD_FAILED", error))?;
    let metadata = file
        .metadata()
        .map_err(|error| io_error("SOURCE_LOAD_FAILED", error))?;
    if !metadata.is_file() {
        return Err(RunControlError::new(
            "SOURCE_LOAD_FAILED",
            format!(
                "source member {} is not a direct regular file",
                path.display()
            ),
            "replace symbolic links and special files with direct source files",
        ));
    }
    Ok(file)
}

fn read_plan(path: &Path) -> Result<RunPlanArtifact, RunControlError> {
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        fs::OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(path)
    };
    #[cfg(not(unix))]
    let file = fs::File::open(path);
    let file = file.map_err(|error| io_error("RUN_PLAN_READ_FAILED", error))?;
    let metadata = file
        .metadata()
        .map_err(|error| io_error("RUN_PLAN_READ_FAILED", error))?;
    if !metadata.is_file() || metadata.len() > MAX_RUN_PLAN_BYTES as u64 {
        return Err(RunControlError::new(
            "RUN_PLAN_CORRUPT",
            "persisted run plan must be a bounded direct regular file",
            "discard the invalid attempt and create a new run plan",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(RunControlError::new(
                "RUN_PLAN_CORRUPT",
                "persisted run plan must not be hard linked",
                "discard the invalid attempt and create a new run plan",
            ));
        }
    }
    let mut payload = Vec::new();
    file.take((MAX_RUN_PLAN_BYTES + 1) as u64)
        .read_to_end(&mut payload)
        .map_err(|error| io_error("RUN_PLAN_READ_FAILED", error))?;
    if payload.len() > MAX_RUN_PLAN_BYTES {
        return Err(RunControlError::new(
            "RUN_PLAN_CORRUPT",
            format!("persisted run plan exceeds {MAX_RUN_PLAN_BYTES} bytes"),
            "discard the invalid attempt and create a new run plan",
        ));
    }
    let text = std::str::from_utf8(&payload).map_err(|error| {
        RunControlError::new(
            "RUN_PLAN_CORRUPT",
            format!("persisted run plan is not valid UTF-8: {error}"),
            "discard the invalid attempt and create a new run plan",
        )
    })?;
    reject_duplicate_json_members(text).map_err(|error| {
        RunControlError::new(
            "RUN_PLAN_CORRUPT",
            format!("persisted run plan JSON is ambiguous: {error}"),
            "discard the invalid attempt and create a new run plan",
        )
    })?;
    let plan: RunPlanArtifact = serde_json::from_slice(&payload).map_err(|error| {
        RunControlError::new(
            "RUN_PLAN_CORRUPT",
            format!("persisted run plan is invalid JSON: {error}"),
            "discard the invalid attempt and create a new run plan",
        )
    })?;
    let original: serde_json::Value = serde_json::from_slice(&payload).map_err(|error| {
        RunControlError::new(
            "RUN_PLAN_CORRUPT",
            format!("persisted run plan is invalid JSON: {error}"),
            "discard the invalid attempt and create a new run plan",
        )
    })?;
    let normalized = serde_json::to_value(&plan).map_err(|error| {
        RunControlError::new(
            "RUN_PLAN_CORRUPT",
            format!("cannot normalize persisted run plan: {error}"),
            "discard the invalid attempt and create a new run plan",
        )
    })?;
    if original != normalized {
        return Err(RunControlError::new(
            "RUN_PLAN_CORRUPT",
            "persisted run plan contains unknown or noncanonical fields",
            "discard the modified attempt and create a new run plan",
        ));
    }
    plan.verify()?;
    Ok(plan)
}

fn validate_prefixed_id(value: &str, prefix: &str, field: &str) -> Result<(), RunControlError> {
    if value.len() != prefix.len() + 1 + 64
        || !value.starts_with(&format!("{prefix}_"))
        || !value[prefix.len() + 1..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(RunControlError::new(
            "RUN_PLAN_MISMATCH",
            format!("{field} must be a canonical {prefix}_ identity"),
            format!("copy the exact {field} from the strict run response"),
        ));
    }
    Ok(())
}

fn io_error(code: &str, error: std::io::Error) -> RunControlError {
    RunControlError::new(
        code,
        error.to_string(),
        "verify path ownership and permissions, then retry",
    )
}

fn derive_consumption_id(consumption: &RunConsumptionBinding) -> Result<String, RunControlError> {
    validate_value("consumer", &consumption.consumer)?;
    if !matches!(
        consumption.consumer.as_str(),
        "python" | "pytorch" | "huggingface"
    ) {
        return Err(RunControlError::invalid(
            "consumer",
            "must be python, pytorch, or huggingface",
        ));
    }
    validate_value("run_id", &consumption.run_id)?;
    validate_value("run_dir", &consumption.run_dir)?;
    let payload = serde_json::to_vec(&ConsumptionIdentity {
        schema_version: RUN_PLAN_SCHEMA_VERSION,
        consumer: &consumption.consumer,
        run_id: &consumption.run_id,
        run_dir: &consumption.run_dir,
    })
    .map_err(|error| RunControlError::invalid("consumption", error.to_string()))?;
    Ok(blake3_content_id(
        "consume",
        b"datajig-run-consumption-v1\0",
        &payload,
    ))
}

fn validate_value(field: &str, value: &str) -> Result<(), RunControlError> {
    if value.is_empty() {
        return Err(RunControlError::invalid(field, "must not be empty"));
    }
    if value.len() > MAX_RUN_IDENTITY_VALUE_BYTES {
        return Err(RunControlError::invalid(
            field,
            format!(
                "is {} bytes > limit {MAX_RUN_IDENTITY_VALUE_BYTES}",
                value.len()
            ),
        ));
    }
    Ok(())
}

fn validate_attempt_nonce(value: &str) -> Result<(), RunControlError> {
    if value.is_empty() || value.len() > MAX_ATTEMPT_NONCE_BYTES {
        return Err(RunControlError::invalid(
            "attempt_nonce",
            format!("must contain 1..={MAX_ATTEMPT_NONCE_BYTES} bytes"),
        ));
    }
    Ok(())
}
