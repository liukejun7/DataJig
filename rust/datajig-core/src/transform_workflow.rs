use crate::identity::blake3_content_id;
use crate::io::save_new_file_atomically;
use crate::{
    InvalidArgumentError, OutputExistsError, ProviderRequest, StagedTransformSource,
    TransformExecutionEvidence, TransformExpectedOutput, TransformInputSpec, TransformLimits,
    TransformPlan, TransformPlanInput, TransformProviderIdentity, TransformReceipt,
    TransformSource, VerifiedTransformOutput, execute_transform_provider, probe_transform_provider,
    stage_transform_sources, validate_transform_query, verify_transform_candidate,
    verify_transform_candidate_read_only,
};
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug)]
pub struct TransformNotAuthorizedError;

impl fmt::Display for TransformNotAuthorizedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("accepted transform plan identity does not match")
    }
}

impl Error for TransformNotAuthorizedError {}

#[derive(Debug)]
pub struct TransformDriftError(String);

impl TransformDriftError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for TransformDriftError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for TransformDriftError {}

#[derive(Debug)]
pub struct TransformProviderUnavailableError;

impl fmt::Display for TransformProviderUnavailableError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(
            "the DuckDB provider is required to execute this plan; install with pip install 'datajig[duckdb]' and invoke the datajig Python entrypoint",
        )
    }
}

impl Error for TransformProviderUnavailableError {}

#[derive(Clone, Debug)]
pub struct TransformPlanRequest {
    pub inputs: Vec<TransformInputSpec>,
    pub sql_path: PathBuf,
    pub parameters: Vec<Value>,
    pub id_field: String,
    pub output_path: PathBuf,
    pub plan_path: PathBuf,
    pub python: PathBuf,
}

#[derive(Clone, Debug, Serialize)]
pub struct TransformPlanArtifact {
    pub plan: PathBuf,
    pub plan_id: String,
    pub output: PathBuf,
    pub output_content_id: String,
    pub schema: Vec<crate::TransformField>,
    pub rows: u64,
    pub bytes: u64,
    pub unique_ids: u64,
    pub provider_id: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct TransformApplyArtifact {
    pub plan_id: String,
    pub receipt: PathBuf,
    pub receipt_id: String,
    pub output: PathBuf,
    pub output_content_id: String,
    pub id_field: String,
    pub recovered: bool,
    pub already_applied: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct TransformInfoArtifact {
    pub artifact_kind: String,
    pub artifact_id: String,
    pub plan_id: String,
    pub output: PathBuf,
    pub output_content_id: String,
    pub rows: u64,
    pub bytes: u64,
    pub unique_ids: u64,
    pub verified: bool,
}

#[derive(Clone, Debug)]
pub struct VerifiedTransformReceipt {
    receipt: TransformReceipt,
    receipt_path: PathBuf,
}

impl VerifiedTransformReceipt {
    pub fn receipt(&self) -> &TransformReceipt {
        &self.receipt
    }

    pub fn receipt_path(&self) -> &Path {
        &self.receipt_path
    }
}

pub fn plan_transform(request: TransformPlanRequest) -> Result<TransformPlanArtifact> {
    let limits = TransformLimits::v1();
    let plan_path = resolve_new_path(&request.plan_path, "transform plan")?;
    let output_path = resolve_new_path(&request.output_path, "transform output")?;
    if plan_path == output_path {
        return Err(
            InvalidArgumentError::new("transform plan and output paths must be different").into(),
        );
    }
    let receipt_path = transform_receipt_path(&output_path);
    if plan_path == receipt_path || fs::symlink_metadata(&receipt_path).is_ok() {
        return Err(InvalidArgumentError::new(
            "transform plan path cannot occupy the output receipt path",
        )
        .into());
    }
    let (sql_path, sql) = read_sql(&request.sql_path, &limits)?;
    let sandbox = Sandbox::create(parent_of(&plan_path)?)?;
    let run = run_transform(
        &request.inputs,
        &sql,
        &request.parameters,
        &request.id_field,
        &request.python,
        &limits,
        sandbox,
    )?;
    let expected = expected_output(&run.verified);
    let plan = TransformPlan::create(TransformPlanInput {
        sources: descriptors(&run.staged),
        sql_path: path_text(&sql_path, "transform SQL")?,
        sql,
        parameters: request.parameters,
        id_field: request.id_field,
        output_path: path_text(&output_path, "transform output")?,
        provider: run.provider.clone(),
        limits,
        expected: expected.clone(),
    })?;
    let artifact = TransformPlanArtifact {
        plan: plan_path.clone(),
        plan_id: plan.plan_id().into(),
        output: output_path,
        output_content_id: expected.output_content_id,
        schema: expected.schema,
        rows: expected.rows,
        bytes: expected.bytes,
        unique_ids: expected.unique_ids,
        provider_id: plan.provider_id().into(),
    };
    drop(run);
    save_new_file_atomically(&plan_path, plan.to_json()?.as_bytes(), "transform plan")?;
    Ok(artifact)
}

pub fn apply_transform(
    plan_path: &Path,
    accept_plan: &str,
    python: &Path,
) -> Result<TransformApplyArtifact> {
    apply_transform_with_optional_provider(plan_path, accept_plan, Some(python))
}

pub fn apply_transform_with_optional_provider(
    plan_path: &Path,
    accept_plan: &str,
    python: Option<&Path>,
) -> Result<TransformApplyArtifact> {
    let plan = TransformPlan::from_path(plan_path)?;
    if plan.plan_id() != accept_plan {
        return Err(TransformNotAuthorizedError.into());
    }
    let output = PathBuf::from(plan.output_path());
    let receipt_path = transform_receipt_path(&output);
    let output_exists = fs::symlink_metadata(&output).is_ok();
    let receipt_exists = fs::symlink_metadata(&receipt_path).is_ok();
    if output_exists {
        let verified = verify_existing_output(&plan)?;
        let expected_receipt = receipt_for_verified(&plan, &verified)?;
        if receipt_exists {
            let receipt = TransformReceipt::from_path(&receipt_path)?;
            if receipt.receipt_id() != expected_receipt.receipt_id() {
                return Err(OutputExistsError.into());
            }
            return Ok(apply_artifact(&plan, &receipt_path, &receipt, false, true));
        }
        save_receipt(&receipt_path, &expected_receipt)?;
        return Ok(apply_artifact(
            &plan,
            &receipt_path,
            &expected_receipt,
            true,
            false,
        ));
    }
    if receipt_exists {
        return Err(OutputExistsError.into());
    }
    let python = python.ok_or(TransformProviderUnavailableError)?;

    let inputs = plan
        .sources()
        .iter()
        .map(|source| {
            TransformInputSpec::new(
                source.alias.clone(),
                PathBuf::from(&source.path),
                source.format,
            )
        })
        .collect::<Result<Vec<_>>>()?;
    let (sql_path, sql) = read_sql(Path::new(plan.sql_path()), plan.limits())?;
    if path_text(&sql_path, "transform SQL")? != plan.sql_path() || sql != plan.sql() {
        return Err(TransformDriftError::new("transform SQL changed after planning").into());
    }
    let sandbox = Sandbox::create(parent_of(&output)?)?;
    let run = run_transform(
        &inputs,
        &sql,
        plan.parameters(),
        plan.id_field(),
        python,
        plan.limits(),
        sandbox,
    )?;
    if descriptors(&run.staged) != plan.sources() {
        return Err(TransformDriftError::new("transform sources changed after planning").into());
    }
    if &run.provider != plan.provider() {
        return Err(TransformDriftError::new("transform provider changed after planning").into());
    }
    let expected = expected_output(&run.verified);
    if &expected != plan.expected() {
        return Err(
            TransformDriftError::new("transform output drifted from the accepted plan").into(),
        );
    }
    let receipt = receipt_for_verified(&plan, &run.verified)?;
    run.verified.publish_new(&output)?;
    File::open(parent_of(&output)?)?.sync_all()?;
    if let Err(error) = save_receipt(&receipt_path, &receipt) {
        return Err(error).context(
            "transform output is published but receipt publication failed; rerun transform-apply with the same accepted plan to recover",
        );
    }
    Ok(apply_artifact(&plan, &receipt_path, &receipt, false, false))
}

pub fn inspect_transform(path: &Path, verify: bool) -> Result<TransformInfoArtifact> {
    let payload = fs::read(path).context("cannot read transform artifact")?;
    if payload.len() > crate::MAX_TRANSFORM_PLAN_BYTES.max(crate::MAX_TRANSFORM_RECEIPT_BYTES) {
        return Err(InvalidArgumentError::new("transform artifact exceeds its size limit").into());
    }
    let value: Value = serde_json::from_slice(&payload)
        .map_err(|_| InvalidArgumentError::new("transform artifact is invalid"))?;
    match value.get("kind").and_then(Value::as_str) {
        Some("transform_plan") => {
            let plan = TransformPlan::from_path(path)?;
            Ok(TransformInfoArtifact {
                artifact_kind: "transform_plan".into(),
                artifact_id: plan.plan_id().into(),
                plan_id: plan.plan_id().into(),
                output: PathBuf::from(plan.output_path()),
                output_content_id: plan.expected().output_content_id.clone(),
                rows: plan.expected().rows,
                bytes: plan.expected().bytes,
                unique_ids: plan.expected().unique_ids,
                verified: verify,
            })
        }
        Some("transform_receipt") => {
            let receipt = TransformReceipt::from_path(path)?;
            if verify {
                verify_receipt_output(&receipt)?;
            }
            Ok(TransformInfoArtifact {
                artifact_kind: "transform_receipt".into(),
                artifact_id: receipt.receipt_id().into(),
                plan_id: receipt.plan_id().into(),
                output: PathBuf::from(receipt.output_path()),
                output_content_id: receipt.output_content_id().into(),
                rows: receipt.rows(),
                bytes: receipt.bytes(),
                unique_ids: receipt.unique_ids(),
                verified: verify,
            })
        }
        _ => Err(InvalidArgumentError::new("unsupported transform artifact kind").into()),
    }
}

pub fn verify_transform_receipt(
    receipt_path: &Path,
    output: &Path,
    id_field: &str,
) -> Result<VerifiedTransformReceipt> {
    let receipt_path = receipt_path
        .canonicalize()
        .context("cannot resolve transform receipt")?;
    let output = output
        .canonicalize()
        .context("cannot resolve transform receipt output")?;
    let receipt = TransformReceipt::from_path(&receipt_path)?;
    let bound_output = Path::new(receipt.output_path())
        .canonicalize()
        .context("cannot resolve output bound by transform receipt")?;
    if bound_output != output || receipt.id_field() != id_field {
        return Err(TransformDriftError::new(
            "transform receipt does not bind this output path and ID field",
        )
        .into());
    }
    verify_receipt_candidate(&receipt, &output)?;
    Ok(VerifiedTransformReceipt {
        receipt,
        receipt_path,
    })
}

pub(crate) fn verify_transform_receipt_snapshot(
    receipt_path: &Path,
    bound_output: &Path,
    snapshot: &Path,
    id_field: &str,
) -> Result<VerifiedTransformReceipt> {
    let receipt_path = receipt_path
        .canonicalize()
        .context("cannot resolve transform receipt")?;
    let bound_output = bound_output
        .canonicalize()
        .context("cannot resolve transform receipt output")?;
    let receipt = TransformReceipt::from_path(&receipt_path)?;
    let receipt_output = Path::new(receipt.output_path())
        .canonicalize()
        .context("cannot resolve output bound by transform receipt")?;
    if receipt_output != bound_output || receipt.id_field() != id_field {
        return Err(TransformDriftError::new(
            "transform receipt does not bind this output path and ID field",
        )
        .into());
    }
    verify_receipt_candidate(&receipt, snapshot)?;
    Ok(VerifiedTransformReceipt {
        receipt,
        receipt_path,
    })
}

struct TransformRun {
    _sandbox: Sandbox,
    staged: Vec<StagedTransformSource>,
    provider: TransformProviderIdentity,
    verified: VerifiedTransformOutput,
}

#[allow(clippy::too_many_arguments)]
fn run_transform(
    inputs: &[TransformInputSpec],
    sql: &str,
    parameters: &[Value],
    id_field: &str,
    python: &Path,
    limits: &TransformLimits,
    sandbox: Sandbox,
) -> Result<TransformRun> {
    let staged = stage_transform_sources(inputs, sandbox.path(), limits)?;
    let aliases = staged
        .iter()
        .map(|source| source.descriptor().alias.clone())
        .collect::<BTreeSet<_>>();
    let query = validate_transform_query(sql, parameters, &aliases, id_field, limits)?;
    let provider = probe_transform_provider(python, limits)?;
    let temp = sandbox.path().join("temp");
    fs::create_dir(&temp)?;
    let candidate = sandbox.path().join("provider-candidate.jsonl");
    let canonical = sandbox.path().join("canonical.jsonl");
    let correlation = correlation_id(sql, &staged);
    let request = ProviderRequest::execute(
        correlation,
        provider.clone(),
        &staged,
        sql.into(),
        parameters.to_vec(),
        id_field.into(),
        candidate.clone(),
        temp,
        limits.clone(),
        query.sql_content_id().into(),
    )?;
    let summary = execute_transform_provider(
        python,
        &request,
        Duration::from_secs(limits.wall_time_seconds),
    )?;
    query.validate_output_rows(summary.rows)?;
    let verified =
        verify_transform_candidate(&candidate, &canonical, id_field, &summary.schema, limits)?;
    if summary.rows != verified.rows || summary.bytes != verified.bytes {
        return Err(TransformDriftError::new(
            "provider metadata does not match the independently verified output",
        )
        .into());
    }
    let _ = fs::remove_file(candidate);
    Ok(TransformRun {
        _sandbox: sandbox,
        staged,
        provider,
        verified,
    })
}

fn verify_existing_output(plan: &TransformPlan) -> Result<VerifiedTransformOutput> {
    let output = Path::new(plan.output_path());
    let verified = verify_transform_candidate_read_only(
        output,
        plan.id_field(),
        &plan.expected().schema,
        plan.limits(),
    )?;
    if expected_output(&verified) != *plan.expected() {
        return Err(OutputExistsError.into());
    }
    Ok(verified)
}

fn verify_receipt_output(receipt: &TransformReceipt) -> Result<()> {
    verify_receipt_candidate(receipt, Path::new(receipt.output_path()))
}

fn verify_receipt_candidate(receipt: &TransformReceipt, output: &Path) -> Result<()> {
    let limits = TransformLimits::v1();
    let verified = verify_transform_candidate_read_only(
        output,
        receipt.id_field(),
        receipt.schema(),
        &limits,
    )?;
    if verified.output_content_id != receipt.output_content_id()
        || verified.rows != receipt.rows()
        || verified.bytes != receipt.bytes()
        || verified.unique_ids != receipt.unique_ids()
    {
        return Err(TransformDriftError::new(
            "published transform output no longer matches its receipt",
        )
        .into());
    }
    Ok(())
}

fn receipt_for_verified(
    plan: &TransformPlan,
    verified: &VerifiedTransformOutput,
) -> Result<TransformReceipt> {
    TransformReceipt::create(
        plan,
        TransformExecutionEvidence {
            output_path: plan.output_path().into(),
            output_content_id: verified.output_content_id.clone(),
            schema: verified.schema.clone(),
            rows: verified.rows,
            bytes: verified.bytes,
            unique_ids: verified.unique_ids,
        },
    )
}

fn apply_artifact(
    plan: &TransformPlan,
    receipt_path: &Path,
    receipt: &TransformReceipt,
    recovered: bool,
    already_applied: bool,
) -> TransformApplyArtifact {
    TransformApplyArtifact {
        plan_id: plan.plan_id().into(),
        receipt: receipt_path.into(),
        receipt_id: receipt.receipt_id().into(),
        output: PathBuf::from(plan.output_path()),
        output_content_id: receipt.output_content_id().into(),
        id_field: plan.id_field().into(),
        recovered,
        already_applied,
    }
}

fn expected_output(verified: &VerifiedTransformOutput) -> TransformExpectedOutput {
    TransformExpectedOutput {
        schema: verified.schema.clone(),
        rows: verified.rows,
        bytes: verified.bytes,
        unique_ids: verified.unique_ids,
        output_content_id: verified.output_content_id.clone(),
    }
}

fn descriptors(staged: &[StagedTransformSource]) -> Vec<TransformSource> {
    staged
        .iter()
        .map(|source| source.descriptor().clone())
        .collect()
}

fn save_receipt(path: &Path, receipt: &TransformReceipt) -> Result<()> {
    save_new_file_atomically(path, receipt.to_json()?.as_bytes(), "transform receipt")
}

fn transform_receipt_path(output: &Path) -> PathBuf {
    PathBuf::from(format!(
        "{}.datajig.transform.json",
        output.to_string_lossy()
    ))
}

fn correlation_id(sql: &str, sources: &[StagedTransformSource]) -> String {
    let mut payload = sql.as_bytes().to_vec();
    for source in sources {
        payload.extend_from_slice(source.descriptor().content_id.as_bytes());
    }
    blake3_content_id(
        "correlation",
        b"datajig-transform-correlation-v1\0",
        &payload,
    )
}

fn read_sql(path: &Path, limits: &TransformLimits) -> Result<(PathBuf, String)> {
    let canonical = path
        .canonicalize()
        .context("cannot resolve transform SQL")?;
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(path)
            .context("cannot open transform SQL without following links")?
    };
    #[cfg(not(unix))]
    let mut file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(InvalidArgumentError::new("transform SQL must be a regular file").into());
    }
    let mut payload = Vec::new();
    file.by_ref()
        .take(limits.sql_bytes as u64 + 1)
        .read_to_end(&mut payload)?;
    if payload.is_empty() || payload.len() > limits.sql_bytes {
        return Err(InvalidArgumentError::new("transform SQL size is invalid").into());
    }
    let sql = String::from_utf8(payload)
        .map_err(|_| InvalidArgumentError::new("transform SQL must be valid UTF-8"))?;
    Ok((canonical, sql))
}

fn resolve_new_path(path: &Path, label: &str) -> Result<PathBuf> {
    if fs::symlink_metadata(path).is_ok() {
        return Err(OutputExistsError.into());
    }
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = parent
        .canonicalize()
        .with_context(|| format!("cannot resolve {label} parent directory"))?;
    let name = path
        .file_name()
        .ok_or_else(|| InvalidArgumentError::new(format!("{label} has no file name")))?;
    Ok(parent.join(name))
}

fn parent_of(path: &Path) -> Result<&Path> {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .or(Some(Path::new(".")))
        .context("transform path has no parent")
}

fn path_text(path: &Path, label: &str) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| InvalidArgumentError::new(format!("{label} path is not valid UTF-8")).into())
}

struct Sandbox {
    path: PathBuf,
}

impl Sandbox {
    fn create(parent: &Path) -> Result<Self> {
        let parent = parent
            .canonicalize()
            .context("cannot resolve transform sandbox parent")?;
        for attempt in 0..100u32 {
            let path = parent.join(format!(
                ".datajig-transform-{}-{attempt}",
                std::process::id()
            ));
            match fs::create_dir(&path) {
                Ok(()) => {
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
                    }
                    return Ok(Self { path });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error).context("cannot create transform sandbox"),
            }
        }
        Err(anyhow::anyhow!("cannot allocate transform sandbox"))
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
