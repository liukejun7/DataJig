use crate::strict_json::reject_duplicate_json_members;
use crate::tabular_source::{
    TabularConsumer, TabularRow, stream_csv, stream_jsonl, stream_parquet,
};
use crate::{
    InvalidArgumentError, TransformField, TransformLimits, TransformProviderIdentity,
    TransformSource, TransformSourceFormat,
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const PROVIDER_PROTOCOL: &str = "datajig.transform-provider.v1";

#[derive(Debug)]
pub struct TransformProviderProtocolError(String);

impl fmt::Display for TransformProviderProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transform provider protocol failed: {}", self.0)
    }
}

impl Error for TransformProviderProtocolError {}

#[derive(Debug)]
pub struct TransformProviderTimeoutError;

impl fmt::Display for TransformProviderTimeoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("transform provider exceeded its execution deadline")
    }
}

impl Error for TransformProviderTimeoutError {}

#[derive(Clone, Debug)]
pub struct TransformInputSpec {
    pub alias: String,
    pub path: PathBuf,
    pub format: TransformSourceFormat,
}

impl TransformInputSpec {
    pub fn new(alias: String, path: PathBuf, format: TransformSourceFormat) -> Result<Self> {
        if !valid_alias(&alias) {
            return Err(InvalidArgumentError::new(
                "transform input alias must match [a-z][a-z0-9_]{0,63}",
            )
            .into());
        }
        Ok(Self {
            alias,
            path,
            format,
        })
    }
}

#[derive(Clone, Debug)]
pub struct StagedTransformSource {
    descriptor: TransformSource,
    staged_path: PathBuf,
}

impl StagedTransformSource {
    pub fn descriptor(&self) -> &TransformSource {
        &self.descriptor
    }

    pub fn staged_path(&self) -> &Path {
        &self.staged_path
    }
}

pub fn stage_transform_sources(
    inputs: &[TransformInputSpec],
    sandbox: &Path,
    limits: &TransformLimits,
) -> Result<Vec<StagedTransformSource>> {
    if inputs.is_empty() || inputs.len() > limits.inputs {
        return Err(InvalidArgumentError::new(format!(
            "transform requires 1 to {} inputs",
            limits.inputs
        ))
        .into());
    }
    let sandbox_metadata =
        fs::symlink_metadata(sandbox).context("cannot inspect transform staging directory")?;
    if sandbox_metadata.file_type().is_symlink() || !sandbox_metadata.is_dir() {
        return Err(
            InvalidArgumentError::new("transform staging path must be a real directory").into(),
        );
    }
    let sandbox = sandbox
        .canonicalize()
        .context("cannot resolve transform staging directory")?;
    let mut aliases = BTreeSet::new();
    for input in inputs {
        if !valid_alias(&input.alias) || !aliases.insert(input.alias.clone()) {
            return Err(InvalidArgumentError::new(
                "transform input aliases must be valid and unique",
            )
            .into());
        }
    }
    let originals = inputs
        .iter()
        .map(|input| {
            input
                .path
                .canonicalize()
                .context("cannot resolve transform input")
        })
        .collect::<Result<Vec<_>>>()?;
    let mut staged = Vec::with_capacity(inputs.len());
    let mut total_bytes = 0u64;
    let mut total_rows = 0u64;
    for (index, (input, original)) in inputs.iter().zip(originals).enumerate() {
        let name = format!(
            "{index:02}-{}.{}",
            input.alias,
            format_extension(input.format)
        );
        let destination = sandbox.join(name);
        let (bytes, content_id) = match stage_regular_file(&input.path, &destination) {
            Ok(result) => result,
            Err(error) => {
                cleanup_staged(&staged, Some(&destination));
                return Err(error);
            }
        };
        let prepared = (|| -> Result<(u64, u64, TransformSource)> {
            let next_bytes = total_bytes
                .checked_add(bytes)
                .context("transform source byte count overflow")?;
            if next_bytes > limits.source_bytes {
                return Err(InvalidArgumentError::new(format!(
                    "transform sources exceed {} bytes",
                    limits.source_bytes
                ))
                .into());
            }
            let rows = count_rows(&destination, input.format, limits.source_rows - total_rows)?;
            let next_rows = total_rows
                .checked_add(rows)
                .context("transform source row count overflow")?;
            if next_rows > limits.source_rows {
                return Err(InvalidArgumentError::new(format!(
                    "transform sources exceed {} rows",
                    limits.source_rows
                ))
                .into());
            }
            let descriptor = TransformSource::create(
                input.alias.clone(),
                path_text(&original)?,
                input.format,
                bytes,
                rows,
                content_id,
            )?;
            Ok((next_bytes, next_rows, descriptor))
        })();
        let (next_bytes, next_rows, descriptor) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                cleanup_staged(&staged, Some(&destination));
                return Err(error);
            }
        };
        total_bytes = next_bytes;
        total_rows = next_rows;
        staged.push(StagedTransformSource {
            descriptor,
            staged_path: destination,
        });
    }
    Ok(staged)
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProviderOperation {
    Execute,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderSource {
    alias: String,
    path: String,
    format: TransformSourceFormat,
    content_id: String,
    bytes: u64,
    rows: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderRequest {
    protocol: &'static str,
    protocol_version: u8,
    correlation_id: String,
    operation: ProviderOperation,
    expected_provider: TransformProviderIdentity,
    sources: Vec<ProviderSource>,
    sql: String,
    parameters: Vec<Value>,
    id_field: String,
    candidate_path: String,
    temp_directory: String,
    limits: TransformLimits,
    ast_policy_digest: String,
}

impl ProviderRequest {
    #[allow(clippy::too_many_arguments)]
    pub fn execute(
        correlation_id: String,
        expected_provider: TransformProviderIdentity,
        sources: &[StagedTransformSource],
        sql: String,
        parameters: Vec<Value>,
        id_field: String,
        candidate_path: PathBuf,
        temp_directory: PathBuf,
        limits: TransformLimits,
        ast_policy_digest: String,
    ) -> Result<Self> {
        if correlation_id.is_empty()
            || sources.is_empty()
            || sql.is_empty()
            || id_field.is_empty()
            || ast_policy_digest.is_empty()
        {
            return Err(
                InvalidArgumentError::new("transform provider request is incomplete").into(),
            );
        }
        let sandbox = sources[0]
            .staged_path
            .parent()
            .context("staged transform source has no sandbox")?
            .canonicalize()
            .context("cannot resolve transform sandbox")?;
        if sources.iter().any(|source| {
            source
                .staged_path
                .parent()
                .and_then(|parent| parent.canonicalize().ok())
                .is_none_or(|parent| parent != sandbox)
        }) {
            return Err(InvalidArgumentError::new(
                "all transform sources must be inside one sandbox",
            )
            .into());
        }
        let candidate_parent = candidate_path
            .parent()
            .context("transform candidate has no parent")?
            .canonicalize()
            .context("cannot resolve transform candidate parent")?;
        let temp_metadata = fs::symlink_metadata(&temp_directory)
            .context("cannot inspect transform temp directory")?;
        let resolved_temp = temp_directory
            .canonicalize()
            .context("cannot resolve transform temp directory")?;
        if candidate_parent != sandbox
            || fs::symlink_metadata(&candidate_path).is_ok()
            || temp_metadata.file_type().is_symlink()
            || !temp_metadata.is_dir()
            || !resolved_temp.starts_with(&sandbox)
        {
            return Err(InvalidArgumentError::new(
                "provider output and temp paths must be new paths inside the transform sandbox",
            )
            .into());
        }
        let candidate_path = path_text(&candidate_path)?;
        let temp_directory = path_text(&temp_directory)?;
        let sources = sources
            .iter()
            .map(|source| {
                Ok(ProviderSource {
                    alias: source.descriptor.alias.clone(),
                    path: path_text(&source.staged_path)?,
                    format: source.descriptor.format,
                    content_id: source.descriptor.content_id.clone(),
                    bytes: source.descriptor.bytes,
                    rows: source.descriptor.rows,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            protocol: PROVIDER_PROTOCOL,
            protocol_version: 1,
            correlation_id,
            operation: ProviderOperation::Execute,
            expected_provider,
            sources,
            sql,
            parameters,
            id_field,
            candidate_path,
            temp_directory,
            limits,
            ast_policy_digest,
        })
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string(self)?)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderResponse {
    protocol: String,
    protocol_version: u8,
    correlation_id: String,
    status: String,
    provider: TransformProviderIdentity,
    schema: Vec<TransformField>,
    rows: u64,
    bytes: u64,
    candidate_complete: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderProbeResponse {
    protocol: String,
    protocol_version: u8,
    correlation_id: String,
    status: String,
    provider: TransformProviderIdentity,
    lockdown_supported: bool,
}

#[derive(Clone, Debug)]
pub struct ProviderExecutionSummary {
    pub schema: Vec<TransformField>,
    pub rows: u64,
    pub bytes: u64,
    pub candidate_path: PathBuf,
}

pub fn probe_transform_provider(
    python: &Path,
    limits: &TransformLimits,
) -> Result<TransformProviderIdentity> {
    let python = validate_provider_executable(python)?;
    let payload = serde_json::to_vec(&serde_json::json!({
        "protocol": PROVIDER_PROTOCOL,
        "protocol_version": 1,
        "correlation_id": "probe",
        "operation": "probe",
        "limits": limits,
    }))?;
    let mut command = provider_command(&python);
    let mut child = command
        .spawn()
        .context("cannot start transform provider probe")?;
    child
        .stdin
        .take()
        .context("transform provider stdin is unavailable")?
        .write_all(&payload)?;
    let stdout = child.stdout.take().context("provider stdout unavailable")?;
    let stderr = child.stderr.take().context("provider stderr unavailable")?;
    let stdout_limit = limits.provider_stdout_bytes;
    let stderr_limit = limits.provider_stderr_bytes;
    let stdout_reader = thread::spawn(move || read_bounded(stdout, stdout_limit));
    let stderr_reader = thread::spawn(move || read_bounded(stderr, stderr_limit));
    let deadline = Instant::now() + Duration::from_secs(5);
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            terminate_provider_tree(&mut child);
            let _ = child.wait();
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            return Err(TransformProviderTimeoutError.into());
        }
        thread::sleep(Duration::from_millis(5));
    };
    let stdout = join_reader(stdout_reader)?;
    let stderr = join_reader(stderr_reader)?;
    if !status.success() {
        return Err(TransformProviderProtocolError(format!(
            "provider probe exited unsuccessfully (diagnostic bytes: {})",
            stderr.len()
        ))
        .into());
    }
    let text = std::str::from_utf8(&stdout)
        .map_err(|_| TransformProviderProtocolError("probe stdout is not UTF-8".into()))?;
    reject_duplicate_json_members(text)
        .map_err(|_| TransformProviderProtocolError("probe stdout is not strict JSON".into()))?;
    let response: ProviderProbeResponse = serde_json::from_str(text).map_err(|_| {
        TransformProviderProtocolError("probe response has an invalid schema".into())
    })?;
    if response.protocol != PROVIDER_PROTOCOL
        || response.protocol_version != 1
        || response.correlation_id != "probe"
        || response.status != "ok"
        || !response.lockdown_supported
    {
        return Err(TransformProviderProtocolError(
            "provider probe did not prove the required lockdown".into(),
        )
        .into());
    }
    Ok(response.provider)
}

pub fn execute_transform_provider(
    python: &Path,
    request: &ProviderRequest,
    timeout: Duration,
) -> Result<ProviderExecutionSummary> {
    let python = validate_provider_executable(python)?;
    let request_payload = request.to_json()?;
    let before = directory_entries(parent_of(Path::new(&request.candidate_path))?)?;
    let mut command = provider_command(&python);
    let mut child = command.spawn().context("cannot start transform provider")?;
    child
        .stdin
        .take()
        .context("transform provider stdin is unavailable")?
        .write_all(request_payload.as_bytes())?;
    let stdout = child.stdout.take().context("provider stdout unavailable")?;
    let stderr = child.stderr.take().context("provider stderr unavailable")?;
    let stdout_limit = request.limits.provider_stdout_bytes;
    let stderr_limit = request.limits.provider_stderr_bytes;
    let stdout_reader = thread::spawn(move || read_bounded(stdout, stdout_limit));
    let stderr_reader = thread::spawn(move || read_bounded(stderr, stderr_limit));
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if Instant::now() >= deadline {
            terminate_provider_tree(&mut child);
            let _ = child.wait();
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            cleanup_candidate(request);
            return Err(TransformProviderTimeoutError.into());
        }
        thread::sleep(Duration::from_millis(5));
    };
    let stdout = join_reader(stdout_reader)?;
    let stderr = join_reader(stderr_reader)?;
    if !status.success() {
        cleanup_candidate(request);
        return Err(TransformProviderProtocolError(format!(
            "provider exited unsuccessfully (diagnostic bytes: {})",
            stderr.len()
        ))
        .into());
    }
    let text = std::str::from_utf8(&stdout)
        .map_err(|_| TransformProviderProtocolError("stdout is not UTF-8".into()))?;
    reject_duplicate_json_members(text)
        .map_err(|_| TransformProviderProtocolError("stdout is not strict JSON".into()))?;
    let response: ProviderResponse = serde_json::from_str(text)
        .map_err(|_| TransformProviderProtocolError("stdout has an invalid schema".into()))?;
    if response.protocol != PROVIDER_PROTOCOL
        || response.protocol_version != 1
        || response.correlation_id != request.correlation_id
        || response.status != "ok"
        || response.provider != request.expected_provider
        || !response.candidate_complete
        || response.schema.len() > request.limits.output_fields
        || response.rows > request.limits.output_rows
        || response.bytes > request.limits.output_bytes
    {
        cleanup_candidate(request);
        return Err(TransformProviderProtocolError(
            "provider response does not match the request contract".into(),
        )
        .into());
    }
    let candidate_path = PathBuf::from(&request.candidate_path);
    let metadata = fs::symlink_metadata(&candidate_path)
        .map_err(|_| TransformProviderProtocolError("candidate output is missing".into()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        cleanup_candidate(request);
        return Err(TransformProviderProtocolError(
            "candidate output is not a regular file".into(),
        )
        .into());
    }
    let after = directory_entries(parent_of(&candidate_path)?)?;
    let candidate_name = candidate_path
        .file_name()
        .context("candidate output has no file name")?
        .to_owned();
    if after
        .difference(&before)
        .any(|entry| entry != &candidate_name)
    {
        cleanup_candidate(request);
        return Err(TransformProviderProtocolError(
            "provider created an unexpected sandbox entry".into(),
        )
        .into());
    }
    Ok(ProviderExecutionSummary {
        schema: response.schema,
        rows: response.rows,
        bytes: response.bytes,
        candidate_path,
    })
}

fn stage_regular_file(source: &Path, destination: &Path) -> Result<(u64, String)> {
    #[cfg(not(unix))]
    {
        let _ = (source, destination);
        anyhow::bail!("transform staging currently requires Unix")
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let mut source_file = OpenOptions::new()
            .read(true)
            .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
            .open(source)
            .context("cannot open transform input without following links")?;
        let before = source_file.metadata()?;
        if !before.is_file() {
            return Err(InvalidArgumentError::new("transform input must be a regular file").into());
        }
        let mut destination_file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(destination)
            .context("cannot create staged transform input")?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"datajig-transform-source-v1\0");
        let mut bytes = 0u64;
        let mut buffer = [0u8; 1024 * 1024];
        loop {
            let read = source_file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            destination_file.write_all(&buffer[..read])?;
            hasher.update(&buffer[..read]);
            bytes = bytes
                .checked_add(read as u64)
                .context("source size overflow")?;
        }
        destination_file.sync_all()?;
        let after = source_file.metadata()?;
        if before.dev() != after.dev()
            || before.ino() != after.ino()
            || before.len() != after.len()
            || before.mtime() != after.mtime()
            || before.mtime_nsec() != after.mtime_nsec()
            || before.ctime() != after.ctime()
            || before.ctime_nsec() != after.ctime_nsec()
            || bytes != after.len()
        {
            let _ = fs::remove_file(destination);
            return Err(
                InvalidArgumentError::new("transform input changed while it was staged").into(),
            );
        }
        Ok((bytes, format!("source_{}", hasher.finalize().to_hex())))
    }
}

struct RowCounter {
    rows: u64,
    maximum: u64,
}

impl TabularConsumer for RowCounter {
    fn headers(&mut self, _headers: &[String]) -> Result<()> {
        Ok(())
    }

    fn row(&mut self, row: TabularRow) -> Result<()> {
        if row
            .values()
            .any(|value| matches!(value, Value::Array(_) | Value::Object(_)))
        {
            return Err(InvalidArgumentError::new(
                "transform JSONL inputs must contain only scalar values",
            )
            .into());
        }
        self.rows += 1;
        if self.rows > self.maximum {
            return Err(InvalidArgumentError::new("transform source row limit exceeded").into());
        }
        Ok(())
    }
}

fn count_rows(path: &Path, format: TransformSourceFormat, maximum: u64) -> Result<u64> {
    let mut counter = RowCounter { rows: 0, maximum };
    match format {
        TransformSourceFormat::Csv => stream_csv(path, b',', &mut counter)?,
        TransformSourceFormat::Parquet => stream_parquet(path, &mut counter)?,
        TransformSourceFormat::Jsonl => stream_jsonl(path, &mut counter)?,
    }
    Ok(counter.rows)
}

fn read_bounded(mut reader: impl Read, maximum: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(
            TransformProviderProtocolError("provider stream exceeded its bound".into()).into(),
        );
    }
    Ok(bytes)
}

fn join_reader(handle: thread::JoinHandle<Result<Vec<u8>>>) -> Result<Vec<u8>> {
    handle
        .join()
        .map_err(|_| TransformProviderProtocolError("provider stream reader failed".into()))?
}

fn directory_entries(path: &Path) -> Result<BTreeSet<std::ffi::OsString>> {
    Ok(fs::read_dir(path)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<std::io::Result<_>>()?)
}

fn parent_of(path: &Path) -> Result<&Path> {
    path.parent()
        .context("transform candidate has no parent directory")
}

fn cleanup_candidate(request: &ProviderRequest) {
    let _ = fs::remove_file(&request.candidate_path);
}

fn terminate_provider_tree(child: &mut std::process::Child) {
    #[cfg(unix)]
    {
        if let Some(pid) = rustix::process::Pid::from_raw(child.id() as i32) {
            let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
            return;
        }
    }
    let _ = child.kill();
}

fn validate_provider_executable(python: &Path) -> Result<PathBuf> {
    let resolved = python
        .canonicalize()
        .context("cannot resolve transform provider executable")?;
    let executable =
        fs::symlink_metadata(&resolved).context("cannot inspect transform provider executable")?;
    if !executable.is_file() {
        return Err(TransformProviderProtocolError(
            "provider executable must be a regular file".into(),
        )
        .into());
    }
    Ok(python.to_owned())
}

fn provider_command(python: &Path) -> Command {
    let mut command = Command::new(python);
    command
        .args(["-I", "-m", "datajig.providers.duckdb"])
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    command
}

fn cleanup_staged(staged: &[StagedTransformSource], current: Option<&Path>) {
    for source in staged {
        let _ = fs::remove_file(&source.staged_path);
    }
    if let Some(path) = current {
        let _ = fs::remove_file(path);
    }
}

fn format_extension(format: TransformSourceFormat) -> &'static str {
    match format {
        TransformSourceFormat::Csv => "csv",
        TransformSourceFormat::Parquet => "parquet",
        TransformSourceFormat::Jsonl => "jsonl",
    }
}

fn path_text(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .context("transform path is not valid UTF-8")
}

fn valid_alias(value: &str) -> bool {
    let mut characters = value.chars();
    matches!(characters.next(), Some(first) if first.is_ascii_lowercase())
        && value.len() <= 64
        && characters.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}
