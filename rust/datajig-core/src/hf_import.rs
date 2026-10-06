use crate::OutputExistsError;
use crate::identity::blake3_content_id;
use crate::io::save_new_file_atomically;
use anyhow::{Context, Result, bail};
use globset::{Glob, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::net::IpAddr;
use std::path::{Component, Path, PathBuf};
use std::sync::LazyLock;
use ureq::Agent;
use ureq::tls::{RootCerts, TlsConfig, TlsProvider};
use url::Url;
use walkdir::WalkDir;

pub const HF_IMPORT_PLAN_SCHEMA_VERSION: u8 = 1;
pub const HF_IMPORT_RECEIPT_SCHEMA_VERSION: u8 = 1;
pub const MAX_HF_IMPORT_FILES: usize = 10_000;
pub const MAX_HF_IMPORT_BYTES: u64 = 1_099_511_627_776;
pub const MAX_HF_IMPORT_PATH_BYTES: usize = 4_096;
pub const MAX_HF_IMPORT_PATTERNS: usize = 64;
pub const MAX_HF_IMPORT_PATTERN_BYTES: usize = 1_024;

const PLAN_KIND: &str = "hugging_face_import_plan";
const RECEIPT_KIND: &str = "hugging_face_import_receipt";
const MAX_PLAN_JSON_BYTES: usize = 64 * 1024 * 1024;
const MAX_RECEIPT_JSON_BYTES: usize = 64 * 1024 * 1024;
const PLAN_IDENTITY_DOMAIN: &[u8] = b"datajig-hugging-face-import-plan-v1\0";
const RECEIPT_IDENTITY_DOMAIN: &[u8] = b"datajig-hugging-face-import-receipt-v1\0";
const DEFAULT_IMPORT_EXTENSIONS: &[&str] = &["arrow", "csv", "json", "jsonl", "parquet"];
const MAX_HF_TREE_ENTRIES: usize = 100_000;
const IMPORT_RECEIPT_NAME: &str = "datajig.hf-import.json";

#[derive(Debug)]
pub struct HfImportNotAuthorizedError;

impl fmt::Display for HfImportNotAuthorizedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("accepted Hugging Face import plan identity does not match")
    }
}

impl Error for HfImportNotAuthorizedError {}

#[derive(Clone, Debug, Serialize)]
pub struct HfImportPlanArtifact {
    pub schema_version: u8,
    pub plan_id: String,
    pub repo_id: String,
    pub requested_revision: String,
    pub resolved_commit: String,
    pub total_files: usize,
    pub total_bytes: u64,
    pub plan: String,
    pub output: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct HfImportApplyArtifact {
    pub schema_version: u8,
    pub outcome: String,
    pub import_id: String,
    pub plan_id: String,
    pub repo_id: String,
    pub resolved_commit: String,
    pub total_files: usize,
    pub total_bytes: u64,
    pub output: String,
    pub receipt: String,
}

#[doc(hidden)]
pub trait HfHubMetadataClient {
    fn resolve_dataset_revision(&self, repo_id: &str, revision: &str) -> Result<String>;
    fn list_dataset_files(&self, repo_id: &str, revision: &str) -> Result<Vec<HfImportFile>>;
}

#[doc(hidden)]
pub trait HfHubDownloadClient {
    fn download_dataset_file(
        &self,
        repo_id: &str,
        revision: &str,
        path: &str,
        expected_size: u64,
        destination: &Path,
    ) -> Result<()>;
}

#[derive(Clone, Debug)]
pub(crate) struct HfHubHttpClient {
    agent: Agent,
    endpoint: String,
    token: Option<String>,
}

impl HfHubHttpClient {
    fn from_environment() -> Result<Self> {
        let endpoint =
            std::env::var("HF_ENDPOINT").unwrap_or_else(|_| "https://huggingface.co".to_owned());
        let token = hugging_face_token()?;
        let parsed_endpoint = Url::parse(&endpoint).context("HF_ENDPOINT must be a valid URL")?;
        validate_hub_endpoint_security(&parsed_endpoint, token.is_some())?;
        Ok(Self {
            agent: hub_http_agent(),
            endpoint,
            token,
        })
    }

    fn api_url(&self, repo_id: &str, operation: &str, revision: &str) -> Result<Url> {
        let (owner, name) = repo_id
            .split_once('/')
            .context("Hugging Face repository ID must be exactly owner/name")?;
        let mut url = Url::parse(&self.endpoint).context("HF_ENDPOINT must be a valid URL")?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("HF_ENDPOINT cannot be used as a base URL"))?;
            segments.pop_if_empty();
            segments.extend(["api", "datasets", owner, name, operation, revision]);
        }
        Ok(url)
    }

    fn download_url(&self, repo_id: &str, revision: &str, path: &str) -> Result<Url> {
        let (owner, name) = repo_id
            .split_once('/')
            .context("Hugging Face repository ID must be exactly owner/name")?;
        let mut url = Url::parse(&self.endpoint).context("HF_ENDPOINT must be a valid URL")?;
        {
            let mut segments = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("HF_ENDPOINT cannot be used as a base URL"))?;
            segments.pop_if_empty();
            segments.extend(["datasets", owner, name, "resolve", revision]);
            segments.extend(path.split('/'));
        }
        Ok(url)
    }

    fn get(&self, url: &str) -> Result<ureq::http::Response<ureq::Body>> {
        let request = self.agent.get(url);
        let response = if let Some(token) = &self.token {
            request
                .header("Authorization", format!("Bearer {token}"))
                .call()
        } else {
            request.call()
        };
        response.map_err(|error| sanitized_hub_error("Hugging Face Hub request failed", error))
    }
}

impl HfHubDownloadClient for HfHubHttpClient {
    fn download_dataset_file(
        &self,
        repo_id: &str,
        revision: &str,
        path: &str,
        expected_size: u64,
        destination: &Path,
    ) -> Result<()> {
        let url = self.download_url(repo_id, revision, path)?;
        let mut response = self.get(url.as_str()).map_err(|error| {
            sanitized_hub_error("cannot download Hugging Face dataset file", error)
        })?;
        let mut target = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination)
            .context("cannot create staged Hugging Face file")?;
        copy_hub_file(
            &mut response.body_mut().as_reader(),
            &mut target,
            expected_size,
        )?;
        target.sync_all()?;
        Ok(())
    }
}

fn copy_hub_file<R: Read, W: Write>(
    source: &mut R,
    destination: &mut W,
    expected_size: u64,
) -> Result<u64> {
    let limit = expected_size
        .checked_add(1)
        .context("planned Hugging Face file size overflow")?;
    let copied = std::io::copy(&mut source.take(limit), destination)
        .context("cannot stage Hugging Face file")?;
    if copied > expected_size {
        bail!("downloaded Hugging Face file exceeded its planned size");
    }
    if copied < expected_size {
        bail!("downloaded Hugging Face file was shorter than its planned size");
    }
    Ok(copied)
}

fn validate_hub_endpoint_security(endpoint: &Url, has_token: bool) -> Result<()> {
    if endpoint.scheme() == "https" {
        return Ok(());
    }
    let loopback = endpoint.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .is_ok_and(|address| address.is_loopback())
    });
    if endpoint.scheme() == "http" && loopback && !has_token {
        return Ok(());
    }
    bail!("HF_ENDPOINT must use HTTPS; unauthenticated loopback HTTP is allowed for testing")
}

fn hub_http_agent() -> Agent {
    Agent::config_builder()
        .tls_config(
            TlsConfig::builder()
                .provider(TlsProvider::Rustls)
                .root_certs(RootCerts::PlatformVerifier)
                .build(),
        )
        .build()
        .into()
}

fn dataset_tree_url(endpoint: &str, repo_id: &str, revision: &str) -> Result<Url> {
    let (owner, name) = repo_id
        .split_once('/')
        .context("Hugging Face repository ID must be exactly owner/name")?;
    let mut url = Url::parse(endpoint).context("HF_ENDPOINT must be a valid URL")?;
    {
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| anyhow::anyhow!("HF_ENDPOINT cannot be used as a base URL"))?;
        segments.pop_if_empty();
        segments.extend(["api", "datasets", owner, name, "tree", revision]);
    }
    url.query_pairs_mut()
        .append_pair("recursive", "true")
        .append_pair("limit", "1000");
    Ok(url)
}

impl HfHubMetadataClient for HfHubHttpClient {
    fn resolve_dataset_revision(&self, repo_id: &str, revision: &str) -> Result<String> {
        let url = self.api_url(repo_id, "revision", revision)?;
        let mut response = self.get(url.as_str())?;
        let info: HubDatasetInfo = response.body_mut().read_json().map_err(|error| {
            sanitized_hub_error("cannot decode Hugging Face dataset metadata", error)
        })?;
        Ok(info.sha)
    }

    fn list_dataset_files(&self, repo_id: &str, revision: &str) -> Result<Vec<HfImportFile>> {
        let endpoint = Url::parse(&self.endpoint).context("HF_ENDPOINT must be a valid URL")?;
        let url = dataset_tree_url(&self.endpoint, repo_id, revision)?;
        let mut next = Some(url.to_string());
        let mut files = Vec::new();
        let mut entries_seen = 0_usize;
        while let Some(page_url) = next.take() {
            let page_url = validate_pagination_url(
                &endpoint,
                Url::parse(&page_url).context("Hugging Face tree URL is invalid")?,
            )?;
            let mut response = self.get(page_url.as_str())?;
            next = next_link(response.headers())?;
            let page: Vec<HubTreeEntry> = response.body_mut().read_json().map_err(|error| {
                sanitized_hub_error("cannot decode Hugging Face dataset tree", error)
            })?;
            entries_seen = entries_seen
                .checked_add(page.len())
                .context("Hugging Face dataset tree entry count overflow")?;
            if entries_seen > MAX_HF_TREE_ENTRIES {
                bail!("Hugging Face dataset tree exceeds {MAX_HF_TREE_ENTRIES} entries");
            }
            for entry in page {
                if entry.kind == "file" {
                    let size = entry
                        .size
                        .context("Hugging Face file tree entry did not include a size")?;
                    files.push(HfImportFile::new(entry.path, size, entry.oid)?);
                }
            }
        }
        Ok(files)
    }
}

#[derive(Deserialize)]
struct HubDatasetInfo {
    sha: String,
    #[serde(flatten)]
    _extra: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
struct HubTreeEntry {
    oid: String,
    path: String,
    size: Option<u64>,
    #[serde(rename = "type")]
    kind: String,
}

pub fn plan_hf_import(
    repo_id: &str,
    revision: &str,
    includes: &[String],
    ignores: &[String],
    output: &Path,
    plan_path: &Path,
) -> Result<HfImportPlanArtifact> {
    let client = HfHubHttpClient::from_environment()?;
    plan_hf_import_with_client(
        &client, repo_id, revision, includes, ignores, output, plan_path,
    )
}

#[doc(hidden)]
pub fn plan_hf_import_with_client<C: HfHubMetadataClient>(
    client: &C,
    repo_id: &str,
    revision: &str,
    includes: &[String],
    ignores: &[String],
    output: &Path,
    plan_path: &Path,
) -> Result<HfImportPlanArtifact> {
    validate_repo_id(repo_id)?;
    validate_requested_revision(revision)?;
    if output.exists() || plan_path.exists() {
        bail!("Hugging Face import output and plan must name new paths");
    }
    let lexical_output = lexical_normalize(std::path::absolute(output)?);
    let lexical_plan = lexical_normalize(std::path::absolute(plan_path)?);
    ensure_paths_are_separate(&lexical_output, &lexical_plan)?;
    let include_set = build_glob_set(includes, "include")?;
    let ignore_set = build_glob_set(ignores, "ignore")?;
    let output = resolve_new_import_output(output)?;
    let plan_path = resolve_new_plan_path(plan_path)?;
    ensure_paths_are_separate(&output, &plan_path)?;

    let resolved_commit = client
        .resolve_dataset_revision(repo_id, revision)
        .map_err(|error| {
            sanitized_hub_error("cannot resolve Hugging Face dataset revision", error)
        })?;
    validate_resolved_commit(&resolved_commit)?;
    let files = client
        .list_dataset_files(repo_id, &resolved_commit)
        .map_err(|error| sanitized_hub_error("cannot list Hugging Face dataset tree", error))?;
    let selected = files
        .into_iter()
        .filter(|file| {
            let included = if includes.is_empty() {
                has_default_import_extension(file.path())
            } else {
                include_set.is_match(file.path())
            };
            included && !ignore_set.is_match(file.path())
        })
        .collect();
    let output_text = path_text(&output, "output")?;
    let plan = HfImportPlan::create(
        repo_id.to_owned(),
        revision.to_owned(),
        resolved_commit,
        includes.to_vec(),
        ignores.to_vec(),
        output_text.clone(),
        selected,
    )?;
    let payload = plan.to_json()?;
    save_new_file_atomically(&plan_path, payload.as_bytes(), "Hugging Face import plan")?;

    Ok(HfImportPlanArtifact {
        schema_version: plan.schema_version(),
        plan_id: plan.plan_id().to_owned(),
        repo_id: plan.repo_id().to_owned(),
        requested_revision: plan.requested_revision().to_owned(),
        resolved_commit: plan.resolved_commit().to_owned(),
        total_files: plan.summary().total_files,
        total_bytes: plan.summary().total_bytes,
        plan: path_text(&plan_path, "plan")?,
        output: output_text,
    })
}

pub fn apply_hf_import(plan_path: &Path, accepted_plan_id: &str) -> Result<HfImportApplyArtifact> {
    let client = HfHubHttpClient::from_environment()?;
    apply_hf_import_with_client(&client, plan_path, accepted_plan_id)
}

#[doc(hidden)]
pub fn apply_hf_import_with_client<C: HfHubDownloadClient>(
    client: &C,
    plan_path: &Path,
    accepted_plan_id: &str,
) -> Result<HfImportApplyArtifact> {
    let plan = load_hf_import_plan(plan_path)?;
    if plan.plan_id() != accepted_plan_id {
        return Err(HfImportNotAuthorizedError.into());
    }
    let output = PathBuf::from(plan.output());
    if fs::symlink_metadata(&output).is_ok() {
        return match validate_existing_import(&plan, &output) {
            Ok(receipt) => apply_artifact(&plan, &receipt, "already_applied"),
            Err(_) => Err(OutputExistsError.into()),
        };
    }
    let parent = safe_existing_output_parent(&output)?;
    let staging = allocate_import_staging(&output, &parent)?;

    for file in plan.files() {
        let destination = staging.path.join(file.path());
        let file_parent = destination
            .parent()
            .context("staged Hugging Face file has no parent")?;
        fs::create_dir_all(file_parent).context("cannot create staged import directory")?;
        client
            .download_dataset_file(
                plan.repo_id(),
                plan.resolved_commit(),
                file.path(),
                file.size(),
                &destination,
            )
            .map_err(|error| {
                sanitized_hub_error("cannot download Hugging Face dataset file", error)
            })?;
    }

    let imported_files = verify_import_tree(&staging.path, &plan, false)?;
    let receipt = HfImportReceipt::create(&plan, imported_files)?;
    let receipt_path = staging.path.join(IMPORT_RECEIPT_NAME);
    save_new_file_atomically(
        &receipt_path,
        receipt.to_json()?.as_bytes(),
        "Hugging Face import receipt",
    )?;
    let verified_files = verify_import_tree(&staging.path, &plan, true)?;
    if verified_files != receipt.files {
        bail!("staged Hugging Face files changed while publishing the receipt");
    }
    sync_import_tree(&staging.path)?;
    publish_import_staging(staging, &output)?;
    apply_artifact(&plan, &receipt, "applied")
}

fn load_hf_import_plan(path: &Path) -> Result<HfImportPlan> {
    let metadata = fs::symlink_metadata(path).context("cannot inspect Hugging Face import plan")?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("Hugging Face import plan must be a regular file");
    }
    if metadata.len() > MAX_PLAN_JSON_BYTES as u64 {
        bail!("Hugging Face import plan is too large");
    }
    let mut payload = String::new();
    File::open(path)?
        .take(MAX_PLAN_JSON_BYTES as u64 + 1)
        .read_to_string(&mut payload)
        .context("Hugging Face import plan must contain valid UTF-8")?;
    if payload.len() > MAX_PLAN_JSON_BYTES {
        bail!("Hugging Face import plan is too large");
    }
    HfImportPlan::from_json(&payload)
}

fn safe_existing_output_parent(output: &Path) -> Result<PathBuf> {
    if !output.is_absolute() {
        bail!("Hugging Face import plan output must be absolute");
    }
    let parent = output
        .parent()
        .context("Hugging Face import output has no parent")?;
    let metadata = fs::symlink_metadata(parent)
        .context("Hugging Face import output parent must already exist")?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("Hugging Face import output parent must be a real directory");
    }
    let canonical = parent
        .canonicalize()
        .context("cannot resolve Hugging Face import output parent")?;
    if canonical != parent {
        bail!("Hugging Face import output parent changed after planning");
    }
    Ok(canonical)
}

struct ImportStaging {
    path: PathBuf,
    cleanup: bool,
}

impl Drop for ImportStaging {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

fn allocate_import_staging(output: &Path, parent: &Path) -> Result<ImportStaging> {
    let name = output
        .file_name()
        .context("Hugging Face import output has no file name")?
        .to_string_lossy();
    for attempt in 0..100_u32 {
        let path = parent.join(format!(
            ".{name}.{}.{}.hf-import.tmp",
            std::process::id(),
            attempt
        ));
        match fs::create_dir(&path) {
            Ok(()) => {
                return Ok(ImportStaging {
                    path,
                    cleanup: true,
                });
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("cannot create Hugging Face import staging"),
        }
    }
    bail!("cannot allocate Hugging Face import staging directory")
}

fn verify_import_tree(
    root: &Path,
    plan: &HfImportPlan,
    allow_receipt: bool,
) -> Result<Vec<HfImportedFile>> {
    let mut actual_paths = BTreeSet::new();
    for entry in WalkDir::new(root).follow_links(false).min_depth(1) {
        let entry = entry.context("cannot inspect staged Hugging Face import")?;
        let file_type = entry.file_type();
        if file_type.is_symlink() {
            bail!("staged Hugging Face import contains a symbolic link");
        }
        if file_type.is_dir() {
            continue;
        }
        if !file_type.is_file() {
            bail!("staged Hugging Face import contains a special file");
        }
        let path = repository_relative_path(root, entry.path())?;
        if allow_receipt && path == IMPORT_RECEIPT_NAME {
            continue;
        }
        actual_paths.insert(path);
    }
    let expected_paths: BTreeSet<_> = plan
        .files()
        .iter()
        .map(|file| file.path().to_owned())
        .collect();
    if actual_paths != expected_paths {
        bail!("staged Hugging Face import file set does not match the plan");
    }

    let mut imported = Vec::with_capacity(plan.files().len());
    let mut total_bytes = 0_u64;
    for source in plan.files() {
        let (size, content_id) = hash_imported_file(&root.join(source.path()))?;
        if size != source.size() {
            bail!("staged Hugging Face file size does not match the plan");
        }
        total_bytes = total_bytes
            .checked_add(size)
            .context("staged Hugging Face byte total overflow")?;
        imported.push(HfImportedFile::new(
            source.path().to_owned(),
            size,
            content_id,
        )?);
    }
    if total_bytes != plan.summary().total_bytes {
        bail!("staged Hugging Face byte total does not match the plan");
    }
    Ok(imported)
}

fn repository_relative_path(root: &Path, path: &Path) -> Result<String> {
    let relative = path
        .strip_prefix(root)
        .context("staged Hugging Face path escaped its root")?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(value) => parts.push(
                value
                    .to_str()
                    .context("staged Hugging Face path must be valid UTF-8")?,
            ),
            _ => bail!("staged Hugging Face path must be canonical and relative"),
        }
    }
    Ok(parts.join("/"))
}

fn hash_imported_file(path: &Path) -> Result<(u64, String)> {
    let metadata = fs::symlink_metadata(path).context("cannot inspect staged Hugging Face file")?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("staged Hugging Face path must be a regular file");
    }
    let mut file = File::open(path).context("cannot open staged Hugging Face file")?;
    let mut hasher = blake3::Hasher::new();
    let mut size = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        size = size
            .checked_add(u64::try_from(read)?)
            .context("staged Hugging Face file size overflow")?;
    }
    Ok((size, format!("file_{}", hasher.finalize().to_hex())))
}

fn validate_existing_import(plan: &HfImportPlan, output: &Path) -> Result<HfImportReceipt> {
    let metadata = fs::symlink_metadata(output)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("existing Hugging Face import output is not a real directory");
    }
    let receipt_path = output.join(IMPORT_RECEIPT_NAME);
    let receipt_metadata = fs::symlink_metadata(&receipt_path)?;
    if receipt_metadata.file_type().is_symlink() || !receipt_metadata.is_file() {
        bail!("existing Hugging Face import receipt is not a regular file");
    }
    if receipt_metadata.len() > MAX_RECEIPT_JSON_BYTES as u64 {
        bail!("Hugging Face import receipt is too large");
    }
    let mut payload = String::new();
    File::open(&receipt_path)?
        .take(MAX_RECEIPT_JSON_BYTES as u64 + 1)
        .read_to_string(&mut payload)?;
    let receipt = HfImportReceipt::from_json(&payload)?;
    if receipt.plan_id() != plan.plan_id()
        || receipt.repo_id() != plan.repo_id()
        || receipt.resolved_commit() != plan.resolved_commit()
        || receipt.output() != plan.output()
    {
        bail!("existing Hugging Face import receipt does not match the plan");
    }
    let files = verify_import_tree(output, plan, true)?;
    if files != receipt.files {
        bail!("existing Hugging Face import bytes do not match the receipt");
    }
    Ok(receipt)
}

pub(crate) fn load_hf_import_receipt(path: &Path) -> Result<HfImportReceipt> {
    let metadata =
        fs::symlink_metadata(path).context("cannot inspect Hugging Face import receipt")?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("Hugging Face import receipt must be a regular file");
    }
    if metadata.len() > MAX_RECEIPT_JSON_BYTES as u64 {
        bail!("Hugging Face import receipt is too large");
    }
    let canonical_path = path
        .canonicalize()
        .context("cannot resolve Hugging Face import receipt")?;
    let mut payload = String::new();
    File::open(&canonical_path)?
        .take(MAX_RECEIPT_JSON_BYTES as u64 + 1)
        .read_to_string(&mut payload)
        .context("Hugging Face import receipt must contain valid UTF-8")?;
    if payload.len() > MAX_RECEIPT_JSON_BYTES {
        bail!("Hugging Face import receipt is too large");
    }
    let receipt = HfImportReceipt::from_json(&payload)?;
    let root = canonical_path
        .parent()
        .context("Hugging Face import receipt has no parent")?;
    if Path::new(receipt.output()) != root {
        bail!("Hugging Face import receipt output does not match its directory");
    }
    Ok(receipt)
}

fn sync_import_tree(root: &Path) -> Result<()> {
    let mut directories = Vec::new();
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = entry.context("cannot sync Hugging Face import")?;
        if entry.file_type().is_file() {
            File::open(entry.path())?.sync_all()?;
        } else if entry.file_type().is_dir() {
            directories.push(entry.path().to_owned());
        }
    }
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}

#[cfg(unix)]
fn publish_import_staging(mut staging: ImportStaging, output: &Path) -> Result<()> {
    if let Err(error) = rustix::fs::renameat_with(
        rustix::fs::CWD,
        &staging.path,
        rustix::fs::CWD,
        output,
        rustix::fs::RenameFlags::NOREPLACE,
    ) {
        if fs::symlink_metadata(output).is_ok() {
            return Err(OutputExistsError.into());
        }
        return Err(error).context("cannot publish Hugging Face import atomically");
    }
    staging.cleanup = false;
    File::open(
        output
            .parent()
            .context("Hugging Face import output has no parent")?,
    )?
    .sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn publish_import_staging(_staging: ImportStaging, _output: &Path) -> Result<()> {
    bail!("native Hugging Face import publication currently requires Unix")
}

fn apply_artifact(
    plan: &HfImportPlan,
    receipt: &HfImportReceipt,
    outcome: &str,
) -> Result<HfImportApplyArtifact> {
    let output = PathBuf::from(plan.output());
    Ok(HfImportApplyArtifact {
        schema_version: receipt.schema_version(),
        outcome: outcome.to_owned(),
        import_id: receipt.import_id().to_owned(),
        plan_id: plan.plan_id().to_owned(),
        repo_id: plan.repo_id().to_owned(),
        resolved_commit: plan.resolved_commit().to_owned(),
        total_files: receipt.summary().total_files,
        total_bytes: receipt.summary().total_bytes,
        output: path_text(&output, "output")?,
        receipt: path_text(&output.join(IMPORT_RECEIPT_NAME), "receipt")?,
    })
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HfImportFile {
    oid: String,
    path: String,
    size: u64,
}

impl HfImportFile {
    pub fn new(path: String, size: u64, oid: String) -> Result<Self> {
        validate_repository_path(&path)?;
        validate_hex_digest(&oid, &[40, 64], "Hub object ID")?;
        Ok(Self { oid, path, size })
    }

    pub fn oid(&self) -> &str {
        &self.oid
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    fn validate(&self) -> Result<()> {
        validate_repository_path(&self.path)?;
        validate_hex_digest(&self.oid, &[40, 64], "Hub object ID")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HfImportSummary {
    pub total_bytes: u64,
    pub total_files: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HfImportPlan {
    files: Vec<HfImportFile>,
    ignores: Vec<String>,
    includes: Vec<String>,
    kind: String,
    output: String,
    plan_id: String,
    repo_id: String,
    requested_revision: String,
    resolved_commit: String,
    schema_version: u8,
    summary: HfImportSummary,
}

impl HfImportPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        repo_id: String,
        requested_revision: String,
        resolved_commit: String,
        mut includes: Vec<String>,
        mut ignores: Vec<String>,
        output: String,
        mut files: Vec<HfImportFile>,
    ) -> Result<Self> {
        validate_repo_id(&repo_id)?;
        validate_requested_revision(&requested_revision)?;
        validate_resolved_commit(&resolved_commit)?;
        canonicalize_patterns(&mut includes, "include")?;
        canonicalize_patterns(&mut ignores, "ignore")?;
        validate_output(&output)?;
        files.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
        validate_source_files(&files)?;
        let summary = source_summary(&files)?;
        let mut plan = Self {
            files,
            ignores,
            includes,
            kind: PLAN_KIND.to_owned(),
            output,
            plan_id: String::new(),
            repo_id,
            requested_revision,
            resolved_commit,
            schema_version: HF_IMPORT_PLAN_SCHEMA_VERSION,
            summary,
        };
        plan.plan_id = plan.compute_id()?;
        Ok(plan)
    }

    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_PLAN_JSON_BYTES {
            bail!("Hugging Face import plan is too large");
        }
        let plan: Self =
            serde_json::from_str(payload).context("invalid Hugging Face import plan JSON")?;
        plan.validate()?;
        Ok(plan)
    }

    pub fn to_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string_pretty(self).context("cannot serialize Hugging Face import plan")
    }

    pub fn compute_id(&self) -> Result<String> {
        let payload = PlanIdentity {
            files: &self.files,
            ignores: &self.ignores,
            includes: &self.includes,
            kind: &self.kind,
            output: &self.output,
            repo_id: &self.repo_id,
            requested_revision: &self.requested_revision,
            resolved_commit: &self.resolved_commit,
            schema_version: self.schema_version,
            summary: &self.summary,
        };
        let encoded = serde_json::to_vec(&payload).context("cannot encode import plan identity")?;
        Ok(blake3_content_id("hfplan", PLAN_IDENTITY_DOMAIN, &encoded))
    }

    pub fn files(&self) -> &[HfImportFile] {
        &self.files
    }

    pub fn ignores(&self) -> &[String] {
        &self.ignores
    }

    pub fn includes(&self) -> &[String] {
        &self.includes
    }

    pub fn output(&self) -> &str {
        &self.output
    }

    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }

    pub fn repo_id(&self) -> &str {
        &self.repo_id
    }

    pub fn requested_revision(&self) -> &str {
        &self.requested_revision
    }

    pub fn resolved_commit(&self) -> &str {
        &self.resolved_commit
    }

    pub fn schema_version(&self) -> u8 {
        self.schema_version
    }

    pub fn summary(&self) -> &HfImportSummary {
        &self.summary
    }

    fn validate(&self) -> Result<()> {
        if self.kind != PLAN_KIND {
            bail!("invalid Hugging Face import plan kind");
        }
        if self.schema_version != HF_IMPORT_PLAN_SCHEMA_VERSION {
            bail!(
                "unsupported Hugging Face import plan schema {}",
                self.schema_version
            );
        }
        validate_repo_id(&self.repo_id)?;
        validate_requested_revision(&self.requested_revision)?;
        validate_resolved_commit(&self.resolved_commit)?;
        validate_canonical_patterns(&self.includes, "include")?;
        validate_canonical_patterns(&self.ignores, "ignore")?;
        validate_output(&self.output)?;
        validate_source_files(&self.files)?;
        if self.summary != source_summary(&self.files)? {
            bail!("Hugging Face import plan summary does not match files");
        }
        if self.plan_id != self.compute_id()? {
            bail!("plan_id does not match Hugging Face import plan");
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct PlanIdentity<'a> {
    files: &'a [HfImportFile],
    ignores: &'a [String],
    includes: &'a [String],
    kind: &'a str,
    output: &'a str,
    repo_id: &'a str,
    requested_revision: &'a str,
    resolved_commit: &'a str,
    schema_version: u8,
    summary: &'a HfImportSummary,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HfImportedFile {
    content_id: String,
    path: String,
    size: u64,
}

impl HfImportedFile {
    pub fn new(path: String, size: u64, content_id: String) -> Result<Self> {
        validate_repository_path(&path)?;
        validate_content_id(&content_id, "file", "imported file content ID")?;
        Ok(Self {
            content_id,
            path,
            size,
        })
    }

    pub fn content_id(&self) -> &str {
        &self.content_id
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    fn validate(&self) -> Result<()> {
        validate_repository_path(&self.path)?;
        validate_content_id(&self.content_id, "file", "imported file content ID")
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HfImportReceipt {
    files: Vec<HfImportedFile>,
    import_id: String,
    kind: String,
    output: String,
    plan_id: String,
    repo_id: String,
    resolved_commit: String,
    schema_version: u8,
    summary: HfImportSummary,
}

impl HfImportReceipt {
    pub fn create(plan: &HfImportPlan, mut files: Vec<HfImportedFile>) -> Result<Self> {
        plan.validate()?;
        files.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
        validate_imported_files(&files)?;
        ensure_receipt_matches_plan(plan, &files)?;
        let summary = imported_summary(&files)?;
        let mut receipt = Self {
            files,
            import_id: String::new(),
            kind: RECEIPT_KIND.to_owned(),
            output: plan.output.clone(),
            plan_id: plan.plan_id.clone(),
            repo_id: plan.repo_id.clone(),
            resolved_commit: plan.resolved_commit.clone(),
            schema_version: HF_IMPORT_RECEIPT_SCHEMA_VERSION,
            summary,
        };
        receipt.import_id = receipt.compute_id()?;
        Ok(receipt)
    }

    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_RECEIPT_JSON_BYTES {
            bail!("Hugging Face import receipt is too large");
        }
        let receipt: Self =
            serde_json::from_str(payload).context("invalid Hugging Face import receipt JSON")?;
        receipt.validate()?;
        Ok(receipt)
    }

    pub fn to_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string_pretty(self).context("cannot serialize Hugging Face import receipt")
    }

    pub fn compute_id(&self) -> Result<String> {
        let payload = ReceiptIdentity {
            files: &self.files,
            kind: &self.kind,
            output: &self.output,
            plan_id: &self.plan_id,
            repo_id: &self.repo_id,
            resolved_commit: &self.resolved_commit,
            schema_version: self.schema_version,
            summary: &self.summary,
        };
        let encoded =
            serde_json::to_vec(&payload).context("cannot encode import receipt identity")?;
        Ok(blake3_content_id(
            "hfimport",
            RECEIPT_IDENTITY_DOMAIN,
            &encoded,
        ))
    }

    pub fn files(&self) -> &[HfImportedFile] {
        &self.files
    }

    pub fn import_id(&self) -> &str {
        &self.import_id
    }

    pub fn output(&self) -> &str {
        &self.output
    }

    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }

    pub fn repo_id(&self) -> &str {
        &self.repo_id
    }

    pub fn resolved_commit(&self) -> &str {
        &self.resolved_commit
    }

    pub fn schema_version(&self) -> u8 {
        self.schema_version
    }

    pub fn summary(&self) -> &HfImportSummary {
        &self.summary
    }

    fn validate(&self) -> Result<()> {
        if self.kind != RECEIPT_KIND {
            bail!("invalid Hugging Face import receipt kind");
        }
        if self.schema_version != HF_IMPORT_RECEIPT_SCHEMA_VERSION {
            bail!(
                "unsupported Hugging Face import receipt schema {}",
                self.schema_version
            );
        }
        validate_content_id(&self.plan_id, "hfplan", "plan ID")?;
        validate_repo_id(&self.repo_id)?;
        validate_resolved_commit(&self.resolved_commit)?;
        validate_output(&self.output)?;
        validate_imported_files(&self.files)?;
        if self.summary != imported_summary(&self.files)? {
            bail!("Hugging Face import receipt summary does not match files");
        }
        if self.import_id != self.compute_id()? {
            bail!("import_id does not match Hugging Face import receipt");
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct ReceiptIdentity<'a> {
    files: &'a [HfImportedFile],
    kind: &'a str,
    output: &'a str,
    plan_id: &'a str,
    repo_id: &'a str,
    resolved_commit: &'a str,
    schema_version: u8,
    summary: &'a HfImportSummary,
}

fn validate_repo_id(repo_id: &str) -> Result<()> {
    let mut parts = repo_id.split('/');
    let owner = parts.next().unwrap_or_default();
    let name = parts.next().unwrap_or_default();
    if owner.is_empty() || name.is_empty() || parts.next().is_some() {
        bail!("Hugging Face repository ID must be exactly owner/name");
    }
    Ok(())
}

fn validate_requested_revision(revision: &str) -> Result<()> {
    if revision.is_empty()
        || revision.contains('\0')
        || revision.len() > MAX_HF_IMPORT_PATTERN_BYTES
    {
        bail!("requested Hugging Face revision must be non-empty and bounded");
    }
    Ok(())
}

fn validate_resolved_commit(commit: &str) -> Result<()> {
    validate_hex_digest(commit, &[40], "resolved Hugging Face commit")
}

fn validate_output(output: &str) -> Result<()> {
    if output.is_empty() || output.contains('\0') {
        bail!("Hugging Face import output path must be non-empty");
    }
    Ok(())
}

fn canonicalize_patterns(patterns: &mut Vec<String>, label: &str) -> Result<()> {
    validate_pattern_values(patterns, label)?;
    patterns.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    patterns.dedup();
    Ok(())
}

fn validate_canonical_patterns(patterns: &[String], label: &str) -> Result<()> {
    validate_pattern_values(patterns, label)?;
    if patterns
        .windows(2)
        .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
    {
        bail!("Hugging Face {label} patterns must be sorted and unique");
    }
    Ok(())
}

fn validate_pattern_values(patterns: &[String], label: &str) -> Result<()> {
    if patterns.len() > MAX_HF_IMPORT_PATTERNS {
        bail!("Hugging Face {label} patterns exceed {MAX_HF_IMPORT_PATTERNS}");
    }
    for pattern in patterns {
        if pattern.is_empty()
            || pattern.contains('\0')
            || pattern.len() > MAX_HF_IMPORT_PATTERN_BYTES
        {
            bail!(
                "Hugging Face {label} pattern must be non-empty and at most {MAX_HF_IMPORT_PATTERN_BYTES} UTF-8 bytes"
            );
        }
    }
    Ok(())
}

fn validate_source_files(files: &[HfImportFile]) -> Result<()> {
    validate_file_count_and_order(files.iter().map(|file| file.path.as_str()))?;
    for file in files {
        file.validate()?;
    }
    source_summary(files)?;
    Ok(())
}

fn validate_imported_files(files: &[HfImportedFile]) -> Result<()> {
    validate_file_count_and_order(files.iter().map(|file| file.path.as_str()))?;
    for file in files {
        file.validate()?;
    }
    imported_summary(files)?;
    Ok(())
}

fn validate_file_count_and_order<'a>(paths: impl Iterator<Item = &'a str>) -> Result<()> {
    let paths: Vec<_> = paths.collect();
    if paths.is_empty() {
        bail!("Hugging Face import selection must not be empty");
    }
    if paths.len() > MAX_HF_IMPORT_FILES {
        bail!("Hugging Face import exceeds {MAX_HF_IMPORT_FILES} files");
    }
    if paths
        .windows(2)
        .any(|pair| pair[0].as_bytes() >= pair[1].as_bytes())
    {
        bail!("Hugging Face import files must be sorted by unique path");
    }
    Ok(())
}

fn source_summary(files: &[HfImportFile]) -> Result<HfImportSummary> {
    summarize(files.iter().map(|file| file.size))
}

fn imported_summary(files: &[HfImportedFile]) -> Result<HfImportSummary> {
    summarize(files.iter().map(|file| file.size))
}

fn summarize(sizes: impl Iterator<Item = u64>) -> Result<HfImportSummary> {
    let mut total_bytes = 0_u64;
    let mut total_files = 0_usize;
    for size in sizes {
        total_bytes = total_bytes
            .checked_add(size)
            .context("Hugging Face import byte total overflow")?;
        total_files += 1;
    }
    if total_bytes > MAX_HF_IMPORT_BYTES {
        bail!("Hugging Face import exceeds {MAX_HF_IMPORT_BYTES} bytes");
    }
    Ok(HfImportSummary {
        total_bytes,
        total_files,
    })
}

fn ensure_receipt_matches_plan(plan: &HfImportPlan, files: &[HfImportedFile]) -> Result<()> {
    if plan.files.len() != files.len()
        || plan
            .files
            .iter()
            .zip(files)
            .any(|(source, imported)| source.path != imported.path || source.size != imported.size)
    {
        bail!("Hugging Face import receipt files do not match the accepted plan");
    }
    Ok(())
}

fn validate_repository_path(path: &str) -> Result<()> {
    if path == IMPORT_RECEIPT_NAME {
        bail!("Hugging Face repository path is reserved for the import receipt");
    }
    if path.is_empty()
        || path.contains('\\')
        || path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        bail!("Hugging Face repository path must be canonical and relative");
    }
    let candidate = Path::new(path);
    if candidate.is_absolute()
        || candidate.components().any(|component| {
            !matches!(component, Component::Normal(_)) || component.as_os_str().to_str().is_none()
        })
    {
        bail!("Hugging Face repository path must be canonical and relative");
    }
    if path.len() > MAX_HF_IMPORT_PATH_BYTES {
        bail!("Hugging Face repository path exceeds {MAX_HF_IMPORT_PATH_BYTES} UTF-8 bytes");
    }
    Ok(())
}

fn ensure_paths_are_separate(output: &Path, plan: &Path) -> Result<()> {
    if output.starts_with(plan) || plan.starts_with(output) {
        bail!("Hugging Face import output and plan paths must not contain one another");
    }
    Ok(())
}

fn validate_content_id(value: &str, prefix: &str, label: &str) -> Result<()> {
    let expected_prefix = format!("{prefix}_");
    let digest = value
        .strip_prefix(&expected_prefix)
        .with_context(|| format!("{label} must start with {expected_prefix}"))?;
    validate_hex_digest(digest, &[64], label)
}

fn validate_hex_digest(value: &str, lengths: &[usize], label: &str) -> Result<()> {
    if !lengths.contains(&value.len())
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("{label} must be lowercase hexadecimal with a supported length");
    }
    Ok(())
}

fn build_glob_set(patterns: &[String], label: &str) -> Result<GlobSet> {
    validate_pattern_values(patterns, label)?;
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let glob =
            Glob::new(pattern).with_context(|| format!("invalid Hugging Face {label} pattern"))?;
        builder.add(glob);
    }
    builder
        .build()
        .with_context(|| format!("invalid Hugging Face {label} pattern set"))
}

fn has_default_import_extension(path: &str) -> bool {
    Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| DEFAULT_IMPORT_EXTENSIONS.contains(&extension))
}

fn resolve_new_import_output(path: &Path) -> Result<PathBuf> {
    let absolute = lexical_normalize(std::path::absolute(path)?);
    let mut existing = absolute.as_path();
    let mut missing = Vec::new();
    while !existing.exists() {
        missing.push(
            existing
                .file_name()
                .context("Hugging Face import output has no file name")?
                .to_os_string(),
        );
        existing = existing
            .parent()
            .context("cannot resolve Hugging Face import output")?;
    }
    let mut resolved = existing
        .canonicalize()
        .context("cannot resolve Hugging Face import output")?;
    for component in missing.into_iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn resolve_new_plan_path(path: &Path) -> Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).context("cannot create Hugging Face import plan directory")?;
    let parent = parent
        .canonicalize()
        .context("cannot resolve Hugging Face import plan directory")?;
    let name = path
        .file_name()
        .context("Hugging Face import plan has no file name")?;
    Ok(parent.join(name))
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

fn path_text(path: &Path, label: &str) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .with_context(|| format!("Hugging Face import {label} path must be valid UTF-8"))
}

fn sanitized_hub_error(context: &str, error: impl std::fmt::Display) -> anyhow::Error {
    static HF_TOKEN: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"hf_[A-Za-z0-9_-]+").expect("Hugging Face token pattern is valid")
    });
    let error = error.to_string();
    let message = HF_TOKEN.replace_all(&error, "[REDACTED]");
    anyhow::anyhow!("{context}: {message}")
}

fn next_link(headers: &ureq::http::HeaderMap) -> Result<Option<String>> {
    let Some(value) = headers.get("link") else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .context("Hugging Face pagination link must be valid text")?;
    for item in value.split(',') {
        let mut parts = item.split(';');
        let target = parts.next().unwrap_or_default().trim();
        let is_next = parts.any(|part| part.trim() == "rel=\"next\"");
        if is_next {
            let url = target
                .strip_prefix('<')
                .and_then(|value| value.strip_suffix('>'))
                .context("Hugging Face pagination link is malformed")?;
            Url::parse(url).context("Hugging Face pagination link is not a valid URL")?;
            return Ok(Some(url.to_owned()));
        }
    }
    Ok(None)
}

fn validate_pagination_url(endpoint: &Url, candidate: Url) -> Result<Url> {
    if endpoint.scheme() != candidate.scheme()
        || endpoint.host_str() != candidate.host_str()
        || endpoint.port_or_known_default() != candidate.port_or_known_default()
    {
        bail!("Hugging Face pagination link changed origin");
    }
    Ok(candidate)
}

fn hugging_face_token() -> Result<Option<String>> {
    if let Ok(token) = std::env::var("HF_TOKEN") {
        return Ok(nonempty_token(token));
    }
    let token_path = if let Ok(path) = std::env::var("HF_TOKEN_PATH") {
        Some(PathBuf::from(path))
    } else if let Ok(home) = std::env::var("HF_HOME") {
        Some(PathBuf::from(home).join("token"))
    } else if let Ok(cache) = std::env::var("XDG_CACHE_HOME") {
        Some(PathBuf::from(cache).join("huggingface/token"))
    } else {
        std::env::var("HOME")
            .ok()
            .map(|home| PathBuf::from(home).join(".cache/huggingface/token"))
    };
    let Some(path) = token_path.filter(|path| path.is_file()) else {
        return Ok(None);
    };
    let token = fs::read_to_string(&path).context("cannot read Hugging Face token file")?;
    Ok(nonempty_token(token))
}

fn nonempty_token(token: String) -> Option<String> {
    let token = token.trim();
    (!token.is_empty()).then(|| token.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{
        HubTreeEntry, copy_hub_file, dataset_tree_url, hub_http_agent,
        validate_hub_endpoint_security, validate_pagination_url,
    };
    use std::io::Cursor;
    use ureq::tls::{RootCerts, TlsProvider};
    use url::Url;

    #[test]
    fn hub_http_agent_uses_platform_certificate_verification() {
        let agent = hub_http_agent();
        let tls = agent.config().tls_config();

        assert_eq!(TlsProvider::Rustls, tls.provider());
        assert!(matches!(tls.root_certs(), RootCerts::PlatformVerifier));
        assert!(!tls.disable_verification());
    }

    #[test]
    fn hub_endpoint_never_sends_tokens_over_plain_http() {
        assert!(
            validate_hub_endpoint_security(&Url::parse("https://example.com").unwrap(), true)
                .is_ok()
        );
        assert!(
            validate_hub_endpoint_security(&Url::parse("http://127.0.0.1:8080").unwrap(), false)
                .is_ok()
        );
        assert!(
            validate_hub_endpoint_security(&Url::parse("http://127.0.0.1:8080").unwrap(), true)
                .is_err()
        );
        assert!(
            validate_hub_endpoint_security(&Url::parse("http://example.com").unwrap(), false)
                .is_err()
        );
    }

    #[test]
    fn hub_file_copy_stops_one_byte_beyond_the_declared_size() {
        let mut source = Cursor::new(vec![b'x'; 32]);
        let mut destination = Vec::new();

        let error = copy_hub_file(&mut source, &mut destination, 4).unwrap_err();

        assert!(error.to_string().contains("exceeded its planned size"));
        assert_eq!(5, destination.len());
    }

    #[test]
    fn dataset_tree_request_does_not_combine_expand_with_large_pagination() {
        let url = dataset_tree_url(
            "https://huggingface.co",
            "lhoestq/demo1",
            "87ecf163bedca9d80598b528940a9c4f99e14c11",
        )
        .unwrap();

        assert_eq!(Some("true"), query_value(&url, "recursive").as_deref());
        assert_eq!(Some("1000"), query_value(&url, "limit").as_deref());
        assert_eq!(None, query_value(&url, "expand"));
    }

    #[test]
    fn hub_tree_directory_entries_do_not_require_a_file_size() {
        let payload = r#"[
            {"type":"directory","oid":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","path":"data"},
            {"type":"file","oid":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","size":7,"path":"data/train.jsonl"}
        ]"#;

        let entries: Vec<HubTreeEntry> =
            serde_json::from_str(payload).expect("Hub directory entries should be accepted");

        assert_eq!(2, entries.len());
    }

    #[test]
    fn pagination_rejects_cross_origin_links_before_reusing_authentication() {
        let endpoint = Url::parse("https://huggingface.co").unwrap();
        let attacker = Url::parse("https://example.invalid/steal-token").unwrap();

        assert!(validate_pagination_url(&endpoint, attacker).is_err());
    }

    fn query_value(url: &Url, name: &str) -> Option<String> {
        url.query_pairs()
            .find_map(|(key, value)| (key == name).then(|| value.into_owned()))
    }
}
