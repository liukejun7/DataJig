#[cfg(unix)]
use crate::ConcurrentModificationError;
use crate::FileRecord;
use crate::file_inventory::normalize_file_records;
#[cfg(unix)]
use crate::{
    Candidate, InvalidArgumentError, atomic_write, enumerate, fingerprint, hash_candidate,
    reject_output_alias, safe_destination,
};
use crate::{MAX_MANIFEST_ENTRIES, MAX_PATH_BYTES};
#[cfg(unix)]
use anyhow::anyhow;
use anyhow::{Context, Result, bail};
#[cfg(unix)]
use cap_std::ambient_authority;
#[cfg(unix)]
use cap_std::fs::Dir;
#[cfg(unix)]
use image::imageops::FilterType;
#[cfg(unix)]
use image::{DynamicImage, ImageFormat, ImageReader, Limits};
#[cfg(unix)]
use rayon::prelude::*;
#[cfg(unix)]
use rustdct::DctPlanner;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::io::{self, Write};
#[cfg(unix)]
use std::io::{Cursor, Read};
use std::path::{Component, Path};
use std::sync::Arc;

pub const MAX_INVENTORY_BYTES: usize = 512 * 1024 * 1024;
pub const MAX_INVENTORY_THREADS: usize = 8;
#[cfg(unix)]
const MAX_MEDIA_BYTES: u64 = 32 * 1024 * 1024;
#[cfg(unix)]
const MAX_DECODE_BYTES: u64 = 64 * 1024 * 1024;
const INVENTORY_SCHEMA_VERSION_V1: u8 = 1;
const INVENTORY_SCHEMA_VERSION_V2: u8 = 2;
const IMAGEFOLDER_ADAPTER: &str = "imagefolder";
const COVERAGE_V1: &str = "supported_media_v1";
const COVERAGE_V2: &str = "all_files_v2";
const SUPPORTED_SUFFIXES: [&str; 4] = [".jpeg", ".jpg", ".png", ".webp"];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedImagePath {
    split: String,
    label: String,
    logical_path: String,
    supported: bool,
}

impl ParsedImagePath {
    pub fn split(&self) -> &str {
        &self.split
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn logical_path(&self) -> &str {
        &self.logical_path
    }

    pub fn supported(&self) -> bool {
        self.supported
    }
}

pub fn parse_imagefolder_path(path: &str) -> Result<ParsedImagePath> {
    validate_relative_path(path)?;
    let mut parts = path.split('/');
    let split = parts.next().unwrap_or_default();
    let label = parts.next().unwrap_or_default();
    let logical_parts = parts.collect::<Vec<_>>();
    if logical_parts.is_empty() {
        bail!("expected <split>/<label>/<file>: {path}");
    }
    if !matches!(split, "test" | "train" | "val") {
        bail!("unsupported split {split:?}; expected one of test, train, val");
    }
    if label.is_empty() {
        bail!("path must include a label and file: {path}");
    }
    let logical_path = logical_parts.join("/");
    let lower = logical_path.to_ascii_lowercase();
    let supported = SUPPORTED_SUFFIXES
        .iter()
        .any(|suffix| lower.ends_with(suffix));
    Ok(ParsedImagePath {
        split: split.to_owned(),
        label: label.to_owned(),
        logical_path,
        supported,
    })
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryRecord {
    pub channels: Option<u8>,
    pub content_hash: String,
    pub decode_error: Option<String>,
    pub height: Option<u32>,
    pub label: String,
    pub logical_path: String,
    pub media_format: Option<String>,
    pub mtime_ns: i128,
    pub perceptual_hash: Option<String>,
    pub relative_path: String,
    pub size: u64,
    pub snapshot: Arc<str>,
    pub split: String,
    pub width: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InventoryFinding {
    code: String,
    message: String,
    sample_ids: Vec<String>,
    severity: String,
}

impl InventoryFinding {
    pub fn new(
        code: impl Into<String>,
        severity: impl Into<String>,
        message: impl Into<String>,
        mut sample_ids: Vec<String>,
    ) -> Result<Self> {
        let code = code.into();
        let severity = severity.into();
        let message = message.into();
        if code.is_empty() || message.is_empty() {
            bail!("inventory finding code and message must not be empty");
        }
        if !matches!(severity.as_str(), "error" | "warning" | "info") {
            bail!("inventory finding severity is invalid");
        }
        for path in &sample_ids {
            validate_relative_path(path)?;
        }
        sample_ids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        sample_ids.dedup();
        Ok(Self {
            code,
            message,
            sample_ids,
            severity,
        })
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn sample_ids(&self) -> &[String] {
        &self.sample_ids
    }

    pub fn severity(&self) -> &str {
        &self.severity
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InventorySummary {
    enumerated_files: usize,
    failed_files: usize,
    findings: usize,
    records: usize,
    supported_files: usize,
    unsupported_files: usize,
}

impl InventorySummary {
    pub fn enumerated_files(&self) -> usize {
        self.enumerated_files
    }

    pub fn findings(&self) -> usize {
        self.findings
    }

    pub fn failed_files(&self) -> usize {
        self.failed_files
    }

    pub fn records(&self) -> usize {
        self.records
    }

    pub fn supported_files(&self) -> usize {
        self.supported_files
    }

    pub fn unsupported_files(&self) -> usize {
        self.unsupported_files
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetInventory {
    adapter: String,
    coverage: String,
    files: Vec<FileRecord>,
    findings: Vec<InventoryFinding>,
    records: Vec<InventoryRecord>,
    root: Arc<str>,
    schema_version: u8,
    summary: InventorySummary,
    unsupported_paths: Vec<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InventoryV1Wire {
    namespace: String,
    findings: Vec<InventoryFinding>,
    records: Vec<InventoryRecord>,
    root: Arc<str>,
    schema_version: u8,
    summary: InventorySummary,
    unsupported_paths: Vec<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct InventoryV2Wire {
    namespace: String,
    adapter: String,
    coverage: String,
    files: Vec<FileRecord>,
    findings: Vec<InventoryFinding>,
    records: Vec<InventoryRecord>,
    root: Arc<str>,
    schema_version: u8,
    summary: InventorySummary,
    unsupported_paths: Vec<String>,
}

#[derive(Serialize)]
struct InventoryV1Ref<'a> {
    namespace: &'static str,
    findings: &'a [InventoryFinding],
    records: &'a [InventoryRecord],
    root: &'a str,
    schema_version: u8,
    summary: &'a InventorySummary,
    unsupported_paths: &'a [String],
}

#[derive(Serialize)]
struct InventoryV2Ref<'a> {
    namespace: &'static str,
    adapter: &'a str,
    coverage: &'a str,
    files: &'a [FileRecord],
    findings: &'a [InventoryFinding],
    records: &'a [InventoryRecord],
    root: &'a str,
    schema_version: u8,
    summary: &'a InventorySummary,
    unsupported_paths: &'a [String],
}

impl DatasetInventory {
    pub fn from_parts(
        root: impl Into<Arc<str>>,
        mut records: Vec<InventoryRecord>,
        mut findings: Vec<InventoryFinding>,
        mut unsupported_paths: Vec<String>,
        enumerated_files: usize,
    ) -> Result<Self> {
        let root = root.into();
        if root.is_empty() {
            bail!("inventory root must not be empty");
        }
        if enumerated_files > MAX_MANIFEST_ENTRIES {
            bail!("inventory exceeds {MAX_MANIFEST_ENTRIES} entries");
        }
        records.sort_by(|left, right| {
            left.relative_path
                .as_bytes()
                .cmp(right.relative_path.as_bytes())
        });
        for record in &records {
            validate_record(record)?;
        }
        if records
            .windows(2)
            .any(|pair| pair[0].relative_path == pair[1].relative_path)
        {
            bail!("inventory record paths must be unique");
        }
        unsupported_paths.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        for path in &unsupported_paths {
            validate_relative_path(path)?;
        }
        if unsupported_paths.windows(2).any(|pair| pair[0] == pair[1]) {
            bail!("unsupported inventory paths must be unique");
        }
        let supported = records
            .iter()
            .map(|record| record.relative_path.as_str())
            .collect::<HashSet<_>>();
        if unsupported_paths
            .iter()
            .any(|path| supported.contains(path.as_str()))
        {
            bail!("inventory paths cannot be both supported and unsupported");
        }
        if records.len().saturating_add(unsupported_paths.len()) > enumerated_files {
            bail!("inventory summary is inconsistent with its entries");
        }
        findings.sort_by(|left, right| {
            (&left.code, &left.sample_ids, &left.message).cmp(&(
                &right.code,
                &right.sample_ids,
                &right.message,
            ))
        });
        let summary = InventorySummary {
            enumerated_files,
            failed_files: findings
                .iter()
                .filter(|finding| {
                    matches!(
                        finding.code.as_str(),
                        "IMAGE_DECODE_FAILED" | "INVALID_LAYOUT"
                    )
                })
                .count(),
            findings: findings.len(),
            records: records.len(),
            supported_files: records.len(),
            unsupported_files: unsupported_paths.len(),
        };
        let files = records
            .iter()
            .map(|record| {
                FileRecord::new(
                    record.relative_path.clone(),
                    record.size,
                    record.content_hash.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            adapter: IMAGEFOLDER_ADAPTER.into(),
            coverage: COVERAGE_V1.into(),
            files,
            findings,
            records,
            root,
            schema_version: INVENTORY_SCHEMA_VERSION_V1,
            summary,
            unsupported_paths,
        })
    }

    pub fn from_parts_v2(
        root: impl Into<Arc<str>>,
        files: Vec<FileRecord>,
        records: Vec<InventoryRecord>,
        findings: Vec<InventoryFinding>,
        unsupported_paths: Vec<String>,
        enumerated_files: usize,
    ) -> Result<Self> {
        let files = normalize_file_records(files)?;
        if files.len() != enumerated_files {
            bail!("schema-2 inventory must contain one file fact per enumerated file");
        }
        let mut value =
            Self::from_parts(root, records, findings, unsupported_paths, enumerated_files)?;
        let by_path = files
            .iter()
            .map(|file| (file.relative_path(), file))
            .collect::<std::collections::HashMap<_, _>>();
        for record in &value.records {
            let file = by_path
                .get(record.relative_path.as_str())
                .context("media record is missing its generic file fact")?;
            if file.size() != record.size || file.content_hash() != record.content_hash {
                bail!("media record does not match its generic file fact");
            }
        }
        for path in &value.unsupported_paths {
            if !by_path.contains_key(path.as_str()) {
                bail!("unsupported path is missing its generic file fact");
            }
        }
        if value.records.len() + value.unsupported_paths.len() != files.len() {
            bail!("every schema-2 file must have exactly one adapter classification");
        }
        value.adapter = IMAGEFOLDER_ADAPTER.into();
        value.coverage = COVERAGE_V2.into();
        value.files = files;
        value.schema_version = INVENTORY_SCHEMA_VERSION_V2;
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        let size = self.serialized_size()?;
        if size > MAX_INVENTORY_BYTES {
            bail!("inventory exceeds {MAX_INVENTORY_BYTES} bytes");
        }
        let mut payload = Vec::with_capacity(size);
        self.write_json_pretty(&mut payload)?;
        String::from_utf8(payload).map_err(Into::into)
    }

    pub fn serialized_size(&self) -> Result<usize> {
        let mut counter = CountingWriter(0);
        self.write_json_pretty(&mut counter)?;
        Ok(counter.0)
    }

    fn write_json_pretty(&self, writer: &mut impl Write) -> Result<()> {
        if self.schema_version == INVENTORY_SCHEMA_VERSION_V1 {
            serde_json::to_writer_pretty(
                writer,
                &InventoryV1Ref {
                    namespace: crate::identity::ARTIFACT_NAMESPACE,
                    findings: &self.findings,
                    records: &self.records,
                    root: &self.root,
                    schema_version: self.schema_version,
                    summary: &self.summary,
                    unsupported_paths: &self.unsupported_paths,
                },
            )?;
        } else {
            serde_json::to_writer_pretty(
                writer,
                &InventoryV2Ref {
                    namespace: crate::identity::ARTIFACT_NAMESPACE,
                    adapter: &self.adapter,
                    coverage: &self.coverage,
                    files: &self.files,
                    findings: &self.findings,
                    records: &self.records,
                    root: &self.root,
                    schema_version: self.schema_version,
                    summary: &self.summary,
                    unsupported_paths: &self.unsupported_paths,
                },
            )?;
        }
        Ok(())
    }

    pub fn schema_version(&self) -> u8 {
        self.schema_version
    }

    pub fn adapter(&self) -> &str {
        &self.adapter
    }

    pub fn coverage(&self) -> &str {
        &self.coverage
    }

    pub fn files(&self) -> &[FileRecord] {
        &self.files
    }

    pub fn summary(&self) -> &InventorySummary {
        &self.summary
    }

    pub fn root(&self) -> &str {
        &self.root
    }

    pub fn records(&self) -> &[InventoryRecord] {
        &self.records
    }

    pub fn findings(&self) -> &[InventoryFinding] {
        &self.findings
    }

    pub fn unsupported_paths(&self) -> &[String] {
        &self.unsupported_paths
    }

    pub fn content_id(&self) -> Result<String> {
        #[derive(Serialize)]
        struct Identity<'a> {
            findings: &'a [InventoryFinding],
            records: Vec<IdentityRecord<'a>>,
            schema_version: u8,
            unsupported_paths: &'a [String],
        }

        #[derive(Serialize)]
        struct IdentityRecord<'a> {
            channels: Option<u8>,
            content_hash: &'a str,
            decode_error: Option<&'a str>,
            height: Option<u32>,
            label: &'a str,
            logical_path: &'a str,
            media_format: Option<&'a str>,
            perceptual_hash: Option<&'a str>,
            relative_path: &'a str,
            size: u64,
            split: &'a str,
            width: Option<u32>,
        }

        let records = self
            .records
            .iter()
            .map(|record| IdentityRecord {
                channels: record.channels,
                content_hash: &record.content_hash,
                decode_error: record.decode_error.as_deref(),
                height: record.height,
                label: &record.label,
                logical_path: &record.logical_path,
                media_format: record.media_format.as_deref(),
                perceptual_hash: record.perceptual_hash.as_deref(),
                relative_path: &record.relative_path,
                size: record.size,
                split: &record.split,
                width: record.width,
            })
            .collect::<Vec<_>>();
        let mut hasher = blake3::Hasher::new();
        if self.schema_version == INVENTORY_SCHEMA_VERSION_V1 {
            hasher.update(b"datajig-inventory-v1\0");
            serde_json::to_writer(
                &mut hasher,
                &Identity {
                    findings: &self.findings,
                    records,
                    schema_version: self.schema_version,
                    unsupported_paths: &self.unsupported_paths,
                },
            )?;
        } else {
            #[derive(Serialize)]
            struct IdentityV2<'a> {
                adapter: &'a str,
                coverage: &'a str,
                files: &'a [FileRecord],
                findings: &'a [InventoryFinding],
                records: Vec<IdentityRecord<'a>>,
                schema_version: u8,
                unsupported_paths: &'a [String],
            }
            hasher.update(b"datajig-inventory-v2\0");
            serde_json::to_writer(
                &mut hasher,
                &IdentityV2 {
                    adapter: &self.adapter,
                    coverage: &self.coverage,
                    files: &self.files,
                    findings: &self.findings,
                    records,
                    schema_version: self.schema_version,
                    unsupported_paths: &self.unsupported_paths,
                },
            )?;
        }
        Ok(format!("inventory_{}", hasher.finalize().to_hex()))
    }

    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_INVENTORY_BYTES {
            bail!("inventory exceeds {MAX_INVENTORY_BYTES} bytes");
        }
        match serde_json::from_str::<InventoryV1Wire>(payload) {
            Ok(parsed) => {
                if parsed.namespace != crate::identity::ARTIFACT_NAMESPACE {
                    bail!("unsupported inventory namespace {}", parsed.namespace);
                }
                if parsed.schema_version != INVENTORY_SCHEMA_VERSION_V1 {
                    bail!("unsupported inventory schema {}", parsed.schema_version);
                }
                let validated = Self::from_parts(
                    Arc::clone(&parsed.root),
                    parsed.records.clone(),
                    parsed.findings.clone(),
                    parsed.unsupported_paths.clone(),
                    parsed.summary.enumerated_files,
                )?;
                if validated.findings != parsed.findings
                    || validated.records != parsed.records
                    || validated.root != parsed.root
                    || validated.summary != parsed.summary
                    || validated.unsupported_paths != parsed.unsupported_paths
                {
                    bail!("inventory summary or ordering is inconsistent");
                }
                Ok(validated)
            }
            Err(_) => {
                let parsed: InventoryV2Wire = serde_json::from_str(payload)
                    .context("invalid inventory JSON for every supported schema")?;
                if parsed.namespace != crate::identity::ARTIFACT_NAMESPACE {
                    bail!("unsupported inventory namespace {}", parsed.namespace);
                }
                if parsed.schema_version != INVENTORY_SCHEMA_VERSION_V2 {
                    bail!("unsupported inventory schema {}", parsed.schema_version);
                }
                let validated = Self::from_parts_v2(
                    Arc::clone(&parsed.root),
                    parsed.files.clone(),
                    parsed.records.clone(),
                    parsed.findings.clone(),
                    parsed.unsupported_paths.clone(),
                    parsed.summary.enumerated_files,
                )?;
                if validated.adapter != parsed.adapter
                    || validated.coverage != parsed.coverage
                    || validated.files != parsed.files
                    || validated.findings != parsed.findings
                    || validated.records != parsed.records
                    || validated.root != parsed.root
                    || validated.summary != parsed.summary
                    || validated.unsupported_paths != parsed.unsupported_paths
                {
                    bail!("inventory summary or ordering is inconsistent");
                }
                Ok(validated)
            }
        }
    }
}

pub fn load_inventory(path: &Path) -> Result<DatasetInventory> {
    let metadata = fs::metadata(path).context("cannot inspect inventory")?;
    if metadata.len() > MAX_INVENTORY_BYTES as u64 {
        bail!("inventory exceeds {MAX_INVENTORY_BYTES} bytes");
    }
    let payload = fs::read(path).context("cannot read inventory")?;
    let payload = String::from_utf8(payload).context("inventory is not valid UTF-8")?;
    DatasetInventory::from_json(&payload)
}

#[cfg(unix)]
pub fn scan_inventory(root: &Path, threads: usize) -> Result<DatasetInventory> {
    scan_inventory_impl(root, threads, None, INVENTORY_SCHEMA_VERSION_V2)
}

#[cfg(unix)]
pub fn scan_inventory_for_schema(
    root: &Path,
    threads: usize,
    schema_version: u8,
) -> Result<DatasetInventory> {
    if !matches!(
        schema_version,
        INVENTORY_SCHEMA_VERSION_V1 | INVENTORY_SCHEMA_VERSION_V2
    ) {
        bail!("unsupported inventory schema {schema_version}");
    }
    scan_inventory_impl(root, threads, None, schema_version)
}

#[cfg(unix)]
pub fn create_inventory(root: &Path, output: &Path, threads: usize) -> Result<DatasetInventory> {
    let inventory = scan_inventory_impl(root, threads, Some(output), INVENTORY_SCHEMA_VERSION_V2)?;
    let root = root.canonicalize().context("cannot resolve dataset root")?;
    let destination = safe_destination(output, &root)?;
    let payload = inventory.to_json()?;
    atomic_write(&destination, payload.as_bytes())?;
    Ok(inventory)
}

#[cfg(unix)]
fn scan_inventory_impl(
    root: &Path,
    threads: usize,
    output: Option<&Path>,
    schema_version: u8,
) -> Result<DatasetInventory> {
    if !(1..=MAX_INVENTORY_THREADS).contains(&threads) {
        return Err(InvalidArgumentError::new(format!(
            "threads must be between 1 and {MAX_INVENTORY_THREADS}"
        ))
        .into());
    }
    let root = root.canonicalize().context("cannot resolve dataset root")?;
    if !root.is_dir() {
        return Err(InvalidArgumentError::new("dataset root is not a directory").into());
    }
    let root_text: Arc<str> = root
        .to_str()
        .ok_or_else(|| anyhow!("dataset root is not valid UTF-8"))?
        .into();
    let root_directory =
        Dir::open_ambient_dir(&root, ambient_authority()).context("cannot open dataset root")?;
    let candidates = enumerate(&root, &root_directory)?;
    if let Some(output) = output {
        let destination = safe_destination(output, &root)?;
        reject_output_alias(&destination, &candidates)?;
    }

    let mut supported = Vec::new();
    let mut unsupported_paths = Vec::new();
    let mut generic_only = Vec::new();
    let mut findings = Vec::new();
    for candidate in &candidates {
        let lower = candidate.relative_path.to_ascii_lowercase();
        if !SUPPORTED_SUFFIXES
            .iter()
            .any(|suffix| lower.ends_with(suffix))
        {
            unsupported_paths.push(candidate.relative_path.clone());
            generic_only.push(candidate);
            continue;
        }
        match parse_imagefolder_path(&candidate.relative_path) {
            Ok(parsed) if parsed.supported() => supported.push((candidate, parsed)),
            Ok(_) => {
                unsupported_paths.push(candidate.relative_path.clone());
                generic_only.push(candidate);
            }
            Err(error) => {
                generic_only.push(candidate);
                if schema_version == INVENTORY_SCHEMA_VERSION_V2 {
                    unsupported_paths.push(candidate.relative_path.clone());
                }
                findings.push(InventoryFinding::new(
                    "INVALID_LAYOUT",
                    "error",
                    error.to_string(),
                    vec![candidate.relative_path.clone()],
                )?)
            }
        }
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .context("cannot create inventory thread pool")?;
    let probed = pool.install(|| {
        supported
            .par_iter()
            .map(|(candidate, parsed)| {
                probe_candidate(
                    &root_directory,
                    candidate,
                    parsed,
                    &root_text,
                    schema_version,
                )
            })
            .collect::<Result<Vec<_>>>()
    })?;
    let mut records = Vec::with_capacity(probed.len());
    for (record, record_findings) in probed {
        records.push(record);
        findings.extend(record_findings);
    }

    let files = if schema_version == INVENTORY_SCHEMA_VERSION_V2 {
        let mut files = records
            .iter()
            .map(|record| {
                FileRecord::new(
                    record.relative_path.clone(),
                    record.size,
                    record.content_hash.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        let generic_files = pool.install(|| {
            generic_only
                .par_iter()
                .map(|candidate| {
                    let entry = hash_candidate(&root_directory, candidate)?;
                    FileRecord::new(
                        entry.path().into(),
                        entry.size(),
                        entry.content_hash().into(),
                    )
                })
                .collect::<Result<Vec<_>>>()
        })?;
        files.extend(generic_files);
        files
    } else {
        Vec::new()
    };

    let final_candidates = enumerate(&root, &root_directory)?;
    if candidates != final_candidates {
        return Err(ConcurrentModificationError::new(
            "dataset membership or metadata changed during inventory",
        )
        .into());
    }
    if schema_version == INVENTORY_SCHEMA_VERSION_V2 {
        DatasetInventory::from_parts_v2(
            root_text,
            files,
            records,
            findings,
            unsupported_paths,
            candidates.len(),
        )
    } else {
        DatasetInventory::from_parts(
            root_text,
            records,
            findings,
            unsupported_paths,
            candidates.len(),
        )
    }
}

#[cfg(not(unix))]
pub fn create_inventory(_root: &Path, _output: &Path, _threads: usize) -> Result<DatasetInventory> {
    bail!("the experimental Rust inventory backend currently requires Unix")
}

#[cfg(not(unix))]
pub fn scan_inventory(_root: &Path, _threads: usize) -> Result<DatasetInventory> {
    bail!("the experimental Rust inventory backend currently requires Unix")
}

#[cfg(not(unix))]
pub fn scan_inventory_for_schema(
    _root: &Path,
    _threads: usize,
    _schema_version: u8,
) -> Result<DatasetInventory> {
    bail!("the experimental Rust inventory backend currently requires Unix")
}

#[cfg(unix)]
fn probe_candidate(
    root_directory: &Dir,
    candidate: &Candidate,
    parsed: &ParsedImagePath,
    snapshot: &Arc<str>,
    schema_version: u8,
) -> Result<(InventoryRecord, Vec<InventoryFinding>)> {
    if candidate.fingerprint.size > MAX_MEDIA_BYTES {
        if schema_version == INVENTORY_SCHEMA_VERSION_V1 {
            bail!(
                "inventory media exceeds {MAX_MEDIA_BYTES} bytes: {}",
                candidate.relative_path
            );
        }
        let entry = hash_candidate(root_directory, candidate)?;
        let message = format!(
            "semantic image decode skipped because {} exceeds {MAX_MEDIA_BYTES} bytes",
            candidate.relative_path
        );
        let record = InventoryRecord {
            channels: None,
            content_hash: entry.content_hash().into(),
            decode_error: Some(message.clone()),
            height: None,
            label: parsed.label.clone(),
            logical_path: parsed.logical_path.clone(),
            media_format: None,
            mtime_ns: candidate.fingerprint.modified_ns,
            perceptual_hash: None,
            relative_path: candidate.relative_path.clone(),
            size: candidate.fingerprint.size,
            snapshot: Arc::clone(snapshot),
            split: parsed.split.clone(),
            width: None,
        };
        let finding = InventoryFinding::new(
            "IMAGE_DECODE_SKIPPED",
            "warning",
            message,
            vec![candidate.relative_path.clone()],
        )?;
        return Ok((record, vec![finding]));
    }
    let file = match root_directory.open(Path::new(&candidate.source_path)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(ConcurrentModificationError::new(format!(
                "dataset entry disappeared before inventory: {}",
                candidate.relative_path
            ))
            .into());
        }
        Err(error) => return Err(error).context("cannot open inventory entry"),
    }
    .into_std();
    let before = fingerprint(&file.metadata()?)?;
    if before != candidate.fingerprint {
        return Err(ConcurrentModificationError::new(format!(
            "dataset entry changed before inventory: {}",
            candidate.relative_path
        ))
        .into());
    }
    let capacity = usize::try_from(before.size.min(MAX_MEDIA_BYTES)).unwrap_or(0);
    let mut bytes = Vec::with_capacity(capacity);
    let mut reader = file.take(MAX_MEDIA_BYTES + 1);
    reader.read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len())? > MAX_MEDIA_BYTES {
        bail!(
            "inventory media exceeds {MAX_MEDIA_BYTES} bytes: {}",
            candidate.relative_path
        );
    }
    let after = fingerprint(&reader.get_ref().metadata()?)?;
    if before != after || u64::try_from(bytes.len())? != before.size {
        return Err(ConcurrentModificationError::new(format!(
            "dataset entry changed while inventorying: {}",
            candidate.relative_path
        ))
        .into());
    }
    let content_hash = blake3::hash(&bytes).to_hex().to_string();
    let decoded = decode_media(&bytes);
    let (perceptual_hash, width, height, media_format, channels, decode_error) = match decoded {
        Ok(media) => (
            Some(media.perceptual_hash),
            Some(media.width),
            Some(media.height),
            Some(media.media_format),
            Some(media.channels),
            None,
        ),
        Err(_) => (
            None,
            None,
            None,
            None,
            None,
            Some(format!("cannot decode {}", candidate.relative_path)),
        ),
    };
    let record = InventoryRecord {
        channels,
        content_hash,
        decode_error: decode_error.clone(),
        height,
        label: parsed.label.clone(),
        logical_path: parsed.logical_path.clone(),
        media_format,
        mtime_ns: candidate.fingerprint.modified_ns,
        perceptual_hash,
        relative_path: candidate.relative_path.clone(),
        size: candidate.fingerprint.size,
        snapshot: Arc::clone(snapshot),
        split: parsed.split.clone(),
        width,
    };
    let mut findings = Vec::new();
    if let Some(message) = decode_error {
        findings.push(InventoryFinding::new(
            "IMAGE_DECODE_FAILED",
            "error",
            message,
            vec![candidate.relative_path.clone()],
        )?);
    } else if channels != Some(3) {
        findings.push(InventoryFinding::new(
            "UNEXPECTED_CHANNELS",
            "warning",
            format!("expected 3 channels but found {}", channels.unwrap_or(0)),
            vec![candidate.relative_path.clone()],
        )?);
    }
    Ok((record, findings))
}

#[cfg(unix)]
struct DecodedMedia {
    perceptual_hash: String,
    width: u32,
    height: u32,
    media_format: String,
    channels: u8,
}

#[cfg(unix)]
fn decode_media(bytes: &[u8]) -> Result<DecodedMedia> {
    let mut reader = ImageReader::new(Cursor::new(bytes)).with_guessed_format()?;
    let format = reader.format().context("unknown image format")?;
    let mut limits = Limits::default();
    limits.max_alloc = Some(MAX_DECODE_BYTES);
    reader.limits(limits);
    let image = reader.decode()?;
    Ok(DecodedMedia {
        perceptual_hash: perceptual_hash(&image),
        width: image.width(),
        height: image.height(),
        media_format: format_name(format)?.to_owned(),
        channels: image.color().channel_count(),
    })
}

#[cfg(unix)]
fn format_name(format: ImageFormat) -> Result<&'static str> {
    match format {
        ImageFormat::Jpeg => Ok("JPEG"),
        ImageFormat::Png => Ok("PNG"),
        ImageFormat::WebP => Ok("WEBP"),
        _ => bail!("unsupported decoded image format"),
    }
}

#[cfg(unix)]
fn perceptual_hash(image: &DynamicImage) -> String {
    const SIDE: usize = 32;
    const LOW: usize = 8;
    let grayscale = image.to_luma8();
    let resized =
        image::imageops::resize(&grayscale, SIDE as u32, SIDE as u32, FilterType::Lanczos3);
    let mut coefficients = resized
        .as_raw()
        .iter()
        .map(|value| f64::from(*value))
        .collect::<Vec<_>>();
    let mut planner = DctPlanner::new();
    let dct = planner.plan_dct2(SIDE);
    for row in coefficients.chunks_exact_mut(SIDE) {
        dct.process_dct2(row);
    }
    let mut column = vec![0.0; SIDE];
    for x in 0..SIDE {
        for y in 0..SIDE {
            column[y] = coefficients[y * SIDE + x];
        }
        dct.process_dct2(&mut column);
        for y in 0..SIDE {
            coefficients[y * SIDE + x] = if column[y].abs() < 1e-9 {
                0.0
            } else {
                column[y]
            };
        }
    }
    let mut low_frequency = Vec::with_capacity(LOW * LOW);
    for y in 0..LOW {
        low_frequency.extend_from_slice(&coefficients[y * SIDE..y * SIDE + LOW]);
    }
    let mut ordered = low_frequency.clone();
    ordered.sort_by(f64::total_cmp);
    let median = (ordered[31] + ordered[32]) / 2.0;
    let bits = low_frequency
        .into_iter()
        .fold(0_u64, |hash, value| (hash << 1) | u64::from(value > median));
    format!("{bits:016x}")
}

fn validate_record(record: &InventoryRecord) -> Result<()> {
    let parsed = parse_imagefolder_path(&record.relative_path)?;
    if !parsed.supported()
        || record.split != parsed.split()
        || record.label != parsed.label()
        || record.logical_path != parsed.logical_path()
    {
        bail!("inventory record does not match its ImageFolder path");
    }
    if record.content_hash.len() != 64
        || !record
            .content_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("inventory content hash must be 64 lowercase hex characters");
    }
    if let Some(hash) = &record.perceptual_hash {
        if hash.len() != 16
            || !hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("inventory perceptual hash must be 16 lowercase hex characters");
        }
    }
    Ok(())
}

fn validate_relative_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > MAX_PATH_BYTES
        || path.contains('\\')
        || path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        bail!("inventory path must be canonical and relative");
    }
    let candidate = Path::new(path);
    if candidate.is_absolute()
        || candidate.components().any(|component| {
            !matches!(component, Component::Normal(_)) || component.as_os_str().to_str().is_none()
        })
    {
        bail!("inventory path must be canonical and relative");
    }
    Ok(())
}

struct CountingWriter(usize);

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(buffer.len())
            .ok_or_else(|| io::Error::other("inventory size overflow"))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
