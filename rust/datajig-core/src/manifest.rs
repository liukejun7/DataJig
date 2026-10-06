use anyhow::{Context, Result, bail};
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::io::{self, Write};
use std::path::{Component, Path};

pub const MAX_MANIFEST_BYTES: usize = 128 * 1024 * 1024;
pub const MAX_MANIFEST_ENTRIES: usize = 250_000;
pub const MAX_PATH_BYTES: usize = 4_096;
const IDENTITY_PREFIX: &[u8] = b"datajig-snapshot-v1\0";
const SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ManifestEntry {
    content_hash: String,
    path: String,
    size: u64,
}

impl ManifestEntry {
    pub fn new(path: String, size: u64, content_hash: String) -> Result<Self> {
        validate_manifest_path(&path)?;
        if content_hash.len() != 64
            || !content_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("manifest content_hash must be 64 lowercase hex characters");
        }
        Ok(Self {
            content_hash,
            path,
            size,
        })
    }

    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct ManifestSummary {
    pub total_bytes: u128,
    pub total_files: usize,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct SnapshotManifest {
    entries: Vec<ManifestEntry>,
    schema_version: u8,
    snapshot_id: String,
    summary: ManifestSummary,
}

impl SnapshotManifest {
    pub fn create(mut entries: Vec<ManifestEntry>) -> Result<Self> {
        if entries.len() > MAX_MANIFEST_ENTRIES {
            bail!("manifest exceeds {MAX_MANIFEST_ENTRIES} entries");
        }
        entries.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
        ensure_unique_paths(&entries)?;
        let snapshot_id = snapshot_id_sorted(&entries)?;
        let summary = summary(&entries);
        Ok(Self {
            entries,
            schema_version: SCHEMA_VERSION,
            snapshot_id,
            summary,
        })
    }

    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_MANIFEST_BYTES {
            bail!("manifest is too large (max {MAX_MANIFEST_BYTES} bytes)");
        }
        let wire: ManifestWire = serde_json::from_str(payload).context("invalid manifest JSON")?;
        if wire.schema_version != SCHEMA_VERSION {
            bail!("unsupported manifest schema {}", wire.schema_version);
        }
        let entries = wire
            .entries
            .0
            .into_iter()
            .map(|entry| ManifestEntry::new(entry.path, entry.size, entry.content_hash))
            .collect::<Result<Vec<_>>>()?;
        if entries
            .windows(2)
            .any(|pair| pair[0].path.as_bytes() >= pair[1].path.as_bytes())
        {
            bail!("manifest entries must be sorted by unique path");
        }
        let expected_id = snapshot_id_sorted(&entries)?;
        if wire.snapshot_id != expected_id {
            bail!("snapshot_id does not match manifest entries");
        }
        let expected_summary = summary(&entries);
        if wire.summary != expected_summary {
            bail!("manifest summary does not match entries");
        }
        Ok(Self {
            entries,
            schema_version: wire.schema_version,
            snapshot_id: wire.snapshot_id,
            summary: wire.summary.into(),
        })
    }

    pub fn to_json(&self) -> Result<String> {
        let estimated = estimate_manifest_size(
            self.entries
                .iter()
                .map(|entry| (entry.path(), entry.size())),
        )?;
        if estimated > MAX_MANIFEST_BYTES {
            bail!("manifest is too large (max {MAX_MANIFEST_BYTES} bytes)");
        }
        serde_json::to_string_pretty(self).context("cannot serialize manifest")
    }

    pub fn entries(&self) -> &[ManifestEntry] {
        &self.entries
    }

    pub fn schema_version(&self) -> u8 {
        self.schema_version
    }

    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    pub fn summary(&self) -> &ManifestSummary {
        &self.summary
    }
}

pub fn snapshot_id(entries: &[ManifestEntry]) -> Result<String> {
    if entries.len() > MAX_MANIFEST_ENTRIES {
        bail!("manifest exceeds {MAX_MANIFEST_ENTRIES} entries");
    }
    let mut ordered = entries.to_vec();
    ordered.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    ensure_unique_paths(&ordered)?;
    snapshot_id_sorted(&ordered)
}

fn snapshot_id_sorted(ordered: &[ManifestEntry]) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(IDENTITY_PREFIX);
    for entry in ordered {
        validate_manifest_path(&entry.path)?;
        let path = entry.path.as_bytes();
        let path_len = u32::try_from(path.len()).context("manifest path is too long")?;
        let digest = blake3::Hash::from_hex(&entry.content_hash)
            .context("content hash must be 64 lowercase hex characters")?;
        hasher.update(&path_len.to_be_bytes());
        hasher.update(path);
        hasher.update(&entry.size.to_be_bytes());
        hasher.update(digest.as_bytes());
    }
    Ok(format!("snap_{}", hasher.finalize().to_hex()))
}

pub fn estimate_manifest_size<'a>(
    entries: impl IntoIterator<Item = (&'a str, u64)>,
) -> Result<usize> {
    const PLACEHOLDER_HASH: &str =
        "0000000000000000000000000000000000000000000000000000000000000000";
    const PLACEHOLDER_ID: &str =
        "snap_0000000000000000000000000000000000000000000000000000000000000000";
    let entries: Vec<_> = entries
        .into_iter()
        .map(|(path, size)| PreviewEntry {
            content_hash: PLACEHOLDER_HASH,
            path,
            size,
        })
        .collect();
    let total_bytes = entries.iter().map(|entry| u128::from(entry.size)).sum();
    let preview = PreviewManifest {
        entries: &entries,
        schema_version: SCHEMA_VERSION,
        snapshot_id: PLACEHOLDER_ID,
        summary: PreviewSummary {
            total_bytes,
            total_files: entries.len(),
        },
    };
    let mut counter = CountingWriter(0);
    serde_json::to_writer_pretty(&mut counter, &preview).context("cannot size manifest")?;
    Ok(counter.0)
}

fn ensure_unique_paths(entries: &[ManifestEntry]) -> Result<()> {
    if entries.windows(2).any(|pair| pair[0].path == pair[1].path) {
        bail!("manifest entry paths must be unique");
    }
    Ok(())
}

fn summary(entries: &[ManifestEntry]) -> ManifestSummary {
    ManifestSummary {
        total_bytes: entries.iter().map(|entry| u128::from(entry.size)).sum(),
        total_files: entries.len(),
    }
}

fn validate_manifest_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.contains('\\')
        || path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        bail!("manifest path must be canonical and relative");
    }
    let candidate = Path::new(path);
    if candidate.is_absolute()
        || candidate.components().any(|component| {
            !matches!(component, Component::Normal(_)) || component.as_os_str().to_str().is_none()
        })
    {
        bail!("manifest path must be canonical and relative");
    }
    if path.len() > MAX_PATH_BYTES {
        bail!("manifest path exceeds {MAX_PATH_BYTES} UTF-8 bytes");
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestWire {
    entries: BoundedEntries,
    schema_version: u8,
    snapshot_id: String,
    summary: ManifestSummaryWire,
}

struct BoundedEntries(Vec<EntryWire>);

impl<'de> Deserialize<'de> for BoundedEntries {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct EntriesVisitor;

        impl<'de> Visitor<'de> for EntriesVisitor {
            type Value = BoundedEntries;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded manifest entry array")
            }

            fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut entries =
                    Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_MANIFEST_ENTRIES));
                while let Some(entry) = sequence.next_element()? {
                    if entries.len() == MAX_MANIFEST_ENTRIES {
                        return Err(serde::de::Error::custom(format_args!(
                            "manifest exceeds {MAX_MANIFEST_ENTRIES} entries"
                        )));
                    }
                    entries.push(entry);
                }
                Ok(BoundedEntries(entries))
            }
        }

        deserializer.deserialize_seq(EntriesVisitor)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryWire {
    content_hash: String,
    path: String,
    size: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestSummaryWire {
    total_bytes: u128,
    total_files: usize,
}

impl PartialEq<ManifestSummary> for ManifestSummaryWire {
    fn eq(&self, other: &ManifestSummary) -> bool {
        self.total_bytes == other.total_bytes && self.total_files == other.total_files
    }
}

impl From<ManifestSummaryWire> for ManifestSummary {
    fn from(value: ManifestSummaryWire) -> Self {
        Self {
            total_bytes: value.total_bytes,
            total_files: value.total_files,
        }
    }
}

#[derive(Serialize)]
struct PreviewManifest<'a> {
    entries: &'a [PreviewEntry<'a>],
    schema_version: u8,
    snapshot_id: &'static str,
    summary: PreviewSummary,
}

#[derive(Serialize)]
struct PreviewEntry<'a> {
    content_hash: &'static str,
    path: &'a str,
    size: u64,
}

#[derive(Serialize)]
struct PreviewSummary {
    total_bytes: u128,
    total_files: usize,
}

struct CountingWriter(usize);

impl Write for CountingWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(buffer.len());
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
