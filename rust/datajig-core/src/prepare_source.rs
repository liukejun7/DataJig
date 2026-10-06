use crate::hf_import::{HfImportedFile, load_hf_import_receipt};
use crate::prepare::{PrepareInvalidDataError, hash_file};
use crate::tabular_source::{
    TabularConsumer, TabularRow, stream_csv, stream_jsonl, stream_parquet,
};
use anyhow::{Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Component, Path, PathBuf};

#[derive(Clone, Copy, Debug)]
pub(crate) enum PrepareSourceFormat {
    Csv { delimiter: u8 },
    Parquet,
    Jsonl,
}

impl PrepareSourceFormat {
    fn matches_path(self, path: &str) -> bool {
        let Some(extension) = Path::new(path).extension().and_then(|value| value.to_str()) else {
            return false;
        };
        match self {
            Self::Csv { .. } => extension.eq_ignore_ascii_case("csv"),
            Self::Parquet => extension.eq_ignore_ascii_case("parquet"),
            Self::Jsonl => extension.eq_ignore_ascii_case("jsonl"),
        }
    }
}

#[derive(Clone, Debug)]
struct ResolvedSourceFile {
    absolute_path: PathBuf,
    content_id: String,
    relative_path: String,
    size: u64,
}

#[derive(Clone, Copy, Debug)]
enum SourceVerification {
    Direct,
    ImportReceipt,
}

#[derive(Clone, Debug)]
pub(crate) struct ResolvedPrepareSource {
    content_id: String,
    files: Vec<ResolvedSourceFile>,
    format: PrepareSourceFormat,
    source_path: PathBuf,
    verification: SourceVerification,
}

impl ResolvedPrepareSource {
    pub(crate) fn resolve(
        source: &Path,
        format: PrepareSourceFormat,
        includes: &[String],
        ignores: &[String],
    ) -> Result<Self> {
        if source.file_name().and_then(|value| value.to_str()) == Some("datajig.hf-import.json") {
            return Self::resolve_import(source, format, includes, ignores);
        }
        Self::resolve_direct(source, format, includes, ignores)
    }

    fn resolve_import(
        source: &Path,
        format: PrepareSourceFormat,
        includes: &[String],
        ignores: &[String],
    ) -> Result<Self> {
        let receipt = load_hf_import_receipt(source)
            .map_err(|error| PrepareInvalidDataError::new(error.to_string()))?;
        let include_set = build_glob_set(includes, "include")?;
        let ignore_set = build_glob_set(ignores, "ignore")?;
        let root = source
            .canonicalize()?
            .parent()
            .context("Hugging Face import receipt has no parent")?
            .to_owned();
        let mut files = Vec::new();
        for imported in receipt.files() {
            let included = if includes.is_empty() {
                format.matches_path(imported.path())
            } else {
                include_set.is_match(imported.path())
            };
            if !included || ignore_set.is_match(imported.path()) {
                continue;
            }
            if !format.matches_path(imported.path()) {
                return Err(PrepareInvalidDataError::new(format!(
                    "selected import file {:?} does not match the recipe source format",
                    imported.path()
                ))
                .into());
            }
            files.push(resolve_import_file(&root, imported)?);
        }
        if files.is_empty() {
            return Err(PrepareInvalidDataError::new(
                "preparation import selection contains no files",
            )
            .into());
        }
        files.sort_by(|left, right| {
            left.relative_path
                .as_bytes()
                .cmp(right.relative_path.as_bytes())
        });
        let resolved = Self {
            content_id: receipt.import_id().to_owned(),
            files,
            format,
            source_path: source.canonicalize()?,
            verification: SourceVerification::ImportReceipt,
        };
        resolved.verify_unchanged()?;
        Ok(resolved)
    }

    fn resolve_direct(
        source: &Path,
        format: PrepareSourceFormat,
        includes: &[String],
        ignores: &[String],
    ) -> Result<Self> {
        if !includes.is_empty() || !ignores.is_empty() {
            return Err(PrepareInvalidDataError::new(
                "include and ignore selectors require a Hugging Face import receipt",
            )
            .into());
        }
        let source_path = source
            .canonicalize()
            .context("cannot resolve preparation source")?;
        if !source_path.is_file() {
            return Err(
                PrepareInvalidDataError::new("preparation source must be a regular file").into(),
            );
        }
        if !format.matches_path(source_path.to_str().unwrap_or_default()) {
            return Err(PrepareInvalidDataError::new(
                "preparation source extension does not match the recipe format",
            )
            .into());
        }
        let content_id = hash_file(&source_path, "source")?;
        let size = fs::metadata(&source_path)?.len();
        let relative_path = source_path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("source")
            .to_owned();
        Ok(Self {
            content_id: content_id.clone(),
            files: vec![ResolvedSourceFile {
                absolute_path: source_path.clone(),
                content_id,
                relative_path,
                size,
            }],
            format,
            source_path,
            verification: SourceVerification::Direct,
        })
    }

    pub(crate) fn content_id(&self) -> &str {
        &self.content_id
    }

    pub(crate) fn source_path(&self) -> &Path {
        &self.source_path
    }

    pub(crate) fn verify_unchanged(&self) -> Result<()> {
        match self.verification {
            SourceVerification::Direct => {
                if hash_file(&self.files[0].absolute_path, "source")? != self.content_id {
                    return Err(PrepareInvalidDataError::new(
                        "direct preparation source changed content",
                    )
                    .into());
                }
            }
            SourceVerification::ImportReceipt => {
                let receipt = load_hf_import_receipt(&self.source_path)
                    .map_err(|error| PrepareInvalidDataError::new(error.to_string()))?;
                if receipt.import_id() != self.content_id {
                    return Err(PrepareInvalidDataError::new(
                        "Hugging Face import receipt changed content",
                    )
                    .into());
                }
                for file in &self.files {
                    verify_import_file(file)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn stream<C: TabularConsumer>(&self, consumer: &mut C) -> Result<()> {
        let mut schema = ShardSchemaConsumer::new(consumer);
        for file in &self.files {
            match self.format {
                PrepareSourceFormat::Csv { delimiter } => {
                    stream_csv(&file.absolute_path, delimiter, &mut schema)?;
                }
                PrepareSourceFormat::Parquet => {
                    stream_parquet(&file.absolute_path, &mut schema)?;
                }
                PrepareSourceFormat::Jsonl => {
                    stream_jsonl(&file.absolute_path, &mut schema)?;
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn selected_paths(&self) -> Vec<&str> {
        self.files
            .iter()
            .map(|file| file.relative_path.as_str())
            .collect()
    }
}

struct ShardSchemaConsumer<'a, C> {
    consumer: &'a mut C,
    headers: Option<Vec<String>>,
}

impl<'a, C> ShardSchemaConsumer<'a, C> {
    fn new(consumer: &'a mut C) -> Self {
        Self {
            consumer,
            headers: None,
        }
    }
}

impl<C: TabularConsumer> TabularConsumer for ShardSchemaConsumer<'_, C> {
    fn headers(&mut self, headers: &[String]) -> Result<()> {
        match &self.headers {
            Some(expected) if expected != headers => Err(PrepareInvalidDataError::new(
                "preparation shards do not have the same ordered schema",
            )
            .into()),
            Some(_) => Ok(()),
            None => {
                self.consumer.headers(headers)?;
                self.headers = Some(headers.to_vec());
                Ok(())
            }
        }
    }

    fn row(&mut self, row: TabularRow) -> Result<()> {
        self.consumer.row(row)
    }
}

fn resolve_import_file(root: &Path, imported: &HfImportedFile) -> Result<ResolvedSourceFile> {
    let file = ResolvedSourceFile {
        absolute_path: resolve_import_path(root, imported.path())?,
        content_id: imported.content_id().to_owned(),
        relative_path: imported.path().to_owned(),
        size: imported.size(),
    };
    verify_import_file(&file)?;
    Ok(file)
}

fn resolve_import_path(root: &Path, relative_path: &str) -> Result<PathBuf> {
    let relative = Path::new(relative_path);
    let component_count = relative.components().count();
    let mut resolved = root.to_owned();
    for (index, component) in relative.components().enumerate() {
        let Component::Normal(component) = component else {
            return Err(PrepareInvalidDataError::new(format!(
                "imported preparation path {relative_path:?} is not canonical"
            ))
            .into());
        };
        resolved.push(component);
        let metadata = fs::symlink_metadata(&resolved).map_err(|_| {
            PrepareInvalidDataError::new(format!(
                "cannot inspect imported preparation path {relative_path:?}"
            ))
        })?;
        if metadata.file_type().is_symlink() {
            return Err(PrepareInvalidDataError::new(format!(
                "imported preparation path {relative_path:?} must not traverse a symbolic link"
            ))
            .into());
        }
        if index + 1 < component_count && !metadata.is_dir() {
            return Err(PrepareInvalidDataError::new(format!(
                "imported preparation path {relative_path:?} has a non-directory parent"
            ))
            .into());
        }
    }
    Ok(resolved)
}

fn verify_import_file(file: &ResolvedSourceFile) -> Result<()> {
    let metadata = fs::symlink_metadata(&file.absolute_path).map_err(|_| {
        PrepareInvalidDataError::new(format!(
            "cannot inspect imported preparation file {:?}",
            file.relative_path
        ))
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(PrepareInvalidDataError::new(format!(
            "imported preparation path {:?} must be a regular file",
            file.relative_path
        ))
        .into());
    }
    if metadata.len() != file.size {
        return Err(PrepareInvalidDataError::new(format!(
            "imported preparation file {:?} changed size",
            file.relative_path
        ))
        .into());
    }
    let mut input = File::open(&file.absolute_path)?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    let actual = format!("file_{}", hasher.finalize().to_hex());
    if actual != file.content_id {
        return Err(PrepareInvalidDataError::new(format!(
            "imported preparation file {:?} changed content",
            file.relative_path
        ))
        .into());
    }
    Ok(())
}

fn build_glob_set(patterns: &[String], label: &str) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        if pattern.is_empty() {
            return Err(PrepareInvalidDataError::new(format!(
                "preparation {label} glob must not be empty"
            ))
            .into());
        }
        let glob = Glob::new(pattern).map_err(|_| {
            PrepareInvalidDataError::new(format!("invalid preparation {label} glob"))
        })?;
        builder.add(glob);
    }
    builder
        .build()
        .map_err(|_| PrepareInvalidDataError::new("cannot build preparation globs").into())
}

#[cfg(test)]
mod tests {
    use super::{PrepareSourceFormat, ResolvedPrepareSource};
    use crate::tabular_source::{TabularConsumer, TabularRow};
    use crate::{HfImportFile, HfImportPlan, HfImportReceipt, HfImportedFile};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn import_source_selects_verified_shards_in_canonical_order() {
        let root = fixture_root("ordered");
        let import_root = root.join("import");
        fs::create_dir_all(import_root.join("data")).unwrap();
        write(&import_root.join("data/z.csv"), b"id\nz\n");
        write(&import_root.join("data/a.csv"), b"id\na\n");
        let receipt = write_receipt(&import_root, &["data/z.csv", "data/a.csv"]);

        let source = ResolvedPrepareSource::resolve(
            &receipt,
            PrepareSourceFormat::Csv { delimiter: b',' },
            &[],
            &[],
        )
        .unwrap();

        assert!(source.content_id().starts_with("hfimport_"));
        assert_eq!(vec!["data/a.csv", "data/z.csv"], source.selected_paths());
        source.verify_unchanged().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn import_source_rejects_wrong_root_tamper_and_symlink() {
        let root = fixture_root("invalid");
        let import_root = root.join("import");
        fs::create_dir_all(import_root.join("data")).unwrap();
        let data = import_root.join("data/train.csv");
        write(&data, b"id\na\n");
        let receipt = write_receipt(&import_root, &["data/train.csv"]);

        write(&data, b"id\nb\n");
        assert!(resolve_csv(&receipt).is_err());
        write(&data, b"id\na\n");

        let moved = root.join("moved.json");
        fs::copy(&receipt, &moved).unwrap();
        assert!(resolve_csv(&moved).is_err());

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            fs::remove_file(&data).unwrap();
            let target = root.join("target.csv");
            write(&target, b"id\na\n");
            symlink(&target, &data).unwrap();
            assert!(resolve_csv(&receipt).is_err());
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn import_source_reverifies_receipt_identity() {
        let root = fixture_root("receipt-reverify");
        let import_root = root.join("import");
        fs::create_dir_all(import_root.join("data")).unwrap();
        write(&import_root.join("data/train.csv"), b"id\na\n");
        let receipt = write_receipt(&import_root, &["data/train.csv"]);
        let source = resolve_csv(&receipt).unwrap();

        fs::remove_file(&receipt).unwrap();

        assert!(source.verify_unchanged().is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn import_source_rejects_symlinked_parent_directory() {
        use std::os::unix::fs::symlink;

        let root = fixture_root("parent-symlink");
        let import_root = root.join("import");
        let outside = root.join("outside");
        fs::create_dir_all(&import_root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        write(&outside.join("train.csv"), b"id\na\n");
        symlink(&outside, import_root.join("data")).unwrap();
        let receipt = write_receipt(&import_root, &["data/train.csv"]);

        assert!(resolve_csv(&receipt).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn import_source_applies_include_then_ignore_and_rejects_empty_or_mixed_results() {
        let root = fixture_root("selectors");
        let import_root = root.join("import");
        fs::create_dir_all(import_root.join("data")).unwrap();
        write(&import_root.join("data/train.csv"), b"id\na\n");
        write(&import_root.join("data/test.csv"), b"id\nb\n");
        write(&import_root.join("data/train.jsonl"), b"{\"id\":\"c\"}\n");
        let receipt = write_receipt(
            &import_root,
            &["data/train.jsonl", "data/test.csv", "data/train.csv"],
        );

        let selected = ResolvedPrepareSource::resolve(
            &receipt,
            PrepareSourceFormat::Csv { delimiter: b',' },
            &["data/*.csv".into()],
            &["data/test*".into()],
        )
        .unwrap();
        assert_eq!(vec!["data/train.csv"], selected.selected_paths());

        assert!(
            ResolvedPrepareSource::resolve(
                &receipt,
                PrepareSourceFormat::Csv { delimiter: b',' },
                &["missing/**".into()],
                &[],
            )
            .is_err()
        );
        assert!(
            ResolvedPrepareSource::resolve(
                &receipt,
                PrepareSourceFormat::Csv { delimiter: b',' },
                &["data/*".into()],
                &[],
            )
            .is_err()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn two_csv_shards_stream_in_canonical_order_with_one_schema() {
        let root = fixture_root("csv-stream");
        let import_root = root.join("import");
        fs::create_dir_all(import_root.join("data")).unwrap();
        write(&import_root.join("data/z.csv"), b"id,value\nz,2\n");
        write(&import_root.join("data/a.csv"), b"id,value\na,1\n");
        let receipt = write_receipt(&import_root, &["data/z.csv", "data/a.csv"]);
        let source = resolve_csv(&receipt).unwrap();
        let mut consumer = RecordingConsumer::default();

        source.stream(&mut consumer).unwrap();

        assert_eq!(
            vec![vec!["id".to_owned(), "value".to_owned()]],
            consumer.headers
        );
        assert_eq!(vec!["a", "z"], consumer.ids);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn multi_shard_schema_mismatch_is_rejected() {
        let root = fixture_root("schema-mismatch");
        let import_root = root.join("import");
        fs::create_dir_all(import_root.join("data")).unwrap();
        write(&import_root.join("data/a.csv"), b"id,value\na,1\n");
        write(&import_root.join("data/b.csv"), b"value,id\n2,b\n");
        let receipt = write_receipt(&import_root, &["data/a.csv", "data/b.csv"]);
        let source = resolve_csv(&receipt).unwrap();

        assert!(source.stream(&mut RecordingConsumer::default()).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn jsonl_objects_stream_and_invalid_lines_fail_closed() {
        let root = fixture_root("jsonl-stream");
        let import_root = root.join("import");
        fs::create_dir_all(import_root.join("data")).unwrap();
        let data = import_root.join("data/train.jsonl");
        write(
            &data,
            b"{\"id\":\"a\",\"value\":1}\n{\"id\":\"b\",\"value\":2}\n",
        );
        let receipt = write_receipt(&import_root, &["data/train.jsonl"]);
        let source =
            ResolvedPrepareSource::resolve(&receipt, PrepareSourceFormat::Jsonl, &[], &[]).unwrap();
        let mut consumer = RecordingConsumer::default();
        source.stream(&mut consumer).unwrap();
        assert_eq!(vec!["a", "b"], consumer.ids);

        write(&data, b"{\"id\":\"a\",\"id\":\"b\"}\n");
        let duplicate_receipt = write_receipt(&import_root, &["data/train.jsonl"]);
        let duplicate = ResolvedPrepareSource::resolve(
            &duplicate_receipt,
            PrepareSourceFormat::Jsonl,
            &[],
            &[],
        )
        .unwrap();
        assert!(duplicate.stream(&mut RecordingConsumer::default()).is_err());

        write(&data, b"[1,2,3]\n");
        let array_receipt = write_receipt(&import_root, &["data/train.jsonl"]);
        let array =
            ResolvedPrepareSource::resolve(&array_receipt, PrepareSourceFormat::Jsonl, &[], &[])
                .unwrap();
        assert!(array.stream(&mut RecordingConsumer::default()).is_err());
        let _ = fs::remove_dir_all(root);
    }

    #[derive(Default)]
    struct RecordingConsumer {
        headers: Vec<Vec<String>>,
        ids: Vec<String>,
    }

    impl TabularConsumer for RecordingConsumer {
        fn headers(&mut self, headers: &[String]) -> anyhow::Result<()> {
            self.headers.push(headers.to_vec());
            Ok(())
        }

        fn row(&mut self, row: TabularRow) -> anyhow::Result<()> {
            self.ids.push(row["id"].as_str().unwrap().to_owned());
            Ok(())
        }
    }

    fn resolve_csv(receipt: &Path) -> anyhow::Result<ResolvedPrepareSource> {
        ResolvedPrepareSource::resolve(
            receipt,
            PrepareSourceFormat::Csv { delimiter: b',' },
            &[],
            &[],
        )
    }

    fn write_receipt(import_root: &Path, paths: &[&str]) -> PathBuf {
        let output = import_root
            .canonicalize()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let source_files = paths
            .iter()
            .map(|path| {
                let bytes = fs::read(import_root.join(path)).unwrap();
                HfImportFile::new((*path).into(), bytes.len() as u64, "a".repeat(40)).unwrap()
            })
            .collect();
        let plan = HfImportPlan::create(
            "owner/dataset".into(),
            "main".into(),
            "b".repeat(40),
            vec![],
            vec![],
            output,
            source_files,
        )
        .unwrap();
        let imported = paths
            .iter()
            .map(|path| {
                let bytes = fs::read(import_root.join(path)).unwrap();
                HfImportedFile::new(
                    (*path).into(),
                    bytes.len() as u64,
                    format!("file_{}", blake3::hash(&bytes).to_hex()),
                )
                .unwrap()
            })
            .collect();
        let receipt = HfImportReceipt::create(&plan, imported).unwrap();
        let path = import_root.join("datajig.hf-import.json");
        fs::write(&path, receipt.to_json().unwrap()).unwrap();
        path
    }

    fn write(path: &Path, bytes: &[u8]) {
        fs::write(path, bytes).unwrap();
    }

    fn fixture_root(label: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "datajig-prepare-source-{}-{nonce}-{label}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        path
    }
}
