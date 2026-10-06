use datajig_core::{
    HF_IMPORT_PLAN_SCHEMA_VERSION, HF_IMPORT_RECEIPT_SCHEMA_VERSION, HfHubDownloadClient,
    HfHubMetadataClient, HfImportFile, HfImportPlan, HfImportReceipt, HfImportedFile,
    MAX_HF_IMPORT_BYTES, MAX_HF_IMPORT_FILES, MAX_HF_IMPORT_PATH_BYTES,
    MAX_HF_IMPORT_PATTERN_BYTES, MAX_HF_IMPORT_PATTERNS, apply_hf_import_with_client,
    plan_hf_import_with_client,
};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn source(path: &str, size: u64, digest: char) -> HfImportFile {
    HfImportFile::new(path.to_owned(), size, digest.to_string().repeat(40))
        .expect("source fixture should be valid")
}

fn imported(path: &str, size: u64, digest: char) -> HfImportedFile {
    HfImportedFile::new(
        path.to_owned(),
        size,
        format!("file_{}", digest.to_string().repeat(64)),
    )
    .expect("imported fixture should be valid")
}

fn plan(files: Vec<HfImportFile>) -> HfImportPlan {
    HfImportPlan::create(
        "owner/dataset".to_owned(),
        "main".to_owned(),
        "a".repeat(40),
        vec!["data/**".to_owned(), "metadata/*.json".to_owned()],
        vec!["data/private/**".to_owned()],
        "raw/dataset".to_owned(),
        files,
    )
    .expect("plan fixture should be valid")
}

#[test]
fn plan_canonicalizes_order_and_has_a_stable_identity() {
    let first = plan(vec![
        source("z/data.parquet", 7, 'b'),
        source("a/data.jsonl", 5, 'a'),
    ]);
    let reordered = HfImportPlan::create(
        "owner/dataset".to_owned(),
        "main".to_owned(),
        "a".repeat(40),
        vec!["metadata/*.json".to_owned(), "data/**".to_owned()],
        vec!["data/private/**".to_owned()],
        "raw/dataset".to_owned(),
        vec![
            source("a/data.jsonl", 5, 'a'),
            source("z/data.parquet", 7, 'b'),
        ],
    )
    .expect("reordered plan should be valid");

    assert_eq!(first, reordered);
    assert_eq!(HF_IMPORT_PLAN_SCHEMA_VERSION, first.schema_version());
    assert_eq!("a/data.jsonl", first.files()[0].path());
    assert_eq!(2, first.summary().total_files);
    assert_eq!(12, first.summary().total_bytes);
    assert_eq!(71, first.plan_id().len());
    assert!(first.plan_id().starts_with("hfplan_"));
    assert_eq!(first.plan_id(), first.compute_id().unwrap());
    assert_eq!(
        first,
        HfImportPlan::from_json(&first.to_json().unwrap()).unwrap()
    );
}

#[test]
fn plan_parser_rejects_unknown_fields_and_identity_or_summary_tampering() {
    let original = plan(vec![source("data/train.jsonl", 5, 'a')]);
    let mut document: serde_json::Value =
        serde_json::from_str(&original.to_json().unwrap()).unwrap();

    document["unexpected"] = serde_json::json!(true);
    let unknown = HfImportPlan::from_json(&document.to_string()).unwrap_err();
    assert!(format!("{unknown:#}").contains("unknown field"));

    document.as_object_mut().unwrap().remove("unexpected");
    document["plan_id"] = serde_json::json!(format!("hfplan_{}", "0".repeat(64)));
    let identity = HfImportPlan::from_json(&document.to_string()).unwrap_err();
    assert!(identity.to_string().contains("plan_id does not match"));

    document["plan_id"] = serde_json::json!(original.plan_id());
    document["summary"]["total_bytes"] = serde_json::json!(6);
    let summary = HfImportPlan::from_json(&document.to_string()).unwrap_err();
    assert!(summary.to_string().contains("summary does not match"));
}

#[test]
fn source_files_reject_unsafe_or_noncanonical_paths() {
    for path in ["", "/absolute", "a//b", "a/../b", "./a", "a\\b"] {
        assert!(
            HfImportFile::new(path.to_owned(), 1, "a".repeat(40)).is_err(),
            "path should be rejected: {path:?}"
        );
    }
    assert!(
        HfImportFile::new("x".repeat(MAX_HF_IMPORT_PATH_BYTES + 1), 1, "a".repeat(40))
            .unwrap_err()
            .to_string()
            .contains("path exceeds")
    );
    assert!(
        HfImportFile::new("datajig.hf-import.json".to_owned(), 1, "a".repeat(40))
            .unwrap_err()
            .to_string()
            .contains("reserved")
    );
}

#[test]
fn plan_rejects_empty_duplicate_or_oversized_file_selections() {
    assert!(
        HfImportPlan::create(
            "owner/dataset".to_owned(),
            "main".to_owned(),
            "a".repeat(40),
            Vec::new(),
            Vec::new(),
            "raw/data".to_owned(),
            Vec::new(),
        )
        .is_err()
    );
    assert!(
        HfImportPlan::create(
            "owner/dataset".to_owned(),
            "main".to_owned(),
            "a".repeat(40),
            Vec::new(),
            Vec::new(),
            "raw/data".to_owned(),
            vec![source("same.csv", 1, 'a'), source("same.csv", 1, 'b')],
        )
        .is_err()
    );

    let too_many = (0..=MAX_HF_IMPORT_FILES)
        .map(|index| source(&format!("data/{index:05}.csv"), 0, 'a'))
        .collect();
    assert!(
        HfImportPlan::create(
            "owner/dataset".to_owned(),
            "main".to_owned(),
            "a".repeat(40),
            Vec::new(),
            Vec::new(),
            "raw/data".to_owned(),
            too_many,
        )
        .is_err()
    );

    assert!(
        HfImportPlan::create(
            "owner/dataset".to_owned(),
            "main".to_owned(),
            "a".repeat(40),
            Vec::new(),
            Vec::new(),
            "raw/data".to_owned(),
            vec![source("huge.parquet", MAX_HF_IMPORT_BYTES + 1, 'a')],
        )
        .is_err()
    );
}

#[test]
fn plan_enforces_repository_revision_commit_and_pattern_contracts() {
    let valid_file = || vec![source("data/train.jsonl", 1, 'a')];
    for repo_id in ["owner", "/dataset", "owner/", "owner/data/extra"] {
        assert!(
            HfImportPlan::create(
                repo_id.to_owned(),
                "main".to_owned(),
                "a".repeat(40),
                Vec::new(),
                Vec::new(),
                "raw/data".to_owned(),
                valid_file(),
            )
            .is_err(),
            "repository ID should be rejected: {repo_id:?}"
        );
    }

    for commit in ["a".repeat(39), "A".repeat(40), "z".repeat(40)] {
        assert!(
            HfImportPlan::create(
                "owner/data".to_owned(),
                "main".to_owned(),
                commit,
                Vec::new(),
                Vec::new(),
                "raw/data".to_owned(),
                valid_file(),
            )
            .is_err()
        );
    }

    let too_many_patterns = vec!["**/*.json".to_owned(); MAX_HF_IMPORT_PATTERNS + 1];
    assert!(
        HfImportPlan::create(
            "owner/data".to_owned(),
            "main".to_owned(),
            "a".repeat(40),
            too_many_patterns,
            Vec::new(),
            "raw/data".to_owned(),
            valid_file(),
        )
        .is_err()
    );
    assert!(
        HfImportPlan::create(
            "owner/data".to_owned(),
            "main".to_owned(),
            "a".repeat(40),
            vec!["x".repeat(MAX_HF_IMPORT_PATTERN_BYTES + 1)],
            Vec::new(),
            "raw/data".to_owned(),
            valid_file(),
        )
        .is_err()
    );
    assert!(
        HfImportPlan::create(
            "owner/data".to_owned(),
            "".to_owned(),
            "a".repeat(40),
            Vec::new(),
            Vec::new(),
            "raw/data".to_owned(),
            valid_file(),
        )
        .is_err()
    );
}

#[test]
fn receipt_binds_plan_and_verified_file_content() {
    let import_plan = plan(vec![
        source("z/data.parquet", 7, 'b'),
        source("a/data.jsonl", 5, 'a'),
    ]);
    let receipt = HfImportReceipt::create(
        &import_plan,
        vec![
            imported("z/data.parquet", 7, 'd'),
            imported("a/data.jsonl", 5, 'c'),
        ],
    )
    .expect("receipt should build");

    assert_eq!(HF_IMPORT_RECEIPT_SCHEMA_VERSION, receipt.schema_version());
    assert_eq!(import_plan.plan_id(), receipt.plan_id());
    assert_eq!("a/data.jsonl", receipt.files()[0].path());
    assert_eq!(12, receipt.summary().total_bytes);
    assert_eq!(73, receipt.import_id().len());
    assert!(receipt.import_id().starts_with("hfimport_"));
    assert_eq!(
        receipt,
        HfImportReceipt::from_json(&receipt.to_json().unwrap()).unwrap()
    );
}

#[test]
fn receipt_rejects_plan_mismatch_and_tampering() {
    let import_plan = plan(vec![source("data/train.jsonl", 5, 'a')]);
    assert!(
        HfImportReceipt::create(&import_plan, vec![imported("data/train.jsonl", 4, 'c')]).is_err()
    );

    let receipt =
        HfImportReceipt::create(&import_plan, vec![imported("data/train.jsonl", 5, 'c')]).unwrap();
    let mut document: serde_json::Value =
        serde_json::from_str(&receipt.to_json().unwrap()).unwrap();
    document["files"][0]["content_id"] = serde_json::json!(format!("file_{}", "d".repeat(64)));
    let error = HfImportReceipt::from_json(&document.to_string()).unwrap_err();
    assert!(error.to_string().contains("import_id does not match"));
}

#[derive(Default)]
struct MockHubClient {
    files: Vec<HfImportFile>,
    resolved_commit: String,
    resolution_error: Option<String>,
    listed_revisions: RefCell<Vec<String>>,
}

impl HfHubMetadataClient for MockHubClient {
    fn resolve_dataset_revision(&self, _repo_id: &str, _revision: &str) -> anyhow::Result<String> {
        if let Some(message) = &self.resolution_error {
            anyhow::bail!(message.clone());
        }
        Ok(self.resolved_commit.clone())
    }

    fn list_dataset_files(
        &self,
        _repo_id: &str,
        revision: &str,
    ) -> anyhow::Result<Vec<HfImportFile>> {
        self.listed_revisions.borrow_mut().push(revision.to_owned());
        Ok(self.files.clone())
    }
}

#[test]
fn hub_plan_resolves_once_and_lists_only_the_immutable_commit() {
    let root = unique_temporary_path("hf-plan-fixed-commit");
    let output = root.join("raw/dataset");
    let plan_path = root.join("artifacts/dataset.hf-plan.json");
    let client = MockHubClient {
        resolved_commit: "b".repeat(40),
        files: vec![
            source("README.md", 3, '1'),
            source("z/test.parquet", 7, '2'),
            source("a/train.jsonl", 5, '3'),
            source("script.py", 11, '4'),
        ],
        ..MockHubClient::default()
    };

    let artifact = plan_hf_import_with_client(
        &client,
        "owner/dataset",
        "main",
        &[],
        &[],
        &output,
        &plan_path,
    )
    .expect("planning should succeed");

    assert_eq!(
        ["b".repeat(40)],
        client.listed_revisions.borrow().as_slice()
    );
    assert_eq!("b".repeat(40), artifact.resolved_commit);
    assert_eq!(2, artifact.total_files);
    assert_eq!(12, artifact.total_bytes);
    assert!(artifact.plan_id.starts_with("hfplan_"));
    assert!(plan_path.is_file());
    assert!(!output.exists(), "planning must not publish dataset bytes");
    let stored = HfImportPlan::from_json(&fs::read_to_string(&plan_path).unwrap()).unwrap();
    assert_eq!(artifact.plan_id, stored.plan_id());
    assert_eq!("a/train.jsonl", stored.files()[0].path());

    let _ = fs::remove_dir_all(root);
}

#[test]
fn hub_plan_applies_explicit_includes_then_ignores() {
    let root = unique_temporary_path("hf-plan-filters");
    let client = MockHubClient {
        resolved_commit: "c".repeat(40),
        files: vec![
            source("notes.txt", 2, '1'),
            source("data/public/readme.txt", 3, '2'),
            source("data/private/secret.txt", 5, '3'),
            source("data/train.jsonl", 7, '4'),
        ],
        ..MockHubClient::default()
    };
    let includes = vec!["**/*.txt".to_owned()];
    let ignores = vec!["data/private/**".to_owned()];

    let artifact = plan_hf_import_with_client(
        &client,
        "owner/dataset",
        "v1",
        &includes,
        &ignores,
        &root.join("dataset"),
        &root.join("plan.json"),
    )
    .unwrap();
    let stored = HfImportPlan::from_json(&fs::read_to_string(&artifact.plan).unwrap()).unwrap();
    let paths: Vec<_> = stored.files().iter().map(HfImportFile::path).collect();

    assert_eq!(vec!["data/public/readme.txt", "notes.txt"], paths);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn moving_branches_produce_plans_bound_to_each_resolution() {
    let root = unique_temporary_path("hf-plan-moving-branch");
    let first = MockHubClient {
        resolved_commit: "d".repeat(40),
        files: vec![source("data/train.jsonl", 1, '1')],
        ..MockHubClient::default()
    };
    let second = MockHubClient {
        resolved_commit: "e".repeat(40),
        files: vec![source("data/train.jsonl", 2, '2')],
        ..MockHubClient::default()
    };

    let first_artifact = plan_hf_import_with_client(
        &first,
        "owner/dataset",
        "main",
        &[],
        &[],
        &root.join("first-output"),
        &root.join("first-plan.json"),
    )
    .unwrap();
    let second_artifact = plan_hf_import_with_client(
        &second,
        "owner/dataset",
        "main",
        &[],
        &[],
        &root.join("second-output"),
        &root.join("second-plan.json"),
    )
    .unwrap();

    assert_ne!(first_artifact.plan_id, second_artifact.plan_id);
    assert_eq!(["d".repeat(40)], first.listed_revisions.borrow().as_slice());
    assert_eq!(
        ["e".repeat(40)],
        second.listed_revisions.borrow().as_slice()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn hub_planning_rejects_bad_globs_limits_and_existing_destinations() {
    let root = unique_temporary_path("hf-plan-rejections");
    fs::create_dir_all(&root).unwrap();
    let ordinary = MockHubClient {
        resolved_commit: "f".repeat(40),
        files: vec![source("data/train.jsonl", 1, '1')],
        ..MockHubClient::default()
    };
    let bad_glob = plan_hf_import_with_client(
        &ordinary,
        "owner/dataset",
        "main",
        &["[".to_owned()],
        &[],
        &root.join("output-a"),
        &root.join("plan-a.json"),
    )
    .unwrap_err();
    assert!(bad_glob.to_string().contains("include pattern"));

    let too_many = MockHubClient {
        resolved_commit: "f".repeat(40),
        files: (0..=MAX_HF_IMPORT_FILES)
            .map(|index| source(&format!("data/{index:05}.jsonl"), 0, '1'))
            .collect(),
        ..MockHubClient::default()
    };
    assert!(
        plan_hf_import_with_client(
            &too_many,
            "owner/dataset",
            "main",
            &[],
            &[],
            &root.join("output-b"),
            &root.join("plan-b.json"),
        )
        .unwrap_err()
        .to_string()
        .contains("10000 files")
    );

    fs::create_dir(root.join("existing-output")).unwrap();
    assert!(
        plan_hf_import_with_client(
            &ordinary,
            "owner/dataset",
            "main",
            &[],
            &[],
            &root.join("existing-output"),
            &root.join("plan-c.json"),
        )
        .unwrap_err()
        .to_string()
        .contains("must name new paths")
    );

    for (output, plan) in [
        (
            root.join("nested-output"),
            root.join("nested-output/plan.json"),
        ),
        (root.join("nested-plan/raw"), root.join("nested-plan")),
    ] {
        let error = plan_hf_import_with_client(
            &ordinary,
            "owner/dataset",
            "main",
            &[],
            &[],
            &output,
            &plan,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must not contain"));
        assert!(!output.exists());
        assert!(!plan.exists());
    }
    let _ = fs::remove_dir_all(root);
}

#[test]
fn hub_authentication_errors_are_redacted() {
    let secret = "hf_super_secret_123456";
    let client = MockHubClient {
        resolution_error: Some(format!("authorization failed for bearer {secret}")),
        ..MockHubClient::default()
    };
    let root = unique_temporary_path("hf-plan-redaction");

    let error = plan_hf_import_with_client(
        &client,
        "owner/private-dataset",
        "main",
        &[],
        &[],
        &root.join("output"),
        &root.join("plan.json"),
    )
    .unwrap_err();
    let message = format!("{error:#}");

    assert!(!message.contains(secret));
    assert!(message.contains("[REDACTED]"));
}

#[derive(Clone, Copy, Default)]
enum DownloadBehavior {
    #[default]
    Exact,
    Missing,
    Truncated,
    Unexpected,
    #[cfg(unix)]
    Symlink,
}

#[derive(Default)]
struct MockDownloadClient {
    behavior: DownloadBehavior,
    contents: BTreeMap<String, Vec<u8>>,
    requests: RefCell<Vec<(String, String)>>,
}

impl HfHubDownloadClient for MockDownloadClient {
    fn download_dataset_file(
        &self,
        _repo_id: &str,
        revision: &str,
        path: &str,
        _expected_size: u64,
        destination: &std::path::Path,
    ) -> anyhow::Result<()> {
        self.requests
            .borrow_mut()
            .push((revision.to_owned(), path.to_owned()));
        if matches!(self.behavior, DownloadBehavior::Missing) {
            return Ok(());
        }
        #[cfg(unix)]
        if matches!(self.behavior, DownloadBehavior::Symlink) {
            std::os::unix::fs::symlink("/dev/null", destination)?;
            return Ok(());
        }
        let content = self.contents.get(path).expect("mock path should exist");
        if matches!(self.behavior, DownloadBehavior::Truncated) {
            fs::write(destination, &content[..content.len().saturating_sub(1)])?;
        } else {
            fs::write(destination, content)?;
        }
        if matches!(self.behavior, DownloadBehavior::Unexpected) {
            let mut root = destination.to_path_buf();
            for _ in std::path::Path::new(path).components() {
                root.pop();
            }
            fs::write(root.join("unexpected.txt"), b"surprise")?;
        }
        Ok(())
    }
}

#[test]
fn apply_downloads_only_the_resolved_commit_and_publishes_verified_receipt() {
    let root = unique_temporary_path("hf-apply-exact");
    let contents = BTreeMap::from([
        ("data/train.jsonl".to_owned(), b"one\n".to_vec()),
        ("data/valid.jsonl".to_owned(), b"two\n".to_vec()),
    ]);
    let (plan_path, import_plan) = write_download_plan(&root, &contents);
    let client = MockDownloadClient {
        contents: contents.clone(),
        ..MockDownloadClient::default()
    };

    let artifact = apply_hf_import_with_client(&client, &plan_path, import_plan.plan_id())
        .expect("apply should succeed");

    assert_eq!("applied", artifact.outcome);
    assert_eq!(2, artifact.total_files);
    assert_eq!(8, artifact.total_bytes);
    assert_eq!("a".repeat(40), artifact.resolved_commit);
    assert_eq!(
        vec![
            ("a".repeat(40), "data/train.jsonl".to_owned()),
            ("a".repeat(40), "data/valid.jsonl".to_owned()),
        ],
        *client.requests.borrow()
    );
    let output = PathBuf::from(&artifact.output);
    assert_eq!(
        b"one\n",
        fs::read(output.join("data/train.jsonl"))
            .unwrap()
            .as_slice()
    );
    let receipt = HfImportReceipt::from_json(
        &fs::read_to_string(output.join("datajig.hf-import.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(artifact.import_id, receipt.import_id());
    assert_eq!(import_plan.plan_id(), receipt.plan_id());
    assert!(receipt.files()[0].content_id().starts_with("file_"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn apply_is_idempotent_only_while_receipt_and_every_byte_still_match() {
    let root = unique_temporary_path("hf-apply-idempotent");
    let contents = BTreeMap::from([("train.jsonl".to_owned(), b"row\n".to_vec())]);
    let (plan_path, import_plan) = write_download_plan(&root, &contents);
    let client = MockDownloadClient {
        contents,
        ..MockDownloadClient::default()
    };

    let first = apply_hf_import_with_client(&client, &plan_path, import_plan.plan_id()).unwrap();
    let replay = apply_hf_import_with_client(&client, &plan_path, import_plan.plan_id()).unwrap();
    assert_eq!("already_applied", replay.outcome);
    assert_eq!(
        1,
        client.requests.borrow().len(),
        "replay must not download"
    );

    fs::write(PathBuf::from(&first.output).join("train.jsonl"), b"evil").unwrap();
    let corrupt = apply_hf_import_with_client(&client, &plan_path, import_plan.plan_id())
        .expect_err("corrupt output must fail closed");
    assert!(corrupt.to_string().contains("different or unsafe content"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn apply_rejects_acceptance_mismatch_without_downloading() {
    let root = unique_temporary_path("hf-apply-acceptance");
    let contents = BTreeMap::from([("train.jsonl".to_owned(), b"row\n".to_vec())]);
    let (plan_path, _) = write_download_plan(&root, &contents);
    let client = MockDownloadClient {
        contents,
        ..MockDownloadClient::default()
    };

    let error =
        apply_hf_import_with_client(&client, &plan_path, &format!("hfplan_{}", "0".repeat(64)))
            .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("accepted Hugging Face import plan")
    );
    assert!(client.requests.borrow().is_empty());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn apply_rejects_partial_or_unexpected_downloads_and_cleans_staging() {
    for behavior in [
        DownloadBehavior::Missing,
        DownloadBehavior::Truncated,
        DownloadBehavior::Unexpected,
    ] {
        assert_failed_apply_cleans_staging(behavior);
    }
}

#[cfg(unix)]
#[test]
fn apply_rejects_downloaded_symlinks_and_cleans_staging() {
    assert_failed_apply_cleans_staging(DownloadBehavior::Symlink);
}

fn assert_failed_apply_cleans_staging(behavior: DownloadBehavior) {
    let root = unique_temporary_path("hf-apply-failure");
    let contents = BTreeMap::from([("data/train.jsonl".to_owned(), b"row\n".to_vec())]);
    let (plan_path, import_plan) = write_download_plan(&root, &contents);
    let output = PathBuf::from(import_plan.output());
    let client = MockDownloadClient {
        behavior,
        contents,
        ..MockDownloadClient::default()
    };

    apply_hf_import_with_client(&client, &plan_path, import_plan.plan_id())
        .expect_err("unsafe or incomplete download must fail");

    assert!(!output.exists(), "failed apply must not publish output");
    let leftovers: Vec<_> = fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.contains("hf-import.tmp"))
        .collect();
    assert!(leftovers.is_empty(), "staging leftovers: {leftovers:?}");
    let _ = fs::remove_dir_all(root);
}

fn write_download_plan(
    root: &std::path::Path,
    contents: &BTreeMap<String, Vec<u8>>,
) -> (PathBuf, HfImportPlan) {
    fs::create_dir_all(root).unwrap();
    let output = root.join("output");
    let files = contents
        .iter()
        .enumerate()
        .map(|(index, (path, content))| {
            source(
                path,
                u64::try_from(content.len()).unwrap(),
                char::from_digit(u32::try_from(index + 1).unwrap(), 16).unwrap(),
            )
        })
        .collect();
    let import_plan = HfImportPlan::create(
        "owner/dataset".to_owned(),
        "main".to_owned(),
        "a".repeat(40),
        Vec::new(),
        Vec::new(),
        output.to_str().unwrap().to_owned(),
        files,
    )
    .unwrap();
    let plan_path = root.join("plan.json");
    fs::write(&plan_path, import_plan.to_json().unwrap()).unwrap();
    (plan_path, import_plan)
}

fn unique_temporary_path(suffix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should follow the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "datajig-hf-import-{}-{nonce}-{suffix}",
        std::process::id()
    ))
}
