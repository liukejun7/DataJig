use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "datajig-run-cli-{}-{}",
            std::process::id(),
            NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn run(root: &Path, args: &[&str]) -> Output {
    let provider = datajig_core::TransformProviderIdentity::create(
        "0.9.0".into(),
        "1.5.6".into(),
        "CPython".into(),
        "3.12.0".into(),
    )
    .unwrap();
    let serialized = serde_json::to_string(&provider).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_datajig-core"));
    command.current_dir(root).args(args);
    if args.first() == Some(&"run") && !args.contains(&"--resume") {
        command.args(["--provider-identity", &serialized]);
    }
    command.output().unwrap()
}

fn stdout_json(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn strict_run_persists_a_resumable_plan_and_exact_acceptance_resumes_it() {
    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id,value\n1,a\n").unwrap();
    let task = "from rows.csv source-id-field id export id-field id";

    let planned = stdout_json(&run(root.path(), &["run", task, "--strict"]));
    assert_eq!(planned["kind"], "run_plan_ready");
    assert_eq!(planned["decision"], "confirmation_required");
    let attempt = planned["artifact"]["attempt_id"].as_str().unwrap();
    let plan = planned["artifact"]["plan_id"].as_str().unwrap();
    assert!(
        root.path()
            .join(".datajig/runs")
            .join(planned["artifact"]["intent_id"].as_str().unwrap())
            .join("workspace")
            .is_dir()
    );

    let resumed = stdout_json(&run(
        root.path(),
        &["run", "--resume", attempt, "--accept-plan", plan],
    ));
    assert_eq!(resumed["kind"], "run_plan_accepted");
    assert_eq!(resumed["decision"], "ready");
    assert_eq!(resumed["artifact"]["attempt_id"], attempt);
    assert_eq!(resumed["artifact"]["plan_id"], plan);
}

#[test]
fn safe_default_run_accepts_its_plan_without_claiming_execution() {
    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
    let task = "from rows.csv source-id-field id export id-field id";

    let response = stdout_json(&run(root.path(), &["run", task]));
    assert_eq!(response["kind"], "run_plan_accepted");
    assert_eq!(response["decision"], "ready");
    assert_eq!(response["artifact"]["execution_status"], "planned");
}

#[test]
fn environment_strict_mode_uses_the_same_two_phase_contract() {
    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
    let task = "from rows.csv source-id-field id export id-field id";
    let output = Command::new(env!("CARGO_BIN_EXE_datajig-core"))
        .current_dir(root.path())
        .env("DATAJIG_STRICT", "1")
        .args([
            "run",
            task,
            "--provider-identity",
            &serde_json::to_string(
                &datajig_core::TransformProviderIdentity::create(
                    "0.9.0".into(),
                    "1.5.6".into(),
                    "CPython".into(),
                    "3.12.0".into(),
                )
                .unwrap(),
            )
            .unwrap(),
        ])
        .output()
        .unwrap();
    let response = stdout_json(&output);
    assert_eq!(response["kind"], "run_plan_ready");
    assert_eq!(response["decision"], "confirmation_required");
}

#[test]
fn unowned_output_is_fatal_even_with_yes_and_creates_no_run_state() {
    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
    fs::create_dir(root.path().join("bundle")).unwrap();
    fs::write(root.path().join("bundle/user.txt"), b"user-owned").unwrap();
    let task = "from rows.csv source-id-field id export id-field id";

    let output = run(root.path(), &["run", task, "--yes"]);
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "RUN_AUTHORIZATION_REJECTED");
    assert!(!root.path().join(".datajig").exists());
    assert_eq!(
        fs::read(root.path().join("bundle/user.txt")).unwrap(),
        b"user-owned"
    );
}

#[test]
fn resume_reverifies_source_bytes_and_output_eligibility() {
    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
    let task = "from rows.csv source-id-field id export id-field id";
    let planned = stdout_json(&run(root.path(), &["run", task, "--strict"]));
    let attempt = planned["artifact"]["attempt_id"].as_str().unwrap();
    let plan = planned["artifact"]["plan_id"].as_str().unwrap();

    fs::write(root.path().join("rows.csv"), b"id\n2\n").unwrap();
    let drifted = run(
        root.path(),
        &["run", "--resume", attempt, "--accept-plan", plan],
    );
    assert!(!drifted.status.success());
    let error: Value = serde_json::from_slice(&drifted.stderr).unwrap();
    assert_eq!(error["error"]["code"], "SOURCE_CHANGED");

    fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
    fs::create_dir(root.path().join("bundle")).unwrap();
    let conflicted = run(
        root.path(),
        &["run", "--resume", attempt, "--accept-plan", plan],
    );
    assert!(!conflicted.status.success());
    let error: Value = serde_json::from_slice(&conflicted.stderr).unwrap();
    assert_eq!(error["error"]["code"], "RUN_AUTHORIZATION_REJECTED");
}

#[test]
fn resume_rejects_a_plan_bound_to_a_different_run_engine() {
    use datajig_core::{
        AuthorizationLevel, RunPlanBinding, TransformProviderIdentity, compile_dsl,
        create_run_plan, fingerprint_run_source, persist_run_plan,
    };

    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
    let compiled = compile_dsl("from rows.csv source-id-field id export id-field id").unwrap();
    let binding = RunPlanBinding {
        source_content_id: fingerprint_run_source(root.path(), Path::new("rows.csv")).unwrap(),
        engine_version: "datajig-run-engine-v1/0.8.4".into(),
        provider_identity: TransformProviderIdentity::create(
            "0.9.0".into(),
            "1.5.6".into(),
            "CPython".into(),
            "3.12.0".into(),
        )
        .unwrap(),
        output: "bundle".into(),
        consumption: None,
    };
    let plan = create_run_plan(
        &compiled,
        &binding,
        "engine-drift-test",
        AuthorizationLevel::Safe,
        false,
    )
    .unwrap();
    persist_run_plan(root.path(), &plan).unwrap();

    let resumed = run(
        root.path(),
        &[
            "run",
            "--resume",
            &plan.attempt_id,
            "--accept-plan",
            &plan.plan_id,
        ],
    );
    assert!(!resumed.status.success());
    let error: Value = serde_json::from_slice(&resumed.stderr).unwrap();
    assert_eq!(error["error"]["code"], "RUN_ENGINE_DRIFT");
}

#[test]
fn consume_options_bind_a_separate_consumption_identity() {
    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
    let task = "from rows.csv source-id-field id export id-field id";

    let plain = stdout_json(&run(root.path(), &["run", task, "--strict"]));
    let consumed = stdout_json(&run(
        root.path(),
        &[
            "run",
            task,
            "--strict",
            "--consume",
            "--consumer",
            "pytorch",
            "--run-id",
            "training-001",
            "--run-dir",
            "runs/training-001",
        ],
    ));
    assert_eq!(
        plain["artifact"]["intent_id"],
        consumed["artifact"]["intent_id"]
    );
    assert_ne!(
        plain["artifact"]["plan_id"],
        consumed["artifact"]["plan_id"]
    );
    assert!(
        consumed["artifact"]["consumption_id"]
            .as_str()
            .unwrap()
            .starts_with("consume_")
    );
}

#[test]
fn invalid_consumption_run_ids_are_rejected_before_plan_persistence() {
    for run_id in ["training 001".to_owned(), "a".repeat(129)] {
        let root = TempDir::new();
        fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
        let output = run(
            root.path(),
            &[
                "run",
                "from rows.csv source-id-field id export id-field id",
                "--strict",
                "--consume",
                "--consumer",
                "pytorch",
                "--run-id",
                &run_id,
                "--run-dir",
                "runs/training-001",
            ],
        );

        assert!(!output.status.success(), "run_id={run_id:?}");
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["error"]["code"], "INVALID_ARGUMENT");
        assert!(!root.path().join(".datajig").exists());
    }
}

#[test]
fn consumption_run_directory_must_not_overlap_delivery() {
    for (delivery, run_dir) in [
        ("delivery", "delivery/runs/training-001"),
        ("Delivery", "delivery/runs/training-001"),
        ("café", "cafe\u{301}/runs/training-001"),
        ("σ", "ς/runs/training-001"),
    ] {
        let root = TempDir::new();
        fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
        let output = run(
            root.path(),
            &[
                "run",
                "from rows.csv source-id-field id export id-field id",
                "--output",
                delivery,
                "--consume",
                "--consumer",
                "pytorch",
                "--run-id",
                "training-001",
                "--run-dir",
                run_dir,
            ],
        );

        assert!(
            !output.status.success(),
            "delivery={delivery} run_dir={run_dir}"
        );
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["error"]["code"], "RUN_PATH_OVERLAP");
        assert!(!root.path().join(".datajig").exists());
        assert!(!root.path().join(delivery).exists());
    }
}

#[test]
fn consumption_run_directory_must_not_overlap_source() {
    let root = TempDir::new();
    fs::create_dir(root.path().join("data")).unwrap();
    fs::write(root.path().join("data/rows.csv"), b"id\n1\n").unwrap();

    let output = run(
        root.path(),
        &[
            "run",
            "from data source-id-field id export id-field id",
            "--strict",
            "--consume",
            "--consumer",
            "pytorch",
            "--run-id",
            "training-001",
            "--run-dir",
            "data/runs/training-001",
        ],
    );

    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "RUN_PATH_OVERLAP");
    assert!(!root.path().join(".datajig").exists());
}

#[test]
fn consumption_run_directory_must_be_new_at_plan_and_resume_time() {
    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
    fs::create_dir_all(root.path().join("runs/training-001")).unwrap();
    let args = [
        "run",
        "from rows.csv source-id-field id export id-field id",
        "--strict",
        "--consume",
        "--consumer",
        "pytorch",
        "--run-id",
        "training-001",
        "--run-dir",
        "runs/training-001",
    ];

    let existing_at_plan = run(root.path(), &args);
    assert!(!existing_at_plan.status.success());
    let error: Value = serde_json::from_slice(&existing_at_plan.stderr).unwrap();
    assert_eq!(error["error"]["code"], "RUN_CONSUMPTION_CONFLICT");
    assert!(!root.path().join(".datajig").exists());

    fs::remove_dir(root.path().join("runs/training-001")).unwrap();
    let planned = stdout_json(&run(root.path(), &args));
    let attempt = planned["artifact"]["attempt_id"].as_str().unwrap();
    let plan = planned["artifact"]["plan_id"].as_str().unwrap();
    fs::create_dir(root.path().join("runs/training-001")).unwrap();

    let existing_at_resume = run(
        root.path(),
        &["run", "--resume", attempt, "--accept-plan", plan],
    );
    assert!(!existing_at_resume.status.success());
    let error: Value = serde_json::from_slice(&existing_at_resume.stderr).unwrap();
    assert_eq!(error["error"]["code"], "RUN_CONSUMPTION_CONFLICT");
}

#[cfg(unix)]
#[test]
fn consumption_run_directory_rejects_a_symlinked_ancestor() {
    use std::os::unix::fs::symlink;

    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
    fs::create_dir(root.path().join("actual-runs")).unwrap();
    symlink("actual-runs", root.path().join("linked-runs")).unwrap();

    let output = run(
        root.path(),
        &[
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--strict",
            "--consume",
            "--consumer",
            "pytorch",
            "--run-id",
            "training-001",
            "--run-dir",
            "linked-runs/training-001",
        ],
    );

    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "RUN_PATH_INVALID");
    assert!(!root.path().join(".datajig").exists());
}

#[test]
fn canonically_equivalent_unicode_paths_cannot_bypass_source_delivery_separation() {
    for (source, delivery) in [("café", "cafe\u{301}/delivery"), ("σ", "ς/delivery")] {
        let root = TempDir::new();
        fs::create_dir(root.path().join(source)).unwrap();
        fs::write(
            root.path().join(source).join("rows.csv"),
            b"id,value\n1,a\n",
        )
        .unwrap();
        let task = format!("from {source} source-id-field id export id-field id");
        let output = run(root.path(), &["run", &task, "--output", delivery]);

        assert!(
            !output.status.success(),
            "source={source} output={delivery}"
        );
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["error"]["code"], "RUN_PATH_OVERLAP");
        assert!(!root.path().join(".datajig").exists());
    }
}

#[test]
fn delivery_must_not_overlap_run_control_state() {
    for delivery in [".datajig", ".datajig/runs", ".DATAJIG/delivery"] {
        let root = TempDir::new();
        fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
        let output = run(
            root.path(),
            &[
                "run",
                "from rows.csv source-id-field id export id-field id",
                "--output",
                delivery,
            ],
        );

        assert!(!output.status.success(), "delivery={delivery}");
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["error"]["code"], "RUN_PATH_OVERLAP");
        assert!(!root.path().join(".datajig").exists());
    }
}

#[test]
fn delivery_rejects_a_non_directory_ancestor_before_plan_persistence() {
    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
    fs::write(root.path().join("blocked"), b"not a directory").unwrap();

    let output = run(
        root.path(),
        &[
            "run",
            "from rows.csv source-id-field id export id-field id",
            "--output",
            "blocked/delivery",
        ],
    );

    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "RUN_PATH_INVALID");
    assert!(!root.path().join(".datajig").exists());
}

#[test]
fn homogeneous_directory_format_is_inferred_into_the_canonical_plan() {
    let root = TempDir::new();
    fs::create_dir(root.path().join("rows")).unwrap();
    fs::write(root.path().join("rows/day-1.csv"), b"id,value\n1,a\n").unwrap();
    fs::write(root.path().join("rows/day-2.csv"), b"id,value\n2,b\n").unwrap();

    let response = stdout_json(&run(root.path(), &["run", "整理 rows", "--strict"]));

    assert_eq!(
        response["artifact"]["canonical_ast"]["source"]["format"],
        "csv"
    );
}

#[test]
fn mixed_directory_formats_are_rejected_before_a_plan_is_persisted() {
    let root = TempDir::new();
    fs::create_dir(root.path().join("mixed")).unwrap();
    fs::write(root.path().join("mixed/rows.csv"), b"id,value\n1,a\n").unwrap();
    fs::write(
        root.path().join("mixed/rows.jsonl"),
        b"{\"id\":2,\"value\":\"b\"}\n",
    )
    .unwrap();

    let output = run(root.path(), &["run", "整理 mixed", "--strict"]);

    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "MIXED_FORMAT");
    assert!(error["error"]["message"].as_str().unwrap().contains("csv"));
    assert!(
        error["error"]["message"]
            .as_str()
            .unwrap()
            .contains("jsonl")
    );
    assert!(!root.path().join(".datajig").exists());
}

#[test]
fn direct_file_format_mismatch_is_rejected_before_plan_persistence() {
    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id,value\n1,a\n").unwrap();
    let task = "from rows.csv format jsonl source-id-field id export id-field id";

    let output = run(root.path(), &["run", task, "--strict"]);

    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "SOURCE_FORMAT_MISMATCH");
    assert!(!root.path().join(".datajig").exists());
}

#[test]
fn source_identity_field_limit_is_enforced_before_plan_persistence() {
    let root = TempDir::new();
    fs::write(root.path().join("rows.csv"), b"id\n1\n").unwrap();
    let long_field = "x".repeat(1025);
    let task = format!("from rows.csv source-id-field {long_field} export id-field {long_field}");

    let output = run(root.path(), &["run", &task, "--strict"]);

    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "INVALID_ARGUMENT");
    assert_eq!(
        error["error"]["details"]["location"],
        "source.source-id-field"
    );
    assert!(!root.path().join(".datajig").exists());
}

#[test]
fn delivery_nested_under_a_directory_source_is_rejected_before_plan_persistence() {
    for (source, delivery) in [
        ("data", "data/delivery"),
        ("./data", "data/delivery"),
        ("data", "./data/delivery"),
        ("data", "DATA/delivery"),
    ] {
        let root = TempDir::new();
        fs::create_dir(root.path().join("data")).unwrap();
        fs::write(root.path().join("data/rows.csv"), b"id,value\n1,a\n").unwrap();
        let task = format!("from {source} source-id-field id export id-field id");

        let output = run(root.path(), &["run", &task, "--output", delivery]);

        assert!(
            !output.status.success(),
            "source={source} output={delivery}"
        );
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["error"]["code"], "RUN_PATH_OVERLAP");
        assert!(!root.path().join(".datajig").exists());
        assert!(!root.path().join("data/delivery").exists());
    }
}
