use datajig_core::{inspect_training_consumption, plan_training_consumption};
use serde_json::Value;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn consumption_plan_binds_verified_split_run_and_consumer() {
    let fixture = BundleFixture::new("plan", &["train=10000"]);
    let run_dir = fixture.root.join("run");
    let plan_path = fixture.root.join("consume.json");

    let artifact = plan_training_consumption(
        &fixture.manifest,
        "train",
        "pytorch",
        "training-run-001",
        &run_dir,
        &plan_path,
    )
    .unwrap();

    assert!(artifact.consumption_plan_id.starts_with("consume_"));
    assert_eq!("training-run-001", artifact.run_id);
    assert_eq!("pytorch", artifact.consumer);
    assert_eq!("train", artifact.split);
    assert_eq!(2, artifact.records);
    assert_eq!(1, artifact.shards);
    assert!(plan_path.is_file());
    assert!(!run_dir.exists());
    let runtime = inspect_training_consumption(&plan_path, &artifact.consumption_plan_id).unwrap();
    assert_eq!(artifact.consumption_plan_id, runtime.consumption_plan_id);
    assert_eq!(artifact.bundle_id, runtime.bundle_id);
    assert_eq!(1, runtime.shards.len());
}

#[test]
fn consumption_plan_identity_survives_plan_relocation() {
    let fixture = BundleFixture::new("relocate", &["train=10000"]);
    let original = fixture.root.join("original.json");
    let relocated = fixture.root.join("relocated.json");
    let artifact = plan_training_consumption(
        &fixture.manifest,
        "train",
        "python",
        "run-relocated",
        &fixture.root.join("run"),
        &original,
    )
    .unwrap();
    fs::copy(&original, &relocated).unwrap();

    let runtime = inspect_training_consumption(&relocated, &artifact.consumption_plan_id).unwrap();

    assert_eq!(artifact.consumption_plan_id, runtime.consumption_plan_id);
    assert_eq!(
        fs::canonicalize(&relocated).unwrap(),
        PathBuf::from(runtime.plan_path)
    );
}

#[test]
fn consumption_plan_rejects_bad_consumer_run_id_empty_split_and_existing_outputs() {
    let fixture = BundleFixture::new("invalid", &["train=9999", "val=1"]);
    let run_dir = fixture.root.join("run");
    let plan_path = fixture.root.join("consume.json");
    assert!(
        plan_training_consumption(
            &fixture.manifest,
            "train",
            "unknown",
            "run",
            &run_dir,
            &plan_path,
        )
        .is_err()
    );
    assert!(
        plan_training_consumption(
            &fixture.manifest,
            "train",
            "python",
            "",
            &run_dir,
            &plan_path,
        )
        .is_err()
    );
    assert!(
        plan_training_consumption(
            &fixture.manifest,
            "val",
            "python",
            "run",
            &run_dir,
            &plan_path,
        )
        .is_err()
    );
    fs::create_dir(&run_dir).unwrap();
    assert!(
        plan_training_consumption(
            &fixture.manifest,
            "train",
            "python",
            "run",
            &run_dir,
            &plan_path,
        )
        .is_err()
    );
}

#[test]
fn consumption_runtime_rejects_tampering_unknown_fields_duplicate_members_and_wrong_acceptance() {
    let fixture = BundleFixture::new("strict", &["train=10000"]);
    let plan_path = fixture.root.join("consume.json");
    let artifact = plan_training_consumption(
        &fixture.manifest,
        "train",
        "huggingface",
        "run-strict",
        &fixture.root.join("run"),
        &plan_path,
    )
    .unwrap();
    assert!(inspect_training_consumption(&plan_path, "consume_wrong").is_err());

    let original = fs::read_to_string(&plan_path).unwrap();
    let mut value: Value = serde_json::from_str(&original).unwrap();
    value["run_id"] = Value::String("forged".into());
    fs::write(&plan_path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    assert!(inspect_training_consumption(&plan_path, &artifact.consumption_plan_id).is_err());

    fs::write(&plan_path, original.replacen("{", "{\"unknown\":true,", 1)).unwrap();
    assert!(inspect_training_consumption(&plan_path, &artifact.consumption_plan_id).is_err());
    let duplicate = original.replacen(
        "\"run_id\": \"run-strict\"",
        "\"run_id\": \"run-strict\",\n  \"run_id\": \"run-strict\"",
        1,
    );
    fs::write(&plan_path, duplicate).unwrap();
    assert!(inspect_training_consumption(&plan_path, &artifact.consumption_plan_id).is_err());
}

#[test]
fn consumption_runtime_reverifies_exact_live_bundle_split() {
    let fixture = BundleFixture::new("stale", &["train=10000"]);
    let plan_path = fixture.root.join("consume.json");
    let artifact = plan_training_consumption(
        &fixture.manifest,
        "train",
        "python",
        "run-stale",
        &fixture.root.join("run"),
        &plan_path,
    )
    .unwrap();
    let shard = fixture.bundle.join("train/part-00000.jsonl");
    fs::write(&shard, "{\"id\":\"changed\"}\n").unwrap();

    assert!(inspect_training_consumption(&plan_path, &artifact.consumption_plan_id).is_err());
}

#[test]
fn consume_commands_publish_plan_and_verified_runtime_envelopes() {
    let fixture = BundleFixture::new("cli", &["train=10000"]);
    let plan_path = fixture.root.join("consume.json");
    let run_dir = fixture.root.join("run");
    let planned = run_core_json(&[
        "consume-plan",
        fixture.manifest.to_str().unwrap(),
        "--split",
        "train",
        "--consumer",
        "python",
        "--run-id",
        "cli-run",
        "--output",
        run_dir.to_str().unwrap(),
        "--plan",
        plan_path.to_str().unwrap(),
    ]);
    assert_eq!("training_consumption_planned", planned["kind"]);
    let plan_id = planned["artifact"]["consumption_plan_id"].as_str().unwrap();

    let inspected = run_core_json(&[
        "consume-info",
        plan_path.to_str().unwrap(),
        "--verify",
        "--accept-plan",
        plan_id,
    ]);

    assert_eq!("training_consumption_info", inspected["kind"]);
    assert_eq!(true, inspected["artifact"]["verified"]);
    assert_eq!(plan_id, inspected["artifact"]["consumption_plan_id"]);
}

struct BundleFixture {
    root: PathBuf,
    bundle: PathBuf,
    manifest: PathBuf,
}

impl BundleFixture {
    fn new(label: &str, splits: &[&str]) -> Self {
        let root = fixture_root(label);
        fs::create_dir(&root).unwrap();
        let source = root.join("source.jsonl");
        fs::write(
            &source,
            "{\"id\":\"a\",\"text\":\"x\"}\n{\"id\":\"b\",\"text\":\"y\"}\n",
        )
        .unwrap();
        run_core(&[
            "init",
            source.to_str().unwrap(),
            "--id-field",
            "id",
            "--state",
            root.join("state").to_str().unwrap(),
        ]);
        let bundle = root.join("bundle");
        let mut arguments = vec![
            "export".to_owned(),
            "--state".to_owned(),
            root.join("state").to_string_lossy().into_owned(),
            "--output".to_owned(),
            bundle.to_string_lossy().into_owned(),
        ];
        for split in splits {
            arguments.extend(["--split".to_owned(), (*split).to_owned()]);
        }
        run_core_owned(&arguments);
        Self {
            manifest: bundle.join("datajig.bundle.json"),
            bundle,
            root,
        }
    }
}

impl Drop for BundleFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn run_core(arguments: &[&str]) {
    let arguments = arguments
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<Vec<_>>();
    run_core_owned(&arguments);
}

fn run_core_json(arguments: &[&str]) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_datajig-core"))
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "datajig-core {:?} failed: {}",
        arguments,
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn run_core_owned(arguments: &[String]) {
    let output = Command::new(env!("CARGO_BIN_EXE_datajig-core"))
        .args(arguments)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "datajig-core {:?} failed: {}",
        arguments,
        String::from_utf8_lossy(&output.stderr)
    );
}

fn fixture_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "datajig-training-consumption-{}-{nonce}-{label}",
        std::process::id()
    ))
}
