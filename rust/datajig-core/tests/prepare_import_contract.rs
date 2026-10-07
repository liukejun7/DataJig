use datajig_core::{
    HfImportFile, HfImportPlan, HfImportReceipt, HfImportedFile, StalePrepareInputError,
    apply_prepare, plan_prepare,
};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn plan_and_apply_import_receipt_bind_identity_and_global_execution() {
    let root = fixture_root("apply");
    let import_root = root.join("import");
    fs::create_dir_all(import_root.join("data")).unwrap();
    fs::write(
        import_root.join("data/z.csv"),
        "id,status,score\nz, active ,2\n",
    )
    .unwrap();
    fs::write(
        import_root.join("data/a.csv"),
        "id,status,score\na, active ,1\n",
    )
    .unwrap();
    let receipt = write_receipt(&import_root, &["data/z.csv", "data/a.csv"]);
    let recipe = root.join("recipe.json");
    write_recipe(
        &recipe,
        json!({
            "format": "csv",
            "delimiter": ",",
            "include": ["data/*.csv"],
            "ignore": []
        }),
    );
    let output = root.join("prepared.jsonl");
    let plan = root.join("prepare-plan.json");

    let planned = plan_prepare(&receipt, &recipe, &output, &plan).unwrap();

    assert!(planned.source_content_id.starts_with("hfimport_"));
    assert_eq!(2, planned.source_rows);
    assert_eq!(2, planned.output_rows);
    let applied = apply_prepare(&plan, &planned.plan_id).unwrap();
    assert_eq!("applied", applied.outcome);
    assert_eq!(
        "{\"id\":\"a\",\"score\":\"1\",\"status\":\" active \"}\n{\"id\":\"z\",\"score\":\"2\",\"status\":\" active \"}\n",
        fs::read_to_string(&output).unwrap()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn selected_import_bytes_changed_after_plan_fail_apply_without_publication() {
    let root = fixture_root("stale");
    let import_root = root.join("import");
    fs::create_dir_all(import_root.join("data")).unwrap();
    let source = import_root.join("data/train.csv");
    fs::write(&source, "id,value\na,1\n").unwrap();
    let receipt = write_receipt(&import_root, &["data/train.csv"]);
    let recipe = root.join("recipe.json");
    write_recipe(&recipe, json!({"format": "csv"}));
    let output = root.join("prepared.jsonl");
    let plan = root.join("prepare-plan.json");
    let planned = plan_prepare(&receipt, &recipe, &output, &plan).unwrap();

    fs::write(&source, "id,value\na,2\n").unwrap();
    let error = apply_prepare(&plan, &planned.plan_id).unwrap_err();

    assert!(error.downcast_ref::<StalePrepareInputError>().is_some());
    assert!(!output.exists());
    assert!(!PathBuf::from(format!("{}.datajig.json", output.display())).exists());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn direct_file_rejects_import_only_selectors() {
    let root = fixture_root("direct-selectors");
    let source = root.join("source.csv");
    fs::write(&source, "id,value\na,1\n").unwrap();
    let recipe = root.join("recipe.json");
    write_recipe(&recipe, json!({"format": "csv", "include": ["*.csv"]}));

    assert!(
        plan_prepare(
            &source,
            &recipe,
            &root.join("prepared.jsonl"),
            &root.join("plan.json")
        )
        .is_err()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn direct_jsonl_prepares_objects_deterministically() {
    let root = fixture_root("direct-jsonl");
    let source = root.join("source.jsonl");
    fs::write(&source, "{\"score\":1,\"id\":\"a\"}\n").unwrap();
    let recipe = root.join("recipe.json");
    write_recipe(&recipe, json!({"format": "jsonl"}));
    let output = root.join("prepared.jsonl");
    let plan = root.join("plan.json");

    let planned = plan_prepare(&source, &recipe, &output, &plan).unwrap();
    apply_prepare(&plan, &planned.plan_id).unwrap();

    assert_eq!(
        "{\"id\":\"a\",\"score\":1}\n",
        fs::read_to_string(output).unwrap()
    );
    let _ = fs::remove_dir_all(root);
}

#[test]
fn generated_source_ids_are_injected_before_prepare_steps() {
    let root = fixture_root("generated-source-id");
    let source = root.join("source.csv");
    fs::write(&source, "name,value\nalice,1\nbob,2\n").unwrap();
    let recipe = root.join("recipe.json");
    let recipe_value = json!({
        "namespace": "datajig",
        "kind": "prepare",
        "schema_version": 1,
        "source": {"format": "csv"},
        "output": {"format": "jsonl"},
        "id_field": "_datajig_source_id",
        "generated_source_id": "_datajig_source_id",
        "steps": [{"op": "select", "fields": ["name", "_datajig_source_id"]}]
    });
    fs::write(&recipe, serde_json::to_vec_pretty(&recipe_value).unwrap()).unwrap();
    let output = root.join("prepared.jsonl");
    let plan = root.join("plan.json");

    let planned = plan_prepare(&source, &recipe, &output, &plan).unwrap();
    apply_prepare(&plan, &planned.plan_id).unwrap();

    let rows = fs::read_to_string(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(2, rows.len());
    assert_eq!("alice", rows[0]["name"]);
    assert_eq!("bob", rows[1]["name"]);
    let first = rows[0]["_datajig_source_id"].as_str().unwrap();
    let second = rows[1]["_datajig_source_id"].as_str().unwrap();
    assert!(first.starts_with("srcrow_"));
    assert!(second.starts_with("srcrow_"));
    assert_ne!(first, second);
    let _ = fs::remove_dir_all(root);
}

#[test]
fn generated_source_id_rejects_an_existing_reserved_column() {
    let root = fixture_root("generated-source-id-conflict");
    let source = root.join("source.csv");
    fs::write(&source, "_datajig_source_id,value\nowned,1\n").unwrap();
    let recipe = root.join("recipe.json");
    let recipe_value = json!({
        "namespace": "datajig",
        "kind": "prepare",
        "schema_version": 1,
        "source": {"format": "csv"},
        "output": {"format": "jsonl"},
        "id_field": "_datajig_source_id",
        "generated_source_id": "_datajig_source_id",
        "steps": []
    });
    fs::write(&recipe, serde_json::to_vec_pretty(&recipe_value).unwrap()).unwrap();

    let error = plan_prepare(
        &source,
        &recipe,
        &root.join("prepared.jsonl"),
        &root.join("plan.json"),
    )
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("already contains generated source id field")
    );
    assert!(!root.join("plan.json").exists());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn applied_output_is_idempotent_after_live_inputs_are_removed() {
    let fixture = direct_csv_plan_fixture("already-applied");
    let planned = plan_prepare(
        &fixture.source,
        &fixture.recipe,
        &fixture.output,
        &fixture.plan,
    )
    .unwrap();
    apply_prepare(&fixture.plan, &planned.plan_id).unwrap();
    fs::remove_file(&fixture.source).unwrap();
    fs::remove_file(&fixture.recipe).unwrap();

    let repeated = apply_prepare(&fixture.plan, &planned.plan_id).unwrap();

    assert_eq!("already_applied", repeated.outcome);
    let _ = fs::remove_dir_all(fixture.root);
}

#[test]
fn matching_output_recovers_receipt_after_live_inputs_are_removed() {
    let fixture = direct_csv_plan_fixture("recovered");
    let planned = plan_prepare(
        &fixture.source,
        &fixture.recipe,
        &fixture.output,
        &fixture.plan,
    )
    .unwrap();
    let applied = apply_prepare(&fixture.plan, &planned.plan_id).unwrap();
    fs::remove_file(&applied.receipt).unwrap();
    fs::remove_file(&fixture.source).unwrap();
    fs::remove_file(&fixture.recipe).unwrap();

    let recovered = apply_prepare(&fixture.plan, &planned.plan_id).unwrap();

    assert_eq!("recovered", recovered.outcome);
    assert!(Path::new(&recovered.receipt).is_file());
    let _ = fs::remove_dir_all(fixture.root);
}

struct DirectFixture {
    root: PathBuf,
    source: PathBuf,
    recipe: PathBuf,
    output: PathBuf,
    plan: PathBuf,
}

fn direct_csv_plan_fixture(label: &str) -> DirectFixture {
    let root = fixture_root(label);
    let source = root.join("source.csv");
    fs::write(&source, "id,value\na,1\n").unwrap();
    let recipe = root.join("recipe.json");
    write_recipe(&recipe, json!({"format": "csv"}));
    DirectFixture {
        source,
        recipe,
        output: root.join("prepared.jsonl"),
        plan: root.join("plan.json"),
        root,
    }
}

fn write_recipe(path: &Path, source: serde_json::Value) {
    let recipe = json!({
        "namespace": "datajig",
        "kind": "prepare",
        "schema_version": 1,
        "source": source,
        "output": {"format": "jsonl"},
        "id_field": "id",
        "steps": []
    });
    fs::write(path, serde_json::to_vec_pretty(&recipe).unwrap()).unwrap();
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

fn fixture_root(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "datajig-prepare-import-{}-{nonce}-{label}",
        std::process::id()
    ));
    fs::create_dir(&path).unwrap();
    path
}
