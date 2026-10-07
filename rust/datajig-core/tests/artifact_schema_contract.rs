use datajig_core::{
    ARTIFACT_SCHEMA_VERSION, JsonlPatchRequest, JsonlSubsetRecipe, RepositoryIntegrationLock,
    TransformPlan, TransformReceipt, artifact_schema, artifact_schema_names,
};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn artifact_schema_catalog_is_sorted_unique_and_complete() {
    let names = artifact_schema_names();
    assert!(names.windows(2).all(|pair| pair[0] < pair[1]));

    for name in names {
        let document = artifact_schema(name).expect("cataloged schema should resolve");
        assert_eq!(name, document["name"]);
        assert_eq!(ARTIFACT_SCHEMA_VERSION, document["schema_version"]);
        assert_eq!("application/json", document["media_type"]);
        assert!(document["schema"].is_object());
        assert!(document["example"].is_object());
        assert_eq!(false, document["schema"]["additionalProperties"]);
    }
    assert!(artifact_schema("not-an-artifact").is_none());
}

#[test]
fn published_transform_examples_are_accepted_by_strict_runtime_parsers() {
    let root = unique_temporary_path("transform-artifacts");
    fs::create_dir(&root).expect("fixture directory should be created");
    let plan_path = root.join("plan.json");
    let receipt_path = root.join("receipt.json");
    let plan = artifact_schema("transform-plan").expect("plan schema should resolve");
    let receipt = artifact_schema("transform-receipt").expect("receipt schema should resolve");
    fs::write(&plan_path, serde_json::to_vec(&plan["example"]).unwrap()).unwrap();
    fs::write(
        &receipt_path,
        serde_json::to_vec(&receipt["example"]).unwrap(),
    )
    .unwrap();

    let parsed_plan = TransformPlan::from_path(&plan_path);
    let parsed_receipt = TransformReceipt::from_path(&receipt_path);
    let _ = fs::remove_dir_all(&root);

    parsed_plan.expect("published transform plan should remain executable");
    parsed_receipt.expect("published transform receipt should remain executable");
    assert_eq!(false, plan["schema"]["additionalProperties"]);
    assert_eq!(false, receipt["schema"]["additionalProperties"]);
    assert_eq!(
        1,
        plan["schema"]["properties"]["limits"]["properties"]["inputs"]["minimum"]
    );
    assert_eq!(
        16,
        plan["schema"]["properties"]["limits"]["properties"]["inputs"]["maximum"]
    );
    assert_eq!(
        900,
        plan["schema"]["properties"]["limits"]["properties"]["wall_time_seconds"]["maximum"]
    );
    assert_eq!(
        json!(["boolean", "integer", "unsigned_integer", "double", "string"]),
        plan["schema"]["properties"]["expected"]["properties"]["schema"]["items"]["properties"]["value_type"]
            ["enum"]
    );
}

#[test]
fn published_subset_view_example_is_accepted_by_the_runtime_parser() {
    let document = artifact_schema("subset-view").expect("schema should resolve");
    let payload = serde_json::to_string(&document["example"]).expect("example should serialize");

    JsonlSubsetRecipe::from_json(&payload).expect("published example should remain executable");
}

#[test]
fn published_patch_example_is_accepted_by_the_runtime_parser() {
    let document = artifact_schema("jsonl-field-patch").expect("schema should resolve");
    let payload = serde_json::to_vec(&document["example"]).expect("example should serialize");
    let path = unique_temporary_path("jsonl-field-patch.json");
    fs::write(&path, payload).expect("fixture should be written");

    let result = JsonlPatchRequest::from_path(&path);
    let _ = fs::remove_file(&path);

    result.expect("published example should remain executable");
}

#[test]
fn published_prepare_recipe_example_is_accepted_by_the_runtime_planner() {
    let document = artifact_schema("prepare-recipe").expect("schema should resolve");
    let root = unique_temporary_path("prepare-recipe");
    fs::create_dir(&root).expect("fixture directory should be created");
    let source = root.join("source.csv");
    let recipe = root.join("recipe.json");
    let prepared = root.join("prepared.jsonl");
    let plan = root.join("plan.json");
    fs::write(&source, "id,status,score\na, active ,N/A\nb,inactive,1\n")
        .expect("source fixture should be written");
    fs::write(
        &recipe,
        serde_json::to_vec(&document["example"]).expect("example should serialize"),
    )
    .expect("recipe fixture should be written");

    let command = Command::new(env!("CARGO_BIN_EXE_datajig-core"))
        .arg("prepare-plan")
        .arg(&source)
        .arg("--recipe")
        .arg(&recipe)
        .arg("--output")
        .arg(&prepared)
        .arg("--plan")
        .arg(&plan)
        .output()
        .expect("datajig-core should start");
    let plan_exists = plan.is_file();
    let output_exists = prepared.exists();
    let stderr = String::from_utf8_lossy(&command.stderr).into_owned();
    let _ = fs::remove_dir_all(&root);

    assert!(command.status.success(), "prepare-plan failed: {stderr}");
    assert!(plan_exists, "prepare-plan should publish its plan artifact");
    assert!(!output_exists, "planning must not publish prepared data");
}

#[test]
fn published_prepare_recipe_supports_jsonl_and_import_selectors() {
    let document = artifact_schema("prepare-recipe").expect("schema should resolve");
    let variants = document["schema"]["properties"]["source"]["oneOf"]
        .as_array()
        .expect("source should publish variants");

    assert_eq!(3, variants.len());
    assert!(variants.iter().any(|variant| {
        variant["properties"]["format"]["const"] == "jsonl"
            && variant["properties"]["include"]["type"] == "array"
            && variant["properties"]["ignore"]["type"] == "array"
    }));
}

#[test]
fn published_training_consumption_artifacts_state_the_exact_claim() {
    let plan = artifact_schema("training-consumption-plan").expect("plan schema should resolve");
    let receipt =
        artifact_schema("training-consumption-receipt").expect("receipt schema should resolve");

    assert_eq!("training_consumption_plan", plan["example"]["kind"]);
    assert_eq!(
        "consume_",
        &plan["example"]["consumption_plan_id"].as_str().unwrap()[..8]
    );
    assert_eq!("training_consumption_receipt", receipt["example"]["kind"]);
    assert_eq!(
        "all_verified_split_records_crossed_adapter_boundary_at_least_once",
        receipt["example"]["claim"]
    );
    assert_eq!(false, plan["schema"]["additionalProperties"]);
    assert_eq!(false, receipt["schema"]["additionalProperties"]);
}

#[test]
fn published_pipeline_config_exposes_the_complete_strict_shape() {
    let document = artifact_schema("pipeline-config").expect("schema should resolve");
    let schema = &document["schema"];
    let example = &document["example"];

    assert_eq!(false, schema["additionalProperties"]);
    assert_eq!(
        false,
        schema["properties"]["pipeline"]["additionalProperties"]
    );
    assert_eq!(
        false,
        schema["properties"]["inputs"]["items"]["additionalProperties"]
    );
    assert_eq!(
        false,
        schema["properties"]["transform"]["additionalProperties"]
    );
    assert_eq!(
        false,
        schema["properties"]["delivery"]["additionalProperties"]
    );
    assert_eq!("data/events.csv", example["inputs"][0]["path"]);
    assert_eq!("deliveries/user-agg-train", example["delivery"]["output"]);
    assert!(
        schema["properties"]["delivery"]["properties"]["output"]["pattern"]
            .as_str()
            .expect("relative path pattern")
            .contains("^")
    );
}

#[test]
fn published_repository_integration_example_is_accepted_by_the_strict_parser() {
    let document = artifact_schema("repository-integration").expect("schema should resolve");
    let payload = serde_json::to_string(&document["example"]).expect("example should serialize");
    let lock = RepositoryIntegrationLock::from_json(&payload)
        .expect("published repository lock should remain executable");

    assert!(lock.integration_id().starts_with("repo_"));
    assert_eq!(
        "^repo_[0-9a-f]{64}$",
        document["schema"]["properties"]["repository_integration_id"]["pattern"]
    );
}

fn unique_temporary_path(suffix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should follow the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "datajig-contract-{}-{nonce}-{suffix}",
        std::process::id()
    ))
}
