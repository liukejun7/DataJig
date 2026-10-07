use datajig_core::{agent_contract_id, render_agent_skill};
use serde_json::{Value, json};
use std::process::{Command, Output};

fn run(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_datajig-core"))
        .args(arguments)
        .output()
        .expect("datajig-core should start")
}

fn run_json(arguments: &[&str]) -> Value {
    let output = run(arguments);
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.stderr.is_empty(),
        "successful commands must not write stderr"
    );
    serde_json::from_slice(&output.stdout).expect("stdout should contain one JSON document")
}

#[test]
fn discovery_commands_publish_one_pinnable_agent_contract_identity() {
    let documents = [
        run_json(&["capabilities"]),
        run_json(&["describe"]),
        run_json(&["artifact-schema"]),
    ];

    let identities = documents.map(|document| {
        document["agent_contract_id"]
            .as_str()
            .expect("discovery output should publish agent_contract_id")
            .to_owned()
    });

    assert!(identities.iter().all(|identity| identity == &identities[0]));
    assert_eq!(73, identities[0].len());
    assert!(identities[0].starts_with("contract_"));
    assert!(
        identities[0]["contract_".len()..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(
        "contract_73cf29aa9d525da5c6ec98e8224b1e5512927d4c6bb3a05e3c774803e42baf03", identities[0],
        "intentional Agent contract changes must update this compatibility pin"
    );
}

#[test]
fn capabilities_and_describe_publish_the_same_available_command_catalog() {
    let capabilities = run_json(&["capabilities"]);
    let catalog = run_json(&["describe"]);
    let capability_names = capabilities["commands"]
        .as_array()
        .expect("capabilities should list commands")
        .iter()
        .map(|value| value.as_str().expect("command name should be a string"))
        .collect::<Vec<_>>();
    let descriptors = catalog["commands"]
        .as_array()
        .expect("describe should return command descriptors");
    let descriptor_names = descriptors
        .iter()
        .map(|value| {
            value["name"]
                .as_str()
                .expect("descriptor should have a name")
        })
        .collect::<Vec<_>>();

    assert_eq!(capability_names, descriptor_names);
    assert!(
        capability_names.windows(2).all(|pair| pair[0] < pair[1]),
        "the command catalog must stay sorted for deterministic discovery"
    );
    for descriptor in descriptors {
        assert!(
            descriptor["summary"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        assert!(
            descriptor["usage"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        assert!(descriptor["read_only"].is_boolean());
        assert!(descriptor["effects"].is_array());
        assert!(descriptor["platforms"].is_array());
        assert!(descriptor["inputs"].is_array());
        assert!(descriptor["output"].is_object());
        assert!(descriptor["exit_codes"].is_array());
    }
    assert_eq!(
        json!([1]),
        capabilities["repository_integration_schema_versions"]
    );
    assert_eq!(
        true,
        capabilities["features"]["repository_managed_agent_ci"]
    );
    assert!(capability_names.contains(&"repository-check"));
    assert!(capability_names.contains(&"repository-install"));
}

#[test]
fn unknown_discovery_entities_fail_with_bounded_json_errors() {
    for arguments in [
        ["describe", "not-a-command"],
        ["artifact-schema", "not-an-artifact"],
    ] {
        let output = run(&arguments);
        assert_eq!(Some(4), output.status.code());
        assert!(output.stdout.is_empty());
        let document: Value =
            serde_json::from_slice(&output.stderr).expect("stderr should contain JSON");
        assert_eq!(1, document["agent_api_version"]);
        assert!(document["error"]["code"].as_str().is_some());
        assert!(
            document["error"]["message"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
    }
}

#[test]
fn generated_agent_skill_pins_the_runtime_contract_identity() {
    let skill = render_agent_skill();
    let identity = agent_contract_id();

    assert!(
        skill.contains(&format!("Agent contract `{identity}`")),
        "generated instructions must identify the exact runtime contract"
    );
}

#[test]
fn discovery_publishes_the_hugging_face_import_contract() {
    let capabilities = run_json(&["capabilities"]);
    let plan = run_json(&["describe", "hf-import-plan"]);
    let apply = run_json(&["describe", "hf-import-apply"]);

    assert_eq!(
        true,
        capabilities["features"]["hugging_face_revision_import"]
    );
    assert_eq!(json!(1), capabilities["hf_import_plan_schema_versions"][0]);
    assert_eq!(
        json!(1),
        capabilities["hf_import_receipt_schema_versions"][0]
    );
    assert_eq!(json!(10_000), capabilities["limits"]["max_hf_import_files"]);
    assert_eq!(
        json!(1_099_511_627_776_u64),
        capabilities["limits"]["max_hf_import_bytes"]
    );
    assert_eq!(json!("hf-import-plan"), plan["command"]["name"]);
    assert_eq!(json!(false), plan["command"]["read_only"]);
    assert_eq!(json!("hf-import-apply"), apply["command"]["name"]);
    assert_eq!(json!(false), apply["command"]["read_only"]);
    assert!(render_agent_skill().contains("hf-import-plan"));
    assert!(render_agent_skill().contains("hf-import-apply"));
}

#[test]
fn discovery_connects_import_receipts_to_multi_shard_preparation() {
    let capabilities = run_json(&["capabilities"]);
    let prepare = run_json(&["describe", "prepare-plan"]);
    let description = prepare["command"]["summary"].as_str().unwrap();
    let usage = prepare["command"]["usage"].as_str().unwrap();

    assert!(description.contains("import receipt"));
    assert!(description.contains("recursive same-format directory"));
    assert!(description.contains("JSONL"));
    assert!(usage.contains("datajig.hf-import.json"));
    assert!(render_agent_skill().contains("datajig.hf-import.json"));
    assert!(render_agent_skill().contains("recursive local directory"));
    assert_eq!(
        json!(["csv", "parquet", "jsonl"]),
        capabilities["prepare_source_formats"]
    );
    assert_eq!(
        json!(["file", "directory", "hugging_face_import_receipt"]),
        capabilities["prepare_source_kinds"]
    );
    assert_eq!(
        json!(10_000),
        capabilities["limits"]["max_local_prepare_source_files"]
    );
    assert_eq!(
        json!(8_589_934_592_u64),
        capabilities["limits"]["max_local_prepare_source_bytes"]
    );
}

#[test]
fn discovery_publishes_the_bounded_transform_contract() {
    let capabilities = run_json(&["capabilities"]);
    let plan = run_json(&["describe", "transform-plan"]);
    let apply = run_json(&["describe", "transform-apply"]);
    let info = run_json(&["describe", "transform-info"]);

    assert_eq!(true, capabilities["features"]["agent_native_transforms"]);
    assert_eq!(json!([2]), capabilities["transform_plan_schema_versions"]);
    assert_eq!(
        json!([1]),
        capabilities["transform_receipt_schema_versions"]
    );
    assert_eq!(
        json!([1]),
        capabilities["transform_provider_protocol_versions"]
    );
    assert_eq!(
        json!(["csv", "parquet", "jsonl"]),
        capabilities["transform_source_formats"]
    );
    assert_eq!(json!(16), capabilities["transform_limits"]["inputs"]);
    assert_eq!(
        json!(["boolean", "integer", "unsigned_integer", "double", "string"]),
        capabilities["transform_output_scalar_types"]
    );
    assert_eq!(json!("duckdb"), capabilities["transform_provider"]["name"]);
    assert_eq!(
        json!("pip install 'datajig[duckdb]'"),
        capabilities["transform_provider"]["install"]
    );
    assert_eq!(json!("transform-plan"), plan["command"]["name"]);
    assert_eq!(json!(false), plan["command"]["read_only"]);
    assert_eq!(json!("transform-apply"), apply["command"]["name"]);
    assert_eq!(json!(false), apply["command"]["read_only"]);
    assert_eq!(json!("transform-info"), info["command"]["name"]);
    assert_eq!(json!(true), info["command"]["read_only"]);
    let skill = render_agent_skill();
    assert!(skill.contains("transform-plan"));
    assert!(skill.contains("transform-receipt"));
}

#[test]
fn command_descriptors_publish_machine_readable_prerequisites() {
    let export = run_json(&["describe", "export"]);
    assert_eq!(
        json!(["workspace:clean", "revision:sealed"]),
        export["command"]["requires"]
    );

    let transform_plan = run_json(&["describe", "transform-plan"]);
    assert_eq!(
        json!(["dependency:duckdb", "inputs:declared-local-tables"]),
        transform_plan["command"]["requires"]
    );

    let transform_apply = run_json(&["describe", "transform-apply"]);
    assert_eq!(
        json!([
            "dependency:duckdb",
            "artifact:transform-plan",
            "authorization:accepted-plan",
            "inputs:unchanged"
        ]),
        transform_apply["command"]["requires"]
    );
}

#[test]
fn discovery_publishes_proof_carrying_training_consumption() {
    let capabilities = run_json(&["capabilities"]);
    let plan = run_json(&["describe", "consume-plan"]);
    let info = run_json(&["describe", "consume-info"]);

    assert_eq!(
        true,
        capabilities["features"]["training_consumption_receipts"]
    );
    assert_eq!(
        json!(1),
        capabilities["training_consumption_plan_schema_versions"][0]
    );
    assert_eq!(
        json!(1),
        capabilities["training_consumption_receipt_schema_versions"][0]
    );
    assert_eq!(
        json!("all_verified_split_records_crossed_adapter_boundary_at_least_once"),
        capabilities["training_consumption_claim"]
    );
    assert_eq!(json!("consume-plan"), plan["command"]["name"]);
    assert_eq!(json!(false), plan["command"]["read_only"]);
    assert_eq!(json!("consume-info"), info["command"]["name"]);
    assert_eq!(json!(true), info["command"]["read_only"]);
    assert!(render_agent_skill().contains("consume-plan"));
    assert!(render_agent_skill().contains("consumed_"));
}

#[test]
fn export_help_and_capabilities_explain_the_split_contract_up_front() {
    let help = run(&["export", "--help"]);
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    let lower_help = help.to_lowercase();
    assert!(help.contains("NAME=WEIGHT"));
    assert!(lower_help.contains("relative integer"));
    assert!(help.contains("--split train=7 --split val=2 --split test=1"));
    assert!(lower_help.contains("soft byte target"));
    assert!(lower_help.contains("final shards may contain fewer records"));

    let capabilities = run_json(&["capabilities"]);
    assert_eq!(
        json!(10_000),
        capabilities["argument_contracts"]["training_split"]["normalized_weight_total"]
    );
    assert_eq!(
        json!("NAME=WEIGHT"),
        capabilities["argument_contracts"]["training_split"]["syntax"]
    );
    assert_eq!(
        json!(["train=7", "val=2", "test=1"]),
        capabilities["argument_contracts"]["training_split"]["example"]
    );
    assert_eq!(
        json!(1),
        capabilities["limits"]["min_training_shard_records"]
    );
    assert_eq!(json!(1), capabilities["limits"]["min_training_shard_bytes"]);
    assert_eq!(
        json!("soft_target_with_oversize_single_record_shards"),
        capabilities["argument_contracts"]["training_shard"]["byte_limit"]
    );
}

#[test]
fn invalid_split_errors_include_a_machine_executable_remediation() {
    let output = run(&[
        "export",
        "--state",
        "missing-state",
        "--output",
        "bundle",
        "--split",
        "train:7000",
    ]);
    assert_eq!(Some(2), output.status.code());
    assert!(output.stdout.is_empty());
    let document: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!("INVALID_ARGUMENT", document["error"]["code"]);
    assert!(
        document["error"]["message"]
            .as_str()
            .unwrap()
            .contains("NAME=WEIGHT")
    );
    assert_eq!(json!("export"), document["next_actions"][0]["command"]);
    assert_eq!(
        json!([
            "--split", "train=7", "--split", "val=2", "--split", "test=1"
        ]),
        document["next_actions"][0]["args"]
    );
}

#[test]
fn check_help_and_descriptor_publish_context_aliases() {
    let help = run(&["check", "--help"]);
    assert!(help.status.success());
    let help = String::from_utf8(help.stdout).unwrap();
    assert!(help.contains("@active"));
    assert!(help.contains("@latest"));
    assert!(help.contains("Omit both"));

    let descriptor = run_json(&["describe", "check"]);
    let inputs = descriptor["command"]["inputs"].as_array().unwrap();
    let change = inputs
        .iter()
        .find(|input| input["name"] == "change")
        .unwrap();
    let changeset = inputs
        .iter()
        .find(|input| input["name"] == "changeset")
        .unwrap();
    assert!(change["description"].as_str().unwrap().contains("@active"));
    assert!(
        change["description"]
            .as_str()
            .unwrap()
            .contains("omit both")
    );
    assert!(
        changeset["description"]
            .as_str()
            .unwrap()
            .contains("@latest")
    );

    let capabilities = run_json(&["capabilities"]);
    assert_eq!(
        json!("resolve the unique active pair or fail closed"),
        capabilities["argument_contracts"]["changeset_selectors"]["omission"]
    );

    for command_name in ["review-plan", "seal", "status"] {
        let descriptor = run_json(&["describe", command_name]);
        let inputs = descriptor["command"]["inputs"].as_array().unwrap();
        for selector in ["change", "changeset"] {
            let input = inputs
                .iter()
                .find(|input| input["name"] == selector)
                .unwrap();
            assert!(
                input["description"].as_str().unwrap().contains("omit both"),
                "{command_name} {selector} should explain automatic context"
            );
        }
    }
}

#[test]
fn tutorial_is_a_discoverable_bounded_write_workflow() {
    let descriptor = run_json(&["describe", "tutorial"]);
    assert_eq!(
        json!("datajig tutorial <OUTPUT>"),
        descriptor["command"]["usage"]
    );
    assert_eq!(
        json!(["linux", "macos"]),
        descriptor["command"]["platforms"]
    );
    assert_eq!(json!(false), descriptor["command"]["read_only"]);
    assert_eq!(json!(true), descriptor["command"]["output"]["bounded"]);
    assert_eq!(
        json!("tutorial_completed"),
        descriptor["command"]["output"]["kind"]
    );

    let capabilities = run_json(&["capabilities"]);
    assert!(
        capabilities["commands"]
            .as_array()
            .unwrap()
            .contains(&json!("tutorial"))
    );
}
