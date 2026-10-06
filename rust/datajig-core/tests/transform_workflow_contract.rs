use datajig_core::{
    MAX_TRANSFORM_INPUTS, MAX_TRANSFORM_OUTPUT_BYTES, MAX_TRANSFORM_OUTPUT_FIELDS,
    MAX_TRANSFORM_OUTPUT_ROWS, MAX_TRANSFORM_PARAMETER_BYTES, MAX_TRANSFORM_PARAMETERS,
    MAX_TRANSFORM_SOURCE_BYTES, MAX_TRANSFORM_SOURCE_ROWS, MAX_TRANSFORM_SQL_BYTES,
    TRANSFORM_PLAN_SCHEMA_VERSION, TRANSFORM_RECEIPT_SCHEMA_VERSION, TransformExecutionEvidence,
    TransformExpectedOutput, TransformField, TransformLimits, TransformPlan, TransformPlanInput,
    TransformProviderIdentity, TransformReceipt, TransformSource, TransformSourceFormat,
};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn transform_plan_identity_covers_every_execution_input() {
    let baseline = plan_input();
    let baseline_id = TransformPlan::create(baseline.clone())
        .unwrap()
        .plan_id()
        .to_owned();

    let mut variants = Vec::new();
    let mut changed = baseline.clone();
    changed.sources[0] = TransformSource::create(
        "events".into(),
        "/data/events.parquet".into(),
        TransformSourceFormat::Parquet,
        18,
        2,
        format!("file_{}", "9".repeat(64)),
    )
    .unwrap();
    variants.push(changed);

    let mut changed = baseline.clone();
    changed.sql.push(' ');
    variants.push(changed);

    let mut changed = baseline.clone();
    changed.parameters = vec![json!(8)];
    variants.push(changed);

    let mut changed = baseline.clone();
    changed.id_field = "record_id".into();
    variants.push(changed);

    let mut changed = baseline.clone();
    changed.output_path = "/output/other.jsonl".into();
    variants.push(changed);

    let mut changed = baseline.clone();
    changed.provider = TransformProviderIdentity::create(
        "0.6.0".into(),
        "1.5.5".into(),
        "CPython".into(),
        "3.12.14".into(),
    )
    .unwrap();
    variants.push(changed);

    let mut changed = baseline.clone();
    changed.limits.output_rows -= 1;
    variants.push(changed);

    let mut changed = baseline;
    changed.expected.output_content_id = format!("prepared_{}", "8".repeat(64));
    variants.push(changed);

    for variant in variants {
        let plan = TransformPlan::create(variant).unwrap();
        assert_ne!(baseline_id, plan.plan_id());
        assert!(plan.plan_id().starts_with("xform_"));
        assert!(plan.sql_content_id().starts_with("sql_"));
        assert!(plan.parameter_content_id().starts_with("params_"));
        assert!(plan.provider_id().starts_with("provider_"));
    }
}

#[test]
fn transform_receipt_identity_binds_plan_and_output() {
    let plan = TransformPlan::create(plan_input()).unwrap();
    let evidence = TransformExecutionEvidence {
        output_path: "/output/prepared.jsonl".into(),
        output_content_id: format!("prepared_{}", "7".repeat(64)),
        schema: vec![
            TransformField::new("id".into(), "utf8".into(), false).unwrap(),
            TransformField::new("score".into(), "int64".into(), true).unwrap(),
        ],
        rows: 2,
        bytes: 42,
        unique_ids: 2,
    };

    let receipt = TransformReceipt::create(&plan, evidence.clone()).unwrap();
    assert!(receipt.receipt_id().starts_with("xformed_"));
    assert_eq!(plan.plan_id(), receipt.plan_id());
    assert_eq!(
        plan.expected().output_content_id,
        receipt.output_content_id()
    );

    let other_plan = TransformPlan::create(TransformPlanInput {
        output_path: "/output/other.jsonl".into(),
        expected: TransformExpectedOutput {
            output_content_id: format!("prepared_{}", "6".repeat(64)),
            ..plan_input().expected
        },
        ..plan_input()
    })
    .unwrap();
    let other_evidence = TransformExecutionEvidence {
        output_path: "/output/other.jsonl".into(),
        output_content_id: format!("prepared_{}", "6".repeat(64)),
        ..evidence
    };
    let other_receipt = TransformReceipt::create(&other_plan, other_evidence).unwrap();
    assert_ne!(receipt.receipt_id(), other_receipt.receipt_id());
}

#[test]
fn transform_artifacts_reject_unknown_duplicate_and_oversized_fields() {
    let plan = TransformPlan::create(plan_input()).unwrap();
    let path = temporary_path("transform-plan.json");
    fs::write(&path, plan.to_json().unwrap()).unwrap();
    assert_eq!(
        plan.plan_id(),
        TransformPlan::from_path(&path).unwrap().plan_id()
    );

    let unknown = plan
        .to_json()
        .unwrap()
        .replacen("{", "{\"unknown\":true,", 1);
    fs::write(&path, unknown).unwrap();
    assert!(TransformPlan::from_path(&path).is_err());

    let duplicate = plan
        .to_json()
        .unwrap()
        .replacen("{", "{\"kind\":\"transform_plan\",", 1);
    fs::write(&path, duplicate).unwrap();
    assert!(TransformPlan::from_path(&path).is_err());

    fs::write(&path, vec![b' '; 1024 * 1024 + 1]).unwrap();
    assert!(TransformPlan::from_path(&path).is_err());
    let _ = fs::remove_file(path);
}

#[test]
fn transform_limits_match_public_contract() {
    let limits = TransformLimits::v1();
    assert_eq!(16, MAX_TRANSFORM_INPUTS);
    assert_eq!(512 * 1024 * 1024, MAX_TRANSFORM_SOURCE_BYTES);
    assert_eq!(2_000_000, MAX_TRANSFORM_SOURCE_ROWS);
    assert_eq!(65_536, MAX_TRANSFORM_SQL_BYTES);
    assert_eq!(256, MAX_TRANSFORM_PARAMETERS);
    assert_eq!(65_536, MAX_TRANSFORM_PARAMETER_BYTES);
    assert_eq!(2_000_000, MAX_TRANSFORM_OUTPUT_ROWS);
    assert_eq!(512 * 1024 * 1024, MAX_TRANSFORM_OUTPUT_BYTES);
    assert_eq!(256, MAX_TRANSFORM_OUTPUT_FIELDS);
    assert_eq!(MAX_TRANSFORM_INPUTS, limits.inputs);
    assert_eq!(MAX_TRANSFORM_SOURCE_BYTES, limits.source_bytes);
    assert_eq!(MAX_TRANSFORM_OUTPUT_ROWS, limits.output_rows);
    assert_eq!(TRANSFORM_PLAN_SCHEMA_VERSION, 1);
    assert_eq!(TRANSFORM_RECEIPT_SCHEMA_VERSION, 1);
}

fn plan_input() -> TransformPlanInput {
    let schema = vec![
        TransformField::new("id".into(), "utf8".into(), false).unwrap(),
        TransformField::new("score".into(), "int64".into(), true).unwrap(),
    ];
    TransformPlanInput {
        sources: vec![
            TransformSource::create(
                "events".into(),
                "/data/events.parquet".into(),
                TransformSourceFormat::Parquet,
                17,
                2,
                format!("file_{}", "1".repeat(64)),
            )
            .unwrap(),
        ],
        sql_path: "/queries/train.sql".into(),
        sql: "SELECT id, score FROM events ORDER BY id".into(),
        parameters: vec![json!(7)],
        id_field: "id".into(),
        output_path: "/output/prepared.jsonl".into(),
        provider: TransformProviderIdentity::create(
            "0.6.0".into(),
            "1.5.6".into(),
            "CPython".into(),
            "3.12.14".into(),
        )
        .unwrap(),
        limits: TransformLimits::v1(),
        expected: TransformExpectedOutput {
            schema,
            rows: 2,
            bytes: 42,
            unique_ids: 2,
            output_content_id: format!("prepared_{}", "7".repeat(64)),
        },
    }
}

fn temporary_path(suffix: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "datajig-transform-contract-{}-{nonce}-{suffix}",
        std::process::id()
    ))
}
