use datajig_core::{
    MAX_TRANSFORM_INPUTS, MAX_TRANSFORM_OUTPUT_BYTES, MAX_TRANSFORM_OUTPUT_FIELDS,
    MAX_TRANSFORM_OUTPUT_ROWS, MAX_TRANSFORM_PARAMETER_BYTES, MAX_TRANSFORM_PARAMETERS,
    MAX_TRANSFORM_SOURCE_BYTES, MAX_TRANSFORM_SOURCE_ROWS, MAX_TRANSFORM_SQL_BYTES,
    TRANSFORM_PLAN_SCHEMA_VERSION, TRANSFORM_RECEIPT_SCHEMA_VERSION, TransformExecutionEvidence,
    TransformExpectedOutput, TransformField, TransformLimits, TransformOutputError,
    TransformOutputErrorKind, TransformPlan, TransformPlanInput, TransformProviderIdentity,
    TransformReceipt, TransformSource, TransformSourceFormat, verify_transform_candidate,
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

#[test]
fn output_verifier_canonicalizes_every_supported_scalar() {
    let candidate = temporary_path("provider-candidate.jsonl");
    let canonical = temporary_path("canonical.jsonl");
    fs::write(
        &candidate,
        concat!(
            "{\"text\":\"é\",\"signed\":-7,\"id\":1.0,\"flag\":true,",
            "\"nothing\":null,\"unsigned\":18446744073709551615,\"zero\":-0.0}\n"
        ),
    )
    .unwrap();
    let schema = vec![
        field("text", "string", false),
        field("signed", "integer", false),
        field("id", "double", false),
        field("flag", "boolean", false),
        field("nothing", "string", true),
        field("unsigned", "unsigned_integer", false),
        field("zero", "double", false),
    ];

    let verified = verify_transform_candidate(
        &candidate,
        &canonical,
        "id",
        &schema,
        &TransformLimits::v1(),
    )
    .unwrap();
    let expected = concat!(
        "{\"flag\":true,\"id\":1.0,\"nothing\":null,\"signed\":-7,",
        "\"text\":\"é\",\"unsigned\":18446744073709551615,\"zero\":0}\n"
    );
    assert_eq!(expected, fs::read_to_string(&canonical).unwrap());
    assert_eq!(1, verified.rows);
    assert_eq!(1, verified.unique_ids);
    assert_eq!(expected.len() as u64, verified.bytes);
    assert_eq!(&canonical, verified.canonical_path());
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-prepared-jsonl-v1\0");
    hasher.update(expected.as_bytes());
    assert_eq!(
        format!("prepared_{}", hasher.finalize().to_hex()),
        verified.output_content_id
    );
    let _ = fs::remove_file(candidate);
    let _ = fs::remove_file(canonical);
}

#[test]
fn output_verifier_rejects_schema_type_and_malformed_rows_without_artifacts() {
    let cases = [
        (
            "duplicate-schema",
            "{\"id\":1}\n",
            vec![field("id", "integer", false), field("id", "integer", false)],
            TransformOutputErrorKind::Schema,
        ),
        (
            "unsafe-schema",
            "{\"id\":1,\"bad\\u0000field\":\"x\"}\n",
            vec![
                field("id", "integer", false),
                field("bad\0field", "string", false),
            ],
            TransformOutputErrorKind::Schema,
        ),
        (
            "unsupported-type",
            "{\"id\":1,\"price\":\"1.2\"}\n",
            vec![
                field("id", "integer", false),
                field("price", "decimal", false),
            ],
            TransformOutputErrorKind::UnsupportedType,
        ),
        (
            "duplicate-member",
            "{\"id\":1,\"id\":2}\n",
            vec![field("id", "integer", false)],
            TransformOutputErrorKind::MalformedRow,
        ),
        (
            "nested",
            "{\"id\":1,\"value\":[]}\n",
            vec![
                field("id", "integer", false),
                field("value", "string", false),
            ],
            TransformOutputErrorKind::MalformedRow,
        ),
        (
            "huge-double",
            "{\"id\":1,\"value\":1e9999}\n",
            vec![
                field("id", "integer", false),
                field("value", "double", false),
            ],
            TransformOutputErrorKind::NonFinite,
        ),
    ];
    for (label, payload, schema, expected_kind) in cases {
        let candidate = temporary_path(&format!("{label}-candidate.jsonl"));
        let canonical = temporary_path(&format!("{label}-canonical.jsonl"));
        fs::write(&candidate, payload).unwrap();
        let error = verify_transform_candidate(
            &candidate,
            &canonical,
            "id",
            &schema,
            &TransformLimits::v1(),
        )
        .unwrap_err();
        assert_eq!(
            expected_kind,
            error.downcast_ref::<TransformOutputError>().unwrap().kind()
        );
        assert!(!canonical.exists(), "{label} left a canonical artifact");
        let _ = fs::remove_file(candidate);
    }
}

#[test]
fn output_verifier_enforces_id_integrity_and_stream_limits() {
    let schema = vec![field("id", "double", false), field("value", "string", true)];
    for (label, payload) in [
        ("missing", "{\"value\":\"x\"}\n"),
        ("null", "{\"id\":null,\"value\":\"x\"}\n"),
        ("wrong-type", "{\"id\":true,\"value\":\"x\"}\n"),
        (
            "duplicate-normalized",
            "{\"id\":1,\"value\":\"a\"}\n{\"id\":1.0,\"value\":\"b\"}\n",
        ),
    ] {
        assert_output_error(
            label,
            payload,
            &schema,
            TransformLimits::v1(),
            TransformOutputErrorKind::IdIntegrity,
        );
    }
    assert_output_error(
        "empty-string",
        "{\"id\":\"   \"}\n",
        &[field("id", "string", false)],
        TransformLimits::v1(),
        TransformOutputErrorKind::IdIntegrity,
    );

    let mut row_limits = TransformLimits::v1();
    row_limits.output_rows = 1;
    assert_output_error(
        "row-limit",
        "{\"id\":1,\"value\":\"a\"}\n{\"id\":2,\"value\":\"b\"}\n",
        &schema,
        row_limits,
        TransformOutputErrorKind::OutputLimit,
    );

    let mut byte_limits = TransformLimits::v1();
    byte_limits.output_bytes = 10;
    assert_output_error(
        "byte-limit",
        "{\"id\":1,\"value\":\"larger than ten bytes\"}\n",
        &schema,
        byte_limits,
        TransformOutputErrorKind::OutputLimit,
    );
}

#[test]
fn output_verifier_accepts_256_fields_and_rejects_the_next() {
    let mut object = serde_json::Map::new();
    let mut schema = Vec::new();
    for index in 0..256 {
        let name = format!("field_{index:03}");
        object.insert(name.clone(), json!(index));
        schema.push(field(&name, "integer", false));
    }
    object.insert("field_000".into(), json!(1));
    let candidate = temporary_path("field-boundary-candidate.jsonl");
    let canonical = temporary_path("field-boundary-canonical.jsonl");
    fs::write(
        &candidate,
        format!("{}\n", serde_json::to_string(&object).unwrap()),
    )
    .unwrap();
    verify_transform_candidate(
        &candidate,
        &canonical,
        "field_000",
        &schema,
        &TransformLimits::v1(),
    )
    .unwrap();
    let _ = fs::remove_file(&canonical);

    schema.push(field("field_256", "integer", false));
    let error = verify_transform_candidate(
        &candidate,
        &canonical,
        "field_000",
        &schema,
        &TransformLimits::v1(),
    )
    .unwrap_err();
    assert_eq!(
        TransformOutputErrorKind::Schema,
        error.downcast_ref::<TransformOutputError>().unwrap().kind()
    );
    assert!(!canonical.exists());
    let _ = fs::remove_file(candidate);
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

fn field(name: &str, value_type: &str, nullable: bool) -> TransformField {
    TransformField::new(name.into(), value_type.into(), nullable).unwrap()
}

fn assert_output_error(
    label: &str,
    payload: &str,
    schema: &[TransformField],
    limits: TransformLimits,
    kind: TransformOutputErrorKind,
) {
    let candidate = temporary_path(&format!("{label}-candidate.jsonl"));
    let canonical = temporary_path(&format!("{label}-canonical.jsonl"));
    fs::write(&candidate, payload).unwrap();
    let error =
        verify_transform_candidate(&candidate, &canonical, "id", schema, &limits).unwrap_err();
    assert_eq!(
        kind,
        error.downcast_ref::<TransformOutputError>().unwrap().kind()
    );
    assert!(!canonical.exists(), "{label} left a canonical artifact");
    let _ = fs::remove_file(candidate);
}
