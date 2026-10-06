use datajig_core::{
    MAX_TRANSFORM_INPUTS, MAX_TRANSFORM_OUTPUT_BYTES, MAX_TRANSFORM_OUTPUT_FIELDS,
    MAX_TRANSFORM_OUTPUT_ROWS, MAX_TRANSFORM_PARAMETER_BYTES, MAX_TRANSFORM_PARAMETERS,
    MAX_TRANSFORM_SOURCE_BYTES, MAX_TRANSFORM_SOURCE_ROWS, MAX_TRANSFORM_SQL_BYTES,
    TRANSFORM_PLAN_SCHEMA_VERSION, TRANSFORM_RECEIPT_SCHEMA_VERSION, TransformExecutionEvidence,
    TransformExpectedOutput, TransformField, TransformInputSpec, TransformLimits, TransformLineage,
    TransformOutputError, TransformOutputErrorKind, TransformPlan, TransformPlanInput,
    TransformPlanRequest, TransformProviderIdentity, TransformReceipt, TransformSource,
    TransformSourceFormat, WorkspaceStore, apply_transform, apply_transform_with_optional_provider,
    initialize_jsonl_workspace_with_receipt, inspect_transform, plan_transform,
    verify_transform_candidate, verify_transform_receipt,
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
    assert_eq!(TRANSFORM_PLAN_SCHEMA_VERSION, 2);
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
fn verified_output_rejects_canonical_path_replacement_before_publication() {
    let root = temporary_path("canonical-replacement");
    fs::create_dir_all(&root).unwrap();
    let candidate = root.join("candidate.jsonl");
    let canonical = root.join("canonical.jsonl");
    let output = root.join("published.jsonl");
    fs::write(&candidate, "{\"id\":1}\n").unwrap();
    let verified = verify_transform_candidate(
        &candidate,
        &canonical,
        "id",
        &[field("id", "integer", false)],
        &TransformLimits::v1(),
    )
    .unwrap();

    fs::remove_file(&canonical).unwrap();
    fs::write(&canonical, "{\"id\":2}\n").unwrap();
    let error = verified.publish_new(&output).unwrap_err().to_string();
    assert!(error.contains("changed after verification"), "{error}");
    assert!(!output.exists());
    let _ = fs::remove_dir_all(root);
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
    let row_candidate = temporary_path("row-limit-candidate.jsonl");
    let row_canonical = temporary_path("row-limit-canonical.jsonl");
    fs::write(
        &row_candidate,
        "{\"id\":1,\"value\":\"a\"}\n{\"id\":2,\"value\":\"b\"}\n",
    )
    .unwrap();
    let row_error =
        verify_transform_candidate(&row_candidate, &row_canonical, "id", &schema, &row_limits)
            .unwrap_err();
    assert!(
        row_error
            .to_string()
            .contains("at least 2 rows > limit 1 row"),
        "{row_error}"
    );
    let row_details = row_error
        .downcast_ref::<TransformOutputError>()
        .and_then(TransformOutputError::limit_details)
        .expect("row limit details should be machine readable");
    assert_eq!("output_rows", row_details.metric());
    assert_eq!(2, row_details.observed());
    assert!(row_details.observed_is_lower_bound());
    assert_eq!(1, row_details.limit());
    assert_eq!("rows", row_details.unit());
    let _ = fs::remove_file(row_candidate);

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
        TransformOutputErrorKind::OutputLimit,
        error.downcast_ref::<TransformOutputError>().unwrap().kind()
    );
    let details = error
        .downcast_ref::<TransformOutputError>()
        .and_then(TransformOutputError::limit_details)
        .expect("field limit details should be machine readable");
    assert_eq!("output_fields", details.metric());
    assert_eq!(257, details.observed());
    assert!(!details.observed_is_lower_bound());
    assert_eq!(256, details.limit());
    assert_eq!("fields", details.unit());
    assert!(
        error.to_string().contains("257 fields > limit 256 fields"),
        "{error}"
    );
    assert!(!canonical.exists());
    let _ = fs::remove_file(candidate);
}

#[test]
fn workflow_plan_apply_info_is_deterministic_and_recoverable() {
    let fixture = workflow_fixture("lifecycle");
    let planned = plan_transform(fixture.request()).unwrap();
    assert!(fixture.plan.exists());
    assert!(!fixture.output.exists());
    assert!(!fixture.receipt().exists());
    assert_eq!(
        planned.plan_id,
        TransformPlan::from_path(&fixture.plan).unwrap().plan_id()
    );

    let applied = apply_transform(&fixture.plan, &planned.plan_id, &fixture.provider).unwrap();
    assert!(fixture.output.exists());
    assert!(fixture.receipt().exists());
    assert!(!applied.recovered);
    assert!(!applied.already_applied);
    let output_before = fs::read(&fixture.output).unwrap();
    let receipt_before = fs::read(fixture.receipt()).unwrap();

    let idempotent =
        apply_transform_with_optional_provider(&fixture.plan, &planned.plan_id, None).unwrap();
    assert!(idempotent.already_applied);
    assert_eq!(output_before, fs::read(&fixture.output).unwrap());
    assert_eq!(receipt_before, fs::read(fixture.receipt()).unwrap());

    fs::remove_file(fixture.receipt()).unwrap();
    let recovered =
        apply_transform_with_optional_provider(&fixture.plan, &planned.plan_id, None).unwrap();
    assert!(recovered.recovered);
    assert!(!recovered.already_applied);
    assert_eq!(output_before, fs::read(&fixture.output).unwrap());
    let info = inspect_transform(&fixture.receipt(), true).unwrap();
    assert!(info.verified);
    assert_eq!(planned.plan_id, info.plan_id);
    let _ = fs::remove_dir_all(fixture.root);
}

#[test]
fn workflow_apply_rejects_drift_and_collisions_without_partial_public_state() {
    let fixture = workflow_fixture("drift");
    let planned = plan_transform(fixture.request()).unwrap();
    fs::write(&fixture.source, "id,value\n1,changed\n2,b\n").unwrap();
    assert!(apply_transform(&fixture.plan, &planned.plan_id, &fixture.provider).is_err());
    assert!(!fixture.output.exists());
    assert!(!fixture.receipt().exists());

    fs::write(&fixture.source, "id,value\n1,a\n2,b\n").unwrap();
    fs::write(&fixture.output, "do-not-overwrite\n").unwrap();
    assert!(apply_transform(&fixture.plan, &planned.plan_id, &fixture.provider).is_err());
    assert_eq!(
        "do-not-overwrite\n",
        fs::read_to_string(&fixture.output).unwrap()
    );
    assert!(!fixture.receipt().exists());
    let _ = fs::remove_dir_all(fixture.root);
}

#[test]
fn workflow_plan_rejects_the_reserved_receipt_path() {
    let mut fixture = workflow_fixture("receipt-path");
    fixture.plan = fixture.receipt();
    let error = plan_transform(fixture.request()).unwrap_err();
    assert!(error.to_string().contains("receipt path"));
    assert!(!fixture.plan.exists());

    fixture.plan = fixture.root.join("safe-plan.json");
    fs::write(fixture.receipt(), "reserved").unwrap();
    let error = plan_transform(fixture.request()).unwrap_err();
    assert!(error.to_string().contains("receipt path"));
    assert!(!fixture.plan.exists());
    let _ = fs::remove_dir_all(fixture.root);
}

#[test]
fn lineage_verified_receipt_changes_revision_identity_and_initializes_workspace() {
    let fixture = workflow_fixture("lineage");
    let planned = plan_transform(fixture.request()).unwrap();
    apply_transform(&fixture.plan, &planned.plan_id, &fixture.provider).unwrap();
    let verified = verify_transform_receipt(&fixture.receipt(), &fixture.output, "id").unwrap();
    let lineage = TransformLineage::from_verified_receipt(&verified).unwrap();
    assert_eq!(planned.plan_id, lineage.plan_id());

    let state = fixture.root.join("state");
    let artifact = initialize_jsonl_workspace_with_receipt(
        &fixture.output,
        &state,
        "id",
        None,
        Some(&fixture.receipt()),
    )
    .unwrap();
    let store = WorkspaceStore::open(&state).unwrap();
    let refs = store.load_refs().unwrap();
    let revision = store.load_revision(refs.head()).unwrap();
    assert_eq!(3, revision.schema_version());
    assert_eq!(Some(&lineage), revision.transform_lineage());
    assert_eq!(artifact.head_revision_id, revision.revision_id());
    let _ = fs::remove_dir_all(fixture.root);
}

#[cfg(unix)]
#[test]
fn receipt_backed_init_supports_a_read_only_dataset_directory() {
    use std::os::unix::fs::PermissionsExt;

    let fixture = workflow_fixture("readonly-lineage");
    let planned = plan_transform(fixture.request()).unwrap();
    apply_transform(&fixture.plan, &planned.plan_id, &fixture.provider).unwrap();
    let state = temporary_path("readonly-lineage-state");
    fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o555)).unwrap();

    let result = initialize_jsonl_workspace_with_receipt(
        &fixture.output,
        &state,
        "id",
        None,
        Some(&fixture.receipt()),
    );

    fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o700)).unwrap();
    let artifact = result.unwrap();
    assert_eq!(state.to_string_lossy(), artifact.state_dir);
    let _ = fs::remove_dir_all(state);
    let _ = fs::remove_dir_all(fixture.root);
}

#[test]
fn lineage_rejects_wrong_id_or_edited_output_before_workspace_creation() {
    let fixture = workflow_fixture("lineage-hostile");
    let planned = plan_transform(fixture.request()).unwrap();
    apply_transform(&fixture.plan, &planned.plan_id, &fixture.provider).unwrap();
    let wrong_state = fixture.root.join("wrong-state");
    assert!(
        initialize_jsonl_workspace_with_receipt(
            &fixture.output,
            &wrong_state,
            "other_id",
            None,
            Some(&fixture.receipt()),
        )
        .is_err()
    );
    assert!(!wrong_state.exists());

    fs::write(&fixture.output, "{\"id\":\"1\",\"value\":\"edited\"}\n").unwrap();
    let edited_state = fixture.root.join("edited-state");
    assert!(
        initialize_jsonl_workspace_with_receipt(
            &fixture.output,
            &edited_state,
            "id",
            None,
            Some(&fixture.receipt()),
        )
        .is_err()
    );
    assert!(!edited_state.exists());
    let _ = fs::remove_dir_all(fixture.root);
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

struct WorkflowFixture {
    root: PathBuf,
    source: PathBuf,
    sql: PathBuf,
    output: PathBuf,
    plan: PathBuf,
    provider: PathBuf,
}

impl WorkflowFixture {
    fn request(&self) -> TransformPlanRequest {
        TransformPlanRequest {
            inputs: vec![
                TransformInputSpec::new(
                    "events".into(),
                    self.source.clone(),
                    TransformSourceFormat::Csv,
                )
                .unwrap(),
            ],
            sql: fs::read_to_string(&self.sql).unwrap(),
            parameters: vec![],
            id_field: "id".into(),
            output_path: self.output.clone(),
            plan_path: self.plan.clone(),
            python: self.provider.clone(),
        }
    }

    fn receipt(&self) -> PathBuf {
        PathBuf::from(format!("{}.datajig.transform.json", self.output.display()))
    }
}

fn workflow_fixture(label: &str) -> WorkflowFixture {
    let root = temporary_path(label);
    fs::create_dir(&root).unwrap();
    let source = root.join("events.csv");
    fs::write(&source, "id,value\n1,a\n2,b\n").unwrap();
    let sql = root.join("transform.sql");
    fs::write(&sql, "SELECT id, value FROM events ORDER BY id").unwrap();
    let output = root.join("prepared.jsonl");
    let plan = root.join("transform-plan.json");
    let provider = write_transform_provider(&root);
    WorkflowFixture {
        root,
        source,
        sql,
        output,
        plan,
        provider,
    }
}

fn write_transform_provider(root: &std::path::Path) -> PathBuf {
    let identity = TransformProviderIdentity::create(
        "0.6.0".into(),
        "1.5.6".into(),
        "CPython".into(),
        "3.12.14".into(),
    )
    .unwrap();
    let output = "{\"id\":\"1\",\"value\":\"a\"}\n{\"id\":\"2\",\"value\":\"b\"}\n";
    let probe = json!({
        "protocol": "datajig.transform-provider.v1",
        "protocol_version": 1,
        "correlation_id": "probe",
        "status": "ok",
        "provider": identity,
        "lockdown_supported": true
    });
    let execute = json!({
        "protocol": "datajig.transform-provider.v1",
        "protocol_version": 1,
        "correlation_id": "__CORRELATION__",
        "status": "ok",
        "provider": identity,
        "schema": [
            {"name": "id", "value_type": "string", "nullable": false},
            {"name": "value", "value_type": "string", "nullable": false}
        ],
        "rows": 2,
        "bytes": output.len(),
        "candidate_complete": true
    });
    let script = format!(
        concat!(
            "#!/bin/sh\n",
            "payload=$(dd 2>/dev/null)\n",
            "case \"$payload\" in\n",
            "  *'\"operation\":\"probe\"'*) printf '%s\\n' '{probe}' ;;\n",
            "  *)\n",
            "    candidate=$(printf '%s' \"$payload\" | sed -n 's/.*\"candidate_path\":\"\\([^\"]*\\)\".*/\\1/p')\n",
            "    correlation=$(printf '%s' \"$payload\" | sed -n 's/.*\"correlation_id\":\"\\([^\"]*\\)\".*/\\1/p')\n",
            "    printf '%s' '{output}' > \"$candidate\"\n",
            "    printf '%s\\n' '{execute}' | sed \"s/__CORRELATION__/$correlation/\" ;;\n",
            "esac\n"
        ),
        probe = shell_single_quote(&probe.to_string()),
        output = shell_single_quote(output),
        execute = shell_single_quote(&execute.to_string()),
    );
    let path = root.join("provider.sh");
    fs::write(&path, script).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
    }
    path
}

fn shell_single_quote(value: &str) -> String {
    value.replace('\'', "'\"'\"'")
}
