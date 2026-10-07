use datajig_core::{
    RunExportAst, RunPrepareAst, RunSourceAst, RunTaskAst, RunTransformAst, SourceFormat,
    SourceIdAst, SourceIdMode, TrainingSplitsAst, canonicalize_run_task, compile_dsl,
};

#[test]
fn canonical_ast_has_fixed_shape_and_preserves_semantic_case() {
    let ast = RunTaskAst {
        namespace: "datajig".into(),
        kind: "run_task".into(),
        schema_version: 1,
        source: RunSourceAst {
            path: "Data/Input.CSV".into(),
            format: Some(SourceFormat::Csv),
            source_id_field: SourceIdAst {
                mode: SourceIdMode::Auto,
                field: "_datajig_source_id".into(),
            },
        },
        prepare: RunPrepareAst {
            generated_source_id: Some("_datajig_source_id".into()),
            steps: vec![],
        },
        transform: RunTransformAst::Sql {
            sql: "SELECT * FROM source ORDER BY _datajig_source_id".into(),
            generated: true,
        },
        export: RunExportAst {
            splits: TrainingSplitsAst {
                train: 80,
                val: 10,
                test: 10,
            },
            id_field: "_datajig_source_id".into(),
        },
    };

    let compiled = canonicalize_run_task(ast).unwrap();

    assert_eq!(
        compiled.canonical_json,
        r#"{"namespace":"datajig","kind":"run_task","schema_version":1,"source":{"path":"Data/Input.CSV","format":"csv","source_id_field":{"mode":"auto","field":"_datajig_source_id"}},"prepare":{"generated_source_id":"_datajig_source_id","steps":[]},"transform":{"kind":"sql","sql":"SELECT * FROM source ORDER BY _datajig_source_id","generated":true},"export":{"splits":{"train":80,"val":10,"test":10},"id_field":"_datajig_source_id"}}"#
    );
    assert!(compiled.intent_id.starts_with("intent_"));
    assert_eq!(compiled.ast.source.path, "Data/Input.CSV");
}

#[test]
fn identical_canonical_ast_has_identical_intent_identity() {
    let ast = RunTaskAst::identity("Records.JSONL", SourceFormat::Jsonl, "RecordID");

    let first = canonicalize_run_task(ast.clone()).unwrap();
    let second = canonicalize_run_task(ast).unwrap();

    assert_eq!(first.canonical_json, second.canonical_json);
    assert_eq!(first.intent_id, second.intent_id);
}

#[test]
fn keywords_whitespace_and_inferred_format_canonicalize_to_one_identity() {
    let inferred = compile_dsl(
        "  FROM   'Data Set/Input.CSV' SOURCE-ID-FIELD UserID EXPORT ID-FIELD UserID  ",
    )
    .unwrap();
    let explicit = compile_dsl(
        "from 'Data Set/Input.CSV' format csv source-id-field UserID export train 80 val 10 test 10 id-field UserID",
    )
    .unwrap();

    assert_eq!(inferred.ast.source.path, "Data Set/Input.CSV");
    assert_eq!(inferred.ast.source.format, Some(SourceFormat::Csv));
    assert_eq!(inferred.ast.source.source_id_field.field, "UserID");
    assert_eq!(inferred.ast.export.splits, TrainingSplitsAst::default());
    assert_eq!(inferred.canonical_json, explicit.canonical_json);
    assert_eq!(inferred.intent_id, explicit.intent_id);
}

#[test]
fn custom_splits_are_parsed_as_positive_percentages() {
    let compiled = compile_dsl(
        "from records.jsonl source-id-field id export train 70 val 20 test 10 id-field id",
    )
    .unwrap();

    assert_eq!(
        compiled.ast.export.splits,
        TrainingSplitsAst {
            train: 70,
            val: 20,
            test: 10,
        }
    );
}

#[test]
fn missing_identity_fields_report_exact_contract_locations() {
    let source_error = compile_dsl("from records.jsonl export id-field id").unwrap_err();
    assert_eq!(source_error.code, "INVALID_DSL");
    assert_eq!(source_error.location, "source.source-id-field");
    assert!(source_error.remediation.contains("source-id-field"));

    let final_error = compile_dsl("from records.jsonl source-id-field id export").unwrap_err();
    assert_eq!(final_error.code, "INVALID_DSL");
    assert_eq!(final_error.location, "export.id-field");
    assert!(final_error.remediation.contains("id-field"));
}

#[test]
fn malformed_split_contract_is_quantified_and_actionable() {
    for task in [
        "from records.jsonl source-id-field id export train 70 val 20 test 20 id-field id",
        "from records.jsonl source-id-field id export train 90 val 10 test 0 id-field id",
    ] {
        let error = compile_dsl(task).unwrap_err();
        assert_eq!(error.code, "INVALID_DSL");
        assert_eq!(error.location, "export.splits");
        assert!(error.message.contains("train="));
        assert!(error.remediation.contains("train 80 val 10 test 10"));
    }
}

#[test]
fn known_out_of_scope_syntax_returns_stable_unsupported_task() {
    for task in [
        "from https://example.test/data.csv source-id-field id export id-field id",
        "from left.csv join right.csv source-id-field id export id-field id",
        "from rows.csv source-id-field id loop export id-field id",
    ] {
        let error = compile_dsl(task).unwrap_err();
        assert_eq!(error.code, "UNSUPPORTED_TASK", "{task}: {error}");
        assert_eq!(error.location, "task");
    }
}

#[test]
fn parent_path_traversal_is_rejected_during_compilation() {
    let error =
        compile_dsl("from ../records.csv source-id-field id export id-field id").unwrap_err();

    assert_eq!(error.code, "INVALID_DSL");
    assert_eq!(error.location, "source.path");
    assert!(error.message.contains(".."));
}

#[test]
fn current_working_directory_is_rejected_as_a_source() {
    for path in [".", "./"] {
        let task = format!("from {path} source-id-field auto export id-field _datajig_source_id");
        let error = compile_dsl(&task).unwrap_err();

        assert_eq!(error.code, "INVALID_DSL");
        assert_eq!(error.location, "source.path");
        assert!(error.message.contains("current working directory"));
    }
}

#[test]
fn oversized_split_values_fail_without_integer_overflow() {
    let error = compile_dsl(
        "from rows.csv source-id-field id export train 65535 val 65535 test 65535 id-field id",
    )
    .unwrap_err();

    assert_eq!(error.code, "INVALID_DSL");
    assert_eq!(error.location, "export.splits");
    assert!(error.message.contains("65535"));
}

#[test]
fn unsupported_keywords_remain_valid_when_used_as_identifiers() {
    let compiled = compile_dsl("from join source-id-field loop export id-field branch").unwrap();

    assert_eq!(compiled.ast.source.path, "join");
    assert_eq!(compiled.ast.source.source_id_field.field, "loop");
    assert_eq!(compiled.ast.export.id_field, "branch");
}

#[test]
fn identity_helper_quotes_non_bare_final_ids() {
    let ast = RunTaskAst::identity("rows.csv", SourceFormat::Csv, "Record ID");

    assert_eq!(
        ast.transform,
        RunTransformAst::Sql {
            sql: "SELECT * FROM source ORDER BY \"Record ID\"".into(),
            generated: true,
        }
    );
}
