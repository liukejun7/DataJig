use datajig_core::{
    AggregateFunctionAst, PrepareStepAst, RunTransformAst, SourceIdMode, compile_dsl,
};

#[test]
fn auto_source_identity_forces_prepare_and_an_identity_transform() {
    let compiled =
        compile_dsl("from rows.csv source-id-field auto export id-field _datajig_source_id")
            .unwrap();

    assert_eq!(compiled.ast.source.source_id_field.mode, SourceIdMode::Auto);
    assert_eq!(
        compiled.ast.prepare.generated_source_id.as_deref(),
        Some("_datajig_source_id")
    );
    assert!(compiled.ast.prepare.steps.is_empty());
    assert_eq!(
        compiled.ast.transform,
        RunTransformAst::Sql {
            sql: "SELECT * FROM source ORDER BY _datajig_source_id".into(),
            generated: true,
        }
    );
}

#[test]
fn auto_source_identity_is_implicitly_retained_by_select_once() {
    let compiled = compile_dsl(
        "from rows.csv source-id-field auto prepare select name,score export id-field _datajig_source_id",
    )
    .unwrap();

    assert_eq!(
        compiled.ast.prepare.steps,
        vec![PrepareStepAst::Select {
            fields: vec!["name".into(), "score".into(), "_datajig_source_id".into(),],
        }]
    );
}

#[test]
fn explicit_transform_keeps_backtick_sql_and_uses_source_alias() {
    let compiled = compile_dsl(
        "from rows.csv source-id-field id transform `SELECT id, score FROM source ORDER BY id` export id-field id",
    )
    .unwrap();

    assert_eq!(
        compiled.ast.transform,
        RunTransformAst::Sql {
            sql: "SELECT id, score FROM source ORDER BY id".into(),
            generated: false,
        }
    );
}

#[test]
fn single_field_aggregate_compiles_all_functions_and_defaults_final_id() {
    let compiled = compile_dsl(
        "from rows.csv source-id-field auto aggregate by UserID \
         record_count=count(UserID),amount_sum=sum(amount),amount_avg=avg(amount),\
         amount_min=min(amount),amount_max=max(amount) export",
    )
    .unwrap();

    assert_eq!(compiled.ast.export.id_field, "UserID");
    let RunTransformAst::Aggregate {
        by,
        aggregations,
        order_by,
    } = compiled.ast.transform
    else {
        panic!("expected aggregate transform");
    };
    assert_eq!(by, "UserID");
    assert_eq!(order_by, "UserID");
    assert_eq!(aggregations.len(), 5);
    assert_eq!(aggregations[0].alias, "record_count");
    assert_eq!(aggregations[0].function, AggregateFunctionAst::Count);
    assert_eq!(aggregations[4].function, AggregateFunctionAst::Max);
}

#[test]
fn aggregate_aliases_reject_group_reserved_and_duplicate_collisions() {
    let cases = [
        "UserID=count(UserID)",
        "_datajig_source_id=count(UserID)",
        "counted=count(UserID),counted=sum(amount)",
    ];
    for aggregate in cases {
        let task =
            format!("from rows.csv source-id-field auto aggregate by UserID {aggregate} export");
        let error = compile_dsl(&task).unwrap_err();
        assert_eq!(error.code, "INVALID_DSL", "{aggregate}: {error}");
        assert!(
            error.location.starts_with("transform.aggregate["),
            "{aggregate}: {error}"
        );
        assert!(error.message.contains("alias"), "{aggregate}: {error}");
    }
}

#[test]
fn aggregate_rejects_unknown_functions_and_non_output_final_ids() {
    let function_error = compile_dsl(
        "from rows.csv source-id-field auto aggregate by UserID total=median(amount) export",
    )
    .unwrap_err();
    assert_eq!(function_error.location, "transform.aggregate[0].function");
    assert!(function_error.remediation.contains("count/sum/avg/min/max"));

    let id_error = compile_dsl(
        "from rows.csv source-id-field auto aggregate by UserID total=sum(amount) export id-field missing",
    )
    .unwrap_err();
    assert_eq!(id_error.location, "export.id-field");
    assert!(id_error.message.contains("aggregate output"));
}

#[test]
fn reserved_source_id_rejects_explicit_dsl_collisions() {
    for (task, location) in [
        (
            "from rows.csv source-id-field _datajig_source_id export id-field _datajig_source_id",
            "source.source-id-field",
        ),
        (
            "from rows.csv source-id-field auto prepare rename id to _datajig_source_id export id-field _datajig_source_id",
            "prepare[0].to",
        ),
    ] {
        let error = compile_dsl(task).unwrap_err();
        assert_eq!(error.code, "RESERVED_FIELD_CONFLICT");
        assert_eq!(error.location, location);
    }
}
