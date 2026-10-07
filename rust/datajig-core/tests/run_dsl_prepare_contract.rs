use datajig_core::{
    CaseModeAst, CastTypeAst, DedupeKeepAst, FilterPredicateAst, MissingModeAst, PrepareStepAst,
    compile_dsl,
};
use serde_json::json;

#[test]
fn all_ten_prepare_operations_compile_to_the_existing_protocol_shape() {
    let compiled = compile_dsl(
        "from input.csv source-id-field id prepare \
         select id,name, \
         filter score gte 1.00, \
         rename name to UserName, \
         cast score as number, \
         trim UserName, \
         case UserName upper, \
         replace status 'N/A' to null, \
         fill score with 0, \
         drop-missing all id,score, \
         dedupe by id keep error \
         export id-field id",
    )
    .unwrap();

    assert_eq!(compiled.ast.prepare.steps.len(), 10);
    assert_eq!(
        compiled.ast.prepare.steps[0],
        PrepareStepAst::Select {
            fields: vec!["id".into(), "name".into()],
        }
    );
    assert_eq!(
        compiled.ast.prepare.steps[1],
        PrepareStepAst::Filter {
            field: "score".into(),
            predicate: FilterPredicateAst::Gte,
            value: json!(1.0),
        }
    );
    assert_eq!(
        compiled.ast.prepare.steps[2],
        PrepareStepAst::Rename {
            from: "name".into(),
            to: "UserName".into(),
        }
    );
    assert_eq!(
        compiled.ast.prepare.steps[3],
        PrepareStepAst::Cast {
            field: "score".into(),
            value_type: CastTypeAst::Number,
        }
    );
    assert_eq!(
        compiled.ast.prepare.steps[4],
        PrepareStepAst::Trim {
            fields: vec!["UserName".into()],
        }
    );
    assert_eq!(
        compiled.ast.prepare.steps[5],
        PrepareStepAst::Case {
            fields: vec!["UserName".into()],
            mode: CaseModeAst::Upper,
        }
    );
    assert_eq!(
        compiled.ast.prepare.steps[6],
        PrepareStepAst::Replace {
            field: "status".into(),
            from: json!("N/A"),
            to: json!(null),
        }
    );
    assert_eq!(
        compiled.ast.prepare.steps[7],
        PrepareStepAst::FillMissing {
            field: "score".into(),
            value: json!(0),
        }
    );
    assert_eq!(
        compiled.ast.prepare.steps[8],
        PrepareStepAst::DropMissing {
            fields: vec!["id".into(), "score".into()],
            mode: MissingModeAst::All,
        }
    );
    assert_eq!(
        compiled.ast.prepare.steps[9],
        PrepareStepAst::Dedupe {
            by: vec!["id".into()],
            keep: DedupeKeepAst::Error,
        }
    );
}

#[test]
fn field_lists_keep_quoted_commas_and_prepare_defaults_are_explicit() {
    let compiled = compile_dsl(
        "from input.csv source-id-field id prepare \
         select 'last,name',score, drop-missing score, dedupe by 'last,name' \
         export id-field id",
    )
    .unwrap();

    assert_eq!(
        compiled.ast.prepare.steps[0],
        PrepareStepAst::Select {
            fields: vec!["last,name".into(), "score".into()],
        }
    );
    assert_eq!(
        compiled.ast.prepare.steps[1],
        PrepareStepAst::DropMissing {
            fields: vec!["score".into()],
            mode: MissingModeAst::Any,
        }
    );
    assert_eq!(
        compiled.ast.prepare.steps[2],
        PrepareStepAst::Dedupe {
            by: vec!["last,name".into()],
            keep: DedupeKeepAst::First,
        }
    );
}

#[test]
fn invalid_prepare_enums_report_the_step_and_property() {
    let cases = [
        (
            "filter score contains 'x'",
            "prepare[0].predicate",
            "eq/ne/lt/lte/gt/gte",
        ),
        (
            "cast score as decimal",
            "prepare[0].type",
            "string/integer/number/boolean",
        ),
        ("case name title", "prepare[0].mode", "upper/lower"),
        ("dedupe by id keep last", "prepare[0].keep", "first/error"),
    ];

    for (step, location, remediation) in cases {
        let task = format!("from input.csv source-id-field id prepare {step} export id-field id");
        let error = compile_dsl(&task).unwrap_err();
        assert_eq!(error.code, "INVALID_DSL", "{step}: {error}");
        assert_eq!(error.location, location, "{step}: {error}");
        assert!(error.remediation.contains(remediation), "{step}: {error}");
    }
}

#[test]
fn unquoted_string_scalars_are_rejected_with_a_fix_example() {
    let error = compile_dsl(
        "from input.csv source-id-field id prepare fill status with missing export id-field id",
    )
    .unwrap_err();

    assert_eq!(error.location, "prepare[0].value");
    assert!(error.remediation.contains("'missing'"));
}
