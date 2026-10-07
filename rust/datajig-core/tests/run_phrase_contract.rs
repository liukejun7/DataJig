use datajig_core::{
    AggregateFunctionAst, CastTypeAst, FilterPredicateAst, PrepareStepAst, RunTransformAst,
    TaskTranslator, compile_task, compile_task_with_translator,
};
use serde_json::json;

#[test]
fn published_phrase_families_expand_to_complete_compilable_tasks() {
    let phrases = [
        "整理 data.csv",
        "把 data.csv 做成训练集",
        "把 data.csv 按 id 去重",
        "把 data.csv 填充 status 空值为 unknown",
        "把 data.csv 删掉 status 为空的行",
        "把 data.csv 的 old 改名为 new",
        "把 data.csv 的 score 转成数字",
        "把 data.csv 的 score 转成整数",
        "把 data.csv 的 score 转成文本",
        "把 data.csv 的 active 转成布尔",
        "把 data.csv 按 user_id 聚合",
        "统计 data.csv 每个 user_id 的数量",
        "统计 data.csv 每个 user_id 的 amount 总和",
        "统计 data.csv 每个 user_id 的 amount 均值",
        "统计 data.csv 每个 user_id 的 amount 最大",
        "统计 data.csv 每个 user_id 的 amount 最小",
        "把 data.csv 只保留 id,name,score",
        "把 data.csv 过滤 score 大于 10",
        "把 data.csv 过滤 score 小于 10",
        "把 data.csv 过滤 score 等于 10",
        "把 data.csv 切成 8:1:1",
    ];

    for phrase in phrases {
        compile_task(phrase).unwrap_or_else(|error| panic!("{phrase}: {error}"));
    }
}

#[test]
fn fill_value_is_mandatory_and_escaped_as_a_string_scalar() {
    let compiled = compile_task("把 data.csv 填充 publisher 空值为 O'Reilly").unwrap();
    assert_eq!(
        compiled.ast.prepare.steps[0],
        PrepareStepAst::FillMissing {
            field: "publisher".into(),
            value: json!("O'Reilly"),
        }
    );

    let error = compile_task("把 data.csv 填充 publisher 空值").unwrap_err();
    assert_eq!(error.code, "UNSUPPORTED_TASK");
    assert!(error.remediation.contains("空值为 <值>"));
}

#[test]
fn aggregate_phrases_generate_fixed_valid_aliases() {
    let generic = compile_task("把 data.csv 按 user_id 聚合").unwrap();
    let RunTransformAst::Aggregate { aggregations, .. } = generic.ast.transform else {
        panic!("expected aggregate");
    };
    assert_eq!(aggregations[0].alias, "record_count");
    assert_eq!(aggregations[0].function, AggregateFunctionAst::Count);

    let sum = compile_task("统计 data.csv 每个 user_id 的 amount 总和").unwrap();
    let RunTransformAst::Aggregate { aggregations, .. } = sum.ast.transform else {
        panic!("expected aggregate");
    };
    assert_eq!(aggregations[0].alias, "amount_sum");
    assert_eq!(aggregations[0].function, AggregateFunctionAst::Sum);
}

#[test]
fn phrase_values_and_enums_map_to_protocol_values() {
    let filtered = compile_task("把 data.csv 过滤 score 大于 10").unwrap();
    assert_eq!(
        filtered.ast.prepare.steps[0],
        PrepareStepAst::Filter {
            field: "score".into(),
            predicate: FilterPredicateAst::Gt,
            value: json!(10),
        }
    );

    let cast = compile_task("把 data.csv 的 score 转成整数").unwrap();
    assert_eq!(
        cast.ast.prepare.steps[0],
        PrepareStepAst::Cast {
            field: "score".into(),
            value_type: CastTypeAst::Integer,
        }
    );
}

#[test]
fn unsupported_classes_return_one_stable_error_with_template_guidance() {
    for task in [
        "合并 a.csv 和 b.csv",
        "整理 https://example.test/data.csv",
        "对 data.csv 循环处理",
        "用 mapping.csv 映射 data.csv",
        "把 data.csv 猜着处理一下",
    ] {
        let error = compile_task(task).unwrap_err();
        assert_eq!(error.code, "UNSUPPORTED_TASK", "{task}: {error}");
        assert!(!error.suggestions.is_empty(), "{task}: {error}");
    }
}

struct FakeTranslator;

impl TaskTranslator for FakeTranslator {
    fn translate(&self, _input: &str) -> Result<String, datajig_core::RunDslError> {
        Ok("from fake.jsonl source-id-field id export id-field id".into())
    }
}

#[test]
fn translator_seam_always_reuses_the_authoritative_dsl_compiler() {
    let compiled = compile_task_with_translator("provider input", &FakeTranslator).unwrap();

    assert_eq!(compiled.ast.source.path, "fake.jsonl");
    assert!(compiled.intent_id.starts_with("intent_"));
}
