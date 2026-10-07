use datajig_core::{
    RunExportAst, RunPrepareAst, RunSourceAst, RunTaskAst, RunTransformAst, SourceFormat,
    SourceIdAst, SourceIdMode, TrainingSplitsAst, canonicalize_run_task,
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
