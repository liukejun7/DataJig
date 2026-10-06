use datajig_core::{
    TransformLimits, TransformOrderError, TransformSqlParseError, TransformSqlPolicyError,
    validate_transform_query,
};
use serde_json::{Value, json};
use std::collections::BTreeSet;

#[test]
fn allowed_transform_queries_are_structurally_authorized() {
    let cases: Vec<(&str, Vec<Value>, u64)> = vec![
        (
            "SELECT id, score FROM events WHERE score >= ? ORDER BY score DESC, id",
            vec![json!(7)],
            2,
        ),
        (
            "SELECT id, lower(name) AS name FROM events ORDER BY id",
            vec![],
            2,
        ),
        (
            "SELECT 'all' AS id, count(*) AS count FROM events",
            vec![],
            1,
        ),
        (
            "SELECT events.id AS id, labels.label FROM events JOIN labels ON events.id = labels.id ORDER BY id",
            vec![],
            2,
        ),
        (
            "SELECT id, score FROM (SELECT id, score FROM events WHERE score > 0) AS selected ORDER BY id",
            vec![],
            2,
        ),
        (
            "SELECT id, score FROM events UNION ALL SELECT id, score FROM labels ORDER BY id",
            vec![],
            2,
        ),
        (
            "SELECT id, CASE WHEN score > 0 THEN CAST(score AS VARCHAR) ELSE '0' END AS score FROM events ORDER BY id",
            vec![],
            2,
        ),
        (
            "SELECT id, coalesce(trim(name), '') AS name FROM events ORDER BY id LIMIT 10 OFFSET 0",
            vec![],
            2,
        ),
    ];
    let aliases = aliases();

    for (sql, parameters, rows) in cases {
        let validated =
            validate_transform_query(sql, &parameters, &aliases, "id", &TransformLimits::v1())
                .unwrap_or_else(|error| panic!("query should be allowed: {sql}: {error:#}"));
        assert!(validated.sql_content_id().starts_with("sql_"));
        assert!(validated.parameter_content_id().starts_with("params_"));
        assert_eq!(parameters.len(), validated.parameter_count());
        validated.validate_output_rows(rows).unwrap();
    }
}

#[test]
fn relation_and_statement_escape_routes_are_denied() {
    let denied = [
        "SELECT * FROM read_csv('/etc/passwd')",
        "SELECT * FROM READ_PARQUET('data/*.parquet')",
        "SELECT * FROM parquet_scan('https://example.test/data.parquet')",
        "SELECT * FROM query('events')",
        "SELECT * FROM main.events",
        "SELECT * FROM memory.main.events",
        "SELECT * FROM duckdb_tables()",
        "SELECT * FROM events; SELECT * FROM labels",
        "SELECT * FROM events /* hidden */;\nATTACH '/tmp/other.db'",
        "ATTACH '/tmp/other.db' AS other",
        "COPY events TO '/tmp/leak.csv'",
        "INSTALL httpfs",
        "LOAD httpfs",
        "PRAGMA database_list",
        "SET enable_external_access = true",
        "CREATE TABLE stolen AS SELECT * FROM events",
        "INSERT INTO events VALUES (1)",
        "WITH selected AS (SELECT * FROM events) SELECT * FROM selected",
        "SELECT id, row_number() OVER () FROM events ORDER BY id",
        "SELECT * FROM \"ｅvents\"",
        "SELECT * FROM (read_csv('/etc/passwd')) AS events",
    ];

    for sql in denied {
        let error = validate_transform_query(sql, &[], &aliases(), "id", &TransformLimits::v1())
            .expect_err(sql);
        assert!(
            error.downcast_ref::<TransformSqlPolicyError>().is_some()
                || error.downcast_ref::<TransformSqlParseError>().is_some(),
            "wrong error for {sql}: {error:#}"
        );
    }
}

#[test]
fn unknown_qualified_and_volatile_functions_are_denied() {
    let denied = [
        "SELECT id, random() FROM events ORDER BY id",
        "SELECT id, uuid() FROM events ORDER BY id",
        "SELECT id, now() FROM events ORDER BY id",
        "SELECT id, current_timestamp FROM events ORDER BY id",
        "SELECT id, getenv('HOME') FROM events ORDER BY id",
        "SELECT id, custom_macro(score) FROM events ORDER BY id",
        "SELECT id, pg_catalog.lower(name) FROM events ORDER BY id",
    ];

    for sql in denied {
        let error = validate_transform_query(sql, &[], &aliases(), "id", &TransformLimits::v1())
            .expect_err(sql);
        assert!(error.downcast_ref::<TransformSqlPolicyError>().is_some());
    }
}

#[test]
fn declared_relations_placeholders_and_scalar_parameters_are_exact() {
    let error = validate_transform_query(
        "SELECT id FROM missing ORDER BY id",
        &[],
        &aliases(),
        "id",
        &TransformLimits::v1(),
    )
    .unwrap_err();
    assert!(error.downcast_ref::<TransformSqlPolicyError>().is_some());

    for parameters in [vec![], vec![json!(1), json!(2)]] {
        let error = validate_transform_query(
            "SELECT id FROM events WHERE score > ? ORDER BY id",
            &parameters,
            &aliases(),
            "id",
            &TransformLimits::v1(),
        )
        .unwrap_err();
        assert!(error.downcast_ref::<TransformSqlPolicyError>().is_some());
    }

    for nested in [json!([1]), json!({"value": 1})] {
        let error = validate_transform_query(
            "SELECT id FROM events WHERE score > ? ORDER BY id",
            &[nested],
            &aliases(),
            "id",
            &TransformLimits::v1(),
        )
        .unwrap_err();
        assert!(error.downcast_ref::<TransformSqlPolicyError>().is_some());
    }
}

#[test]
fn multirow_output_requires_id_as_the_final_top_level_order_key() {
    let missing = validate_transform_query(
        "SELECT id, score FROM events",
        &[],
        &aliases(),
        "id",
        &TransformLimits::v1(),
    )
    .unwrap();
    missing.validate_output_rows(1).unwrap();
    let error = missing.validate_output_rows(2).unwrap_err();
    assert!(error.downcast_ref::<TransformOrderError>().is_some());

    let wrong = validate_transform_query(
        "SELECT id, score FROM events ORDER BY score",
        &[],
        &aliases(),
        "id",
        &TransformLimits::v1(),
    )
    .unwrap_err();
    assert!(wrong.downcast_ref::<TransformOrderError>().is_some());

    let qualified = validate_transform_query(
        "SELECT events.id AS id, score FROM events ORDER BY events.id",
        &[],
        &aliases(),
        "id",
        &TransformLimits::v1(),
    )
    .unwrap_err();
    assert!(qualified.downcast_ref::<TransformOrderError>().is_some());
}

#[test]
fn query_identity_preserves_exact_sql_and_canonical_parameters() {
    let first = validate_transform_query(
        "SELECT id FROM events ORDER BY id",
        &[json!(1)],
        &aliases(),
        "id",
        &TransformLimits::v1(),
    )
    .unwrap_err();
    assert!(first.downcast_ref::<TransformSqlPolicyError>().is_some());

    let first = validate_transform_query(
        "SELECT id FROM events WHERE score = ? ORDER BY id",
        &[json!(1)],
        &aliases(),
        "id",
        &TransformLimits::v1(),
    )
    .unwrap();
    let whitespace = validate_transform_query(
        "SELECT id FROM events WHERE score = ? ORDER BY id ",
        &[json!(1)],
        &aliases(),
        "id",
        &TransformLimits::v1(),
    )
    .unwrap();
    let parameter = validate_transform_query(
        "SELECT id FROM events WHERE score = ? ORDER BY id",
        &[json!(2)],
        &aliases(),
        "id",
        &TransformLimits::v1(),
    )
    .unwrap();

    assert_ne!(first.sql_content_id(), whitespace.sql_content_id());
    assert_eq!(
        first.parameter_content_id(),
        whitespace.parameter_content_id()
    );
    assert_ne!(
        first.parameter_content_id(),
        parameter.parameter_content_id()
    );
}

#[test]
fn excessive_query_nesting_is_rejected_before_parser_recursion() {
    let nested = format!(
        "SELECT {}id{} FROM events ORDER BY id",
        "(".repeat(129),
        ")".repeat(129)
    );
    let error = validate_transform_query(&nested, &[], &aliases(), "id", &TransformLimits::v1())
        .unwrap_err();
    assert!(error.downcast_ref::<TransformSqlPolicyError>().is_some());

    let literal = format!("SELECT '{}' AS id", "(".repeat(200).replace('(', "()"));
    validate_transform_query(&literal, &[], &aliases(), "id", &TransformLimits::v1()).unwrap();
}

fn aliases() -> BTreeSet<String> {
    ["events".to_owned(), "labels".to_owned()]
        .into_iter()
        .collect()
}
