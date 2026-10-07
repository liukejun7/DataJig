use super::RunDslError;
use crate::identity::blake3_content_id;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const RUN_DSL_SCHEMA_VERSION: u8 = 1;
pub const RUN_INTENT_ID_DOMAIN: &[u8] = b"datajig-run-intent-v1\0";

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunTaskAst {
    pub namespace: String,
    pub kind: String,
    pub schema_version: u8,
    pub source: RunSourceAst,
    pub prepare: RunPrepareAst,
    pub transform: RunTransformAst,
    pub export: RunExportAst,
}

impl RunTaskAst {
    pub fn identity(
        path: impl Into<String>,
        format: SourceFormat,
        id_field: impl Into<String>,
    ) -> Self {
        let id_field = id_field.into();
        Self {
            namespace: "datajig".into(),
            kind: "run_task".into(),
            schema_version: RUN_DSL_SCHEMA_VERSION,
            source: RunSourceAst {
                path: path.into(),
                format: Some(format),
                source_id_field: SourceIdAst {
                    mode: SourceIdMode::Field,
                    field: id_field.clone(),
                },
            },
            prepare: RunPrepareAst {
                generated_source_id: None,
                steps: Vec::new(),
            },
            transform: RunTransformAst::Sql {
                sql: format!(
                    "SELECT * FROM source ORDER BY {}",
                    sql_identifier(&id_field)
                ),
                generated: true,
            },
            export: RunExportAst {
                splits: TrainingSplitsAst::default(),
                id_field,
            },
        }
    }
}

pub(crate) fn sql_identifier(field: &str) -> String {
    let mut characters = field.chars();
    let bare = characters
        .next()
        .is_some_and(|character| character == '_' || character.is_ascii_alphabetic())
        && characters.all(|character| character == '_' || character.is_ascii_alphanumeric());
    if bare {
        field.to_owned()
    } else {
        format!("\"{}\"", field.replace('"', "\"\""))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunSourceAst {
    pub path: String,
    pub format: Option<SourceFormat>,
    pub source_id_field: SourceIdAst,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceFormat {
    Csv,
    Jsonl,
    Parquet,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct SourceIdAst {
    pub mode: SourceIdMode,
    pub field: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceIdMode {
    Auto,
    Field,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunPrepareAst {
    pub generated_source_id: Option<String>,
    pub steps: Vec<PrepareStepAst>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum PrepareStepAst {
    Select {
        fields: Vec<String>,
    },
    Filter {
        field: String,
        predicate: FilterPredicateAst,
        value: Value,
    },
    Rename {
        from: String,
        to: String,
    },
    Cast {
        field: String,
        #[serde(rename = "type")]
        value_type: CastTypeAst,
    },
    Trim {
        fields: Vec<String>,
    },
    Case {
        fields: Vec<String>,
        mode: CaseModeAst,
    },
    Replace {
        field: String,
        from: Value,
        to: Value,
    },
    FillMissing {
        field: String,
        value: Value,
    },
    DropMissing {
        fields: Vec<String>,
        mode: MissingModeAst,
    },
    Dedupe {
        by: Vec<String>,
        keep: DedupeKeepAst,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterPredicateAst {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CastTypeAst {
    String,
    Integer,
    Number,
    Boolean,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseModeAst {
    Lower,
    Upper,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingModeAst {
    Any,
    All,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DedupeKeepAst {
    First,
    Error,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunTransformAst {
    Sql {
        sql: String,
        generated: bool,
    },
    Aggregate {
        by: String,
        aggregations: Vec<AggregateAst>,
        order_by: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AggregateAst {
    pub alias: String,
    pub function: AggregateFunctionAst,
    pub field: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateFunctionAst {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunExportAst {
    pub splits: TrainingSplitsAst,
    pub id_field: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct TrainingSplitsAst {
    pub train: u16,
    pub val: u16,
    pub test: u16,
}

impl Default for TrainingSplitsAst {
    fn default() -> Self {
        Self {
            train: 80,
            val: 10,
            test: 10,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledRunTask {
    pub ast: RunTaskAst,
    pub canonical_json: String,
    pub intent_id: String,
}

pub fn canonicalize_run_task(ast: RunTaskAst) -> Result<CompiledRunTask, RunDslError> {
    let canonical_json = serde_json::to_string(&ast)
        .map_err(|error| RunDslError::internal(format!("cannot serialize run AST: {error}")))?;
    let intent_id = blake3_content_id("intent", RUN_INTENT_ID_DOMAIN, canonical_json.as_bytes());
    Ok(CompiledRunTask {
        ast,
        canonical_json,
        intent_id,
    })
}
