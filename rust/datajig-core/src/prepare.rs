use crate::InvalidArgumentError;
use crate::identity::{ARTIFACT_NAMESPACE, blake3_content_id};
use crate::io::{publish_new_noreplace, save_new_file_atomically};
use crate::jsonl_inspect::{compare_json_numbers, jsonl_record_id_digest};
use crate::prepare_source::{PrepareSourceFormat, ResolvedPrepareSource};
use crate::strict_json::reject_duplicate_json_members;
use crate::tabular_source::{TabularConsumer, TabularRow};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write, sink};
use std::path::{Path, PathBuf};

pub const PREPARE_RECIPE_SCHEMA_VERSION: u8 = 1;
const PREPARE_PLAN_SCHEMA_VERSION: u8 = 1;
const PREPARE_RECEIPT_SCHEMA_VERSION: u8 = 1;
const MAX_RECIPE_BYTES: usize = 64 * 1024;
const MAX_PLAN_BYTES: usize = 256 * 1024;
const MAX_STEPS: usize = 64;
pub(crate) const MAX_FIELDS: usize = 1024;
pub(crate) const MAX_FIELD_BYTES: usize = 1024;
pub(crate) const MAX_CELL_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_ROWS: usize = 5_000_000;
pub(crate) const MAX_OUTPUT_LINE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug)]
pub struct PrepareInvalidDataError(String);

impl PrepareInvalidDataError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for PrepareInvalidDataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for PrepareInvalidDataError {}

#[derive(Debug)]
pub struct PrepareNotAuthorizedError;

impl fmt::Display for PrepareNotAuthorizedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("accepted preparation plan identity does not match")
    }
}

impl Error for PrepareNotAuthorizedError {}

#[derive(Debug)]
pub struct StalePrepareInputError;

impl fmt::Display for StalePrepareInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("preparation source or recipe changed after planning")
    }
}

impl Error for StalePrepareInputError {}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PrepareRecipe {
    namespace: String,
    kind: String,
    schema_version: u8,
    source: PrepareSource,
    output: PrepareOutput,
    id_field: String,
    #[serde(default)]
    generated_source_id: Option<String>,
    steps: Vec<PrepareStep>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "format", rename_all = "snake_case", deny_unknown_fields)]
enum PrepareSource {
    Csv {
        #[serde(default = "default_delimiter")]
        delimiter: String,
        #[serde(default)]
        include: Vec<String>,
        #[serde(default)]
        ignore: Vec<String>,
    },
    Parquet {
        #[serde(default)]
        include: Vec<String>,
        #[serde(default)]
        ignore: Vec<String>,
    },
    Jsonl {
        #[serde(default)]
        include: Vec<String>,
        #[serde(default)]
        ignore: Vec<String>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PrepareOutput {
    format: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum PrepareStep {
    Filter {
        field: String,
        predicate: FilterPredicate,
        value: Value,
    },
    Select {
        fields: Vec<String>,
    },
    Rename {
        from: String,
        to: String,
    },
    Cast {
        field: String,
        #[serde(rename = "type")]
        value_type: CastType,
    },
    Trim {
        fields: Vec<String>,
    },
    Case {
        fields: Vec<String>,
        mode: CaseMode,
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
        mode: MissingMode,
    },
    Dedupe {
        by: Vec<String>,
        keep: DedupeKeep,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum FilterPredicate {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CastType {
    String,
    Integer,
    Number,
    Boolean,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CaseMode {
    Lower,
    Upper,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum MissingMode {
    Any,
    All,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum DedupeKeep {
    First,
    Error,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PreparePlan {
    namespace: String,
    kind: String,
    schema_version: u8,
    plan_id: String,
    source_path: String,
    source_content_id: String,
    recipe_path: String,
    recipe_id: String,
    id_field: String,
    output_path: String,
    output_content_id: String,
    source_rows: usize,
    output_rows: usize,
    filtered_rows: usize,
    duplicate_rows: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct PreparePlanArtifact {
    pub schema_version: u8,
    pub plan_id: String,
    pub source_content_id: String,
    pub recipe_id: String,
    pub id_field: String,
    pub output_content_id: String,
    pub source_rows: usize,
    pub output_rows: usize,
    pub filtered_rows: usize,
    pub duplicate_rows: usize,
    pub plan: String,
    pub output: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct PrepareApplyArtifact {
    pub schema_version: u8,
    pub outcome: String,
    pub plan_id: String,
    pub source_content_id: String,
    pub recipe_id: String,
    pub id_field: String,
    pub output_content_id: String,
    pub output_rows: usize,
    pub output: String,
    pub receipt: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PrepareReceipt {
    namespace: String,
    kind: String,
    schema_version: u8,
    plan_id: String,
    source_content_id: String,
    recipe_id: String,
    id_field: String,
    output_content_id: String,
    output_rows: usize,
}

#[derive(Serialize)]
struct PrepareIdentity<'a> {
    schema_version: u8,
    source_path: &'a str,
    source_content_id: &'a str,
    recipe_path: &'a str,
    recipe_id: &'a str,
    id_field: &'a str,
    output_path: &'a str,
    output_content_id: &'a str,
    source_rows: usize,
    output_rows: usize,
    filtered_rows: usize,
    duplicate_rows: usize,
}

#[derive(Clone, Debug)]
struct ExecutionSummary {
    source_rows: usize,
    output_rows: usize,
    filtered_rows: usize,
    duplicate_rows: usize,
    output_content_id: String,
}

fn default_delimiter() -> String {
    ",".into()
}

pub fn plan_prepare(
    source: &Path,
    recipe_path: &Path,
    output: &Path,
    plan_path: &Path,
) -> Result<PreparePlanArtifact> {
    if output.exists() || receipt_path(output).exists() {
        return Err(InvalidArgumentError::new(
            "preparation output and receipt must name new files",
        )
        .into());
    }
    if plan_path.exists() {
        return Err(
            InvalidArgumentError::new("preparation plan output must name a new file").into(),
        );
    }
    let recipe_path = canonical_regular_file(recipe_path, "preparation recipe")?;
    let output = resolve_new_path(output, "preparation output")?;
    let plan_path = resolve_new_path(plan_path, "preparation plan")?;
    if output.extension().and_then(|extension| extension.to_str()) != Some("jsonl") {
        return Err(
            InvalidArgumentError::new("preparation output must use the .jsonl extension").into(),
        );
    }
    if output == plan_path || receipt_path(&output) == plan_path {
        return Err(InvalidArgumentError::new(
            "preparation output, receipt, and plan paths must be distinct",
        )
        .into());
    }
    let (recipe, recipe_id) = load_recipe(&recipe_path)?;
    let resolved_source = resolve_source(source, &recipe)?;
    let source_content_id = resolved_source.content_id().to_owned();
    let summary = execute(&resolved_source, &recipe, sink())?;
    if resolved_source.verify_unchanged().is_err() {
        return Err(StalePrepareInputError.into());
    }
    let source_path = path_text(resolved_source.source_path())?;
    let recipe_path_text = path_text(&recipe_path)?;
    let output_path = path_text(&output)?;
    let identity = PrepareIdentity {
        schema_version: PREPARE_PLAN_SCHEMA_VERSION,
        source_path: &source_path,
        source_content_id: &source_content_id,
        recipe_path: &recipe_path_text,
        recipe_id: &recipe_id,
        id_field: &recipe.id_field,
        output_path: &output_path,
        output_content_id: &summary.output_content_id,
        source_rows: summary.source_rows,
        output_rows: summary.output_rows,
        filtered_rows: summary.filtered_rows,
        duplicate_rows: summary.duplicate_rows,
    };
    let identity_payload = serde_json::to_vec(&identity)?;
    let plan_id = blake3_content_id("prep", b"datajig-prepare-plan-v1\0", &identity_payload);
    let plan = PreparePlan {
        namespace: ARTIFACT_NAMESPACE.into(),
        kind: "prepare_plan".into(),
        schema_version: PREPARE_PLAN_SCHEMA_VERSION,
        plan_id: plan_id.clone(),
        source_path,
        source_content_id: source_content_id.clone(),
        recipe_path: recipe_path_text,
        recipe_id: recipe_id.clone(),
        id_field: recipe.id_field.clone(),
        output_path,
        output_content_id: summary.output_content_id.clone(),
        source_rows: summary.source_rows,
        output_rows: summary.output_rows,
        filtered_rows: summary.filtered_rows,
        duplicate_rows: summary.duplicate_rows,
    };
    let payload = serde_json::to_vec_pretty(&plan)?;
    if payload.len() > MAX_PLAN_BYTES {
        bail!("preparation plan exceeds {MAX_PLAN_BYTES} bytes");
    }
    save_new_file_atomically(&plan_path, &payload, "preparation plan")?;
    Ok(PreparePlanArtifact {
        schema_version: PREPARE_PLAN_SCHEMA_VERSION,
        plan_id,
        source_content_id,
        recipe_id,
        id_field: recipe.id_field,
        output_content_id: summary.output_content_id,
        source_rows: summary.source_rows,
        output_rows: summary.output_rows,
        filtered_rows: summary.filtered_rows,
        duplicate_rows: summary.duplicate_rows,
        plan: path_text(&plan_path)?,
        output: path_text(&output)?,
    })
}

pub fn apply_prepare(plan_path: &Path, accept_plan: &str) -> Result<PrepareApplyArtifact> {
    let plan_path = canonical_regular_file(plan_path, "preparation plan")?;
    let plan = load_plan(&plan_path)?;
    if plan.plan_id != accept_plan {
        return Err(PrepareNotAuthorizedError.into());
    }
    let source_path = PathBuf::from(&plan.source_path);
    let recipe_path = PathBuf::from(&plan.recipe_path);
    let output = PathBuf::from(&plan.output_path);
    let receipt = receipt_path(&output);
    if output.exists() || receipt.exists() {
        if output.is_file() && receipt.is_file() {
            let existing = load_receipt(&receipt)?;
            if existing.plan_id == plan.plan_id
                && existing.source_content_id == plan.source_content_id
                && existing.recipe_id == plan.recipe_id
                && existing.id_field == plan.id_field
                && existing.output_content_id == plan.output_content_id
                && existing.output_rows == plan.output_rows
                && hash_prepared_output(&output)? == plan.output_content_id
            {
                return apply_artifact(&plan, &output, &receipt, "already_applied");
            }
        }
        if output.is_file()
            && !receipt.exists()
            && hash_prepared_output(&output)? == plan.output_content_id
        {
            save_receipt(&plan, &receipt)?;
            return apply_artifact(&plan, &output, &receipt, "recovered");
        }
        return Err(InvalidArgumentError::new(
            "preparation output or receipt already exists and does not match the accepted plan",
        )
        .into());
    }
    let (recipe, recipe_id) =
        load_recipe(&recipe_path).map_err(|_| anyhow::Error::new(StalePrepareInputError))?;
    if recipe_id != plan.recipe_id || recipe.id_field != plan.id_field {
        return Err(StalePrepareInputError.into());
    }
    let source = resolve_source(&source_path, &recipe)
        .map_err(|_| anyhow::Error::new(StalePrepareInputError))?;
    if source.content_id() != plan.source_content_id {
        return Err(StalePrepareInputError.into());
    }
    let (temporary_path, temporary_file) = allocate_output_temp(&output)?;
    let result = (|| -> Result<PrepareApplyArtifact> {
        let summary = execute(&source, &recipe, temporary_file)?;
        if source.verify_unchanged().is_err()
            || summary.output_content_id != plan.output_content_id
            || summary.source_rows != plan.source_rows
            || summary.output_rows != plan.output_rows
            || summary.filtered_rows != plan.filtered_rows
            || summary.duplicate_rows != plan.duplicate_rows
        {
            return Err(StalePrepareInputError.into());
        }
        publish_temp_new(&temporary_path, &output)?;
        if let Err(error) = save_receipt(&plan, &receipt) {
            let _ = fs::remove_file(&output);
            return Err(error);
        }
        apply_artifact(&plan, &output, &receipt, "applied")
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

fn save_receipt(plan: &PreparePlan, receipt: &Path) -> Result<()> {
    let value = PrepareReceipt {
        namespace: ARTIFACT_NAMESPACE.into(),
        kind: "prepare_receipt".into(),
        schema_version: PREPARE_RECEIPT_SCHEMA_VERSION,
        plan_id: plan.plan_id.clone(),
        source_content_id: plan.source_content_id.clone(),
        recipe_id: plan.recipe_id.clone(),
        id_field: plan.id_field.clone(),
        output_content_id: plan.output_content_id.clone(),
        output_rows: plan.output_rows,
    };
    let payload = serde_json::to_vec_pretty(&value)?;
    save_new_file_atomically(receipt, &payload, "preparation receipt")
}

fn apply_artifact(
    plan: &PreparePlan,
    output: &Path,
    receipt: &Path,
    outcome: &str,
) -> Result<PrepareApplyArtifact> {
    Ok(PrepareApplyArtifact {
        schema_version: PREPARE_RECEIPT_SCHEMA_VERSION,
        outcome: outcome.into(),
        plan_id: plan.plan_id.clone(),
        source_content_id: plan.source_content_id.clone(),
        recipe_id: plan.recipe_id.clone(),
        id_field: plan.id_field.clone(),
        output_content_id: plan.output_content_id.clone(),
        output_rows: plan.output_rows,
        output: path_text(output)?,
        receipt: path_text(receipt)?,
    })
}

fn load_receipt(path: &Path) -> Result<PrepareReceipt> {
    let payload = read_bounded(path, MAX_PLAN_BYTES, "preparation receipt")?;
    let text = std::str::from_utf8(&payload)
        .map_err(|_| InvalidArgumentError::new("preparation receipt is not valid UTF-8"))?;
    reject_duplicate_json_members(text)
        .map_err(|_| InvalidArgumentError::new("preparation receipt is invalid"))?;
    let receipt: PrepareReceipt = serde_json::from_str(text)
        .map_err(|_| InvalidArgumentError::new("preparation receipt is invalid"))?;
    if receipt.namespace != ARTIFACT_NAMESPACE
        || receipt.kind != "prepare_receipt"
        || receipt.schema_version != PREPARE_RECEIPT_SCHEMA_VERSION
    {
        return Err(InvalidArgumentError::new(
            "unsupported preparation receipt identity or schema",
        )
        .into());
    }
    Ok(receipt)
}

fn load_recipe(path: &Path) -> Result<(PrepareRecipe, String)> {
    let payload = read_bounded(path, MAX_RECIPE_BYTES, "preparation recipe")?;
    let text = std::str::from_utf8(&payload)
        .map_err(|_| InvalidArgumentError::new("preparation recipe is not valid UTF-8"))?;
    reject_duplicate_json_members(text)
        .map_err(|_| InvalidArgumentError::new("preparation recipe is invalid"))?;
    let recipe: PrepareRecipe = serde_json::from_str(text).map_err(|error| {
        InvalidArgumentError::new(format!("preparation recipe is invalid: {error}"))
    })?;
    validate_recipe(&recipe)?;
    let canonical = serde_json::to_vec(&recipe)?;
    let recipe_id = blake3_content_id("recipe", b"datajig-prepare-recipe-v1\0", &canonical);
    Ok((recipe, recipe_id))
}

fn validate_recipe(recipe: &PrepareRecipe) -> Result<()> {
    if recipe.namespace != ARTIFACT_NAMESPACE {
        return Err(recipe_contract_error("preparation recipe namespace must be 'datajig'").into());
    }
    if recipe.kind != "prepare" {
        return Err(recipe_contract_error("preparation recipe kind must be 'prepare'").into());
    }
    if recipe.schema_version != PREPARE_RECIPE_SCHEMA_VERSION {
        return Err(recipe_contract_error(format!(
            "preparation recipe schema_version {} is unsupported; expected {}",
            recipe.schema_version, PREPARE_RECIPE_SCHEMA_VERSION
        ))
        .into());
    }
    if recipe.output.format != "jsonl" {
        return Err(
            recipe_contract_error("preparation recipe output.format must be 'jsonl'").into(),
        );
    }
    if let PrepareSource::Csv { delimiter, .. } = &recipe.source {
        if delimiter_byte(delimiter).is_none() {
            return Err(
                InvalidArgumentError::new("CSV delimiter must be exactly one ASCII byte").into(),
            );
        }
    }
    let (_, includes, ignores) = source_parts(&recipe.source)?;
    validate_source_patterns(includes, "include")?;
    validate_source_patterns(ignores, "ignore")?;
    validate_field(&recipe.id_field)?;
    if let Some(field) = &recipe.generated_source_id {
        validate_field(field)?;
    }
    if recipe.steps.len() > MAX_STEPS {
        return Err(InvalidArgumentError::new(format!(
            "preparation recipe exceeds {MAX_STEPS} steps"
        ))
        .into());
    }
    for step in &recipe.steps {
        match step {
            PrepareStep::Filter { field, value, .. } => {
                validate_field(field)?;
                if matches!(value, Value::Array(_) | Value::Object(_)) {
                    return Err(
                        InvalidArgumentError::new("filter values must be JSON scalars").into(),
                    );
                }
            }
            PrepareStep::Select { fields } => validate_fields(fields)?,
            PrepareStep::Rename { from, to } => {
                validate_field(from)?;
                validate_field(to)?;
            }
            PrepareStep::Cast { field, .. } => validate_field(field)?,
            PrepareStep::Trim { fields } | PrepareStep::Case { fields, .. } => {
                validate_fields(fields)?
            }
            PrepareStep::Replace { field, from, to } => {
                validate_field(field)?;
                validate_scalar(from, "replace from")?;
                validate_scalar(to, "replace to")?;
            }
            PrepareStep::FillMissing { field, value } => {
                validate_field(field)?;
                validate_scalar(value, "fill_missing value")?;
            }
            PrepareStep::DropMissing { fields, .. } => validate_fields(fields)?,
            PrepareStep::Dedupe { by, .. } => validate_fields(by)?,
        }
    }
    Ok(())
}

fn recipe_contract_error(message: impl Into<String>) -> InvalidArgumentError {
    InvalidArgumentError::new(message).with_remediation(
        "Inspect the complete preparation recipe schema and canonical example.",
        "artifact-schema",
        vec!["prepare-recipe".into()],
    )
}

fn execute<W: Write>(
    source: &ResolvedPrepareSource,
    recipe: &PrepareRecipe,
    writer: W,
) -> Result<ExecutionSummary> {
    let mut execution = PrepareExecution::new(recipe, writer);
    source.stream_with_generated_id(&mut execution, recipe.generated_source_id.as_deref())?;
    execution.finish()
}

fn resolve_source(source: &Path, recipe: &PrepareRecipe) -> Result<ResolvedPrepareSource> {
    let (format, includes, ignores) = source_parts(&recipe.source)?;
    ResolvedPrepareSource::resolve(source, format, includes, ignores)
}

fn source_parts(source: &PrepareSource) -> Result<(PrepareSourceFormat, &[String], &[String])> {
    Ok(match source {
        PrepareSource::Csv {
            delimiter,
            include,
            ignore,
        } => (
            PrepareSourceFormat::Csv {
                delimiter: delimiter_byte(delimiter).ok_or_else(|| {
                    InvalidArgumentError::new("CSV delimiter must be exactly one ASCII byte")
                })?,
            },
            include,
            ignore,
        ),
        PrepareSource::Parquet { include, ignore } => {
            (PrepareSourceFormat::Parquet, include, ignore)
        }
        PrepareSource::Jsonl { include, ignore } => (PrepareSourceFormat::Jsonl, include, ignore),
    })
}

fn validate_source_patterns(patterns: &[String], label: &str) -> Result<()> {
    if patterns.len() > 64 {
        return Err(InvalidArgumentError::new(format!(
            "preparation source exceeds 64 {label} globs"
        ))
        .into());
    }
    if patterns
        .iter()
        .any(|pattern| pattern.is_empty() || pattern.len() > 1024)
    {
        return Err(InvalidArgumentError::new(format!(
            "preparation source {label} globs must contain 1..=1024 bytes"
        ))
        .into());
    }
    Ok(())
}

struct PrepareExecution<'a, W> {
    recipe: &'a PrepareRecipe,
    writer: W,
    headers_seen: bool,
    dedupe_sets: Vec<Option<HashSet<Vec<u8>>>>,
    output_ids: HashSet<[u8; 32]>,
    source_rows: usize,
    output_rows: usize,
    filtered_rows: usize,
    duplicate_rows: usize,
    output_hasher: blake3::Hasher,
}

impl<'a, W: Write> PrepareExecution<'a, W> {
    fn new(recipe: &'a PrepareRecipe, writer: W) -> Self {
        let mut output_hasher = blake3::Hasher::new();
        output_hasher.update(b"datajig-prepared-jsonl-v1\0");
        Self {
            recipe,
            writer,
            headers_seen: false,
            dedupe_sets: recipe
                .steps
                .iter()
                .map(|step| matches!(step, PrepareStep::Dedupe { .. }).then(HashSet::new))
                .collect(),
            output_ids: HashSet::new(),
            source_rows: 0,
            output_rows: 0,
            filtered_rows: 0,
            duplicate_rows: 0,
            output_hasher,
        }
    }

    fn finish(mut self) -> Result<ExecutionSummary> {
        if !self.headers_seen {
            return Err(PrepareInvalidDataError::new("preparation source has no schema").into());
        }
        self.writer.flush()?;
        let output_content_id = format!("prepared_{}", self.output_hasher.finalize().to_hex());
        Ok(ExecutionSummary {
            source_rows: self.source_rows,
            output_rows: self.output_rows,
            filtered_rows: self.filtered_rows,
            duplicate_rows: self.duplicate_rows,
            output_content_id,
        })
    }
}

impl<W: Write> TabularConsumer for PrepareExecution<'_, W> {
    fn headers(&mut self, headers: &[String]) -> Result<()> {
        if self.headers_seen {
            return Err(PrepareInvalidDataError::new(
                "preparation source reported its schema more than once",
            )
            .into());
        }
        validate_headers(headers)?;
        validate_pipeline_headers(headers, self.recipe)?;
        self.headers_seen = true;
        Ok(())
    }

    fn row(&mut self, mut row: TabularRow) -> Result<()> {
        self.source_rows = self
            .source_rows
            .checked_add(1)
            .context("preparation row count overflow")?;
        if self.source_rows > MAX_ROWS {
            return Err(PrepareInvalidDataError::new(format!(
                "preparation source exceeds {MAX_ROWS} rows"
            ))
            .into());
        }
        let mut keep = true;
        for (index, step) in self.recipe.steps.iter().enumerate() {
            let duplicates_before = self.duplicate_rows;
            if !apply_step(
                step,
                &mut row,
                self.dedupe_sets[index].as_mut(),
                self.source_rows,
                &mut self.duplicate_rows,
            )? {
                if self.duplicate_rows == duplicates_before {
                    self.filtered_rows += 1;
                }
                keep = false;
                break;
            }
        }
        if !keep {
            return Ok(());
        }
        let id = row.get(&self.recipe.id_field).ok_or_else(|| {
            PrepareInvalidDataError::new(format!(
                "prepared row {} is missing id field {:?}",
                self.source_rows, self.recipe.id_field
            ))
        })?;
        let id_key = jsonl_record_id_digest(id).ok_or_else(|| {
            PrepareInvalidDataError::new(format!(
                "prepared row {} has an invalid id field",
                self.source_rows
            ))
        })?;
        if !self.output_ids.insert(id_key) {
            return Err(PrepareInvalidDataError::new(format!(
                "prepared output contains duplicate id at source row {}",
                self.source_rows
            ))
            .into());
        }
        let line = serde_json::to_vec(&row)?;
        if line.len() > MAX_OUTPUT_LINE_BYTES {
            return Err(PrepareInvalidDataError::new(format!(
                "prepared output row exceeds {MAX_OUTPUT_LINE_BYTES} bytes"
            ))
            .into());
        }
        self.writer.write_all(&line)?;
        self.writer.write_all(b"\n")?;
        self.output_hasher.update(&line);
        self.output_hasher.update(b"\n");
        self.output_rows += 1;
        Ok(())
    }
}

fn apply_step(
    step: &PrepareStep,
    row: &mut BTreeMap<String, Value>,
    dedupe_set: Option<&mut HashSet<Vec<u8>>>,
    source_row: usize,
    duplicate_rows: &mut usize,
) -> Result<bool> {
    match step {
        PrepareStep::Filter {
            field,
            predicate,
            value,
        } => {
            let candidate = row.get(field).ok_or_else(|| {
                PrepareInvalidDataError::new(format!(
                    "source row {source_row} is missing filter field {field:?}"
                ))
            })?;
            Ok(compare_filter(candidate, value, *predicate)?)
        }
        PrepareStep::Select { fields } => {
            let mut selected = BTreeMap::new();
            for field in fields {
                let value = row.remove(field).ok_or_else(|| {
                    PrepareInvalidDataError::new(format!(
                        "source row {source_row} is missing selected field {field:?}"
                    ))
                })?;
                selected.insert(field.clone(), value);
            }
            *row = selected;
            Ok(true)
        }
        PrepareStep::Rename { from, to } => {
            if from != to && row.contains_key(to) {
                return Err(PrepareInvalidDataError::new(format!(
                    "source row {source_row} rename target {to:?} already exists"
                ))
                .into());
            }
            let value = row.remove(from).ok_or_else(|| {
                PrepareInvalidDataError::new(format!(
                    "source row {source_row} is missing rename field {from:?}"
                ))
            })?;
            row.insert(to.clone(), value);
            Ok(true)
        }
        PrepareStep::Cast { field, value_type } => {
            let value = row.get_mut(field).ok_or_else(|| {
                PrepareInvalidDataError::new(format!(
                    "source row {source_row} is missing cast field {field:?}"
                ))
            })?;
            *value = cast_value(value, *value_type).map_err(|_| {
                PrepareInvalidDataError::new(format!(
                    "source row {source_row} cannot cast field {field:?}"
                ))
            })?;
            Ok(true)
        }
        PrepareStep::Trim { fields } => {
            for field in fields {
                let value = row.get_mut(field).ok_or_else(|| {
                    PrepareInvalidDataError::new(format!(
                        "source row {source_row} is missing trim field {field:?}"
                    ))
                })?;
                let text = value.as_str().ok_or_else(|| {
                    PrepareInvalidDataError::new(format!(
                        "source row {source_row} cannot trim non-string field {field:?}"
                    ))
                })?;
                *value = Value::String(text.trim().into());
            }
            Ok(true)
        }
        PrepareStep::Case { fields, mode } => {
            for field in fields {
                let value = row.get_mut(field).ok_or_else(|| {
                    PrepareInvalidDataError::new(format!(
                        "source row {source_row} is missing case field {field:?}"
                    ))
                })?;
                let text = value.as_str().ok_or_else(|| {
                    PrepareInvalidDataError::new(format!(
                        "source row {source_row} cannot change case of non-string field {field:?}"
                    ))
                })?;
                *value = Value::String(match mode {
                    CaseMode::Lower => text.to_lowercase(),
                    CaseMode::Upper => text.to_uppercase(),
                });
            }
            Ok(true)
        }
        PrepareStep::Replace { field, from, to } => {
            let value = row.get_mut(field).ok_or_else(|| {
                PrepareInvalidDataError::new(format!(
                    "source row {source_row} is missing replace field {field:?}"
                ))
            })?;
            if scalar_equal(value, from) {
                *value = to.clone();
            }
            Ok(true)
        }
        PrepareStep::FillMissing { field, value } => {
            let candidate = row.get_mut(field).ok_or_else(|| {
                PrepareInvalidDataError::new(format!(
                    "source row {source_row} is missing fill_missing field {field:?}"
                ))
            })?;
            if is_missing(candidate) {
                *candidate = value.clone();
            }
            Ok(true)
        }
        PrepareStep::DropMissing { fields, mode } => {
            let missing = fields
                .iter()
                .map(|field| {
                    row.get(field).map(is_missing).ok_or_else(|| {
                        PrepareInvalidDataError::new(format!(
                            "source row {source_row} is missing drop_missing field {field:?}"
                        ))
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(!match mode {
                MissingMode::Any => missing.iter().any(|value| *value),
                MissingMode::All => missing.iter().all(|value| *value),
            })
        }
        PrepareStep::Dedupe { by, keep } => {
            let set = dedupe_set.expect("dedupe step has state");
            let key = by
                .iter()
                .map(|field| {
                    row.get(field).cloned().ok_or_else(|| {
                        PrepareInvalidDataError::new(format!(
                            "source row {source_row} is missing dedupe field {field:?}"
                        ))
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            if set.insert(serde_json::to_vec(&key)?) {
                Ok(true)
            } else {
                *duplicate_rows += 1;
                match keep {
                    DedupeKeep::First => Ok(false),
                    DedupeKeep::Error => Err(PrepareInvalidDataError::new(format!(
                        "source row {source_row} duplicates a prior dedupe key"
                    ))
                    .into()),
                }
            }
        }
    }
}

fn is_missing(value: &Value) -> bool {
    matches!(value, Value::Null) || matches!(value, Value::String(text) if text.is_empty())
}

fn scalar_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Number(left), Value::Number(right)) => compare_json_numbers(left, right).is_eq(),
        _ => left == right,
    }
}

fn compare_filter(candidate: &Value, expected: &Value, predicate: FilterPredicate) -> Result<bool> {
    let ordering = match (candidate, expected) {
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::Number(left), Value::Number(right)) => compare_json_numbers(left, right),
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
        (Value::String(left), Value::Number(right)) => {
            let left: Number = serde_json::from_str(left).map_err(|_| {
                PrepareInvalidDataError::new("numeric filter candidate is not a number")
            })?;
            compare_json_numbers(&left, right)
        }
        (Value::String(left), Value::Bool(right)) => {
            let left = parse_bool(left).ok_or_else(|| {
                PrepareInvalidDataError::new("boolean filter candidate is invalid")
            })?;
            left.cmp(right)
        }
        (Value::String(left), Value::Null) => {
            if left.is_empty() {
                std::cmp::Ordering::Equal
            } else {
                std::cmp::Ordering::Greater
            }
        }
        _ => {
            return Err(PrepareInvalidDataError::new(
                "string filters compare source cells with scalar recipe values",
            )
            .into());
        }
    };
    Ok(match predicate {
        FilterPredicate::Eq => ordering.is_eq(),
        FilterPredicate::Ne => !ordering.is_eq(),
        FilterPredicate::Lt => ordering.is_lt(),
        FilterPredicate::Lte => !ordering.is_gt(),
        FilterPredicate::Gt => ordering.is_gt(),
        FilterPredicate::Gte => !ordering.is_lt(),
    })
}

fn cast_value(value: &Value, value_type: CastType) -> Result<Value> {
    let text = value
        .as_str()
        .ok_or_else(|| PrepareInvalidDataError::new("cast source is not a string"))?;
    match value_type {
        CastType::String => Ok(Value::String(text.into())),
        CastType::Integer => {
            let value: i64 = text.parse().map_err(anyhow::Error::from)?;
            Ok(Value::Number(value.into()))
        }
        CastType::Number => {
            let value: Number = serde_json::from_str(text)?;
            Ok(Value::Number(value))
        }
        CastType::Boolean => parse_bool(text)
            .map(Value::Bool)
            .ok_or_else(|| PrepareInvalidDataError::new("invalid boolean").into()),
    }
}

fn parse_bool(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn validate_headers(headers: &[String]) -> Result<()> {
    if headers.is_empty() || headers.len() > MAX_FIELDS {
        return Err(PrepareInvalidDataError::new(format!(
            "tabular schema must contain 1 to {MAX_FIELDS} fields"
        ))
        .into());
    }
    let mut seen = HashSet::new();
    for header in headers {
        validate_field(header)?;
        if !seen.insert(header) {
            return Err(
                PrepareInvalidDataError::new("tabular schema contains duplicate fields").into(),
            );
        }
    }
    Ok(())
}

fn validate_pipeline_headers(headers: &[String], recipe: &PrepareRecipe) -> Result<()> {
    let mut fields = headers.iter().cloned().collect::<HashSet<_>>();
    for step in &recipe.steps {
        match step {
            PrepareStep::Filter { field, .. }
            | PrepareStep::Cast { field, .. }
            | PrepareStep::Replace { field, .. }
            | PrepareStep::FillMissing { field, .. } => {
                require_pipeline_field(&fields, field)?;
            }
            PrepareStep::Trim { fields: targets }
            | PrepareStep::Case {
                fields: targets, ..
            }
            | PrepareStep::DropMissing {
                fields: targets, ..
            } => {
                for field in targets {
                    require_pipeline_field(&fields, field)?;
                }
            }
            PrepareStep::Select { fields: selected } => {
                for field in selected {
                    require_pipeline_field(&fields, field)?;
                }
                fields = selected.iter().cloned().collect();
            }
            PrepareStep::Rename { from, to } => {
                require_pipeline_field(&fields, from)?;
                if from != to && fields.contains(to) {
                    return Err(PrepareInvalidDataError::new(format!(
                        "rename target {to:?} already exists in the tabular schema"
                    ))
                    .into());
                }
                fields.remove(from);
                fields.insert(to.clone());
            }
            PrepareStep::Dedupe { by, .. } => {
                for field in by {
                    require_pipeline_field(&fields, field)?;
                }
            }
        }
    }
    if !fields.contains(&recipe.id_field) {
        return Err(PrepareInvalidDataError::new(format!(
            "prepared schema is missing id field {:?}",
            recipe.id_field
        ))
        .into());
    }
    Ok(())
}

fn require_pipeline_field(fields: &HashSet<String>, field: &str) -> Result<()> {
    if !fields.contains(field) {
        return Err(PrepareInvalidDataError::new(format!(
            "tabular schema is missing field {field:?}"
        ))
        .into());
    }
    Ok(())
}

fn validate_fields(fields: &[String]) -> Result<()> {
    if fields.is_empty() || fields.len() > MAX_FIELDS {
        return Err(InvalidArgumentError::new(format!(
            "field list must contain 1 to {MAX_FIELDS} names"
        ))
        .into());
    }
    let mut seen = HashSet::new();
    for field in fields {
        validate_field(field)?;
        if !seen.insert(field) {
            return Err(InvalidArgumentError::new("field list contains duplicates").into());
        }
    }
    Ok(())
}

fn validate_scalar(value: &Value, label: &str) -> Result<()> {
    if matches!(value, Value::Array(_) | Value::Object(_)) {
        return Err(InvalidArgumentError::new(format!("{label} must be a JSON scalar")).into());
    }
    Ok(())
}

pub(crate) fn validate_field(field: &str) -> Result<()> {
    if field.is_empty() || field.len() > MAX_FIELD_BYTES {
        return Err(InvalidArgumentError::new(format!(
            "field names must be between 1 and {MAX_FIELD_BYTES} bytes"
        ))
        .into());
    }
    Ok(())
}

fn delimiter_byte(value: &str) -> Option<u8> {
    let bytes = value.as_bytes();
    (bytes.len() == 1 && bytes[0].is_ascii()).then_some(bytes[0])
}

fn load_plan(path: &Path) -> Result<PreparePlan> {
    let payload = read_bounded(path, MAX_PLAN_BYTES, "preparation plan")?;
    let text = std::str::from_utf8(&payload)
        .map_err(|_| InvalidArgumentError::new("preparation plan is not valid UTF-8"))?;
    reject_duplicate_json_members(text)
        .map_err(|_| InvalidArgumentError::new("preparation plan is invalid"))?;
    let plan: PreparePlan = serde_json::from_str(text)
        .map_err(|_| InvalidArgumentError::new("preparation plan is invalid"))?;
    if plan.namespace != ARTIFACT_NAMESPACE
        || plan.kind != "prepare_plan"
        || plan.schema_version != PREPARE_PLAN_SCHEMA_VERSION
    {
        return Err(
            InvalidArgumentError::new("unsupported preparation plan identity or schema").into(),
        );
    }
    let identity = PrepareIdentity {
        schema_version: plan.schema_version,
        source_path: &plan.source_path,
        source_content_id: &plan.source_content_id,
        recipe_path: &plan.recipe_path,
        recipe_id: &plan.recipe_id,
        id_field: &plan.id_field,
        output_path: &plan.output_path,
        output_content_id: &plan.output_content_id,
        source_rows: plan.source_rows,
        output_rows: plan.output_rows,
        filtered_rows: plan.filtered_rows,
        duplicate_rows: plan.duplicate_rows,
    };
    let expected = blake3_content_id(
        "prep",
        b"datajig-prepare-plan-v1\0",
        &serde_json::to_vec(&identity)?,
    );
    if expected != plan.plan_id {
        return Err(InvalidArgumentError::new("preparation plan identity is invalid").into());
    }
    Ok(plan)
}

pub(crate) fn hash_file(path: &Path, prefix: &str) -> Result<String> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32);
    }
    let mut file = options
        .open(path)
        .with_context(|| format!("cannot open {prefix} file"))?;
    if !file.metadata()?.is_file() {
        return Err(PrepareInvalidDataError::new(format!(
            "{prefix} source must be a regular file"
        ))
        .into());
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-prepare-input-v1\0");
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{prefix}_{}", hasher.finalize().to_hex()))
}

fn hash_prepared_output(path: &Path) -> Result<String> {
    let mut file = File::open(path).context("cannot open prepared output")?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-prepared-jsonl-v1\0");
    let mut buffer = [0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("prepared_{}", hasher.finalize().to_hex()))
}

fn read_bounded(path: &Path, maximum: usize, label: &str) -> Result<Vec<u8>> {
    let file = File::open(path).with_context(|| format!("cannot open {label}"))?;
    let metadata = file.metadata()?;
    if metadata.len() > maximum as u64 {
        bail!("{label} exceeds {maximum} bytes");
    }
    let mut payload = Vec::with_capacity(metadata.len() as usize);
    file.take(maximum as u64 + 1).read_to_end(&mut payload)?;
    if payload.len() > maximum {
        bail!("{label} exceeds {maximum} bytes");
    }
    Ok(payload)
}

fn canonical_regular_file(path: &Path, label: &str) -> Result<PathBuf> {
    let path = path
        .canonicalize()
        .with_context(|| format!("cannot resolve {label}"))?;
    if !path.is_file() {
        return Err(InvalidArgumentError::new(format!("{label} must be a regular file")).into());
    }
    Ok(path)
}

fn resolve_new_path(path: &Path, label: &str) -> Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).with_context(|| format!("cannot create {label} directory"))?;
    let parent = parent
        .canonicalize()
        .with_context(|| format!("cannot resolve {label} directory"))?;
    let name = path
        .file_name()
        .ok_or_else(|| InvalidArgumentError::new(format!("{label} has no file name")))?;
    Ok(parent.join(name))
}

fn receipt_path(output: &Path) -> PathBuf {
    PathBuf::from(format!("{}.datajig.json", output.to_string_lossy()))
}

fn path_text(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| InvalidArgumentError::new("preparation paths must be valid UTF-8").into())
}

fn allocate_output_temp(output: &Path) -> Result<(PathBuf, File)> {
    let parent = output
        .parent()
        .context("preparation output has no parent")?;
    let name = output
        .file_name()
        .context("preparation output has no file name")?
        .to_string_lossy();
    for attempt in 0..100u32 {
        let path = parent.join(format!(
            ".{name}.{}.{}.prepare.tmp",
            std::process::id(),
            attempt
        ));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("cannot create preparation output temporary"),
        }
    }
    bail!("cannot allocate preparation output temporary")
}

fn publish_temp_new(temporary: &Path, output: &Path) -> Result<()> {
    File::open(temporary)?.sync_all()?;
    publish_new_noreplace(temporary, output)
        .context("cannot atomically publish preparation output")?;
    File::open(
        output
            .parent()
            .context("preparation output has no parent")?,
    )?
    .sync_all()?;
    Ok(())
}
