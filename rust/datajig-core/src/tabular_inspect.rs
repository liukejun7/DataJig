use crate::jsonl_inspect::{
    MAX_JSONL_OUTPUT_FIELDS, MAX_JSONL_OUTPUT_FINDINGS, jsonl_record_id_digest,
};
use crate::prepare::{MAX_FIELDS, MAX_ROWS, hash_file, validate_field};
use crate::tabular_source::{TabularConsumer, TabularRow, stream_csv, stream_parquet};
use crate::{ConcurrentModificationError, InvalidArgumentError};
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::path::Path;

pub const TABULAR_INSPECTION_SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Debug, Serialize)]
pub struct TabularInspection {
    namespace: &'static str,
    schema_version: u8,
    adapter: &'static str,
    format: &'static str,
    source: String,
    source_content_id: String,
    bytes: u64,
    records: usize,
    id_field: String,
    missing_ids: usize,
    null_ids: usize,
    invalid_ids: usize,
    duplicate_ids: usize,
    field_count: usize,
    fields: Vec<TabularFieldSummary>,
    fields_truncated: bool,
    findings: usize,
    finding_items: Vec<TabularFinding>,
    findings_truncated: bool,
    recipe_template: Value,
}

impl TabularInspection {
    pub fn finding_count(&self) -> usize {
        self.findings
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn format(&self) -> &str {
        self.format
    }
}

#[derive(Clone, Debug, Serialize)]
struct TabularFieldSummary {
    name: String,
    present: usize,
    nulls: usize,
    empty_strings: usize,
    types: Vec<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
struct TabularFinding {
    code: &'static str,
    row: usize,
    message: &'static str,
}

#[derive(Default)]
struct FieldCounter {
    present: usize,
    nulls: usize,
    empty_strings: usize,
    types: BTreeSet<&'static str>,
}

struct InspectionConsumer<'a> {
    id_field: &'a str,
    headers_seen: bool,
    id_field_present: bool,
    records: usize,
    missing_ids: usize,
    null_ids: usize,
    invalid_ids: usize,
    duplicate_ids: usize,
    seen_ids: HashSet<[u8; 32]>,
    fields: BTreeMap<String, FieldCounter>,
    findings: usize,
    finding_items: Vec<TabularFinding>,
}

impl<'a> InspectionConsumer<'a> {
    fn new(id_field: &'a str) -> Self {
        Self {
            id_field,
            headers_seen: false,
            id_field_present: false,
            records: 0,
            missing_ids: 0,
            null_ids: 0,
            invalid_ids: 0,
            duplicate_ids: 0,
            seen_ids: HashSet::new(),
            fields: BTreeMap::new(),
            findings: 0,
            finding_items: Vec::new(),
        }
    }

    fn push_finding(&mut self, code: &'static str, row: usize, message: &'static str) {
        self.findings += 1;
        if self.finding_items.len() < MAX_JSONL_OUTPUT_FINDINGS {
            self.finding_items
                .push(TabularFinding { code, row, message });
        }
    }
}

impl TabularConsumer for InspectionConsumer<'_> {
    fn headers(&mut self, headers: &[String]) -> Result<()> {
        if self.headers_seen {
            return Err(
                InvalidArgumentError::new("tabular source reported its schema twice").into(),
            );
        }
        if headers.is_empty() || headers.len() > MAX_FIELDS {
            return Err(InvalidArgumentError::new(format!(
                "tabular schema must contain 1 to {MAX_FIELDS} fields"
            ))
            .into());
        }
        self.headers_seen = true;
        self.id_field_present = headers.iter().any(|header| header == self.id_field);
        for header in headers {
            validate_field(header)?;
            if self
                .fields
                .insert(header.clone(), FieldCounter::default())
                .is_some()
            {
                return Err(
                    InvalidArgumentError::new("tabular schema contains duplicate fields").into(),
                );
            }
        }
        if !self.id_field_present {
            self.push_finding(
                "MISSING_ID_FIELD",
                0,
                "schema is missing the configured ID field",
            );
        }
        Ok(())
    }

    fn row(&mut self, row: TabularRow) -> Result<()> {
        self.records = self
            .records
            .checked_add(1)
            .context("tabular inspection row count overflow")?;
        if self.records > MAX_ROWS {
            return Err(InvalidArgumentError::new(format!(
                "tabular source exceeds {MAX_ROWS} rows"
            ))
            .into());
        }
        for (name, value) in &row {
            if let Some(counter) = self.fields.get_mut(name) {
                counter.present += 1;
                if value.is_null() {
                    counter.nulls += 1;
                }
                if matches!(value, Value::String(text) if text.is_empty()) {
                    counter.empty_strings += 1;
                }
                counter.types.insert(value_type(value));
            }
        }
        if !self.id_field_present {
            self.missing_ids += 1;
            return Ok(());
        }
        let Some(value) = row.get(self.id_field) else {
            self.missing_ids += 1;
            self.push_finding(
                "MISSING_ID",
                self.records,
                "row is missing the configured ID field",
            );
            return Ok(());
        };
        if value.is_null() {
            self.null_ids += 1;
            self.push_finding("NULL_ID", self.records, "row ID is null");
            return Ok(());
        }
        let Some(digest) = jsonl_record_id_digest(value) else {
            self.invalid_ids += 1;
            self.push_finding(
                "INVALID_ID",
                self.records,
                "row ID must be a non-empty string or number",
            );
            return Ok(());
        };
        if !self.seen_ids.insert(digest) {
            self.duplicate_ids += 1;
            self.push_finding(
                "DUPLICATE_ID",
                self.records,
                "row ID duplicates a prior row",
            );
        }
        Ok(())
    }
}

pub fn inspect_tabular(
    source: &Path,
    id_field: &str,
    delimiter: Option<&str>,
) -> Result<TabularInspection> {
    if id_field.is_empty() || id_field.len() > 4_096 {
        return Err(
            InvalidArgumentError::new("id field must contain 1 to 4096 UTF-8 bytes").into(),
        );
    }
    let source = source
        .canonicalize()
        .context("cannot resolve tabular source")?;
    if !source.is_file() {
        return Err(InvalidArgumentError::new("tabular source is not a regular file").into());
    }
    let format = tabular_format(&source)?;
    let delimiter = match (format, delimiter) {
        ("csv", Some(value)) => delimiter_byte(value)?,
        ("csv", None) => b',',
        ("parquet", None | Some(",")) => b',',
        ("parquet", Some(_)) => {
            return Err(InvalidArgumentError::new("--delimiter is only valid for CSV").into());
        }
        _ => unreachable!(),
    };
    let before = hash_file(&source, "source")?;
    let bytes = fs::metadata(&source)
        .context("cannot inspect tabular source")?
        .len();
    let mut consumer = InspectionConsumer::new(id_field);
    match format {
        "csv" => stream_csv(&source, delimiter, &mut consumer)?,
        "parquet" => stream_parquet(&source, &mut consumer)?,
        _ => unreachable!(),
    }
    let after = hash_file(&source, "source")?;
    if before != after {
        return Err(
            ConcurrentModificationError::new("tabular source changed during inspection").into(),
        );
    }
    if !consumer.headers_seen {
        return Err(InvalidArgumentError::new("tabular source has no schema").into());
    }
    let field_count = consumer.fields.len();
    let fields = consumer
        .fields
        .into_iter()
        .take(MAX_JSONL_OUTPUT_FIELDS)
        .map(|(name, counter)| TabularFieldSummary {
            name,
            present: counter.present,
            nulls: counter.nulls,
            empty_strings: counter.empty_strings,
            types: counter.types.into_iter().collect(),
        })
        .collect();
    let source_text = path_text(&source)?;
    let recipe_source = if format == "csv" {
        json!({"format": "csv", "delimiter": char::from(delimiter).to_string()})
    } else {
        json!({"format": "parquet"})
    };
    Ok(TabularInspection {
        namespace: crate::identity::ARTIFACT_NAMESPACE,
        schema_version: TABULAR_INSPECTION_SCHEMA_VERSION,
        adapter: "tabular",
        format,
        source: source_text,
        source_content_id: before,
        bytes,
        records: consumer.records,
        id_field: id_field.to_owned(),
        missing_ids: consumer.missing_ids,
        null_ids: consumer.null_ids,
        invalid_ids: consumer.invalid_ids,
        duplicate_ids: consumer.duplicate_ids,
        field_count,
        fields,
        fields_truncated: field_count > MAX_JSONL_OUTPUT_FIELDS,
        findings: consumer.findings,
        finding_items: consumer.finding_items,
        findings_truncated: consumer.findings > MAX_JSONL_OUTPUT_FINDINGS,
        recipe_template: json!({
            "namespace": "datajig",
            "kind": "prepare",
            "schema_version": 1,
            "source": recipe_source,
            "output": {"format": "jsonl"},
            "id_field": id_field,
            "steps": []
        }),
    })
}

fn tabular_format(source: &Path) -> Result<&'static str> {
    match source
        .extension()
        .and_then(|value| value.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("csv") => Ok("csv"),
        Some("parquet") => Ok("parquet"),
        _ => Err(
            InvalidArgumentError::new("tabular inspect requires a .csv or .parquet file").into(),
        ),
    }
}

fn delimiter_byte(value: &str) -> Result<u8> {
    let bytes = value.as_bytes();
    if bytes.len() == 1 && bytes[0].is_ascii() {
        Ok(bytes[0])
    } else {
        Err(InvalidArgumentError::new("CSV delimiter must be exactly one ASCII byte").into())
    }
}

fn value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn path_text(path: &Path) -> Result<String> {
    path.to_str()
        .map(str::to_owned)
        .context("tabular source path is not valid UTF-8")
}
