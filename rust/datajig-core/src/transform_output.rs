use crate::jsonl_inspect::jsonl_record_id_digest;
use crate::prepare::MAX_OUTPUT_LINE_BYTES;
use crate::strict_json::reject_duplicate_json_members;
use crate::{TransformField, TransformLimits};
use anyhow::{Context, Result};
use serde_json::{Map, Number, Value};
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

pub type ProviderObservedSchema = [TransformField];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransformOutputErrorKind {
    UnsupportedType,
    NonFinite,
    Schema,
    MalformedRow,
    IdIntegrity,
    OutputLimit,
}

#[derive(Debug)]
pub struct TransformOutputError {
    kind: TransformOutputErrorKind,
    message: String,
}

impl TransformOutputError {
    pub fn kind(&self) -> TransformOutputErrorKind {
        self.kind
    }

    fn new(kind: TransformOutputErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for TransformOutputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for TransformOutputError {}

#[derive(Clone, Debug)]
pub struct VerifiedTransformOutput {
    pub schema: Vec<TransformField>,
    pub rows: u64,
    pub bytes: u64,
    pub unique_ids: u64,
    pub output_content_id: String,
    canonical_path: PathBuf,
}

impl VerifiedTransformOutput {
    pub fn canonical_path(&self) -> &Path {
        &self.canonical_path
    }
}

pub fn verify_transform_candidate(
    candidate: &Path,
    canonical: &Path,
    id_field: &str,
    observed_schema: &ProviderObservedSchema,
    limits: &TransformLimits,
) -> Result<VerifiedTransformOutput> {
    let schema = validate_schema(observed_schema, id_field, limits)?;
    let candidate_metadata =
        fs::symlink_metadata(candidate).context("cannot inspect transform provider candidate")?;
    if candidate_metadata.file_type().is_symlink() || !candidate_metadata.is_file() {
        return Err(output_error(
            TransformOutputErrorKind::MalformedRow,
            "transform candidate must be a regular file",
        ));
    }
    if candidate_metadata.len() > limits.output_bytes {
        return Err(output_error(
            TransformOutputErrorKind::OutputLimit,
            "transform candidate exceeds the output byte limit",
        ));
    }
    let source = File::open(candidate).context("cannot open transform provider candidate")?;
    let output = create_private_file(canonical)?;
    let mut guard = CanonicalGuard::new(canonical);
    let mut reader = BufReader::new(source);
    let mut writer = BufWriter::new(output);
    let mut line = Vec::new();
    let mut rows = 0u64;
    let mut bytes = 0u64;
    let mut ids = HashSet::<[u8; 32]>::new();
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-prepared-jsonl-v1\0");

    loop {
        line.clear();
        let read = Read::by_ref(&mut reader)
            .take(MAX_OUTPUT_LINE_BYTES as u64 + 2)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        if line.len() > MAX_OUTPUT_LINE_BYTES + 1
            || (line.len() == MAX_OUTPUT_LINE_BYTES + 1 && line.last() != Some(&b'\n'))
        {
            return Err(output_error(
                TransformOutputErrorKind::OutputLimit,
                "transform output row exceeds the line byte limit",
            ));
        }
        rows = rows
            .checked_add(1)
            .context("transform row count overflow")?;
        if rows > limits.output_rows {
            return Err(output_error(
                TransformOutputErrorKind::OutputLimit,
                "transform output exceeds the row limit",
            ));
        }
        if line.last() == Some(&b'\n') {
            line.pop();
            if line.last() == Some(&b'\r') {
                line.pop();
            }
        }
        if line.is_empty() {
            return Err(output_error(
                TransformOutputErrorKind::MalformedRow,
                "transform output contains a blank row",
            ));
        }
        let text = std::str::from_utf8(&line).map_err(|_| {
            TransformOutputError::new(
                TransformOutputErrorKind::MalformedRow,
                "transform output is not valid UTF-8",
            )
        })?;
        reject_duplicate_json_members(text).map_err(|_| {
            TransformOutputError::new(
                TransformOutputErrorKind::MalformedRow,
                "transform output row is not strict JSON",
            )
        })?;
        let value: Value = serde_json::from_str(text).map_err(|_| {
            TransformOutputError::new(
                TransformOutputErrorKind::MalformedRow,
                "transform output row is not a JSON object",
            )
        })?;
        let mut object = value.as_object().cloned().ok_or_else(|| {
            TransformOutputError::new(
                TransformOutputErrorKind::MalformedRow,
                "transform output row must be a JSON object",
            )
        })?;
        let id = object.get(id_field).ok_or_else(|| {
            TransformOutputError::new(
                TransformOutputErrorKind::IdIntegrity,
                "transform output row is missing its ID field",
            )
        })?;
        let digest = jsonl_record_id_digest(id).ok_or_else(|| {
            TransformOutputError::new(
                TransformOutputErrorKind::IdIntegrity,
                "transform output ID must be a nonempty string or number",
            )
        })?;
        normalize_row(&mut object, &schema)?;
        if !ids.insert(digest) {
            return Err(output_error(
                TransformOutputErrorKind::IdIntegrity,
                "transform output contains a duplicate normalized ID",
            ));
        }
        let encoded = serde_json::to_vec(&object)?;
        let next_bytes = bytes
            .checked_add(encoded.len() as u64 + 1)
            .context("transform output size overflow")?;
        if next_bytes > limits.output_bytes {
            return Err(output_error(
                TransformOutputErrorKind::OutputLimit,
                "canonical transform output exceeds the byte limit",
            ));
        }
        writer.write_all(&encoded)?;
        writer.write_all(b"\n")?;
        hasher.update(&encoded);
        hasher.update(b"\n");
        bytes = next_bytes;
    }
    writer.flush()?;
    writer.get_ref().sync_all()?;
    guard.keep();
    Ok(VerifiedTransformOutput {
        schema: observed_schema.to_vec(),
        rows,
        bytes,
        unique_ids: ids.len() as u64,
        output_content_id: format!("prepared_{}", hasher.finalize().to_hex()),
        canonical_path: canonical.to_owned(),
    })
}

fn validate_schema(
    observed: &ProviderObservedSchema,
    id_field: &str,
    limits: &TransformLimits,
) -> Result<Vec<SchemaField>> {
    if observed.is_empty() || observed.len() > limits.output_fields {
        return Err(output_error(
            TransformOutputErrorKind::Schema,
            "transform output schema is empty or exceeds its field limit",
        ));
    }
    let mut names = HashSet::new();
    let mut schema = Vec::with_capacity(observed.len());
    for field in observed {
        if field.name.is_empty()
            || field.name.len() > 1024
            || field.name.chars().any(char::is_control)
            || !names.insert(field.name.clone())
        {
            return Err(output_error(
                TransformOutputErrorKind::Schema,
                "transform output field names must be safe and unique",
            ));
        }
        let value_type = match field.value_type.as_str() {
            "boolean" => OutputType::Boolean,
            "integer" => OutputType::Integer,
            "unsigned_integer" => OutputType::UnsignedInteger,
            "double" => OutputType::Double,
            "string" => OutputType::String,
            _ => {
                return Err(output_error(
                    TransformOutputErrorKind::UnsupportedType,
                    format!(
                        "transform output field {:?} has unsupported type {:?}",
                        field.name, field.value_type
                    ),
                ));
            }
        };
        schema.push(SchemaField {
            name: field.name.clone(),
            value_type,
            nullable: field.nullable,
        });
    }
    if id_field.is_empty() || !names.contains(id_field) {
        return Err(output_error(
            TransformOutputErrorKind::IdIntegrity,
            "transform output schema is missing the configured ID field",
        ));
    }
    Ok(schema)
}

fn normalize_row(object: &mut Map<String, Value>, schema: &[SchemaField]) -> Result<()> {
    if object.len() != schema.len() || schema.iter().any(|field| !object.contains_key(&field.name))
    {
        return Err(output_error(
            TransformOutputErrorKind::MalformedRow,
            "transform output row does not match the observed schema",
        ));
    }
    for field in schema {
        let value = object
            .get_mut(&field.name)
            .expect("schema membership was checked");
        if value.is_null() {
            if !field.nullable {
                return Err(output_error(
                    TransformOutputErrorKind::MalformedRow,
                    format!(
                        "transform output field {:?} is unexpectedly null",
                        field.name
                    ),
                ));
            }
            continue;
        }
        match field.value_type {
            OutputType::Boolean if value.is_boolean() => {}
            OutputType::Integer if value.as_i64().is_some() => {}
            OutputType::UnsignedInteger if value.as_u64().is_some() => {}
            OutputType::String if value.is_string() => {}
            OutputType::Double => normalize_double(value, &field.name)?,
            _ => {
                return Err(output_error(
                    TransformOutputErrorKind::MalformedRow,
                    format!(
                        "transform output field {:?} does not match its declared type",
                        field.name
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn normalize_double(value: &mut Value, field: &str) -> Result<()> {
    let Some(number) = value.as_number() else {
        return Err(output_error(
            TransformOutputErrorKind::MalformedRow,
            format!("transform output field {field:?} is not numeric"),
        ));
    };
    let Some(number) = number.as_f64() else {
        return Err(output_error(
            TransformOutputErrorKind::NonFinite,
            format!("transform output field {field:?} is not a finite double"),
        ));
    };
    if !number.is_finite() {
        return Err(output_error(
            TransformOutputErrorKind::NonFinite,
            format!("transform output field {field:?} is not finite"),
        ));
    }
    *value = if number == 0.0 {
        Value::Number(Number::from(0))
    } else {
        Value::Number(Number::from_f64(number).expect("finite double converts to JSON"))
    };
    Ok(())
}

fn create_private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(path)
        .context("cannot create canonical transform candidate")
}

fn output_error(kind: TransformOutputErrorKind, message: impl Into<String>) -> anyhow::Error {
    TransformOutputError::new(kind, message).into()
}

#[derive(Clone, Copy)]
enum OutputType {
    Boolean,
    Integer,
    UnsignedInteger,
    Double,
    String,
}

struct SchemaField {
    name: String,
    value_type: OutputType,
    nullable: bool,
}

struct CanonicalGuard<'a> {
    path: &'a Path,
    keep: bool,
}

impl<'a> CanonicalGuard<'a> {
    fn new(path: &'a Path) -> Self {
        Self { path, keep: false }
    }

    fn keep(&mut self) {
        self.keep = true;
    }
}

impl Drop for CanonicalGuard<'_> {
    fn drop(&mut self) {
        if !self.keep {
            let _ = fs::remove_file(self.path);
        }
    }
}
