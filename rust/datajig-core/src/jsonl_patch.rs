use crate::jsonl_inspect::{canonical_record_digest, jsonl_record_id_digest};
use crate::{InvalidArgumentError, MAX_JSONL_LINE_BYTES};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(not(unix))]
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

pub const JSONL_PATCH_SCHEMA_VERSION: u8 = 1;
pub const MAX_JSONL_PATCH_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlPatchValue {
    present: bool,
    value: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlPatchRequest {
    namespace: String,
    kind: String,
    schema_version: u8,
    report_content_id: String,
    candidate_state_id: String,
    finding_id: String,
    record_id: String,
    record_content_id: String,
    before: JsonlPatchValue,
    after: JsonlPatchValue,
}

#[derive(Clone, Debug, Serialize)]
pub struct JsonlPatchPreviewArtifact {
    pub schema_version: u8,
    pub patch_id: String,
    pub report_content_id: String,
    pub finding_id: String,
    pub finding_code: String,
    pub field: String,
    pub change_id: String,
    pub changeset_id: String,
    pub record_id: String,
    pub source_state_id: String,
    pub source_dataset_content_id: String,
    pub source_record_content_id: String,
    pub predicted_record_content_id: String,
    pub line: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedJsonlPatch {
    pub preview: JsonlPatchPreviewArtifact,
    pub preimage: Vec<u8>,
    pub postimage: Vec<u8>,
}

#[derive(Serialize)]
struct PatchIdentity<'a> {
    schema_version: u8,
    request: &'a JsonlPatchRequest,
    finding_code: &'a str,
    field: &'a str,
    line: usize,
    predicted_record_content_id: &'a str,
}

impl JsonlPatchRequest {
    pub fn from_path(path: &Path) -> Result<Self> {
        let file = open_direct_regular(path, "JSONL patch request")?;
        let metadata = file
            .metadata()
            .map_err(|_| InvalidArgumentError::new("cannot inspect JSONL patch request"))?;
        if metadata.len() > MAX_JSONL_PATCH_BYTES as u64 {
            return Err(InvalidArgumentError::new(format!(
                "JSONL patch request exceeds {MAX_JSONL_PATCH_BYTES} bytes"
            ))
            .into());
        }
        let mut payload = Vec::with_capacity(usize::try_from(metadata.len())?);
        file.take(u64::try_from(MAX_JSONL_PATCH_BYTES)? + 1)
            .read_to_end(&mut payload)
            .map_err(|_| InvalidArgumentError::new("cannot read JSONL patch request"))?;
        if payload.len() > MAX_JSONL_PATCH_BYTES {
            return Err(InvalidArgumentError::new(format!(
                "JSONL patch request exceeds {MAX_JSONL_PATCH_BYTES} bytes"
            ))
            .into());
        }
        let text = std::str::from_utf8(&payload)
            .map_err(|_| InvalidArgumentError::new("JSONL patch request is not valid UTF-8"))?;
        let value: Self = serde_json::from_str(text).map_err(|error| {
            InvalidArgumentError::new(format!("JSONL patch request is invalid: {error}"))
        })?;
        value.validate()?;
        Ok(value)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_bound(
        report_content_id: String,
        candidate_state_id: String,
        finding_id: String,
        record_id: String,
        record_content_id: String,
        before: Option<Value>,
        after: Option<Value>,
    ) -> Result<Self> {
        let value = Self {
            namespace: crate::identity::ARTIFACT_NAMESPACE.into(),
            kind: "jsonl_field_patch".into(),
            schema_version: JSONL_PATCH_SCHEMA_VERSION,
            report_content_id,
            candidate_state_id,
            finding_id,
            record_id,
            record_content_id,
            before: JsonlPatchValue::from_option(before),
            after: JsonlPatchValue::from_option(after),
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    pub fn report_content_id(&self) -> &str {
        &self.report_content_id
    }

    pub fn candidate_state_id(&self) -> &str {
        &self.candidate_state_id
    }

    pub fn finding_id(&self) -> &str {
        &self.finding_id
    }

    pub fn record_id(&self) -> &str {
        &self.record_id
    }

    pub fn record_content_id(&self) -> &str {
        &self.record_content_id
    }

    pub(crate) fn after(&self) -> (bool, &Value) {
        (self.after.present, &self.after.value)
    }

    fn validate(&self) -> Result<()> {
        if self.namespace != crate::identity::ARTIFACT_NAMESPACE
            || self.kind != "jsonl_field_patch"
            || self.schema_version != JSONL_PATCH_SCHEMA_VERSION
        {
            return Err(InvalidArgumentError::new(
                "unsupported JSONL patch request identity or schema",
            )
            .into());
        }
        validate_digest(&self.report_content_id, "review_", "report content ID")?;
        validate_digest(
            &self.candidate_state_id,
            "recordstate_",
            "candidate state ID",
        )?;
        validate_digest(&self.record_id, "rid_", "record ID")?;
        validate_digest(&self.record_content_id, "record_", "record content ID")?;
        validate_finding_id(&self.finding_id)?;
        self.before.validate("before")?;
        self.after.validate("after")?;
        if self.before == self.after {
            return Err(InvalidArgumentError::new(
                "JSONL patch before and after states are identical",
            )
            .into());
        }
        Ok(())
    }
}

impl PartialEq for JsonlPatchValue {
    fn eq(&self, other: &Self) -> bool {
        self.present == other.present && self.value == other.value
    }
}

impl JsonlPatchValue {
    fn from_option(value: Option<Value>) -> Self {
        match value {
            Some(value) => Self {
                present: true,
                value,
            },
            None => Self {
                present: false,
                value: Value::Null,
            },
        }
    }

    fn validate(&self, side: &str) -> Result<()> {
        if !self.present && !self.value.is_null() {
            return Err(InvalidArgumentError::new(format!(
                "JSONL patch {side} value must be null when present is false"
            ))
            .into());
        }
        if self.present && matches!(self.value, Value::Array(_) | Value::Object(_)) {
            return Err(InvalidArgumentError::new(format!(
                "JSONL patch {side} value must be a scalar or null"
            ))
            .into());
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_patch_preview(
    request: &JsonlPatchRequest,
    source: &Path,
    id_field: &str,
    finding_code: &str,
    field: &str,
    line: usize,
    change_id: &str,
    changeset_id: &str,
    source_dataset_content_id: &str,
) -> Result<JsonlPatchPreviewArtifact> {
    Ok(prepare_jsonl_patch(
        request,
        source,
        id_field,
        finding_code,
        field,
        line,
        change_id,
        changeset_id,
        source_dataset_content_id,
    )?
    .preview)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn prepare_jsonl_patch(
    request: &JsonlPatchRequest,
    source: &Path,
    id_field: &str,
    finding_code: &str,
    field: &str,
    line: usize,
    change_id: &str,
    changeset_id: &str,
    source_dataset_content_id: &str,
) -> Result<PreparedJsonlPatch> {
    if field == id_field {
        return Err(InvalidArgumentError::new("JSONL patch cannot modify the ID field").into());
    }
    if field.is_empty() || field.len() > crate::MAX_JSONL_FIELD_NAME_BYTES {
        return Err(InvalidArgumentError::new("JSONL patch field is invalid").into());
    }
    let preimage = load_record_line(source, line)?;
    let (content, terminator) = split_line_terminator(&preimage);
    let text = std::str::from_utf8(content)
        .map_err(|_| InvalidArgumentError::new("JSONL patch target is not valid UTF-8"))?;
    let mut record: Value = serde_json::from_str(text)
        .map_err(|_| InvalidArgumentError::new("JSONL patch target is not valid JSON"))?;
    let object = record
        .as_object()
        .ok_or_else(|| InvalidArgumentError::new("JSONL patch target is not an object record"))?;
    let actual_id = object
        .get(id_field)
        .and_then(jsonl_record_id_digest)
        .map(|digest| format!("rid_{}", blake3::Hash::from(digest).to_hex()))
        .ok_or_else(|| InvalidArgumentError::new("JSONL patch target has an invalid record ID"))?;
    if actual_id != request.record_id {
        return Err(InvalidArgumentError::new(
            "JSONL patch record ID does not match the verified target",
        )
        .into());
    }
    let source_record_content_id = format!(
        "record_{}",
        blake3::Hash::from(canonical_record_digest(&record)).to_hex()
    );
    if source_record_content_id != request.record_content_id {
        return Err(InvalidArgumentError::new(
            "JSONL patch record content does not match the verified target",
        )
        .into());
    }
    let before_matches = if request.before.present {
        object.get(field) == Some(&request.before.value)
    } else {
        !object.contains_key(field)
    };
    if !before_matches {
        return Err(InvalidArgumentError::new(
            "JSONL patch before state does not match the verified target",
        )
        .into());
    }
    let object = record
        .as_object_mut()
        .expect("validated JSONL patch target remains an object");
    if request.after.present {
        object.insert(field.into(), request.after.value.clone());
    } else {
        object.remove(field);
    }
    let predicted_record_content_id = format!(
        "record_{}",
        blake3::Hash::from(canonical_record_digest(&record)).to_hex()
    );
    let identity = PatchIdentity {
        schema_version: JSONL_PATCH_SCHEMA_VERSION,
        request,
        finding_code,
        field,
        line,
        predicted_record_content_id: &predicted_record_content_id,
    };
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-jsonl-field-patch-v1\0");
    serde_json::to_writer(&mut hasher, &identity)?;
    let mut postimage = serde_json::to_vec(&record)?;
    if postimage.len() > MAX_JSONL_LINE_BYTES {
        return Err(InvalidArgumentError::new("JSONL patch result line is too large").into());
    }
    postimage.extend_from_slice(terminator);
    Ok(PreparedJsonlPatch {
        preview: JsonlPatchPreviewArtifact {
            schema_version: JSONL_PATCH_SCHEMA_VERSION,
            patch_id: format!("patch_{}", hasher.finalize().to_hex()),
            report_content_id: request.report_content_id.clone(),
            finding_id: request.finding_id.clone(),
            finding_code: finding_code.into(),
            field: field.into(),
            change_id: change_id.into(),
            changeset_id: changeset_id.into(),
            record_id: request.record_id.clone(),
            source_state_id: request.candidate_state_id.clone(),
            source_dataset_content_id: source_dataset_content_id.into(),
            source_record_content_id,
            predicted_record_content_id,
            line,
        },
        preimage,
        postimage,
    })
}

fn load_record_line(source: &Path, target_line: usize) -> Result<Vec<u8>> {
    if target_line == 0 {
        return Err(InvalidArgumentError::new("JSONL patch line must be positive").into());
    }
    let file = open_direct_regular(source, "JSONL patch source")?;
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::new();
    for current_line in 1..=target_line {
        buffer.clear();
        let read = Read::by_ref(&mut reader)
            .take(u64::try_from(MAX_JSONL_LINE_BYTES)? + 2)
            .read_until(b'\n', &mut buffer)
            .context("cannot read JSONL patch source")?;
        if read == 0 {
            return Err(InvalidArgumentError::new(
                "JSONL patch line does not exist in the verified source",
            )
            .into());
        }
        if buffer.len() > MAX_JSONL_LINE_BYTES + 1
            || (buffer.len() == MAX_JSONL_LINE_BYTES + 1 && !buffer.ends_with(b"\n"))
        {
            return Err(InvalidArgumentError::new("JSONL patch target line is too large").into());
        }
        if current_line == target_line {
            break;
        }
    }
    Ok(buffer)
}

fn split_line_terminator(line: &[u8]) -> (&[u8], &[u8]) {
    if let Some(content) = line.strip_suffix(b"\r\n") {
        (content, b"\r\n")
    } else if let Some(content) = line.strip_suffix(b"\n") {
        (content, b"\n")
    } else {
        (line, b"")
    }
}

#[cfg(unix)]
fn open_direct_regular(path: &Path, artifact: &str) -> Result<File> {
    use rustix::fs::{Mode, OFlags, open};

    let descriptor = open(
        path,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
        Mode::empty(),
    )
    .map_err(|_| InvalidArgumentError::new(format!("cannot open {artifact}")))?;
    let file = File::from(descriptor);
    let metadata = file
        .metadata()
        .map_err(|_| InvalidArgumentError::new(format!("cannot inspect {artifact}")))?;
    if !metadata.is_file() {
        return Err(
            InvalidArgumentError::new(format!("{artifact} must be a direct regular file")).into(),
        );
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_direct_regular(path: &Path, artifact: &str) -> Result<File> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| InvalidArgumentError::new(format!("cannot inspect {artifact}")))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(
            InvalidArgumentError::new(format!("{artifact} must be a direct regular file")).into(),
        );
    }
    File::open(path)
        .map_err(|_| InvalidArgumentError::new(format!("cannot open {artifact}")).into())
}

fn validate_digest(value: &str, prefix: &str, name: &str) -> Result<()> {
    let digest = value
        .strip_prefix(prefix)
        .ok_or_else(|| InvalidArgumentError::new(format!("{name} has an invalid prefix")))?;
    blake3::Hash::from_hex(digest)
        .map_err(|_| InvalidArgumentError::new(format!("{name} is invalid")))?;
    Ok(())
}

fn validate_finding_id(value: &str) -> Result<()> {
    let body = value
        .strip_prefix("fnd_")
        .ok_or_else(|| InvalidArgumentError::new("finding ID has an invalid prefix"))?;
    let (fingerprint, occurrence) = body
        .rsplit_once('_')
        .ok_or_else(|| InvalidArgumentError::new("finding ID is invalid"))?;
    if fingerprint.len() != 20
        || !fingerprint.bytes().all(|byte| byte.is_ascii_hexdigit())
        || occurrence.len() != 4
        || !occurrence.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(InvalidArgumentError::new("finding ID is invalid").into());
    }
    Ok(())
}
