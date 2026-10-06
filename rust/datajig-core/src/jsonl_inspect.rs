use crate::{ConcurrentModificationError, InvalidArgumentError};
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::{self, File, Metadata};
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use std::time::SystemTime;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

pub const JSONL_INSPECTION_SCHEMA_VERSION: u8 = 1;
pub const RECORD_DIFF_SCHEMA_VERSION: u8 = 1;
pub const MAX_JSONL_LINE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_JSONL_FIELDS: usize = 10_000;
pub const MAX_JSONL_FIELD_NAME_BYTES: usize = 1_024;
pub const MAX_JSONL_OUTPUT_FIELDS: usize = 100;
pub const MAX_JSONL_OUTPUT_FINDINGS: usize = 20;
// Record-state construction currently keeps a sorted identity index in memory. Keep the
// advertised bound conservative until the index is backed by an external sort/on-disk reader.
pub const MAX_JSONL_DIFF_RECORDS: usize = 250_000;
pub const MAX_JSONL_DIFF_CHANGES: usize = 100;

#[derive(Clone, Debug, Serialize)]
pub struct JsonlRecordDiff {
    namespace: &'static str,
    schema_version: u8,
    adapter: &'static str,
    id_field: String,
    before_content_id: String,
    after_content_id: String,
    summary: JsonlRecordDiffSummary,
    total_changes: usize,
    byte_only_changed: bool,
    change_items: Vec<JsonlRecordChange>,
    changes_truncated: bool,
}

impl JsonlRecordDiff {
    pub fn total_changes(&self) -> usize {
        self.total_changes
    }
    pub fn byte_only_changed(&self) -> bool {
        self.byte_only_changed
    }

    pub fn summary_counts(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.summary.added,
            self.summary.removed,
            self.summary.modified,
            self.summary.moved,
            self.summary.unchanged,
        )
    }
}

#[derive(Clone, Debug, Default, Serialize)]
struct JsonlRecordDiffSummary {
    added: usize,
    removed: usize,
    modified: usize,
    moved: usize,
    unchanged: usize,
}

#[derive(Clone, Debug, Serialize)]
struct JsonlRecordChange {
    kind: &'static str,
    record_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    before_line: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    after_line: Option<usize>,
}

pub(crate) struct RecordFact {
    pub(crate) line: usize,
    pub(crate) content_hash: blake3::Hash,
}

#[derive(Clone, Debug, Serialize)]
pub struct JsonlInspection {
    namespace: &'static str,
    schema_version: u8,
    adapter: &'static str,
    format: &'static str,
    source: String,
    dataset_content_id: String,
    bytes: u64,
    physical_lines: usize,
    records: usize,
    blank_lines: usize,
    invalid_records: usize,
    id_field: String,
    missing_ids: usize,
    null_ids: usize,
    invalid_ids: usize,
    duplicate_ids: usize,
    field_count: usize,
    fields: Vec<JsonlFieldSummary>,
    fields_truncated: bool,
    findings: usize,
    finding_items: Vec<JsonlFinding>,
    findings_truncated: bool,
    #[serde(skip)]
    source_fingerprint: SourceFingerprint,
}

impl JsonlInspection {
    pub fn finding_count(&self) -> usize {
        self.findings
    }

    pub fn dataset_content_id(&self) -> &str {
        &self.dataset_content_id
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub(crate) fn id_field(&self) -> &str {
        &self.id_field
    }

    pub(crate) fn state_parts(&self) -> JsonlInspectionStateParts<'_> {
        JsonlInspectionStateParts {
            bytes: self.bytes,
            physical_lines: self.physical_lines,
            records: self.records,
            blank_lines: self.blank_lines,
            invalid_records: self.invalid_records,
            missing_ids: self.missing_ids,
            null_ids: self.null_ids,
            invalid_ids: self.invalid_ids,
            duplicate_ids: self.duplicate_ids,
            field_count: self.field_count,
            fields: &self.fields,
            fields_truncated: self.fields_truncated,
            findings: self.findings,
            finding_items: &self.finding_items,
            findings_truncated: self.findings_truncated,
        }
    }
}

pub(crate) struct JsonlInspectionStateParts<'a> {
    pub(crate) bytes: u64,
    pub(crate) physical_lines: usize,
    pub(crate) records: usize,
    pub(crate) blank_lines: usize,
    pub(crate) invalid_records: usize,
    pub(crate) missing_ids: usize,
    pub(crate) null_ids: usize,
    pub(crate) invalid_ids: usize,
    pub(crate) duplicate_ids: usize,
    pub(crate) field_count: usize,
    pub(crate) fields: &'a [JsonlFieldSummary],
    pub(crate) fields_truncated: bool,
    pub(crate) findings: usize,
    pub(crate) finding_items: &'a [JsonlFinding],
    pub(crate) findings_truncated: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct JsonlFieldSummary {
    pub(crate) name: String,
    pub(crate) present: usize,
    pub(crate) nulls: usize,
    pub(crate) types: Vec<&'static str>,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct JsonlFinding {
    pub(crate) code: &'static str,
    pub(crate) line: usize,
    pub(crate) message: &'static str,
}

#[derive(Default)]
struct FieldCounter {
    present: usize,
    nulls: usize,
    types: BTreeSet<&'static str>,
}

pub fn inspect_jsonl(source: &Path, id_field: &str) -> Result<JsonlInspection> {
    inspect_jsonl_with_limits(source, id_field, None, None)
}

pub(crate) fn inspect_jsonl_with_record_limit(
    source: &Path,
    id_field: &str,
    record_limit: Option<usize>,
) -> Result<JsonlInspection> {
    inspect_jsonl_with_limits(source, id_field, record_limit, None)
}

pub(crate) fn inspect_jsonl_with_limits(
    source: &Path,
    id_field: &str,
    record_limit: Option<usize>,
    byte_limit: Option<u64>,
) -> Result<JsonlInspection> {
    if id_field.is_empty() || id_field.len() > 4_096 {
        return Err(
            InvalidArgumentError::new("id field must contain 1 to 4096 UTF-8 bytes").into(),
        );
    }
    if source
        .extension()
        .and_then(|value| value.to_str())
        .is_none_or(|value| !value.eq_ignore_ascii_case("jsonl"))
    {
        return Err(InvalidArgumentError::new("inspect currently requires a .jsonl file").into());
    }
    let source = source
        .canonicalize()
        .context("cannot resolve JSONL source")?;
    let source_text = source
        .to_str()
        .context("JSONL source path is not valid UTF-8")?
        .to_owned();
    let file = File::open(&source).context("cannot open JSONL source")?;
    let before = source_fingerprint(&file.metadata().context("cannot inspect JSONL source")?)?;
    if !before.is_file {
        return Err(InvalidArgumentError::new("JSONL source is not a regular file").into());
    }
    if byte_limit.is_some_and(|limit| before.len > limit) {
        return Err(InvalidArgumentError::new("JSONL source exceeds its byte limit").into());
    }
    let mut reader = BufReader::new(file);
    let mut content_hasher = blake3::Hasher::new();
    content_hasher.update(b"datajig-jsonl-recordset-v1\0");
    let mut bytes = 0_u64;
    let mut physical_lines = 0usize;
    let mut records = 0usize;
    let mut blank_lines = 0usize;
    let mut invalid_records = 0usize;
    let mut missing_ids = 0usize;
    let mut null_ids = 0usize;
    let mut invalid_ids = 0usize;
    let mut duplicate_ids = 0usize;
    let mut fields = BTreeMap::<String, FieldCounter>::new();
    let mut seen_ids = HashSet::<blake3::Hash>::new();
    let mut finding_count = 0usize;
    let mut finding_items = Vec::new();
    let mut line = Vec::new();

    loop {
        line.clear();
        let read = Read::by_ref(&mut reader)
            .take(u64::try_from(MAX_JSONL_LINE_BYTES + 2)?)
            .read_until(b'\n', &mut line)
            .context("cannot read JSONL source")?;
        if read == 0 {
            break;
        }
        physical_lines += 1;
        bytes = bytes
            .checked_add(u64::try_from(read)?)
            .context("JSONL byte count overflow")?;
        if byte_limit.is_some_and(|limit| bytes > limit) {
            return Err(ConcurrentModificationError::new(
                "JSONL source grew beyond its byte limit during inspection",
            )
            .into());
        }
        content_hasher.update(&line);
        let mut content = line.as_slice();
        if content.ends_with(b"\n") {
            content = &content[..content.len() - 1];
        }
        if content.ends_with(b"\r") {
            content = &content[..content.len() - 1];
        }
        if content.len() > MAX_JSONL_LINE_BYTES {
            return Err(InvalidArgumentError::new(format!(
                "JSONL line {physical_lines} exceeds {MAX_JSONL_LINE_BYTES} bytes"
            ))
            .into());
        }
        let text = match std::str::from_utf8(content) {
            Ok(text) => text,
            Err(_) => {
                invalid_records += 1;
                push_finding(
                    &mut finding_count,
                    &mut finding_items,
                    "INVALID_UTF8",
                    physical_lines,
                    "line is not valid UTF-8",
                );
                continue;
            }
        };
        if text.trim().is_empty() {
            blank_lines += 1;
            continue;
        }
        let value: Value = match serde_json::from_str(text) {
            Ok(value) => value,
            Err(_) => {
                invalid_records += 1;
                push_finding(
                    &mut finding_count,
                    &mut finding_items,
                    "INVALID_JSON",
                    physical_lines,
                    "line is not valid JSON",
                );
                continue;
            }
        };
        let Value::Object(object) = value else {
            invalid_records += 1;
            push_finding(
                &mut finding_count,
                &mut finding_items,
                "NON_OBJECT_RECORD",
                physical_lines,
                "record must be a JSON object",
            );
            continue;
        };
        records += 1;
        if record_limit.is_some_and(|limit| records > limit) {
            return Err(InvalidArgumentError::new(format!(
                "record diff exceeds {} records per input",
                record_limit.expect("record limit is present")
            ))
            .into());
        }
        for (name, value) in &object {
            if name.len() > MAX_JSONL_FIELD_NAME_BYTES {
                return Err(InvalidArgumentError::new(format!(
                    "JSONL field name exceeds {MAX_JSONL_FIELD_NAME_BYTES} UTF-8 bytes"
                ))
                .into());
            }
            if !fields.contains_key(name) && fields.len() == MAX_JSONL_FIELDS {
                return Err(InvalidArgumentError::new(format!(
                    "JSONL contains more than {MAX_JSONL_FIELDS} unique top-level fields"
                ))
                .into());
            }
            let counter = fields.entry(name.clone()).or_default();
            counter.present += 1;
            if value.is_null() {
                counter.nulls += 1;
            }
            counter.types.insert(value_type(value));
        }
        match object.get(id_field) {
            None => {
                missing_ids += 1;
                push_finding(
                    &mut finding_count,
                    &mut finding_items,
                    "MISSING_ID",
                    physical_lines,
                    "record is missing the configured ID field",
                );
            }
            Some(Value::Null) => {
                null_ids += 1;
                push_finding(
                    &mut finding_count,
                    &mut finding_items,
                    "NULL_ID",
                    physical_lines,
                    "record ID is null",
                );
            }
            Some(Value::String(value)) => {
                if value.trim().is_empty() {
                    invalid_ids += 1;
                    push_finding(
                        &mut finding_count,
                        &mut finding_items,
                        "INVALID_ID",
                        physical_lines,
                        "record ID must not be an empty string",
                    );
                } else {
                    record_id(
                        b"string\0",
                        value.as_bytes(),
                        physical_lines,
                        &mut seen_ids,
                        &mut duplicate_ids,
                        &mut finding_count,
                        &mut finding_items,
                    );
                }
            }
            Some(Value::Number(value)) => {
                let canonical = canonical_number(value);
                record_id(
                    b"number\0",
                    &canonical,
                    physical_lines,
                    &mut seen_ids,
                    &mut duplicate_ids,
                    &mut finding_count,
                    &mut finding_items,
                );
            }
            Some(_) => {
                invalid_ids += 1;
                push_finding(
                    &mut finding_count,
                    &mut finding_items,
                    "INVALID_ID",
                    physical_lines,
                    "record ID must be a string or number",
                );
            }
        }
    }

    let field_count = fields.len();
    let fields = fields
        .into_iter()
        .take(MAX_JSONL_OUTPUT_FIELDS)
        .map(|(name, counter)| JsonlFieldSummary {
            name,
            present: counter.present,
            nulls: counter.nulls,
            types: counter.types.into_iter().collect(),
        })
        .collect();
    let stale = || ConcurrentModificationError::new("JSONL source changed during inspection");
    let after_metadata = reader.get_ref().metadata().map_err(|_| stale())?;
    let after = source_fingerprint(&after_metadata).map_err(|_| stale())?;
    let current_metadata = fs::metadata(&source).map_err(|_| stale())?;
    let current = source_fingerprint(&current_metadata).map_err(|_| stale())?;
    if before != after || after != current || bytes != after.len {
        return Err(stale().into());
    }
    Ok(JsonlInspection {
        namespace: crate::identity::ARTIFACT_NAMESPACE,
        schema_version: JSONL_INSPECTION_SCHEMA_VERSION,
        adapter: "jsonl",
        format: "jsonl",
        source: source_text,
        dataset_content_id: format!("records_{}", content_hasher.finalize().to_hex()),
        bytes,
        physical_lines,
        records,
        blank_lines,
        invalid_records,
        id_field: id_field.to_owned(),
        missing_ids,
        null_ids,
        invalid_ids,
        duplicate_ids,
        field_count,
        fields_truncated: field_count > MAX_JSONL_OUTPUT_FIELDS,
        fields,
        findings: finding_count,
        findings_truncated: finding_count > MAX_JSONL_OUTPUT_FINDINGS,
        finding_items,
        source_fingerprint: before,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SourceFingerprint {
    is_file: bool,
    len: u64,
    modified: SystemTime,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    ctime: i64,
    #[cfg(unix)]
    ctime_nsec: i64,
}

fn source_fingerprint(metadata: &Metadata) -> Result<SourceFingerprint> {
    Ok(SourceFingerprint {
        is_file: metadata.is_file(),
        len: metadata.len(),
        modified: metadata
            .modified()
            .context("JSONL modification time is unavailable")?,
        #[cfg(unix)]
        device: metadata.dev(),
        #[cfg(unix)]
        inode: metadata.ino(),
        #[cfg(unix)]
        ctime: metadata.ctime(),
        #[cfg(unix)]
        ctime_nsec: metadata.ctime_nsec(),
    })
}

pub(crate) fn canonical_number(number: &serde_json::Number) -> Vec<u8> {
    let raw = number.to_string();
    let (negative, unsigned) = raw
        .strip_prefix('-')
        .map_or((false, raw.as_str()), |value| (true, value));
    let exponent_index = unsigned.find(['e', 'E']);
    let (mantissa, exponent) = exponent_index.map_or((unsigned, "0"), |index| {
        (&unsigned[..index], &unsigned[index + 1..])
    });
    let decimal_index = mantissa.find('.');
    let fractional_digits = decimal_index.map_or(0, |index| mantissa.len() - index - 1);
    let mut digits = mantissa
        .bytes()
        .filter(|byte| *byte != b'.')
        .collect::<Vec<_>>();
    let first_nonzero = digits.iter().position(|byte| *byte != b'0');
    let Some(first_nonzero) = first_nonzero else {
        return b"0e0".to_vec();
    };
    digits.drain(..first_nonzero);
    let trailing_zeros = digits
        .iter()
        .rev()
        .take_while(|byte| **byte == b'0')
        .count();
    digits.truncate(digits.len() - trailing_zeros);
    let adjustment = i64::try_from(trailing_zeros).expect("line limit bounds digit count")
        - i64::try_from(fractional_digits).expect("line limit bounds digit count");
    let normalized_exponent = add_small_to_signed_decimal(exponent, adjustment);
    let mut canonical = Vec::with_capacity(digits.len() + normalized_exponent.len() + 2);
    if negative {
        canonical.push(b'-');
    }
    canonical.extend_from_slice(&digits);
    canonical.push(b'e');
    canonical.extend_from_slice(normalized_exponent.as_bytes());
    canonical
}

pub(crate) fn compare_json_numbers(
    left: &serde_json::Number,
    right: &serde_json::Number,
) -> std::cmp::Ordering {
    compare_canonical_numbers(&canonical_number(left), &canonical_number(right))
}

pub(crate) fn compare_canonical_numbers(left: &[u8], right: &[u8]) -> std::cmp::Ordering {
    let (left_negative, left_digits, left_exponent) = split_canonical_number(left);
    let (right_negative, right_digits, right_exponent) = split_canonical_number(right);
    if left_digits == b"0" || right_digits == b"0" {
        return match (left_digits == b"0", right_digits == b"0") {
            (true, true) => std::cmp::Ordering::Equal,
            (true, false) => {
                if right_negative {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            (false, true) => {
                if left_negative {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (false, false) => unreachable!(),
        };
    }
    if left_negative != right_negative {
        return if left_negative {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        };
    }
    let left_order = add_small_to_signed_decimal(
        std::str::from_utf8(left_exponent).expect("canonical exponent is ASCII"),
        i64::try_from(left_digits.len()).expect("line bound limits digits"),
    );
    let right_order = add_small_to_signed_decimal(
        std::str::from_utf8(right_exponent).expect("canonical exponent is ASCII"),
        i64::try_from(right_digits.len()).expect("line bound limits digits"),
    );
    let magnitude = compare_signed_decimal(&left_order, &right_order).then_with(|| {
        let width = left_digits.len().max(right_digits.len());
        (0..width)
            .map(|index| left_digits.get(index).copied().unwrap_or(b'0'))
            .cmp((0..width).map(|index| right_digits.get(index).copied().unwrap_or(b'0')))
    });
    if left_negative {
        magnitude.reverse()
    } else {
        magnitude
    }
}

fn compare_signed_decimal(left: &str, right: &str) -> std::cmp::Ordering {
    let (left_negative, left_magnitude) = split_signed_decimal(left);
    let (right_negative, right_magnitude) = split_signed_decimal(right);
    if left_negative != right_negative {
        return if left_negative {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        };
    }
    let magnitude = left_magnitude
        .len()
        .cmp(&right_magnitude.len())
        .then_with(|| left_magnitude.cmp(right_magnitude));
    if left_negative {
        magnitude.reverse()
    } else {
        magnitude
    }
}

fn split_canonical_number(value: &[u8]) -> (bool, &[u8], &[u8]) {
    let negative = value.first() == Some(&b'-');
    let unsigned = if negative { &value[1..] } else { value };
    let exponent = unsigned
        .iter()
        .position(|byte| *byte == b'e')
        .expect("canonical number contains exponent separator");
    (negative, &unsigned[..exponent], &unsigned[exponent + 1..])
}

fn split_signed_decimal(value: &str) -> (bool, &str) {
    value
        .strip_prefix('-')
        .map_or((false, value), |magnitude| (true, magnitude))
}

pub(crate) fn jsonl_record_id_digest(value: &Value) -> Option<[u8; 32]> {
    match value {
        Value::String(value) if !value.trim().is_empty() => {
            Some(*record_id_hash(b"string\0", value.as_bytes()).as_bytes())
        }
        Value::Number(value) => {
            Some(*record_id_hash(b"number\0", &canonical_number(value)).as_bytes())
        }
        _ => None,
    }
}

pub(crate) fn canonical_scalar_key(value: &Value) -> Option<Vec<u8>> {
    match value {
        Value::Bool(value) => Some(if *value {
            b"b1".to_vec()
        } else {
            b"b0".to_vec()
        }),
        Value::Number(value) => {
            let mut key = b"n".to_vec();
            key.extend_from_slice(&canonical_number(value));
            Some(key)
        }
        Value::String(value) => {
            let mut key = b"s".to_vec();
            key.extend_from_slice(value.as_bytes());
            Some(key)
        }
        _ => None,
    }
}

fn add_small_to_signed_decimal(value: &str, adjustment: i64) -> String {
    let (negative, magnitude) = value.strip_prefix('-').map_or_else(
        || (false, value.strip_prefix('+').unwrap_or(value)),
        |value| (true, value),
    );
    let magnitude = magnitude.trim_start_matches('0');
    let magnitude = if magnitude.is_empty() { "0" } else { magnitude };
    if adjustment == 0 {
        return signed_magnitude(negative, magnitude.to_owned());
    }
    let adjustment_negative = adjustment < 0;
    let adjustment_magnitude = adjustment.unsigned_abs();
    if negative == adjustment_negative && magnitude != "0" {
        return signed_magnitude(negative, add_small(magnitude, adjustment_magnitude));
    }
    if magnitude == "0" {
        return signed_magnitude(adjustment_negative, adjustment_magnitude.to_string());
    }
    let adjustment_text = adjustment_magnitude.to_string();
    match magnitude
        .len()
        .cmp(&adjustment_text.len())
        .then_with(|| magnitude.cmp(&adjustment_text))
    {
        std::cmp::Ordering::Greater => {
            signed_magnitude(negative, subtract_small(magnitude, adjustment_magnitude))
        }
        std::cmp::Ordering::Equal => "0".into(),
        std::cmp::Ordering::Less => {
            let magnitude_value = magnitude
                .parse::<u64>()
                .expect("a smaller decimal exponent fits the adjustment type");
            signed_magnitude(
                adjustment_negative,
                (adjustment_magnitude - magnitude_value).to_string(),
            )
        }
    }
}

fn add_small(magnitude: &str, mut value: u64) -> String {
    let mut digits = magnitude.as_bytes().to_vec();
    for digit in digits.iter_mut().rev() {
        if value == 0 {
            break;
        }
        let sum = u64::from(*digit - b'0') + value;
        *digit = b'0' + u8::try_from(sum % 10).expect("decimal digit fits u8");
        value = sum / 10;
    }
    if value > 0 {
        let mut prefix = value.to_string().into_bytes();
        prefix.extend_from_slice(&digits);
        digits = prefix;
    }
    String::from_utf8(digits).expect("decimal digits are UTF-8")
}

fn subtract_small(magnitude: &str, mut value: u64) -> String {
    let mut digits = magnitude.as_bytes().to_vec();
    let mut borrow = 0_i16;
    for digit in digits.iter_mut().rev() {
        let subtrahend = i16::from(u8::try_from(value % 10).expect("decimal digit fits u8"));
        value /= 10;
        let mut current = i16::from(*digit - b'0') - subtrahend - borrow;
        if current < 0 {
            current += 10;
            borrow = 1;
        } else {
            borrow = 0;
        }
        *digit = b'0' + u8::try_from(current).expect("decimal digit is non-negative");
    }
    let first = digits
        .iter()
        .position(|digit| *digit != b'0')
        .unwrap_or(digits.len() - 1);
    String::from_utf8(digits[first..].to_vec()).expect("decimal digits are UTF-8")
}

fn signed_magnitude(negative: bool, magnitude: String) -> String {
    if magnitude == "0" || !negative {
        magnitude
    } else {
        format!("-{magnitude}")
    }
}

pub fn diff_jsonl_records(before: &Path, after: &Path, id_field: &str) -> Result<JsonlRecordDiff> {
    let before_inspection =
        inspect_jsonl_with_record_limit(before, id_field, Some(MAX_JSONL_DIFF_RECORDS))?;
    let after_inspection =
        inspect_jsonl_with_record_limit(after, id_field, Some(MAX_JSONL_DIFF_RECORDS))?;
    if before_inspection.finding_count() != 0 || after_inspection.finding_count() != 0 {
        return Err(InvalidArgumentError::new(
            "record diff requires both inputs to pass inspect without findings",
        )
        .into());
    }
    let before_records = index_jsonl_records(
        Path::new(before_inspection.source()),
        id_field,
        before_inspection.dataset_content_id(),
        &before_inspection.source_fingerprint,
    )?;
    let after_records = index_jsonl_records(
        Path::new(after_inspection.source()),
        id_field,
        after_inspection.dataset_content_id(),
        &after_inspection.source_fingerprint,
    )?;
    validate_current_source(
        Path::new(before_inspection.source()),
        &before_inspection.source_fingerprint,
    )?;
    validate_current_source(
        Path::new(after_inspection.source()),
        &after_inspection.source_fingerprint,
    )?;
    Ok(diff_record_maps(
        before_inspection.dataset_content_id(),
        after_inspection.dataset_content_id(),
        id_field,
        &before_records,
        &after_records,
    ))
}

pub(crate) fn diff_record_maps(
    before_content_id: &str,
    after_content_id: &str,
    id_field: &str,
    before_records: &BTreeMap<[u8; 32], RecordFact>,
    after_records: &BTreeMap<[u8; 32], RecordFact>,
) -> JsonlRecordDiff {
    let mut summary = JsonlRecordDiffSummary::default();
    let mut changes = Vec::new();
    let mut before_iter = before_records.iter().peekable();
    let mut after_iter = after_records.iter().peekable();
    while before_iter.peek().is_some() || after_iter.peek().is_some() {
        let (key, left, right) = match (before_iter.peek(), after_iter.peek()) {
            (Some((left_key, _)), Some((right_key, _))) => match left_key.cmp(right_key) {
                std::cmp::Ordering::Less => {
                    let (key, value) = before_iter.next().expect("peeked before record");
                    (key, Some(value), None)
                }
                std::cmp::Ordering::Greater => {
                    let (key, value) = after_iter.next().expect("peeked after record");
                    (key, None, Some(value))
                }
                std::cmp::Ordering::Equal => {
                    let (key, left) = before_iter.next().expect("peeked before record");
                    let (_, right) = after_iter.next().expect("peeked after record");
                    (key, Some(left), Some(right))
                }
            },
            (Some(_), None) => {
                let (key, value) = before_iter.next().expect("peeked before record");
                (key, Some(value), None)
            }
            (None, Some(_)) => {
                let (key, value) = after_iter.next().expect("peeked after record");
                (key, None, Some(value))
            }
            (None, None) => unreachable!(),
        };
        let change = match (left, right) {
            (Some(left), Some(right)) if left.content_hash != right.content_hash => {
                summary.modified += 1;
                Some(("modified", Some(left.line), Some(right.line)))
            }
            (Some(left), Some(right)) if left.line != right.line => {
                summary.moved += 1;
                Some(("moved", Some(left.line), Some(right.line)))
            }
            (Some(_), Some(_)) => {
                summary.unchanged += 1;
                None
            }
            (Some(left), None) => {
                summary.removed += 1;
                Some(("removed", Some(left.line), None))
            }
            (None, Some(right)) => {
                summary.added += 1;
                Some(("added", None, Some(right.line)))
            }
            (None, None) => unreachable!(),
        };
        if let Some((kind, before_line, after_line)) = change {
            push_record_change(&mut changes, kind, key, before_line, after_line);
        }
    }
    let total_changes = summary.added + summary.removed + summary.modified + summary.moved;
    let byte_only_changed = total_changes == 0 && before_content_id != after_content_id;
    let changes_truncated = total_changes > changes.len();
    JsonlRecordDiff {
        namespace: crate::identity::ARTIFACT_NAMESPACE,
        schema_version: RECORD_DIFF_SCHEMA_VERSION,
        adapter: "jsonl",
        id_field: id_field.to_owned(),
        before_content_id: before_content_id.to_owned(),
        after_content_id: after_content_id.to_owned(),
        summary,
        total_changes,
        byte_only_changed,
        change_items: changes,
        changes_truncated,
    }
}

pub(crate) fn validated_record_facts(
    inspection: &JsonlInspection,
    id_field: &str,
) -> Result<BTreeMap<[u8; 32], RecordFact>> {
    if inspection.finding_count() != 0 {
        return Err(InvalidArgumentError::new(
            "record facts require a JSONL inspection without findings",
        )
        .into());
    }
    index_jsonl_records(
        Path::new(inspection.source()),
        id_field,
        inspection.dataset_content_id(),
        &inspection.source_fingerprint,
    )
}

fn index_jsonl_records(
    source: &Path,
    id_field: &str,
    expected: &str,
    expected_fingerprint: &SourceFingerprint,
) -> Result<BTreeMap<[u8; 32], RecordFact>> {
    let stale = || ConcurrentModificationError::new("JSONL changed after inspection");
    let source = source.canonicalize().map_err(|_| stale())?;
    let file = File::open(&source).map_err(|_| stale())?;
    let opened_fingerprint =
        source_fingerprint(&file.metadata().map_err(|_| stale())?).map_err(|_| stale())?;
    if &opened_fingerprint != expected_fingerprint {
        return Err(stale().into());
    }
    let mut reader = BufReader::new(file);
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-jsonl-recordset-v1\0");
    let mut records = BTreeMap::new();
    let mut line = Vec::new();
    let mut line_number = 0usize;
    loop {
        line.clear();
        let read = Read::by_ref(&mut reader)
            .take(u64::try_from(MAX_JSONL_LINE_BYTES + 2)?)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        line_number += 1;
        hasher.update(&line);
        let mut content = line.as_slice();
        if content.ends_with(b"\n") {
            content = &content[..content.len() - 1];
        }
        if content.ends_with(b"\r") {
            content = &content[..content.len() - 1];
        }
        let text = std::str::from_utf8(content)
            .map_err(|_| ConcurrentModificationError::new("JSONL changed after inspection"))?;
        if text.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(text)
            .map_err(|_| ConcurrentModificationError::new("JSONL changed after inspection"))?;
        let Value::Object(object) = value else {
            return Err(ConcurrentModificationError::new("JSONL changed after inspection").into());
        };
        if records.len() == MAX_JSONL_DIFF_RECORDS {
            return Err(InvalidArgumentError::new(format!(
                "record diff exceeds {MAX_JSONL_DIFF_RECORDS} records per input"
            ))
            .into());
        }
        let id = object
            .get(id_field)
            .ok_or_else(|| ConcurrentModificationError::new("JSONL changed after inspection"))?;
        let id_hash = match id {
            Value::String(value) => record_id_hash(b"string\0", value.as_bytes()),
            Value::Number(value) => record_id_hash(b"number\0", &canonical_number(value)),
            _ => {
                return Err(
                    ConcurrentModificationError::new("JSONL changed after inspection").into(),
                );
            }
        };
        let mut record_hasher = blake3::Hasher::new();
        record_hasher.update(b"datajig-jsonl-record-v1\0");
        hash_canonical_value(&mut record_hasher, &Value::Object(object));
        if records
            .insert(
                *id_hash.as_bytes(),
                RecordFact {
                    line: line_number,
                    content_hash: record_hasher.finalize(),
                },
            )
            .is_some()
        {
            return Err(ConcurrentModificationError::new("JSONL changed after inspection").into());
        }
    }
    if format!("records_{}", hasher.finalize().to_hex()) != expected {
        return Err(stale().into());
    }
    let after_fingerprint = source_fingerprint(&reader.get_ref().metadata().map_err(|_| stale())?)
        .map_err(|_| stale())?;
    let current_fingerprint =
        source_fingerprint(&fs::metadata(&source).map_err(|_| stale())?).map_err(|_| stale())?;
    if &after_fingerprint != expected_fingerprint || current_fingerprint != after_fingerprint {
        return Err(stale().into());
    }
    Ok(records)
}

fn validate_current_source(source: &Path, expected: &SourceFingerprint) -> Result<()> {
    let stale = || ConcurrentModificationError::new("JSONL changed during record diff");
    let current =
        source_fingerprint(&fs::metadata(source).map_err(|_| stale())?).map_err(|_| stale())?;
    if &current != expected {
        return Err(stale().into());
    }
    Ok(())
}

fn push_record_change(
    changes: &mut Vec<JsonlRecordChange>,
    kind: &'static str,
    key: &[u8; 32],
    before_line: Option<usize>,
    after_line: Option<usize>,
) {
    if changes.len() < MAX_JSONL_DIFF_CHANGES {
        changes.push(JsonlRecordChange {
            kind,
            record_id: format!("rid_{}", blake3::Hash::from(*key).to_hex()),
            before_line,
            after_line,
        });
    }
}

fn hash_canonical_value(hasher: &mut blake3::Hasher, value: &Value) {
    match value {
        Value::Null => {
            hasher.update(b"n");
        }
        Value::Bool(value) => {
            hasher.update(if *value { b"t" } else { b"f" });
        }
        Value::Number(value) => {
            hasher.update(b"d");
            hasher.update(&canonical_number(value));
        }
        Value::String(value) => {
            hasher.update(b"s");
            hasher.update(&(value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
        Value::Array(values) => {
            hasher.update(b"a");
            hasher.update(&(values.len() as u64).to_be_bytes());
            for value in values {
                hash_canonical_value(hasher, value);
            }
        }
        Value::Object(values) => {
            hasher.update(b"o");
            hasher.update(&(values.len() as u64).to_be_bytes());
            for (key, value) in values {
                hasher.update(&(key.len() as u64).to_be_bytes());
                hasher.update(key.as_bytes());
                hash_canonical_value(hasher, value);
            }
        }
    }
}

pub(crate) fn canonical_record_digest(value: &Value) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-jsonl-record-v1\0");
    hash_canonical_value(&mut hasher, value);
    *hasher.finalize().as_bytes()
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

#[allow(clippy::too_many_arguments)]
fn record_id(
    kind: &[u8],
    value: &[u8],
    line: usize,
    seen: &mut HashSet<blake3::Hash>,
    duplicates: &mut usize,
    finding_count: &mut usize,
    findings: &mut Vec<JsonlFinding>,
) {
    if !seen.insert(record_id_hash(kind, value)) {
        *duplicates += 1;
        push_finding(
            finding_count,
            findings,
            "DUPLICATE_ID",
            line,
            "record ID duplicates an earlier record",
        );
    }
}

fn record_id_hash(kind: &[u8], value: &[u8]) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-jsonl-record-id-v1\0");
    hasher.update(kind);
    hasher.update(value);
    hasher.finalize()
}

fn push_finding(
    count: &mut usize,
    findings: &mut Vec<JsonlFinding>,
    code: &'static str,
    line: usize,
    message: &'static str,
) {
    *count += 1;
    if findings.len() < MAX_JSONL_OUTPUT_FINDINGS {
        findings.push(JsonlFinding {
            code,
            line,
            message,
        });
    }
}
