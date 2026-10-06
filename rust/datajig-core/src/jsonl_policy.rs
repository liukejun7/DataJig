use crate::jsonl_inspect::{
    MAX_JSONL_DIFF_RECORDS, MAX_JSONL_LINE_BYTES, canonical_number, canonical_scalar_key,
    compare_canonical_numbers, compare_json_numbers, inspect_jsonl_with_record_limit,
    jsonl_record_id_digest,
};
use crate::{ConcurrentModificationError, InvalidArgumentError};
use anyhow::{Context, Result, bail};
use regex::{Regex, RegexBuilder};
use serde::de::{Error as _, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::{Number, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;

pub const JSONL_QUALITY_POLICY_SCHEMA_VERSION: u8 = 1;
pub const MAX_JSONL_POLICY_BYTES: usize = 1024 * 1024;
pub const MAX_JSONL_POLICY_FIELDS: usize = 256;
pub const MAX_JSONL_POLICY_ENUM_MEMBERS: usize = 256;
pub const MAX_JSONL_POLICY_PATTERN_BYTES: usize = 4 * 1024;
pub const MAX_JSONL_POLICY_REGEX_FIELDS: usize = 64;
pub const MAX_JSONL_POLICY_UNIQUE_FIELDS: usize = 4;
pub const MAX_JSONL_POLICY_SAMPLES: usize = 20;
const MAX_JSONL_POLICY_REGEX_SIZE: usize = 256 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum GateMode {
    ChangedOnly,
    Full,
}

impl GateMode {
    fn as_str(&self) -> &'static str {
        match self {
            Self::ChangedOnly => "changed_only",
            Self::Full => "full",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FieldPolicy {
    #[serde(default)]
    required: bool,
    #[serde(default = "default_true")]
    nullable: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    types: Vec<String>,
    #[serde(default, rename = "enum", skip_serializing_if = "Option::is_none")]
    enum_values: Option<Vec<Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    minimum: Option<Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    maximum: Option<Number>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pattern: Option<String>,
    #[serde(default)]
    unique: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlQualityPolicy {
    namespace: String,
    schema_version: u8,
    adapter: String,
    mode: GateMode,
    #[serde(deserialize_with = "deserialize_unique_fields")]
    fields: BTreeMap<String, FieldPolicy>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    policy_id: String,
}

impl JsonlQualityPolicy {
    pub fn from_path(path: &Path) -> Result<Self> {
        let path_metadata =
            std::fs::symlink_metadata(path).context("cannot inspect JSONL quality policy path")?;
        if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
            bail!("JSONL quality policy must be a direct regular file");
        }
        let file = File::open(path).context("cannot open JSONL quality policy")?;
        let metadata = file
            .metadata()
            .context("cannot inspect JSONL quality policy")?;
        if !metadata.is_file() {
            bail!("JSONL quality policy must be a regular file");
        }
        if metadata.len() > MAX_JSONL_POLICY_BYTES as u64 {
            bail!("JSONL quality policy exceeds {MAX_JSONL_POLICY_BYTES} bytes");
        }
        let mut payload = Vec::with_capacity(metadata.len() as usize);
        file.take((MAX_JSONL_POLICY_BYTES + 1) as u64)
            .read_to_end(&mut payload)
            .context("cannot read JSONL quality policy")?;
        if payload.len() > MAX_JSONL_POLICY_BYTES {
            bail!("JSONL quality policy exceeds {MAX_JSONL_POLICY_BYTES} bytes");
        }
        let payload =
            std::str::from_utf8(&payload).context("JSONL quality policy is not valid UTF-8")?;
        Self::from_json(payload)
    }

    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_JSONL_POLICY_BYTES {
            bail!("JSONL quality policy exceeds {MAX_JSONL_POLICY_BYTES} bytes");
        }
        let mut value: Self =
            serde_json::from_str(payload).context("invalid JSONL quality policy")?;
        let supplied_id = std::mem::take(&mut value.policy_id);
        value.normalize_and_validate()?;
        let computed = value.compute_id()?;
        if !supplied_id.is_empty() && supplied_id != computed {
            bail!("quality policy identity does not match its content");
        }
        value.policy_id = computed;
        value.ensure_size()?;
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        let payload = serde_json::to_string_pretty(self)?;
        if payload.len() > MAX_JSONL_POLICY_BYTES {
            bail!("JSONL quality policy exceeds {MAX_JSONL_POLICY_BYTES} bytes");
        }
        Ok(payload)
    }

    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    pub fn mode(&self) -> &str {
        self.mode.as_str()
    }

    pub(crate) fn accepts_field_value(
        &self,
        field: &str,
        present: bool,
        value: &Value,
    ) -> Result<bool> {
        let Some(rule) = self.fields.get(field) else {
            return Ok(false);
        };
        if !present {
            return Ok(!rule.required);
        }
        if value.is_null() {
            return Ok(rule.nullable);
        }
        let actual_type = json_type(value);
        if !rule.types.is_empty() && !rule.types.iter().any(|allowed| allowed == actual_type) {
            return Ok(false);
        }
        if rule.enum_values.as_ref().is_some_and(|members| {
            let candidate = canonical_scalar_key(value);
            candidate.is_none_or(|candidate| {
                !members
                    .iter()
                    .any(|member| canonical_scalar_key(member).as_ref() == Some(&candidate))
            })
        }) {
            return Ok(false);
        }
        if let Value::Number(number) = value {
            let candidate = canonical_number(number);
            if rule.minimum.as_ref().is_some_and(|minimum| {
                compare_canonical_numbers(&candidate, &canonical_number(minimum)) == Ordering::Less
            }) || rule.maximum.as_ref().is_some_and(|maximum| {
                compare_canonical_numbers(&candidate, &canonical_number(maximum))
                    == Ordering::Greater
            }) {
                return Ok(false);
            }
        }
        if let (Value::String(value), Some(pattern)) = (value, rule.pattern.as_deref()) {
            if !compile_pattern(pattern)?.is_match(value) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub(crate) fn is_unique_field(&self, field: &str) -> bool {
        self.fields.get(field).is_some_and(|rule| rule.unique)
    }

    fn normalize_and_validate(&mut self) -> Result<()> {
        if self.namespace != crate::identity::ARTIFACT_NAMESPACE
            || self.schema_version != JSONL_QUALITY_POLICY_SCHEMA_VERSION
            || self.adapter != "jsonl"
        {
            bail!("unsupported JSONL quality policy identity or schema");
        }
        if self.fields.is_empty() || self.fields.len() > MAX_JSONL_POLICY_FIELDS {
            bail!("quality policy must declare 1 to {MAX_JSONL_POLICY_FIELDS} fields");
        }
        let mut regex_fields = 0usize;
        let mut unique_fields = 0usize;
        for (field, policy) in &mut self.fields {
            if field.is_empty() || field.len() > crate::MAX_JSONL_FIELD_NAME_BYTES {
                bail!("quality policy field name is invalid");
            }
            policy.types.sort();
            policy.types.dedup();
            if policy.types.iter().any(|value| {
                !matches!(
                    value.as_str(),
                    "array" | "boolean" | "number" | "object" | "string"
                )
            }) {
                bail!("quality policy field {field} has an unsupported type");
            }
            if policy.minimum.is_some() || policy.maximum.is_some() {
                require_type(field, policy, "number", "numeric range")?;
                policy.minimum = policy.minimum.take().map(normalize_number).transpose()?;
                policy.maximum = policy.maximum.take().map(normalize_number).transpose()?;
                if policy
                    .minimum
                    .as_ref()
                    .zip(policy.maximum.as_ref())
                    .is_some_and(|(minimum, maximum)| {
                        compare_json_numbers(minimum, maximum) == Ordering::Greater
                    })
                {
                    bail!("quality policy field {field} has minimum greater than maximum");
                }
            }
            if let Some(pattern) = &policy.pattern {
                require_type(field, policy, "string", "pattern")?;
                if pattern.len() > MAX_JSONL_POLICY_PATTERN_BYTES {
                    bail!("quality policy field {field} pattern is too large");
                }
                compile_pattern(pattern)
                    .with_context(|| format!("quality policy field {field} pattern is invalid"))?;
                regex_fields += 1;
            }
            if regex_fields > MAX_JSONL_POLICY_REGEX_FIELDS {
                bail!("quality policy has too many regex fields");
            }
            if policy.unique
                && (policy.types.is_empty()
                    || policy
                        .types
                        .iter()
                        .any(|value| !matches!(value.as_str(), "boolean" | "number" | "string")))
            {
                bail!("quality policy field {field} uniqueness requires scalar types");
            }
            if policy.unique {
                unique_fields += 1;
                if unique_fields > MAX_JSONL_POLICY_UNIQUE_FIELDS {
                    bail!(
                        "quality policy has more than {MAX_JSONL_POLICY_UNIQUE_FIELDS} unique fields"
                    );
                }
            }
            if let Some(values) = &mut policy.enum_values {
                if values.is_empty() || values.len() > MAX_JSONL_POLICY_ENUM_MEMBERS {
                    bail!("quality policy field {field} enum size is invalid");
                }
                if policy.types.is_empty() {
                    bail!("quality policy field {field} enum requires declared types");
                }
                for value in values.iter_mut() {
                    if let Value::Number(number) = value {
                        *number = normalize_number(number.clone())?;
                    }
                    let value_type = json_type(value);
                    if value_type == "null"
                        || canonical_scalar_key(value).is_none()
                        || !policy.types.iter().any(|allowed| allowed == value_type)
                    {
                        bail!("quality policy field {field} enum member has an invalid type");
                    }
                }
                values.sort_by_key(|value| canonical_scalar_key(value).expect("validated scalar"));
                values.dedup_by(|left, right| {
                    canonical_scalar_key(left) == canonical_scalar_key(right)
                });
            }
        }
        Ok(())
    }

    fn compute_id(&self) -> Result<String> {
        let mut identity = self.clone();
        identity.policy_id.clear();
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"datajig-jsonl-quality-policy-v1\0");
        serde_json::to_writer(&mut hasher, &identity)?;
        Ok(format!("policy_{}", hasher.finalize().to_hex()))
    }

    fn ensure_size(&self) -> Result<()> {
        let _ = self.to_json()?;
        Ok(())
    }
}

fn deserialize_unique_fields<'de, D>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, FieldPolicy>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct UniqueFieldsVisitor;

    impl<'de> Visitor<'de> for UniqueFieldsVisitor {
        type Value = BTreeMap<String, FieldPolicy>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a map with unique quality policy field names")
        }

        fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut fields = BTreeMap::new();
            while let Some((field, policy)) = map.next_entry::<String, FieldPolicy>()? {
                if fields.insert(field.clone(), policy).is_some() {
                    return Err(A::Error::custom(format!(
                        "duplicate quality policy field {field}"
                    )));
                }
            }
            Ok(fields)
        }
    }

    deserializer.deserialize_map(UniqueFieldsVisitor)
}

#[derive(Clone, Debug, Serialize)]
pub struct JsonlQualityFinding {
    code: &'static str,
    field: String,
    rule: &'static str,
    total_violations: usize,
    gated_violations: usize,
    sample_ids: Vec<String>,
    samples_truncated: bool,
}

impl JsonlQualityFinding {
    pub(crate) fn code(&self) -> &str {
        self.code
    }

    pub(crate) fn field(&self) -> &str {
        &self.field
    }

    pub(crate) fn rule(&self) -> &str {
        self.rule
    }

    pub(crate) fn total_violations(&self) -> usize {
        self.total_violations
    }

    pub(crate) fn gated_violations(&self) -> usize {
        self.gated_violations
    }

    pub(crate) fn sample_ids(&self) -> &[String] {
        &self.sample_ids
    }

    pub(crate) fn samples_truncated(&self) -> bool {
        self.samples_truncated
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct JsonlQualityEvaluation {
    policy_id: String,
    mode: String,
    evaluated_records: usize,
    total_violations: usize,
    gated_violations: usize,
    findings: Vec<JsonlQualityFinding>,
}

impl JsonlQualityEvaluation {
    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    pub fn mode(&self) -> &str {
        &self.mode
    }

    pub fn gated_violations(&self) -> usize {
        self.gated_violations
    }

    pub fn findings(&self) -> &[JsonlQualityFinding] {
        &self.findings
    }
}

#[derive(Default)]
struct FindingAccumulator {
    total: usize,
    gated: usize,
    samples: BTreeSet<String>,
}

struct CompiledFieldPolicy {
    enum_keys: Option<HashSet<Vec<u8>>>,
    minimum: Option<Vec<u8>>,
    maximum: Option<Vec<u8>>,
    pattern: Option<Regex>,
}

type RuleKey = (String, &'static str, &'static str);

pub fn evaluate_jsonl_quality(
    source: &Path,
    id_field: &str,
    expected_content_id: &str,
    policy: &JsonlQualityPolicy,
    gated_record_ids: Option<&BTreeSet<[u8; 32]>>,
) -> Result<JsonlQualityEvaluation> {
    if matches!(policy.mode, GateMode::ChangedOnly) != gated_record_ids.is_some() {
        return Err(InvalidArgumentError::new(
            "changed-only policy evaluation requires the changed record identities",
        )
        .into());
    }
    let inspection =
        inspect_jsonl_with_record_limit(source, id_field, Some(MAX_JSONL_DIFF_RECORDS))?;
    if inspection.finding_count() != 0 {
        return Err(InvalidArgumentError::new(
            "quality evaluation requires a structurally valid JSONL dataset",
        )
        .into());
    }
    if inspection.dataset_content_id() != expected_content_id {
        return Err(ConcurrentModificationError::new(
            "JSONL content does not match the staged quality candidate",
        )
        .into());
    }
    let compiled = policy
        .fields
        .iter()
        .map(|(field, rule)| {
            Ok((
                field.clone(),
                CompiledFieldPolicy {
                    enum_keys: rule.enum_values.as_ref().map(|members| {
                        members
                            .iter()
                            .map(|member| {
                                canonical_scalar_key(member).expect("validated scalar enum")
                            })
                            .collect()
                    }),
                    minimum: rule.minimum.as_ref().map(canonical_number),
                    maximum: rule.maximum.as_ref().map(canonical_number),
                    pattern: rule.pattern.as_deref().map(compile_pattern).transpose()?,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut accumulators = BTreeMap::<RuleKey, FindingAccumulator>::new();
    let mut unique_values = BTreeMap::<String, HashMap<[u8; 32], Vec<([u8; 32], bool)>>>::new();
    let source = source
        .canonicalize()
        .context("cannot resolve JSONL quality source")?;
    let file = File::open(&source).context("cannot open JSONL quality source")?;
    let mut reader = BufReader::new(file);
    let mut content_hasher = blake3::Hasher::new();
    content_hasher.update(b"datajig-jsonl-recordset-v1\0");
    let mut line = Vec::new();
    let mut evaluated_records = 0usize;
    loop {
        line.clear();
        let read = Read::by_ref(&mut reader)
            .take(u64::try_from(MAX_JSONL_LINE_BYTES + 2)?)
            .read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        content_hasher.update(&line);
        let mut content = line.as_slice();
        if content.ends_with(b"\n") {
            content = &content[..content.len() - 1];
        }
        if content.ends_with(b"\r") {
            content = &content[..content.len() - 1];
        }
        let text = std::str::from_utf8(content).map_err(|_| {
            ConcurrentModificationError::new("JSONL changed during quality evaluation")
        })?;
        if text.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(text).map_err(|_| {
            ConcurrentModificationError::new("JSONL changed during quality evaluation")
        })?;
        let object = value.as_object().ok_or_else(|| {
            ConcurrentModificationError::new("JSONL changed during quality evaluation")
        })?;
        let id = object
            .get(id_field)
            .and_then(jsonl_record_id_digest)
            .ok_or_else(|| {
                ConcurrentModificationError::new("JSONL changed during quality evaluation")
            })?;
        evaluated_records += 1;
        let gated = gated_record_ids.is_none_or(|ids| ids.contains(&id));
        for (field, rule) in &policy.fields {
            let compiled_rule = &compiled[field];
            let Some(value) = object.get(field) else {
                if rule.required {
                    record_violation(
                        &mut accumulators,
                        field,
                        "required",
                        "JSONL_POLICY_REQUIRED",
                        id,
                        gated,
                    );
                }
                continue;
            };
            if value.is_null() {
                if !rule.nullable {
                    record_violation(
                        &mut accumulators,
                        field,
                        "null",
                        "JSONL_POLICY_NULL",
                        id,
                        gated,
                    );
                }
                continue;
            }
            let actual_type = json_type(value);
            if !rule.types.is_empty() && !rule.types.iter().any(|allowed| allowed == actual_type) {
                record_violation(
                    &mut accumulators,
                    field,
                    "type",
                    "JSONL_POLICY_TYPE",
                    id,
                    gated,
                );
                continue;
            }
            if compiled_rule.enum_keys.as_ref().is_some_and(|members| {
                let key = canonical_scalar_key(value);
                key.is_none_or(|key| !members.contains(&key))
            }) {
                record_violation(
                    &mut accumulators,
                    field,
                    "enum",
                    "JSONL_POLICY_ENUM",
                    id,
                    gated,
                );
            }
            if let Value::Number(number) = value {
                let candidate = canonical_number(number);
                let below = compiled_rule.minimum.as_ref().is_some_and(|minimum| {
                    compare_canonical_numbers(&candidate, minimum) == Ordering::Less
                });
                let above = compiled_rule.maximum.as_ref().is_some_and(|maximum| {
                    compare_canonical_numbers(&candidate, maximum) == Ordering::Greater
                });
                if below || above {
                    record_violation(
                        &mut accumulators,
                        field,
                        "range",
                        "JSONL_POLICY_RANGE",
                        id,
                        gated,
                    );
                }
            }
            if let (Value::String(value), Some(pattern)) = (value, &compiled_rule.pattern) {
                if !pattern.is_match(value) {
                    record_violation(
                        &mut accumulators,
                        field,
                        "pattern",
                        "JSONL_POLICY_PATTERN",
                        id,
                        gated,
                    );
                }
            }
            if rule.unique {
                if let Some(key) = canonical_scalar_key(value) {
                    let mut hasher = blake3::Hasher::new();
                    hasher.update(b"datajig-jsonl-quality-unique-v1\0");
                    hasher.update(&key);
                    unique_values
                        .entry(field.clone())
                        .or_default()
                        .entry(*hasher.finalize().as_bytes())
                        .or_default()
                        .push((id, gated));
                }
            }
        }
    }
    if format!("records_{}", content_hasher.finalize().to_hex()) != expected_content_id {
        return Err(
            ConcurrentModificationError::new("JSONL changed during quality evaluation").into(),
        );
    }
    for (field, groups) in unique_values {
        for records in groups.into_values().filter(|records| records.len() > 1) {
            let group_gated = records.iter().any(|(_, gated)| *gated);
            for (id, _) in records {
                record_violation(
                    &mut accumulators,
                    &field,
                    "unique",
                    "JSONL_POLICY_UNIQUE",
                    id,
                    group_gated,
                );
            }
        }
    }
    let mut total_violations = 0usize;
    let mut gated_violations = 0usize;
    let findings = accumulators
        .into_iter()
        .filter_map(|((field, rule, code), accumulator)| {
            total_violations += accumulator.total;
            gated_violations += accumulator.gated;
            (accumulator.gated > 0).then(|| JsonlQualityFinding {
                code,
                field,
                rule,
                total_violations: accumulator.total,
                gated_violations: accumulator.gated,
                samples_truncated: accumulator.gated > accumulator.samples.len(),
                sample_ids: accumulator.samples.into_iter().collect(),
            })
        })
        .collect();
    Ok(JsonlQualityEvaluation {
        policy_id: policy.policy_id.clone(),
        mode: policy.mode().into(),
        evaluated_records,
        total_violations,
        gated_violations,
        findings,
    })
}

fn record_violation(
    accumulators: &mut BTreeMap<RuleKey, FindingAccumulator>,
    field: &str,
    rule: &'static str,
    code: &'static str,
    id: [u8; 32],
    gated: bool,
) {
    let entry = accumulators.entry((field.into(), rule, code)).or_default();
    entry.total += 1;
    if gated {
        entry.gated += 1;
        entry
            .samples
            .insert(format!("rid_{}", blake3::Hash::from(id).to_hex()));
        if entry.samples.len() > MAX_JSONL_POLICY_SAMPLES {
            entry.samples.pop_last();
        }
    }
}

fn normalize_number(number: Number) -> Result<Number> {
    let canonical = String::from_utf8(canonical_number(&number))?;
    canonical
        .parse()
        .context("cannot normalize JSON quality policy number")
}

fn require_type(field: &str, policy: &FieldPolicy, expected: &str, constraint: &str) -> Result<()> {
    if policy.types.len() != 1 || policy.types[0] != expected {
        bail!("quality policy field {field} {constraint} requires type {expected}");
    }
    Ok(())
}

fn compile_pattern(pattern: &str) -> Result<Regex> {
    RegexBuilder::new(pattern)
        .size_limit(MAX_JSONL_POLICY_REGEX_SIZE)
        .build()
        .map_err(Into::into)
}

fn json_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn default_true() -> bool {
    true
}
