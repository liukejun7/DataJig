use anyhow::{Context, Result, bail};
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs;
use std::path::Path;

pub const MAX_REPORT_BYTES: usize = 256 * 1024 * 1024;
pub const MAX_REPORT_ITEMS: usize = 250_000;
const MAX_REPORT_STRING_BYTES: usize = 1024 * 1024;
const REPORT_SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warning,
    Info,
}

impl Severity {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warning => "warning",
            Self::Info => "info",
        }
    }

    fn rank(self) -> u8 {
        match self {
            Self::Error => 0,
            Self::Warning => 1,
            Self::Info => 2,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Finding {
    #[serde(deserialize_with = "deserialize_bounded_string")]
    code: String,
    evidence: BoundedMap<EvidenceValue>,
    #[serde(default, deserialize_with = "deserialize_optional_bounded_string")]
    id: Option<String>,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    message: String,
    #[serde(deserialize_with = "deserialize_bounded_string_vec")]
    sample_ids: BoundedVec<String>,
    severity: Severity,
}

impl Finding {
    pub fn code(&self) -> &str {
        &self.code
    }

    pub fn severity(&self) -> Severity {
        self.severity
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn sample_ids(&self) -> &[String] {
        &self.sample_ids.0
    }

    pub(crate) fn evidence(&self) -> &BoundedMap<EvidenceValue> {
        &self.evidence
    }

    pub(crate) fn evidence_bool(&self, key: &str) -> Option<bool> {
        match self.evidence.0.get(key) {
            Some(EvidenceValue::Bool(value)) => Some(*value),
            _ => None,
        }
    }

    pub(crate) fn evidence_usize(&self, key: &str) -> Option<usize> {
        match self.evidence.0.get(key) {
            Some(EvidenceValue::Number(value)) => {
                value.as_u64().and_then(|value| usize::try_from(value).ok())
            }
            _ => None,
        }
    }

    pub(crate) fn evidence_string(&self, key: &str) -> Option<&str> {
        match self.evidence.0.get(key) {
            Some(EvidenceValue::String(value)) => Some(value),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct IndexedFinding<'a> {
    id: String,
    finding: &'a Finding,
}

impl<'a> IndexedFinding<'a> {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn finding(&self) -> &'a Finding {
        self.finding
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewReport {
    #[serde(deserialize_with = "deserialize_bounded_string")]
    namespace: String,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    baseline: String,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    candidate: String,
    complete: bool,
    distributions: BoundedVec<DistributionDelta>,
    findings: BoundedVec<Finding>,
    matches: BoundedVec<SampleMatch>,
    metadata: BoundedMap<EvidenceValue>,
    policy: PolicyResult,
    samples: BoundedVec<SampleRecord>,
    schema_version: u8,
}

impl ReviewReport {
    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_REPORT_BYTES {
            bail!("report is too large (max {MAX_REPORT_BYTES} bytes)");
        }
        let report: Self = serde_json::from_str(payload).context("invalid report JSON")?;
        report.validate()?;
        Ok(report)
    }

    pub fn baseline(&self) -> &str {
        &self.baseline
    }

    pub fn candidate(&self) -> &str {
        &self.candidate
    }

    pub fn complete(&self) -> bool {
        self.complete
    }

    pub fn status(&self) -> &str {
        self.policy.status.as_str()
    }

    pub fn schema_version(&self) -> u8 {
        self.schema_version
    }

    pub fn metadata_string(&self, key: &str) -> Option<&str> {
        match self.metadata.0.get(key) {
            Some(EvidenceValue::String(value)) => Some(value),
            _ => None,
        }
    }

    pub fn indexed_findings(&self) -> Result<Vec<IndexedFinding<'_>>> {
        struct Prepared<'a> {
            finding: &'a Finding,
            fingerprint: String,
            sample_ids: Vec<&'a str>,
        }

        let mut prepared = self
            .findings
            .0
            .iter()
            .map(|finding| {
                let mut sample_ids = finding
                    .sample_ids()
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                sample_ids.sort_unstable();
                Ok(Prepared {
                    finding,
                    fingerprint: finding_fingerprint(finding)?,
                    sample_ids,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        prepared.sort_by(|left, right| {
            left.finding
                .severity
                .rank()
                .cmp(&right.finding.severity.rank())
                .then_with(|| left.finding.code.cmp(&right.finding.code))
                .then_with(|| left.sample_ids.cmp(&right.sample_ids))
                .then_with(|| left.finding.message.cmp(&right.finding.message))
                .then_with(|| left.fingerprint.cmp(&right.fingerprint))
        });
        let mut occurrences = HashMap::<String, usize>::new();
        Ok(prepared
            .into_iter()
            .map(|item| {
                let occurrence = occurrences.entry(item.fingerprint.clone()).or_default();
                *occurrence += 1;
                IndexedFinding {
                    id: format!("fnd_{}_{:04}", &item.fingerprint[..20], occurrence),
                    finding: item.finding,
                }
            })
            .collect())
    }

    pub fn effective_severity<'a>(&'a self, code: &str, fallback: &'a str) -> &'a str {
        match self.policy.effective_policy.0.get(code) {
            Some(EvidenceValue::String(value)) => value,
            _ => fallback,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.namespace != crate::identity::ARTIFACT_NAMESPACE {
            bail!("unsupported report namespace {}", self.namespace);
        }
        if self.schema_version != REPORT_SCHEMA_VERSION {
            bail!("unsupported report schema {}", self.schema_version);
        }
        validate_string(&self.baseline, "baseline")?;
        validate_string(&self.candidate, "candidate")?;
        validate_evidence_map(&self.metadata, "metadata")?;
        for sample in &self.samples.0 {
            sample.validate()?;
        }
        for finding in &self.findings.0 {
            finding.validate()?;
        }
        for item in &self.matches.0 {
            item.validate()?;
        }
        for item in &self.distributions.0 {
            item.validate()?;
        }
        self.policy.validate()
    }
}

pub fn load_report(path: &Path) -> Result<ReviewReport> {
    let metadata = fs::metadata(path).context("cannot inspect report")?;
    if metadata.len() > MAX_REPORT_BYTES as u64 {
        bail!("report is too large (max {MAX_REPORT_BYTES} bytes)");
    }
    let payload = fs::read(path).context("cannot read report")?;
    let payload = String::from_utf8(payload).context("report is not valid UTF-8")?;
    ReviewReport::from_json(&payload)
}

impl Finding {
    fn validate(&self) -> Result<()> {
        validate_string(&self.code, "finding.code")?;
        validate_string(&self.message, "finding.message")?;
        if let Some(id) = &self.id {
            validate_string(id, "finding.id")?;
        }
        for sample_id in &self.sample_ids.0 {
            validate_string(sample_id, "finding.sample_ids")?;
        }
        validate_evidence_map(&self.evidence, "finding.evidence")
    }
}

#[derive(Clone, Debug)]
pub(crate) enum EvidenceValue {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Sequence(Vec<EvidenceScalar>),
}

impl<'de> Deserialize<'de> for EvidenceValue {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(EvidenceValueVisitor)
    }
}

struct EvidenceValueVisitor;

impl<'de> Visitor<'de> for EvidenceValueVisitor {
    type Value = EvidenceValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a scalar or bounded scalar array")
    }

    fn visit_unit<E>(self) -> std::result::Result<Self::Value, E> {
        Ok(EvidenceValue::Null)
    }

    fn visit_bool<E>(self, value: bool) -> std::result::Result<Self::Value, E> {
        Ok(EvidenceValue::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E> {
        Ok(EvidenceValue::Number(value.into()))
    }

    fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E> {
        Ok(EvidenceValue::Number(value.into()))
    }

    fn visit_f64<E>(self, value: f64) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        serde_json::Number::from_f64(value)
            .map(EvidenceValue::Number)
            .ok_or_else(|| E::custom("evidence numbers must be finite"))
    }

    fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        bounded_owned(value, "evidence string")
            .map(EvidenceValue::String)
            .map_err(E::custom)
    }

    fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if value.len() > MAX_REPORT_STRING_BYTES {
            return Err(E::custom(format_args!(
                "evidence string exceeds {MAX_REPORT_STRING_BYTES} UTF-8 bytes"
            )));
        }
        Ok(EvidenceValue::String(value))
    }

    fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values =
            Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_REPORT_ITEMS));
        while let Some(value) = sequence.next_element()? {
            if values.len() == MAX_REPORT_ITEMS {
                return Err(serde::de::Error::custom(format_args!(
                    "evidence array exceeds {MAX_REPORT_ITEMS} entries"
                )));
            }
            values.push(value);
        }
        Ok(EvidenceValue::Sequence(values))
    }

    fn visit_map<A>(self, map: A) -> std::result::Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        serde_json::Number::deserialize(serde::de::value::MapAccessDeserializer::new(map))
            .map(EvidenceValue::Number)
            .map_err(|_| serde::de::Error::custom("evidence objects are not supported"))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum EvidenceScalar {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
}

impl<'de> Deserialize<'de> for EvidenceScalar {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        match EvidenceValue::deserialize(deserializer)? {
            EvidenceValue::Null => Ok(Self::Null),
            EvidenceValue::Bool(value) => Ok(Self::Bool(value)),
            EvidenceValue::Number(value) => Ok(Self::Number(value)),
            EvidenceValue::String(value) => Ok(Self::String(value)),
            EvidenceValue::Sequence(_) => Err(serde::de::Error::custom(
                "nested evidence arrays are not supported",
            )),
        }
    }
}

fn validate_evidence_map(values: &BoundedMap<EvidenceValue>, name: &str) -> Result<()> {
    for (key, value) in &values.0 {
        validate_string(key, name)?;
        value.validate(name)?;
    }
    Ok(())
}

impl EvidenceValue {
    fn validate(&self, name: &str) -> Result<()> {
        match self {
            Self::String(value) => validate_string(value, name),
            Self::Sequence(values) => {
                if values.len() > MAX_REPORT_ITEMS {
                    bail!("{name} exceeds {MAX_REPORT_ITEMS} entries");
                }
                for value in values {
                    if let EvidenceScalar::String(value) = value {
                        validate_string(value, name)?;
                    }
                }
                Ok(())
            }
            Self::Null | Self::Bool(_) | Self::Number(_) => Ok(()),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SampleRecord {
    channels: Option<i64>,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    content_hash: String,
    #[serde(deserialize_with = "deserialize_optional_bounded_string")]
    decode_error: Option<String>,
    height: Option<i64>,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    label: String,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    logical_path: String,
    #[serde(deserialize_with = "deserialize_optional_bounded_string")]
    media_format: Option<String>,
    mtime_ns: i128,
    #[serde(deserialize_with = "deserialize_optional_bounded_string")]
    perceptual_hash: Option<String>,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    relative_path: String,
    size: i128,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    snapshot: String,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    split: String,
    width: Option<i64>,
}

impl SampleRecord {
    fn validate(&self) -> Result<()> {
        let _ = (
            self.channels,
            self.height,
            self.mtime_ns,
            self.size,
            self.width,
        );
        for (value, name) in [
            (&self.content_hash, "sample.content_hash"),
            (&self.label, "sample.label"),
            (&self.logical_path, "sample.logical_path"),
            (&self.relative_path, "sample.relative_path"),
            (&self.snapshot, "sample.snapshot"),
            (&self.split, "sample.split"),
        ] {
            validate_string(value, name)?;
        }
        for (value, name) in [
            (&self.decode_error, "sample.decode_error"),
            (&self.media_format, "sample.media_format"),
            (&self.perceptual_hash, "sample.perceptual_hash"),
        ] {
            if let Some(value) = value {
                validate_string(value, name)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SampleMatch {
    #[serde(deserialize_with = "deserialize_bounded_string")]
    baseline_id: String,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    candidate_id: String,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    confidence: String,
    distance: Option<i64>,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    method: String,
}

impl SampleMatch {
    fn validate(&self) -> Result<()> {
        if self.distance.is_some_and(|value| value < 0) {
            bail!("match.distance must be non-negative");
        }
        for (value, name) in [
            (&self.baseline_id, "match.baseline_id"),
            (&self.candidate_id, "match.candidate_id"),
            (&self.confidence, "match.confidence"),
            (&self.method, "match.method"),
        ] {
            validate_string(value, name)?;
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DistributionDelta {
    after_count: i128,
    after_proportion: f64,
    before_count: i128,
    before_proportion: f64,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    dimension: String,
    #[serde(deserialize_with = "deserialize_bounded_string")]
    key: String,
    percentage_delta: Option<f64>,
}

impl DistributionDelta {
    fn validate(&self) -> Result<()> {
        let _ = (self.after_count, self.before_count);
        if !self.after_proportion.is_finite()
            || !self.before_proportion.is_finite()
            || self
                .percentage_delta
                .is_some_and(|value| !value.is_finite())
        {
            bail!("distribution numbers must be finite");
        }
        validate_string(&self.dimension, "distribution.dimension")?;
        validate_string(&self.key, "distribution.key")
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ReviewStatus {
    Pass,
    Warn,
    Fail,
    Incomplete,
}

impl ReviewStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Warn => "warn",
            Self::Fail => "fail",
            Self::Incomplete => "incomplete",
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyResult {
    effective_policy: BoundedMap<EvidenceValue>,
    #[serde(deserialize_with = "deserialize_bounded_string_vec")]
    failures: BoundedVec<String>,
    status: ReviewStatus,
    #[serde(deserialize_with = "deserialize_bounded_string_vec")]
    warnings: BoundedVec<String>,
}

impl PolicyResult {
    fn validate(&self) -> Result<()> {
        validate_evidence_map(&self.effective_policy, "policy.effective_policy")?;
        for value in self.failures.0.iter().chain(&self.warnings.0) {
            validate_string(value, "policy message")?;
        }
        Ok(())
    }
}

struct Fingerprint<'a> {
    code: &'a str,
    evidence: &'a BoundedMap<EvidenceValue>,
    message: &'a str,
    sample_ids: Vec<&'a str>,
    severity: &'a str,
}

fn canonical_fingerprint(identity: &Fingerprint<'_>) -> Result<String> {
    let code = serde_json::to_string(identity.code)?;
    let message = serde_json::to_string(identity.message)?;
    let severity = serde_json::to_string(identity.severity)?;
    let sample_ids = identity
        .sample_ids
        .iter()
        .map(serde_json::to_string)
        .collect::<std::result::Result<Vec<_>, _>>()?
        .join(",");
    let evidence = identity
        .evidence
        .0
        .iter()
        .map(|(key, value)| {
            Ok(format!(
                "{}:{}",
                serde_json::to_string(key)?,
                canonical_evidence(value)?
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join(",");
    Ok(format!(
        "{{\"code\":{code},\"evidence\":{{{evidence}}},\"message\":{message},\"sample_ids\":[{sample_ids}],\"severity\":{severity}}}"
    ))
}

fn canonical_evidence(value: &EvidenceValue) -> Result<String> {
    match value {
        EvidenceValue::Null => Ok("null".to_owned()),
        EvidenceValue::Bool(value) => Ok(value.to_string()),
        EvidenceValue::Number(value) => python_number_lexeme(value),
        EvidenceValue::String(value) => Ok(serde_json::to_string(value)?),
        EvidenceValue::Sequence(values) => Ok(format!(
            "[{}]",
            values
                .iter()
                .map(canonical_scalar)
                .collect::<Result<Vec<_>>>()?
                .join(",")
        )),
    }
}

fn canonical_scalar(value: &EvidenceScalar) -> Result<String> {
    match value {
        EvidenceScalar::Null => Ok("null".to_owned()),
        EvidenceScalar::Bool(value) => Ok(value.to_string()),
        EvidenceScalar::Number(value) => python_number_lexeme(value),
        EvidenceScalar::String(value) => Ok(serde_json::to_string(value)?),
    }
}

pub(crate) fn canonical_number_value(value: &serde_json::Number) -> Result<serde_json::Value> {
    Ok(serde_json::from_str(&python_number_lexeme(value)?)?)
}

fn python_number_lexeme(value: &serde_json::Number) -> Result<String> {
    let raw = value.to_string();
    if !raw
        .chars()
        .any(|character| matches!(character, '.' | 'e' | 'E'))
    {
        return Ok(raw);
    }
    let number = raw
        .parse::<f64>()
        .context("evidence number is out of range")?;
    if !number.is_finite() {
        bail!("evidence numbers must be finite");
    }
    let rust = serde_json::Number::from_f64(number)
        .context("evidence numbers must be finite")?
        .to_string();
    if let Some(normalized) = normalized_exponent(&rust)? {
        return Ok(normalized);
    }
    let absolute = number.abs();
    if number != 0.0 && !(1e-4..1e16).contains(&absolute) {
        let scientific = format!("{number:e}");
        return normalized_exponent(&scientific)?
            .context("scientific number is missing an exponent");
    }
    Ok(rust)
}

fn normalized_exponent(value: &str) -> Result<Option<String>> {
    let Some(index) = value.find(['e', 'E']) else {
        return Ok(None);
    };
    let mantissa = &value[..index];
    let exponent = value[index + 1..]
        .parse::<i32>()
        .context("invalid number exponent")?;
    Ok(Some(format!("{mantissa}e{exponent:+03}")))
}

fn bounded_owned(value: &str, name: &str) -> std::result::Result<String, String> {
    if value.len() > MAX_REPORT_STRING_BYTES {
        return Err(format!(
            "{name} exceeds {MAX_REPORT_STRING_BYTES} UTF-8 bytes"
        ));
    }
    Ok(value.to_owned())
}

struct BoundedString(String);

impl<'de> Deserialize<'de> for BoundedString {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BoundedStringVisitor;

        impl Visitor<'_> for BoundedStringVisitor {
            type Value = BoundedString;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded UTF-8 string")
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                bounded_owned(value, "report string")
                    .map(BoundedString)
                    .map_err(E::custom)
            }

            fn visit_string<E>(self, value: String) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                if value.len() > MAX_REPORT_STRING_BYTES {
                    return Err(E::custom(format_args!(
                        "report string exceeds {MAX_REPORT_STRING_BYTES} UTF-8 bytes"
                    )));
                }
                Ok(BoundedString(value))
            }
        }

        deserializer.deserialize_string(BoundedStringVisitor)
    }
}

fn deserialize_bounded_string<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(BoundedString::deserialize(deserializer)?.0)
}

fn deserialize_optional_bounded_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<BoundedString>::deserialize(deserializer)?.map(|value| value.0))
}

fn deserialize_bounded_string_vec<'de, D>(
    deserializer: D,
) -> std::result::Result<BoundedVec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct StringVecVisitor;

    impl<'de> Visitor<'de> for StringVecVisitor {
        type Value = BoundedVec<String>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded string array")
        }

        fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
        where
            A: SeqAccess<'de>,
        {
            let mut values =
                Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_REPORT_ITEMS));
            while let Some(value) = sequence.next_element::<BoundedString>()? {
                if values.len() == MAX_REPORT_ITEMS {
                    return Err(serde::de::Error::custom(format_args!(
                        "report array exceeds {MAX_REPORT_ITEMS} entries"
                    )));
                }
                values.push(value.0);
            }
            Ok(BoundedVec(values))
        }
    }

    deserializer.deserialize_seq(StringVecVisitor)
}

fn finding_fingerprint(finding: &Finding) -> Result<String> {
    let mut sample_ids = finding
        .sample_ids()
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    sample_ids.sort_unstable();
    let identity = Fingerprint {
        code: finding.code(),
        evidence: finding.evidence(),
        message: finding.message(),
        sample_ids,
        severity: finding.severity().as_str(),
    };
    let payload = canonical_fingerprint(&identity)?;
    let mut hasher = Sha256::new();
    hasher.update(b"datajig-finding-v1\0");
    hasher.update(payload.as_bytes());
    Ok(format!("{:x}", hasher.finalize()))
}

pub(crate) fn report_content_id(payload: &[u8]) -> String {
    crate::identity::blake3_content_id("review", b"datajig-review-v1\0", payload)
}

fn validate_string(value: &str, name: &str) -> Result<()> {
    if value.len() > MAX_REPORT_STRING_BYTES {
        bail!("{name} exceeds {MAX_REPORT_STRING_BYTES} UTF-8 bytes");
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct BoundedVec<T>(Vec<T>);

impl<'de, T> Deserialize<'de> for BoundedVec<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BoundedVisitor<T>(std::marker::PhantomData<T>);

        impl<'de, T> Visitor<'de> for BoundedVisitor<T>
        where
            T: Deserialize<'de>,
        {
            type Value = BoundedVec<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded report array")
            }

            fn visit_seq<A>(self, mut sequence: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                if sequence
                    .size_hint()
                    .is_some_and(|size| size > MAX_REPORT_ITEMS)
                {
                    return Err(serde::de::Error::custom(format_args!(
                        "report array exceeds {MAX_REPORT_ITEMS} entries"
                    )));
                }
                let mut values =
                    Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_REPORT_ITEMS));
                while let Some(value) = sequence.next_element()? {
                    if values.len() == MAX_REPORT_ITEMS {
                        return Err(serde::de::Error::custom(format_args!(
                            "report array exceeds {MAX_REPORT_ITEMS} entries"
                        )));
                    }
                    values.push(value);
                }
                Ok(BoundedVec(values))
            }
        }

        deserializer.deserialize_seq(BoundedVisitor(std::marker::PhantomData))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct BoundedMap<T>(pub(crate) BTreeMap<String, T>);

impl<'de, T> Deserialize<'de> for BoundedMap<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct BoundedMapVisitor<T>(std::marker::PhantomData<T>);

        impl<'de, T> Visitor<'de> for BoundedMapVisitor<T>
        where
            T: Deserialize<'de>,
        {
            type Value = BoundedMap<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded object with unique keys")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                while let Some(BoundedString(key)) = map.next_key::<BoundedString>()? {
                    if values.len() == MAX_REPORT_ITEMS {
                        return Err(serde::de::Error::custom(format_args!(
                            "report object exceeds {MAX_REPORT_ITEMS} entries"
                        )));
                    }
                    if values.contains_key(&key) {
                        return Err(serde::de::Error::custom(format_args!(
                            "duplicate map key {key:?}"
                        )));
                    }
                    values.insert(key, map.next_value()?);
                }
                Ok(BoundedMap(values))
            }
        }

        deserializer.deserialize_map(BoundedMapVisitor(std::marker::PhantomData))
    }
}
