use crate::report::{EvidenceScalar, EvidenceValue, canonical_number_value};
use crate::{IndexedFinding, ReviewReport, Severity};
use anyhow::{Result, bail};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;

pub const DEFAULT_PAGE_SIZE: i64 = 50;
pub const MAX_PAGE_SIZE: i64 = 200;
pub const MAX_COMPACT_FINDINGS: i64 = 20;
pub const MAX_COMPACT_JSON_CHARS: usize = 50_000;
const MAX_COMPACT_CODES: usize = 50;
const MAX_COMPACT_SAMPLE_IDS: usize = 5;
const MAX_COMPACT_EVIDENCE_ITEMS: usize = 8;
const MAX_COMPACT_SEQUENCE_ITEMS: usize = 5;
const MAX_COMPACT_TEXT_CHARS: usize = 240;

#[derive(Debug)]
pub struct FindingNotFoundError(String);

impl FindingNotFoundError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for FindingNotFoundError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for FindingNotFoundError {}

pub fn list_findings(
    report: &ReviewReport,
    severities: &[Severity],
    codes: &[String],
    offset: i64,
    limit: i64,
) -> Result<Value> {
    validate_page(offset, limit)?;
    let severity_filter = severities.iter().copied().collect::<BTreeSet<_>>();
    let code_filter = codes.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let filtered = report
        .indexed_findings()?
        .into_iter()
        .filter(|item| {
            (severity_filter.is_empty() || severity_filter.contains(&item.finding().severity()))
                && (code_filter.is_empty() || code_filter.contains(item.finding().code()))
        })
        .collect::<Vec<_>>();
    let start = usize::try_from(offset)
        .unwrap_or(usize::MAX)
        .min(filtered.len());
    let end = start
        .saturating_add(usize::try_from(limit).unwrap_or(0))
        .min(filtered.len());
    let findings = filtered[start..end]
        .iter()
        .map(|item| finding_payload(report, item))
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({
        "agent_api_version": 1,
        "kind": "finding_page",
        "report": report_descriptor(report),
        "filters": {
            "severities": severity_filter.into_iter().map(Severity::as_str).collect::<Vec<_>>(),
            "codes": code_filter,
        },
        "page": {
            "offset": offset,
            "limit": limit,
            "returned": findings.len(),
            "total": filtered.len(),
            "has_more": end < filtered.len(),
        },
        "findings": findings,
    }))
}

pub fn get_finding(report: &ReviewReport, finding_id: &str) -> Result<Value> {
    let indexed = report.indexed_findings()?;
    let item = indexed
        .iter()
        .find(|item| item.id() == finding_id)
        .ok_or_else(|| {
            FindingNotFoundError::new(format!("finding {finding_id:?} was not found"))
        })?;
    Ok(json!({
        "agent_api_version": 1,
        "kind": "finding",
        "report": report_descriptor(report),
        "finding": finding_payload(report, item)?,
    }))
}

pub fn compact_summary(report: &ReviewReport, limit: i64) -> Result<Value> {
    if !(0..=MAX_COMPACT_FINDINGS).contains(&limit) {
        bail!("limit must be between 0 and {MAX_COMPACT_FINDINGS}");
    }
    let indexed = report.indexed_findings()?;
    let mut severity_counts = HashMap::<&str, usize>::new();
    let mut code_counts = HashMap::<&str, usize>::new();
    for item in &indexed {
        *severity_counts
            .entry(item.finding().severity().as_str())
            .or_default() += 1;
        *code_counts.entry(item.finding().code()).or_default() += 1;
    }
    let compact_codes = compact_code_counts(&code_counts);
    let code_kinds_total = code_counts.len();
    let selected = indexed
        .iter()
        .take(usize::try_from(limit).unwrap_or(0))
        .map(|item| compact_finding_payload(report, item))
        .collect::<Vec<_>>();
    let mut result = json!({
        "agent_api_version": 1,
        "kind": "compact_summary",
        "report": compact_report_descriptor(report),
        "counts": {
            "total": indexed.len(),
            "by_severity": {
                "error": severity_counts.get("error").copied().unwrap_or(0),
                "info": severity_counts.get("info").copied().unwrap_or(0),
                "warning": severity_counts.get("warning").copied().unwrap_or(0),
            },
            "by_code": compact_codes,
        },
        "code_kinds_total": code_kinds_total,
        "code_kinds_truncated": code_kinds_total > compact_codes_len(&code_counts),
        "findings": selected,
        "truncated": indexed.len() > usize::try_from(limit).unwrap_or(0).min(indexed.len()),
        "budget_truncated": false,
    });
    while result["findings"]
        .as_array()
        .is_some_and(|findings| !findings.is_empty())
        && json_chars(&result)? > MAX_COMPACT_JSON_CHARS
    {
        result["findings"].as_array_mut().expect("array").pop();
        result["truncated"] = Value::Bool(true);
        result["budget_truncated"] = Value::Bool(true);
    }
    Ok(result)
}

fn report_descriptor(report: &ReviewReport) -> Value {
    json!({
        "schema_version": report.schema_version(),
        "baseline": report.baseline(),
        "candidate": report.candidate(),
        "complete": report.complete(),
        "status": report.status(),
    })
}

fn compact_report_descriptor(report: &ReviewReport) -> Value {
    json!({
        "schema_version": report.schema_version(),
        "baseline": compact_text(report.baseline()),
        "candidate": compact_text(report.candidate()),
        "complete": report.complete(),
        "status": report.status(),
    })
}

fn finding_payload(report: &ReviewReport, indexed: &IndexedFinding<'_>) -> Result<Value> {
    let finding = indexed.finding();
    let fallback = finding.severity().as_str();
    let mut sample_ids = finding.sample_ids().to_vec();
    sample_ids.sort_unstable();
    Ok(json!({
        "id": indexed.id(),
        "code": finding.code(),
        "severity": fallback,
        "effective_severity": report.effective_severity(finding.code(), fallback),
        "message": finding.message(),
        "sample_ids": sample_ids,
        "evidence": evidence_map_value(finding.evidence())?,
    }))
}

fn compact_finding_payload(report: &ReviewReport, indexed: &IndexedFinding<'_>) -> Value {
    let finding = indexed.finding();
    let fallback = finding.severity().as_str();
    let mut sample_ids = finding.sample_ids().to_vec();
    sample_ids.sort_unstable();
    let evidence = finding
        .evidence()
        .0
        .iter()
        .take(MAX_COMPACT_EVIDENCE_ITEMS)
        .fold(Map::new(), |mut values, (key, value)| {
            values.insert(compact_text(key), compact_evidence_value(value));
            values
        });
    json!({
        "id": indexed.id(),
        "code": compact_text(finding.code()),
        "severity": fallback,
        "effective_severity": report.effective_severity(finding.code(), fallback),
        "message": compact_text(finding.message()),
        "message_truncated": finding.message().chars().count() > MAX_COMPACT_TEXT_CHARS,
        "sample_ids": sample_ids.iter().take(MAX_COMPACT_SAMPLE_IDS).map(|item| compact_text(item)).collect::<Vec<_>>(),
        "sample_ids_total": sample_ids.len(),
        "sample_ids_truncated": sample_ids.len() > MAX_COMPACT_SAMPLE_IDS,
        "evidence": evidence,
        "evidence_total": finding.evidence().0.len(),
        "evidence_truncated": finding.evidence().0.len() > MAX_COMPACT_EVIDENCE_ITEMS,
    })
}

fn compact_code_counts(counts: &HashMap<&str, usize>) -> BTreeMap<String, usize> {
    let mut ordered = counts
        .iter()
        .map(|(code, count)| (*code, *count))
        .collect::<Vec<_>>();
    ordered.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(right.0)));
    let mut compact = BTreeMap::new();
    for (code, count) in ordered.into_iter().take(MAX_COMPACT_CODES) {
        *compact.entry(compact_text(code)).or_default() += count;
    }
    compact
}

fn compact_codes_len(counts: &HashMap<&str, usize>) -> usize {
    compact_code_counts(counts).len()
}

fn compact_evidence_value(value: &EvidenceValue) -> Value {
    match value {
        EvidenceValue::Sequence(values) => json!({
            "items": values.iter().take(MAX_COMPACT_SEQUENCE_ITEMS).map(evidence_scalar_value).collect::<Vec<_>>(),
            "total": values.len(),
            "truncated": values.len() > MAX_COMPACT_SEQUENCE_ITEMS,
        }),
        EvidenceValue::String(value) => Value::String(compact_text(value)),
        EvidenceValue::Null => Value::Null,
        EvidenceValue::Bool(value) => Value::Bool(*value),
        EvidenceValue::Number(value) => canonical_number_value(value).expect("validated number"),
    }
}

fn evidence_scalar_value(value: &EvidenceScalar) -> Value {
    match value {
        EvidenceScalar::String(value) => Value::String(compact_text(value)),
        EvidenceScalar::Null => Value::Null,
        EvidenceScalar::Bool(value) => Value::Bool(*value),
        EvidenceScalar::Number(value) => canonical_number_value(value).expect("validated number"),
    }
}

fn compact_text(value: &str) -> String {
    if value.chars().count() <= MAX_COMPACT_TEXT_CHARS {
        return value.to_owned();
    }
    let mut compact = value
        .chars()
        .take(MAX_COMPACT_TEXT_CHARS - 1)
        .collect::<String>();
    compact.push('…');
    compact
}

fn evidence_map_value(evidence: &crate::report::BoundedMap<EvidenceValue>) -> Result<Value> {
    let mut values = Map::new();
    for (key, value) in &evidence.0 {
        values.insert(key.clone(), evidence_value(value)?);
    }
    Ok(Value::Object(values))
}

fn evidence_value(value: &EvidenceValue) -> Result<Value> {
    Ok(match value {
        EvidenceValue::Null => Value::Null,
        EvidenceValue::Bool(value) => Value::Bool(*value),
        EvidenceValue::Number(value) => canonical_number_value(value)?,
        EvidenceValue::String(value) => Value::String(value.clone()),
        EvidenceValue::Sequence(values) => Value::Array(
            values
                .iter()
                .map(|value| match value {
                    EvidenceScalar::Null => Ok(Value::Null),
                    EvidenceScalar::Bool(value) => Ok(Value::Bool(*value)),
                    EvidenceScalar::Number(value) => canonical_number_value(value),
                    EvidenceScalar::String(value) => Ok(Value::String(value.clone())),
                })
                .collect::<Result<Vec<_>>>()?,
        ),
    })
}

fn json_chars(value: &Value) -> Result<usize> {
    Ok(serde_json::to_string(value)?.chars().count())
}

fn validate_page(offset: i64, limit: i64) -> Result<()> {
    if offset < 0 {
        bail!("offset must be non-negative");
    }
    if !(1..=MAX_PAGE_SIZE).contains(&limit) {
        bail!("limit must be between 1 and {MAX_PAGE_SIZE}");
    }
    Ok(())
}
