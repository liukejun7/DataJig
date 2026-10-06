use crate::io::save_file_atomically;
use crate::{
    JsonlQualityEvaluation, JsonlQualityPolicy, JsonlRecordStateBundle, ReviewArtifact,
    ReviewReport, diff_jsonl_record_states,
};
use anyhow::{Context, Result};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub(crate) fn create_jsonl_review(
    before: &JsonlRecordStateBundle,
    after: &JsonlRecordStateBundle,
    policy: Option<&JsonlQualityPolicy>,
    quality: Option<&JsonlQualityEvaluation>,
    metadata: &[(&str, &str)],
    output: &Path,
) -> Result<ReviewArtifact> {
    let payload = jsonl_review_payload(before, after, policy, quality, metadata)?;
    let report: Value = serde_json::from_slice(&payload)?;
    let status = report["policy"]["status"]
        .as_str()
        .context("generated JSONL review status is invalid")?;
    save_file_atomically(output, &payload, "JSONL workspace review")?;
    let output = output
        .canonicalize()
        .context("cannot resolve JSONL workspace review")?
        .to_string_lossy()
        .into_owned();
    Ok(ReviewArtifact {
        bytes: payload.len(),
        content_id: crate::report::report_content_id(&payload),
        findings: report["findings"].as_array().map_or(0, Vec::len),
        output,
        report_schema_version: 1,
        status: status.into(),
    })
}

pub(crate) fn jsonl_review_payload(
    before: &JsonlRecordStateBundle,
    after: &JsonlRecordStateBundle,
    policy: Option<&JsonlQualityPolicy>,
    quality: Option<&JsonlQualityEvaluation>,
    metadata: &[(&str, &str)],
) -> Result<Vec<u8>> {
    let mut findings = validation_findings(after);
    if before.state().finding_count() == 0 && after.state().finding_count() == 0 {
        findings.extend(change_findings(before, after)?);
    }
    if let Some(quality) = quality {
        findings.extend(quality_findings(quality));
    }
    findings.sort_by(|left, right| {
        severity_rank(left["severity"].as_str().unwrap_or("info"))
            .cmp(&severity_rank(right["severity"].as_str().unwrap_or("info")))
            .then_with(|| {
                left["code"]
                    .as_str()
                    .unwrap_or_default()
                    .cmp(right["code"].as_str().unwrap_or_default())
            })
    });
    let failures = findings
        .iter()
        .filter(|finding| finding["severity"] == "error")
        .filter_map(|finding| finding["code"].as_str())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let status = if failures.is_empty() { "pass" } else { "fail" };
    let mut report_metadata = Map::new();
    report_metadata.insert(
        "base_state_id".into(),
        json!(before.state().record_state_id()),
    );
    report_metadata.insert(
        "candidate_state_id".into(),
        json!(after.state().record_state_id()),
    );
    report_metadata.insert(
        "baseline_dataset_content_id".into(),
        json!(before.state().dataset_content_id()),
    );
    report_metadata.insert(
        "candidate_dataset_content_id".into(),
        json!(after.state().dataset_content_id()),
    );
    report_metadata.insert("coverage".into(), json!("records_all_v1"));
    report_metadata.insert("id_field".into(), json!(before.state().id_field()));
    report_metadata.insert("review_engine".into(), json!("rust-jsonl-v1"));
    if let Some(policy) = policy {
        report_metadata.insert("quality_policy_id".into(), json!(policy.policy_id()));
        report_metadata.insert("quality_gate_mode".into(), json!(policy.mode()));
        report_metadata.insert(
            "quality_evaluation".into(),
            json!(if quality.is_some() {
                "complete"
            } else {
                "skipped_structural_failure"
            }),
        );
    }
    for (key, value) in metadata {
        report_metadata.insert((*key).into(), json!(value));
    }
    let effective_policy = findings
        .iter()
        .filter_map(|finding| {
            Some((
                finding["code"].as_str()?.to_owned(),
                finding["severity"].clone(),
            ))
        })
        .collect::<Map<_, _>>();
    let report = json!({
        "namespace": crate::identity::ARTIFACT_NAMESPACE,
        "schema_version": 1,
        "baseline": format!("recordstate:{}", before.state().record_state_id()),
        "candidate": format!("recordstate:{}", after.state().record_state_id()),
        "complete": true,
        "metadata": report_metadata,
        "samples": [],
        "findings": findings,
        "matches": [],
        "distributions": [],
        "policy": {
            "status": status,
            "failures": failures,
            "warnings": [],
            "effective_policy": effective_policy,
        },
    });
    let payload = serde_json::to_vec_pretty(&report)?;
    ReviewReport::from_json(std::str::from_utf8(&payload)?)
        .context("generated JSONL review failed schema validation")?;
    Ok(payload)
}

fn quality_findings(evaluation: &JsonlQualityEvaluation) -> Vec<Value> {
    evaluation
        .findings()
        .iter()
        .map(|item| {
            finding(
                item.code(),
                "error",
                "record values violate the pinned JSONL quality policy",
                item.sample_ids().to_vec(),
                json!({
                    "field": item.field(),
                    "rule": item.rule(),
                    "total_violations": item.total_violations(),
                    "gated_violations": item.gated_violations(),
                    "samples_truncated": item.samples_truncated(),
                }),
            )
        })
        .collect()
}

fn validation_findings(state: &JsonlRecordStateBundle) -> Vec<Value> {
    let (invalid, missing, nulls, compound, duplicates) = state.state().validation_counts();
    [
        (
            "JSONL_INVALID_RECORD",
            invalid,
            "JSONL contains invalid or non-object records",
        ),
        (
            "JSONL_MISSING_ID",
            missing,
            "records are missing the configured ID field",
        ),
        ("JSONL_NULL_ID", nulls, "record IDs contain null values"),
        (
            "JSONL_INVALID_ID",
            compound,
            "record IDs must be strings or numbers",
        ),
        (
            "JSONL_DUPLICATE_ID",
            duplicates,
            "record IDs are not unique",
        ),
    ]
    .into_iter()
    .filter(|(_, count, _)| *count > 0)
    .map(|(code, count, message)| finding(code, "error", message, vec![], json!({"count": count})))
    .collect()
}

fn change_findings(
    before: &JsonlRecordStateBundle,
    after: &JsonlRecordStateBundle,
) -> Result<Vec<Value>> {
    let diff = serde_json::to_value(diff_jsonl_record_states(before, after)?)?;
    let summary = diff["summary"]
        .as_object()
        .context("record diff summary is invalid")?;
    let mut previews = BTreeMap::<&str, Vec<String>>::new();
    for item in diff["change_items"].as_array().into_iter().flatten() {
        if let (Some(kind), Some(record_id)) = (item["kind"].as_str(), item["record_id"].as_str()) {
            previews.entry(kind).or_default().push(record_id.into());
        }
    }
    let mut findings = Vec::new();
    for (kind, code, message) in [
        ("added", "RECORD_ADDED", "records were added"),
        ("removed", "RECORD_REMOVED", "records were removed"),
        ("modified", "RECORD_MODIFIED", "record content changed"),
        (
            "moved",
            "RECORD_MOVED",
            "records moved to different physical lines",
        ),
    ] {
        let count = summary.get(kind).and_then(Value::as_u64).unwrap_or(0);
        if count > 0 {
            findings.push(finding(
                code,
                "info",
                message,
                previews.remove(kind).unwrap_or_default(),
                json!({"count": count}),
            ));
        }
    }
    if diff["byte_only_changed"].as_bool() == Some(true) {
        findings.push(finding(
            "RECORD_BYTES_ONLY_CHANGED",
            "info",
            "JSONL bytes changed while canonical records remained equivalent",
            vec![],
            json!({"count": 1}),
        ));
    }
    Ok(findings)
}

fn finding(
    code: &str,
    severity: &str,
    message: &str,
    sample_ids: Vec<String>,
    evidence: Value,
) -> Value {
    json!({
        "code": code,
        "severity": severity,
        "message": message,
        "sample_ids": sample_ids,
        "evidence": evidence,
    })
}

fn severity_rank(value: &str) -> u8 {
    match value {
        "error" => 0,
        "warning" => 1,
        _ => 2,
    }
}
