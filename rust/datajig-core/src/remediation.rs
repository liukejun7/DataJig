use crate::io::save_file_atomically;
use crate::{MAX_REPORT_BYTES, ReviewReport};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::fs;
use std::path::Path;

pub const REMEDIATION_PLAN_SCHEMA_VERSION: u8 = 1;
pub const MAX_REMEDIATION_ACTIONS: usize = 50_000;
const SAMPLE_PREVIEW_LIMIT: usize = 5;

#[derive(Clone, Debug, Serialize)]
pub struct RemediationPlanArtifact {
    pub actions: usize,
    pub content_id: String,
    pub decision: String,
    pub output: String,
    pub report_content_id: String,
    pub schema_version: u8,
}

#[derive(Debug, Serialize)]
struct RemediationPlan {
    namespace: &'static str,
    schema_version: u8,
    report_content_id: String,
    report_status: String,
    decision: String,
    summary: RemediationSummary,
    actions: Vec<RemediationAction>,
}

#[derive(Debug, Serialize)]
struct RemediationSummary {
    actions: usize,
    errors: usize,
    warnings: usize,
    info: usize,
}

#[derive(Debug, Serialize)]
struct RemediationAction {
    id: String,
    finding_id: String,
    code: String,
    effective_severity: String,
    kind: &'static str,
    mode: &'static str,
    risk: &'static str,
    instruction: &'static str,
    sample_count: usize,
    sample_ids_preview: Vec<String>,
    verify_with: &'static str,
}

pub fn create_remediation_plan(report: &Path, output: &Path) -> Result<RemediationPlanArtifact> {
    let metadata = fs::metadata(report).context("cannot inspect workspace review")?;
    if metadata.len() > MAX_REPORT_BYTES as u64 {
        bail!("report is too large (max {MAX_REPORT_BYTES} bytes)");
    }
    let report_payload = fs::read(report).context("cannot read workspace review")?;
    create_remediation_plan_from_payload(&report_payload, output)
}

pub(crate) fn create_remediation_plan_from_payload(
    report_payload: &[u8],
    output: &Path,
) -> Result<RemediationPlanArtifact> {
    if report_payload.len() > MAX_REPORT_BYTES {
        bail!("report is too large (max {MAX_REPORT_BYTES} bytes)");
    }
    let report_text = std::str::from_utf8(report_payload).context("report is not valid UTF-8")?;
    let report = ReviewReport::from_json(report_text)?;
    let report_content_id = crate::report::report_content_id(report_payload);
    let decision = match report.status() {
        "pass" => "seal",
        "warn" => "inspect",
        "fail" => "fix",
        _ => "retry",
    };
    let indexed = if decision == "seal" {
        Vec::new()
    } else {
        report.indexed_findings()?
    };
    if indexed.len() > MAX_REMEDIATION_ACTIONS {
        bail!("remediation plan exceeds {MAX_REMEDIATION_ACTIONS} actions");
    }

    let mut summary = RemediationSummary {
        actions: indexed.len(),
        errors: 0,
        warnings: 0,
        info: 0,
    };
    let mut actions = Vec::with_capacity(indexed.len());
    for indexed_finding in indexed {
        let finding = indexed_finding.finding();
        let fallback = finding.severity().as_str();
        let configured_severity = report.effective_severity(finding.code(), fallback);
        let effective_severity = match configured_severity {
            "error" | "warning" | "info" => configured_severity,
            _ => fallback,
        };
        match effective_severity {
            "error" => summary.errors += 1,
            "warning" => summary.warnings += 1,
            _ => summary.info += 1,
        }
        let strategy = strategy(finding.code());
        let action_id = action_id(indexed_finding.id(), strategy.kind);
        actions.push(RemediationAction {
            id: action_id,
            finding_id: indexed_finding.id().into(),
            code: finding.code().into(),
            effective_severity: effective_severity.into(),
            kind: strategy.kind,
            mode: "agent_decision",
            risk: strategy.risk,
            instruction: strategy.instruction,
            sample_count: finding.sample_ids().len(),
            sample_ids_preview: finding
                .sample_ids()
                .iter()
                .take(SAMPLE_PREVIEW_LIMIT)
                .cloned()
                .collect(),
            verify_with: "check",
        });
    }
    actions.sort_by(|left, right| {
        severity_rank(&left.effective_severity)
            .cmp(&severity_rank(&right.effective_severity))
            .then_with(|| left.code.cmp(&right.code))
            .then_with(|| left.finding_id.cmp(&right.finding_id))
    });
    let plan = RemediationPlan {
        namespace: crate::identity::ARTIFACT_NAMESPACE,
        schema_version: REMEDIATION_PLAN_SCHEMA_VERSION,
        report_content_id: report_content_id.clone(),
        report_status: report.status().into(),
        decision: decision.into(),
        summary,
        actions,
    };
    let payload = serde_json::to_vec_pretty(&plan)?;
    let content_id =
        crate::identity::blake3_content_id("plan", b"datajig-remediation-plan-v1\0", &payload);
    save_file_atomically(output, &payload, "remediation plan")?;
    let output = output
        .canonicalize()
        .context("cannot resolve remediation plan")?
        .to_string_lossy()
        .into_owned();
    Ok(RemediationPlanArtifact {
        actions: plan.actions.len(),
        content_id,
        decision: decision.into(),
        output,
        report_content_id,
        schema_version: REMEDIATION_PLAN_SCHEMA_VERSION,
    })
}

fn severity_rank(severity: &str) -> u8 {
    match severity {
        "error" => 0,
        "warning" => 1,
        _ => 2,
    }
}

struct Strategy {
    kind: &'static str,
    risk: &'static str,
    instruction: &'static str,
}

fn strategy(code: &str) -> Strategy {
    match code {
        "JSONL_POLICY_REQUIRED" => Strategy {
            kind: "supply_required_field",
            risk: "medium",
            instruction: "Add the required field to each sampled changed record, then restage and check.",
        },
        "JSONL_POLICY_NULL" => Strategy {
            kind: "replace_null_value",
            risk: "medium",
            instruction: "Replace disallowed nulls in sampled changed records, then restage and check.",
        },
        "JSONL_POLICY_TYPE" => Strategy {
            kind: "correct_field_type",
            risk: "high",
            instruction: "Convert sampled changed records to an allowed field type without lossy coercion, then restage and check.",
        },
        "JSONL_POLICY_ENUM" => Strategy {
            kind: "select_allowed_value",
            risk: "high",
            instruction: "Choose an allowed value for each sampled changed record from the pinned policy, then restage and check.",
        },
        "JSONL_POLICY_RANGE" => Strategy {
            kind: "correct_numeric_range",
            risk: "high",
            instruction: "Correct out-of-range numeric values using domain evidence, then restage and check.",
        },
        "JSONL_POLICY_PATTERN" => Strategy {
            kind: "correct_string_format",
            risk: "medium",
            instruction: "Correct string formatting to match the pinned policy, then restage and check.",
        },
        "JSONL_POLICY_UNIQUE" => Strategy {
            kind: "resolve_duplicate_value",
            risk: "high",
            instruction: "Resolve duplicate scalar values without changing logical record IDs, then restage and check.",
        },
        "JSONL_INVALID_RECORD" => Strategy {
            kind: "repair_record_encoding",
            risk: "medium",
            instruction: "Repair or remove malformed, non-object, or non-UTF-8 JSONL records, then stage and run check.",
        },
        "JSONL_MISSING_ID" | "JSONL_NULL_ID" | "JSONL_INVALID_ID" => Strategy {
            kind: "repair_record_identity",
            risk: "high",
            instruction: "Assign a valid string or numeric ID in the configured ID field, then stage and run check.",
        },
        "JSONL_DUPLICATE_ID" => Strategy {
            kind: "deduplicate_record_identity",
            risk: "high",
            instruction: "Resolve duplicate logical record IDs without merging values implicitly, then stage and run check.",
        },
        "CROSS_SPLIT_EXACT_LEAKAGE" | "EXACT_DUPLICATE_GROUP" => Strategy {
            kind: "deduplicate",
            risk: "high",
            instruction: "Choose the canonical sample, remove or relocate duplicates, then run check.",
        },
        "CROSS_SPLIT_NEAR_LEAKAGE" | "NEAR_DUPLICATE_GROUP" | "AMBIGUOUS_MATCH" => Strategy {
            kind: "inspect_similarity",
            risk: "medium",
            instruction: "Inspect the related samples and keep, remove, or relabel them according to dataset intent.",
        },
        "IMAGE_DECODE_FAILED" => Strategy {
            kind: "repair_media",
            risk: "medium",
            instruction: "Replace the unreadable media from a trusted source or remove it, then run check.",
        },
        "INVALID_LAYOUT" => Strategy {
            kind: "normalize_layout",
            risk: "medium",
            instruction: "Move the sample into the expected split and label layout, then run check.",
        },
        "LABEL_DISTRIBUTION_CHANGED" | "MEDIA_DISTRIBUTION_CHANGED" => Strategy {
            kind: "inspect_distribution",
            risk: "low",
            instruction: "Inspect the affected distribution and accept or correct the underlying sample changes.",
        },
        "SAMPLE_ADDED" | "SAMPLE_REMOVED" => Strategy {
            kind: "review_membership",
            risk: "medium",
            instruction: "Confirm the membership change is intended; restore or remove the sample if it is not.",
        },
        "LABEL_CHANGED" | "SPLIT_CHANGED" | "SAMPLE_MOVED" => Strategy {
            kind: "review_placement",
            risk: "medium",
            instruction: "Confirm the new label or split is intended; otherwise restore the prior placement.",
        },
        "CONTENT_CHANGED" | "PROBABLE_REENCODE" => Strategy {
            kind: "review_content",
            risk: "medium",
            instruction: "Confirm the content replacement or re-encode is intended; otherwise restore the prior sample.",
        },
        _ => Strategy {
            kind: "inspect",
            risk: "medium",
            instruction: "Inspect the finding evidence, make the smallest intended correction, then run check.",
        },
    }
}

fn action_id(finding_id: &str, kind: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-remediation-action-v1\0");
    hasher.update(finding_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(kind.as_bytes());
    format!("act_{}", &hasher.finalize().to_hex()[..20])
}
