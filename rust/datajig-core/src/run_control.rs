use crate::CompiledRunTask;
use crate::identity::blake3_content_id;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;

pub const RUN_PLAN_SCHEMA_VERSION: u8 = 1;
const MAX_RUN_IDENTITY_VALUE_BYTES: usize = 4096;
const MAX_ATTEMPT_NONCE_BYTES: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunConsumptionBinding {
    pub consumer: String,
    pub run_id: String,
    pub run_dir: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunPlanBinding {
    pub source_content_id: String,
    pub engine_version: String,
    pub output: String,
    pub consumption: Option<RunConsumptionBinding>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunIdentities {
    pub intent_id: String,
    pub plan_id: String,
    pub attempt_id: String,
    pub consumption_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationLevel {
    Safe,
    Dangerous,
    Fatal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorizationStatus {
    Authorized,
    ConfirmationRequired,
    Rejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct RunAuthorizationDecision {
    pub level: AuthorizationLevel,
    pub status: AuthorizationStatus,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RunControlError {
    pub code: String,
    pub message: String,
    pub remediation: String,
}

impl RunControlError {
    fn invalid(field: &str, message: impl Into<String>) -> Self {
        Self {
            code: "INVALID_RUN_PLAN".into(),
            message: format!("{field}: {}", message.into()),
            remediation: format!("provide a valid bounded {field}"),
        }
    }
}

impl fmt::Display for RunControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for RunControlError {}

#[derive(Serialize)]
struct ConsumptionIdentity<'a> {
    schema_version: u8,
    consumer: &'a str,
    run_id: &'a str,
    run_dir: &'a str,
}

#[derive(Serialize)]
struct PlanIdentity<'a> {
    schema_version: u8,
    intent_id: &'a str,
    source_content_id: &'a str,
    engine_version: &'a str,
    output: &'a str,
    consumption_id: Option<&'a str>,
}

#[derive(Serialize)]
struct AttemptIdentity<'a> {
    schema_version: u8,
    plan_id: &'a str,
    nonce: &'a str,
}

pub fn derive_run_identities(
    compiled: &CompiledRunTask,
    binding: &RunPlanBinding,
    attempt_nonce: &str,
) -> Result<RunIdentities, RunControlError> {
    validate_value("source_content_id", &binding.source_content_id)?;
    validate_value("engine_version", &binding.engine_version)?;
    validate_value("output", &binding.output)?;
    validate_attempt_nonce(attempt_nonce)?;

    let consumption_id = binding
        .consumption
        .as_ref()
        .map(derive_consumption_id)
        .transpose()?;
    let plan_payload = serde_json::to_vec(&PlanIdentity {
        schema_version: RUN_PLAN_SCHEMA_VERSION,
        intent_id: &compiled.intent_id,
        source_content_id: &binding.source_content_id,
        engine_version: &binding.engine_version,
        output: &binding.output,
        consumption_id: consumption_id.as_deref(),
    })
    .map_err(|error| RunControlError::invalid("plan", error.to_string()))?;
    let plan_id = blake3_content_id("plan", b"datajig-run-plan-v1\0", &plan_payload);
    let attempt_payload = serde_json::to_vec(&AttemptIdentity {
        schema_version: RUN_PLAN_SCHEMA_VERSION,
        plan_id: &plan_id,
        nonce: attempt_nonce,
    })
    .map_err(|error| RunControlError::invalid("attempt", error.to_string()))?;
    let attempt_id = blake3_content_id("attempt", b"datajig-run-attempt-v1\0", &attempt_payload);
    Ok(RunIdentities {
        intent_id: compiled.intent_id.clone(),
        plan_id,
        attempt_id,
        consumption_id,
    })
}

pub fn authorize_run(level: AuthorizationLevel, yes: bool) -> RunAuthorizationDecision {
    let status = match (level, yes) {
        (AuthorizationLevel::Safe, _) | (AuthorizationLevel::Dangerous, true) => {
            AuthorizationStatus::Authorized
        }
        (AuthorizationLevel::Dangerous, false) => AuthorizationStatus::ConfirmationRequired,
        (AuthorizationLevel::Fatal, _) => AuthorizationStatus::Rejected,
    };
    RunAuthorizationDecision { level, status }
}

fn derive_consumption_id(consumption: &RunConsumptionBinding) -> Result<String, RunControlError> {
    validate_value("consumer", &consumption.consumer)?;
    if !matches!(
        consumption.consumer.as_str(),
        "python" | "pytorch" | "huggingface"
    ) {
        return Err(RunControlError::invalid(
            "consumer",
            "must be python, pytorch, or huggingface",
        ));
    }
    validate_value("run_id", &consumption.run_id)?;
    validate_value("run_dir", &consumption.run_dir)?;
    let payload = serde_json::to_vec(&ConsumptionIdentity {
        schema_version: RUN_PLAN_SCHEMA_VERSION,
        consumer: &consumption.consumer,
        run_id: &consumption.run_id,
        run_dir: &consumption.run_dir,
    })
    .map_err(|error| RunControlError::invalid("consumption", error.to_string()))?;
    Ok(blake3_content_id(
        "consume",
        b"datajig-run-consumption-v1\0",
        &payload,
    ))
}

fn validate_value(field: &str, value: &str) -> Result<(), RunControlError> {
    if value.is_empty() {
        return Err(RunControlError::invalid(field, "must not be empty"));
    }
    if value.len() > MAX_RUN_IDENTITY_VALUE_BYTES {
        return Err(RunControlError::invalid(
            field,
            format!(
                "is {} bytes > limit {MAX_RUN_IDENTITY_VALUE_BYTES}",
                value.len()
            ),
        ));
    }
    Ok(())
}

fn validate_attempt_nonce(value: &str) -> Result<(), RunControlError> {
    if value.is_empty() || value.len() > MAX_ATTEMPT_NONCE_BYTES {
        return Err(RunControlError::invalid(
            "attempt_nonce",
            format!("must contain 1..={MAX_ATTEMPT_NONCE_BYTES} bytes"),
        ));
    }
    Ok(())
}
