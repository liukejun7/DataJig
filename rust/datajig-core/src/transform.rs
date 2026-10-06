use crate::InvalidArgumentError;
use crate::identity::{ARTIFACT_NAMESPACE, blake3_content_id};
use crate::strict_json::reject_duplicate_json_members;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::Path;

pub const TRANSFORM_PLAN_SCHEMA_VERSION: u8 = 1;
pub const TRANSFORM_RECEIPT_SCHEMA_VERSION: u8 = 1;
pub const TRANSFORM_PROVIDER_PROTOCOL_VERSION: u8 = 1;
pub const MAX_TRANSFORM_INPUTS: usize = 16;
pub const MAX_TRANSFORM_SOURCE_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_TRANSFORM_SOURCE_ROWS: u64 = 2_000_000;
pub const MAX_TRANSFORM_SQL_BYTES: usize = 65_536;
pub const MAX_TRANSFORM_PARAMETERS: usize = 256;
pub const MAX_TRANSFORM_PARAMETER_BYTES: usize = 65_536;
pub const MAX_TRANSFORM_OUTPUT_ROWS: u64 = 2_000_000;
pub const MAX_TRANSFORM_OUTPUT_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_TRANSFORM_OUTPUT_FIELDS: usize = 256;
pub const MAX_TRANSFORM_PLAN_BYTES: usize = 1024 * 1024;
pub const MAX_TRANSFORM_RECEIPT_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransformLimits {
    pub inputs: usize,
    pub source_bytes: u64,
    pub source_rows: u64,
    pub sql_bytes: usize,
    pub parameters: usize,
    pub parameter_bytes: usize,
    pub output_rows: u64,
    pub output_bytes: u64,
    pub output_fields: usize,
    pub fetch_batch_rows: usize,
    pub duckdb_memory_bytes: u64,
    pub provider_stdout_bytes: usize,
    pub provider_stderr_bytes: usize,
    pub wall_time_seconds: u64,
}

impl TransformLimits {
    pub fn v1() -> Self {
        Self {
            inputs: MAX_TRANSFORM_INPUTS,
            source_bytes: MAX_TRANSFORM_SOURCE_BYTES,
            source_rows: MAX_TRANSFORM_SOURCE_ROWS,
            sql_bytes: MAX_TRANSFORM_SQL_BYTES,
            parameters: MAX_TRANSFORM_PARAMETERS,
            parameter_bytes: MAX_TRANSFORM_PARAMETER_BYTES,
            output_rows: MAX_TRANSFORM_OUTPUT_ROWS,
            output_bytes: MAX_TRANSFORM_OUTPUT_BYTES,
            output_fields: MAX_TRANSFORM_OUTPUT_FIELDS,
            fetch_batch_rows: 65_536,
            duckdb_memory_bytes: 512 * 1024 * 1024,
            provider_stdout_bytes: 1024 * 1024,
            provider_stderr_bytes: 1024 * 1024,
            wall_time_seconds: 15 * 60,
        }
    }

    fn validate(&self) -> Result<()> {
        if self.inputs == 0
            || self.inputs > MAX_TRANSFORM_INPUTS
            || self.source_bytes == 0
            || self.source_bytes > MAX_TRANSFORM_SOURCE_BYTES
            || self.source_rows == 0
            || self.source_rows > MAX_TRANSFORM_SOURCE_ROWS
            || self.sql_bytes == 0
            || self.sql_bytes > MAX_TRANSFORM_SQL_BYTES
            || self.parameters > MAX_TRANSFORM_PARAMETERS
            || self.parameter_bytes == 0
            || self.parameter_bytes > MAX_TRANSFORM_PARAMETER_BYTES
            || self.output_rows == 0
            || self.output_rows > MAX_TRANSFORM_OUTPUT_ROWS
            || self.output_bytes == 0
            || self.output_bytes > MAX_TRANSFORM_OUTPUT_BYTES
            || self.output_fields == 0
            || self.output_fields > MAX_TRANSFORM_OUTPUT_FIELDS
            || self.fetch_batch_rows == 0
            || self.fetch_batch_rows > 65_536
            || self.duckdb_memory_bytes == 0
            || self.duckdb_memory_bytes > 512 * 1024 * 1024
            || self.provider_stdout_bytes == 0
            || self.provider_stdout_bytes > 1024 * 1024
            || self.provider_stderr_bytes == 0
            || self.provider_stderr_bytes > 1024 * 1024
            || self.wall_time_seconds == 0
            || self.wall_time_seconds > 15 * 60
        {
            return Err(InvalidArgumentError::new("transform limits exceed protocol v1").into());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransformSourceFormat {
    Csv,
    Parquet,
    Jsonl,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransformSource {
    pub alias: String,
    pub path: String,
    pub format: TransformSourceFormat,
    pub bytes: u64,
    pub rows: u64,
    pub content_id: String,
}

impl TransformSource {
    pub fn create(
        alias: String,
        path: String,
        format: TransformSourceFormat,
        bytes: u64,
        rows: u64,
        content_id: String,
    ) -> Result<Self> {
        let value = Self {
            alias,
            path,
            format,
            bytes,
            rows,
            content_id,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<()> {
        if !valid_alias(&self.alias) {
            return Err(InvalidArgumentError::new(
                "transform input alias must match [a-z][a-z0-9_]{0,63}",
            )
            .into());
        }
        if self.path.is_empty() || self.content_id.is_empty() {
            return Err(InvalidArgumentError::new(
                "transform source path and content ID are required",
            )
            .into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransformField {
    pub name: String,
    pub value_type: String,
    pub nullable: bool,
}

impl TransformField {
    pub fn new(name: String, value_type: String, nullable: bool) -> Result<Self> {
        if name.is_empty() || name.len() > 1024 || value_type.is_empty() {
            return Err(InvalidArgumentError::new("transform field is invalid").into());
        }
        Ok(Self {
            name,
            value_type,
            nullable,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransformProviderIdentity {
    protocol: String,
    protocol_version: u8,
    implementation: String,
    implementation_version: String,
    duckdb_version: String,
    python_implementation: String,
    python_version: String,
    serializer_version: u8,
    source_loader_policy_version: u8,
    sql_policy_version: u8,
    provider_id: String,
}

#[derive(Serialize)]
struct TransformProviderIdentityPayload<'a> {
    protocol: &'a str,
    protocol_version: u8,
    implementation: &'a str,
    implementation_version: &'a str,
    duckdb_version: &'a str,
    python_implementation: &'a str,
    python_version: &'a str,
    serializer_version: u8,
    source_loader_policy_version: u8,
    sql_policy_version: u8,
}

impl TransformProviderIdentity {
    pub fn create(
        implementation_version: String,
        duckdb_version: String,
        python_implementation: String,
        python_version: String,
    ) -> Result<Self> {
        if implementation_version.is_empty()
            || duckdb_version.is_empty()
            || python_implementation.is_empty()
            || python_version.is_empty()
        {
            return Err(
                InvalidArgumentError::new("transform provider identity is incomplete").into(),
            );
        }
        let mut value = Self {
            protocol: "datajig.transform-provider.v1".into(),
            protocol_version: TRANSFORM_PROVIDER_PROTOCOL_VERSION,
            implementation: "datajig-duckdb-python".into(),
            implementation_version,
            duckdb_version,
            python_implementation,
            python_version,
            serializer_version: 1,
            source_loader_policy_version: 1,
            sql_policy_version: 1,
            provider_id: String::new(),
        };
        value.provider_id = value.compute_id()?;
        Ok(value)
    }

    fn compute_id(&self) -> Result<String> {
        let payload = TransformProviderIdentityPayload {
            protocol: &self.protocol,
            protocol_version: self.protocol_version,
            implementation: &self.implementation,
            implementation_version: &self.implementation_version,
            duckdb_version: &self.duckdb_version,
            python_implementation: &self.python_implementation,
            python_version: &self.python_version,
            serializer_version: self.serializer_version,
            source_loader_policy_version: self.source_loader_policy_version,
            sql_policy_version: self.sql_policy_version,
        };
        Ok(blake3_content_id(
            "provider",
            b"datajig-transform-provider-v1\0",
            &serde_json::to_vec(&payload)?,
        ))
    }

    fn validate(&self) -> Result<()> {
        if self.protocol != "datajig.transform-provider.v1"
            || self.protocol_version != TRANSFORM_PROVIDER_PROTOCOL_VERSION
            || self.implementation != "datajig-duckdb-python"
            || self.serializer_version != 1
            || self.source_loader_policy_version != 1
            || self.sql_policy_version != 1
            || self.provider_id != self.compute_id()?
        {
            return Err(InvalidArgumentError::new("transform provider identity is invalid").into());
        }
        Ok(())
    }

    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransformExpectedOutput {
    pub schema: Vec<TransformField>,
    pub rows: u64,
    pub bytes: u64,
    pub unique_ids: u64,
    pub output_content_id: String,
}

#[derive(Clone, Debug)]
pub struct TransformPlanInput {
    pub sources: Vec<TransformSource>,
    pub sql_path: String,
    pub sql: String,
    pub parameters: Vec<Value>,
    pub id_field: String,
    pub output_path: String,
    pub provider: TransformProviderIdentity,
    pub limits: TransformLimits,
    pub expected: TransformExpectedOutput,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransformPlan {
    namespace: String,
    kind: String,
    schema_version: u8,
    plan_id: String,
    sources: Vec<TransformSource>,
    sql_path: String,
    sql: String,
    sql_content_id: String,
    parameters: Vec<Value>,
    parameter_content_id: String,
    id_field: String,
    output_path: String,
    provider: TransformProviderIdentity,
    provider_id: String,
    limits: TransformLimits,
    expected: TransformExpectedOutput,
}

#[derive(Serialize)]
struct TransformPlanIdentity<'a> {
    schema_version: u8,
    sources: &'a [TransformSource],
    sql_path: &'a str,
    sql: &'a str,
    sql_content_id: &'a str,
    parameters: &'a [Value],
    parameter_content_id: &'a str,
    id_field: &'a str,
    output_path: &'a str,
    provider: &'a TransformProviderIdentity,
    provider_id: &'a str,
    limits: &'a TransformLimits,
    expected: &'a TransformExpectedOutput,
}

impl TransformPlan {
    pub fn create(mut input: TransformPlanInput) -> Result<Self> {
        input
            .sources
            .sort_by(|left, right| left.alias.cmp(&right.alias));
        let sql_content_id =
            blake3_content_id("sql", b"datajig-transform-sql-v1\0", input.sql.as_bytes());
        let parameter_payload = serde_json::to_vec(&input.parameters)?;
        let parameter_content_id = blake3_content_id(
            "params",
            b"datajig-transform-parameters-v1\0",
            &parameter_payload,
        );
        let mut value = Self {
            namespace: ARTIFACT_NAMESPACE.into(),
            kind: "transform_plan".into(),
            schema_version: TRANSFORM_PLAN_SCHEMA_VERSION,
            plan_id: String::new(),
            sources: input.sources,
            sql_path: input.sql_path,
            sql: input.sql,
            sql_content_id,
            parameters: input.parameters,
            parameter_content_id,
            id_field: input.id_field,
            output_path: input.output_path,
            provider_id: input.provider.provider_id().into(),
            provider: input.provider,
            limits: input.limits,
            expected: input.expected,
        };
        value.validate_fields()?;
        value.plan_id = value.compute_id()?;
        Ok(value)
    }

    pub fn from_path(path: &Path) -> Result<Self> {
        let payload = fs::read(path).with_context(|| "cannot read transform plan")?;
        if payload.len() > MAX_TRANSFORM_PLAN_BYTES {
            return Err(InvalidArgumentError::new("transform plan exceeds 1048576 bytes").into());
        }
        let text = std::str::from_utf8(&payload)
            .map_err(|_| InvalidArgumentError::new("transform plan is not valid UTF-8"))?;
        reject_duplicate_json_members(text)
            .map_err(|_| InvalidArgumentError::new("transform plan is invalid"))?;
        let value: Self = serde_json::from_str(text)
            .map_err(|_| InvalidArgumentError::new("transform plan is invalid"))?;
        value.validate_fields()?;
        if value.plan_id != value.compute_id()? {
            return Err(InvalidArgumentError::new("transform plan identity is invalid").into());
        }
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    fn validate_fields(&self) -> Result<()> {
        if self.namespace != ARTIFACT_NAMESPACE
            || self.kind != "transform_plan"
            || self.schema_version != TRANSFORM_PLAN_SCHEMA_VERSION
        {
            return Err(InvalidArgumentError::new("unsupported transform plan schema").into());
        }
        self.limits.validate()?;
        self.provider.validate()?;
        if self.provider_id != self.provider.provider_id {
            return Err(InvalidArgumentError::new("transform provider binding is invalid").into());
        }
        if self.sources.is_empty() || self.sources.len() > self.limits.inputs {
            return Err(InvalidArgumentError::new("transform input count is invalid").into());
        }
        let mut previous = None;
        let mut source_bytes = 0u64;
        let mut source_rows = 0u64;
        for source in &self.sources {
            source.validate()?;
            if previous.is_some_and(|alias: &str| alias >= source.alias.as_str()) {
                return Err(InvalidArgumentError::new("transform aliases must be unique").into());
            }
            previous = Some(source.alias.as_str());
            source_bytes = source_bytes
                .checked_add(source.bytes)
                .ok_or_else(|| InvalidArgumentError::new("transform source bytes overflow"))?;
            source_rows = source_rows
                .checked_add(source.rows)
                .ok_or_else(|| InvalidArgumentError::new("transform source rows overflow"))?;
        }
        if source_bytes > self.limits.source_bytes || source_rows > self.limits.source_rows {
            return Err(InvalidArgumentError::new("transform source limit exceeded").into());
        }
        if self.sql.is_empty() || self.sql.len() > self.limits.sql_bytes {
            return Err(InvalidArgumentError::new("transform SQL size is invalid").into());
        }
        let expected_sql_id =
            blake3_content_id("sql", b"datajig-transform-sql-v1\0", self.sql.as_bytes());
        let parameter_payload = serde_json::to_vec(&self.parameters)?;
        if self.parameters.len() > self.limits.parameters
            || parameter_payload.len() > self.limits.parameter_bytes
        {
            return Err(InvalidArgumentError::new("transform parameters exceed limits").into());
        }
        for value in &self.parameters {
            if matches!(value, Value::Array(_) | Value::Object(_)) {
                return Err(
                    InvalidArgumentError::new("transform parameters must be scalars").into(),
                );
            }
        }
        let expected_parameter_id = blake3_content_id(
            "params",
            b"datajig-transform-parameters-v1\0",
            &parameter_payload,
        );
        if self.sql_content_id != expected_sql_id
            || self.parameter_content_id != expected_parameter_id
        {
            return Err(InvalidArgumentError::new("transform query identity is invalid").into());
        }
        if self.sql_path.is_empty()
            || self.id_field.is_empty()
            || self.output_path.is_empty()
            || self.expected.schema.is_empty()
            || self.expected.schema.len() > self.limits.output_fields
            || self.expected.rows > self.limits.output_rows
            || self.expected.bytes > self.limits.output_bytes
            || self.expected.unique_ids != self.expected.rows
            || self.expected.output_content_id.is_empty()
        {
            return Err(InvalidArgumentError::new("transform expected output is invalid").into());
        }
        Ok(())
    }

    fn identity(&self) -> TransformPlanIdentity<'_> {
        TransformPlanIdentity {
            schema_version: self.schema_version,
            sources: &self.sources,
            sql_path: &self.sql_path,
            sql: &self.sql,
            sql_content_id: &self.sql_content_id,
            parameters: &self.parameters,
            parameter_content_id: &self.parameter_content_id,
            id_field: &self.id_field,
            output_path: &self.output_path,
            provider: &self.provider,
            provider_id: &self.provider_id,
            limits: &self.limits,
            expected: &self.expected,
        }
    }

    fn compute_id(&self) -> Result<String> {
        Ok(blake3_content_id(
            "xform",
            b"datajig-transform-plan-v1\0",
            &serde_json::to_vec(&self.identity())?,
        ))
    }

    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }

    pub fn sql_content_id(&self) -> &str {
        &self.sql_content_id
    }

    pub fn parameter_content_id(&self) -> &str {
        &self.parameter_content_id
    }

    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    pub fn expected(&self) -> &TransformExpectedOutput {
        &self.expected
    }

    pub fn sources(&self) -> &[TransformSource] {
        &self.sources
    }

    pub fn sql_path(&self) -> &str {
        &self.sql_path
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn parameters(&self) -> &[Value] {
        &self.parameters
    }

    pub fn id_field(&self) -> &str {
        &self.id_field
    }

    pub fn output_path(&self) -> &str {
        &self.output_path
    }

    pub fn provider(&self) -> &TransformProviderIdentity {
        &self.provider
    }

    pub fn limits(&self) -> &TransformLimits {
        &self.limits
    }
}

#[derive(Clone, Debug)]
pub struct TransformExecutionEvidence {
    pub output_path: String,
    pub output_content_id: String,
    pub schema: Vec<TransformField>,
    pub rows: u64,
    pub bytes: u64,
    pub unique_ids: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransformReceipt {
    namespace: String,
    kind: String,
    schema_version: u8,
    receipt_id: String,
    plan_id: String,
    source_aliases: Vec<String>,
    source_content_ids: Vec<String>,
    sql_content_id: String,
    parameter_content_id: String,
    provider: TransformProviderIdentity,
    provider_id: String,
    id_field: String,
    output_path: String,
    output_content_id: String,
    schema: Vec<TransformField>,
    rows: u64,
    bytes: u64,
    unique_ids: u64,
}

#[derive(Serialize)]
struct TransformReceiptIdentity<'a> {
    schema_version: u8,
    plan_id: &'a str,
    source_aliases: &'a [String],
    source_content_ids: &'a [String],
    sql_content_id: &'a str,
    parameter_content_id: &'a str,
    provider: &'a TransformProviderIdentity,
    provider_id: &'a str,
    id_field: &'a str,
    output_path: &'a str,
    output_content_id: &'a str,
    schema: &'a [TransformField],
    rows: u64,
    bytes: u64,
    unique_ids: u64,
}

impl TransformReceipt {
    pub fn create(plan: &TransformPlan, evidence: TransformExecutionEvidence) -> Result<Self> {
        if evidence.output_path != plan.output_path
            || evidence.output_content_id != plan.expected.output_content_id
            || evidence.schema != plan.expected.schema
            || evidence.rows != plan.expected.rows
            || evidence.bytes != plan.expected.bytes
            || evidence.unique_ids != plan.expected.unique_ids
        {
            return Err(InvalidArgumentError::new(
                "transform execution evidence does not match the accepted plan",
            )
            .into());
        }
        let mut value = Self {
            namespace: ARTIFACT_NAMESPACE.into(),
            kind: "transform_receipt".into(),
            schema_version: TRANSFORM_RECEIPT_SCHEMA_VERSION,
            receipt_id: String::new(),
            plan_id: plan.plan_id.clone(),
            source_aliases: plan
                .sources
                .iter()
                .map(|source| source.alias.clone())
                .collect(),
            source_content_ids: plan
                .sources
                .iter()
                .map(|source| source.content_id.clone())
                .collect(),
            sql_content_id: plan.sql_content_id.clone(),
            parameter_content_id: plan.parameter_content_id.clone(),
            provider: plan.provider.clone(),
            provider_id: plan.provider_id.clone(),
            id_field: plan.id_field.clone(),
            output_path: evidence.output_path,
            output_content_id: evidence.output_content_id,
            schema: evidence.schema,
            rows: evidence.rows,
            bytes: evidence.bytes,
            unique_ids: evidence.unique_ids,
        };
        value.receipt_id = value.compute_id()?;
        value.validate()?;
        Ok(value)
    }

    pub fn from_path(path: &Path) -> Result<Self> {
        let payload = fs::read(path).with_context(|| "cannot read transform receipt")?;
        if payload.len() > MAX_TRANSFORM_RECEIPT_BYTES {
            return Err(
                InvalidArgumentError::new("transform receipt exceeds 1048576 bytes").into(),
            );
        }
        let text = std::str::from_utf8(&payload)
            .map_err(|_| InvalidArgumentError::new("transform receipt is not valid UTF-8"))?;
        reject_duplicate_json_members(text)
            .map_err(|_| InvalidArgumentError::new("transform receipt is invalid"))?;
        let value: Self = serde_json::from_str(text)
            .map_err(|_| InvalidArgumentError::new("transform receipt is invalid"))?;
        value.validate()?;
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    fn identity(&self) -> TransformReceiptIdentity<'_> {
        TransformReceiptIdentity {
            schema_version: self.schema_version,
            plan_id: &self.plan_id,
            source_aliases: &self.source_aliases,
            source_content_ids: &self.source_content_ids,
            sql_content_id: &self.sql_content_id,
            parameter_content_id: &self.parameter_content_id,
            provider: &self.provider,
            provider_id: &self.provider_id,
            id_field: &self.id_field,
            output_path: &self.output_path,
            output_content_id: &self.output_content_id,
            schema: &self.schema,
            rows: self.rows,
            bytes: self.bytes,
            unique_ids: self.unique_ids,
        }
    }

    fn compute_id(&self) -> Result<String> {
        Ok(blake3_content_id(
            "xformed",
            b"datajig-transform-receipt-v1\0",
            &serde_json::to_vec(&self.identity())?,
        ))
    }

    fn validate(&self) -> Result<()> {
        if self.namespace != ARTIFACT_NAMESPACE
            || self.kind != "transform_receipt"
            || self.schema_version != TRANSFORM_RECEIPT_SCHEMA_VERSION
            || self.source_content_ids.is_empty()
            || self.source_aliases.len() != self.source_content_ids.len()
            || self.plan_id.is_empty()
            || self.id_field.is_empty()
            || self.output_path.is_empty()
            || self.output_content_id.is_empty()
            || self.unique_ids != self.rows
            || self.receipt_id != self.compute_id()?
        {
            return Err(InvalidArgumentError::new("transform receipt identity is invalid").into());
        }
        self.provider.validate()?;
        if self.provider.provider_id() != self.provider_id {
            return Err(InvalidArgumentError::new("transform receipt provider is invalid").into());
        }
        crate::revision::validate_content_id(&self.receipt_id, "xformed", "receipt_id")?;
        crate::revision::validate_content_id(&self.plan_id, "xform", "plan_id")?;
        crate::revision::validate_content_id(&self.sql_content_id, "sql", "sql_content_id")?;
        crate::revision::validate_content_id(
            &self.parameter_content_id,
            "params",
            "parameter_content_id",
        )?;
        crate::revision::validate_content_id(&self.provider_id, "provider", "provider_id")?;
        crate::revision::validate_content_id(
            &self.output_content_id,
            "prepared",
            "output_content_id",
        )?;
        if self.schema.is_empty()
            || self.schema.len() > MAX_TRANSFORM_OUTPUT_FIELDS
            || self.rows > MAX_TRANSFORM_OUTPUT_ROWS
            || self.bytes > MAX_TRANSFORM_OUTPUT_BYTES
            || !self.schema.iter().any(|field| field.name == self.id_field)
            || self
                .source_aliases
                .iter()
                .zip(self.source_aliases.iter().skip(1))
                .any(|(left, right)| left >= right)
            || self.source_aliases.iter().any(|alias| !valid_alias(alias))
            || self.source_content_ids.iter().any(String::is_empty)
        {
            return Err(InvalidArgumentError::new("transform receipt evidence is invalid").into());
        }
        Ok(())
    }

    pub fn receipt_id(&self) -> &str {
        &self.receipt_id
    }

    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }

    pub fn output_content_id(&self) -> &str {
        &self.output_content_id
    }

    pub fn output_path(&self) -> &str {
        &self.output_path
    }

    pub fn id_field(&self) -> &str {
        &self.id_field
    }

    pub fn schema(&self) -> &[TransformField] {
        &self.schema
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn unique_ids(&self) -> u64 {
        self.unique_ids
    }

    pub fn source_content_ids(&self) -> &[String] {
        &self.source_content_ids
    }

    pub fn source_aliases(&self) -> &[String] {
        &self.source_aliases
    }

    pub fn sql_content_id(&self) -> &str {
        &self.sql_content_id
    }

    pub fn parameter_content_id(&self) -> &str {
        &self.parameter_content_id
    }

    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    pub fn provider(&self) -> &TransformProviderIdentity {
        &self.provider
    }
}

fn valid_alias(value: &str) -> bool {
    let mut chars = value.chars();
    matches!(chars.next(), Some(first) if first.is_ascii_lowercase())
        && value.len() <= 64
        && chars.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        })
}
