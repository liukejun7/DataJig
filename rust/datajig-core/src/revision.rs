use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub const REVISION_SCHEMA_VERSION: u8 = 1;
pub const RECORD_REVISION_SCHEMA_VERSION: u8 = 2;
pub const TRANSFORM_LINEAGE_REVISION_SCHEMA_VERSION: u8 = 3;
pub const MAX_REVISION_BYTES: usize = 1024 * 1024;
const MAX_REVISION_STRING_BYTES: usize = 4096;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransformLineage {
    plan_id: String,
    receipt_id: String,
    provider: crate::TransformProviderIdentity,
    provider_id: String,
    source_aliases: Vec<String>,
    source_content_ids: Vec<String>,
    sql_content_id: String,
    parameter_content_id: String,
    output_content_id: String,
    receipt_path: String,
    verified: bool,
}

impl TransformLineage {
    pub fn from_verified_receipt(receipt: &crate::VerifiedTransformReceipt) -> Result<Self> {
        let artifact = receipt.receipt();
        let value = Self {
            plan_id: artifact.plan_id().into(),
            receipt_id: artifact.receipt_id().into(),
            provider: artifact.provider().clone(),
            provider_id: artifact.provider_id().into(),
            source_aliases: artifact.source_aliases().to_vec(),
            source_content_ids: artifact.source_content_ids().to_vec(),
            sql_content_id: artifact.sql_content_id().into(),
            parameter_content_id: artifact.parameter_content_id().into(),
            output_content_id: artifact.output_content_id().into(),
            receipt_path: receipt
                .receipt_path()
                .to_str()
                .context("transform receipt path is not valid UTF-8")?
                .into(),
            verified: true,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<()> {
        validate_content_id(&self.plan_id, "xform", "transform plan ID")?;
        validate_content_id(&self.receipt_id, "xformed", "transform receipt ID")?;
        validate_content_id(&self.provider_id, "provider", "transform provider ID")?;
        validate_content_id(&self.sql_content_id, "sql", "transform SQL ID")?;
        validate_content_id(
            &self.parameter_content_id,
            "params",
            "transform parameter ID",
        )?;
        validate_content_id(
            &self.output_content_id,
            "prepared",
            "transform output content ID",
        )?;
        if !self.verified
            || self.provider.provider_id() != self.provider_id
            || self.source_aliases.is_empty()
            || self.source_aliases.len() != self.source_content_ids.len()
            || self.source_aliases.iter().any(|value| value.is_empty())
            || self.source_content_ids.iter().any(|value| value.is_empty())
        {
            bail!("transform lineage is invalid");
        }
        validate_string(&self.receipt_path, "transform receipt path")
    }

    pub fn plan_id(&self) -> &str {
        &self.plan_id
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RevisionProvenance {
    actor_kind: String,
    task_id: Option<String>,
    tool_version: String,
    message: String,
}

impl RevisionProvenance {
    pub fn new(
        actor_kind: String,
        task_id: Option<String>,
        tool_version: String,
        message: String,
    ) -> Result<Self> {
        let value = Self {
            actor_kind,
            task_id,
            tool_version,
            message,
        };
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<()> {
        validate_string(&self.actor_kind, "revision actor_kind")?;
        if let Some(task_id) = &self.task_id {
            validate_string(task_id, "revision task_id")?;
        }
        validate_string(&self.tool_version, "revision tool_version")?;
        validate_string(&self.message, "revision message")
    }

    pub fn task_id(&self) -> Option<&str> {
        self.task_id.as_deref()
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DatasetRevision {
    schema_version: u8,
    parent: Option<String>,
    adapter: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    inventory_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    state_id: Option<String>,
    dataset_content_id: String,
    accepted_report_id: Option<String>,
    provenance: RevisionProvenance,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    transform_lineage: Option<TransformLineage>,
    created_at_unix_ns: String,
    revision_id: String,
}

#[derive(Serialize)]
struct RevisionIdentityV1<'a> {
    schema_version: u8,
    parent: &'a Option<String>,
    adapter: &'a str,
    inventory_id: &'a str,
    dataset_content_id: &'a str,
    accepted_report_id: &'a Option<String>,
    provenance: &'a RevisionProvenance,
    created_at_unix_ns: &'a str,
}

#[derive(Serialize)]
struct RevisionIdentityV2<'a> {
    schema_version: u8,
    parent: &'a Option<String>,
    adapter: &'a str,
    state_id: &'a str,
    dataset_content_id: &'a str,
    accepted_report_id: &'a Option<String>,
    provenance: &'a RevisionProvenance,
    created_at_unix_ns: &'a str,
}

#[derive(Serialize)]
struct RevisionIdentityV3<'a> {
    schema_version: u8,
    parent: &'a Option<String>,
    adapter: &'a str,
    state_id: &'a str,
    dataset_content_id: &'a str,
    accepted_report_id: &'a Option<String>,
    provenance: &'a RevisionProvenance,
    transform_lineage: &'a Option<TransformLineage>,
    created_at_unix_ns: &'a str,
}

impl DatasetRevision {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        parent: Option<String>,
        adapter: String,
        inventory_id: String,
        dataset_content_id: String,
        accepted_report_id: Option<String>,
        provenance: RevisionProvenance,
        created_at_unix_ns: String,
    ) -> Result<Self> {
        let mut value = Self {
            schema_version: REVISION_SCHEMA_VERSION,
            parent,
            adapter,
            inventory_id: Some(inventory_id),
            state_id: None,
            dataset_content_id,
            accepted_report_id,
            provenance,
            transform_lineage: None,
            created_at_unix_ns,
            revision_id: String::new(),
        };
        value.validate_fields()?;
        value.revision_id = value.compute_id()?;
        Ok(value)
    }

    pub fn new_record(
        parent: Option<String>,
        state_id: String,
        dataset_content_id: String,
        accepted_report_id: Option<String>,
        provenance: RevisionProvenance,
        created_at_unix_ns: String,
    ) -> Result<Self> {
        Self::new_record_with_lineage(
            parent,
            state_id,
            dataset_content_id,
            accepted_report_id,
            provenance,
            None,
            created_at_unix_ns,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_record_with_lineage(
        parent: Option<String>,
        state_id: String,
        dataset_content_id: String,
        accepted_report_id: Option<String>,
        provenance: RevisionProvenance,
        transform_lineage: Option<TransformLineage>,
        created_at_unix_ns: String,
    ) -> Result<Self> {
        let mut value = Self {
            schema_version: TRANSFORM_LINEAGE_REVISION_SCHEMA_VERSION,
            parent,
            adapter: "jsonl".into(),
            inventory_id: None,
            state_id: Some(state_id),
            dataset_content_id,
            accepted_report_id,
            provenance,
            transform_lineage,
            created_at_unix_ns,
            revision_id: String::new(),
        };
        value.validate_fields()?;
        value.revision_id = value.compute_id()?;
        Ok(value)
    }

    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_REVISION_BYTES {
            bail!("revision exceeds {MAX_REVISION_BYTES} bytes");
        }
        let value: Self = serde_json::from_str(payload).context("invalid revision JSON")?;
        value.validate_fields()?;
        if value.revision_id != value.compute_id()? {
            bail!("revision_id does not match revision content");
        }
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        let payload = serde_json::to_string_pretty(self)?;
        if payload.len() > MAX_REVISION_BYTES {
            bail!("revision exceeds {MAX_REVISION_BYTES} bytes");
        }
        Ok(payload)
    }

    pub fn revision_id(&self) -> &str {
        &self.revision_id
    }

    pub fn parent(&self) -> Option<&str> {
        self.parent.as_deref()
    }

    pub fn adapter(&self) -> &str {
        &self.adapter
    }

    pub fn inventory_id(&self) -> &str {
        self.inventory_id
            .as_deref()
            .expect("inventory_id is only available on imagefolder revisions")
    }

    pub fn state_id(&self) -> &str {
        self.state_id
            .as_deref()
            .or(self.inventory_id.as_deref())
            .expect(
                "validated revisions contain either a record state or an inventory state identity",
            )
    }

    pub fn accepted_report_id(&self) -> Option<&str> {
        self.accepted_report_id.as_deref()
    }

    pub fn dataset_content_id(&self) -> &str {
        &self.dataset_content_id
    }

    pub fn provenance(&self) -> &RevisionProvenance {
        &self.provenance
    }

    pub fn schema_version(&self) -> u8 {
        self.schema_version
    }

    pub fn transform_lineage(&self) -> Option<&TransformLineage> {
        self.transform_lineage.as_ref()
    }

    fn validate_fields(&self) -> Result<()> {
        if let Some(parent) = &self.parent {
            validate_content_id(parent, "rev", "revision parent")?;
        }
        validate_string(&self.adapter, "revision adapter")?;
        match self.schema_version {
            REVISION_SCHEMA_VERSION => {
                if self.adapter != "imagefolder"
                    || self.state_id.is_some()
                    || self.transform_lineage.is_some()
                {
                    bail!("schema-1 revision descriptor is invalid");
                }
                validate_content_id(
                    self.inventory_id
                        .as_deref()
                        .context("schema-1 revision is missing inventory_id")?,
                    "inventory",
                    "inventory_id",
                )?;
                validate_content_id(&self.dataset_content_id, "inventory", "dataset_content_id")?;
            }
            RECORD_REVISION_SCHEMA_VERSION => {
                if self.adapter != "jsonl"
                    || self.inventory_id.is_some()
                    || self.transform_lineage.is_some()
                {
                    bail!("schema-2 revision descriptor is invalid");
                }
                validate_content_id(
                    self.state_id
                        .as_deref()
                        .context("schema-2 revision is missing state_id")?,
                    "recordstate",
                    "state_id",
                )?;
                validate_content_id(&self.dataset_content_id, "records", "dataset_content_id")?;
            }
            TRANSFORM_LINEAGE_REVISION_SCHEMA_VERSION => {
                if self.adapter != "jsonl" || self.inventory_id.is_some() {
                    bail!("schema-3 revision descriptor is invalid");
                }
                validate_content_id(
                    self.state_id
                        .as_deref()
                        .context("schema-3 revision is missing state_id")?,
                    "recordstate",
                    "state_id",
                )?;
                validate_content_id(&self.dataset_content_id, "records", "dataset_content_id")?;
                if let Some(lineage) = &self.transform_lineage {
                    lineage.validate()?;
                }
            }
            _ => bail!("unsupported revision schema {}", self.schema_version),
        }
        if let Some(report) = &self.accepted_report_id {
            validate_content_id(report, "review", "accepted_report_id")?;
        }
        self.provenance.validate()?;
        validate_string(&self.created_at_unix_ns, "created_at_unix_ns")?;
        if !self
            .created_at_unix_ns
            .bytes()
            .all(|byte| byte.is_ascii_digit())
        {
            bail!("created_at_unix_ns must be a decimal string");
        }
        Ok(())
    }

    fn compute_id(&self) -> Result<String> {
        let (domain, payload) = match self.schema_version {
            REVISION_SCHEMA_VERSION => (
                b"datajig-revision-v1\0".as_slice(),
                serde_json::to_vec(&RevisionIdentityV1 {
                    schema_version: self.schema_version,
                    parent: &self.parent,
                    adapter: &self.adapter,
                    inventory_id: self.inventory_id.as_deref().expect("validated revision"),
                    dataset_content_id: &self.dataset_content_id,
                    accepted_report_id: &self.accepted_report_id,
                    provenance: &self.provenance,
                    created_at_unix_ns: &self.created_at_unix_ns,
                })?,
            ),
            RECORD_REVISION_SCHEMA_VERSION => (
                b"datajig-revision-v2\0".as_slice(),
                serde_json::to_vec(&RevisionIdentityV2 {
                    schema_version: self.schema_version,
                    parent: &self.parent,
                    adapter: &self.adapter,
                    state_id: self.state_id.as_deref().expect("validated revision"),
                    dataset_content_id: &self.dataset_content_id,
                    accepted_report_id: &self.accepted_report_id,
                    provenance: &self.provenance,
                    created_at_unix_ns: &self.created_at_unix_ns,
                })?,
            ),
            TRANSFORM_LINEAGE_REVISION_SCHEMA_VERSION => (
                b"datajig-revision-v3\0".as_slice(),
                serde_json::to_vec(&RevisionIdentityV3 {
                    schema_version: self.schema_version,
                    parent: &self.parent,
                    adapter: &self.adapter,
                    state_id: self.state_id.as_deref().expect("validated revision"),
                    dataset_content_id: &self.dataset_content_id,
                    accepted_report_id: &self.accepted_report_id,
                    provenance: &self.provenance,
                    transform_lineage: &self.transform_lineage,
                    created_at_unix_ns: &self.created_at_unix_ns,
                })?,
            ),
            _ => unreachable!("validated revision schema"),
        };
        Ok(crate::identity::blake3_content_id("rev", domain, &payload))
    }
}

pub(crate) fn validate_content_id(value: &str, prefix: &str, name: &str) -> Result<()> {
    let Some(digest) = value.strip_prefix(&format!("{prefix}_")) else {
        bail!("{name} has an invalid prefix");
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("{name} must contain a 64-character lowercase hexadecimal digest");
    }
    Ok(())
}

fn validate_string(value: &str, name: &str) -> Result<()> {
    if value.is_empty() || value.len() > MAX_REVISION_STRING_BYTES {
        bail!("{name} must contain 1 to {MAX_REVISION_STRING_BYTES} UTF-8 bytes");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_one_and_two_identities_remain_readable_and_unchanged() {
        let provenance = RevisionProvenance::new(
            "system".into(),
            None,
            "0.5.1".into(),
            "Compatibility fixture".into(),
        )
        .unwrap();
        let mut image = DatasetRevision {
            schema_version: REVISION_SCHEMA_VERSION,
            parent: None,
            adapter: "imagefolder".into(),
            inventory_id: Some(format!("inventory_{}", "1".repeat(64))),
            state_id: None,
            dataset_content_id: format!("inventory_{}", "2".repeat(64)),
            accepted_report_id: None,
            provenance: provenance.clone(),
            transform_lineage: None,
            created_at_unix_ns: "1".into(),
            revision_id: String::new(),
        };
        image.validate_fields().unwrap();
        image.revision_id = image.compute_id().unwrap();
        let image_id = image.revision_id.clone();
        assert_eq!(
            "rev_62e20be0c952b583338b58f921d692a0b171cf6d2e6c35b71d35c30744fe80ae",
            image_id
        );
        assert_eq!(
            image_id,
            DatasetRevision::from_json(&image.to_json().unwrap())
                .unwrap()
                .revision_id()
        );

        let mut records = DatasetRevision {
            schema_version: RECORD_REVISION_SCHEMA_VERSION,
            parent: None,
            adapter: "jsonl".into(),
            inventory_id: None,
            state_id: Some(format!("recordstate_{}", "3".repeat(64))),
            dataset_content_id: format!("records_{}", "4".repeat(64)),
            accepted_report_id: None,
            provenance,
            transform_lineage: None,
            created_at_unix_ns: "2".into(),
            revision_id: String::new(),
        };
        records.validate_fields().unwrap();
        records.revision_id = records.compute_id().unwrap();
        let record_id = records.revision_id.clone();
        assert_eq!(
            "rev_c7f161e84c0307da7969bb9e3bc1f6145dae6f903fa135f8c5022cede5d7ed9f",
            record_id
        );
        assert_eq!(
            record_id,
            DatasetRevision::from_json(&records.to_json().unwrap())
                .unwrap()
                .revision_id()
        );
    }

    #[test]
    fn schema_three_identity_binds_optional_transform_lineage() {
        let provider = crate::TransformProviderIdentity::create(
            "0.6.0".into(),
            "1.5.6".into(),
            "CPython".into(),
            "3.12.14".into(),
        )
        .unwrap();
        let lineage = TransformLineage {
            plan_id: format!("xform_{}", "1".repeat(64)),
            receipt_id: format!("xformed_{}", "2".repeat(64)),
            provider_id: provider.provider_id().into(),
            provider,
            source_aliases: vec!["events".into()],
            source_content_ids: vec![format!("source_{}", "3".repeat(64))],
            sql_content_id: format!("sql_{}", "4".repeat(64)),
            parameter_content_id: format!("params_{}", "5".repeat(64)),
            output_content_id: format!("prepared_{}", "6".repeat(64)),
            receipt_path: "/data/prepared.jsonl.datajig.transform.json".into(),
            verified: true,
        };
        let arguments = (
            None,
            format!("recordstate_{}", "7".repeat(64)),
            format!("records_{}", "8".repeat(64)),
            None,
            RevisionProvenance::new("system".into(), None, "0.6.0".into(), "Initialize".into())
                .unwrap(),
            String::from("3"),
        );
        let without = DatasetRevision::new_record_with_lineage(
            arguments.0.clone(),
            arguments.1.clone(),
            arguments.2.clone(),
            arguments.3.clone(),
            arguments.4.clone(),
            None,
            arguments.5.clone(),
        )
        .unwrap();
        let with = DatasetRevision::new_record_with_lineage(
            arguments.0,
            arguments.1,
            arguments.2,
            arguments.3,
            arguments.4,
            Some(lineage.clone()),
            arguments.5,
        )
        .unwrap();
        assert_ne!(without.revision_id(), with.revision_id());
        assert_eq!(Some(&lineage), with.transform_lineage());
    }
}
