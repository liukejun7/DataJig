use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

pub const REVISION_SCHEMA_VERSION: u8 = 1;
pub const RECORD_REVISION_SCHEMA_VERSION: u8 = 2;
pub const MAX_REVISION_BYTES: usize = 1024 * 1024;
const MAX_REVISION_STRING_BYTES: usize = 4096;

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
        let mut value = Self {
            schema_version: RECORD_REVISION_SCHEMA_VERSION,
            parent,
            adapter: "jsonl".into(),
            inventory_id: None,
            state_id: Some(state_id),
            dataset_content_id,
            accepted_report_id,
            provenance,
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

    fn validate_fields(&self) -> Result<()> {
        if let Some(parent) = &self.parent {
            validate_content_id(parent, "rev", "revision parent")?;
        }
        validate_string(&self.adapter, "revision adapter")?;
        match self.schema_version {
            REVISION_SCHEMA_VERSION => {
                if self.adapter != "imagefolder" || self.state_id.is_some() {
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
                if self.adapter != "jsonl" || self.inventory_id.is_some() {
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
