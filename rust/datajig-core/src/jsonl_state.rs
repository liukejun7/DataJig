use crate::jsonl_inspect::{
    JsonlInspection, RecordFact, diff_record_maps, inspect_jsonl_with_limits,
    inspect_jsonl_with_record_limit, validated_record_facts,
};
use crate::{InvalidArgumentError, JsonlRecordDiff, MAX_JSONL_DIFF_RECORDS};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

pub const JSONL_RECORD_STATE_SCHEMA_VERSION: u8 = 1;
pub const JSONL_RECORD_PAGE_SCHEMA_VERSION: u8 = 1;
pub const MAX_JSONL_RECORD_PAGE_FACTS: usize = 50_000;
pub const MAX_JSONL_RECORD_PAGES: usize = 5;
pub const MAX_JSONL_RECORD_PAGE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_JSONL_RECORD_STATE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlRecordPageFact {
    record_id: String,
    content_hash: String,
    line: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct JsonlRecordLocation {
    pub line: usize,
    pub record_id: String,
    pub record_content_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct JsonlStructuralLocation {
    pub line: usize,
    pub issue_code: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlRecordPage {
    namespace: String,
    schema_version: u8,
    adapter: String,
    page_index: usize,
    facts: Vec<JsonlRecordPageFact>,
    page_id: String,
}

impl JsonlRecordPage {
    fn new(page_index: usize, facts: Vec<JsonlRecordPageFact>) -> Result<Self> {
        let mut value = Self {
            namespace: crate::identity::ARTIFACT_NAMESPACE.into(),
            schema_version: JSONL_RECORD_PAGE_SCHEMA_VERSION,
            adapter: "jsonl".into(),
            page_index,
            facts,
            page_id: String::new(),
        };
        value.validate_fields()?;
        value.page_id = value.compute_id()?;
        value.ensure_size()?;
        Ok(value)
    }

    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_JSONL_RECORD_PAGE_BYTES {
            bail!("JSONL record page exceeds {MAX_JSONL_RECORD_PAGE_BYTES} bytes");
        }
        let value: Self = serde_json::from_str(payload).context("invalid JSONL record page")?;
        value.validate_fields()?;
        if value.page_id != value.compute_id()? {
            bail!("record page identity does not match its content");
        }
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        let payload = serde_json::to_string_pretty(self)?;
        if payload.len() > MAX_JSONL_RECORD_PAGE_BYTES {
            bail!("JSONL record page exceeds {MAX_JSONL_RECORD_PAGE_BYTES} bytes");
        }
        Ok(payload)
    }

    pub fn page_id(&self) -> &str {
        &self.page_id
    }

    pub fn fact_count(&self) -> usize {
        self.facts.len()
    }

    fn validate_fields(&self) -> Result<()> {
        if self.namespace != crate::identity::ARTIFACT_NAMESPACE
            || self.schema_version != JSONL_RECORD_PAGE_SCHEMA_VERSION
            || self.adapter != "jsonl"
        {
            bail!("unsupported JSONL record page identity or schema");
        }
        if self.facts.is_empty() || self.facts.len() > MAX_JSONL_RECORD_PAGE_FACTS {
            bail!("JSONL record page fact count is invalid");
        }
        let mut previous: Option<&str> = None;
        for fact in &self.facts {
            validate_digest(&fact.record_id, "rid_", "record ID")?;
            validate_digest(&fact.content_hash, "record_", "record content hash")?;
            if fact.line == 0 {
                bail!("record line must be positive");
            }
            if previous.is_some_and(|value| value >= fact.record_id.as_str()) {
                bail!("record page facts must be strictly sorted by record ID");
            }
            previous = Some(&fact.record_id);
        }
        Ok(())
    }

    fn compute_id(&self) -> Result<String> {
        let mut identity = self.clone();
        identity.page_id.clear();
        content_id("datajig-jsonl-record-page-v1\0", "recordpage", &identity)
    }

    fn ensure_size(&self) -> Result<()> {
        let _ = self.to_json()?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct JsonlStateFieldSummary {
    name: String,
    present: usize,
    nulls: usize,
    types: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct JsonlStateFinding {
    code: String,
    line: usize,
    message: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlRecordState {
    namespace: String,
    schema_version: u8,
    adapter: String,
    id_field: String,
    dataset_content_id: String,
    bytes: u64,
    physical_lines: usize,
    records: usize,
    blank_lines: usize,
    invalid_records: usize,
    missing_ids: usize,
    null_ids: usize,
    invalid_ids: usize,
    duplicate_ids: usize,
    field_count: usize,
    fields: Vec<JsonlStateFieldSummary>,
    fields_truncated: bool,
    findings: usize,
    finding_items: Vec<JsonlStateFinding>,
    findings_truncated: bool,
    typed_id_algorithm: String,
    record_hash_algorithm: String,
    page_size: usize,
    page_ids: Vec<String>,
    indexed_records: usize,
    record_state_id: String,
}

impl JsonlRecordState {
    fn from_inspection(
        inspection: &JsonlInspection,
        page_ids: Vec<String>,
        indexed: usize,
    ) -> Result<Self> {
        let parts = inspection.state_parts();
        let mut value = Self {
            namespace: crate::identity::ARTIFACT_NAMESPACE.into(),
            schema_version: JSONL_RECORD_STATE_SCHEMA_VERSION,
            adapter: "jsonl".into(),
            id_field: inspection.id_field().into(),
            dataset_content_id: inspection.dataset_content_id().into(),
            bytes: parts.bytes,
            physical_lines: parts.physical_lines,
            records: parts.records,
            blank_lines: parts.blank_lines,
            invalid_records: parts.invalid_records,
            missing_ids: parts.missing_ids,
            null_ids: parts.null_ids,
            invalid_ids: parts.invalid_ids,
            duplicate_ids: parts.duplicate_ids,
            field_count: parts.field_count,
            fields: parts
                .fields
                .iter()
                .map(|field| JsonlStateFieldSummary {
                    name: field.name.clone(),
                    present: field.present,
                    nulls: field.nulls,
                    types: field.types.iter().map(|value| (*value).into()).collect(),
                })
                .collect(),
            fields_truncated: parts.fields_truncated,
            findings: parts.findings,
            finding_items: parts
                .finding_items
                .iter()
                .map(|finding| JsonlStateFinding {
                    code: finding.code.into(),
                    line: finding.line,
                    message: finding.message.into(),
                })
                .collect(),
            findings_truncated: parts.findings_truncated,
            typed_id_algorithm: "datajig-jsonl-record-id-v1".into(),
            record_hash_algorithm: "datajig-jsonl-record-v1".into(),
            page_size: MAX_JSONL_RECORD_PAGE_FACTS,
            page_ids,
            indexed_records: indexed,
            record_state_id: String::new(),
        };
        value.validate_fields()?;
        value.record_state_id = value.compute_id()?;
        value.ensure_size()?;
        Ok(value)
    }

    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_JSONL_RECORD_STATE_BYTES {
            bail!("JSONL record state exceeds {MAX_JSONL_RECORD_STATE_BYTES} bytes");
        }
        let value: Self = serde_json::from_str(payload).context("invalid JSONL record state")?;
        value.validate_fields()?;
        if value.record_state_id != value.compute_id()? {
            bail!("record state identity does not match its content");
        }
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        let payload = serde_json::to_string_pretty(self)?;
        if payload.len() > MAX_JSONL_RECORD_STATE_BYTES {
            bail!("JSONL record state exceeds {MAX_JSONL_RECORD_STATE_BYTES} bytes");
        }
        Ok(payload)
    }

    pub fn record_state_id(&self) -> &str {
        &self.record_state_id
    }

    pub fn dataset_content_id(&self) -> &str {
        &self.dataset_content_id
    }

    pub fn id_field(&self) -> &str {
        &self.id_field
    }

    pub fn finding_count(&self) -> usize {
        self.findings
    }

    pub fn findings_truncated(&self) -> bool {
        self.findings_truncated
    }

    pub fn structural_locations(&self, report_code: &str) -> Vec<JsonlStructuralLocation> {
        self.finding_items
            .iter()
            .filter(|finding| structural_code_matches(report_code, &finding.code))
            .map(|finding| JsonlStructuralLocation {
                line: finding.line,
                issue_code: finding.code.clone(),
            })
            .collect()
    }

    pub fn record_count(&self) -> usize {
        self.records
    }

    pub fn byte_count(&self) -> u64 {
        self.bytes
    }

    pub fn invalid_record_count(&self) -> usize {
        self.invalid_records
    }

    pub fn validation_counts(&self) -> (usize, usize, usize, usize, usize) {
        (
            self.invalid_records,
            self.missing_ids,
            self.null_ids,
            self.invalid_ids,
            self.duplicate_ids,
        )
    }

    pub fn page_ids(&self) -> &[String] {
        &self.page_ids
    }

    fn validate_fields(&self) -> Result<()> {
        if self.namespace != crate::identity::ARTIFACT_NAMESPACE
            || self.schema_version != JSONL_RECORD_STATE_SCHEMA_VERSION
            || self.adapter != "jsonl"
        {
            bail!("unsupported JSONL record state identity or schema");
        }
        if self.id_field.is_empty() || self.id_field.len() > 4_096 {
            bail!("record state ID field is invalid");
        }
        validate_digest(&self.dataset_content_id, "records_", "dataset content ID")?;
        if self.page_size != MAX_JSONL_RECORD_PAGE_FACTS
            || self.page_ids.len() > MAX_JSONL_RECORD_PAGES
        {
            bail!("record state page descriptor is invalid");
        }
        for id in &self.page_ids {
            validate_digest(id, "recordpage_", "record page ID")?;
        }
        if self.typed_id_algorithm != "datajig-jsonl-record-id-v1"
            || self.record_hash_algorithm != "datajig-jsonl-record-v1"
        {
            bail!("record state hash algorithm is unsupported");
        }
        if self.findings == 0 && self.indexed_records != self.records {
            bail!("valid record state must index every record");
        }
        if self.findings > 0 && self.indexed_records != 0 {
            bail!("invalid record state cannot expose a partial semantic index");
        }
        Ok(())
    }

    fn compute_id(&self) -> Result<String> {
        let mut identity = self.clone();
        identity.record_state_id.clear();
        content_id("datajig-jsonl-record-state-v1\0", "recordstate", &identity)
    }

    fn ensure_size(&self) -> Result<()> {
        let _ = self.to_json()?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct JsonlRecordStateBundle {
    state: JsonlRecordState,
    pages: Vec<JsonlRecordPage>,
}

impl JsonlRecordStateBundle {
    pub fn new(state: JsonlRecordState, pages: Vec<JsonlRecordPage>) -> Result<Self> {
        if state.page_ids.len() != pages.len()
            || state
                .page_ids
                .iter()
                .zip(&pages)
                .any(|(expected, page)| expected != page.page_id())
        {
            bail!("record state pages do not match the root object");
        }
        let fact_count = pages.iter().map(JsonlRecordPage::fact_count).sum::<usize>();
        if fact_count != state.indexed_records {
            bail!("record state indexed count does not match its pages");
        }
        for (index, page) in pages.iter().enumerate() {
            if page.page_index != index {
                bail!("record state page order is invalid");
            }
        }
        Ok(Self { state, pages })
    }

    pub fn state(&self) -> &JsonlRecordState {
        &self.state
    }

    pub fn pages(&self) -> &[JsonlRecordPage] {
        &self.pages
    }

    pub fn locate_record(&self, record_id: &str) -> Result<Option<JsonlRecordLocation>> {
        validate_digest(record_id, "rid_", "record ID")?;
        Ok(self
            .pages
            .iter()
            .flat_map(|page| &page.facts)
            .find(|fact| fact.record_id == record_id)
            .map(|fact| JsonlRecordLocation {
                line: fact.line,
                record_id: fact.record_id.clone(),
                record_content_id: fact.content_hash.clone(),
            }))
    }

    fn record_map(&self) -> Result<BTreeMap<[u8; 32], RecordFact>> {
        if self.state.finding_count() != 0 {
            return Err(InvalidArgumentError::new(
                "record diff requires both states to pass inspection without findings",
            )
            .into());
        }
        let mut records = BTreeMap::new();
        for fact in self.pages.iter().flat_map(|page| &page.facts) {
            let id = parse_digest(&fact.record_id, "rid_", "record ID")?;
            let content = parse_digest(&fact.content_hash, "record_", "record content hash")?;
            records.insert(
                id,
                RecordFact {
                    line: fact.line,
                    content_hash: blake3::Hash::from(content),
                },
            );
        }
        Ok(records)
    }
}

fn structural_code_matches(report_code: &str, issue_code: &str) -> bool {
    match report_code {
        "JSONL_INVALID_RECORD" => {
            matches!(
                issue_code,
                "INVALID_UTF8" | "INVALID_JSON" | "NON_OBJECT_RECORD"
            )
        }
        "JSONL_MISSING_ID" => issue_code == "MISSING_ID",
        "JSONL_NULL_ID" => issue_code == "NULL_ID",
        "JSONL_INVALID_ID" => issue_code == "INVALID_ID",
        "JSONL_DUPLICATE_ID" => issue_code == "DUPLICATE_ID",
        _ => false,
    }
}

#[derive(Clone, Debug)]
pub struct BuiltJsonlRecordState(JsonlRecordStateBundle);

impl BuiltJsonlRecordState {
    pub fn state(&self) -> &JsonlRecordState {
        self.0.state()
    }

    pub fn pages(&self) -> &[JsonlRecordPage] {
        self.0.pages()
    }

    pub fn bundle(&self) -> JsonlRecordStateBundle {
        self.0.clone()
    }
}

pub fn build_jsonl_record_state(source: &Path, id_field: &str) -> Result<BuiltJsonlRecordState> {
    let inspection =
        inspect_jsonl_with_record_limit(source, id_field, Some(MAX_JSONL_DIFF_RECORDS))?;
    build_jsonl_record_state_from_inspection(&inspection, id_field)
}

fn build_jsonl_record_state_from_inspection(
    inspection: &JsonlInspection,
    id_field: &str,
) -> Result<BuiltJsonlRecordState> {
    let records = if inspection.finding_count() == 0 {
        validated_record_facts(inspection, id_field)?
    } else {
        BTreeMap::new()
    };
    let mut pages = Vec::with_capacity(records.len().div_ceil(MAX_JSONL_RECORD_PAGE_FACTS));
    let mut page_facts = Vec::with_capacity(MAX_JSONL_RECORD_PAGE_FACTS);
    for (id, fact) in &records {
        page_facts.push(JsonlRecordPageFact {
            record_id: format!("rid_{}", blake3::Hash::from(*id).to_hex()),
            content_hash: format!("record_{}", fact.content_hash.to_hex()),
            line: fact.line,
        });
        if page_facts.len() == MAX_JSONL_RECORD_PAGE_FACTS {
            pages.push(JsonlRecordPage::new(pages.len(), page_facts)?);
            page_facts = Vec::with_capacity(MAX_JSONL_RECORD_PAGE_FACTS);
        }
    }
    if !page_facts.is_empty() {
        pages.push(JsonlRecordPage::new(pages.len(), page_facts)?);
    }
    let page_ids = pages.iter().map(|page| page.page_id.clone()).collect();
    let state = JsonlRecordState::from_inspection(inspection, page_ids, records.len())?;
    Ok(BuiltJsonlRecordState(JsonlRecordStateBundle::new(
        state, pages,
    )?))
}

pub(crate) fn build_jsonl_record_state_with_byte_limit(
    source: &Path,
    id_field: &str,
    byte_limit: u64,
) -> Result<BuiltJsonlRecordState> {
    let inspection = inspect_jsonl_with_limits(
        source,
        id_field,
        Some(MAX_JSONL_DIFF_RECORDS),
        Some(byte_limit),
    )?;
    build_jsonl_record_state_from_inspection(&inspection, id_field)
}

pub fn diff_jsonl_record_states(
    before: &JsonlRecordStateBundle,
    after: &JsonlRecordStateBundle,
) -> Result<JsonlRecordDiff> {
    if before.state.id_field != after.state.id_field {
        return Err(InvalidArgumentError::new("record states use different ID fields").into());
    }
    let before_records = before.record_map()?;
    let after_records = after.record_map()?;
    Ok(diff_record_maps(
        before.state.dataset_content_id(),
        after.state.dataset_content_id(),
        before.state.id_field(),
        &before_records,
        &after_records,
    ))
}

pub fn changed_record_ids(
    before: &JsonlRecordStateBundle,
    after: &JsonlRecordStateBundle,
) -> Result<BTreeSet<[u8; 32]>> {
    if before.state.id_field != after.state.id_field {
        return Err(InvalidArgumentError::new("record states use different ID fields").into());
    }
    let before_records = before.record_map()?;
    let after_records = after.record_map()?;
    Ok(after_records
        .iter()
        .filter_map(|(id, fact)| {
            let changed = before_records
                .get(id)
                .is_none_or(|before| before.content_hash != fact.content_hash);
            changed.then_some(*id)
        })
        .collect())
}

fn content_id(prefix: &str, kind: &str, value: &impl Serialize) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(prefix.as_bytes());
    serde_json::to_writer(&mut hasher, value)?;
    Ok(format!("{kind}_{}", hasher.finalize().to_hex()))
}

fn validate_digest(value: &str, prefix: &str, name: &str) -> Result<()> {
    let _ = parse_digest(value, prefix, name)?;
    Ok(())
}

fn parse_digest(value: &str, prefix: &str, name: &str) -> Result<[u8; 32]> {
    let digest = value
        .strip_prefix(prefix)
        .with_context(|| format!("{name} has an invalid prefix"))?;
    let hash = blake3::Hash::from_hex(digest).with_context(|| format!("{name} is invalid"))?;
    Ok(*hash.as_bytes())
}
