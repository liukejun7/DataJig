use crate::jsonl_inspect::{canonical_number, compare_json_numbers};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Number, Value};
use std::cmp::Ordering;

pub const JSONL_SUBSET_RECIPE_SCHEMA_VERSION: u8 = 1;
pub const MAX_JSONL_SUBSET_RECIPE_BYTES: usize = 64 * 1024;
pub const MAX_JSONL_SUBSET_CLAUSES: usize = 64;
pub const MAX_JSONL_SUBSET_VALUES: usize = 256;
pub const MAX_JSONL_SUBSET_LITERAL_BYTES: usize = 4 * 1024;
pub const MAX_JSONL_SUBSET_SEED_BYTES: usize = 256;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlSubsetRecipe {
    namespace: String,
    kind: String,
    schema_version: u8,
    #[serde(default, rename = "where")]
    clauses: Vec<JsonlSubsetClause>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sample: Option<JsonlSubsetSample>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct JsonlSubsetClause {
    field: String,
    op: JsonlSubsetOperator,
    #[serde(default, skip_serializing_if = "OptionalField::is_missing")]
    value: OptionalField<Value>,
    #[serde(default, skip_serializing_if = "OptionalField::is_missing")]
    values: OptionalField<Vec<Value>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
enum OptionalField<T> {
    #[default]
    Missing,
    Present(T),
}

impl<T> OptionalField<T> {
    fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }

    fn as_ref(&self) -> Option<&T> {
        match self {
            Self::Missing => None,
            Self::Present(value) => Some(value),
        }
    }

    fn as_mut(&mut self) -> Option<&mut T> {
        match self {
            Self::Missing => None,
            Self::Present(value) => Some(value),
        }
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for OptionalField<T> {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::Present)
    }
}

impl<T: Serialize> Serialize for OptionalField<T> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Missing => serializer.serialize_unit(),
            Self::Present(value) => value.serialize(serializer),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum JsonlSubsetOperator {
    Exists,
    Missing,
    IsNull,
    Eq,
    In,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct JsonlSubsetSample {
    rate_bps: u16,
    seed: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct JsonlViewRecordFact {
    record_id: [u8; 32],
    content_hash: [u8; 32],
}

impl JsonlViewRecordFact {
    pub(crate) fn new(record_id: [u8; 32], content_hash: [u8; 32]) -> Self {
        Self {
            record_id,
            content_hash,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct JsonlSubsetViewBinding {
    recipe: JsonlSubsetRecipe,
    recipe_id: String,
    view_id: String,
    source_records: usize,
    selected_records: usize,
}

impl JsonlSubsetViewBinding {
    pub(crate) fn new(
        source: &crate::TrainingSourceBinding,
        recipe: JsonlSubsetRecipe,
        source_records: usize,
        mut facts: Vec<JsonlViewRecordFact>,
    ) -> Result<Self> {
        if source_records > crate::MAX_JSONL_DIFF_RECORDS || facts.len() > source_records {
            bail!("subset view record counts are invalid");
        }
        facts.sort_by_key(|fact| fact.record_id);
        if facts
            .windows(2)
            .any(|pair| pair[0].record_id == pair[1].record_id)
        {
            bail!("subset view contains duplicate record identifiers");
        }
        let recipe_id = recipe.recipe_id();
        let view_id = compute_view_id(source, &recipe_id, source_records, &facts)?;
        let value = Self {
            recipe,
            recipe_id,
            view_id,
            source_records,
            selected_records: facts.len(),
        };
        value.validate()?;
        Ok(value)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.recipe.validate()?;
        validate_prefixed_digest(&self.recipe_id, "recipe", "subset recipe_id")?;
        validate_prefixed_digest(&self.view_id, "view", "subset view_id")?;
        if self.recipe.recipe_id() != self.recipe_id {
            bail!("subset recipe identity does not match its recipe");
        }
        if self.source_records > crate::MAX_JSONL_DIFF_RECORDS
            || self.selected_records > self.source_records
        {
            bail!("subset view record counts are invalid");
        }
        Ok(())
    }

    pub fn recipe(&self) -> &JsonlSubsetRecipe {
        &self.recipe
    }

    pub fn recipe_id(&self) -> &str {
        &self.recipe_id
    }

    pub fn view_id(&self) -> &str {
        &self.view_id
    }

    pub fn source_records(&self) -> usize {
        self.source_records
    }

    pub fn selected_records(&self) -> usize {
        self.selected_records
    }
}

fn compute_view_id(
    source: &crate::TrainingSourceBinding,
    recipe_id: &str,
    source_records: usize,
    facts: &[JsonlViewRecordFact],
) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-subset-view-v1\0");
    serde_json::to_writer(&mut hasher, source)?;
    hasher.update(&(recipe_id.len() as u64).to_be_bytes());
    hasher.update(recipe_id.as_bytes());
    hasher.update(&(source_records as u64).to_be_bytes());
    hasher.update(&(facts.len() as u64).to_be_bytes());
    for fact in facts {
        hasher.update(&fact.record_id);
        hasher.update(&fact.content_hash);
    }
    Ok(format!("view_{}", hasher.finalize().to_hex()))
}

fn validate_prefixed_digest(value: &str, prefix: &str, label: &str) -> Result<()> {
    let Some(digest) = value.strip_prefix(&format!("{prefix}_")) else {
        bail!("{label} is invalid");
    };
    if digest.len() != 64
        || !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        bail!("{label} is invalid");
    }
    Ok(())
}

impl JsonlSubsetRecipe {
    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_JSONL_SUBSET_RECIPE_BYTES {
            bail!("subset recipe exceeds {MAX_JSONL_SUBSET_RECIPE_BYTES} bytes");
        }
        crate::strict_json::reject_duplicate_json_members(payload)
            .context("invalid subset recipe")?;
        let mut value: Self = serde_json::from_str(payload).context("invalid subset recipe")?;
        value.normalize()?;
        value.validate()?;
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        let payload = serde_json::to_string_pretty(self)?;
        if payload.len() > MAX_JSONL_SUBSET_RECIPE_BYTES {
            bail!("subset recipe exceeds {MAX_JSONL_SUBSET_RECIPE_BYTES} bytes");
        }
        Ok(payload)
    }

    pub fn recipe_id(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"datajig-subset-recipe-v1\0");
        serde_json::to_writer(&mut hasher, self).expect("normalized recipe serializes");
        format!("recipe_{}", hasher.finalize().to_hex())
    }

    pub fn matches(&self, record: &Map<String, Value>) -> bool {
        self.clauses
            .iter()
            .all(|clause| clause.matches(record.get(&clause.field)))
    }

    pub fn includes_sample(&self, record_id: [u8; 32]) -> bool {
        let Some(sample) = &self.sample else {
            return true;
        };
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"datajig-subset-sample-v1\0");
        hasher.update(&(sample.seed.len() as u64).to_be_bytes());
        hasher.update(sample.seed.as_bytes());
        hasher.update(&record_id);
        let digest = hasher.finalize();
        let hash_prefix = u64::from_be_bytes(
            digest.as_bytes()[..8]
                .try_into()
                .expect("BLAKE3 digest has eight bytes"),
        );
        let slot = ((u128::from(hash_prefix) * 10_000) >> 64) as u16;
        slot < sample.rate_bps
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let mut canonical = self.clone();
        canonical.normalize()?;
        if canonical != *self {
            bail!("subset recipe is not canonical");
        }
        if serde_json::to_vec(self)?.len() > MAX_JSONL_SUBSET_RECIPE_BYTES {
            bail!("subset recipe exceeds {MAX_JSONL_SUBSET_RECIPE_BYTES} bytes");
        }
        Ok(())
    }

    fn normalize(&mut self) -> Result<()> {
        if self.namespace != crate::identity::ARTIFACT_NAMESPACE
            || self.kind != "subset_view"
            || self.schema_version != JSONL_SUBSET_RECIPE_SCHEMA_VERSION
        {
            bail!("unsupported subset recipe identity or schema");
        }
        if self.clauses.len() > MAX_JSONL_SUBSET_CLAUSES {
            bail!("subset recipe exceeds {MAX_JSONL_SUBSET_CLAUSES} predicates");
        }
        for clause in &mut self.clauses {
            clause.normalize()?;
        }
        self.clauses.sort_by(|left, right| {
            serde_json::to_vec(left)
                .expect("normalized predicate serializes")
                .cmp(&serde_json::to_vec(right).expect("normalized predicate serializes"))
        });
        if self.clauses.windows(2).any(|pair| pair[0] == pair[1]) {
            bail!("subset recipe contains a duplicate predicate");
        }
        if let Some(sample) = &self.sample {
            if !(1..=10_000).contains(&sample.rate_bps) {
                bail!("subset sample rate_bps must be between 1 and 10000");
            }
            if sample.seed.is_empty() || sample.seed.len() > MAX_JSONL_SUBSET_SEED_BYTES {
                bail!(
                    "subset sample seed must be between 1 and {MAX_JSONL_SUBSET_SEED_BYTES} bytes"
                );
            }
            if sample.rate_bps == 10_000 {
                self.sample = None;
            }
        }
        Ok(())
    }
}

impl JsonlSubsetClause {
    fn normalize(&mut self) -> Result<()> {
        if self.field.is_empty() || self.field.len() > crate::MAX_JSONL_FIELD_NAME_BYTES {
            bail!("subset predicate field is invalid");
        }
        match self.op {
            JsonlSubsetOperator::Exists
            | JsonlSubsetOperator::Missing
            | JsonlSubsetOperator::IsNull => {
                if !self.value.is_missing() || !self.values.is_missing() {
                    bail!("subset predicate operator does not accept a literal");
                }
            }
            JsonlSubsetOperator::Eq => {
                if !self.values.is_missing() {
                    bail!("subset eq predicate does not accept values");
                }
                let value = self
                    .value
                    .as_mut()
                    .context("subset eq predicate requires value")?;
                normalize_scalar(value)?;
            }
            JsonlSubsetOperator::In => {
                if !self.value.is_missing() {
                    bail!("subset in predicate does not accept value");
                }
                let values = self
                    .values
                    .as_mut()
                    .context("subset in predicate requires values")?;
                if values.is_empty() || values.len() > MAX_JSONL_SUBSET_VALUES {
                    bail!(
                        "subset in predicate requires between 1 and {MAX_JSONL_SUBSET_VALUES} values"
                    );
                }
                for value in values.iter_mut() {
                    normalize_scalar(value)?;
                }
                values.sort_by_key(scalar_key);
                values.dedup_by(|left, right| scalar_key(left) == scalar_key(right));
            }
            JsonlSubsetOperator::Lt
            | JsonlSubsetOperator::Lte
            | JsonlSubsetOperator::Gt
            | JsonlSubsetOperator::Gte => {
                if !self.values.is_missing() {
                    bail!("numeric subset predicate does not accept values");
                }
                let value = self
                    .value
                    .as_mut()
                    .context("numeric subset predicate requires value")?;
                if !value.is_number() {
                    bail!("numeric subset predicate requires a JSON number");
                }
                normalize_scalar(value)?;
            }
        }
        Ok(())
    }

    fn matches(&self, candidate: Option<&Value>) -> bool {
        match self.op {
            JsonlSubsetOperator::Exists => candidate.is_some(),
            JsonlSubsetOperator::Missing => candidate.is_none(),
            JsonlSubsetOperator::IsNull => candidate == Some(&Value::Null),
            JsonlSubsetOperator::Eq => candidate.is_some_and(|candidate| {
                scalar_key(candidate) == scalar_key(self.value.as_ref().expect("validated value"))
            }),
            JsonlSubsetOperator::In => candidate.is_some_and(|candidate| {
                let candidate = scalar_key(candidate);
                self.values
                    .as_ref()
                    .expect("validated values")
                    .binary_search_by_key(&candidate, scalar_key)
                    .is_ok()
            }),
            JsonlSubsetOperator::Lt
            | JsonlSubsetOperator::Lte
            | JsonlSubsetOperator::Gt
            | JsonlSubsetOperator::Gte => {
                let (Some(Value::Number(candidate)), Some(Value::Number(expected))) =
                    (candidate, self.value.as_ref())
                else {
                    return false;
                };
                let ordering = compare_json_numbers(candidate, expected);
                match self.op {
                    JsonlSubsetOperator::Lt => ordering == Ordering::Less,
                    JsonlSubsetOperator::Lte => ordering != Ordering::Greater,
                    JsonlSubsetOperator::Gt => ordering == Ordering::Greater,
                    JsonlSubsetOperator::Gte => ordering != Ordering::Less,
                    _ => unreachable!(),
                }
            }
        }
    }
}

fn normalize_scalar(value: &mut Value) -> Result<()> {
    match value {
        Value::Null | Value::Bool(_) => Ok(()),
        Value::Number(number) => {
            let canonical = String::from_utf8(canonical_number(number))?;
            *number = serde_json::from_str::<Number>(&canonical)
                .context("cannot normalize subset numeric literal")?;
            Ok(())
        }
        Value::String(value) if value.len() <= MAX_JSONL_SUBSET_LITERAL_BYTES => Ok(()),
        Value::String(_) => {
            bail!("subset string literal exceeds {MAX_JSONL_SUBSET_LITERAL_BYTES} bytes")
        }
        Value::Array(_) | Value::Object(_) => {
            bail!("subset predicates accept only scalar JSON literals")
        }
    }
}

fn scalar_key(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => b"z".to_vec(),
        Value::Bool(value) => if *value { b"b1" } else { b"b0" }.to_vec(),
        Value::Number(value) => {
            let mut key = b"n".to_vec();
            key.extend_from_slice(&canonical_number(value));
            key
        }
        Value::String(value) => {
            let mut key = b"s".to_vec();
            key.extend_from_slice(value.as_bytes());
            key
        }
        Value::Array(_) | Value::Object(_) => b"invalid".to_vec(),
    }
}
