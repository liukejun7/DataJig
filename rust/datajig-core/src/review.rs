use crate::inventory::{
    DatasetInventory, InventoryRecord, load_inventory, scan_inventory, scan_inventory_for_schema,
};
use crate::io::save_file_atomically;
use crate::{InvalidArgumentError, MAX_INVENTORY_THREADS, ReviewReport};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

pub const MAX_PHASH_THRESHOLD: usize = 64;
pub const MAX_REVIEW_MATCH_CANDIDATES: usize = 1_000_000;

#[derive(Clone, Debug, Serialize)]
pub struct ReviewArtifact {
    pub bytes: usize,
    pub content_id: String,
    pub findings: usize,
    pub output: String,
    pub report_schema_version: u8,
    pub status: String,
}

enum DatasetReference {
    Path(PathBuf),
    Inventory(PathBuf),
}

impl DatasetReference {
    fn parse(value: &str) -> Result<Self> {
        if let Some(path) = value.strip_prefix("path:") {
            return nonempty_path(path, "path").map(Self::Path);
        }
        if let Some(path) = value.strip_prefix("inventory:") {
            return nonempty_path(path, "inventory").map(Self::Inventory);
        }
        if value.is_empty() {
            return Err(InvalidArgumentError::new("dataset reference must not be empty").into());
        }
        if value
            .split_once(':')
            .is_some_and(|(scheme, _)| is_reference_scheme(scheme))
        {
            return Err(InvalidArgumentError::new(
                "unsupported dataset reference; expected path:<directory> or inventory:<file>",
            )
            .into());
        }
        Ok(Self::Path(PathBuf::from(value)))
    }

    fn resolve_for_schema(
        &self,
        threads: usize,
        schema_version: Option<u8>,
    ) -> Result<DatasetInventory> {
        match self {
            Self::Path(path) => match schema_version {
                Some(schema_version) => scan_inventory_for_schema(path, threads, schema_version),
                None => scan_inventory(path, threads),
            },
            Self::Inventory(path) => load_inventory(path),
        }
    }

    fn path_root(&self) -> Option<&Path> {
        match self {
            Self::Path(path) => Some(path),
            Self::Inventory(_) => None,
        }
    }

    fn inventory_path(&self) -> Option<&Path> {
        match self {
            Self::Inventory(path) => Some(path),
            Self::Path(_) => None,
        }
    }
}

fn nonempty_path(value: &str, scheme: &str) -> Result<PathBuf> {
    if value.is_empty() {
        return Err(InvalidArgumentError::new(format!(
            "{scheme} dataset reference must include a path"
        ))
        .into());
    }
    Ok(PathBuf::from(value))
}

fn is_reference_scheme(value: &str) -> bool {
    !value.is_empty()
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '+' | '-' | '.')
        })
}

pub fn create_review(
    before: &str,
    after: &str,
    output: &Path,
    threads: usize,
    phash_threshold: usize,
) -> Result<ReviewArtifact> {
    create_review_with_metadata(before, after, output, threads, phash_threshold, &[])
}

pub(crate) fn create_review_with_metadata(
    before: &str,
    after: &str,
    output: &Path,
    threads: usize,
    phash_threshold: usize,
    extra_metadata: &[(&str, &str)],
) -> Result<ReviewArtifact> {
    create_review_internal(
        before,
        after,
        output,
        threads,
        phash_threshold,
        extra_metadata,
        None,
    )
}

pub(crate) fn create_review_for_schema(
    before: &str,
    after: &str,
    output: &Path,
    threads: usize,
    phash_threshold: usize,
    schema_version: u8,
) -> Result<ReviewArtifact> {
    create_review_internal(
        before,
        after,
        output,
        threads,
        phash_threshold,
        &[],
        Some(schema_version),
    )
}

fn create_review_internal(
    before: &str,
    after: &str,
    output: &Path,
    threads: usize,
    phash_threshold: usize,
    extra_metadata: &[(&str, &str)],
    path_schema_version: Option<u8>,
) -> Result<ReviewArtifact> {
    if !(1..=MAX_INVENTORY_THREADS).contains(&threads) {
        return Err(InvalidArgumentError::new(format!(
            "threads must be between 1 and {MAX_INVENTORY_THREADS}"
        ))
        .into());
    }
    if phash_threshold > MAX_PHASH_THRESHOLD {
        return Err(InvalidArgumentError::new(format!(
            "phash threshold must be between 0 and {MAX_PHASH_THRESHOLD}"
        ))
        .into());
    }
    let before_reference = DatasetReference::parse(before)?;
    let after_reference = DatasetReference::parse(after)?;
    let before = before_reference.resolve_for_schema(threads, path_schema_version)?;
    let after = after_reference.resolve_for_schema(threads, path_schema_version)?;
    let destination = safe_review_destination(
        output,
        [before_reference.path_root(), after_reference.path_root()]
            .into_iter()
            .flatten(),
        [
            before_reference.inventory_path(),
            after_reference.inventory_path(),
        ]
        .into_iter()
        .flatten(),
    )?;
    let mut report = build_report(&before, &after, phash_threshold)?;
    let metadata = report["metadata"]
        .as_object_mut()
        .context("generated review metadata is invalid")?;
    for (key, value) in extra_metadata {
        if metadata.contains_key(*key) {
            bail!("review metadata key {key:?} is reserved");
        }
        metadata.insert((*key).into(), json!(value));
    }
    let payload = serde_json::to_string_pretty(&report)?;
    ReviewReport::from_json(&payload).context("generated review failed schema validation")?;
    save_file_atomically(&destination, payload.as_bytes(), "review report")?;
    let canonical = destination
        .canonicalize()
        .context("cannot resolve generated review report")?;
    let findings = report["findings"].as_array().map_or(0, Vec::len);
    let status = report["policy"]["status"]
        .as_str()
        .context("generated review status is invalid")?
        .to_owned();
    Ok(ReviewArtifact {
        bytes: payload.len(),
        content_id: crate::report::report_content_id(payload.as_bytes()),
        findings,
        output: canonical.to_string_lossy().into_owned(),
        report_schema_version: 1,
        status,
    })
}

fn safe_review_destination<'a>(
    output: &Path,
    roots: impl IntoIterator<Item = &'a Path>,
    input_artifacts: impl IntoIterator<Item = &'a Path>,
) -> Result<PathBuf> {
    let absolute = lexical_normalize(std::path::absolute(output)?);
    let name = absolute
        .file_name()
        .context("review output has no file name")?;
    let parent = absolute.parent().context("review output has no parent")?;
    let mut existing = parent;
    let mut missing = Vec::new();
    while !existing.exists() {
        missing.push(
            existing
                .file_name()
                .context("cannot resolve review output parent")?,
        );
        existing = existing
            .parent()
            .context("cannot resolve review output parent")?;
    }
    let mut resolved_parent = existing
        .canonicalize()
        .context("cannot resolve review output parent")?;
    for component in missing.into_iter().rev() {
        resolved_parent.push(component);
    }
    let destination = resolved_parent.join(name);
    for root in roots {
        let root = root.canonicalize().context("cannot resolve dataset root")?;
        if destination == root || destination.starts_with(root) {
            return Err(InvalidArgumentError::new(
                "review output must stay outside path-referenced datasets",
            )
            .into());
        }
    }
    for input in input_artifacts {
        if destination
            == input
                .canonicalize()
                .context("cannot resolve inventory input")?
        {
            return Err(InvalidArgumentError::new(
                "review output must not replace an input inventory",
            )
            .into());
        }
    }
    Ok(destination)
}

fn lexical_normalize(path: PathBuf) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                normalized.push(component.as_os_str());
            }
        }
    }
    normalized
}

#[derive(Clone)]
struct Match {
    before: usize,
    after: usize,
    method: &'static str,
    distance: Option<usize>,
}

fn build_report(
    before: &DatasetInventory,
    after: &DatasetInventory,
    threshold: usize,
) -> Result<Value> {
    let matches = match_records(before.records(), after.records(), threshold)?;
    let mut matched_before = vec![false; before.records().len()];
    let mut matched_after = vec![false; after.records().len()];
    let mut findings = inventory_findings(before, after);
    let mut match_values = Vec::with_capacity(matches.len());
    for item in &matches {
        matched_before[item.before] = true;
        matched_after[item.after] = true;
        let left = &before.records()[item.before];
        let right = &after.records()[item.after];
        let sample_ids = vec![left.relative_path.clone(), right.relative_path.clone()];
        if left.split != right.split {
            findings.push(finding(
                "SPLIT_CHANGED",
                "info",
                "sample changed split",
                sample_ids.clone(),
                Map::new(),
            ));
        }
        if left.label != right.label {
            findings.push(finding(
                "LABEL_CHANGED",
                "info",
                "sample changed label",
                sample_ids.clone(),
                Map::new(),
            ));
        }
        if left.logical_path != right.logical_path {
            findings.push(finding(
                "SAMPLE_MOVED",
                "info",
                "sample path changed",
                sample_ids.clone(),
                Map::new(),
            ));
        }
        if left.content_hash != right.content_hash {
            let similar = item.distance.is_some_and(|distance| distance <= threshold);
            let mut evidence = Map::new();
            if let Some(distance) = item.distance {
                evidence.insert("perceptual_distance".into(), json!(distance));
            }
            findings.push(finding(
                if similar {
                    "PROBABLE_REENCODE"
                } else {
                    "CONTENT_CHANGED"
                },
                "info",
                if similar {
                    "sample bytes changed but perceptual content is similar"
                } else {
                    "sample content changed"
                },
                sample_ids,
                evidence,
            ));
        }
        match_values.push(json!({
            "baseline_id": left.relative_path,
            "candidate_id": right.relative_path,
            "method": item.method,
            "distance": item.distance,
            "confidence": if left.content_hash == right.content_hash { "exact" } else { "probable" },
        }));
    }
    for (index, record) in after.records().iter().enumerate() {
        if !matched_after[index] {
            findings.push(finding(
                "SAMPLE_ADDED",
                "info",
                "sample was added",
                vec![record.relative_path.clone()],
                Map::new(),
            ));
        }
    }
    for (index, record) in before.records().iter().enumerate() {
        if !matched_before[index] {
            findings.push(finding(
                "SAMPLE_REMOVED",
                "info",
                "sample was removed",
                vec![record.relative_path.clone()],
                Map::new(),
            ));
        }
    }
    findings.extend(exact_leakage_findings(after.records()));
    let distributions = distribution_deltas(before.records(), after.records());
    findings.extend(distribution_findings(&distributions));
    findings.sort_by(|left, right| finding_sort_key(left).cmp(&finding_sort_key(right)));
    let policy = evaluate_policy(&findings);
    let mut samples = before
        .records()
        .iter()
        .chain(after.records())
        .collect::<Vec<_>>();
    samples.sort_by(|left, right| {
        left.relative_path
            .as_bytes()
            .cmp(right.relative_path.as_bytes())
    });
    match_values.sort_by(|left, right| {
        string_at(left, "baseline_id")
            .cmp(string_at(right, "baseline_id"))
            .then_with(|| string_at(left, "candidate_id").cmp(string_at(right, "candidate_id")))
    });
    Ok(json!({
        "namespace": "datajig",
        "schema_version": 1,
        "baseline": before.root(),
        "candidate": after.root(),
        "complete": true,
        "metadata": {
            "baseline_inventory_id": before.content_id()?,
            "baseline_coverage": before.coverage(),
            "baseline_unsupported": before.unsupported_paths().len(),
            "candidate_inventory_id": after.content_id()?,
            "candidate_coverage": after.coverage(),
            "candidate_unsupported": after.unsupported_paths().len(),
            "layout": "imagefolder",
            "phash_threshold": threshold,
            "distribution_bucket_boundaries": [
                "size: <256, 256-511, 512-1023, >=1024, unknown",
                "aspect_ratio: portrait <0.8, square 0.8-1.25, landscape >1.25, unknown"
            ],
            "review_engine": "rust-native-v1"
        },
        "samples": samples,
        "findings": findings,
        "matches": match_values,
        "distributions": distributions,
        "policy": policy,
    }))
}

fn match_records(
    before: &[InventoryRecord],
    after: &[InventoryRecord],
    threshold: usize,
) -> Result<Vec<Match>> {
    let mut used_before = vec![false; before.len()];
    let mut used_after = vec![false; after.len()];
    let mut matches = Vec::new();
    let after_paths = after
        .iter()
        .enumerate()
        .map(|(index, record)| (record.relative_path.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    for (before_index, record) in before.iter().enumerate() {
        if let Some(&after_index) = after_paths.get(record.relative_path.as_str()) {
            used_before[before_index] = true;
            used_after[after_index] = true;
            matches.push(Match {
                before: before_index,
                after: after_index,
                method: "path",
                distance: phash_distance(
                    record.perceptual_hash.as_deref(),
                    after[after_index].perceptual_hash.as_deref(),
                ),
            });
        }
    }
    let mut before_digests = BTreeMap::<&str, Vec<usize>>::new();
    let mut after_digests = BTreeMap::<&str, Vec<usize>>::new();
    for (index, record) in before
        .iter()
        .enumerate()
        .filter(|(index, _)| !used_before[*index])
    {
        before_digests
            .entry(&record.content_hash)
            .or_default()
            .push(index);
    }
    for (index, record) in after
        .iter()
        .enumerate()
        .filter(|(index, _)| !used_after[*index])
    {
        after_digests
            .entry(&record.content_hash)
            .or_default()
            .push(index);
    }
    for digest in before_digests
        .keys()
        .filter(|digest| after_digests.contains_key(**digest))
    {
        for (&before_index, &after_index) in
            before_digests[*digest].iter().zip(&after_digests[*digest])
        {
            used_before[before_index] = true;
            used_after[after_index] = true;
            matches.push(Match {
                before: before_index,
                after: after_index,
                method: "content",
                distance: Some(0),
            });
        }
    }
    let mut candidates = Vec::new();
    let mut index = PhashTree::default();
    for (after_index, record) in after
        .iter()
        .enumerate()
        .filter(|(index, _)| !used_after[*index])
    {
        if let Some(hash) = parse_phash(record.perceptual_hash.as_deref()) {
            index.insert(hash, after_index);
        }
    }
    for (before_index, left) in before
        .iter()
        .enumerate()
        .filter(|(index, _)| !used_before[*index])
    {
        if let Some(hash) = parse_phash(left.perceptual_hash.as_deref()) {
            for (distance, after_index) in index.query(hash, threshold as u32) {
                if candidates.len() == MAX_REVIEW_MATCH_CANDIDATES {
                    bail!(
                        "perceptual matching exceeds {MAX_REVIEW_MATCH_CANDIDATES} candidate pairs; lower the pHash threshold"
                    );
                }
                candidates.push((
                    distance as usize,
                    left.relative_path.as_str(),
                    after[after_index].relative_path.as_str(),
                    before_index,
                    after_index,
                ));
            }
        }
    }
    candidates.sort_unstable();
    for (distance, _, _, before_index, after_index) in candidates {
        if !used_before[before_index] && !used_after[after_index] {
            used_before[before_index] = true;
            used_after[after_index] = true;
            matches.push(Match {
                before: before_index,
                after: after_index,
                method: "perceptual",
                distance: Some(distance),
            });
        }
    }
    matches.sort_by(|left, right| {
        before[left.before]
            .relative_path
            .cmp(&before[right.before].relative_path)
            .then_with(|| {
                after[left.after]
                    .relative_path
                    .cmp(&after[right.after].relative_path)
            })
    });
    Ok(matches)
}

fn phash_distance(left: Option<&str>, right: Option<&str>) -> Option<usize> {
    Some((parse_phash(left)? ^ parse_phash(right)?).count_ones() as usize)
}

fn parse_phash(value: Option<&str>) -> Option<u64> {
    u64::from_str_radix(value?, 16).ok()
}

#[derive(Default)]
struct PhashTree {
    nodes: Vec<PhashNode>,
}

struct PhashNode {
    hash: u64,
    items: Vec<usize>,
    children: BTreeMap<u32, usize>,
}

impl PhashTree {
    fn insert(&mut self, hash: u64, item: usize) {
        if self.nodes.is_empty() {
            self.nodes.push(PhashNode {
                hash,
                items: vec![item],
                children: BTreeMap::new(),
            });
            return;
        }
        let mut node_index = 0;
        loop {
            let distance = (self.nodes[node_index].hash ^ hash).count_ones();
            if distance == 0 {
                self.nodes[node_index].items.push(item);
                return;
            }
            if let Some(&child) = self.nodes[node_index].children.get(&distance) {
                node_index = child;
                continue;
            }
            let child = self.nodes.len();
            self.nodes.push(PhashNode {
                hash,
                items: vec![item],
                children: BTreeMap::new(),
            });
            self.nodes[node_index].children.insert(distance, child);
            return;
        }
    }

    fn query(&self, hash: u64, threshold: u32) -> Vec<(u32, usize)> {
        if self.nodes.is_empty() {
            return Vec::new();
        }
        let mut matches = Vec::new();
        let mut pending = vec![0_usize];
        while let Some(node_index) = pending.pop() {
            let node = &self.nodes[node_index];
            let distance = (node.hash ^ hash).count_ones();
            if distance <= threshold {
                matches.extend(node.items.iter().map(|&item| (distance, item)));
            }
            let minimum = distance.saturating_sub(threshold);
            let maximum = distance.saturating_add(threshold).min(64);
            pending.extend(
                node.children
                    .range(minimum..=maximum)
                    .map(|(_, &child)| child),
            );
        }
        matches
    }
}

fn inventory_findings(before: &DatasetInventory, after: &DatasetInventory) -> Vec<Value> {
    before
        .findings()
        .iter()
        .chain(after.findings())
        .map(|item| {
            finding(
                item.code(),
                item.severity(),
                item.message(),
                item.sample_ids().to_vec(),
                Map::new(),
            )
        })
        .collect()
}

fn exact_leakage_findings(records: &[InventoryRecord]) -> Vec<Value> {
    let mut groups = BTreeMap::<&str, Vec<&InventoryRecord>>::new();
    for record in records {
        groups.entry(&record.content_hash).or_default().push(record);
    }
    let mut findings = Vec::new();
    for (digest, mut group) in groups.into_iter().filter(|(_, group)| group.len() > 1) {
        group.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
        let sample_ids = group
            .iter()
            .map(|item| item.relative_path.clone())
            .collect::<Vec<_>>();
        let mut evidence = Map::new();
        evidence.insert("content_hash".into(), json!(digest));
        evidence.insert("group_size".into(), json!(group.len()));
        findings.push(finding(
            "EXACT_DUPLICATE_GROUP",
            "warning",
            "identical image bytes appear more than once",
            sample_ids.clone(),
            evidence.clone(),
        ));
        if group
            .iter()
            .map(|item| item.split.as_str())
            .collect::<BTreeSet<_>>()
            .len()
            > 1
        {
            findings.push(finding(
                "CROSS_SPLIT_EXACT_LEAKAGE",
                "error",
                "identical image bytes appear across splits",
                sample_ids,
                evidence,
            ));
        }
    }
    findings
}

fn dimensions(record: &InventoryRecord) -> [(String, String); 7] {
    [
        ("split".into(), record.split.clone()),
        ("label".into(), record.label.clone()),
        ("width".into(), size_bucket(record.width).into()),
        ("height".into(), size_bucket(record.height).into()),
        (
            "aspect_ratio".into(),
            aspect_bucket(record.width, record.height).into(),
        ),
        (
            "format".into(),
            record
                .media_format
                .clone()
                .unwrap_or_else(|| "unknown".into()),
        ),
        (
            "channels".into(),
            record
                .channels
                .map_or_else(|| "unknown".into(), |value| value.to_string()),
        ),
    ]
}

fn size_bucket(value: Option<u32>) -> &'static str {
    match value {
        None => "unknown",
        Some(0..=255) => "<256",
        Some(256..=511) => "256-511",
        Some(512..=1023) => "512-1023",
        Some(_) => ">=1024",
    }
}

fn aspect_bucket(width: Option<u32>, height: Option<u32>) -> &'static str {
    match (width, height) {
        (Some(_), Some(0)) | (None, _) | (_, None) => "unknown",
        (Some(width), Some(height)) => {
            let ratio = f64::from(width) / f64::from(height);
            if ratio < 0.8 {
                "portrait"
            } else if ratio <= 1.25 {
                "square"
            } else {
                "landscape"
            }
        }
    }
}

fn distribution_deltas(before: &[InventoryRecord], after: &[InventoryRecord]) -> Vec<Value> {
    let count = |records: &[InventoryRecord]| {
        let mut counts = BTreeMap::<(String, String), usize>::new();
        for record in records {
            for dimension in dimensions(record) {
                *counts.entry(dimension).or_default() += 1;
            }
        }
        counts
    };
    let before_counts = count(before);
    let after_counts = count(after);
    let keys = before_counts
        .keys()
        .chain(after_counts.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    keys.into_iter()
        .map(|(dimension, key)| {
            let before_count = before_counts
                .get(&(dimension.clone(), key.clone()))
                .copied()
                .unwrap_or(0);
            let after_count = after_counts
                .get(&(dimension.clone(), key.clone()))
                .copied()
                .unwrap_or(0);
            let before_proportion = if before.is_empty() {
                0.0
            } else {
                before_count as f64 / before.len() as f64
            };
            let after_proportion = if after.is_empty() {
                0.0
            } else {
                after_count as f64 / after.len() as f64
            };
            let percentage_delta = (before_proportion != 0.0)
                .then_some(((after_proportion - before_proportion) / before_proportion) * 100.0);
            json!({
                "dimension": dimension, "key": key,
                "before_count": before_count, "after_count": after_count,
                "before_proportion": before_proportion, "after_proportion": after_proportion,
                "percentage_delta": percentage_delta,
            })
        })
        .collect()
}

fn distribution_findings(deltas: &[Value]) -> Vec<Value> {
    let media = ["width", "height", "aspect_ratio", "format", "channels"];
    deltas
        .iter()
        .filter_map(|delta| {
            let dimension = delta["dimension"].as_str()?;
            let key = delta["key"].as_str()?;
            let change =
                (delta["after_proportion"].as_f64()? - delta["before_proportion"].as_f64()?).abs();
            let code = if dimension == "label" && change >= 0.1 {
                "LABEL_DISTRIBUTION_CHANGED"
            } else if media.contains(&dimension) && change >= 0.1 {
                "MEDIA_DISTRIBUTION_CHANGED"
            } else {
                return None;
            };
            let evidence = Map::from_iter([
                ("dimension".into(), json!(dimension)),
                ("key".into(), json!(key)),
                (
                    "before_proportion".into(),
                    delta["before_proportion"].clone(),
                ),
                ("after_proportion".into(), delta["after_proportion"].clone()),
            ]);
            Some(finding(
                code,
                "warning",
                &format!("{dimension} distribution changed for {key}"),
                vec![],
                evidence,
            ))
        })
        .collect()
}

fn finding(
    code: &str,
    severity: &str,
    message: &str,
    mut sample_ids: Vec<String>,
    evidence: Map<String, Value>,
) -> Value {
    sample_ids.sort();
    sample_ids.dedup();
    json!({
        "code": code, "severity": severity, "message": message,
        "sample_ids": sample_ids, "evidence": evidence,
    })
}

fn finding_sort_key(value: &Value) -> (&str, Vec<&str>, &str) {
    (
        string_at(value, "code"),
        value["sample_ids"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect(),
        string_at(value, "message"),
    )
}

fn string_at<'a>(value: &'a Value, key: &str) -> &'a str {
    value[key].as_str().unwrap_or_default()
}

fn evaluate_policy(findings: &[Value]) -> Value {
    let defaults = BTreeMap::from([
        ("AMBIGUOUS_MATCH", "warning"),
        ("CROSS_SPLIT_EXACT_LEAKAGE", "error"),
        ("CROSS_SPLIT_NEAR_LEAKAGE", "warning"),
        ("IMAGE_DECODE_FAILED", "error"),
        ("INVALID_LAYOUT", "error"),
        ("LABEL_DISTRIBUTION_CHANGED", "warning"),
        ("MEDIA_DISTRIBUTION_CHANGED", "warning"),
        ("NEAR_DUPLICATE_GROUP", "warning"),
    ]);
    let mut failures = BTreeSet::new();
    let mut warnings = BTreeSet::new();
    for finding in findings {
        let code = string_at(finding, "code");
        match defaults
            .get(code)
            .copied()
            .unwrap_or_else(|| string_at(finding, "severity"))
        {
            "error" => {
                failures.insert(code);
            }
            "warning" => {
                warnings.insert(code);
            }
            _ => {}
        }
    }
    let status = if !failures.is_empty() {
        "fail"
    } else if !warnings.is_empty() {
        "warn"
    } else {
        "pass"
    };
    let mut effective = defaults
        .into_iter()
        .map(|(key, value)| (key.to_owned(), json!(value)))
        .collect::<Map<_, _>>();
    effective.insert("label_delta_threshold".into(), json!(0.1));
    effective.insert("media_delta_threshold".into(), json!(0.1));
    json!({
        "status": status,
        "failures": failures,
        "warnings": warnings,
        "effective_policy": effective,
    })
}
