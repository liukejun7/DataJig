use crate::inventory::{DatasetInventory, load_inventory};
use crate::io::save_file_atomically;
use crate::revision::{DatasetRevision, validate_content_id};
use crate::{ChangeDeclaration, DatasetChangeset, MAX_CHANGESET_BYTES, MAX_REPORT_BYTES};
use crate::{
    ConcurrentModificationError, OutputExistsError, RevisionContentCorruptError,
    RevisionContentUnavailableError, WorkspaceBusyError,
};
use crate::{
    JsonlQualityPolicy, JsonlRecordPage, JsonlRecordState, JsonlRecordStateBundle,
    MAX_JSONL_RECORD_PAGE_BYTES, MAX_JSONL_RECORD_STATE_BYTES,
};
use anyhow::{Context, Result, bail};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub struct WorkspaceLock {
    file: File,
}

impl Drop for WorkspaceLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

const REFS_SCHEMA_VERSION: u8 = 1;
const MAX_REFS_BYTES: usize = 64 * 1024;
const MAX_SELECTOR_OBJECTS: usize = 4_096;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceRefs {
    schema_version: u8,
    head: String,
}

impl WorkspaceRefs {
    pub fn new(head: String) -> Result<Self> {
        validate_content_id(&head, "rev", "HEAD")?;
        Ok(Self {
            schema_version: REFS_SCHEMA_VERSION,
            head,
        })
    }

    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_REFS_BYTES {
            bail!("workspace refs exceed {MAX_REFS_BYTES} bytes");
        }
        let value: Self = serde_json::from_str(payload).context("invalid workspace refs JSON")?;
        if value.schema_version != REFS_SCHEMA_VERSION {
            bail!("unsupported refs schema {}", value.schema_version);
        }
        validate_content_id(&value.head, "rev", "HEAD")?;
        Ok(value)
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(self)?)
    }

    pub fn head(&self) -> &str {
        &self.head
    }
}

pub struct WorkspaceStore {
    root: PathBuf,
}

pub(crate) struct BlobMaterialization {
    pub bytes: u64,
    pub status: &'static str,
}

impl WorkspaceStore {
    pub fn open(root: &Path) -> Result<Self> {
        let root = root
            .canonicalize()
            .context("cannot resolve workspace store")?;
        if !root.is_dir() {
            bail!("workspace store is not a directory");
        }
        Ok(Self { root })
    }

    pub fn lock_shared(&self) -> Result<WorkspaceLock> {
        self.lock_workspace(false)
    }

    pub fn lock_exclusive(&self) -> Result<WorkspaceLock> {
        self.lock_workspace(true)
    }

    fn lock_workspace(&self, exclusive: bool) -> Result<WorkspaceLock> {
        let path = self.root.join(".workspace.lock");
        #[cfg(unix)]
        let file = {
            use std::os::unix::fs::OpenOptionsExt;
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .mode(0o600)
                .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
                .open(&path)
                .context("cannot open workspace transaction lock")?
        };
        #[cfg(not(unix))]
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .context("cannot open workspace transaction lock")?;
        let metadata = file
            .metadata()
            .context("cannot inspect workspace transaction lock")?;
        if !metadata.is_file() {
            bail!("workspace transaction lock must be a direct regular file");
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if metadata.nlink() != 1 {
                bail!("workspace transaction lock must not be hard linked");
            }
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .context("cannot secure workspace transaction lock")?;
        }
        let result = if exclusive {
            FileExt::try_lock_exclusive(&file)
        } else {
            FileExt::try_lock_shared(&file)
        };
        match result {
            Ok(()) => Ok(WorkspaceLock { file }),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                Err(WorkspaceBusyError.into())
            }
            Err(error) => Err(error).context("cannot lock workspace transaction"),
        }
    }

    pub fn publish_inventory(&self, inventory: &DatasetInventory) -> Result<String> {
        let id = inventory.content_id()?;
        let path = self.object_path("inventories", &id, "inventory")?;
        publish_immutable(
            &path,
            inventory.to_json()?.as_bytes(),
            "workspace inventory",
        )?;
        Ok(id)
    }

    pub fn load_inventory(&self, id: &str) -> Result<DatasetInventory> {
        let path = self.object_path("inventories", id, "inventory")?;
        let value = load_inventory(&path).context("cannot load workspace inventory object")?;
        if value.content_id()? != id {
            bail!("inventory object identity does not match its filename");
        }
        Ok(value)
    }

    pub fn publish_record_state_bundle(&self, bundle: &JsonlRecordStateBundle) -> Result<String> {
        for page in bundle.pages() {
            let path = self.object_path("record-pages", page.page_id(), "recordpage")?;
            publish_immutable(
                &path,
                page.to_json()?.as_bytes(),
                "workspace JSONL record page",
            )?;
        }
        let state = bundle.state();
        let path = self.object_path("record-states", state.record_state_id(), "recordstate")?;
        publish_immutable(
            &path,
            state.to_json()?.as_bytes(),
            "workspace JSONL record state",
        )?;
        Ok(state.record_state_id().into())
    }

    pub fn load_record_state_bundle(&self, id: &str) -> Result<JsonlRecordStateBundle> {
        let path = self.object_path("record-states", id, "recordstate")?;
        let payload = load_bounded_text(
            &path,
            MAX_JSONL_RECORD_STATE_BYTES,
            "workspace JSONL record state",
        )?;
        let state = JsonlRecordState::from_json(&payload)?;
        if state.record_state_id() != id {
            bail!("record state identity does not match its filename");
        }
        let pages = state
            .page_ids()
            .iter()
            .map(|page_id| {
                let page_path = self.object_path("record-pages", page_id, "recordpage")?;
                let page_payload = load_bounded_text(
                    &page_path,
                    MAX_JSONL_RECORD_PAGE_BYTES,
                    "workspace JSONL record page",
                )?;
                let page = JsonlRecordPage::from_json(&page_payload)?;
                if page.page_id() != page_id {
                    bail!("record page identity does not match its filename");
                }
                Ok(page)
            })
            .collect::<Result<Vec<_>>>()?;
        JsonlRecordStateBundle::new(state, pages)
    }

    pub fn publish_jsonl_quality_policy(&self, policy: &JsonlQualityPolicy) -> Result<String> {
        let path = self.object_path("jsonl-policies", policy.policy_id(), "policy")?;
        publish_immutable(
            &path,
            policy.to_json()?.as_bytes(),
            "workspace JSONL quality policy",
        )?;
        Ok(policy.policy_id().into())
    }

    pub fn load_jsonl_quality_policy(&self, id: &str) -> Result<JsonlQualityPolicy> {
        let path = self.object_path("jsonl-policies", id, "policy")?;
        let policy = JsonlQualityPolicy::from_path(&path)?;
        if policy.policy_id() != id {
            bail!("quality policy identity does not match its filename");
        }
        Ok(policy)
    }

    pub fn publish_review(&self, payload: &[u8], id: &str) -> Result<()> {
        if payload.len() > MAX_REPORT_BYTES {
            bail!("workspace review exceeds {MAX_REPORT_BYTES} bytes");
        }
        validate_content_id(id, "review", "review ID")?;
        if crate::report::report_content_id(payload) != id {
            bail!("review identity does not match its content");
        }
        let path = self.object_path("reviews", id, "review")?;
        publish_immutable(&path, payload, "workspace accepted review")
    }

    pub fn load_review(&self, id: &str) -> Result<crate::ReviewReport> {
        let path = self.object_path("reviews", id, "review")?;
        let payload = load_bounded_bytes(&path, MAX_REPORT_BYTES, "workspace accepted review")?;
        if crate::report::report_content_id(&payload) != id {
            bail!("review object identity does not match its filename");
        }
        let payload = std::str::from_utf8(&payload)
            .context("workspace accepted review is not valid UTF-8")?;
        crate::ReviewReport::from_json(payload).context("cannot load workspace accepted review")
    }

    pub fn publish_revision(&self, revision: &DatasetRevision) -> Result<()> {
        let path = self.object_path("revisions", revision.revision_id(), "rev")?;
        publish_immutable(&path, revision.to_json()?.as_bytes(), "workspace revision")
    }

    pub fn publish_jsonl_blob(&self, source: &Path, expected_id: &str) -> Result<u64> {
        let target = self.jsonl_blob_path(expected_id)?;
        if target.exists() {
            return verify_recordset_file(&target, expected_id, true);
        }
        match copy_recordset_no_clobber(
            source,
            &target,
            expected_id,
            None,
            "JSONL revision blob",
            false,
        ) {
            Ok(result) => Ok(result.bytes),
            Err(error) if error.downcast_ref::<OutputExistsError>().is_some() => {
                verify_recordset_file(&target, expected_id, true)
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn materialize_jsonl_blob(
        &self,
        id: &str,
        expected_bytes: u64,
        output: &Path,
    ) -> Result<BlobMaterialization> {
        let source = self.jsonl_blob_path(id)?;
        if !source.exists() {
            return Err(RevisionContentUnavailableError.into());
        }
        copy_recordset_no_clobber(
            &source,
            output,
            id,
            Some(expected_bytes),
            "materialized revision",
            true,
        )
    }

    pub(crate) fn verified_jsonl_blob_path(
        &self,
        id: &str,
        expected_bytes: u64,
    ) -> Result<PathBuf> {
        let source = self.jsonl_blob_path(id)?;
        match fs::symlink_metadata(&source) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(RevisionContentUnavailableError.into());
            }
            Err(_) => return Err(RevisionContentCorruptError.into()),
        }
        let bytes = verify_recordset_file(&source, id, true)?;
        if bytes != expected_bytes {
            return Err(RevisionContentCorruptError.into());
        }
        Ok(source)
    }

    pub fn publish_change_declaration(&self, change: &ChangeDeclaration) -> Result<()> {
        let path = self.object_path("change-declarations", change.change_id(), "chg")?;
        publish_immutable(&path, change.to_json()?.as_bytes(), "change declaration")
    }

    pub fn load_change_declaration(&self, id: &str) -> Result<ChangeDeclaration> {
        let path = self.object_path("change-declarations", id, "chg")?;
        let payload = load_bounded_text(&path, MAX_CHANGESET_BYTES, "change declaration")?;
        let value = ChangeDeclaration::from_json(&payload)?;
        if value.change_id() != id {
            bail!("change declaration identity does not match its filename");
        }
        Ok(value)
    }

    pub(crate) fn list_change_declarations(&self) -> Result<Vec<ChangeDeclaration>> {
        self.list_object_ids("change-declarations", "chg")?
            .into_iter()
            .map(|id| self.load_change_declaration(&id))
            .collect()
    }

    pub fn publish_changeset(&self, changeset: &DatasetChangeset) -> Result<()> {
        let path = self.object_path("changesets", changeset.changeset_id(), "changeset")?;
        publish_immutable(&path, changeset.to_json()?.as_bytes(), "dataset changeset")
    }

    pub(crate) fn publish_patch_receipt(&self, id: &str, payload: &[u8]) -> Result<()> {
        validate_content_id(id, "apply", "patch apply ID")?;
        if payload.len() > MAX_CHANGESET_BYTES {
            bail!("patch apply receipt exceeds {MAX_CHANGESET_BYTES} bytes");
        }
        let path = self.object_path("patch-applies", id, "apply")?;
        publish_immutable(&path, payload, "patch apply receipt")
    }

    pub(crate) fn publish_patch_undo_receipt(&self, id: &str, payload: &[u8]) -> Result<()> {
        validate_content_id(id, "revert", "patch undo receipt ID")?;
        if payload.len() > MAX_CHANGESET_BYTES {
            bail!("patch undo receipt exceeds {MAX_CHANGESET_BYTES} bytes");
        }
        let path = self.object_path("patch-undos", id, "revert")?;
        publish_immutable(&path, payload, "patch undo receipt")
    }

    pub fn load_changeset(&self, id: &str) -> Result<DatasetChangeset> {
        let path = self.object_path("changesets", id, "changeset")?;
        let payload = load_bounded_text(&path, MAX_CHANGESET_BYTES, "dataset changeset")?;
        let value = DatasetChangeset::from_json(&payload)?;
        if value.changeset_id() != id {
            bail!("dataset changeset identity does not match its filename");
        }
        Ok(value)
    }

    pub(crate) fn list_changesets(&self) -> Result<Vec<DatasetChangeset>> {
        self.list_object_ids("changesets", "changeset")?
            .into_iter()
            .map(|id| self.load_changeset(&id))
            .collect()
    }

    pub fn load_revision(&self, id: &str) -> Result<DatasetRevision> {
        let path = self.object_path("revisions", id, "rev")?;
        let metadata = fs::metadata(&path).context("cannot inspect workspace revision object")?;
        if metadata.len() > crate::MAX_REVISION_BYTES as u64 {
            bail!("revision object is too large");
        }
        let payload = fs::read_to_string(&path).context("cannot read workspace revision object")?;
        let revision = DatasetRevision::from_json(&payload)?;
        if revision.revision_id() != id {
            bail!("revision object identity does not match its filename");
        }
        Ok(revision)
    }

    pub fn load_refs(&self) -> Result<WorkspaceRefs> {
        let path = self.root.join("refs.json");
        let metadata = fs::metadata(&path).context("cannot inspect workspace refs")?;
        if metadata.len() > MAX_REFS_BYTES as u64 {
            bail!("workspace refs exceed {MAX_REFS_BYTES} bytes");
        }
        let payload = fs::read_to_string(path).context("cannot read workspace refs")?;
        WorkspaceRefs::from_json(&payload)
    }

    pub fn compare_and_swap_head(&self, expected: Option<&str>, new: &str) -> Result<()> {
        validate_content_id(new, "rev", "new HEAD")?;
        self.load_revision(new)
            .context("new HEAD does not name a valid revision")?;
        if let Some(expected) = expected {
            validate_content_id(expected, "rev", "expected HEAD")?;
        }
        let lock_path = self.root.join(".refs.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .context("cannot open workspace refs lock")?;
        lock.lock_exclusive()
            .context("cannot lock workspace refs")?;
        let refs_path = self.root.join("refs.json");
        let current = if refs_path.exists() {
            Some(self.load_refs()?)
        } else {
            None
        };
        let current_id = current.as_ref().map(WorkspaceRefs::head);
        if current_id != expected {
            return Err(ConcurrentModificationError::new(format!(
                "workspace HEAD changed; expected {expected:?}, found {current_id:?}"
            ))
            .into());
        }
        let refs = WorkspaceRefs::new(new.to_owned())?;
        save_file_atomically(&refs_path, refs.to_json()?.as_bytes(), "workspace refs")?;
        FileExt::unlock(&lock).context("cannot unlock workspace refs")?;
        Ok(())
    }

    pub(crate) fn inventory_path(&self, id: &str) -> Result<PathBuf> {
        self.object_path("inventories", id, "inventory")
    }

    fn jsonl_blob_path(&self, id: &str) -> Result<PathBuf> {
        validate_content_id(id, "records", "dataset content ID")?;
        Ok(self
            .root
            .join("objects")
            .join("jsonl-blobs")
            .join(format!("{id}.jsonl")))
    }

    fn object_path(&self, kind: &str, id: &str, prefix: &str) -> Result<PathBuf> {
        validate_content_id(id, prefix, "workspace object id")?;
        Ok(self
            .root
            .join("objects")
            .join(kind)
            .join(format!("{id}.json")))
    }

    fn list_object_ids(&self, kind: &str, prefix: &str) -> Result<Vec<String>> {
        let directory = self.root.join("objects").join(kind);
        let mut ids = Vec::new();
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(ids),
            Err(error) => {
                return Err(error).with_context(|| format!("cannot list workspace {kind} objects"));
            }
        };
        for entry in entries {
            let entry = entry.with_context(|| format!("cannot inspect workspace {kind} object"))?;
            let file_type = entry
                .file_type()
                .with_context(|| format!("cannot inspect workspace {kind} object type"))?;
            if !file_type.is_file() || file_type.is_symlink() {
                bail!("workspace {kind} objects must be direct regular files");
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("workspace {kind} object name is not valid UTF-8"))?;
            let id = name
                .strip_suffix(".json")
                .with_context(|| format!("workspace {kind} object name is invalid"))?;
            validate_content_id(id, prefix, "workspace object id")?;
            ids.push(id.to_owned());
            if ids.len() > MAX_SELECTOR_OBJECTS {
                bail!("workspace {kind} object count exceeds {MAX_SELECTOR_OBJECTS}");
            }
        }
        ids.sort();
        Ok(ids)
    }
}

#[cfg(unix)]
fn open_direct_file(path: &Path, artifact: &str, require_single_link: bool) -> Result<File> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = OpenOptions::new()
        .read(true)
        .custom_flags((rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK).bits() as i32)
        .open(path)
        .with_context(|| format!("cannot open {artifact}"))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("cannot inspect {artifact}"))?;
    if !metadata.is_file() || (require_single_link && metadata.nlink() != 1) {
        bail!("{artifact} must be a direct, unlinked regular file");
    }
    Ok(file)
}

#[cfg(not(unix))]
fn open_direct_file(path: &Path, artifact: &str, _require_single_link: bool) -> Result<File> {
    let file = File::open(path).with_context(|| format!("cannot open {artifact}"))?;
    if !file.metadata()?.is_file() {
        bail!("{artifact} must be a regular file");
    }
    Ok(file)
}

fn verify_recordset_file(path: &Path, expected_id: &str, single_link: bool) -> Result<u64> {
    let mut file = open_direct_file(path, "JSONL revision blob", single_link)
        .map_err(|_| RevisionContentCorruptError)?;
    let (actual_id, bytes) = stream_recordset(&mut file, None)?;
    if actual_id != expected_id {
        return Err(RevisionContentCorruptError.into());
    }
    Ok(bytes)
}

fn stream_recordset(source: &mut File, mut output: Option<&mut File>) -> Result<(String, u64)> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"datajig-jsonl-recordset-v1\0");
    let mut bytes = 0_u64;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = source.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        if let Some(writer) = output.as_deref_mut() {
            writer.write_all(&buffer[..read])?;
        }
        bytes = bytes
            .checked_add(read as u64)
            .context("JSONL revision byte count overflow")?;
    }
    Ok((format!("records_{}", hasher.finalize().to_hex()), bytes))
}

#[cfg(unix)]
fn copy_recordset_no_clobber(
    source: &Path,
    output: &Path,
    expected_id: &str,
    expected_bytes: Option<u64>,
    artifact: &str,
    source_single_link: bool,
) -> Result<BlobMaterialization> {
    use std::os::unix::fs::OpenOptionsExt;
    validate_content_id(expected_id, "records", "dataset content ID")?;
    if output.exists() || fs::symlink_metadata(output).is_ok() {
        let bytes =
            verify_recordset_file(output, expected_id, true).map_err(|_| OutputExistsError)?;
        if expected_bytes.is_some_and(|expected| expected != bytes) {
            return Err(RevisionContentCorruptError.into());
        }
        return Ok(BlobMaterialization {
            bytes,
            status: "already_present",
        });
    }
    let parent = output
        .parent()
        .context("materialized output has no parent")?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    fs::create_dir_all(parent).with_context(|| format!("cannot create {artifact} directory"))?;
    let name = output
        .file_name()
        .context("materialized output has no file name")?;
    let mut source = open_direct_file(source, artifact, source_single_link).map_err(|error| {
        if source_single_link {
            anyhow::Error::new(RevisionContentCorruptError)
        } else {
            error
        }
    })?;
    let mut temporary = None;
    for attempt in 0..100_u32 {
        let path = parent.join(format!(
            ".{}.{}.{}.tmp",
            name.to_string_lossy(),
            std::process::id(),
            attempt
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(file) => {
                temporary = Some((path, file));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).with_context(|| format!("cannot create {artifact}")),
        }
    }
    let (temporary_path, mut temporary_file) =
        temporary.with_context(|| format!("cannot allocate temporary {artifact}"))?;
    let result = (|| -> Result<BlobMaterialization> {
        let (actual_id, bytes) = stream_recordset(&mut source, Some(&mut temporary_file))?;
        if actual_id != expected_id {
            return Err(RevisionContentCorruptError.into());
        }
        if expected_bytes.is_some_and(|expected| expected != bytes) {
            return Err(RevisionContentCorruptError.into());
        }
        temporary_file.sync_all()?;
        drop(temporary_file);
        fs::hard_link(&temporary_path, output).map_err(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                anyhow::Error::new(OutputExistsError)
            } else {
                anyhow::Error::new(error).context(format!("cannot publish {artifact}"))
            }
        })?;
        fs::remove_file(&temporary_path)?;
        File::open(parent)?.sync_all()?;
        Ok(BlobMaterialization {
            bytes,
            status: "created",
        })
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result
}

#[cfg(not(unix))]
fn copy_recordset_no_clobber(
    _source: &Path,
    _output: &Path,
    _expected_id: &str,
    _expected_bytes: Option<u64>,
    artifact: &str,
    _source_single_link: bool,
) -> Result<BlobMaterialization> {
    bail!("native {artifact} publication currently requires Unix")
}

fn load_bounded_text(path: &Path, limit: usize, artifact: &str) -> Result<String> {
    let payload = load_bounded_bytes(path, limit, artifact)?;
    String::from_utf8(payload).with_context(|| format!("{artifact} is not valid UTF-8"))
}

fn load_bounded_bytes(path: &Path, limit: usize, artifact: &str) -> Result<Vec<u8>> {
    let path_metadata =
        fs::symlink_metadata(path).with_context(|| format!("cannot inspect {artifact} path"))?;
    if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
        bail!("{artifact} must be a direct regular file");
    }
    let file = File::open(path).with_context(|| format!("cannot open {artifact}"))?;
    let metadata = file
        .metadata()
        .with_context(|| format!("cannot inspect {artifact}"))?;
    if !metadata.is_file() {
        bail!("{artifact} must be a regular file");
    }
    if metadata.len() > limit as u64 {
        bail!("{artifact} exceeds {limit} bytes");
    }
    let mut payload = Vec::with_capacity(metadata.len() as usize);
    file.take((limit + 1) as u64)
        .read_to_end(&mut payload)
        .with_context(|| format!("cannot read {artifact}"))?;
    if payload.len() > limit {
        bail!("{artifact} exceeds {limit} bytes");
    }
    Ok(payload)
}

fn publish_immutable(path: &Path, payload: &[u8], artifact: &str) -> Result<()> {
    if path.exists() {
        let existing =
            fs::read(path).with_context(|| format!("cannot read existing {artifact}"))?;
        if existing != payload {
            bail!("existing {artifact} has different content");
        }
        return Ok(());
    }
    save_file_atomically(path, payload, artifact)
}
