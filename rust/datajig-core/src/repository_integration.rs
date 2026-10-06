use crate::MAX_PATH_BYTES;
use crate::agent_protocol::{AGENT_API_VERSION, agent_contract_id, render_agent_skill};
use crate::identity::{ARTIFACT_NAMESPACE, blake3_content_id};
use crate::io::{
    save_executable_file_atomically, save_file_atomically, save_new_executable_file_atomically,
    save_new_file_atomically,
};
use crate::status_workspace;
use crate::strict_json::reject_duplicate_json_members;
use crate::workspace::workspace_dataset_path;
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::io::Read;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Output};
use std::{error::Error, fmt, fs};

pub const REPOSITORY_INTEGRATION_SCHEMA_VERSION: u8 = 1;
const MAX_REPOSITORY_LOCK_BYTES: usize = 1024 * 1024;
const MAX_REPOSITORY_STATES: usize = 64;
const SKILL_PATH: &str = ".agents/skills/datajig/SKILL.md";
const WORKFLOW_PATH: &str = ".github/workflows/datajig.yml";
const HOOK_PATH: &str = ".githooks/pre-commit";
const HOOKS_PATH_VALUE: &str = ".githooks";
const LOCK_PATH: &str = ".datajig-repository.json";
const MAX_GIT_OUTPUT_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub struct RepositoryConflictError;

impl fmt::Display for RepositoryConflictError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("repository integration conflicts with existing state")
    }
}

impl Error for RepositoryConflictError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RepositoryComponents {
    pub hook: bool,
    pub github_actions: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LockComponents {
    agent_skill: bool,
    github_actions: bool,
    hook: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManagedFile {
    path: String,
    content_id: String,
    bytes: usize,
    executable: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryIntegrationLock {
    namespace: String,
    kind: String,
    schema_version: u8,
    repository_integration_id: String,
    datajig_version: String,
    agent_api_version: u8,
    agent_contract_id: String,
    components: LockComponents,
    states: Vec<String>,
    managed_files: Vec<ManagedFile>,
    hooks_path: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RepositoryInstallArtifact {
    pub repository_integration_id: String,
    pub agent_contract_id: String,
    pub decision: String,
    pub root: String,
    pub states: usize,
    pub managed_files: usize,
    pub hook_active: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct RepositoryStateCheck {
    pub state: String,
    pub clean: bool,
    pub dataset_id: String,
    pub head_revision_id: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct RepositoryCheckArtifact {
    pub repository_integration_id: String,
    pub agent_contract_id: String,
    pub decision: String,
    pub root: String,
    pub states: Vec<RepositoryStateCheck>,
    pub managed_files: usize,
    pub hook_active: bool,
    pub ci: bool,
}

struct DesiredManagedFile {
    path: &'static str,
    payload: Vec<u8>,
    executable: bool,
}

struct DesiredRepositoryInstallation {
    lock: RepositoryIntegrationLock,
    files: Vec<DesiredManagedFile>,
}

#[derive(Serialize)]
struct RepositoryIntegrationIdentity<'a> {
    namespace: &'a str,
    kind: &'a str,
    schema_version: u8,
    datajig_version: &'a str,
    agent_api_version: u8,
    agent_contract_id: &'a str,
    components: &'a LockComponents,
    states: &'a [String],
    managed_files: &'a [ManagedFile],
    hooks_path: &'a Option<String>,
}

impl RepositoryIntegrationLock {
    pub fn from_json(payload: &str) -> Result<Self> {
        if payload.len() > MAX_REPOSITORY_LOCK_BYTES {
            bail!("repository integration lock exceeds {MAX_REPOSITORY_LOCK_BYTES} bytes");
        }
        reject_duplicate_json_members(payload).context("invalid repository integration lock")?;
        let lock: Self =
            serde_json::from_str(payload).context("invalid repository integration lock")?;
        lock.validate()?;
        Ok(lock)
    }

    pub fn to_json(&self) -> Result<String> {
        self.validate()?;
        let mut payload = serde_json::to_string_pretty(self)?;
        payload.push('\n');
        if payload.len() > MAX_REPOSITORY_LOCK_BYTES {
            bail!("repository integration lock exceeds {MAX_REPOSITORY_LOCK_BYTES} bytes");
        }
        Ok(payload)
    }

    pub fn integration_id(&self) -> &str {
        &self.repository_integration_id
    }

    fn identity(&self) -> RepositoryIntegrationIdentity<'_> {
        RepositoryIntegrationIdentity {
            namespace: &self.namespace,
            kind: &self.kind,
            schema_version: self.schema_version,
            datajig_version: &self.datajig_version,
            agent_api_version: self.agent_api_version,
            agent_contract_id: &self.agent_contract_id,
            components: &self.components,
            states: &self.states,
            managed_files: &self.managed_files,
            hooks_path: &self.hooks_path,
        }
    }

    fn compute_id(&self) -> Result<String> {
        Ok(blake3_content_id(
            "repo",
            b"datajig-repository-integration-v1\0",
            &serde_json::to_vec(&self.identity())?,
        ))
    }

    fn validate(&self) -> Result<()> {
        if self.namespace != ARTIFACT_NAMESPACE
            || self.kind != "repository_integration"
            || self.schema_version != REPOSITORY_INTEGRATION_SCHEMA_VERSION
        {
            bail!("unsupported repository integration identity or schema");
        }
        if self.datajig_version.is_empty()
            || self.agent_api_version == 0
            || !valid_content_id(&self.agent_contract_id, "contract")
        {
            bail!("repository integration version or contract is invalid");
        }
        if !self.components.agent_skill {
            bail!("repository integration must include the Agent Skill");
        }
        validate_states(&self.states)?;
        let expected_paths = managed_paths(self.components.hook, self.components.github_actions);
        let actual_paths = self
            .managed_files
            .iter()
            .map(|file| file.path.as_str())
            .collect::<Vec<_>>();
        if actual_paths != expected_paths {
            bail!("repository integration managed paths are invalid");
        }
        for file in &self.managed_files {
            if file.bytes == 0
                || !valid_content_id(&file.content_id, "managed")
                || file.executable != (file.path == HOOK_PATH)
            {
                bail!("repository integration managed file is invalid");
            }
        }
        let expected_hooks_path = self.components.hook.then(|| HOOKS_PATH_VALUE.to_owned());
        if self.hooks_path != expected_hooks_path {
            bail!("repository integration hook path is invalid");
        }
        if self.repository_integration_id != self.compute_id()? {
            bail!("repository integration identity does not match its content");
        }
        Ok(())
    }
}

pub fn build_repository_integration_lock(
    states: &[PathBuf],
    components: RepositoryComponents,
) -> Result<RepositoryIntegrationLock> {
    Ok(desired_repository_installation(states, components)?.lock)
}

pub(crate) fn repository_integration_schema_example() -> RepositoryIntegrationLock {
    let mut lock = RepositoryIntegrationLock {
        namespace: ARTIFACT_NAMESPACE.into(),
        kind: "repository_integration".into(),
        schema_version: REPOSITORY_INTEGRATION_SCHEMA_VERSION,
        repository_integration_id: String::new(),
        datajig_version: python_package_version(),
        agent_api_version: AGENT_API_VERSION,
        agent_contract_id: format!("contract_{}", "0".repeat(64)),
        components: LockComponents {
            agent_skill: true,
            github_actions: true,
            hook: true,
        },
        states: vec![".datajig".into()],
        managed_files: vec![
            ManagedFile {
                path: SKILL_PATH.into(),
                content_id: format!("managed_{}", "0".repeat(64)),
                bytes: 1,
                executable: false,
            },
            ManagedFile {
                path: WORKFLOW_PATH.into(),
                content_id: format!("managed_{}", "1".repeat(64)),
                bytes: 1,
                executable: false,
            },
            ManagedFile {
                path: HOOK_PATH.into(),
                content_id: format!("managed_{}", "2".repeat(64)),
                bytes: 1,
                executable: true,
            },
        ],
        hooks_path: Some(HOOKS_PATH_VALUE.into()),
    };
    lock.repository_integration_id = lock
        .compute_id()
        .expect("the repository integration schema example must serialize");
    lock
}

pub fn install_repository(
    root: &Path,
    states: &[PathBuf],
    components: RepositoryComponents,
) -> Result<RepositoryInstallArtifact> {
    let root = resolve_git_root(root)?;
    let desired = desired_repository_installation(states, components)?;
    if !components.github_actions {
        require_managed_path_absent(&root, WORKFLOW_PATH)?;
    }
    if !components.hook {
        require_managed_path_absent(&root, HOOK_PATH)?;
    }
    let lock_path = root.join(LOCK_PATH);
    let existing_lock = match fs::symlink_metadata(&lock_path) {
        Ok(_) => {
            let payload = read_regular(&lock_path, MAX_REPOSITORY_LOCK_BYTES)?;
            let payload = std::str::from_utf8(&payload)
                .context("repository integration lock must be valid UTF-8")?;
            Some(RepositoryIntegrationLock::from_json(payload)?)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => return Err(error).context("cannot inspect repository integration lock"),
    };
    if let Some(lock) = &existing_lock {
        validate_monotonic_upgrade(lock, &desired.lock)?;
        preflight_managed_upgrade(&root, lock, &desired)?;
    } else {
        preflight_managed_paths(&root, &desired.files, true)?;
    }
    preflight_hook_config(&root, components.hook)?;

    for file in &desired.files {
        let path = root.join(file.path);
        if desired_file_matches(&path, file)? {
            continue;
        }
        create_managed_parents(&root, Path::new(file.path))?;
        if fs::symlink_metadata(&path).is_ok() && file.executable {
            save_executable_file_atomically(&path, &file.payload, "repository integration file")?;
        } else if fs::symlink_metadata(&path).is_ok() {
            save_file_atomically(&path, &file.payload, "repository integration file")?;
        } else if file.executable {
            save_new_executable_file_atomically(
                &path,
                &file.payload,
                "repository integration file",
            )?;
        } else {
            save_new_file_atomically(&path, &file.payload, "repository integration file")?;
        }
    }
    repository_test_failpoint("after_assets");
    if components.hook {
        set_hook_config(&root)?;
    }
    repository_test_failpoint("after_hook_config");
    let lock_payload = desired.lock.to_json()?;
    if existing_lock.is_none() {
        save_new_file_atomically(
            &lock_path,
            lock_payload.as_bytes(),
            "repository integration lock",
        )?;
    } else if existing_lock.as_ref().is_some_and(|lock| {
        lock.repository_integration_id != desired.lock.repository_integration_id
    }) {
        save_file_atomically(
            &lock_path,
            lock_payload.as_bytes(),
            "repository integration lock",
        )?;
    }

    let decision = match &existing_lock {
        None => "installed",
        Some(lock) if lock.repository_integration_id == desired.lock.repository_integration_id => {
            "unchanged"
        }
        Some(_) => "updated",
    };
    Ok(RepositoryInstallArtifact {
        repository_integration_id: desired.lock.repository_integration_id.clone(),
        agent_contract_id: desired.lock.agent_contract_id.clone(),
        decision: decision.into(),
        root: root.to_string_lossy().into_owned(),
        states: desired.lock.states.len(),
        managed_files: desired.files.len(),
        hook_active: components.hook,
    })
}

pub fn check_repository(root: &Path, ci: bool) -> Result<RepositoryCheckArtifact> {
    let root = resolve_git_root(root)?;
    let lock_path = root.join(LOCK_PATH);
    let payload = read_regular(&lock_path, MAX_REPOSITORY_LOCK_BYTES)
        .context("cannot read repository integration lock")?;
    let payload =
        std::str::from_utf8(&payload).context("repository integration lock must be valid UTF-8")?;
    let lock = RepositoryIntegrationLock::from_json(payload)?;
    let components = RepositoryComponents {
        hook: lock.components.hook,
        github_actions: lock.components.github_actions,
    };
    let state_paths = lock.states.iter().map(PathBuf::from).collect::<Vec<_>>();
    let desired = desired_repository_installation(&state_paths, components)?;
    if lock.repository_integration_id != desired.lock.repository_integration_id {
        return Err(RepositoryConflictError.into());
    }
    preflight_managed_paths(&root, &desired.files, false)?;
    if !components.github_actions {
        require_managed_path_absent(&root, WORKFLOW_PATH)?;
    }
    if !components.hook {
        require_managed_path_absent(&root, HOOK_PATH)?;
    }
    if !ci && components.hook {
        verify_hook_config(&root)?;
    }

    let mut states = Vec::with_capacity(lock.states.len());
    for state in &lock.states {
        let relative = Path::new(state);
        validate_existing_parents(&root, &relative.join(".datajig-check"))?;
        let state_path = root.join(relative);
        let canonical = state_path
            .canonicalize()
            .context("cannot resolve bound repository workspace")?;
        if !canonical.starts_with(&root) || !canonical.is_dir() {
            return Err(RepositoryConflictError.into());
        }
        let status =
            status_workspace(&canonical, 1).context("cannot verify bound repository workspace")?;
        if !status.clean || status.decision != "clean" || status.unstaged_changes == Some(true) {
            bail!("bound repository workspace is not clean");
        }
        verify_dataset_index_matches_worktree(&root, &workspace_dataset_path(&canonical)?)?;
        states.push(RepositoryStateCheck {
            state: state.clone(),
            clean: true,
            dataset_id: status.dataset_id,
            head_revision_id: status.head_revision_id,
        });
    }

    Ok(RepositoryCheckArtifact {
        repository_integration_id: lock.repository_integration_id,
        agent_contract_id: lock.agent_contract_id,
        decision: "ready".into(),
        root: root.to_string_lossy().into_owned(),
        states,
        managed_files: lock.managed_files.len(),
        hook_active: components.hook,
        ci,
    })
}

fn verify_dataset_index_matches_worktree(root: &Path, dataset: &Path) -> Result<()> {
    let dataset = dataset
        .canonicalize()
        .context("cannot resolve bound workspace dataset")?;
    let Ok(relative) = dataset.strip_prefix(root) else {
        return Ok(());
    };
    let head = run_git(root, &["rev-parse", "--verify", "HEAD"])?;
    let _ = bounded_stdout(&head)?;
    if head.status.success() {
        let staged_deletions = Command::new("git")
            .arg("-C")
            .arg(root)
            .args([
                "diff",
                "--cached",
                "--diff-filter=D",
                "--quiet",
                "HEAD",
                "--",
            ])
            .arg(relative)
            .output()
            .context("cannot start Git")?;
        let _ = bounded_stdout(&staged_deletions)?;
        match staged_deletions.status.code() {
            Some(0) => {}
            Some(1) => bail!("bound repository dataset contains staged deletions"),
            _ => bail!("cannot inspect staged bound repository dataset deletions"),
        }
    } else if !matches!(head.status.code(), Some(1 | 128)) {
        bail!("cannot resolve Git HEAD while verifying a bound repository dataset");
    }
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(["diff", "--quiet", "--"])
        .arg(relative)
        .output()
        .context("cannot start Git")?;
    let _ = bounded_stdout(&output)?;
    match output.status.code() {
        Some(0) => Ok(()),
        Some(1) => bail!("bound repository dataset differs between the Git index and worktree"),
        _ => bail!("cannot compare the bound repository dataset with the Git index"),
    }
}

fn require_managed_path_absent(root: &Path, relative: &str) -> Result<()> {
    validate_existing_parents(root, Path::new(relative))?;
    match fs::symlink_metadata(root.join(relative)) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(RepositoryConflictError.into()),
        Err(error) => Err(error).context("cannot inspect inactive repository integration path"),
    }
}

fn validate_monotonic_upgrade(
    current: &RepositoryIntegrationLock,
    desired: &RepositoryIntegrationLock,
) -> Result<()> {
    if (current.components.hook && !desired.components.hook)
        || (current.components.github_actions && !desired.components.github_actions)
        || current
            .states
            .iter()
            .any(|state| desired.states.binary_search(state).is_err())
    {
        return Err(RepositoryConflictError.into());
    }
    Ok(())
}

fn preflight_managed_upgrade(
    root: &Path,
    current: &RepositoryIntegrationLock,
    desired: &DesiredRepositoryInstallation,
) -> Result<()> {
    for old in &current.managed_files {
        validate_existing_parents(root, Path::new(&old.path))?;
        let path = root.join(&old.path);
        let desired_file = desired.files.iter().find(|file| file.path == old.path);
        let matches_desired = match desired_file {
            Some(file) => desired_file_is_exact_or_recoverable(&path, file)?,
            None => false,
        };
        if managed_file_matches(&path, old)? || matches_desired {
            continue;
        }
        return Err(RepositoryConflictError.into());
    }
    for file in &desired.files {
        if current
            .managed_files
            .iter()
            .any(|old| old.path == file.path)
        {
            continue;
        }
        validate_existing_parents(root, Path::new(file.path))?;
        let path = root.join(file.path);
        match fs::symlink_metadata(&path) {
            Ok(_) if desired_file_is_exact_or_recoverable(&path, file)? => {}
            Ok(_) => return Err(RepositoryConflictError.into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("cannot inspect repository integration file"),
        }
    }
    Ok(())
}

fn managed_file_matches(path: &Path, expected: &ManagedFile) -> Result<bool> {
    let payload = match read_regular(path, expected.bytes) {
        Ok(payload) => payload,
        Err(error) if error.downcast_ref::<RepositoryConflictError>().is_some() => {
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    Ok(payload.len() == expected.bytes
        && blake3_content_id("managed", b"datajig-repository-managed-file-v1\0", &payload)
            == expected.content_id
        && is_executable(path)? == expected.executable)
}

fn desired_file_matches(path: &Path, expected: &DesiredManagedFile) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            let payload = match read_regular(path, expected.payload.len()) {
                Ok(payload) => payload,
                Err(error) if error.downcast_ref::<RepositoryConflictError>().is_some() => {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };
            Ok(payload == expected.payload && is_executable(path)? == expected.executable)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).context("cannot inspect repository integration file"),
    }
}

fn desired_file_is_exact_or_recoverable(
    path: &Path,
    expected: &DesiredManagedFile,
) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => {
            let payload = match read_regular(path, expected.payload.len()) {
                Ok(payload) => payload,
                Err(error) if error.downcast_ref::<RepositoryConflictError>().is_some() => {
                    return Ok(false);
                }
                Err(error) => return Err(error),
            };
            if payload != expected.payload {
                return Ok(false);
            }
            let actual_executable = is_executable(path)?;
            Ok(expected.executable || !actual_executable)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error).context("cannot inspect repository integration file"),
    }
}

fn desired_repository_installation(
    states: &[PathBuf],
    components: RepositoryComponents,
) -> Result<DesiredRepositoryInstallation> {
    let states = normalize_states(states)?;
    let mut files = Vec::new();
    files.push(DesiredManagedFile {
        path: SKILL_PATH,
        payload: render_agent_skill().into_bytes(),
        executable: false,
    });
    if components.github_actions {
        files.push(DesiredManagedFile {
            path: WORKFLOW_PATH,
            payload: render_repository_workflow(),
            executable: false,
        });
    }
    if components.hook {
        files.push(DesiredManagedFile {
            path: HOOK_PATH,
            payload: render_repository_hook(),
            executable: true,
        });
    }
    let managed_files = files
        .iter()
        .map(|file| managed_file(file.path, file.payload.clone(), file.executable))
        .collect();
    let mut lock = RepositoryIntegrationLock {
        namespace: ARTIFACT_NAMESPACE.into(),
        kind: "repository_integration".into(),
        schema_version: REPOSITORY_INTEGRATION_SCHEMA_VERSION,
        repository_integration_id: String::new(),
        datajig_version: python_package_version(),
        agent_api_version: AGENT_API_VERSION,
        agent_contract_id: agent_contract_id(),
        components: LockComponents {
            agent_skill: true,
            github_actions: components.github_actions,
            hook: components.hook,
        },
        states,
        managed_files,
        hooks_path: components.hook.then(|| HOOKS_PATH_VALUE.into()),
    };
    lock.repository_integration_id = lock.compute_id()?;
    lock.validate()?;
    Ok(DesiredRepositoryInstallation { lock, files })
}

fn resolve_git_root(root: &Path) -> Result<PathBuf> {
    let metadata = fs::symlink_metadata(root).context("cannot inspect repository root")?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        bail!("repository root must be a real directory");
    }
    let root = root
        .canonicalize()
        .context("cannot resolve repository root")?;
    let inside = run_git(&root, &["rev-parse", "--is-inside-work-tree"])?;
    if !inside.status.success() || bounded_stdout(&inside)? != "true" {
        bail!("repository root must belong to a non-bare Git worktree");
    }
    let top = run_git(&root, &["rev-parse", "--show-toplevel"])?;
    if !top.status.success() {
        bail!("cannot resolve Git worktree root");
    }
    PathBuf::from(bounded_stdout(&top)?)
        .canonicalize()
        .context("cannot resolve Git worktree root")
}

fn preflight_managed_paths(
    root: &Path,
    files: &[DesiredManagedFile],
    recover_missing_executable: bool,
) -> Result<()> {
    for file in files {
        validate_existing_parents(root, Path::new(file.path))?;
        let path = root.join(file.path);
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                let actual = read_regular(&path, file.payload.len())?;
                if actual != file.payload {
                    return Err(RepositoryConflictError.into());
                }
                let actual_executable = is_executable(&path)?;
                let mode_is_recoverable =
                    recover_missing_executable && file.executable && !actual_executable;
                if file.executable != actual_executable && !mode_is_recoverable {
                    return Err(RepositoryConflictError.into());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).context("cannot inspect repository integration file");
            }
        }
    }
    Ok(())
}

fn validate_existing_parents(root: &Path, relative: &Path) -> Result<()> {
    let mut current = root.to_path_buf();
    let parent = relative
        .parent()
        .context("repository integration path has no parent")?;
    for component in parent.components() {
        let Component::Normal(name) = component else {
            bail!("repository integration path is invalid");
        };
        current.push(name);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => return Err(RepositoryConflictError.into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error).context("cannot inspect managed parent directory"),
        }
    }
    Ok(())
}

fn create_managed_parents(root: &Path, relative: &Path) -> Result<()> {
    let mut current = root.to_path_buf();
    let parent = relative
        .parent()
        .context("repository integration path has no parent")?;
    for component in parent.components() {
        let Component::Normal(name) = component else {
            bail!("repository integration path is invalid");
        };
        current.push(name);
        match fs::create_dir(&current) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&current)?;
                if !metadata.is_dir() || metadata.file_type().is_symlink() {
                    return Err(RepositoryConflictError.into());
                }
            }
            Err(error) => return Err(error).context("cannot create managed parent directory"),
        }
    }
    Ok(())
}

fn preflight_hook_config(root: &Path, enabled: bool) -> Result<()> {
    if !enabled {
        return Ok(());
    }
    let output = run_git(root, &["config", "--local", "--get", "core.hooksPath"])?;
    match output.status.code() {
        Some(0) if bounded_stdout(&output)? == HOOKS_PATH_VALUE => Ok(()),
        Some(1) => Ok(()),
        _ => Err(RepositoryConflictError.into()),
    }
}

fn verify_hook_config(root: &Path) -> Result<()> {
    let output = run_git(root, &["config", "--local", "--get", "core.hooksPath"])?;
    if output.status.code() != Some(0) || bounded_stdout(&output)? != HOOKS_PATH_VALUE {
        return Err(RepositoryConflictError.into());
    }
    Ok(())
}

fn set_hook_config(root: &Path) -> Result<()> {
    let output = run_git(
        root,
        &["config", "--local", "core.hooksPath", HOOKS_PATH_VALUE],
    )?;
    if !output.status.success() {
        bail!("cannot activate repository-managed Git hooks");
    }
    let _ = bounded_stdout(&output)?;
    Ok(())
}

#[cfg(debug_assertions)]
fn repository_test_failpoint(name: &str) {
    if std::env::var("DATAJIG_TEST_REPOSITORY_FAILPOINT").as_deref() == Ok(name) {
        panic!("repository install failpoint: {name}");
    }
}

#[cfg(not(debug_assertions))]
fn repository_test_failpoint(_name: &str) {}

fn run_git(root: &Path, arguments: &[&str]) -> Result<Output> {
    Command::new("git")
        .arg("-C")
        .arg(root)
        .args(arguments)
        .output()
        .context("cannot start Git")
}

fn bounded_stdout(output: &Output) -> Result<String> {
    if output.stdout.len() > MAX_GIT_OUTPUT_BYTES || output.stderr.len() > MAX_GIT_OUTPUT_BYTES {
        bail!("Git output exceeds its size limit");
    }
    Ok(std::str::from_utf8(&output.stdout)
        .context("Git output is not valid UTF-8")?
        .trim()
        .to_owned())
}

#[cfg(unix)]
fn read_regular(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let file = fs::OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
        .open(path)
        .context("cannot open managed file")?;
    let metadata = file.metadata().context("cannot inspect managed file")?;
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.len() > maximum as u64 {
        return Err(RepositoryConflictError.into());
    }
    let mut payload = Vec::with_capacity(metadata.len() as usize);
    file.take((maximum + 1) as u64)
        .read_to_end(&mut payload)
        .context("cannot read managed file")?;
    if payload.len() > maximum {
        return Err(RepositoryConflictError.into());
    }
    Ok(payload)
}

#[cfg(not(unix))]
fn read_regular(path: &Path, maximum: usize) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).context("cannot inspect managed file")?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > maximum as u64 {
        return Err(RepositoryConflictError.into());
    }
    let payload = fs::read(path).context("cannot read managed file")?;
    if payload.len() > maximum {
        return Err(RepositoryConflictError.into());
    }
    Ok(payload)
}

#[cfg(unix)]
fn is_executable(path: &Path) -> Result<bool> {
    use std::os::unix::fs::PermissionsExt;
    Ok(fs::metadata(path)?.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(_path: &Path) -> Result<bool> {
    Ok(false)
}

pub fn render_repository_workflow() -> Vec<u8> {
    format!(
        "name: DataJig\n\n'on':\n  pull_request:\n  push:\n    branches:\n      - main\n\npermissions:\n  contents: read\n\njobs:\n  datajig:\n    runs-on: ubuntu-24.04\n    steps:\n      - uses: actions/checkout@11d5960a326750d5838078e36cf38b85af677262\n      - uses: actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065\n        with:\n          python-version: '3.11'\n      - name: Install DataJig\n        run: python -m pip install \"datajig=={}\"\n      - name: Verify DataJig repository integration\n        run: datajig repository-check --root . --ci\n",
        python_package_version()
    )
    .into_bytes()
}

pub fn render_repository_hook() -> Vec<u8> {
    b"#!/bin/sh\nset -eu\nrepository_root=$(git rev-parse --show-toplevel)\nexec datajig repository-check --root \"$repository_root\"\n".to_vec()
}

fn managed_file(path: &str, payload: Vec<u8>, executable: bool) -> ManagedFile {
    ManagedFile {
        path: path.into(),
        content_id: blake3_content_id("managed", b"datajig-repository-managed-file-v1\0", &payload),
        bytes: payload.len(),
        executable,
    }
}

fn python_package_version() -> String {
    env!("CARGO_PKG_VERSION").replace("-dev.", ".dev")
}

fn normalize_states(states: &[PathBuf]) -> Result<Vec<String>> {
    if states.len() > MAX_REPOSITORY_STATES {
        bail!("repository integration exceeds {MAX_REPOSITORY_STATES} state bindings");
    }
    let mut normalized = Vec::with_capacity(states.len());
    for state in states {
        let text = state
            .to_str()
            .context("repository state path must be valid UTF-8")?;
        validate_relative_path(text, "repository state")?;
        normalized.push(text.to_owned());
    }
    normalized.sort();
    if normalized.windows(2).any(|pair| pair[0] == pair[1]) {
        bail!("repository state paths must be unique");
    }
    Ok(normalized)
}

fn validate_states(states: &[String]) -> Result<()> {
    if states.len() > MAX_REPOSITORY_STATES {
        bail!("repository integration exceeds {MAX_REPOSITORY_STATES} state bindings");
    }
    let mut previous: Option<&str> = None;
    for state in states {
        validate_relative_path(state, "repository state")?;
        if previous.is_some_and(|value| value >= state.as_str()) {
            bail!("repository state paths must be sorted and unique");
        }
        previous = Some(state);
    }
    Ok(())
}

fn validate_relative_path(value: &str, label: &str) -> Result<()> {
    if value.is_empty() || value.len() > MAX_PATH_BYTES || value.contains('\\') {
        bail!("{label} path is invalid");
    }
    let path = Path::new(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        bail!("{label} path must be normalized and relative");
    }
    let normalized = path
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    if normalized != value {
        bail!("{label} path must be normalized and relative");
    }
    Ok(())
}

fn managed_paths(hook: bool, github_actions: bool) -> Vec<&'static str> {
    let mut paths = vec![SKILL_PATH];
    if github_actions {
        paths.push(WORKFLOW_PATH);
    }
    if hook {
        paths.push(HOOK_PATH);
    }
    paths
}

fn valid_content_id(value: &str, prefix: &str) -> bool {
    value
        .strip_prefix(&format!("{prefix}_"))
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}
