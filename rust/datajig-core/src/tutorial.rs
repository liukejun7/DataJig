use crate::io::save_file_atomically;
use crate::{
    DEFAULT_TRAINING_SHARD_BYTES, DEFAULT_TRAINING_SHARD_RECORDS, InvalidArgumentError,
    begin_changeset, check_changeset, export_training_bundle_with_view_at_revision,
    initialize_jsonl_workspace, seal_changeset, stage_changeset,
};
use anyhow::{Context, Result, bail};
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};

const BASELINE: &[u8] = b"{\"id\":\"paper-1\",\"score\":0.5,\"title\":\"First draft\"}\n";
const CANDIDATE: &[u8] = b"{\"id\":\"paper-1\",\"score\":0.9,\"title\":\"Reviewed draft\"}\n{\"id\":\"paper-2\",\"score\":0.8,\"title\":\"New evidence\"}\n";

#[derive(Clone, Debug, Serialize)]
pub struct TutorialArtifact {
    pub output_dir: String,
    pub dataset_path: String,
    pub state_dir: String,
    pub manifest_path: String,
    pub dataset_id: String,
    pub change_id: String,
    pub changeset_id: String,
    pub revision_id: String,
    pub bundle_id: String,
    pub completed: Vec<&'static str>,
}

struct TutorialOutput {
    path: PathBuf,
    persist: bool,
}

impl TutorialOutput {
    fn create(output: PathBuf) -> Result<Self> {
        if output.to_str().is_none() {
            return Err(InvalidArgumentError::new("tutorial output must be valid UTF-8").into());
        }
        if fs::symlink_metadata(&output).is_ok() {
            return Err(InvalidArgumentError::new("tutorial output must not already exist").into());
        }
        let parent = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).context("cannot create tutorial output parent")?;
        let parent = parent
            .canonicalize()
            .context("cannot resolve tutorial output parent")?;
        let name = output
            .file_name()
            .context("tutorial output must name a new directory")?;
        let path = parent.join(name);
        if path.to_str().is_none() {
            return Err(InvalidArgumentError::new("tutorial output must be valid UTF-8").into());
        }
        if fs::symlink_metadata(&path).is_ok() {
            return Err(InvalidArgumentError::new("tutorial output must not already exist").into());
        }
        fs::create_dir(&path).context("cannot create tutorial output directory")?;
        Ok(Self {
            path,
            persist: false,
        })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn persist(mut self) {
        self.persist = true;
    }
}

impl Drop for TutorialOutput {
    fn drop(&mut self) {
        if !self.persist {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}

pub fn run_tutorial(output: &Path) -> Result<TutorialArtifact> {
    let output_guard = TutorialOutput::create(output.to_path_buf())?;
    let output = output_guard.path().to_path_buf();

    let dataset = output.join("dataset.jsonl");
    let state = output.join(".datajig");
    let bundle = output.join("training-bundle");
    save_file_atomically(&dataset, BASELINE, "tutorial baseline")?;
    let initialized = initialize_jsonl_workspace(&dataset, &state, "id")?;
    let change = begin_changeset(
        &state,
        1,
        "Demonstrate a verified agent dataset edit",
        "tutorial-1",
        "agent",
    )?;
    save_file_atomically(&dataset, CANDIDATE, "tutorial candidate")?;
    let changeset = stage_changeset(&state, 1, change.change_id())?;
    let checked = check_changeset(&state, 1, 6, change.change_id(), changeset.changeset_id())?;
    if checked.decision != "seal" {
        bail!("tutorial candidate did not produce a sealable review");
    }
    let sealed = seal_changeset(
        &state,
        1,
        "Accept tutorial dataset revision",
        Some(&checked.report_content_id),
        change.change_id(),
        changeset.changeset_id(),
    )?;
    let exported = export_training_bundle_with_view_at_revision(
        &state,
        &bundle,
        None,
        None,
        "datajig-tutorial-v1".into(),
        &["train=10000".into()],
        DEFAULT_TRAINING_SHARD_RECORDS,
        DEFAULT_TRAINING_SHARD_BYTES,
    )?;
    let artifact = TutorialArtifact {
        output_dir: path_text(&output)?,
        dataset_path: path_text(&dataset)?,
        state_dir: initialized.state_dir,
        manifest_path: exported.manifest,
        dataset_id: initialized.dataset_id,
        change_id: change.change_id().into(),
        changeset_id: changeset.changeset_id().into(),
        revision_id: sealed.revision_id,
        bundle_id: exported.bundle_id,
        completed: vec![
            "init",
            "changeset-begin",
            "edit",
            "changeset-stage",
            "check",
            "seal",
            "export",
        ],
    };
    output_guard.persist();
    Ok(artifact)
}

fn path_text(path: &Path) -> Result<String> {
    path.canonicalize()
        .context("cannot resolve tutorial path")?
        .to_str()
        .map(str::to_owned)
        .ok_or_else(|| InvalidArgumentError::new("tutorial path must be valid UTF-8").into())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temporary_output(suffix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "datajig-tutorial-{suffix}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock after epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn incomplete_tutorial_output_is_removed_on_drop() {
        let output = temporary_output("cleanup");
        {
            let guard = TutorialOutput::create(output.clone()).expect("create output");
            fs::write(guard.path().join("partial"), b"partial").expect("write marker");
        }
        assert!(!output.exists());
    }

    #[test]
    fn non_utf8_output_is_rejected_before_creation() {
        let mut bytes = temporary_output("non-utf8").into_os_string().into_vec();
        bytes.push(0xff);
        let output = PathBuf::from(OsString::from_vec(bytes));

        let error = run_tutorial(&output).expect_err("non-UTF-8 path must fail");

        assert!(error.to_string().contains("UTF-8"));
        assert!(!output.exists());
    }
}
