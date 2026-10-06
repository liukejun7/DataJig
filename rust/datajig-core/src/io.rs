use crate::manifest::{MAX_MANIFEST_BYTES, SnapshotManifest};
use anyhow::{Context, Result, bail};
use std::fs::File;
#[cfg(unix)]
use std::fs::{self, OpenOptions};
use std::io::Read;
#[cfg(unix)]
use std::io::Write;
use std::path::Path;

pub fn load_manifest(path: &Path) -> Result<SnapshotManifest> {
    let file = File::open(path).context("cannot open manifest")?;
    if file.metadata()?.len() > MAX_MANIFEST_BYTES as u64 {
        bail!("manifest is too large (max {MAX_MANIFEST_BYTES} bytes)");
    }
    let mut payload = Vec::new();
    file.take((MAX_MANIFEST_BYTES + 1) as u64)
        .read_to_end(&mut payload)?;
    if payload.len() > MAX_MANIFEST_BYTES {
        bail!("manifest is too large (max {MAX_MANIFEST_BYTES} bytes)");
    }
    let payload = String::from_utf8(payload).context("manifest must contain valid UTF-8")?;
    SnapshotManifest::from_json(&payload)
}

#[cfg(unix)]
pub fn save_manifest(path: &Path, payload: &[u8]) -> Result<()> {
    if payload.len() > MAX_MANIFEST_BYTES {
        bail!("manifest is too large (max {MAX_MANIFEST_BYTES} bytes)");
    }
    save_file_atomically(path, payload, "manifest")
}

#[cfg(unix)]
pub(crate) fn save_file_atomically(path: &Path, payload: &[u8], artifact: &str) -> Result<()> {
    save_file_atomically_with_mode(path, payload, artifact, false, false)
}

#[cfg(unix)]
pub(crate) fn save_new_file_atomically(path: &Path, payload: &[u8], artifact: &str) -> Result<()> {
    save_file_atomically_with_mode(path, payload, artifact, true, false)
}

#[cfg(unix)]
pub(crate) fn save_executable_file_atomically(
    path: &Path,
    payload: &[u8],
    artifact: &str,
) -> Result<()> {
    save_file_atomically_with_mode(path, payload, artifact, false, true)
}

#[cfg(unix)]
pub(crate) fn save_new_executable_file_atomically(
    path: &Path,
    payload: &[u8],
    artifact: &str,
) -> Result<()> {
    save_file_atomically_with_mode(path, payload, artifact, true, true)
}

#[cfg(unix)]
fn save_file_atomically_with_mode(
    path: &Path,
    payload: &[u8],
    artifact: &str,
    require_new: bool,
    executable: bool,
) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("{artifact} output has no parent"))?;
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    fs::create_dir_all(parent)
        .with_context(|| format!("cannot create {artifact} output directory"))?;
    let name = path
        .file_name()
        .with_context(|| format!("{artifact} output has no file name"))?;
    let mut temporary = None;
    for attempt in 0..100_u32 {
        let candidate = parent.join(format!(
            ".{}.{}.{}.tmp",
            name.to_string_lossy(),
            std::process::id(),
            attempt
        ));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| format!("cannot create temporary {artifact}"));
            }
        }
    }
    let (temporary_path, mut file) =
        temporary.with_context(|| format!("cannot allocate temporary {artifact}"))?;
    let result = (|| -> Result<()> {
        file.write_all(payload)?;
        if executable {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o755))?;
        }
        file.sync_all()?;
        drop(file);
        if require_new {
            publish_new_noreplace(&temporary_path, path)
                .with_context(|| format!("cannot publish new {artifact} atomically"))?;
        } else {
            fs::rename(&temporary_path, path)
                .with_context(|| format!("cannot publish {artifact} atomically"))?;
        }
        File::open(parent)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary_path);
    }
    result
}

pub(crate) fn rename_noreplace_is_unsupported(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::Unsupported
        || matches!(error.raw_os_error(), Some(22 | 38 | 45 | 95))
}

#[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
pub(crate) fn publish_new_noreplace(from: &Path, to: &Path) -> std::io::Result<()> {
    match rustix::fs::renameat_with(
        rustix::fs::CWD,
        from,
        rustix::fs::CWD,
        to,
        rustix::fs::RenameFlags::NOREPLACE,
    ) {
        Ok(()) => Ok(()),
        Err(error) => {
            let error = std::io::Error::from(error);
            if !rename_noreplace_is_unsupported(&error) {
                return Err(error);
            }
            publish_by_hard_link(from, to)
        }
    }
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_vendor = "apple"))
))]
pub(crate) fn publish_new_noreplace(from: &Path, to: &Path) -> std::io::Result<()> {
    publish_by_hard_link(from, to)
}

#[cfg(unix)]
fn publish_by_hard_link(from: &Path, to: &Path) -> std::io::Result<()> {
    fs::hard_link(from, to)?;
    if let Err(error) = fs::remove_file(from) {
        let _ = fs::remove_file(to);
        return Err(error);
    }
    Ok(())
}

#[cfg(not(unix))]
pub(crate) fn save_new_file_atomically(
    _path: &Path,
    _payload: &[u8],
    artifact: &str,
) -> Result<()> {
    bail!("native {artifact} publication currently requires Unix")
}

#[cfg(not(unix))]
pub(crate) fn save_file_atomically(_path: &Path, _payload: &[u8], artifact: &str) -> Result<()> {
    bail!("native {artifact} publication currently requires Unix")
}

#[cfg(not(unix))]
pub(crate) fn save_executable_file_atomically(
    _path: &Path,
    _payload: &[u8],
    artifact: &str,
) -> Result<()> {
    bail!("native {artifact} publication currently requires Unix")
}

#[cfg(not(unix))]
pub(crate) fn save_new_executable_file_atomically(
    _path: &Path,
    _payload: &[u8],
    artifact: &str,
) -> Result<()> {
    bail!("native {artifact} publication currently requires Unix")
}

#[cfg(not(unix))]
pub fn save_manifest(_path: &Path, _payload: &[u8]) -> Result<()> {
    bail!("native manifest publication currently requires Unix")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_noreplace_errors_enable_the_safe_file_fallback() {
        for code in [22, 38, 95] {
            assert!(rename_noreplace_is_unsupported(
                &std::io::Error::from_raw_os_error(code)
            ));
        }
        assert!(!rename_noreplace_is_unsupported(
            &std::io::Error::from_raw_os_error(13)
        ));
    }
}
