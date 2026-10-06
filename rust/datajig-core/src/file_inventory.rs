use crate::MAX_PATH_BYTES;
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FileRecord {
    content_hash: String,
    relative_path: String,
    size: u64,
}

impl FileRecord {
    pub fn new(relative_path: String, size: u64, content_hash: String) -> Result<Self> {
        let value = Self {
            content_hash,
            relative_path,
            size,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn content_hash(&self) -> &str {
        &self.content_hash
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_relative_path(&self.relative_path)?;
        if self.content_hash.len() != 64
            || !self
                .content_hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            bail!("file content_hash must be 64 lowercase hex characters");
        }
        Ok(())
    }
}

pub(crate) fn normalize_file_records(mut files: Vec<FileRecord>) -> Result<Vec<FileRecord>> {
    files.sort_by(|left, right| {
        left.relative_path
            .as_bytes()
            .cmp(right.relative_path.as_bytes())
    });
    for file in &files {
        file.validate()?;
    }
    if files
        .windows(2)
        .any(|pair| pair[0].relative_path == pair[1].relative_path)
    {
        bail!("inventory file paths must be unique");
    }
    Ok(files)
}

fn validate_relative_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.len() > MAX_PATH_BYTES
        || path.contains('\\')
        || path
            .split('/')
            .any(|component| component.is_empty() || matches!(component, "." | ".."))
    {
        bail!("file path must be canonical and relative");
    }
    let candidate = Path::new(path);
    if candidate.is_absolute()
        || candidate.components().any(|component| {
            !matches!(component, Component::Normal(_)) || component.as_os_str().to_str().is_none()
        })
    {
        bail!("file path must be canonical and relative");
    }
    Ok(())
}
