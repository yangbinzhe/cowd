//! Lightweight workspace checkpoints for mutation recovery.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointCreateInput {
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointRestoreInput {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointDiffInput {
    pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointSummary {
    pub id: String,
    pub path: String,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointListOutput {
    #[serde(rename = "type")]
    pub kind: String,
    pub checkpoints: Vec<CheckpointSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointDiffOutput {
    #[serde(rename = "type")]
    pub kind: String,
    pub id: String,
    #[serde(rename = "changedFiles")]
    pub changed_files: Vec<String>,
    #[serde(rename = "addedFiles")]
    pub added_files: Vec<String>,
    #[serde(rename = "deletedFiles")]
    pub deleted_files: Vec<String>,
}

pub fn checkpoint_create(input: CheckpointCreateInput) -> io::Result<CheckpointSummary> {
    let root = std::env::current_dir()?;
    checkpoint_create_in(&root, input)
}

pub fn checkpoint_create_in(
    root: impl AsRef<Path>,
    input: CheckpointCreateInput,
) -> io::Result<CheckpointSummary> {
    let root = root.as_ref();
    let id = checkpoint_id();
    let checkpoint_dir = checkpoints_root(root).join(&id);
    fs::create_dir_all(&checkpoint_dir)?;
    copy_dir(root, &checkpoint_dir)?;
    let manifest = build_manifest(root)?;
    write_manifest(&checkpoint_dir, &manifest)?;
    if let Some(label) = &input.label {
        fs::write(checkpoint_dir.join(".label"), label)?;
    }
    Ok(CheckpointSummary {
        id,
        path: checkpoint_dir.to_string_lossy().into_owned(),
        label: input.label,
    })
}

pub fn checkpoint_list() -> io::Result<CheckpointListOutput> {
    let root = std::env::current_dir()?;
    checkpoint_list_in(&root)
}

pub fn checkpoint_list_in(root: impl AsRef<Path>) -> io::Result<CheckpointListOutput> {
    let root = root.as_ref();
    let dir = checkpoints_root(root);
    let mut checkpoints = Vec::new();
    if let Ok(entries) = fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let id = entry.file_name().to_string_lossy().into_owned();
            let label = fs::read_to_string(path.join(".label")).ok();
            checkpoints.push(CheckpointSummary {
                id,
                path: path.to_string_lossy().into_owned(),
                label,
            });
        }
    }
    checkpoints.sort_by(|left, right| right.id.cmp(&left.id));
    Ok(CheckpointListOutput {
        kind: "checkpoint_list".to_string(),
        checkpoints,
    })
}

pub fn checkpoint_diff(input: CheckpointDiffInput) -> io::Result<CheckpointDiffOutput> {
    let root = std::env::current_dir()?;
    checkpoint_diff_in(&root, input)
}

pub fn checkpoint_diff_in(
    root: impl AsRef<Path>,
    input: CheckpointDiffInput,
) -> io::Result<CheckpointDiffOutput> {
    let root = root.as_ref();
    let checkpoint = checkpoints_root(root).join(&input.id);
    if !checkpoint.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "checkpoint not found",
        ));
    }
    let manifest = read_manifest(&checkpoint).or_else(|_| build_manifest(&checkpoint))?;
    let current_manifest = build_manifest(root)?;
    let mut changed = Vec::new();
    let mut deleted = Vec::new();
    let mut added = Vec::new();

    for relative in &manifest.files {
        let current_path = root.join(relative);
        let checkpoint_path = checkpoint.join(relative);
        if !current_path.exists() {
            deleted.push(relative.clone());
        } else if fs::read(&current_path).ok() != fs::read(&checkpoint_path).ok() {
            changed.push(relative.clone());
        }
    }
    for relative in &current_manifest.files {
        if !manifest.files.contains(relative) {
            added.push(relative.clone());
        }
    }

    changed.sort();
    added.sort();
    deleted.sort();
    Ok(CheckpointDiffOutput {
        kind: "checkpoint_diff".to_string(),
        id: input.id,
        changed_files: changed,
        added_files: added,
        deleted_files: deleted,
    })
}

pub fn checkpoint_restore(input: CheckpointRestoreInput) -> io::Result<CheckpointSummary> {
    let root = std::env::current_dir()?;
    checkpoint_restore_in(&root, input)
}

pub fn checkpoint_restore_in(
    root: impl AsRef<Path>,
    input: CheckpointRestoreInput,
) -> io::Result<CheckpointSummary> {
    let root = root.as_ref();
    let checkpoint = checkpoints_root(root).join(&input.id);
    if !checkpoint.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "checkpoint not found",
        ));
    }
    let manifest = read_manifest(&checkpoint).or_else(|_| build_manifest(&checkpoint))?;
    let backup_dir = restore_backup_dir(root);
    if backup_dir.exists() {
        let _ = fs::remove_dir_all(&backup_dir);
    }
    fs::create_dir_all(&backup_dir)?;
    copy_dir(root, &backup_dir)?;
    let backup_manifest = build_manifest(&backup_dir)?;

    let restore_result = (|| -> io::Result<()> {
        remove_files_not_in_manifest(root, &manifest.files)?;
        copy_dir(&checkpoint, root)
    })();

    if let Err(error) = restore_result {
        let _ = remove_files_not_in_manifest(root, &backup_manifest.files);
        let _ = copy_dir(&backup_dir, root);
        let _ = fs::remove_dir_all(&backup_dir);
        return Err(error);
    }
    let _ = fs::remove_dir_all(&backup_dir);
    Ok(CheckpointSummary {
        id: input.id,
        path: checkpoint.to_string_lossy().into_owned(),
        label: fs::read_to_string(checkpoint.join(".label")).ok(),
    })
}

fn checkpoints_root(root: &Path) -> PathBuf {
    root.join(".cowd").join("checkpoints")
}

fn restore_backup_dir(root: &Path) -> PathBuf {
    root.join(".cowd")
        .join("restore-backups")
        .join("latest-restore")
}

fn checkpoint_id() -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!("checkpoint-{millis}")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CheckpointManifest {
    files: BTreeSet<String>,
}

fn manifest_path(checkpoint: &Path) -> PathBuf {
    checkpoint.join(".manifest.json")
}

fn write_manifest(checkpoint: &Path, manifest: &CheckpointManifest) -> io::Result<()> {
    let encoded = serde_json::to_vec_pretty(manifest).map_err(io::Error::other)?;
    fs::write(manifest_path(checkpoint), encoded)
}

fn read_manifest(checkpoint: &Path) -> io::Result<CheckpointManifest> {
    let raw = fs::read(manifest_path(checkpoint))?;
    serde_json::from_slice(&raw).map_err(io::Error::other)
}

fn build_manifest(root: &Path) -> io::Result<CheckpointManifest> {
    let mut files = BTreeSet::new();
    collect_manifest_files(root, root, &mut files)?;
    Ok(CheckpointManifest { files })
}

fn collect_manifest_files(
    root: &Path,
    current: &Path,
    files: &mut BTreeSet<String>,
) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if should_skip(&path) {
            continue;
        }
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_manifest_files(root, &path, files)?;
        } else if metadata.is_file() {
            let relative = path.strip_prefix(root).unwrap_or(&path);
            files.insert(relative.to_string_lossy().into_owned());
        }
    }
    Ok(())
}

fn remove_files_not_in_manifest(root: &Path, files: &BTreeSet<String>) -> io::Result<()> {
    let current_manifest = build_manifest(root)?;
    for relative in current_manifest.files {
        if files.contains(&relative) {
            continue;
        }
        let path = root.join(relative);
        if path.is_file() {
            fs::remove_file(path)?;
        }
    }
    remove_empty_dirs(root, root)
}

fn remove_empty_dirs(root: &Path, current: &Path) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if should_skip(&path) {
            continue;
        }
        if entry.metadata()?.is_dir() {
            remove_empty_dirs(root, &path)?;
            if path != root && fs::read_dir(&path)?.next().is_none() {
                fs::remove_dir(path)?;
            }
        }
    }
    Ok(())
}

fn copy_dir(from: &Path, to: &Path) -> io::Result<()> {
    for entry in fs::read_dir(from)? {
        let entry = entry?;
        let source = entry.path();
        if should_skip(&source) {
            continue;
        }
        let target = to.join(entry.file_name());
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            fs::create_dir_all(&target)?;
            copy_dir(&source, &target)?;
        } else if metadata.is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(&source, &target)?;
        }
    }
    Ok(())
}

fn should_skip(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                ".git"
                    | ".cowd"
                    | ".label"
                    | ".manifest.json"
                    | "target"
                    | "node_modules"
                    | "dist"
                    | "build"
                    | ".cache"
            )
        })
}
