//! Lightweight workspace checkpoints for mutation recovery.

use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointCreateInput {
    pub label: Option<String>,
    /// Optional workspace-relative paths for a bounded checkpoint. An empty
    /// list preserves the historical full-workspace behavior.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub paths: Vec<String>,
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

pub fn checkpoint_create_in(
    root: impl AsRef<Path>,
    input: CheckpointCreateInput,
) -> io::Result<CheckpointSummary> {
    let root = root.as_ref();
    let scopes = normalize_scopes(root, &input.paths)?;
    let id = checkpoint_id();
    let checkpoint_dir = checkpoints_root(root).join(&id);
    fs::create_dir_all(&checkpoint_dir)?;
    let create_result = (|| -> io::Result<()> {
        let manifest = if scopes.is_empty() {
            copy_dir(root, &checkpoint_dir)?;
            build_manifest(root)?
        } else {
            copy_scoped_paths(root, &checkpoint_dir, &scopes)?;
            build_scoped_manifest(root, &scopes)?
        };
        write_manifest(&checkpoint_dir, &manifest)?;
        if let Some(label) = &input.label {
            fs::write(checkpoint_dir.join(".label"), label)?;
        }
        Ok(())
    })();
    if let Err(error) = create_result {
        let _ = fs::remove_dir_all(&checkpoint_dir);
        return Err(error);
    }
    Ok(CheckpointSummary {
        id,
        path: checkpoint_dir.to_string_lossy().into_owned(),
        label: input.label,
    })
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
    let current_manifest = if manifest.scopes.is_empty() {
        build_manifest(root)?
    } else {
        build_scoped_manifest(root, &manifest.scopes)?
    };
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
    let backup_dir = restore_backup_dir(root, &input.id);
    if backup_dir.exists() {
        let _ = fs::remove_dir_all(&backup_dir);
    }
    fs::create_dir_all(&backup_dir)?;
    let backup_manifest = if manifest.scopes.is_empty() {
        copy_dir(root, &backup_dir)?;
        build_manifest(&backup_dir)?
    } else {
        copy_scoped_paths(root, &backup_dir, &manifest.scopes)?;
        build_scoped_manifest(root, &manifest.scopes)?
    };

    let restore_result = (|| -> io::Result<()> {
        if manifest.scopes.is_empty() {
            remove_files_not_in_manifest(root, &manifest.files)?;
            copy_dir(&checkpoint, root)
        } else {
            replace_scoped_paths(&checkpoint, root, &manifest.scopes)
        }
    })();

    if let Err(error) = restore_result {
        let rollback_result = if manifest.scopes.is_empty() {
            remove_files_not_in_manifest(root, &backup_manifest.files)
                .and_then(|()| copy_dir(&backup_dir, root))
        } else {
            replace_scoped_paths(&backup_dir, root, &manifest.scopes)
        };
        if let Err(rollback_error) = rollback_result {
            return Err(io::Error::other(format!(
                "checkpoint restore failed: {error}; rollback failed: {rollback_error}; recovery backup retained at {}",
                backup_dir.display()
            )));
        }
        if let Err(cleanup_error) = fs::remove_dir_all(&backup_dir) {
            tracing::warn!(
                path = %backup_dir.display(),
                error = %cleanup_error,
                "checkpoint rollback succeeded but backup cleanup failed"
            );
        }
        return Err(error);
    }
    if let Err(cleanup_error) = fs::remove_dir_all(&backup_dir) {
        tracing::warn!(
            path = %backup_dir.display(),
            error = %cleanup_error,
            "checkpoint restore succeeded but backup cleanup failed"
        );
    }
    Ok(CheckpointSummary {
        id: input.id,
        path: checkpoint.to_string_lossy().into_owned(),
        label: fs::read_to_string(checkpoint.join(".label")).ok(),
    })
}

fn checkpoints_root(root: &Path) -> PathBuf {
    root.join(".cowd").join("checkpoints")
}

fn restore_backup_dir(root: &Path, checkpoint_id: &str) -> PathBuf {
    root.join(".cowd")
        .join("restore-backups")
        .join(format!("restore-{checkpoint_id}-{}", unique_sequence()))
}

fn checkpoint_id() -> String {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or_default();
    format!(
        "checkpoint-{millis}-p{}-{}",
        std::process::id(),
        unique_sequence()
    )
}

fn unique_sequence() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct CheckpointManifest {
    files: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    scopes: Vec<String>,
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
    Ok(CheckpointManifest {
        files,
        scopes: Vec::new(),
    })
}

fn build_scoped_manifest(root: &Path, scopes: &[String]) -> io::Result<CheckpointManifest> {
    let mut files = BTreeSet::new();
    for scope in scopes {
        let path = root.join(scope);
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            continue;
        };
        if metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("checkpoint scope `{scope}` is a symlink"),
            ));
        }
        if metadata.is_dir() {
            collect_manifest_files_strict(root, &path, &mut files)?;
        } else if metadata.is_file() {
            files.insert(scope.clone());
        }
    }
    Ok(CheckpointManifest {
        files,
        scopes: scopes.to_vec(),
    })
}

fn collect_manifest_files_strict(
    root: &Path,
    current: &Path,
    files: &mut BTreeSet<String>,
) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("checkpoint scope contains symlink `{}`", path.display()),
            ));
        }
        if file_type.is_dir() {
            collect_manifest_files_strict(root, &path, files)?;
        } else if file_type.is_file() {
            let relative = path.strip_prefix(root).unwrap_or(&path);
            files.insert(relative.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
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

fn normalize_scopes(root: &Path, paths: &[String]) -> io::Result<Vec<String>> {
    let mut scopes = BTreeSet::new();
    for raw in paths {
        let path = Path::new(raw.trim());
        if path.as_os_str().is_empty() || path.is_absolute() {
            return Err(invalid_scope(raw));
        }
        let mut normalized = PathBuf::new();
        for component in path.components() {
            match component {
                Component::Normal(value) => normalized.push(value),
                Component::CurDir => {}
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(invalid_scope(raw));
                }
            }
        }
        if normalized.as_os_str().is_empty()
            || normalized
                .components()
                .any(|component| component.as_os_str() == ".cowd")
        {
            return Err(invalid_scope(raw));
        }

        let mut cursor = root.to_path_buf();
        for component in normalized.components() {
            cursor.push(component.as_os_str());
            match fs::symlink_metadata(&cursor) {
                Ok(metadata) if metadata.file_type().is_symlink() => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("checkpoint scope `{raw}` crosses a symlink"),
                    ));
                }
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => break,
                Err(error) => return Err(error),
            }
        }
        scopes.insert(normalized.to_string_lossy().replace('\\', "/"));
    }

    let mut normalized = Vec::new();
    for scope in scopes {
        if normalized
            .iter()
            .any(|parent: &String| path_within_scope(&scope, parent))
        {
            continue;
        }
        normalized.retain(|child| !path_within_scope(child, &scope));
        normalized.push(scope);
    }
    normalized.sort();
    Ok(normalized)
}

fn invalid_scope(raw: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("unsafe checkpoint scope `{raw}`"),
    )
}

fn path_within_scope(path: &str, scope: &str) -> bool {
    path == scope
        || path
            .strip_prefix(scope)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn copy_scoped_paths(from: &Path, to: &Path, scopes: &[String]) -> io::Result<()> {
    for scope in scopes {
        let source = from.join(scope);
        if !source.exists() {
            continue;
        }
        copy_entry_strict(&source, &to.join(scope))?;
    }
    Ok(())
}

fn copy_entry_strict(from: &Path, to: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(from)?;
    if metadata.file_type().is_symlink() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("checkpoint scope contains symlink `{}`", from.display()),
        ));
    }
    if metadata.is_dir() {
        fs::create_dir_all(to)?;
        for entry in fs::read_dir(from)? {
            let entry = entry?;
            copy_entry_strict(&entry.path(), &to.join(entry.file_name()))?;
        }
    } else if metadata.is_file() {
        if let Some(parent) = to.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(from, to)?;
    }
    Ok(())
}

fn replace_scoped_paths(from: &Path, to: &Path, scopes: &[String]) -> io::Result<()> {
    for scope in scopes {
        remove_path_if_exists(&to.join(scope))?;
    }
    copy_scoped_paths(from, to, scopes)
}

fn remove_path_if_exists(path: &Path) -> io::Result<()> {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return Ok(());
    };
    if metadata.is_dir() && !metadata.file_type().is_symlink() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_workspace(name: &str) -> PathBuf {
        let unique = format!(
            "cowd-checkpoint-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        );
        let root = std::env::temp_dir().join(unique);
        fs::create_dir_all(&root).expect("workspace");
        root
    }

    #[test]
    fn scoped_checkpoint_copies_diffs_and_restores_only_declared_paths() {
        let root = temp_workspace("scoped");
        fs::create_dir_all(root.join("src")).expect("src");
        fs::write(root.join("src/target.txt"), "before\n").expect("target");
        fs::write(root.join("src/unrelated.txt"), "stable\n").expect("unrelated");

        let created = checkpoint_create_in(
            &root,
            CheckpointCreateInput {
                label: Some("bounded".into()),
                paths: vec!["src/target.txt".into()],
            },
        )
        .expect("create scoped checkpoint");
        let checkpoint = checkpoints_root(&root).join(&created.id);
        assert!(checkpoint.join("src/target.txt").is_file());
        assert!(!checkpoint.join("src/unrelated.txt").exists());

        fs::write(root.join("src/target.txt"), "after\n").expect("mutate target");
        fs::write(root.join("src/unrelated.txt"), "outside scope\n").expect("mutate unrelated");
        let diff = checkpoint_diff_in(
            &root,
            CheckpointDiffInput {
                id: created.id.clone(),
            },
        )
        .expect("scoped diff");
        assert_eq!(diff.changed_files, vec!["src/target.txt"]);
        assert!(diff.added_files.is_empty());
        assert!(diff.deleted_files.is_empty());

        checkpoint_restore_in(&root, CheckpointRestoreInput { id: created.id })
            .expect("scoped restore");
        assert_eq!(
            fs::read_to_string(root.join("src/target.txt")).expect("restored target"),
            "before\n"
        );
        assert_eq!(
            fs::read_to_string(root.join("src/unrelated.txt")).expect("unrelated retained"),
            "outside scope\n"
        );
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn scoped_restore_removes_a_path_that_was_absent_at_checkpoint_time() {
        let root = temp_workspace("absent");
        fs::create_dir_all(root.join("src")).expect("src");
        let created = checkpoint_create_in(
            &root,
            CheckpointCreateInput {
                label: None,
                paths: vec!["src/new.txt".into()],
            },
        )
        .expect("checkpoint absent path");
        fs::write(root.join("src/new.txt"), "created later\n").expect("new path");

        checkpoint_restore_in(&root, CheckpointRestoreInput { id: created.id })
            .expect("restore absent path");
        assert!(!root.join("src/new.txt").exists());
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn scoped_checkpoint_rejects_workspace_escape_and_internal_state() {
        let root = temp_workspace("unsafe");
        for path in ["../escape", "/absolute", ".cowd/checkpoints/escape"] {
            let error = checkpoint_create_in(
                &root,
                CheckpointCreateInput {
                    label: None,
                    paths: vec![path.into()],
                },
            )
            .expect_err("unsafe path must fail");
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn concurrent_scoped_checkpoints_use_distinct_storage() {
        let root = temp_workspace("concurrent");
        fs::write(root.join("target.txt"), "stable\n").expect("target");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let handles = (0..8)
            .map(|index| {
                let root = root.clone();
                let barrier = std::sync::Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    checkpoint_create_in(
                        root,
                        CheckpointCreateInput {
                            label: Some(format!("parallel-{index}")),
                            paths: vec!["target.txt".into()],
                        },
                    )
                    .expect("parallel checkpoint")
                    .id
                })
            })
            .collect::<Vec<_>>();
        let ids = handles
            .into_iter()
            .map(|handle| handle.join().expect("checkpoint thread"))
            .collect::<BTreeSet<_>>();
        assert_eq!(ids.len(), 8);
        assert!(ids
            .iter()
            .all(|id| checkpoints_root(&root).join(id).is_dir()));
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[cfg(unix)]
    #[test]
    fn scoped_checkpoint_rejects_symlink_traversal() {
        use std::os::unix::fs::symlink;

        let root = temp_workspace("symlink");
        let outside = temp_workspace("symlink-outside");
        symlink(&outside, root.join("linked")).expect("symlink");
        let error = checkpoint_create_in(
            &root,
            CheckpointCreateInput {
                label: None,
                paths: vec!["linked/file.txt".into()],
            },
        )
        .expect_err("symlink traversal must fail");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        fs::remove_dir_all(root).expect("cleanup root");
        fs::remove_dir_all(outside).expect("cleanup outside");
    }
}
