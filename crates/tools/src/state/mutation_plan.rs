//! Preview and apply conservative multi-file text mutations.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::path_policy::WorkspacePathPolicy;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationEdit {
    pub path: String,
    pub old_string: String,
    pub new_string: String,
    pub replace_all: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationPreviewInput {
    pub edits: Vec<MutationEdit>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationApplyInput {
    pub edits: Vec<MutationEdit>,
    #[serde(default)]
    pub expected_hashes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationPreview {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "fileCount")]
    pub file_count: usize,
    #[serde(rename = "conflictCount")]
    pub conflict_count: usize,
    pub files: Vec<FileMutationPreview>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileMutationPreview {
    pub path: String,
    #[serde(rename = "expectedHash")]
    pub expected_hash: String,
    #[serde(rename = "replacementCount")]
    pub replacement_count: usize,
    pub conflicts: Vec<String>,
    #[serde(rename = "originalPreview")]
    pub original_preview: String,
    #[serde(rename = "updatedPreview")]
    pub updated_preview: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MutationApplyOutput {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "appliedCount")]
    pub applied_count: usize,
    pub applied: Vec<FileMutationApplied>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileMutationApplied {
    pub path: String,
    #[serde(rename = "resolvedPath")]
    pub resolved_path: String,
    #[serde(rename = "previousHash")]
    pub previous_hash: String,
    /// Cryptographic digest of the exact pre-image read before this
    /// transaction. `previousHash` remains the compact optimistic-CAS token.
    #[serde(rename = "previousSha256")]
    pub previous_sha256: String,
    #[serde(rename = "newHash")]
    pub new_hash: String,
    /// Cryptographic digest of the exact bytes committed by this transaction.
    #[serde(rename = "sha256")]
    pub sha256: String,
    #[serde(rename = "replacementCount")]
    pub replacement_count: usize,
}

pub fn preview_mutations(
    policy: &WorkspacePathPolicy,
    input: MutationPreviewInput,
) -> io::Result<MutationPreview> {
    let grouped = group_edits(input.edits);
    let mut files = Vec::new();
    let mut conflict_count = 0usize;

    for (path, edits) in grouped {
        let resolved = policy.resolve(&path)?;
        let original = fs::read_to_string(&resolved)?;
        let expected_hash = stable_hash(&original);
        let mut updated = original.clone();
        let mut replacement_count = 0usize;
        let mut conflicts = Vec::new();

        for edit in edits {
            if edit.old_string == edit.new_string {
                conflicts.push(format!(
                    "old_string and new_string are identical in `{path}`"
                ));
                continue;
            }
            let matches = updated.matches(&edit.old_string).count();
            if matches == 0 {
                conflicts.push(format!("old_string not found in `{path}`"));
                continue;
            }
            let replace_all = edit.replace_all.unwrap_or(false);
            if matches > 1 && !replace_all {
                conflicts.push(format!(
                    "old_string matched {matches} times in `{path}`; set replace_all to true"
                ));
                continue;
            }
            if replace_all {
                updated = updated.replace(&edit.old_string, &edit.new_string);
                replacement_count += matches;
            } else {
                updated = updated.replacen(&edit.old_string, &edit.new_string, 1);
                replacement_count += 1;
            }
        }

        conflict_count += conflicts.len();
        files.push(FileMutationPreview {
            path,
            expected_hash,
            replacement_count,
            conflicts,
            original_preview: preview_text(&original),
            updated_preview: preview_text(&updated),
        });
    }

    Ok(MutationPreview {
        kind: "mutation_preview".to_string(),
        file_count: files.len(),
        conflict_count,
        files,
    })
}

pub fn apply_mutations(
    policy: &WorkspacePathPolicy,
    input: MutationApplyInput,
) -> io::Result<MutationApplyOutput> {
    let preview = preview_mutations(
        policy,
        MutationPreviewInput {
            edits: input.edits.clone(),
        },
    )?;
    let conflicts = preview
        .files
        .iter()
        .flat_map(|file| file.conflicts.iter())
        .cloned()
        .collect::<Vec<_>>();
    if !conflicts.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("mutation preview has conflicts: {}", conflicts.join("; ")),
        ));
    }

    let grouped = group_edits(input.edits);
    let mut planned = Vec::new();
    for (path, edits) in grouped {
        let resolved = policy.resolve(&path)?;
        let original = fs::read_to_string(&resolved)?;
        let previous_hash = stable_hash(&original);
        let previous_sha256 = format!("{:x}", Sha256::digest(original.as_bytes()));
        if let Some(expected) = input.expected_hashes.get(&path) {
            if expected != &previous_hash {
                return Err(io::Error::other(format!(
                    "file `{path}` changed before apply: expected {expected}, got {previous_hash}"
                )));
            }
        }
        let mut updated = original.clone();
        let mut replacement_count = 0usize;
        for edit in edits {
            let replace_all = edit.replace_all.unwrap_or(false);
            let matches = updated.matches(&edit.old_string).count();
            if replace_all {
                updated = updated.replace(&edit.old_string, &edit.new_string);
                replacement_count += matches;
            } else {
                updated = updated.replacen(&edit.old_string, &edit.new_string, 1);
                replacement_count += 1;
            }
        }
        planned.push(PlannedMutation {
            path: resolved,
            display_path: path,
            updated,
            previous_hash,
            previous_sha256,
            replacement_count,
        });
    }

    let transaction_id = next_transaction_id();
    let mut temp_paths: Vec<PathBuf> = Vec::new();
    for mutation in &planned {
        let parent = mutation.path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("file `{}` has no parent directory", mutation.display_path),
            )
        })?;
        let file_name = mutation
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("mutation");
        let temp_path = parent.join(format!(
            ".{file_name}.cowd-txn-{transaction_id}-{}",
            temp_paths.len()
        ));
        if let Err(error) = fs::write(&temp_path, &mutation.updated) {
            for temp_path in temp_paths {
                if let Err(cleanup_error) = fs::remove_file(&temp_path) {
                    tracing::warn!(
                        path = %temp_path.display(),
                        error = %cleanup_error,
                        "failed to remove staged mutation after transaction preparation failed"
                    );
                }
            }
            return Err(error);
        }
        temp_paths.push(temp_path);
    }

    let backup_paths = commit_staged_mutations(&planned, &temp_paths, transaction_id)?;

    // T11: post-apply verification. The transaction is not considered applied
    // until every target file matches the planned content and, inside a git
    // worktree, `git diff --check` reports no whitespace/conflict errors.
    if let Err(verify_error) = verify_applied_mutations(&planned, &backup_paths, &temp_paths) {
        return Err(verify_error);
    }

    for backup_path in backup_paths {
        if let Err(error) = fs::remove_file(&backup_path) {
            if error.kind() != io::ErrorKind::NotFound {
                tracing::warn!(
                    path = %backup_path.display(),
                    error = %error,
                    "mutation verified but backup cleanup failed"
                );
            }
        }
    }

    let applied = planned
        .into_iter()
        .map(|mutation| FileMutationApplied {
            resolved_path: mutation.path.to_string_lossy().into_owned(),
            path: mutation.display_path,
            previous_hash: mutation.previous_hash,
            previous_sha256: mutation.previous_sha256,
            new_hash: stable_hash(&mutation.updated),
            sha256: format!("{:x}", Sha256::digest(mutation.updated.as_bytes())),
            replacement_count: mutation.replacement_count,
        })
        .collect::<Vec<_>>();

    Ok(MutationApplyOutput {
        kind: "mutation_apply".to_string(),
        applied_count: applied.len(),
        applied,
    })
}

fn commit_staged_mutations(
    planned: &[PlannedMutation],
    temp_paths: &[PathBuf],
    transaction_id: u64,
) -> io::Result<Vec<PathBuf>> {
    let backup_paths = planned
        .iter()
        .enumerate()
        .map(|(index, mutation)| {
            let parent = mutation.path.parent().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("file `{}` has no parent directory", mutation.display_path),
                )
            })?;
            let file_name = mutation
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("mutation");
            Ok(parent.join(format!(".{file_name}.cowd-backup-{transaction_id}-{index}")))
        })
        .collect::<io::Result<Vec<_>>>()?;

    let mut committed = Vec::new();
    let mut active_backup = None;
    let commit_result = (|| -> io::Result<()> {
        for (index, mutation) in planned.iter().enumerate() {
            fs::rename(&mutation.path, &backup_paths[index])?;
            active_backup = Some(index);
            fs::rename(&temp_paths[index], &mutation.path)?;
            active_backup = None;
            committed.push(index);
        }
        Ok(())
    })();

    if let Err(commit_error) = commit_result {
        let mut rollback_errors = Vec::new();
        if let Some(index) = active_backup {
            if let Err(error) = fs::rename(&backup_paths[index], &planned[index].path) {
                rollback_errors.push(format!("{}: {error}", planned[index].display_path));
            }
        }
        for index in committed.into_iter().rev() {
            if let Err(error) = fs::remove_file(&planned[index].path) {
                if error.kind() != io::ErrorKind::NotFound {
                    rollback_errors.push(format!(
                        "{} (remove replacement): {error}",
                        planned[index].display_path
                    ));
                    continue;
                }
            }
            if let Err(error) = fs::rename(&backup_paths[index], &planned[index].path) {
                rollback_errors.push(format!(
                    "{} (restore backup {}): {error}",
                    planned[index].display_path,
                    backup_paths[index].display()
                ));
            }
        }
        for temp_path in temp_paths {
            if let Err(error) = fs::remove_file(temp_path) {
                if error.kind() != io::ErrorKind::NotFound {
                    rollback_errors.push(format!(
                        "{} (remove staged file): {error}",
                        temp_path.display()
                    ));
                }
            }
        }
        if rollback_errors.is_empty() {
            return Err(commit_error);
        }
        return Err(io::Error::other(format!(
            "mutation commit failed: {commit_error}; rollback incomplete: {}",
            rollback_errors.join("; ")
        )));
    }

    Ok(backup_paths)
}

fn verify_applied_mutations(
    planned: &[PlannedMutation],
    backup_paths: &[PathBuf],
    temp_paths: &[PathBuf],
) -> io::Result<()> {
    let mut verified_paths = Vec::new();
    for mutation in planned {
        let actual = fs::read_to_string(&mutation.path).map_err(|error| {
            io::Error::other(format!(
                "post-apply verification could not read `{}`: {error}",
                mutation.display_path
            ))
        })?;
        let actual_hash = stable_hash(&actual);
        if actual_hash != stable_hash(&mutation.updated) {
            let error = io::Error::other(format!(
                "post-apply verification failed for `{}`: expected hash {}, got {actual_hash}",
                mutation.display_path,
                stable_hash(&mutation.updated)
            ));
            restore_verified_backups(planned, backup_paths, temp_paths, &error);
            return Err(error);
        }
        verified_paths.push(mutation.path.clone());
    }

    if let Err(error) = verify_git_diff_check(planned, &verified_paths) {
        restore_verified_backups(planned, backup_paths, temp_paths, &error);
        return Err(error);
    }
    Ok(())
}

fn verify_git_diff_check(planned: &[PlannedMutation], paths: &[PathBuf]) -> io::Result<()> {
    let workspace_root = planned
        .first()
        .and_then(|mutation| mutation.path.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."));
    if !is_git_worktree(&workspace_root) {
        // Not a git worktree (or no git binary): content hashes remain the
        // authoritative check.
        return Ok(());
    }
    let result = Command::new("git")
        .arg("diff")
        .arg("--check")
        .arg("--")
        .args(paths)
        .current_dir(&workspace_root)
        .output();
    let output = match result {
        Ok(output) => output,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // No git binary: nothing to verify beyond content hashes.
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Err(io::Error::other(format!(
        "post-apply `git diff --check` failed{}{}",
        if stdout.is_empty() {
            String::new()
        } else {
            format!(":\n{stdout}")
        },
        if stderr.is_empty() {
            String::new()
        } else {
            format!(":\n{stderr}")
        },
    )))
}

fn is_git_worktree(directory: &Path) -> bool {
    Command::new("git")
        .arg("rev-parse")
        .arg("--is-inside-work-tree")
        .current_dir(directory)
        .output()
        .is_ok_and(|output| output.status.success())
}

fn restore_verified_backups(
    planned: &[PlannedMutation],
    backup_paths: &[PathBuf],
    temp_paths: &[PathBuf],
    cause: &io::Error,
) {
    let mut rollback_errors = Vec::new();
    for (index, mutation) in planned.iter().enumerate() {
        let backup = backup_paths.get(index);
        if fs::remove_file(&mutation.path).is_err() && !mutation.path.exists() {
            // Missing replacement is fine when we restore the backup below.
        }
        if let Some(backup) = backup {
            if let Err(error) = fs::rename(backup, &mutation.path) {
                if error.kind() != io::ErrorKind::NotFound {
                    rollback_errors.push(format!("{}: {error}", mutation.display_path));
                }
            }
        }
    }
    for temp_path in temp_paths {
        if let Err(error) = fs::remove_file(temp_path) {
            if error.kind() != io::ErrorKind::NotFound {
                rollback_errors.push(format!("{} (staged): {error}", temp_path.display()));
            }
        }
    }
    if rollback_errors.is_empty() {
        tracing::error!(
            error = %cause,
            "mutation verification failed; transaction rolled back from backups"
        );
    } else {
        tracing::error!(
            error = %cause,
            rollback_errors = %rollback_errors.join("; "),
            "mutation verification failed AND rollback was incomplete"
        );
    }
}

fn next_transaction_id() -> u64 {
    static NEXT_TRANSACTION_ID: AtomicU64 = AtomicU64::new(1);
    ((std::process::id() as u64) << 32) | NEXT_TRANSACTION_ID.fetch_add(1, Ordering::Relaxed)
}

struct PlannedMutation {
    path: PathBuf,
    display_path: String,
    updated: String,
    previous_hash: String,
    previous_sha256: String,
    replacement_count: usize,
}

fn group_edits(edits: Vec<MutationEdit>) -> BTreeMap<String, Vec<MutationEdit>> {
    let mut grouped: BTreeMap<String, Vec<MutationEdit>> = BTreeMap::new();
    for edit in edits {
        grouped.entry(edit.path.clone()).or_default().push(edit);
    }
    grouped
}

fn stable_hash(value: &str) -> String {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn preview_text(value: &str) -> String {
    const LIMIT: usize = 4000;
    if value.len() <= LIMIT {
        value.to_string()
    } else {
        format!("{}... [truncated]", &value[..LIMIT])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::path_policy::WorkspacePathPolicy;

    fn temp_file(name: &str, content: &str) -> std::path::PathBuf {
        let root =
            std::env::temp_dir().join(format!("cowd-mutation-plan-{}-{name}", std::process::id()));
        let _ = fs::create_dir_all(&root);
        let path = root.join("file.txt");
        fs::write(&path, content).expect("write temp file");
        path
    }

    #[test]
    fn preview_detects_multiple_match_conflict() {
        let path = temp_file("conflict", "alpha\nalpha\n");
        let policy = WorkspacePathPolicy::new(path.parent().expect("parent"));
        let preview = preview_mutations(
            &policy,
            MutationPreviewInput {
                edits: vec![MutationEdit {
                    path: path.to_string_lossy().into_owned(),
                    old_string: "alpha".to_string(),
                    new_string: "omega".to_string(),
                    replace_all: None,
                }],
            },
        )
        .expect("preview");

        assert_eq!(preview.conflict_count, 1);
        assert_eq!(preview.files[0].replacement_count, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn apply_refuses_stale_expected_hash() {
        let path = temp_file("stale", "alpha\n");
        let policy = WorkspacePathPolicy::new(path.parent().expect("parent"));
        let err = apply_mutations(
            &policy,
            MutationApplyInput {
                edits: vec![MutationEdit {
                    path: path.to_string_lossy().into_owned(),
                    old_string: "alpha".to_string(),
                    new_string: "omega".to_string(),
                    replace_all: None,
                }],
                expected_hashes: BTreeMap::from([(
                    path.to_string_lossy().into_owned(),
                    "stale".to_string(),
                )]),
            },
        )
        .expect_err("stale hash should fail");

        assert!(err.to_string().contains("changed before apply"));
        let _ = fs::remove_file(path);
    }

    #[test]
    fn apply_preflights_all_files_before_writing_any_file() {
        let root = std::env::temp_dir().join(format!(
            "cowd-mutation-plan-{}-preflight",
            std::process::id()
        ));
        let _ = fs::create_dir_all(&root);
        let first = root.join("first.txt");
        let second = root.join("missing.txt");
        let policy = WorkspacePathPolicy::new(&root);
        fs::write(&first, "alpha\n").expect("write first");

        let err = apply_mutations(
            &policy,
            MutationApplyInput {
                edits: vec![
                    MutationEdit {
                        path: first.to_string_lossy().into_owned(),
                        old_string: "alpha".to_string(),
                        new_string: "omega".to_string(),
                        replace_all: None,
                    },
                    MutationEdit {
                        path: second.to_string_lossy().into_owned(),
                        old_string: "beta".to_string(),
                        new_string: "theta".to_string(),
                        replace_all: None,
                    },
                ],
                expected_hashes: BTreeMap::new(),
            },
        )
        .expect_err("missing second file should fail before first write");

        assert!(err.kind() == io::ErrorKind::NotFound);
        assert_eq!(
            fs::read_to_string(&first).expect("first unchanged"),
            "alpha\n"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn commit_failure_restores_every_previously_replaced_file() {
        let root = std::env::temp_dir().join(format!(
            "cowd-mutation-plan-{}-rollback",
            next_transaction_id()
        ));
        fs::create_dir_all(&root).expect("create rollback root");
        let first = root.join("first.txt");
        let second = root.join("second.txt");
        fs::write(&first, "first-original").expect("write first");
        fs::write(&second, "second-original").expect("write second");
        let transaction_id = next_transaction_id();
        let first_temp = root.join(format!(".first.txt.cowd-txn-{transaction_id}-0"));
        let second_temp = root.join(format!(".second.txt.cowd-txn-{transaction_id}-1"));
        fs::write(&first_temp, "first-updated").expect("stage first");
        fs::write(&second_temp, "second-updated").expect("stage second");
        fs::remove_file(&second_temp).expect("force second commit failure");
        let planned = vec![
            PlannedMutation {
                path: first.clone(),
                display_path: "first.txt".to_string(),
                updated: "first-updated".to_string(),
                previous_hash: stable_hash("first-original"),
                previous_sha256: format!("{:x}", Sha256::digest(b"first-original")),
                replacement_count: 1,
            },
            PlannedMutation {
                path: second.clone(),
                display_path: "second.txt".to_string(),
                updated: "second-updated".to_string(),
                previous_hash: stable_hash("second-original"),
                previous_sha256: format!("{:x}", Sha256::digest(b"second-original")),
                replacement_count: 1,
            },
        ];

        commit_staged_mutations(&planned, &[first_temp, second_temp], transaction_id)
            .expect_err("second staged rename must fail");

        assert_eq!(
            fs::read_to_string(&first).expect("read restored first"),
            "first-original"
        );
        assert_eq!(
            fs::read_to_string(&second).expect("read untouched second"),
            "second-original"
        );
        fs::remove_dir_all(root).expect("remove rollback root");
    }

    #[test]
    fn apply_in_non_git_directory_verifies_and_commits() {
        let root = std::env::temp_dir().join(format!(
            "cowd-mutation-plan-{}-non-git",
            next_transaction_id()
        ));
        fs::create_dir_all(&root).expect("create non-git root");
        fs::write(root.join("a.txt"), "alpha\n").expect("write a.txt");
        let policy = WorkspacePathPolicy::new(&root);

        let applied = apply_mutations(
            &policy,
            MutationApplyInput {
                edits: vec![MutationEdit {
                    path: "a.txt".to_string(),
                    old_string: "alpha".to_string(),
                    new_string: "beta".to_string(),
                    replace_all: None,
                }],
                expected_hashes: BTreeMap::new(),
            },
        )
        .expect("apply succeeds in a non-git directory");

        assert_eq!(applied.applied_count, 1);
        assert_eq!(
            fs::read_to_string(root.join("a.txt")).expect("read a.txt"),
            "beta\n"
        );
        fs::remove_dir_all(root).expect("remove non-git root");
    }
}
