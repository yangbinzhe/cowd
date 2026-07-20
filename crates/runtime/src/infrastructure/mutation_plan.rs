//! Preview and apply conservative multi-file text mutations.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

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
    #[serde(rename = "previousHash")]
    pub previous_hash: String,
    #[serde(rename = "newHash")]
    pub new_hash: String,
    #[serde(rename = "replacementCount")]
    pub replacement_count: usize,
}

pub fn preview_mutations(input: MutationPreviewInput) -> io::Result<MutationPreview> {
    let grouped = group_edits(input.edits);
    let mut files = Vec::new();
    let mut conflict_count = 0usize;

    for (path, edits) in grouped {
        let original = fs::read_to_string(&path)?;
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

pub fn apply_mutations(input: MutationApplyInput) -> io::Result<MutationApplyOutput> {
    let preview = preview_mutations(MutationPreviewInput {
        edits: input.edits.clone(),
    })?;
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
        let original = fs::read_to_string(&path)?;
        let previous_hash = stable_hash(&original);
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
            path: PathBuf::from(&path),
            display_path: path,
            original,
            updated,
            previous_hash,
            replacement_count,
        });
    }

    let mut temp_paths = Vec::new();
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
            ".{file_name}.cowd-txn-{}-{}",
            std::process::id(),
            temp_paths.len()
        ));
        if let Err(error) = fs::write(&temp_path, &mutation.updated) {
            for temp_path in temp_paths {
                let _ = fs::remove_file(temp_path);
            }
            return Err(error);
        }
        temp_paths.push(temp_path);
    }

    let mut committed = Vec::new();
    let commit_result = (|| -> io::Result<()> {
        for (index, mutation) in planned.iter().enumerate() {
            fs::rename(&temp_paths[index], &mutation.path)?;
            committed.push(index);
        }
        Ok(())
    })();

    if let Err(error) = commit_result {
        for temp_path in temp_paths {
            let _ = fs::remove_file(temp_path);
        }
        for index in committed.into_iter().rev() {
            let mutation = &planned[index];
            let _ = fs::write(&mutation.path, &mutation.original);
        }
        return Err(error);
    }

    let applied = planned
        .into_iter()
        .map(|mutation| FileMutationApplied {
            path: mutation.display_path,
            previous_hash: mutation.previous_hash,
            new_hash: stable_hash(&mutation.updated),
            replacement_count: mutation.replacement_count,
        })
        .collect::<Vec<_>>();

    Ok(MutationApplyOutput {
        kind: "mutation_apply".to_string(),
        applied_count: applied.len(),
        applied,
    })
}

struct PlannedMutation {
    path: PathBuf,
    display_path: String,
    original: String,
    updated: String,
    previous_hash: String,
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
        let preview = preview_mutations(MutationPreviewInput {
            edits: vec![MutationEdit {
                path: path.to_string_lossy().into_owned(),
                old_string: "alpha".to_string(),
                new_string: "omega".to_string(),
                replace_all: None,
            }],
        })
        .expect("preview");

        assert_eq!(preview.conflict_count, 1);
        assert_eq!(preview.files[0].replacement_count, 0);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn apply_refuses_stale_expected_hash() {
        let path = temp_file("stale", "alpha\n");
        let err = apply_mutations(MutationApplyInput {
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
        })
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
        fs::write(&first, "alpha\n").expect("write first");

        let err = apply_mutations(MutationApplyInput {
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
        })
        .expect_err("missing second file should fail before first write");

        assert!(err.kind() == io::ErrorKind::NotFound);
        assert_eq!(
            fs::read_to_string(&first).expect("first unchanged"),
            "alpha\n"
        );
        let _ = fs::remove_dir_all(root);
    }
}
