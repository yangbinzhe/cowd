//! Exact workspace resource scopes inferred from an objective.
//!
//! This module is deliberately independent of Team/strategy planning. It is
//! only a capability-boundary helper for explicit file targets. Agents remain
//! free to decide their work decomposition; a scope merely prevents a tool
//! from escaping the workspace or silently widening an explicit lease.

use std::path::{Path, PathBuf};

pub(crate) fn explicit_workspace_resource_scopes(
    workspace_root: &Path,
    objective: &str,
    requires_write: bool,
) -> Vec<String> {
    let mut paths = explicit_workspace_paths(workspace_root, objective, requires_write);
    if !requires_write {
        return paths
            .into_iter()
            .map(|path| format!("read:{path}"))
            .collect();
    }
    let mut write_paths = paths
        .iter()
        .filter(|path| objective_marks_path_for_write(workspace_root, objective, path))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    bind_bare_artifacts_to_declared_directory(objective, &mut paths, &mut write_paths);
    if write_paths.is_empty() && paths.len() == 1 {
        return paths
            .into_iter()
            .map(|path| format!("write:{path}"))
            .collect();
    }
    paths
        .into_iter()
        .map(|path| {
            if write_paths.contains(&path) {
                format!("write:{path}")
            } else {
                format!("read:{path}")
            }
        })
        .collect()
}

/// A request such as "create directory `reports/run-1` and create
/// `summary.md`" names a single delivery directory and then its artifact
/// leaves. Treating the bare leaf as `./summary.md` (and treating the
/// directory itself as a file delivery) creates an invented, impossible
/// completion contract. Bind only this unambiguous shape to its declared
/// parent; other requests retain their explicit paths unchanged.
fn bind_bare_artifacts_to_declared_directory(
    objective: &str,
    paths: &mut Vec<String>,
    write_paths: &mut std::collections::BTreeSet<String>,
) {
    let directory_candidates = paths
        .iter()
        .filter(|path| declared_directory_path(objective, path))
        .cloned()
        .collect::<Vec<_>>();
    let [parent] = directory_candidates.as_slice() else {
        return;
    };
    let bare_artifacts = paths
        .iter()
        .filter(|path| is_bare_planned_file_token(path))
        .cloned()
        .collect::<Vec<_>>();
    if bare_artifacts.is_empty() {
        return;
    }

    paths.retain(|path| path != parent);
    write_paths.remove(parent);
    for artifact in bare_artifacts {
        let bound = format!("{parent}/{artifact}");
        paths.retain(|path| path != &artifact);
        if write_paths.remove(&artifact) {
            write_paths.insert(bound.clone());
        }
        paths.push(bound);
    }
    paths.sort();
    paths.dedup();
}

fn declared_directory_path(objective: &str, relative: &str) -> bool {
    if relative == "." || !relative.contains('/') || Path::new(relative).extension().is_some() {
        return false;
    }
    objective_declares_directory_reference(objective, relative)
}

fn objective_declares_directory_reference(objective: &str, relative: &str) -> bool {
    const DIRECTORY_MARKERS: &[&str] = &["目录", "文件夹", "directory", "folder"];
    [relative.to_string(), format!("./{relative}")]
        .iter()
        .filter_map(|candidate| objective.find(candidate))
        .any(|offset| {
            let before = &objective[..offset];
            let clause_start = before
                .char_indices()
                .rev()
                .find(|(_, character)| matches!(character, '。' | '；' | ';' | '\n' | '！' | '？'))
                .map_or(0, |(index, character)| index + character.len_utf8());
            let clause = objective[clause_start..].to_ascii_lowercase();
            DIRECTORY_MARKERS
                .iter()
                .any(|marker| clause_contains_action(&clause, marker))
        })
}

fn objective_marks_path_for_write(workspace_root: &Path, objective: &str, relative: &str) -> bool {
    const WRITE_MARKERS: &[&str] = &[
        "写入", "生成", "保存", "输出", "创建", "修改", "更新", "编辑", "修复", "重构", "落盘",
        "替换", "改动", "调整", "write", "create", "generate", "save", "modify", "update", "edit",
        "replace", "refactor", "fix",
    ];
    const READ_ONLY_MARKERS: &[&str] = &[
        "只读",
        "不修改",
        "不要修改",
        "不得修改",
        "无需修改",
        "read only",
        "read-only",
        "do not modify",
        "without modifying",
    ];
    let absolute = workspace_root.join(relative).to_string_lossy().to_string();
    [absolute, format!("./{relative}"), relative.to_string()]
        .iter()
        .any(|candidate| {
            objective.match_indices(candidate).any(|(offset, _)| {
                let before = &objective[..offset];
                let clause_start = before
                    .char_indices()
                    .rev()
                    .find(|(_, character)| {
                        matches!(character, '。' | '；' | ';' | '\n' | '！' | '？')
                    })
                    .map_or(0, |(index, character)| index + character.len_utf8());
                let clause = before[clause_start..].to_ascii_lowercase();
                !READ_ONLY_MARKERS
                    .iter()
                    .any(|marker| clause_contains_action(&clause, marker))
                    && WRITE_MARKERS
                        .iter()
                        .any(|marker| clause_contains_action(&clause, marker))
            })
        })
}

fn clause_contains_action(clause: &str, marker: &str) -> bool {
    if !marker.is_ascii() {
        return clause.contains(marker);
    }
    clause.match_indices(marker).any(|(offset, value)| {
        let before = clause[..offset].chars().next_back();
        let after = clause[offset + value.len()..].chars().next();
        before.is_none_or(|character| !character.is_ascii_alphanumeric() && character != '_')
            && after.is_none_or(|character| !character.is_ascii_alphanumeric() && character != '_')
    })
}

fn explicit_workspace_paths(
    workspace_root: &Path,
    objective: &str,
    allow_missing: bool,
) -> Vec<String> {
    let Ok(canonical_root) = workspace_root.canonicalize() else {
        return Vec::new();
    };
    let mut paths = objective
        .split(|character: char| {
            character.is_whitespace()
                || matches!(
                    character,
                    ',' | ';'
                        | ':'
                        | '('
                        | ')'
                        | '['
                        | ']'
                        | '{'
                        | '}'
                        | '<'
                        | '>'
                        | '，'
                        | '。'
                        | '；'
                        | '：'
                        | '、'
                        | '（'
                        | '）'
                        | '【'
                        | '】'
                )
        })
        .map(|token| token.trim_matches(['`', '\'', '"']))
        .filter(|token| !is_definition_like_token(workspace_root, token))
        .filter(|token| {
            token.starts_with('/')
                || token.starts_with("./")
                || (token.contains('/') && !token.contains("://") && !token.starts_with("http"))
                || (allow_missing && is_bare_planned_file_token(token))
        })
        .filter_map(|token| {
            let token = workspace_pattern_existing_prefix(token, allow_missing)?
                .unwrap_or_else(|| token.to_string());
            if !is_probable_workspace_path_token(workspace_root, &token)
                && !(allow_missing && declared_directory_path(objective, &token))
            {
                return None;
            }
            let candidate = if token.starts_with('/') {
                PathBuf::from(&token)
            } else {
                // Relative targets belong to the authorized workspace. The
                // server's process cwd is not a Session path authority.
                workspace_root.join(token.trim_start_matches("./"))
            };
            workspace_relative_explicit_path(
                workspace_root,
                &canonical_root,
                &candidate,
                allow_missing,
            )
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths.dedup();
    paths
}

fn workspace_pattern_existing_prefix(token: &str, allow_missing: bool) -> Option<Option<String>> {
    let is_pattern = |segment: &str| {
        segment == "..."
            || segment.contains('*')
            || segment.contains('?')
            || segment.contains('[')
            || segment.contains(']')
    };
    let segments = token.split('/').collect::<Vec<_>>();
    if segments.iter().any(|segment| *segment == "..") {
        return None;
    }
    let Some(index) = segments.iter().position(|segment| is_pattern(segment)) else {
        return Some(None);
    };
    if allow_missing {
        return None;
    }
    let prefix = segments[..index].join("/");
    if prefix.is_empty() || prefix == "." {
        return None;
    }
    Some(Some(
        if token.starts_with('/') && !prefix.starts_with('/') {
            format!("/{prefix}")
        } else {
            prefix
        },
    ))
}

fn is_probable_workspace_path_token(workspace_root: &Path, token: &str) -> bool {
    if token.starts_with('/') || token.starts_with("./") || !token.is_ascii() {
        return token.starts_with('/') || token.starts_with("./");
    }
    if workspace_root.join(token).exists() {
        return true;
    }
    token.rsplit('/').next().is_some_and(|leaf| {
        leaf.rsplit_once('.').is_some_and(|(name, extension)| {
            !name.is_empty()
                && !extension.is_empty()
                && extension
                    .chars()
                    .all(|character| character.is_ascii_alphanumeric())
        })
    })
}

fn is_bare_planned_file_token(token: &str) -> bool {
    if token.is_empty()
        || !token.is_ascii()
        || token.contains('/')
        || token.contains('\\')
        || token.contains("..")
        || token.contains("://")
    {
        return false;
    }
    let path = Path::new(token);
    let Some(name) = path.file_stem().and_then(|value| value.to_str()) else {
        return false;
    };
    let Some(extension) = path.extension().and_then(|value| value.to_str()) else {
        return false;
    };
    !name.is_empty()
        && name
            .chars()
            .any(|character| character.is_ascii_alphabetic())
        && extension
            .chars()
            .next()
            .is_some_and(|character| character.is_ascii_alphabetic())
        && extension
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
}

fn is_definition_like_token(workspace_root: &Path, token: &str) -> bool {
    const NAMESPACES: &[&str] = &[
        "agent",
        "app",
        "builtin",
        "cowd",
        "definition",
        "skill",
        "template",
        "user",
        "workspace",
        "team",
    ];
    let Some((namespace, rest)) = token.split_once('/') else {
        return false;
    };
    if !NAMESPACES.contains(&namespace) || rest.is_empty() || rest.contains('.') {
        return false;
    }
    let exists = workspace_root.join(token).exists();
    !exists
}

fn workspace_relative_explicit_path(
    workspace_root: &Path,
    canonical_root: &Path,
    candidate: &Path,
    allow_missing: bool,
) -> Option<String> {
    if let Ok(canonical) = candidate.canonicalize() {
        let relative = canonical.strip_prefix(canonical_root).ok()?;
        return Some(if relative.as_os_str().is_empty() {
            ".".to_string()
        } else {
            relative.to_string_lossy().replace('\\', "/")
        });
    }
    if !allow_missing {
        return None;
    }
    let relative = if candidate.is_absolute() {
        candidate.strip_prefix(workspace_root).ok()?.to_path_buf()
    } else {
        candidate.to_path_buf()
    };
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            std::path::Component::Normal(value) => parts.push(value.to_os_string()),
            std::path::Component::CurDir => {}
            _ => return None,
        }
    }
    if parts.is_empty() {
        return Some(".".to_string());
    }
    let relative = parts.iter().collect::<PathBuf>();
    let mut ancestor = workspace_root.join(&relative);
    while !ancestor.exists() {
        ancestor = ancestor.parent()?.to_path_buf();
    }
    ancestor
        .canonicalize()
        .ok()?
        .starts_with(canonical_root)
        .then(|| relative.to_string_lossy().replace('\\', "/"))
}

#[cfg(test)]
mod tests {
    use super::explicit_workspace_resource_scopes;

    #[test]
    fn explicit_targets_use_session_workspace_even_when_process_cwd_has_same_file() {
        let workspace = tempfile::tempdir().unwrap();
        // Both Cargo's crate cwd and direct binary execution at repository
        // root have this manifest. Never mutate the process-global cwd.
        assert!(std::env::current_dir()
            .unwrap()
            .join("Cargo.toml")
            .is_file());
        std::fs::write(workspace.path().join("Cargo.toml"), "session manifest").unwrap();
        assert_eq!(
            explicit_workspace_resource_scopes(workspace.path(), "read ./Cargo.toml", false),
            vec!["read:Cargo.toml"]
        );
        std::fs::remove_file(workspace.path().join("Cargo.toml")).unwrap();
        assert!(
            explicit_workspace_resource_scopes(workspace.path(), "read ./Cargo.toml", false)
                .is_empty()
        );
        assert_eq!(
            explicit_workspace_resource_scopes(workspace.path(), "create ./Cargo.toml", true),
            vec!["write:Cargo.toml"]
        );
        let outside = std::env::current_dir().unwrap().join("Cargo.toml");
        assert!(explicit_workspace_resource_scopes(
            workspace.path(),
            &format!("read {}", outside.display()),
            false
        )
        .is_empty());
    }

    #[test]
    fn binds_bare_artifacts_to_one_declared_delivery_directory() {
        let workspace = tempfile::tempdir().expect("workspace");
        let objective = "请在当前工作区创建隔离目录 .cowd-e2e/causal-ledger，并实际创建 calculate.py 与 README.md。";

        assert_eq!(
            explicit_workspace_resource_scopes(workspace.path(), objective, true),
            vec![
                "write:.cowd-e2e/causal-ledger/README.md".to_string(),
                "write:.cowd-e2e/causal-ledger/calculate.py".to_string(),
            ]
        );
    }

    #[test]
    fn keeps_bare_artifact_at_workspace_root_without_a_declared_directory() {
        let workspace = tempfile::tempdir().expect("workspace");

        assert_eq!(
            explicit_workspace_resource_scopes(workspace.path(), "创建 README.md。", true),
            vec!["write:README.md".to_string()]
        );
    }
}
