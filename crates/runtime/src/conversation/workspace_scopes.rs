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
    let paths = explicit_workspace_paths(workspace_root, objective, requires_write);
    if !requires_write {
        return paths
            .into_iter()
            .map(|path| format!("read:{path}"))
            .collect();
    }
    let write_paths = paths
        .iter()
        .filter(|path| objective_marks_path_for_write(workspace_root, objective, path))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
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
            if !is_probable_workspace_path_token(workspace_root, &token) {
                return None;
            }
            let candidate = if token.starts_with('/') {
                PathBuf::from(&token)
            } else {
                let rooted = workspace_root.join(token.trim_start_matches("./"));
                std::env::current_dir()
                    .ok()
                    .and_then(|cwd| {
                        cwd.canonicalize()
                            .ok()
                            .map(|cwd| cwd.join(token.trim_start_matches("./")))
                    })
                    .filter(|cwd_candidate| cwd_candidate.exists())
                    .unwrap_or(rooted)
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
    if workspace_root.join(token).exists()
        || std::env::current_dir()
            .map(|cwd| cwd.join(token).exists())
            .unwrap_or(false)
    {
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
    let exists = workspace_root.join(token).exists()
        || std::env::current_dir()
            .map(|cwd| cwd.join(token).exists())
            .unwrap_or(false);
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
