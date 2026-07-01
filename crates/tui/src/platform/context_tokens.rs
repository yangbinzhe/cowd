use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContextToken {
    Diff,
    Staged,
    File(PathBuf),
    Folder(PathBuf),
    Url(String),
    Git(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextValidationError {
    pub token: String,
    pub message: String,
}

impl std::fmt::Display for ContextValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.token, self.message)
    }
}

impl std::error::Error for ContextValidationError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextWorkspaceEntry {
    pub path: String,
    pub is_dir: bool,
}

impl ContextWorkspaceEntry {
    pub fn new(path: impl Into<String>, is_dir: bool) -> Self {
        Self {
            path: normalize_entry_path(&path.into()),
            is_dir,
        }
    }
}

pub fn parse_context_tokens(text: &str) -> Vec<ContextToken> {
    text.split_whitespace()
        .filter_map(parse_context_token)
        .collect()
}

pub fn validate_context_tokens_against_entries(
    text: &str,
    entries: &[ContextWorkspaceEntry],
) -> Result<Vec<ContextToken>, ContextValidationError> {
    let mut tokens = Vec::new();
    for raw in text.split_whitespace().filter(|raw| raw.starts_with('@')) {
        if let Some(token) = validate_raw_context_token(raw, entries) {
            tokens.push(token?);
        }
    }
    Ok(tokens)
}

fn parse_context_token(raw: &str) -> Option<ContextToken> {
    if raw == "@diff" {
        return Some(ContextToken::Diff);
    }
    if raw == "@staged" {
        return Some(ContextToken::Staged);
    }
    if let Some(path) = raw.strip_prefix("@file:") {
        return Some(ContextToken::File(PathBuf::from(path)));
    }
    if let Some(path) = raw.strip_prefix("@folder:") {
        return Some(ContextToken::Folder(PathBuf::from(path)));
    }
    if let Some(url) = raw.strip_prefix("@url:") {
        return Some(ContextToken::Url(url.to_string()));
    }
    if let Some(reference) = raw.strip_prefix("@git:") {
        return Some(ContextToken::Git(reference.to_string()));
    }
    None
}

fn validate_raw_context_token(
    raw: &str,
    entries: &[ContextWorkspaceEntry],
) -> Option<Result<ContextToken, ContextValidationError>> {
    if raw == "@diff" {
        return Some(Ok(ContextToken::Diff));
    }
    if raw == "@staged" {
        return Some(Ok(ContextToken::Staged));
    }
    if let Some(path) = raw.strip_prefix("@file:") {
        return Some(validate_path_token(raw, path, entries, false));
    }
    if let Some(path) = raw.strip_prefix("@folder:") {
        return Some(validate_path_token(raw, path, entries, true));
    }
    if let Some(url) = raw.strip_prefix("@url:") {
        return Some(if url.is_empty() {
            Err(error(raw, "URL不能为空"))
        } else {
            Ok(ContextToken::Url(url.to_string()))
        });
    }
    if let Some(reference) = raw.strip_prefix("@git:") {
        return Some(if reference.is_empty() {
            Err(error(raw, "Git引用不能为空"))
        } else {
            Ok(ContextToken::Git(reference.to_string()))
        });
    }
    None
}

fn validate_path_token(
    raw: &str,
    path_text: &str,
    entries: &[ContextWorkspaceEntry],
    want_dir: bool,
) -> Result<ContextToken, ContextValidationError> {
    if path_text.is_empty() {
        return Err(error(raw, "路径不能为空"));
    }

    let normalized = normalize_user_path(raw, path_text)?;
    if entries.is_empty() {
        return Err(error(raw, "Gateway工作区投影尚未加载，无法确认文件上下文"));
    }

    let Some(entry) = entries.iter().find(|entry| entry.path == normalized) else {
        return Err(error(raw, "路径不在Gateway工作区投影中"));
    };

    match (want_dir, entry.is_dir) {
        (true, true) => Ok(ContextToken::Folder(normalized.into())),
        (false, false) => Ok(ContextToken::File(normalized.into())),
        (true, false) => Err(error(raw, "需要选择文件夹")),
        (false, true) => Err(error(raw, "需要选择文件")),
    }
}

fn normalize_user_path(raw: &str, path_text: &str) -> Result<String, ContextValidationError> {
    let normalized = normalize_entry_path(path_text);
    if normalized.is_empty() {
        return Err(error(raw, "路径不能为空"));
    }
    if normalized.starts_with('/') || normalized.split('/').any(|part| part == "..") {
        return Err(error(raw, "路径必须是工作区内相对路径"));
    }
    Ok(normalized)
}

fn normalize_entry_path(path_text: &str) -> String {
    path_text
        .trim()
        .replace('\\', "/")
        .trim_start_matches("./")
        .trim_end_matches('/')
        .to_string()
}

fn error(token: &str, message: &str) -> ContextValidationError {
    ContextValidationError {
        token: token.to_string(),
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_entries() -> Vec<ContextWorkspaceEntry> {
        vec![
            ContextWorkspaceEntry::new("src/main.rs", false),
            ContextWorkspaceEntry::new("src", true),
            ContextWorkspaceEntry::new("README.md", false),
        ]
    }

    #[test]
    fn parses_structured_context_tokens() {
        let tokens = parse_context_tokens(
            "检查 @diff @staged @file:src/main.rs @folder:src @url:https://example.com @git:HEAD",
        );

        assert_eq!(tokens.len(), 6);
        assert!(tokens.contains(&ContextToken::Diff));
        assert!(tokens.contains(&ContextToken::Staged));
        assert!(tokens.contains(&ContextToken::File("src/main.rs".into())));
        assert!(tokens.contains(&ContextToken::Folder("src".into())));
    }

    #[test]
    fn validates_existing_file_and_folder_tokens() {
        let entries = fixture_entries();
        let tokens = validate_context_tokens_against_entries(
            "@file:src/main.rs @folder:src @diff",
            &entries,
        )
        .unwrap();

        assert!(tokens.contains(&ContextToken::File("src/main.rs".into())));
        assert!(tokens.contains(&ContextToken::Folder("src".into())));
        assert!(tokens.contains(&ContextToken::Diff));
    }

    #[test]
    fn rejects_missing_file_token() {
        let entries = fixture_entries();
        let err =
            validate_context_tokens_against_entries("@file:missing.rs", &entries).unwrap_err();

        assert_eq!(err.token, "@file:missing.rs");
        assert!(err.message.contains("Gateway工作区投影"));
    }

    #[test]
    fn rejects_folder_when_file_expected() {
        let entries = fixture_entries();
        let err = validate_context_tokens_against_entries("@file:src", &entries).unwrap_err();

        assert_eq!(err.token, "@file:src");
        assert!(err.message.contains("需要选择文件"));
    }

    #[test]
    fn rejects_context_file_when_projection_is_empty() {
        let err = validate_context_tokens_against_entries("@file:src/main.rs", &[]).unwrap_err();

        assert_eq!(err.token, "@file:src/main.rs");
        assert!(err.message.contains("投影尚未加载"));
    }

    #[test]
    fn rejects_parent_path_escape() {
        let entries = fixture_entries();
        let err = validate_context_tokens_against_entries("@file:../secret", &entries).unwrap_err();

        assert_eq!(err.token, "@file:../secret");
        assert!(err.message.contains("相对路径"));
    }
}
