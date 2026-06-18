use std::path::{Path, PathBuf};

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

pub fn parse_context_tokens(text: &str) -> Vec<ContextToken> {
    text.split_whitespace()
        .filter_map(parse_context_token)
        .collect()
}

pub fn validate_context_tokens(
    text: &str,
    cwd: &Path,
) -> Result<Vec<ContextToken>, ContextValidationError> {
    let cwd = cwd.canonicalize().map_err(|err| ContextValidationError {
        token: cwd.display().to_string(),
        message: format!("无法读取当前工作目录：{err}"),
    })?;

    let mut tokens = Vec::new();
    for raw in text.split_whitespace().filter(|raw| raw.starts_with('@')) {
        if let Some(token) = validate_raw_context_token(raw, &cwd) {
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
    cwd: &Path,
) -> Option<Result<ContextToken, ContextValidationError>> {
    if raw == "@diff" {
        return Some(Ok(ContextToken::Diff));
    }
    if raw == "@staged" {
        return Some(Ok(ContextToken::Staged));
    }
    if let Some(path) = raw.strip_prefix("@file:") {
        return Some(validate_path_token(raw, path, cwd, false));
    }
    if let Some(path) = raw.strip_prefix("@folder:") {
        return Some(validate_path_token(raw, path, cwd, true));
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
    cwd: &Path,
    want_dir: bool,
) -> Result<ContextToken, ContextValidationError> {
    if path_text.is_empty() {
        return Err(error(raw, "路径不能为空"));
    }

    let candidate = cwd.join(path_text);
    let canonical = candidate
        .canonicalize()
        .map_err(|err| error(raw, &format!("路径不存在或不可读取：{err}")))?;

    if !canonical.starts_with(cwd) {
        return Err(error(raw, "路径必须位于当前工作目录内"));
    }

    if want_dir {
        if !canonical.is_dir() {
            return Err(error(raw, "需要选择文件夹"));
        }
        Ok(ContextToken::Folder(path_text.into()))
    } else {
        if !canonical.is_file() {
            return Err(error(raw, "需要选择文件"));
        }
        Ok(ContextToken::File(path_text.into()))
    }
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

    fn temp_dir(prefix: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
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
        let dir = temp_dir("cowd-context-token-valid");
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("src/main.rs"), "fn main() {}").unwrap();

        let tokens = validate_context_tokens("@file:src/main.rs @folder:src @diff", &dir).unwrap();

        assert!(tokens.contains(&ContextToken::File("src/main.rs".into())));
        assert!(tokens.contains(&ContextToken::Folder("src".into())));
        assert!(tokens.contains(&ContextToken::Diff));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rejects_missing_file_token() {
        let dir = temp_dir("cowd-context-token-missing");

        let err = validate_context_tokens("@file:missing.rs", &dir).unwrap_err();

        assert_eq!(err.token, "@file:missing.rs");
        assert!(err.message.contains("路径不存在"));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rejects_folder_when_file_expected() {
        let dir = temp_dir("cowd-context-token-folder-as-file");
        std::fs::create_dir_all(dir.join("src")).unwrap();

        let err = validate_context_tokens("@file:src", &dir).unwrap_err();

        assert_eq!(err.token, "@file:src");
        assert!(err.message.contains("需要选择文件"));

        let _ = std::fs::remove_dir_all(dir);
    }
}
