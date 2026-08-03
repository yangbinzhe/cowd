#![allow(clippy::should_implement_trait, clippy::must_use_candidate)]
//! LSP (Language Server Protocol) client registry for tool dispatch.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sandbox_launcher::{shell_command, SandboxLaunchSpec};
use serde::{Deserialize, Serialize};

/// Supported LSP actions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LspAction {
    Diagnostics,
    Hover,
    Definition,
    References,
    Completion,
    Symbols,
    Format,
}

impl LspAction {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "diagnostics" => Some(Self::Diagnostics),
            "hover" => Some(Self::Hover),
            "definition" | "goto_definition" => Some(Self::Definition),
            "references" | "find_references" => Some(Self::References),
            "completion" | "completions" => Some(Self::Completion),
            "symbols" | "document_symbols" => Some(Self::Symbols),
            "format" | "formatting" => Some(Self::Format),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LspDiagnostic {
    pub path: String,
    pub line: u32,
    pub character: u32,
    pub severity: String,
    pub message: String,
    pub source: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LspLocation {
    pub path: String,
    pub line: u32,
    pub character: u32,
    pub end_line: Option<u32>,
    pub end_character: Option<u32>,
    pub preview: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LspHoverResult {
    pub content: String,
    pub language: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LspCompletionItem {
    pub label: String,
    pub kind: Option<String>,
    pub detail: Option<String>,
    pub insert_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LspSymbol {
    pub name: String,
    pub kind: String,
    pub path: String,
    pub line: u32,
    pub character: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LspServerStatus {
    Connected,
    Disconnected,
    Starting,
    Error,
}

impl std::fmt::Display for LspServerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connected => write!(f, "connected"),
            Self::Disconnected => write!(f, "disconnected"),
            Self::Starting => write!(f, "starting"),
            Self::Error => write!(f, "error"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LspServerState {
    pub language: String,
    pub status: LspServerStatus,
    pub root_path: Option<String>,
    pub capabilities: Vec<String>,
    pub diagnostics: Vec<LspDiagnostic>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default)]
pub struct LspRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

#[derive(Debug, Default)]
struct RegistryInner {
    servers: HashMap<String, LspServerState>,
}

impl LspRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &self,
        language: &str,
        status: LspServerStatus,
        root_path: Option<&str>,
        capabilities: Vec<String>,
    ) {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("lsp registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.servers.insert(
            language.to_owned(),
            LspServerState {
                language: language.to_owned(),
                status,
                root_path: root_path.map(str::to_owned),
                capabilities,
                diagnostics: Vec::new(),
                command: None,
            },
        );
    }

    pub fn register_command(
        &self,
        language: &str,
        root_path: Option<&str>,
        capabilities: Vec<String>,
        command: Vec<String>,
    ) {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("lsp registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.servers.insert(
            language.to_owned(),
            LspServerState {
                language: language.to_owned(),
                status: LspServerStatus::Connected,
                root_path: root_path.map(str::to_owned),
                capabilities,
                diagnostics: Vec::new(),
                command: Some(command),
            },
        );
    }

    pub fn get(&self, language: &str) -> Option<LspServerState> {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("lsp registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.servers.get(language).cloned()
    }

    /// Find the appropriate server for a file path based on extension.
    pub fn find_server_for_path(&self, path: &str) -> Option<LspServerState> {
        let ext = std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");

        let language = match ext {
            "rs" => "rust",
            "ts" | "tsx" => "typescript",
            "js" | "jsx" => "javascript",
            "py" => "python",
            "go" => "go",
            "java" => "java",
            "c" | "h" => "c",
            "cpp" | "hpp" | "cc" => "cpp",
            "rb" => "ruby",
            "lua" => "lua",
            _ => return None,
        };

        self.get(language)
    }

    /// List all registered servers.
    pub fn list_servers(&self) -> Vec<LspServerState> {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("lsp registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.servers.values().cloned().collect()
    }

    /// Add diagnostics to a server.
    pub fn add_diagnostics(
        &self,
        language: &str,
        diagnostics: Vec<LspDiagnostic>,
    ) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("lsp registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let server = inner
            .servers
            .get_mut(language)
            .ok_or_else(|| format!("LSP server not found for language: {language}"))?;
        server.diagnostics.extend(diagnostics);
        Ok(())
    }

    /// Get diagnostics for a specific file path.
    pub fn get_diagnostics(&self, path: &str) -> Vec<LspDiagnostic> {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("lsp registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner
            .servers
            .values()
            .flat_map(|s| &s.diagnostics)
            .filter(|d| d.path == path)
            .cloned()
            .collect()
    }

    /// Clear diagnostics for a language server.
    pub fn clear_diagnostics(&self, language: &str) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("lsp registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let server = inner
            .servers
            .get_mut(language)
            .ok_or_else(|| format!("LSP server not found for language: {language}"))?;
        server.diagnostics.clear();
        Ok(())
    }

    /// Disconnect a server.
    pub fn disconnect(&self, language: &str) -> Option<LspServerState> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("lsp registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.servers.remove(language)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("lsp registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.servers.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Dispatch an LSP action and return a structured result.
    pub fn dispatch(
        &self,
        action: &str,
        path: Option<&str>,
        line: Option<u32>,
        character: Option<u32>,
        _query: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let lsp_action =
            LspAction::from_str(action).ok_or_else(|| format!("unknown LSP action: {action}"))?;

        // For diagnostics, we can check existing cached diagnostics
        if lsp_action == LspAction::Diagnostics {
            if let Some(path) = path {
                let diags = self.get_diagnostics(path);
                return Ok(serde_json::json!({
                    "action": "diagnostics",
                    "path": path,
                    "diagnostics": diags,
                    "count": diags.len()
                }));
            }
            // All diagnostics across all servers
            let inner = self.inner.lock().unwrap_or_else(|poisoned| {
                tracing::warn!("lsp registry lock poisoned; recovering");
                poisoned.into_inner()
            });
            let all_diags: Vec<_> = inner
                .servers
                .values()
                .flat_map(|s| &s.diagnostics)
                .collect();
            return Ok(serde_json::json!({
                "action": "diagnostics",
                "diagnostics": all_diags,
                "count": all_diags.len()
            }));
        }

        // For other actions, we need a connected server for the given file
        let path = path.ok_or("path is required for this LSP action")?;
        let server = self
            .find_server_for_path(path)
            .ok_or_else(|| format!("no LSP server available for path: {path}"))?;

        if server.status != LspServerStatus::Connected {
            return Ok(lsp_unavailable(
                action,
                path,
                &server.language,
                format!("server status is {}", server.status),
            ));
        }

        let Some(command) = server
            .command
            .as_ref()
            .filter(|command| !command.is_empty())
        else {
            return Ok(lsp_unavailable(
                action,
                path,
                &server.language,
                "no stdio JSON-RPC command registered",
            ));
        };

        match call_lsp_stdio(command, &server, lsp_action, path, line, character, _query) {
            Ok(result) => Ok(serde_json::json!({
                "action": action,
                "path": path,
                "line": line,
                "character": character,
                "language": server.language,
                "status": "ok",
                "result": result,
                "evidence": {
                    "transport": "lsp_stdio_json_rpc",
                    "server": server.language,
                    "capabilities": server.capabilities
                }
            })),
            Err(error) => Ok(lsp_unavailable(action, path, &server.language, error)),
        }
    }
}

fn lsp_unavailable(
    action: &str,
    path: &str,
    language: &str,
    reason: impl Into<String>,
) -> serde_json::Value {
    serde_json::json!({
        "action": action,
        "path": path,
        "language": language,
        "status": "unavailable",
        "unavailable_reason": reason.into(),
        "fallback": ["rg", "tree_sitter", "code_index"]
    })
}

fn call_lsp_stdio(
    command: &[String],
    server: &LspServerState,
    action: LspAction,
    path: &str,
    line: Option<u32>,
    character: Option<u32>,
    query: Option<&str>,
) -> Result<serde_json::Value, String> {
    let timeout = Duration::from_millis(
        std::env::var("COWD_LSP_TIMEOUT_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(3_000),
    );
    let command = command.to_vec();
    let server = server.clone();
    let path = path.to_string();
    let query = query.map(str::to_string);
    let (tx, rx) = std::sync::mpsc::channel();

    std::thread::spawn(move || {
        let result = call_lsp_stdio_inner(&command, &server, action, &path, line, character, query);
        let _ = tx.send(result);
    });

    rx.recv_timeout(timeout).map_err(|_| {
        format!(
            "LSP stdio request timed out after {}ms",
            timeout.as_millis()
        )
    })?
}

fn call_lsp_stdio_inner(
    command: &[String],
    server: &LspServerState,
    action: LspAction,
    path: &str,
    line: Option<u32>,
    character: Option<u32>,
    query: Option<String>,
) -> Result<serde_json::Value, String> {
    let workspace = server
        .root_path
        .as_deref()
        .map(PathBuf::from)
        .ok_or_else(|| "LSP server workspace root is not registered".to_string())?
        .canonicalize()
        .map_err(|error| format!("resolve LSP workspace failed: {error}"))?;
    let invocation = std::iter::once(command[0].as_str())
        .chain(command[1..].iter().map(String::as_str))
        .map(shell_quote)
        .collect::<Vec<_>>()
        .join(" ");
    let mut spec = SandboxLaunchSpec::workspace(&workspace);
    spec.working_directory = Some(workspace.clone());
    let prepared = shell_command(&format!("exec {invocation}"), &spec)
        .map_err(|error| format!("prepare hardened LSP sandbox failed: {error}"))?;
    let mut child = prepared
        .into_command()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("spawn LSP command failed: {error}"))?;

    {
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| "LSP command stdin unavailable".to_string())?;
        write_lsp_message(stdin, &initialize_request(1, &workspace))?;
        write_lsp_message(
            stdin,
            &action_request(2, action, path, line, character, query, &workspace),
        )?;
        stdin.flush().map_err(|error| error.to_string())?;
    }

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "LSP command stdout unavailable".to_string())?;
    let mut reader = BufReader::new(stdout);
    let mut last_response = None;
    for _ in 0..4 {
        let response = read_lsp_message(&mut reader)?;
        if response.get("id").and_then(serde_json::Value::as_i64) == Some(2) {
            let _ = child.kill();
            if let Some(error) = response.get("error") {
                return Err(format!("LSP error response: {error}"));
            }
            return Ok(response
                .get("result")
                .cloned()
                .unwrap_or(serde_json::Value::Null));
        }
        last_response = Some(response);
    }
    let _ = child.kill();
    Err(format!(
        "LSP action response not received; last_response={}",
        last_response
            .map(|value| value.to_string())
            .unwrap_or_else(|| "none".to_string())
    ))
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn initialize_request(id: u64, workspace: &Path) -> serde_json::Value {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "initialize",
        "params": {
            "rootUri": file_uri(workspace, workspace),
            "capabilities": {}
        }
    })
}

fn action_request(
    id: u64,
    action: LspAction,
    path: &str,
    line: Option<u32>,
    character: Option<u32>,
    query: Option<String>,
    workspace: &Path,
) -> serde_json::Value {
    let position = serde_json::json!({
        "line": line.unwrap_or(0),
        "character": character.unwrap_or(0)
    });
    let text_document = serde_json::json!({ "uri": file_uri(path, workspace) });
    let (method, params) = match action {
        LspAction::Symbols => (
            "textDocument/documentSymbol",
            serde_json::json!({ "textDocument": text_document }),
        ),
        LspAction::Definition => (
            "textDocument/definition",
            serde_json::json!({ "textDocument": text_document, "position": position }),
        ),
        LspAction::Hover => (
            "textDocument/hover",
            serde_json::json!({ "textDocument": text_document, "position": position }),
        ),
        LspAction::References => (
            "textDocument/references",
            serde_json::json!({
                "textDocument": text_document,
                "position": position,
                "context": { "includeDeclaration": true }
            }),
        ),
        LspAction::Completion => (
            "textDocument/completion",
            serde_json::json!({ "textDocument": text_document, "position": position }),
        ),
        LspAction::Format => (
            "textDocument/formatting",
            serde_json::json!({ "textDocument": text_document, "options": { "tabSize": 4, "insertSpaces": true } }),
        ),
        LspAction::Diagnostics => (
            "workspace/symbol",
            serde_json::json!({ "query": query.unwrap_or_default() }),
        ),
    };
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": method,
        "params": params
    })
}

fn file_uri(path: impl AsRef<Path>, workspace: &Path) -> String {
    let path = path.as_ref();
    if let Some(path) = path.to_str() {
        if path.starts_with("file://") {
            return path.to_string();
        }
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        workspace.join(path)
    };
    format!("file://{}", absolute.to_string_lossy().replace('\\', "/"))
}

fn write_lsp_message(writer: &mut dyn Write, payload: &serde_json::Value) -> Result<(), String> {
    let body = payload.to_string();
    write!(writer, "Content-Length: {}\r\n\r\n{}", body.len(), body)
        .map_err(|error| error.to_string())
}

fn read_lsp_message(reader: &mut BufReader<impl Read>) -> Result<serde_json::Value, String> {
    let mut content_length = None;
    loop {
        let mut line = String::new();
        let bytes = reader
            .read_line(&mut line)
            .map_err(|error| error.to_string())?;
        if bytes == 0 {
            return Err("LSP stdout closed before headers".to_string());
        }
        let trimmed = line.trim_end_matches(['\r', '\n']);
        if trimmed.is_empty() {
            break;
        }
        if let Some(value) = trimmed.strip_prefix("Content-Length:") {
            content_length = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|error| format!("invalid LSP Content-Length: {error}"))?,
            );
        }
    }
    let length = content_length.ok_or_else(|| "missing LSP Content-Length".to_string())?;
    let mut body = vec![0_u8; length];
    reader
        .read_exact(&mut body)
        .map_err(|error| error.to_string())?;
    serde_json::from_slice(&body).map_err(|error| format!("invalid LSP JSON response: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framed_json(value: &serde_json::Value) -> String {
        let body = value.to_string();
        format!("Content-Length: {}\r\n\r\n{}", body.len(), body)
    }

    fn shell_quote(value: &str) -> String {
        format!("'{}'", value.replace('\'', "'\"'\"'"))
    }

    fn chrono_like_test_nonce() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    }

    #[test]
    fn registers_and_retrieves_server() {
        let registry = LspRegistry::new();
        registry.register(
            "rust",
            LspServerStatus::Connected,
            Some("/workspace"),
            vec!["hover".into(), "completion".into()],
        );

        let server = registry.get("rust").expect("should exist");
        assert_eq!(server.language, "rust");
        assert_eq!(server.status, LspServerStatus::Connected);
        assert_eq!(server.capabilities.len(), 2);
    }

    #[test]
    fn finds_server_by_file_extension() {
        let registry = LspRegistry::new();
        registry.register("rust", LspServerStatus::Connected, None, vec![]);
        registry.register("typescript", LspServerStatus::Connected, None, vec![]);

        let rs_server = registry.find_server_for_path("src/main.rs").unwrap();
        assert_eq!(rs_server.language, "rust");

        let ts_server = registry.find_server_for_path("src/index.ts").unwrap();
        assert_eq!(ts_server.language, "typescript");

        assert!(registry.find_server_for_path("data.csv").is_none());
    }

    #[test]
    fn manages_diagnostics() {
        let registry = LspRegistry::new();
        registry.register("rust", LspServerStatus::Connected, None, vec![]);

        registry
            .add_diagnostics(
                "rust",
                vec![LspDiagnostic {
                    path: "src/main.rs".into(),
                    line: 10,
                    character: 5,
                    severity: "error".into(),
                    message: "mismatched types".into(),
                    source: Some("rust-analyzer".into()),
                }],
            )
            .unwrap();

        let diags = registry.get_diagnostics("src/main.rs");
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].message, "mismatched types");

        registry.clear_diagnostics("rust").unwrap();
        assert!(registry.get_diagnostics("src/main.rs").is_empty());
    }

    #[test]
    fn dispatches_diagnostics_action() {
        let registry = LspRegistry::new();
        registry.register("rust", LspServerStatus::Connected, None, vec![]);
        registry
            .add_diagnostics(
                "rust",
                vec![LspDiagnostic {
                    path: "src/lib.rs".into(),
                    line: 1,
                    character: 0,
                    severity: "warning".into(),
                    message: "unused import".into(),
                    source: None,
                }],
            )
            .unwrap();

        let result = registry
            .dispatch("diagnostics", Some("src/lib.rs"), None, None, None)
            .unwrap();
        assert_eq!(result["count"], 1);
    }

    #[test]
    fn dispatches_hover_action() {
        let registry = LspRegistry::new();
        registry.register("rust", LspServerStatus::Connected, None, vec![]);

        let result = registry
            .dispatch("hover", Some("src/main.rs"), Some(10), Some(5), None)
            .unwrap();
        assert_eq!(result["action"], "hover");
        assert_eq!(result["language"], "rust");
        assert_eq!(result["status"], "unavailable");
        assert!(result["unavailable_reason"]
            .as_str()
            .unwrap()
            .contains("no stdio JSON-RPC command registered"));
    }

    #[test]
    fn rejects_action_on_disconnected_server() {
        let registry = LspRegistry::new();
        registry.register("rust", LspServerStatus::Disconnected, None, vec![]);

        let result = registry
            .dispatch("hover", Some("src/main.rs"), Some(1), Some(0), None)
            .unwrap();
        assert_eq!(result["status"], "unavailable");
        assert!(result["unavailable_reason"]
            .as_str()
            .unwrap()
            .contains("disconnected"));
    }

    #[test]
    fn rejects_unknown_action() {
        let registry = LspRegistry::new();
        assert!(registry
            .dispatch("unknown_action", Some("file.rs"), None, None, None)
            .is_err());
    }

    #[test]
    fn disconnects_server() {
        let registry = LspRegistry::new();
        registry.register("rust", LspServerStatus::Connected, None, vec![]);
        assert_eq!(registry.len(), 1);

        let removed = registry.disconnect("rust");
        assert!(removed.is_some());
        assert!(registry.is_empty());
    }

    #[test]
    fn lsp_action_from_str_all_aliases() {
        // given
        let cases = [
            ("diagnostics", Some(LspAction::Diagnostics)),
            ("hover", Some(LspAction::Hover)),
            ("definition", Some(LspAction::Definition)),
            ("goto_definition", Some(LspAction::Definition)),
            ("references", Some(LspAction::References)),
            ("find_references", Some(LspAction::References)),
            ("completion", Some(LspAction::Completion)),
            ("completions", Some(LspAction::Completion)),
            ("symbols", Some(LspAction::Symbols)),
            ("document_symbols", Some(LspAction::Symbols)),
            ("format", Some(LspAction::Format)),
            ("formatting", Some(LspAction::Format)),
            ("unknown", None),
        ];

        // when
        let resolved: Vec<_> = cases
            .into_iter()
            .map(|(input, expected)| (input, LspAction::from_str(input), expected))
            .collect();

        // then
        for (input, actual, expected) in resolved {
            assert_eq!(actual, expected, "unexpected action resolution for {input}");
        }
    }

    #[test]
    fn lsp_server_status_display_all_variants() {
        // given
        let cases = [
            (LspServerStatus::Connected, "connected"),
            (LspServerStatus::Disconnected, "disconnected"),
            (LspServerStatus::Starting, "starting"),
            (LspServerStatus::Error, "error"),
        ];

        // when
        let rendered: Vec<_> = cases
            .into_iter()
            .map(|(status, expected)| (status.to_string(), expected))
            .collect();

        // then
        assert_eq!(
            rendered,
            vec![
                ("connected".to_string(), "connected"),
                ("disconnected".to_string(), "disconnected"),
                ("starting".to_string(), "starting"),
                ("error".to_string(), "error"),
            ]
        );
    }

    #[test]
    fn dispatch_diagnostics_without_path_aggregates() {
        // given
        let registry = LspRegistry::new();
        registry.register("rust", LspServerStatus::Connected, None, vec![]);
        registry.register("python", LspServerStatus::Connected, None, vec![]);
        registry
            .add_diagnostics(
                "rust",
                vec![LspDiagnostic {
                    path: "src/lib.rs".into(),
                    line: 1,
                    character: 0,
                    severity: "warning".into(),
                    message: "unused import".into(),
                    source: Some("rust-analyzer".into()),
                }],
            )
            .expect("rust diagnostics should add");
        registry
            .add_diagnostics(
                "python",
                vec![LspDiagnostic {
                    path: "script.py".into(),
                    line: 2,
                    character: 4,
                    severity: "error".into(),
                    message: "undefined name".into(),
                    source: Some("pyright".into()),
                }],
            )
            .expect("python diagnostics should add");

        // when
        let result = registry
            .dispatch("diagnostics", None, None, None, None)
            .expect("aggregate diagnostics should work");

        // then
        assert_eq!(result["action"], "diagnostics");
        assert_eq!(result["count"], 2);
        assert_eq!(result["diagnostics"].as_array().map(Vec::len), Some(2));
    }

    #[test]
    fn dispatch_non_diagnostics_requires_path() {
        // given
        let registry = LspRegistry::new();

        // when
        let result = registry.dispatch("hover", None, Some(1), Some(0), None);

        // then
        assert_eq!(
            result.expect_err("path should be required"),
            "path is required for this LSP action"
        );
    }

    #[test]
    fn dispatch_no_server_for_path_errors() {
        // given
        let registry = LspRegistry::new();

        // when
        let result = registry.dispatch("hover", Some("notes.md"), Some(1), Some(0), None);

        // then
        let error = result.expect_err("missing server should fail");
        assert!(error.contains("no LSP server available for path: notes.md"));
    }

    #[test]
    fn dispatch_disconnected_server_error_payload() {
        // given
        let registry = LspRegistry::new();
        registry.register("typescript", LspServerStatus::Disconnected, None, vec![]);

        // when
        let result = registry.dispatch("hover", Some("src/index.ts"), Some(3), Some(2), None);

        // then
        let result = result.expect("disconnected server should return unavailable payload");
        assert_eq!(result["language"], "typescript");
        assert_eq!(result["status"], "unavailable");
        assert!(result["unavailable_reason"]
            .as_str()
            .unwrap()
            .contains("disconnected"));
    }

    #[cfg(unix)]
    #[test]
    fn dispatch_symbols_uses_stdio_json_rpc_when_command_registered() {
        use std::os::unix::fs::PermissionsExt;

        let init = framed_json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": { "capabilities": { "documentSymbolProvider": true } }
        }));
        let symbols = framed_json(&serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "result": [{
                "name": "main",
                "kind": 12,
                "range": {
                    "start": { "line": 1, "character": 0 },
                    "end": { "line": 1, "character": 9 }
                },
                "selectionRange": {
                    "start": { "line": 1, "character": 3 },
                    "end": { "line": 1, "character": 7 }
                }
            }]
        }));
        let workspace = std::env::temp_dir().join(format!(
            "cowd-fake-lsp-{}-{}",
            std::process::id(),
            chrono_like_test_nonce()
        ));
        std::fs::create_dir_all(workspace.join("src")).unwrap();
        let script_path = workspace.join("fake-lsp.sh");
        std::fs::write(
            &script_path,
            format!(
                "#!/bin/sh\ncat >/dev/null &\nprintf %s {}\nprintf %s {}\n",
                shell_quote(&init),
                shell_quote(&symbols)
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&script_path).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&script_path, permissions).unwrap();

        let registry = LspRegistry::new();
        registry.register_command(
            "rust",
            Some(workspace.to_str().unwrap()),
            vec!["textDocument/documentSymbol".into()],
            vec![script_path.to_string_lossy().to_string()],
        );

        let result = registry
            .dispatch("symbols", Some("src/main.rs"), None, None, None)
            .unwrap();

        assert_eq!(result["status"], "ok");
        assert_eq!(result["result"][0]["name"], "main");
        assert_eq!(result["evidence"]["transport"], "lsp_stdio_json_rpc");

        let _ = std::fs::remove_dir_all(workspace);
    }

    #[test]
    fn find_server_for_all_extensions() {
        // given
        let registry = LspRegistry::new();
        for language in [
            "rust",
            "typescript",
            "javascript",
            "python",
            "go",
            "java",
            "c",
            "cpp",
            "ruby",
            "lua",
        ] {
            registry.register(language, LspServerStatus::Connected, None, vec![]);
        }
        let cases = [
            ("src/main.rs", "rust"),
            ("src/index.ts", "typescript"),
            ("src/view.tsx", "typescript"),
            ("src/app.js", "javascript"),
            ("src/app.jsx", "javascript"),
            ("script.py", "python"),
            ("main.go", "go"),
            ("Main.java", "java"),
            ("native.c", "c"),
            ("native.h", "c"),
            ("native.cpp", "cpp"),
            ("native.hpp", "cpp"),
            ("native.cc", "cpp"),
            ("script.rb", "ruby"),
            ("script.lua", "lua"),
        ];

        // when
        let resolved: Vec<_> = cases
            .into_iter()
            .map(|(path, expected)| {
                (
                    path,
                    registry
                        .find_server_for_path(path)
                        .map(|server| server.language),
                    expected,
                )
            })
            .collect();

        // then
        for (path, actual, expected) in resolved {
            assert_eq!(
                actual.as_deref(),
                Some(expected),
                "unexpected mapping for {path}"
            );
        }
    }

    #[test]
    fn find_server_for_path_no_extension() {
        // given
        let registry = LspRegistry::new();
        registry.register("rust", LspServerStatus::Connected, None, vec![]);

        // when
        let result = registry.find_server_for_path("Makefile");

        // then
        assert!(result.is_none());
    }

    #[test]
    fn list_servers_with_multiple() {
        // given
        let registry = LspRegistry::new();
        registry.register("rust", LspServerStatus::Connected, None, vec![]);
        registry.register("typescript", LspServerStatus::Starting, None, vec![]);
        registry.register("python", LspServerStatus::Error, None, vec![]);

        // when
        let servers = registry.list_servers();

        // then
        assert_eq!(servers.len(), 3);
        assert!(servers.iter().any(|server| server.language == "rust"));
        assert!(servers.iter().any(|server| server.language == "typescript"));
        assert!(servers.iter().any(|server| server.language == "python"));
    }

    #[test]
    fn get_missing_server_returns_none() {
        // given
        let registry = LspRegistry::new();

        // when
        let server = registry.get("missing");

        // then
        assert!(server.is_none());
    }

    #[test]
    fn add_diagnostics_missing_language_errors() {
        // given
        let registry = LspRegistry::new();

        // when
        let result = registry.add_diagnostics("missing", vec![]);

        // then
        let error = result.expect_err("missing language should fail");
        assert!(error.contains("LSP server not found for language: missing"));
    }

    #[test]
    fn get_diagnostics_across_servers() {
        // given
        let registry = LspRegistry::new();
        let shared_path = "shared/file.txt";
        registry.register("rust", LspServerStatus::Connected, None, vec![]);
        registry.register("python", LspServerStatus::Connected, None, vec![]);
        registry
            .add_diagnostics(
                "rust",
                vec![LspDiagnostic {
                    path: shared_path.into(),
                    line: 4,
                    character: 1,
                    severity: "warning".into(),
                    message: "warn".into(),
                    source: None,
                }],
            )
            .expect("rust diagnostics should add");
        registry
            .add_diagnostics(
                "python",
                vec![LspDiagnostic {
                    path: shared_path.into(),
                    line: 8,
                    character: 3,
                    severity: "error".into(),
                    message: "err".into(),
                    source: None,
                }],
            )
            .expect("python diagnostics should add");

        // when
        let diagnostics = registry.get_diagnostics(shared_path);

        // then
        assert_eq!(diagnostics.len(), 2);
        assert!(diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message == "warn"));
        assert!(diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message == "err"));
    }

    #[test]
    fn clear_diagnostics_missing_language_errors() {
        // given
        let registry = LspRegistry::new();

        // when
        let result = registry.clear_diagnostics("missing");

        // then
        let error = result.expect_err("missing language should fail");
        assert!(error.contains("LSP server not found for language: missing"));
    }
}
