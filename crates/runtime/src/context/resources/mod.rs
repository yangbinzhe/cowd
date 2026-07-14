use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::{Arc, RwLock},
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::cowd_dirs;

pub const MAX_RESOURCE_BYTES: u64 = 100 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceKind {
    Image,
    Audio,
    Video,
    Pdf,
    Text,
    Markdown,
    Csv,
    Document,
    Archive,
    Code,
    Binary,
    Unknown,
}

impl ResourceKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Video => "video",
            Self::Pdf => "pdf",
            Self::Text => "text",
            Self::Markdown => "markdown",
            Self::Csv => "csv",
            Self::Document => "document",
            Self::Archive => "archive",
            Self::Code => "code",
            Self::Binary => "binary",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceEnvelope {
    pub id: String,
    pub uri: String,
    pub source: String,
    pub source_message_id: Option<String>,
    pub session_id: Option<String>,
    pub original_name: String,
    pub declared_mime: Option<String>,
    pub detected_mime: Option<String>,
    pub kind: ResourceKind,
    pub size_bytes: u64,
    pub sha256: String,
    pub storage_path: PathBuf,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceHint {
    pub resource_id: String,
    pub kind: ResourceKind,
    pub confidence: String,
    pub native_model_support: String,
    pub recommended_directions: Vec<String>,
    pub available_tools: Vec<String>,
    pub available_skills: Vec<String>,
    pub available_plugins: Vec<String>,
    pub available_mcp_resources: Vec<String>,
    pub available_local_commands: Vec<String>,
    pub missing_capabilities: Vec<String>,
    pub permission_required: Vec<String>,
    pub safe_next_steps: Vec<String>,
    pub guardrails: Vec<String>,
}

/// Bounded attachment metadata that may enter a model request.
///
/// `ResourceHint` remains the full UI/diagnostic projection. This type makes
/// the request boundary explicit so installed capability inventories and
/// remediation detail cannot grow every prompt linearly.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourcePromptHint {
    pub resource_id: String,
    pub uri: String,
    pub name: String,
    pub kind: ResourceKind,
    pub detected_mime: Option<String>,
    pub size_bytes: u64,
    pub native_model_support: String,
    pub recommended_directions: Vec<String>,
    pub guardrails: Vec<String>,
}

impl ResourceHint {
    #[must_use]
    pub fn prompt_hint(&self, envelope: &ResourceEnvelope) -> ResourcePromptHint {
        ResourcePromptHint {
            resource_id: self.resource_id.clone(),
            uri: envelope.uri.clone(),
            name: envelope.original_name.clone(),
            kind: self.kind,
            detected_mime: envelope.detected_mime.clone(),
            size_bytes: envelope.size_bytes,
            native_model_support: self.native_model_support.clone(),
            recommended_directions: self
                .recommended_directions
                .iter()
                .take(3)
                .cloned()
                .collect(),
            guardrails: self.guardrails.iter().take(2).cloned().collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceEvidence {
    pub resource_id: String,
    pub turn_id: Option<String>,
    pub action: String,
    pub actor: String,
    pub tool_or_skill: Option<String>,
    pub status: String,
    pub summary: String,
    pub artifact_path: Option<PathBuf>,
    pub error_summary: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceCapabilitySnapshot {
    pub tools: Vec<String>,
    pub skills: Vec<String>,
    pub plugins: Vec<String>,
    pub mcp_resources: Vec<String>,
    pub local_commands: Vec<String>,
    pub provider_native: Vec<String>,
}

impl Default for ResourceCapabilitySnapshot {
    fn default() -> Self {
        Self::core_runtime()
    }
}

impl ResourceCapabilitySnapshot {
    /// A stable, no-I/O baseline used on the request path. Dynamic service
    /// discovery belongs to a refreshed runtime snapshot, never attachment
    /// rendering.
    #[must_use]
    pub fn core_runtime() -> Self {
        Self {
            tools: vec![
                "read_file".to_string(),
                "read_many".to_string(),
                "grep_many".to_string(),
                "glob_many".to_string(),
                "tool_batch_readonly".to_string(),
                "vision_analyze".to_string(),
                "execute_code".to_string(),
            ],
            skills: Vec::new(),
            plugins: Vec::new(),
            mcp_resources: vec![
                "ListMcpResources".to_string(),
                "ReadMcpResource".to_string(),
            ],
            local_commands: Vec::new(),
            provider_native: vec!["image_input_when_supported".to_string()],
        }
    }

    /// Performs optional environment discovery outside the request path.
    #[must_use]
    pub fn discover_environment() -> Self {
        let mut snapshot = Self::core_runtime();
        snapshot.skills = list_installed_names(cowd_dirs::user_skills_dir());
        snapshot.plugins = list_installed_names(cowd_dirs::user_plugins_dir());
        snapshot.local_commands = detect_local_commands(&[
            "file",
            "ffprobe",
            "ffmpeg",
            "pdftotext",
            "pdfinfo",
            "pandoc",
            "python3",
            "unzip",
        ]);
        snapshot
    }
}

/// Runtime-owned capability snapshot for attachment advice. It is refreshed
/// when service/plugin configuration changes; rendering only clones the most
/// recently validated state and never scans disks or shells out.
#[derive(Clone, Debug)]
pub struct ResourceCapabilityIndex {
    snapshot: Arc<RwLock<ResourceCapabilitySnapshot>>,
}

impl Default for ResourceCapabilityIndex {
    fn default() -> Self {
        Self::new(ResourceCapabilitySnapshot::default())
    }
}

impl ResourceCapabilityIndex {
    #[must_use]
    pub fn new(snapshot: ResourceCapabilitySnapshot) -> Self {
        Self {
            snapshot: Arc::new(RwLock::new(snapshot)),
        }
    }

    #[must_use]
    pub fn snapshot(&self) -> ResourceCapabilitySnapshot {
        self.snapshot
            .read()
            .map(|snapshot| snapshot.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone())
    }

    pub fn refresh(&self, snapshot: ResourceCapabilitySnapshot) {
        if let Ok(mut current) = self.snapshot.write() {
            *current = snapshot;
        }
    }

    #[must_use]
    pub fn refresh_from_environment(&self) -> ResourceCapabilitySnapshot {
        let snapshot = ResourceCapabilitySnapshot::discover_environment();
        self.refresh(snapshot.clone());
        snapshot
    }
}

#[derive(Debug, Clone)]
pub struct ResourceStore {
    root: PathBuf,
    capabilities: ResourceCapabilityIndex,
}

impl ResourceStore {
    #[must_use]
    pub fn default_for_config_home(config_home: &Path) -> Self {
        Self::for_config_home_with_capabilities(config_home, ResourceCapabilityIndex::default())
    }

    /// Builds a resource store backed by the Gateway-owned capability index.
    ///
    /// Callers on a service request path must pass the shared index rather than
    /// creating a fresh one. That keeps attachment rendering free of discovery
    /// I/O while allowing lifecycle reloads to atomically publish a new view.
    #[must_use]
    pub fn for_config_home_with_capabilities(
        config_home: &Path,
        capabilities: ResourceCapabilityIndex,
    ) -> Self {
        Self {
            root: config_home.join("storage").join("resources"),
            capabilities,
        }
    }

    #[must_use]
    pub fn default_user() -> Self {
        Self::default_for_config_home(&cowd_dirs::config_home_dir())
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn capability_index(&self) -> &ResourceCapabilityIndex {
        &self.capabilities
    }

    pub fn refresh_capabilities(&self, snapshot: ResourceCapabilitySnapshot) {
        self.capabilities.refresh(snapshot);
    }

    pub fn register_resource_from_path(
        &self,
        path: impl AsRef<Path>,
        source: impl Into<String>,
        source_message_id: Option<String>,
        session_id: Option<String>,
        declared_mime: Option<String>,
    ) -> Result<(ResourceEnvelope, ResourceHint), String> {
        let path = path.as_ref();
        if !path.is_file() {
            return Err(format!("resource path is not a file: {}", path.display()));
        }
        let size_bytes = fs::metadata(path)
            .map_err(|e| format!("read resource metadata: {e}"))?
            .len();
        if size_bytes > MAX_RESOURCE_BYTES {
            return Err(format!(
                "resource is too large: {} bytes exceeds {} bytes",
                size_bytes, MAX_RESOURCE_BYTES
            ));
        }
        let mut file = fs::File::open(path).map_err(|e| format!("open resource: {e}"))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|e| format!("read resource: {e}"))?;
        let sha256 = sha256_hex(&bytes);
        let original_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("resource.bin")
            .to_string();
        let extension = Path::new(&original_name)
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase());
        let detected_mime = detect_mime(path, declared_mime.as_deref(), extension.as_deref());
        let kind = detect_kind(&original_name, detected_mime.as_deref());
        let id = format!("res_{}", Uuid::new_v4().simple());
        let storage_path = self.object_path(&sha256, extension.as_deref());
        if let Some(parent) = storage_path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("create resource object dir: {e}"))?;
        }
        if !storage_path.exists() {
            fs::write(&storage_path, &bytes).map_err(|e| format!("write resource object: {e}"))?;
        }
        let envelope = ResourceEnvelope {
            id: id.clone(),
            uri: format!("resource://{id}"),
            source: source.into(),
            source_message_id,
            session_id,
            original_name,
            declared_mime,
            detected_mime,
            kind,
            size_bytes: bytes.len() as u64,
            sha256: format!("sha256:{sha256}"),
            storage_path,
            created_at: Utc::now(),
            metadata: serde_json::json!({
                "input_path": path.display().to_string(),
            }),
        };
        self.write_metadata(&envelope)?;
        let capabilities = self.capabilities.snapshot();
        let hint = resource_hint(&envelope, &capabilities);
        self.append_evidence(ResourceEvidence {
            resource_id: envelope.id.clone(),
            turn_id: None,
            action: "register_resource_from_path".to_string(),
            actor: "runtime.resource_store".to_string(),
            tool_or_skill: None,
            status: "stored".to_string(),
            summary: format!(
                "Stored {} resource {} as {}",
                envelope.kind.as_str(),
                envelope.original_name,
                envelope.uri
            ),
            artifact_path: Some(envelope.storage_path.clone()),
            error_summary: None,
            created_at: Utc::now(),
        })?;
        Ok((envelope, hint))
    }

    pub fn get(&self, id: &str) -> Result<ResourceEnvelope, String> {
        let path = self.metadata_path(id);
        let raw = fs::read_to_string(&path)
            .map_err(|e| format!("resource metadata not found for {id}: {e}"))?;
        serde_json::from_str(&raw).map_err(|e| format!("decode resource metadata for {id}: {e}"))
    }

    pub fn evidence(&self, id: &str) -> Vec<ResourceEvidence> {
        let path = self.evidence_path(id);
        fs::read_to_string(path)
            .ok()
            .map(|raw| {
                raw.lines()
                    .filter_map(|line| serde_json::from_str::<ResourceEvidence>(line).ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn append_evidence(&self, evidence: ResourceEvidence) -> Result<(), String> {
        let path = self.evidence_path(&evidence.resource_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("create resource evidence dir: {e}"))?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| format!("open resource evidence: {e}"))?;
        let line = serde_json::to_string(&evidence)
            .map_err(|e| format!("encode resource evidence: {e}"))?;
        file.write_all(line.as_bytes())
            .and_then(|_| file.write_all(b"\n"))
            .map_err(|e| format!("write resource evidence: {e}"))
    }

    fn object_path(&self, sha256: &str, extension: Option<&str>) -> PathBuf {
        let prefix = sha256.chars().take(2).collect::<String>();
        let file_name = match extension.filter(|ext| !ext.trim().is_empty()) {
            Some(extension) => format!("{sha256}.{extension}"),
            None => sha256.to_string(),
        };
        self.root.join("objects").join(prefix).join(file_name)
    }

    fn metadata_path(&self, id: &str) -> PathBuf {
        self.root.join("metadata").join(format!("{id}.json"))
    }

    fn evidence_path(&self, id: &str) -> PathBuf {
        self.root.join("evidence").join(format!("{id}.jsonl"))
    }

    fn write_metadata(&self, envelope: &ResourceEnvelope) -> Result<(), String> {
        let path = self.metadata_path(&envelope.id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("create resource metadata dir: {e}"))?;
        }
        let rendered = serde_json::to_string_pretty(envelope)
            .map_err(|e| format!("encode resource metadata: {e}"))?;
        fs::write(path, rendered).map_err(|e| format!("write resource metadata: {e}"))
    }
}

pub fn register_resource_from_path(
    config_home: &Path,
    path: impl AsRef<Path>,
    source: impl Into<String>,
    source_message_id: Option<String>,
    session_id: Option<String>,
    declared_mime: Option<String>,
) -> Result<(ResourceEnvelope, ResourceHint), String> {
    ResourceStore::default_for_config_home(config_home).register_resource_from_path(
        path,
        source,
        source_message_id,
        session_id,
        declared_mime,
    )
}

#[must_use]
pub fn resource_hint(
    envelope: &ResourceEnvelope,
    capabilities: &ResourceCapabilitySnapshot,
) -> ResourceHint {
    let mut recommended_directions = Vec::new();
    let mut missing_capabilities = Vec::new();
    let mut permission_required = Vec::new();
    let mut guardrails = Vec::new();

    match envelope.kind {
        ResourceKind::Image => {
            recommended_directions.push(
                "Use the existing structured image input when available; otherwise call vision_analyze with the stored path.".to_string(),
            );
            guardrails.push("Do not OCR or describe visual content unless image input or vision_analyze actually inspected it.".to_string());
        }
        ResourceKind::Audio => {
            recommended_directions
                .push("Use ffprobe/ffmpeg for metadata or normalization when useful.".to_string());
            recommended_directions.push(
                "If spoken content is required, use or install a transcription skill/plugin before claiming content.".to_string(),
            );
            missing_capabilities.push(
                "audio transcription skill/plugin if content understanding is required".to_string(),
            );
            permission_required.push(
                "Installing a transcription model, Python package, or sidecar requires explicit permission.".to_string(),
            );
            guardrails.push("Do not claim audio content before a real transcription or native audio understanding path succeeds.".to_string());
        }
        ResourceKind::Pdf => {
            recommended_directions.push(
                "Use pdftotext/pdfinfo or the existing PDF extraction tool when text is required."
                    .to_string(),
            );
            guardrails.push(
                "Report extraction confidence when PDF text is partial or unavailable.".to_string(),
            );
        }
        ResourceKind::Text | ResourceKind::Markdown | ResourceKind::Code => {
            recommended_directions.push(
                "Use read_file/read_many or batch readonly tools to inspect text content directly."
                    .to_string(),
            );
        }
        ResourceKind::Csv => {
            recommended_directions.push(
                "Use execute_code or a table-oriented skill to sample schema and rows when analysis is needed.".to_string(),
            );
        }
        ResourceKind::Document => {
            recommended_directions.push(
                "Try pandoc/unzip or an installed document skill/plugin; install Office parsers only when needed.".to_string(),
            );
            missing_capabilities
                .push("document-specific parser skill/plugin may be required".to_string());
        }
        ResourceKind::Video => {
            recommended_directions.push(
                "Use ffprobe for metadata; use ffmpeg to extract audio or frames when task requires content.".to_string(),
            );
            missing_capabilities.push(
                "video understanding or transcription skill/plugin may be required".to_string(),
            );
        }
        ResourceKind::Archive => {
            recommended_directions.push(
                "Use unzip to list contents first; only extract selected files needed for the task.".to_string(),
            );
        }
        ResourceKind::Binary | ResourceKind::Unknown => {
            recommended_directions.push(
                "Use file/magic bytes to classify; do not infer content from the filename alone."
                    .to_string(),
            );
            missing_capabilities.push("specific parser for this binary format".to_string());
            guardrails.push("If no parser is available, explain the boundary and suggest how to add capability.".to_string());
        }
    }

    ResourceHint {
        resource_id: envelope.id.clone(),
        kind: envelope.kind,
        confidence: if envelope.detected_mime.is_some() {
            "medium".to_string()
        } else {
            "low".to_string()
        },
        native_model_support: match envelope.kind {
            ResourceKind::Image => "use structured image input when current provider supports it"
                .to_string(),
            _ => "not assumed; use tools/skills/plugins/MCP unless provider explicitly supports this file kind".to_string(),
        },
        recommended_directions,
        available_tools: capabilities.tools.clone(),
        available_skills: capabilities.skills.clone(),
        available_plugins: capabilities.plugins.clone(),
        available_mcp_resources: capabilities.mcp_resources.clone(),
        available_local_commands: capabilities.local_commands.clone(),
        missing_capabilities,
        permission_required,
        safe_next_steps: vec![
            "Decide whether the task actually needs this resource content.".to_string(),
            "Use the narrowest existing native/tool/skill/plugin path first.".to_string(),
            "If no capability exists, request permission to install or construct one.".to_string(),
            "If still blocked, reply with saved resource id, detected type, attempted path, and remaining boundary.".to_string(),
        ],
        guardrails,
    }
}

#[must_use]
pub fn render_resource_context_markdown(resources: &[ResourcePromptHint]) -> String {
    if resources.is_empty() {
        return String::new();
    }
    let mut rendered = String::from("\n\n## Attached Resources\n\n");
    for hint in resources {
        rendered.push_str(&format!("### {}\n", hint.uri));
        rendered.push_str(&format!("- name: {}\n", hint.name));
        rendered.push_str(&format!("- kind: {}\n", hint.kind.as_str()));
        if let Some(mime) = &hint.detected_mime {
            rendered.push_str(&format!("- detected_mime: {mime}\n"));
        }
        rendered.push_str(&format!("- size: {} bytes\n", hint.size_bytes));
        rendered.push_str("- status: stored\n");
        rendered.push_str(&format!(
            "- native_model_support: {}\n",
            hint.native_model_support
        ));
        rendered.push_str("- recommended_directions:\n");
        for direction in &hint.recommended_directions {
            rendered.push_str(&format!("  - {direction}\n"));
        }
        if !hint.guardrails.is_empty() {
            rendered.push_str("- guardrails:\n");
            for guardrail in &hint.guardrails {
                rendered.push_str(&format!("  - {guardrail}\n"));
            }
        }
        rendered.push('\n');
    }
    rendered.push_str(
        "Resource handling principle: use native support or an actually exposed tool first. Query runtime capabilities only when a narrower parser/skill/plugin path is needed. Do not invent unseen content.\n",
    );
    rendered
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn detect_mime(path: &Path, declared: Option<&str>, extension: Option<&str>) -> Option<String> {
    if let Some(declared) = declared.filter(|value| !value.trim().is_empty()) {
        if declared != "application/octet-stream" {
            return Some(declared.to_string());
        }
    }
    if let Some(from_extension) = extension.and_then(mime_from_extension) {
        return Some(from_extension.to_string());
    }
    if let Some(from_file) = mime_from_file_command(path) {
        if !from_file.trim().is_empty() {
            return Some(from_file);
        }
    }
    None
}

fn mime_from_file_command(path: &Path) -> Option<String> {
    let output = Command::new("file")
        .arg("--brief")
        .arg("--mime-type")
        .arg(path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let mime = String::from_utf8_lossy(&output.stdout).trim().to_string();
    (!mime.is_empty()).then_some(mime)
}

fn mime_from_extension(extension: &str) -> Option<&'static str> {
    match extension {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "mp3" => Some("audio/mpeg"),
        "wav" => Some("audio/wav"),
        "m4a" => Some("audio/mp4"),
        "ogg" | "opus" => Some("audio/ogg"),
        "mp4" => Some("video/mp4"),
        "mov" => Some("video/quicktime"),
        "webm" => Some("video/webm"),
        "pdf" => Some("application/pdf"),
        "txt" => Some("text/plain"),
        "md" | "markdown" => Some("text/markdown"),
        "json" => Some("application/json"),
        "yaml" | "yml" => Some("application/yaml"),
        "csv" => Some("text/csv"),
        "zip" => Some("application/zip"),
        "docx" => Some("application/vnd.openxmlformats-officedocument.wordprocessingml.document"),
        "xlsx" => Some("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        "pptx" => Some("application/vnd.openxmlformats-officedocument.presentationml.presentation"),
        "rs" | "py" | "js" | "ts" | "tsx" | "jsx" | "go" | "java" | "c" | "h" | "cpp" | "hpp"
        | "html" | "css" | "vue" | "toml" => Some("text/plain"),
        _ => None,
    }
}

fn detect_kind(original_name: &str, mime: Option<&str>) -> ResourceKind {
    let extension = Path::new(original_name)
        .extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.to_ascii_lowercase());
    if let Some(mime) = mime {
        if mime.starts_with("image/") {
            return ResourceKind::Image;
        }
        if mime.starts_with("audio/") {
            return ResourceKind::Audio;
        }
        if mime.starts_with("video/") {
            return ResourceKind::Video;
        }
        if mime == "application/pdf" {
            return ResourceKind::Pdf;
        }
        if mime.contains("zip") {
            return ResourceKind::Archive;
        }
        if mime.contains("wordprocessingml")
            || mime.contains("spreadsheetml")
            || mime.contains("presentationml")
        {
            return ResourceKind::Document;
        }
        if mime.starts_with("text/") || mime == "application/json" || mime == "application/yaml" {
            return match extension.as_deref() {
                Some("md" | "markdown") => ResourceKind::Markdown,
                Some("csv") => ResourceKind::Csv,
                Some(
                    "rs" | "py" | "js" | "ts" | "tsx" | "jsx" | "go" | "java" | "c" | "h" | "cpp"
                    | "hpp" | "html" | "css" | "vue" | "toml",
                ) => ResourceKind::Code,
                _ => ResourceKind::Text,
            };
        }
    }
    match extension.as_deref() {
        Some("png" | "jpg" | "jpeg" | "gif" | "webp") => ResourceKind::Image,
        Some("mp3" | "wav" | "m4a" | "ogg" | "opus") => ResourceKind::Audio,
        Some("mp4" | "mov" | "webm") => ResourceKind::Video,
        Some("pdf") => ResourceKind::Pdf,
        Some("md" | "markdown") => ResourceKind::Markdown,
        Some("csv") => ResourceKind::Csv,
        Some("txt" | "json" | "yaml" | "yml") => ResourceKind::Text,
        Some("zip" | "tar" | "gz" | "tgz" | "rar" | "7z") => ResourceKind::Archive,
        Some("doc" | "docx" | "xls" | "xlsx" | "ppt" | "pptx") => ResourceKind::Document,
        Some(
            "rs" | "py" | "js" | "ts" | "tsx" | "jsx" | "go" | "java" | "c" | "h" | "cpp" | "hpp"
            | "html" | "css" | "vue" | "toml",
        ) => ResourceKind::Code,
        Some(_) => ResourceKind::Binary,
        None => ResourceKind::Unknown,
    }
}

fn list_installed_names(root: PathBuf) -> Vec<String> {
    fs::read_dir(root)
        .ok()
        .into_iter()
        .flat_map(|entries| entries.flatten())
        .filter_map(|entry| {
            entry
                .path()
                .file_name()
                .and_then(|name| name.to_str())
                .map(ToOwned::to_owned)
        })
        .take(64)
        .collect()
}

fn detect_local_commands(commands: &[&str]) -> Vec<String> {
    commands
        .iter()
        .filter(|command| command_available(command))
        .map(|command| (*command).to_string())
        .collect()
}

fn command_available(command: &str) -> bool {
    Command::new("sh")
        .arg("-c")
        .arg(format!("command -v {}", shell_escape(command)))
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn shell_escape(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_mp3_as_audio_resource_without_workspace_write() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_home = temp.path().join("home");
        let input = temp.path().join("voice.mp3");
        fs::write(&input, b"fake mp3").expect("write mp3");

        let store = ResourceStore::default_for_config_home(&config_home);
        let (envelope, hint) = store
            .register_resource_from_path(
                &input,
                "test",
                Some("msg-1".to_string()),
                Some("session-1".to_string()),
                Some("application/octet-stream".to_string()),
            )
            .expect("register resource");

        assert_eq!(envelope.kind, ResourceKind::Audio);
        assert_eq!(envelope.source_message_id.as_deref(), Some("msg-1"));
        assert!(envelope
            .storage_path
            .starts_with(config_home.join("storage/resources/objects")));
        assert!(hint
            .missing_capabilities
            .iter()
            .any(|value| value.contains("transcription")));
        assert!(!temp.path().join("workspace").exists());
    }

    #[test]
    fn renders_markdown_resource_hint_with_boundary() {
        let temp = tempfile::tempdir().expect("tempdir");
        let input = temp.path().join("voice.mp3");
        fs::write(&input, b"fake mp3").expect("write mp3");
        let store = ResourceStore::default_for_config_home(&temp.path().join("home"));
        let pair = store
            .register_resource_from_path(&input, "test", None, None, None)
            .expect("register resource");

        let prompt_hint = pair.1.prompt_hint(&pair.0);

        let rendered = render_resource_context_markdown(&[prompt_hint]);

        assert!(rendered.contains("## Attached Resources"));
        assert!(rendered.contains("resource://res_"));
        assert!(rendered.contains("kind: audio"));
        assert!(rendered.contains("Do not claim audio content"));
        assert!(rendered.contains("Resource handling principle"));
    }

    #[test]
    fn classifies_core_resource_scenarios() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = ResourceStore::default_for_config_home(&temp.path().join("home"));
        let scenarios = [
            ("image.png", b"fake png".as_slice(), ResourceKind::Image),
            ("voice.mp3", b"fake mp3".as_slice(), ResourceKind::Audio),
            ("report.pdf", b"%PDF-1.7".as_slice(), ResourceKind::Pdf),
            ("notes.md", b"# Notes".as_slice(), ResourceKind::Markdown),
            (
                "payload.bin",
                b"\x00\x01\x02\x03".as_slice(),
                ResourceKind::Binary,
            ),
        ];

        let mut pairs = Vec::new();
        for (name, bytes, expected_kind) in scenarios {
            let path = temp.path().join(name);
            fs::write(&path, bytes).expect("write scenario file");
            let (envelope, hint) = store
                .register_resource_from_path(&path, "test", None, None, None)
                .expect("register resource");
            assert_eq!(envelope.kind, expected_kind, "{name} kind");
            pairs.push(hint.prompt_hint(&envelope));
        }

        let rendered = render_resource_context_markdown(&pairs);
        assert!(rendered.contains("kind: image"));
        assert!(rendered.contains("kind: audio"));
        assert!(rendered.contains("kind: pdf"));
        assert!(rendered.contains("kind: markdown"));
        assert!(rendered.contains("kind: binary"));
        assert!(rendered.contains("Resource handling principle"));
    }

    #[test]
    fn prompt_hint_excludes_capability_inventory_and_is_bounded() {
        let envelope = ResourceEnvelope {
            id: "res_test".to_string(),
            uri: "resource://res_test".to_string(),
            source: "test".to_string(),
            source_message_id: None,
            session_id: None,
            original_name: "report.pdf".to_string(),
            declared_mime: None,
            detected_mime: Some("application/pdf".to_string()),
            kind: ResourceKind::Pdf,
            size_bytes: 42,
            sha256: "sha256:test".to_string(),
            storage_path: PathBuf::from("/tmp/report.pdf"),
            created_at: Utc::now(),
            metadata: serde_json::json!({}),
        };
        let hint = ResourceHint {
            resource_id: envelope.id.clone(),
            kind: ResourceKind::Pdf,
            confidence: "high".to_string(),
            native_model_support: "not assumed".to_string(),
            recommended_directions: (0..8).map(|index| format!("direction-{index}")).collect(),
            available_tools: vec!["tool-secret-inventory".to_string()],
            available_skills: vec!["skill-secret-inventory".to_string()],
            available_plugins: vec!["plugin-secret-inventory".to_string()],
            available_mcp_resources: vec!["mcp-secret-inventory".to_string()],
            available_local_commands: vec!["command-secret-inventory".to_string()],
            missing_capabilities: Vec::new(),
            permission_required: Vec::new(),
            safe_next_steps: Vec::new(),
            guardrails: (0..5).map(|index| format!("guardrail-{index}")).collect(),
        };

        let rendered = render_resource_context_markdown(&[hint.prompt_hint(&envelope)]);
        assert!(rendered.contains("direction-2"));
        assert!(!rendered.contains("direction-3"));
        assert!(rendered.contains("guardrail-1"));
        assert!(!rendered.contains("guardrail-2"));
        assert!(!rendered.contains("secret-inventory"));
    }

    #[test]
    fn rejects_resource_above_runtime_limit_before_reading() {
        let temp = tempfile::tempdir().expect("tempdir");
        let store = ResourceStore::default_for_config_home(&temp.path().join("home"));
        let path = temp.path().join("huge.bin");
        let file = fs::File::create(&path).expect("create sparse file");
        file.set_len(MAX_RESOURCE_BYTES + 1)
            .expect("mark sparse file length");

        let error = store
            .register_resource_from_path(&path, "test", None, None, None)
            .expect_err("oversized resource should be rejected");

        assert!(error.contains("resource is too large"));
    }

    #[test]
    fn capability_index_refreshes_without_attachment_path_discovery() {
        let index = ResourceCapabilityIndex::default();
        assert!(index.snapshot().skills.is_empty());
        index.refresh(ResourceCapabilitySnapshot {
            tools: vec!["read_file".to_string()],
            skills: vec!["transcribe".to_string()],
            plugins: vec!["document-reader".to_string()],
            mcp_resources: Vec::new(),
            local_commands: vec!["pdftotext".to_string()],
            provider_native: Vec::new(),
        });
        let snapshot = index.snapshot();
        assert_eq!(snapshot.skills, vec!["transcribe"]);
        assert_eq!(snapshot.local_commands, vec!["pdftotext"]);
    }
}
