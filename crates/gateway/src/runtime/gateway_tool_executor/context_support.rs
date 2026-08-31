//! Pure helpers for bounded context retrieval and resource capability views.
//!
//! Keeping serialization and matching logic outside the effectful executor
//! makes the Gateway adapter small enough to audit without creating a second
//! state or execution owner.

use super::*;

pub(super) fn orchestration_tool_protocol_failed(
    disposition: runtime::orchestration::RuntimeOrchestrationDisposition,
    status: &str,
) -> bool {
    disposition == runtime::orchestration::RuntimeOrchestrationDisposition::PreAdmission
        && matches!(status, "rejected" | "unavailable" | "blocked" | "failed")
}

pub(super) fn resource_capability_keywords(
    kind: &str,
    mime: Option<&str>,
    intent: &str,
) -> Vec<String> {
    let mut keywords = vec![kind.to_string()];
    keywords.extend(
        match kind {
            "image" => ["vision", "image", "ocr"].as_slice(),
            "audio" => ["audio", "ffmpeg", "ffprobe", "transcribe"].as_slice(),
            "video" => ["video", "ffmpeg", "ffprobe", "transcribe"].as_slice(),
            "pdf" => ["pdf", "pdftotext", "pdfinfo", "document"].as_slice(),
            "document" => ["document", "pandoc", "unzip", "office"].as_slice(),
            "archive" => ["archive", "unzip", "tar"].as_slice(),
            "csv" => ["csv", "python", "dataframe"].as_slice(),
            "text" | "markdown" | "code" => ["text", "code", "grep"].as_slice(),
            _ => [].as_slice(),
        }
        .iter()
        .map(|value| (*value).to_string()),
    );
    if let Some(mime) = mime {
        keywords.extend(
            mime.split(|character: char| !character.is_ascii_alphanumeric())
                .filter(|part| part.len() >= 3)
                .map(str::to_ascii_lowercase),
        );
    }
    keywords.extend(
        intent
            .split(|character: char| !character.is_alphanumeric())
            .filter(|part| part.len() >= 4)
            .take(4)
            .map(str::to_ascii_lowercase),
    );
    keywords.sort();
    keywords.dedup();
    keywords
}

pub(super) fn capability_name_matches(value: &str, keywords: &[String]) -> bool {
    let normalized = value.to_ascii_lowercase();
    keywords.iter().any(|keyword| normalized.contains(keyword))
}

pub(super) fn context_reference_contract() -> serde_json::Value {
    serde_json::json!({
        "evidence_refs": "audit locators retained with the result; they are not MCP resources",
        "drill_down_tool": "context_retrieve",
        "instruction": "Evidence locators are not MCP resources. Use a selected item's read_request or the response next_request; do not pass session:// or memory: references to read_mcp_resource_tool.",
    })
}

pub(super) fn bounded_context_text(content: &str, max_chars: usize) -> (String, bool) {
    let mut chars = content.chars();
    let bounded = chars.by_ref().take(max_chars).collect::<String>();
    let truncated = chars.next().is_some();
    (bounded, truncated)
}

pub(super) fn session_message_preview(content_json: &str, max_chars: usize) -> String {
    let value = serde_json::from_str::<serde_json::Value>(content_json).unwrap_or_default();
    let blocks = value.as_array().map_or_else(Vec::new, Clone::clone);
    let mut parts = Vec::new();
    for block in blocks {
        let kind = block
            .get("type")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default();
        let text = match kind {
            "text" | "reasoning_summary" => block
                .get("text")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
            "tool_use" => block
                .get("name")
                .and_then(serde_json::Value::as_str)
                .map(|name| format!("[tool:{name}]")),
            "tool_result" => block
                .get("output")
                .and_then(serde_json::Value::as_str)
                .map(|output| format!("[tool_result] {output}")),
            "image" => Some("[image]".to_string()),
            _ => None,
        };
        if let Some(text) = text.filter(|text| !text.trim().is_empty()) {
            parts.push(text);
        }
    }
    let joined = parts.join("\n");
    let mut preview = joined.chars().take(max_chars).collect::<String>();
    if joined.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview
}

pub(super) fn exact_session_message_page(
    message: &session::SessionMessage,
    block_cursor: usize,
    block_limit: usize,
    scope: ContextRetrieveScope,
) -> Result<serde_json::Value, ToolError> {
    let blocks = serde_json::from_str::<Vec<serde_json::Value>>(&message.content_json)
        .map_err(|error| ToolError::new(format!("stored Session message is malformed: {error}")))?;
    let start = block_cursor.min(blocks.len());
    let end = start.saturating_add(block_limit).min(blocks.len());
    let selected = blocks[start..end]
        .iter()
        .enumerate()
        .map(|(relative_index, block)| {
            let encoded = serde_json::to_vec(block).unwrap_or_default();
            serde_json::json!({
                "index": start + relative_index,
                "digest": format!("{:x}", Sha256::digest(&encoded)),
                "content": block,
            })
        })
        .collect::<Vec<_>>();
    let next_cursor = (end < blocks.len()).then_some(end);
    let scope_name = match scope {
        ContextRetrieveScope::Current => "current",
        ContextRetrieveScope::ExplicitSession => "explicit_session",
        ContextRetrieveScope::RelatedSessions => "related_sessions",
        ContextRetrieveScope::WorkspaceSessions => "workspace_sessions",
    };
    Ok(serde_json::json!({
        "kind": "runtime.context_retrieval",
        "source": "session_history",
        "scope": scope_name,
        "status": "completed",
        "target_session_id": message.session_id,
        "message_id": message.stable_message_id,
        "sequence": message.sequence,
        "role": message.role,
        "created_at_ms": message.created_at_ms,
        "message_digest": format!("{:x}", Sha256::digest(message.content_json.as_bytes())),
        "block_cursor": start,
        "block_count": blocks.len(),
        "selected_count": selected.len(),
        "selected": selected,
        "next_request": next_cursor.map(|cursor| serde_json::json!({
            "source": "session_history",
            "scope": scope_name,
            "session_id": (scope == ContextRetrieveScope::ExplicitSession)
                .then_some(message.session_id.clone()),
            "message_id": message.stable_message_id,
            "block_cursor": cursor,
            "block_limit": block_limit,
        })),
        "truncated": next_cursor.is_some(),
        "authorization_basis": if scope == ContextRetrieveScope::Current {
            "current_session"
        } else {
            "explicit_authorized_session"
        },
        "reference_contract": context_reference_contract(),
    }))
}

pub(super) fn evidence_scope_allowed(authorized_scopes: &[String], visibility_scope: &str) -> bool {
    authorized_scopes
        .iter()
        .any(|scope| scope == visibility_scope)
}

pub(super) fn session_record_title(record: &session::SessionRecord) -> String {
    record
        .metadata_json
        .as_deref()
        .and_then(|metadata| serde_json::from_str::<serde_json::Value>(metadata).ok())
        .and_then(|metadata| {
            metadata
                .get("title")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|title| !title.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| {
            (!record.chat_id.trim().is_empty())
                .then(|| record.chat_id.clone())
                .unwrap_or_else(|| record.session_id.clone())
        })
}
