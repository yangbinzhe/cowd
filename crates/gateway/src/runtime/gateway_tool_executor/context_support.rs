//! Pure helpers for bounded context retrieval and resource capability views.
//!
//! Keeping serialization and matching logic outside the effectful executor
//! makes the Gateway adapter small enough to audit without creating a second
//! state or execution owner.

use super::*;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct EvidencePageCursor {
    version: u8,
    sha256: String,
    query: Option<String>,
    next_chunk: usize,
}

/// Stateless paging over immutable, already-authorized content. A cursor is
/// never authorization: the executor rechecks the receipt and scope each time.
pub(super) fn evidence_content_page(
    content: &str,
    sha256: &str,
    request: &EvidenceRetrieveToolRequest,
) -> Result<serde_json::Value, ToolError> {
    let mut offsets = content
        .char_indices()
        .step_by(1_500)
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    offsets.push(content.len());
    let total = offsets.len() - 1;
    let start = match request.cursor.as_deref() {
        None => 0,
        Some(value) => {
            let cursor: EvidencePageCursor = serde_json::from_str(value).map_err(|_| {
                ToolError::new("invalid evidence cursor; use the returned next_request")
            })?;
            if cursor.version != 1 || cursor.sha256 != sha256 || cursor.query != request.query {
                return Err(ToolError::new(
                    "evidence cursor source/query changed; restart reading without cursor",
                ));
            }
            if cursor.next_chunk > total {
                return Err(ToolError::new("evidence cursor is outside the content"));
            }
            cursor.next_chunk
        }
    };
    let limit = request.limit.unwrap_or(8).clamp(1, 16);
    let terms = request
        .query
        .as_deref()
        .unwrap_or_default()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(str::to_lowercase)
        .collect::<Vec<_>>();
    let mut matches = offsets
        .windows(2)
        .enumerate()
        .skip(start)
        .filter(|(_, range)| {
            if terms.is_empty() {
                return true;
            }
            let chunk = content[range[0]..range[1]].to_lowercase();
            terms.iter().any(|term| chunk.contains(term))
        });
    let selected = matches.by_ref().take(limit).map(|(index, range)| {
        serde_json::json!({"index": index, "content": &content[range[0]..range[1]]})
    }).collect::<Vec<_>>();
    let next_cursor = if let Some((index, _)) = matches.next() {
        Some(
            serde_json::to_string(&EvidencePageCursor {
                version: 1,
                sha256: sha256.to_string(),
                query: request.query.clone(),
                next_chunk: index,
            })
            .map_err(|error| ToolError::new(error.to_string()))?,
        )
    } else {
        None
    };
    let next_request = next_cursor.as_ref().map(|cursor| {
        let mut value = serde_json::json!({"evidence_ref": request.evidence_ref, "cursor": cursor, "limit": limit});
        if let Some(query) = &request.query { value["query"] = query.clone().into(); }
        value
    });
    Ok(serde_json::json!({
        "chunks": selected, "total_chunks": total, "truncated": next_cursor.is_some(),
        "next_cursor": next_cursor, "next_request": next_request,
        "coverage": if terms.is_empty() { "sequential_content" } else { "query_matching_chunks" },
    }))
}

#[cfg(test)]
mod evidence_paging_tests {
    use super::*;

    fn request(query: Option<&str>) -> EvidenceRetrieveToolRequest {
        EvidenceRetrieveToolRequest {
            evidence_ref: "artifact://immutable".into(),
            query: query.map(str::to_owned),
            limit: Some(1),
            cursor: None,
        }
    }

    #[test]
    fn hundred_unicode_pages_reconstruct_original_without_loss() {
        let content = format!("{}终点", "甲🙂乙".repeat(50_000));
        let mut input = request(None);
        let mut restored = String::new();
        let mut pages = 0;
        loop {
            let page = evidence_content_page(&content, "hash", &input).unwrap();
            for chunk in page["chunks"].as_array().unwrap() {
                restored.push_str(chunk["content"].as_str().unwrap());
            }
            pages += 1;
            if page["next_request"].is_null() {
                break;
            }
            input = serde_json::from_value(page["next_request"].clone()).unwrap();
        }
        assert_eq!(pages, 101);
        assert_eq!(restored, content);
    }

    #[test]
    fn query_pages_preserve_indices_and_do_not_fabricate_matches() {
        let content = format!(
            "{}{}{}",
            "x".repeat(1500),
            "y".repeat(1500),
            "x".repeat(1500)
        );
        let page = evidence_content_page(&content, "hash", &request(Some("x"))).unwrap();
        assert_eq!(page["chunks"][0]["index"], 0);
        let input = serde_json::from_value(page["next_request"].clone()).unwrap();
        let last = evidence_content_page(&content, "hash", &input).unwrap();
        assert_eq!(last["chunks"][0]["index"], 2);
        assert!(last["next_request"].is_null());
        let missing = evidence_content_page(&content, "hash", &request(Some("absent"))).unwrap();
        assert_eq!(missing["chunks"], serde_json::json!([]));
        assert_eq!(missing["truncated"], false);
    }

    #[test]
    fn cursor_rejects_source_query_and_range_changes() {
        let content = "x".repeat(3000);
        let page = evidence_content_page(&content, "hash", &request(None)).unwrap();
        let mut input: EvidenceRetrieveToolRequest =
            serde_json::from_value(page["next_request"].clone()).unwrap();
        assert!(evidence_content_page(&content, "new-hash", &input).is_err());
        input.query = Some("changed".into());
        assert!(evidence_content_page(&content, "hash", &input).is_err());
        input.query = None;
        input.cursor = Some(
            serde_json::to_string(&EvidencePageCursor {
                version: 1,
                sha256: "hash".into(),
                query: None,
                next_chunk: usize::MAX,
            })
            .unwrap(),
        );
        assert!(evidence_content_page(&content, "hash", &input).is_err());
        input.cursor = Some("bad cursor".into());
        assert!(evidence_content_page(&content, "hash", &input).is_err());
        let empty = evidence_content_page("", "empty", &request(None)).unwrap();
        assert_eq!(empty["total_chunks"], 0);
        assert!(empty["next_request"].is_null());
    }
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

/// Emit business data before routing/coverage metadata without changing its
/// values. Fixed tool-use instructions belong in the tool descriptor.
pub(super) fn serialize_context_result(
    value: &serde_json::Value,
) -> Result<String, serde_json::Error> {
    struct DataFirst<'a>(&'a serde_json::Value);
    impl serde::Serialize for DataFirst<'_> {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeMap;
            let Some(object) = self.0.as_object() else {
                return serde::Serialize::serialize(self.0, serializer);
            };
            let first = ["selected", "exact", "chunks", "projection"];
            let mut map = serializer.serialize_map(Some(object.len()))?;
            for key in first {
                if let Some(value) = object.get(key) {
                    map.serialize_entry(key, value)?;
                }
            }
            for (key, value) in object {
                if !first.contains(&key.as_str()) {
                    map.serialize_entry(key, value)?;
                }
            }
            map.end()
        }
    }
    serde_json::to_string_pretty(&DataFirst(value))
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

pub(super) fn compact_context_request(mut request: serde_json::Value) -> serde_json::Value {
    if let Some(object) = request.as_object_mut() {
        object.retain(|_, value| !value.is_null());
    }
    request
}

pub(super) fn session_message_read_request(
    message: &session::SessionMessage,
    current_session: &str,
) -> serde_json::Value {
    compact_context_request(serde_json::json!({"source":"session_history",
        "scope":if message.session_id == current_session {"current"} else {"explicit_session"},
        "session_id":(message.session_id != current_session).then_some(&message.session_id),
        "message_id":message.stable_message_id,"message_digest":format!("{:x}",Sha256::digest(message.content_json.as_bytes()))}))
}

pub(super) fn exact_session_message_page(
    message: &session::SessionMessage,
    block_cursor: usize,
    block_limit: usize,
    scope: ContextRetrieveScope,
) -> Result<serde_json::Value, ToolError> {
    let scope_name = match scope {
        ContextRetrieveScope::Current => "current",
        ContextRetrieveScope::ExplicitSession => "explicit_session",
        ContextRetrieveScope::RelatedSessions => "related_sessions",
        ContextRetrieveScope::WorkspaceSessions => "workspace_sessions",
    };
    let blocks = serde_json::from_str::<Vec<serde_json::Value>>(&message.content_json)
        .map_err(|error| ToolError::new(format!("stored Session message is malformed: {error}")))?;
    if block_cursor > blocks.len() {
        return Err(ToolError::new(
            "Session block cursor is outside the message",
        ));
    }
    let start = block_cursor;
    let end = start.saturating_add(block_limit).min(blocks.len());
    let selected = blocks[start..end]
        .iter()
        .enumerate()
        .map(|(relative_index, block)| {
            let encoded = serde_json::to_vec(block).unwrap_or_default();
            serde_json::json!({
                "index": start + relative_index,
                "source_kind":"session_message_block",
                "ref":format!("session://{}/messages/{}#block={}",message.session_id,message.stable_message_id,start+relative_index),
                "revision":format!("{:x}",Sha256::digest(&encoded)),
                "scope":format!("session:{}",message.session_id),"source_time":message.created_at_ms,"information_status":"persisted_block",
                "preview":block.get("text").and_then(serde_json::Value::as_str).map(|text|bounded_context_text(text,480).0),
                "read_request":compact_context_request(serde_json::json!({"source":"session_history","scope":scope_name,
                    "session_id":(scope==ContextRetrieveScope::ExplicitSession).then_some(&message.session_id),
                    "message_id":message.stable_message_id,"block_cursor":start+relative_index,"block_limit":1,
                    "message_digest":format!("{:x}",Sha256::digest(message.content_json.as_bytes()))})),
                "digest": format!("{:x}", Sha256::digest(&encoded)),
                "content": block,
            })
        })
        .collect::<Vec<_>>();
    let next_cursor = (end < blocks.len()).then_some(end);
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
        "next_cursor":next_cursor,
        "coverage":{"kind":"exact_message_blocks","complete":next_cursor.is_none()},
        "selected_count": selected.len(),
        "selected": selected,
        "next_request": next_cursor.map(|cursor| compact_context_request(serde_json::json!({
            "source": "session_history",
            "scope": scope_name,
            "session_id": (scope == ContextRetrieveScope::ExplicitSession)
                .then_some(message.session_id.clone()),
            "message_id": message.stable_message_id,
            "block_cursor": cursor,
            "message_digest":format!("{:x}",Sha256::digest(message.content_json.as_bytes())),
            "block_limit": block_limit,
        }))),
        "truncated": next_cursor.is_some(),
        "authorization_basis": if scope == ContextRetrieveScope::Current {
            "current_session"
        } else {
            "explicit_authorized_session"
        },
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
