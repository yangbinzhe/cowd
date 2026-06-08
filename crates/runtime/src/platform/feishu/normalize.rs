//! Feishu message normalization.
//!
//! Parses inbound Feishu webhook event payloads (all 9 message types) into
//! a unified [`NormalizedMessage`] struct suitable for downstream processing.
//!
//! Matches Hermes' `normalize_feishu_message()` behaviour:
//! - Resolves `@_user_N` text placeholders via the `mentions[]` payload field.
//! - Delegates `post` payload parsing to the [`super::markdown`] module.
//! - Strips leading self-mentions (the bot itself) so the LLM does not
//!   see its own name as the first token.
//!
//! # Message-type → MessageType mapping
//!
//! | Feishu type       | `MessageType`  |
//! |-------------------|----------------|
//! | `text`            | `Text`         |
//! | `post`            | `Text`         |
//! | `image`           | `Photo`        |
//! | `file`            | `Document`     |
//! | `audio`           | `Voice`        |
//! | `media`           | `Video`        |
//! | `merge_forward`   | `Text`         |
//! | `share_chat`      | `Text`         |
//! | `interactive`/`card` | `Text`      |

use serde_json::Value;

/// Normalized representation of any Feishu inbound message.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedMessage {
    /// Semantic message category.
    pub message_type: super::super::types::MessageType,
    /// Resolved text (mention placeholders replaced, self-mentions stripped).
    pub text: String,
    /// Image keys extracted from the message (for later download).
    pub image_keys: Vec<String>,
    /// Media references (files, audio, video) extracted from the message.
    pub media_refs: Vec<super::markdown::MediaRef>,
    /// Resolved mention information.
    pub mentions: Vec<MentionRef>,
    /// Raw message metadata preserved for downstream consumers.
    pub metadata: Value,
}

/// A single mention in a Feishu message.
#[derive(Debug, Clone, PartialEq)]
pub struct MentionRef {
    /// The raw at-key placeholder found in the message text (e.g. `@_user_1`).
    pub key: String,
    /// Display name of the mentioned user / bot.
    pub name: String,
    /// Feishu open_id of the mentioned user.
    pub open_id: String,
    /// Whether this is an @all mention.
    pub is_all: bool,
    /// Whether this mention targets the bot itself.
    pub is_self: bool,
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

/// Parse a raw Feishu webhook message into a [`NormalizedMessage`].
///
/// The `raw_message` should be the JSON value from the webhook event's
/// message body.  `bot_open_id` is the *open_id* of the bot application
/// and is used to detect self-mentions.
///
/// # Unknown types
///
/// Returns `text = "[Unknown message type: {type}]"` with no media.
pub fn normalize_feishu_message(raw_message: &Value, bot_open_id: &str) -> NormalizedMessage {
    let msg_type = raw_message
        .get("msg_type")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // Extract mentions array from the top-level message payload.
    let mentions_raw = raw_message
        .get("mentions")
        .and_then(|v| v.as_array())
        .map(|a| a.as_slice())
        .unwrap_or(&[]);
    let mentions = build_mentions_map(mentions_raw, bot_open_id);

    // The `content` field is a JSON-encoded string, not a nested object.
    let content_str = raw_message
        .get("content")
        .and_then(|v| v.as_str())
        .unwrap_or("{}");
    let content_json: Value = serde_json::from_str(content_str).unwrap_or_default();

    match msg_type {
        "text" => normalize_text_message(&content_json, &mentions, raw_message, bot_open_id),
        "post" => normalize_post_message(content_str, &mentions, raw_message, bot_open_id),
        "image" => normalize_image_message(&content_json, &mentions, raw_message),
        "file" => normalize_file_message(&content_json, &mentions, raw_message),
        "audio" => normalize_audio_message(&content_json, &mentions, raw_message),
        "media" => normalize_media_message(&content_json, &mentions, raw_message),
        "merge_forward" => normalize_merge_forward(&content_json, &mentions, raw_message),
        "share_chat" => normalize_share_chat(&content_json, &mentions, raw_message),
        "interactive" | "card" => normalize_interactive_card(&content_json, &mentions, raw_message),
        other => NormalizedMessage {
            message_type: super::super::types::MessageType::Text,
            text: format!("[Unknown message type: {}]", other),
            image_keys: vec![],
            media_refs: vec![],
            mentions,
            metadata: raw_message.clone(),
        },
    }
}

// ---------------------------------------------------------------------------
// Per-type normalizers
// ---------------------------------------------------------------------------

fn normalize_text_message(
    content: &Value,
    mentions: &[MentionRef],
    raw_message: &Value,
    _bot_open_id: &str,
) -> NormalizedMessage {
    let raw_text = content
        .get("text")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let text = normalize_text(&raw_text, mentions);
    let text = strip_edge_self_mentions(&text, mentions);

    NormalizedMessage {
        message_type: super::super::types::MessageType::Text,
        text,
        image_keys: vec![],
        media_refs: vec![],
        mentions: mentions.to_vec(),
        metadata: raw_message.clone(),
    }
}

fn normalize_post_message(
    content_str: &str,
    mentions: &[MentionRef],
    raw_message: &Value,
    _bot_open_id: &str,
) -> NormalizedMessage {
    let parsed = super::markdown::parse_post_payload(content_str);
    let text = normalize_text(&parsed.text_content, mentions);
    let text = strip_edge_self_mentions(&text, mentions);

    NormalizedMessage {
        message_type: super::super::types::MessageType::Text,
        text,
        image_keys: parsed.image_keys,
        media_refs: parsed.media_refs,
        mentions: mentions.to_vec(),
        metadata: raw_message.clone(),
    }
}

fn normalize_image_message(
    content: &Value,
    mentions: &[MentionRef],
    raw_message: &Value,
) -> NormalizedMessage {
    let image_key = content
        .get("image_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let image_keys = if image_key.is_empty() {
        vec![]
    } else {
        vec![image_key]
    };

    NormalizedMessage {
        message_type: super::super::types::MessageType::Photo,
        text: "[Image]".to_string(),
        image_keys,
        media_refs: vec![],
        mentions: mentions.to_vec(),
        metadata: raw_message.clone(),
    }
}

fn normalize_file_message(
    content: &Value,
    mentions: &[MentionRef],
    raw_message: &Value,
) -> NormalizedMessage {
    let file_key = content
        .get("file_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let file_name = content
        .get("file_name")
        .and_then(|v| v.as_str())
        .unwrap_or("file")
        .to_string();

    let media_refs = if file_key.is_empty() {
        vec![]
    } else {
        vec![super::markdown::MediaRef {
            file_key: file_key.clone(),
            file_name: file_name.clone(),
            resource_type: "file".to_string(),
        }]
    };

    NormalizedMessage {
        message_type: super::super::types::MessageType::Document,
        text: format!("[File: {}]", file_name),
        image_keys: vec![],
        media_refs,
        mentions: mentions.to_vec(),
        metadata: raw_message.clone(),
    }
}

fn normalize_audio_message(
    content: &Value,
    mentions: &[MentionRef],
    raw_message: &Value,
) -> NormalizedMessage {
    let file_key = content
        .get("file_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let media_refs = if file_key.is_empty() {
        vec![]
    } else {
        vec![super::markdown::MediaRef {
            file_key: file_key.clone(),
            file_name: "audio".to_string(),
            resource_type: "audio".to_string(),
        }]
    };

    NormalizedMessage {
        message_type: super::super::types::MessageType::Voice,
        text: "[Voice message]".to_string(),
        image_keys: vec![],
        media_refs,
        mentions: mentions.to_vec(),
        metadata: raw_message.clone(),
    }
}

fn normalize_media_message(
    content: &Value,
    mentions: &[MentionRef],
    raw_message: &Value,
) -> NormalizedMessage {
    let file_key = content
        .get("file_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let media_refs = if file_key.is_empty() {
        vec![]
    } else {
        vec![super::markdown::MediaRef {
            file_key: file_key.clone(),
            file_name: "video".to_string(),
            resource_type: "video".to_string(),
        }]
    };

    NormalizedMessage {
        message_type: super::super::types::MessageType::Video,
        text: "[Video]".to_string(),
        image_keys: vec![],
        media_refs,
        mentions: mentions.to_vec(),
        metadata: raw_message.clone(),
    }
}

fn normalize_merge_forward(
    content: &Value,
    mentions: &[MentionRef],
    raw_message: &Value,
) -> NormalizedMessage {
    let title = content.get("title").and_then(|v| v.as_str()).unwrap_or("");

    let mut text_parts: Vec<String> = vec![format!("[Forward: {}]\n", title)];

    if let Some(preview) = content.get("preview").and_then(|v| v.as_array()) {
        for entry in preview {
            if let Some(s) = entry.as_str() {
                text_parts.push(format!("- {}\n", s));
            }
        }
    }

    NormalizedMessage {
        message_type: super::super::types::MessageType::Text,
        text: text_parts.concat(),
        image_keys: vec![],
        media_refs: vec![],
        mentions: mentions.to_vec(),
        metadata: raw_message.clone(),
    }
}

fn normalize_share_chat(
    content: &Value,
    mentions: &[MentionRef],
    raw_message: &Value,
) -> NormalizedMessage {
    let chat_name = content
        .get("chat_name")
        .and_then(|v| v.as_str())
        .unwrap_or("Unknown Chat");
    let chat_id = content
        .get("chat_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    NormalizedMessage {
        message_type: super::super::types::MessageType::Text,
        text: format!("[Shared Chat: {} ({})]", chat_name, chat_id),
        image_keys: vec![],
        media_refs: vec![],
        mentions: mentions.to_vec(),
        metadata: raw_message.clone(),
    }
}

fn normalize_interactive_card(
    content: &Value,
    mentions: &[MentionRef],
    raw_message: &Value,
) -> NormalizedMessage {
    let title = content
        .get("header")
        .and_then(|h| h.get("title"))
        .and_then(|t| t.get("content"))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let mut text_parts: Vec<String> = Vec::new();
    if !title.is_empty() {
        text_parts.push(format!("[Card: {}]\n", title));
    }

    if let Some(elements) = content.get("elements").and_then(|v| v.as_array()) {
        for el in elements {
            let tag = el.get("tag").and_then(|v| v.as_str()).unwrap_or("");
            match tag {
                "markdown" => {
                    if let Some(c) = el.get("content").and_then(|v| v.as_str()) {
                        text_parts.push(format!("{}\n", c));
                    }
                }
                "plain_text" => {
                    if let Some(c) = el.get("content").and_then(|v| v.as_str()) {
                        text_parts.push(format!("{}\n", c));
                    }
                }
                "div" => {
                    // Divs contain nested elements — walk text fields.
                    if let Some(fields) = el.get("fields").and_then(|v| v.as_array()) {
                        for field in fields {
                            if let Some(t) = field.get("content").and_then(|v| v.as_str()) {
                                text_parts.push(format!("{}\n", t));
                            }
                        }
                    }
                    if let Some(t) = el
                        .get("text")
                        .and_then(|t| t.get("content"))
                        .and_then(|v| v.as_str())
                    {
                        text_parts.push(format!("{}\n", t));
                    }
                }
                "action" => {
                    if let Some(actions) = el.get("actions").and_then(|v| v.as_array()) {
                        for action in actions {
                            if let Some(label) = action
                                .get("text")
                                .and_then(|t| t.get("content"))
                                .and_then(|v| v.as_str())
                            {
                                text_parts.push(format!("[Action: {}]\n", label));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    let text = if text_parts.is_empty() {
        "[Interactive Card]".to_string()
    } else {
        text_parts.concat().trim().to_string()
    };

    NormalizedMessage {
        message_type: super::super::types::MessageType::Text,
        text,
        image_keys: vec![],
        media_refs: vec![],
        mentions: mentions.to_vec(),
        metadata: raw_message.clone(),
    }
}

// ---------------------------------------------------------------------------
// Public helpers
// ---------------------------------------------------------------------------

/// Build a `Vec<MentionRef>` from the `mentions[]` array in a Feishu message
/// payload.
///
/// Each entry typically has:
/// - `key`: The at-placeholder in the text (e.g. `@_user_1`).
/// - `name`: Display name.
/// - `id` or `open_id`: Feishu open_id.
/// - `is_all`: `true` for `@all` mentions.
///
/// Self-detection: an entry is marked `is_self` when its `open_id` matches
/// `bot_open_id`.
pub fn build_mentions_map(mentions: &[Value], bot_open_id: &str) -> Vec<MentionRef> {
    mentions
        .iter()
        .filter_map(|m| {
            let key = m
                .get("key")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let name = m
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let open_id = m
                .get("id")
                .or_else(|| m.get("open_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let is_all = m.get("is_all").and_then(|v| v.as_bool()).unwrap_or(false);
            let is_self = !open_id.is_empty() && open_id == bot_open_id;

            Some(MentionRef {
                key,
                name,
                open_id,
                is_all,
                is_self,
            })
        })
        .collect()
}

/// Replace `@_user_N` / mention key placeholders in `text` with `@{name}`.
///
/// Iterates over `mentions` and substitutes each entry's `key` for the
/// human-friendly `@name` form.
pub fn normalize_text(text: &str, mentions: &[MentionRef]) -> String {
    let mut result = text.to_string();
    for mention in mentions {
        if !mention.key.is_empty() {
            result = result.replace(&mention.key, &format!("@{}", mention.name));
        }
    }
    result
}

/// Strip leading self-mentions so the bot's own name/tag does not appear
/// as the first token of the resolved text.
///
/// When the bot was @-mentioned at the very beginning of the message, the
/// LLM does not need to see it.  This function removes leading `@BotName `
/// (with trailing whitespace) for any mention where `is_self == true`.
pub fn strip_edge_self_mentions(text: &str, mentions: &[MentionRef]) -> String {
    let mut result = text.to_string();
    for mention in mentions {
        if !mention.is_self {
            continue;
        }
        // Try with trailing space first (common case)
        let pattern = format!("@{} ", mention.name);
        if result.starts_with(&pattern) {
            result = result[pattern.len()..].to_string();
            continue;
        }
        // Also match when at the very beginning with no space separator
        let alt_pattern = format!("@{}", mention.name);
        if result.starts_with(&alt_pattern) && result.len() > alt_pattern.len() {
            let after = result.as_bytes().get(alt_pattern.len()).copied();
            if after.map_or(false, |c| c == b' ' || c == b'\n') {
                result = result[alt_pattern.len()..].trim_start().to_string();
            }
        }
    }
    result
}

// ===========================================================================
// Tests
// ===========================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Helper: build a minimal message payload with msg_type and content string.
    fn make_msg(msg_type: &str, content: &Value) -> Value {
        json!({
            "message_id": "om_test001",
            "chat_id": "oc_test001",
            "msg_type": msg_type,
            "content": content.to_string(),
            "mentions": []
        })
    }

    fn make_msg_with_mentions(msg_type: &str, content: &Value, mentions: Value) -> Value {
        json!({
            "message_id": "om_test002",
            "chat_id": "oc_test002",
            "msg_type": msg_type,
            "content": content.to_string(),
            "mentions": mentions
        })
    }

    // -----------------------------------------------------------------------
    // 1. text message with @-mentions
    // -----------------------------------------------------------------------
    #[test]
    fn test_text_with_mentions() {
        let raw = json!({
            "message_id": "om_001",
            "chat_id": "oc_001",
            "msg_type": "text",
            "content": r#"{"text":"@_user_1 hello @_user_2"}"#,
            "mentions": [
                {"key": "@_user_1", "name": "Alice", "id": "ou_alice"},
                {"key": "@_user_2", "name": "Bob", "id": "ou_bob"}
            ]
        });

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Text
        );
        assert_eq!(result.text, "@Alice hello @Bob");
        assert_eq!(result.mentions.len(), 2);
        assert_eq!(result.mentions[0].name, "Alice");
        assert_eq!(result.mentions[1].name, "Bob");
    }

    // -----------------------------------------------------------------------
    // 2. text message without mentions
    // -----------------------------------------------------------------------
    #[test]
    fn test_text_without_mentions() {
        let raw = make_msg("text", &json!({"text": "hello world"}));

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(result.text, "hello world");
        assert!(result.mentions.is_empty());
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Text
        );
    }

    // -----------------------------------------------------------------------
    // 3. post message (minimal valid Feishu post JSON)
    // -----------------------------------------------------------------------
    #[test]
    fn test_post_message() {
        let raw = make_msg(
            "post",
            &json!({"zh_cn": {"content": [[{"tag": "text", "text": "Hello"}], [{"tag": "md", "text": "**world**"}]]}}),
        );

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Text
        );
        assert_eq!(result.text, "Hello**world**");
    }

    // -----------------------------------------------------------------------
    // 4. image message
    // -----------------------------------------------------------------------
    #[test]
    fn test_image_message() {
        let raw = make_msg("image", &json!({"image_key": "img_abc123"}));

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Photo
        );
        assert_eq!(result.text, "[Image]");
        assert_eq!(result.image_keys, vec!["img_abc123"]);
    }

    // -----------------------------------------------------------------------
    // 5. file message
    // -----------------------------------------------------------------------
    #[test]
    fn test_file_message() {
        let raw = make_msg(
            "file",
            &json!({"file_key": "file_xyz", "file_name": "report.pdf"}),
        );

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Document
        );
        assert_eq!(result.text, "[File: report.pdf]");
        assert_eq!(result.media_refs.len(), 1);
        assert_eq!(result.media_refs[0].file_key, "file_xyz");
        assert_eq!(result.media_refs[0].file_name, "report.pdf");
        assert_eq!(result.media_refs[0].resource_type, "file");
    }

    // -----------------------------------------------------------------------
    // 6. audio message
    // -----------------------------------------------------------------------
    #[test]
    fn test_audio_message() {
        let raw = make_msg("audio", &json!({"file_key": "audio_key_001"}));

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Voice
        );
        assert_eq!(result.text, "[Voice message]");
        assert_eq!(result.media_refs.len(), 1);
        assert_eq!(result.media_refs[0].resource_type, "audio");
    }

    // -----------------------------------------------------------------------
    // 7. media / video message
    // -----------------------------------------------------------------------
    #[test]
    fn test_media_video_message() {
        let raw = make_msg("media", &json!({"file_key": "video_key_001"}));

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Video
        );
        assert_eq!(result.text, "[Video]");
        assert_eq!(result.media_refs.len(), 1);
        assert_eq!(result.media_refs[0].resource_type, "video");
    }

    // -----------------------------------------------------------------------
    // 8. merge_forward stub
    // -----------------------------------------------------------------------
    #[test]
    fn test_merge_forward() {
        let raw = make_msg(
            "merge_forward",
            &json!({"title": "Group Chat History", "preview": ["Alice: hello", "Bob: hi"]}),
        );

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Text
        );
        assert!(result.text.contains("[Forward: Group Chat History]"));
        assert!(result.text.contains("- Alice: hello"));
        assert!(result.text.contains("- Bob: hi"));
    }

    // -----------------------------------------------------------------------
    // 9. share_chat stub
    // -----------------------------------------------------------------------
    #[test]
    fn test_share_chat() {
        let raw = make_msg(
            "share_chat",
            &json!({"chat_name": "Project Alpha", "chat_id": "oc_alpha"}),
        );

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Text
        );
        assert_eq!(result.text, "[Shared Chat: Project Alpha (oc_alpha)]");
    }

    // -----------------------------------------------------------------------
    // 10. interactive / card stub
    // -----------------------------------------------------------------------
    #[test]
    fn test_interactive_card() {
        let raw = make_msg(
            "interactive",
            &json!({
                "header": {"title": {"tag": "plain_text", "content": "Confirmation"}},
                "elements": [
                    {"tag": "markdown", "content": "Are you sure?"},
                    {"tag": "action", "actions": [
                        {"tag": "button", "text": {"tag": "plain_text", "content": "Yes"}, "value": {}}
                    ]}
                ]
            }),
        );

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Text
        );
        assert!(result.text.contains("[Card: Confirmation]"));
        assert!(result.text.contains("Are you sure?"));
        assert!(result.text.contains("[Action: Yes]"));
    }

    // -----------------------------------------------------------------------
    // 11. self-mention stripping
    // -----------------------------------------------------------------------
    #[test]
    fn test_strip_self_mention() {
        let raw = make_msg_with_mentions(
            "text",
            &json!({"text": "@_user_1 help me"}),
            json!([
                {"key": "@_user_1", "name": "MyBot", "id": "ou_bot", "is_all": false}
            ]),
        );

        let result = normalize_feishu_message(&raw, "ou_bot");
        // "MyBot" is self (open_id == bot_open_id) → leading @MyBot stripped
        assert_eq!(result.text, "help me");
        assert!(result.mentions[0].is_self);
    }

    #[test]
    fn test_self_mention_not_stripped_when_not_first() {
        let raw = make_msg_with_mentions(
            "text",
            &json!({"text": "hi @_user_1 help"}),
            json!([
                {"key": "@_user_1", "name": "MyBot", "id": "ou_bot"}
            ]),
        );

        let result = normalize_feishu_message(&raw, "ou_bot");
        // "@MyBot" is not at the beginning, so it stays
        assert_eq!(result.text, "hi @MyBot help");
    }

    #[test]
    fn test_self_mention_stripping_with_trailing_space() {
        let raw = make_msg_with_mentions(
            "text",
            &json!({"text": "@_user_1 please do something"}),
            json!([
                {"key": "@_user_1", "name": "Bot", "id": "ou_self"}
            ]),
        );

        let result = normalize_feishu_message(&raw, "ou_self");
        assert_eq!(result.text, "please do something");
    }

    // -----------------------------------------------------------------------
    // 12. unknown message type fallback
    // -----------------------------------------------------------------------
    #[test]
    fn test_unknown_message_type() {
        let raw = make_msg("sticker", &json!({"file_key": "sticker_001"}));

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Text
        );
        assert_eq!(result.text, "[Unknown message type: sticker]");
        assert!(result.image_keys.is_empty());
        assert!(result.media_refs.is_empty());
    }

    // -----------------------------------------------------------------------
    // Additional: build_mentions_map edge cases
    // -----------------------------------------------------------------------
    #[test]
    fn test_build_mentions_map_empty() {
        let mentions = build_mentions_map(&[], "ou_bot");
        assert!(mentions.is_empty());
    }

    #[test]
    fn test_build_mentions_map_marks_self() {
        let raw = &[
            json!({"key": "@_user_1", "name": "Bot", "id": "ou_bot"}),
            json!({"key": "@_user_2", "name": "Human", "id": "ou_human"}),
        ];
        let mentions = build_mentions_map(raw, "ou_bot");

        assert_eq!(mentions.len(), 2);
        assert!(mentions[0].is_self);
        assert!(!mentions[1].is_self);
    }

    #[test]
    fn test_build_mentions_map_uses_open_id_field() {
        let raw = &[json!({"key": "@_user_1", "name": "Bot", "open_id": "ou_bot"})];
        let mentions = build_mentions_map(raw, "ou_bot");
        assert!(mentions[0].is_self);
    }

    #[test]
    fn test_normalize_text_no_mentions() {
        let text = "hello world";
        assert_eq!(normalize_text(text, &[]), "hello world");
    }

    #[test]
    fn test_normalize_text_multiple_placeholders() {
        let mentions = vec![
            MentionRef {
                key: "@_user_1".into(),
                name: "Alice".into(),
                open_id: "ou_a".into(),
                is_all: false,
                is_self: false,
            },
            MentionRef {
                key: "@_user_2".into(),
                name: "Bob".into(),
                open_id: "ou_b".into(),
                is_all: false,
                is_self: false,
            },
        ];
        let text = "@_user_1 and @_user_2 are here";
        assert_eq!(normalize_text(text, &mentions), "@Alice and @Bob are here");
    }

    #[test]
    fn test_image_message_no_key() {
        let raw = make_msg("image", &json!({}));
        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Photo
        );
        assert_eq!(result.text, "[Image]");
        assert!(result.image_keys.is_empty());
    }

    #[test]
    fn test_file_message_no_key() {
        let raw = make_msg("file", &json!({}));
        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(
            result.message_type,
            super::super::super::types::MessageType::Document
        );
        assert_eq!(result.text, "[File: file]"); // fallback name
        assert!(result.media_refs.is_empty());
    }

    #[test]
    fn test_interactive_card_minimal() {
        let raw = make_msg("card", &json!({}));
        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(result.text, "[Interactive Card]");
    }

    #[test]
    fn test_merge_forward_empty() {
        let raw = make_msg("merge_forward", &json!({}));
        let result = normalize_feishu_message(&raw, "ou_bot");
        assert!(result.text.contains("[Forward: ]"));
    }

    // -----------------------------------------------------------------------
    // metadata preservation
    // -----------------------------------------------------------------------
    #[test]
    fn test_metadata_preserved() {
        let raw = json!({
            "message_id": "om_meta",
            "chat_id": "oc_meta",
            "msg_type": "text",
            "content": r#"{"text":"hi"}"#,
            "mentions": [],
            "custom_field": true
        });

        let result = normalize_feishu_message(&raw, "ou_bot");
        assert_eq!(result.metadata["message_id"], "om_meta");
        assert_eq!(result.metadata["chat_id"], "oc_meta");
        assert_eq!(result.metadata["custom_field"], true);
    }
}
