//! Markdown ↔ Feishu post/card conversion module.
//!
//! This module provides functions for converting between markdown text and
//! Feishu message formats (post, card, text), matching Hermes' `feishu.py`
//! markdown helpers.
//!
//! # Message formats
//!
//! - **Post**: Rich text message with structured content (text, images, mentions).
//!   Use [`build_post_payload`] to convert markdown → post JSON, and
//!   [`parse_post_payload`] to parse post JSON → text + media refs.
//! - **Card**: Interactive card with header, markdown body, and action buttons.
//!   Use [`build_card_payload`] to build the card JSON.
//! - **Text**: Simple plain-text message. Use [`build_text_payload`] for the
//!   basic `{"text": "..."}` payload.

use regex::Regex;
use serde_json::{json, Value};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Result of parsing a Feishu post payload.
#[derive(Debug, Clone, PartialEq)]
pub struct PostParseResult {
    /// Extracted plain/markdown text content.
    pub text_content: String,
    /// Extracted image keys.
    pub image_keys: Vec<String>,
    /// Extracted media references (files, audio, video).
    pub media_refs: Vec<MediaRef>,
}

/// A media reference extracted from a Feishu post.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaRef {
    /// File key for the media resource.
    pub file_key: String,
    /// File name of the media resource.
    pub file_name: String,
    /// Resource type: `"image"`, `"file"`, `"audio"`, `"video"`.
    pub resource_type: String,
}

/// Definition for a card action button.
#[derive(Debug, Clone)]
pub struct CardActionDef {
    /// Button label text.
    pub label: String,
    /// Action identifier returned in callback.
    pub action_id: String,
    /// Button style: `"primary"`, `"default"`, `"danger"`.
    pub style: String,
}

// ---------------------------------------------------------------------------
// Public functions
// ---------------------------------------------------------------------------

/// Convert markdown text to a Feishu post JSON payload.
///
/// When code fences (` ``` `) are present, the content is split into
/// alternating md / code_block rows so that fenced code is rendered as a
/// distinct block in the Feishu client.  Plain text without fences produces
/// a single `md` row.
///
/// # Examples
///
/// ```ignore
/// // Note: this module is `pub use`-re-exported from
/// // `channel_adapters::platform::feishu`, so callers outside the crate use
/// // `channel_adapters::platform::feishu::build_post_payload(...)`.
/// let payload = build_post_payload("Hello **world**");
/// assert!(payload.contains(r#""tag":"md""#));
/// assert!(payload.contains("Hello **world**"));
/// ```
pub fn build_post_payload(content: &str) -> String {
    if content.is_empty() {
        return json!({"zh_cn": {"content": [[{"tag": "md", "text": ""}]]}}).to_string();
    }

    // Detect code fences
    if !content.contains("```") {
        // Single row of markdown
        let row = vec![json!({"tag": "md", "text": content})];
        return json!({"zh_cn": {"content": [row]}}).to_string();
    }

    // Split on ``` fences — even parts are prose, odd parts are code
    let parts: Vec<&str> = content.split("```").collect();
    let mut rows: Vec<Vec<Value>> = Vec::new();

    for (i, part) in parts.iter().enumerate() {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }

        if i % 2 == 0 {
            // Prose block
            rows.push(vec![json!({"tag": "md", "text": trimmed})]);
        } else {
            // Code block — strip optional language identifier on the first line
            let code = if let Some(nl) = trimmed.find('\n') {
                trimmed[nl + 1..].trim()
            } else {
                trimmed
            };
            if !code.is_empty() {
                rows.push(vec![json!({"tag": "code_block", "text": code})]);
            }
        }
    }

    json!({"zh_cn": {"content": rows}}).to_string()
}

/// Parse a Feishu post JSON payload into text content + extracted media refs.
///
/// Walks the locale wrapper (prefers `zh_cn`, then `en_us`, then the first
/// locale found), collects text from `text` / `md` / `code_block` / `at`
/// elements, extracts `image_key` values from `img` / `image` elements, and
/// builds [`MediaRef`] entries for `media` / `file` / `audio` / `video`.
///
/// # Text styling
///
/// `text` elements with a `style` array are converted to markdown:
///
/// | Style            | Markdown  |
/// |------------------|-----------|
/// | `bold`           | `**…**`   |
/// | `italic`         | `*…*`     |
/// | `strikethrough`  | `~~…~~`   |
/// | `code`           | `` `…` `` |
///
/// # Mentions
///
/// `at` elements resolve the display name from the element's `user_name`
/// field first, then from the locale-level `mentions_map` keyed by `user_id`.
pub fn parse_post_payload(payload: &str) -> PostParseResult {
    let mut text_parts: Vec<String> = Vec::new();
    let mut image_keys: Vec<String> = Vec::new();
    let mut media_refs: Vec<MediaRef> = Vec::new();

    let root: Value = match serde_json::from_str(payload) {
        Ok(v) => v,
        Err(_) => {
            return PostParseResult {
                text_content: String::new(),
                image_keys,
                media_refs,
            };
        }
    };

    let root_obj = match root.as_object() {
        Some(o) => o,
        None => {
            return PostParseResult {
                text_content: String::new(),
                image_keys,
                media_refs,
            };
        }
    };

    // Try preferred locales, then fall back to any locale
    let locale_data = root_obj
        .get("zh_cn")
        .or_else(|| root_obj.get("en_us"))
        .or_else(|| root_obj.values().next());

    let locale_obj = match locale_data.and_then(|v| v.as_object()) {
        Some(o) => o,
        None => {
            return PostParseResult {
                text_content: String::new(),
                image_keys,
                media_refs,
            };
        }
    };

    // Build mentions map from locale-level mentions_map (keyed by user_id)
    let mentions_map: HashMap<String, String> = locale_obj
        .get("mentions_map")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    // Walk content rows
    if let Some(content) = locale_obj.get("content").and_then(|v| v.as_array()) {
        for row in content {
            if let Some(elements) = row.as_array() {
                for element in elements {
                    let tag = element.get("tag").and_then(|v| v.as_str()).unwrap_or("");
                    match tag {
                        "text" => {
                            let text = element.get("text").and_then(|v| v.as_str()).unwrap_or("");
                            let styles: Vec<&str> = element
                                .get("style")
                                .and_then(|v| v.as_array())
                                .map(|a| a.iter().filter_map(|s| s.as_str()).collect())
                                .unwrap_or_default();
                            text_parts.push(apply_text_styles(text, &styles));
                        }
                        "md" => {
                            if let Some(t) = element.get("text").and_then(|v| v.as_str()) {
                                text_parts.push(t.to_string());
                            }
                        }
                        "code_block" => {
                            if let Some(t) = element.get("text").and_then(|v| v.as_str()) {
                                text_parts.push(format!("\n```\n{}\n```\n", t));
                            }
                        }
                        "img" | "image" => {
                            if let Some(key) = element.get("image_key").and_then(|v| v.as_str()) {
                                image_keys.push(key.to_string());
                            }
                        }
                        "media" | "file" | "audio" | "video" => {
                            let file_key = element
                                .get("file_key")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            let file_name = element
                                .get("file_name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            if !file_key.is_empty() {
                                media_refs.push(MediaRef {
                                    file_key: file_key.to_string(),
                                    file_name: file_name.to_string(),
                                    resource_type: tag.to_string(),
                                });
                            }
                        }
                        "at" => {
                            let user_id = element
                                .get("user_id")
                                .and_then(|v| v.as_str())
                                .unwrap_or("");
                            let name = element
                                .get("user_name")
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                                .map(|s| s.to_string())
                                .or_else(|| mentions_map.get(user_id).cloned())
                                .unwrap_or_else(|| user_id.to_string());
                            text_parts.push(format!("@{}", name));
                        }
                        "a" => {
                            // Link element
                            let text = element.get("text").and_then(|v| v.as_str()).unwrap_or("");
                            let href = element.get("href").and_then(|v| v.as_str()).unwrap_or("");
                            if href.is_empty() {
                                text_parts.push(text.to_string());
                            } else {
                                text_parts.push(format!("[{}]({})", text, href));
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    PostParseResult {
        text_content: text_parts.join(""),
        image_keys,
        media_refs,
    }
}

/// Apply Feishu text styles to produce markdown formatting.
fn apply_text_styles(text: &str, styles: &[&str]) -> String {
    let mut result = text.to_string();
    // Apply in reverse so innermost style is closest to the text
    for style in styles.iter().rev() {
        match *style {
            "bold" => result = format!("**{}**", result),
            "italic" => result = format!("*{}*", result),
            "strikethrough" => result = format!("~~{}~~", result),
            "code" | "inline_code" => result = format!("`{}`", result),
            "underline" => result = format!("<u>{}</u>", result),
            _ => {}
        }
    }
    result
}

/// Build a Feishu interactive card JSON payload.
///
/// Produces a card with:
/// - `wide_screen_mode` enabled
/// - Blue header with the given title
/// - Markdown body (omitted when `content` is empty)
/// - Action row with buttons from `actions` (omitted when empty)
///
/// Button `style` maps to the Feishu `"type"` field (`"primary"`,
/// `"default"`, `"danger"`).
pub fn build_card_payload(title: &str, content: &str, actions: &[CardActionDef]) -> String {
    let mut elements: Vec<Value> = Vec::new();

    if !content.is_empty() {
        elements.push(json!({"tag": "markdown", "content": content}));
    }

    if !actions.is_empty() {
        let action_buttons: Vec<Value> = actions
            .iter()
            .map(|a| {
                json!({
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": a.label},
                    "type": a.style,
                    "value": {"action": a.action_id}
                })
            })
            .collect();

        elements.push(json!({
            "tag": "action",
            "actions": action_buttons
        }));
    }

    json!({
        "config": {"wide_screen_mode": true},
        "header": {
            "title": {"tag": "plain_text", "content": title},
            "template": "blue"
        },
        "elements": elements
    })
    .to_string()
}

/// Strip markdown formatting, producing plain text.
///
/// This is used as a fallback when a post message is rejected — plain text
/// has fewer restrictions than rich post messages.
///
/// Transformations:
///
/// | Input              | Output       |
/// |--------------------|--------------|
/// | `**bold**`         | `bold`       |
/// | `*italic*`         | `italic`     |
/// | `~~strike~~`       | `strike`     |
/// | `` `code` ``       | `code`       |
/// | `[text](url)`      | `text (url)` |
/// | `# heading`        | `heading`    |
/// | `> quote`          | `quote`      |
/// | `- list item`      | `list item`  |
/// | ` ```fenced``` `   | *(removed)*  |
pub fn strip_markdown(text: &str) -> String {
    // Each regex is compiled once via `Regex::new` then applied in order.
    // Order matters: bold → italic ensures triple-asterisk ***bold+italic***
    // is handled correctly.

    // 1. Remove fenced code blocks first (multi-line)
    let re_fence = Regex::new(r"(?s)```[^\n]*\n.*?```").unwrap();
    let mut result = re_fence.replace_all(text, "").to_string();

    // Also handle inline ```code```
    let re_fence_inline = Regex::new(r"```([^`]+)```").unwrap();
    result = re_fence_inline.replace_all(&result, "$1").to_string();

    // 2. Images: ![alt](url) → alt
    let re_img = Regex::new(r"!\[([^\]]*)\]\([^)]*\)").unwrap();
    result = re_img.replace_all(&result, "$1").to_string();

    // 3. Links: [text](url) → text (url)
    let re_link = Regex::new(r"\[([^\]]*)\]\(([^)]*)\)").unwrap();
    result = re_link.replace_all(&result, "$1 ($2)").to_string();

    // 4. Bold: **text** or __text__ (double delimiters processed first)
    let re_bold_star = Regex::new(r"\*\*(.+?)\*\*").unwrap();
    result = re_bold_star.replace_all(&result, "$1").to_string();
    let re_bold_underscore = Regex::new(r"__(.+?)__").unwrap();
    result = re_bold_underscore.replace_all(&result, "$1").to_string();

    // 5. Strikethrough: ~~text~~
    let re_strike = Regex::new(r"~~(.+?)~~").unwrap();
    result = re_strike.replace_all(&result, "$1").to_string();

    // 6. Italic: *text* or _text_ (processed after bold so *** → **bold** + *italic*)
    let re_italic_star = Regex::new(r"\*(.+?)\*").unwrap();
    result = re_italic_star.replace_all(&result, "$1").to_string();
    let re_italic_underscore = Regex::new(r"_(.+?)_").unwrap();
    result = re_italic_underscore.replace_all(&result, "$1").to_string();

    // 7. Inline code: `text`
    let re_code = Regex::new(r"`([^`]+)`").unwrap();
    result = re_code.replace_all(&result, "$1").to_string();

    // 8. Headings: # text (line-anchored)
    let re_h = Regex::new(r"(?m)^#{1,6}\s+").unwrap();
    result = re_h.replace_all(&result, "").to_string();

    // 9. Blockquotes: > text
    let re_bq = Regex::new(r"(?m)^>\s?").unwrap();
    result = re_bq.replace_all(&result, "").to_string();

    // 10. Unordered list markers: - item or * item
    let re_ul = Regex::new(r"(?m)^[\-\*]\s+").unwrap();
    result = re_ul.replace_all(&result, "").to_string();

    // 11. Ordered list markers: 1. item
    let re_ol = Regex::new(r"(?m)^\d+\.\s+").unwrap();
    result = re_ol.replace_all(&result, "").to_string();

    // 12. Horizontal rules: --- or ***
    let re_hr = Regex::new(r"(?m)^[-*]{3,}\s*$").unwrap();
    result = re_hr.replace_all(&result, "").to_string();

    // 13. Collapse multiple blank lines
    let re_blanks = Regex::new(r"\n{3,}").unwrap();
    result = re_blanks.replace_all(&result, "\n\n").to_string();

    result.trim().to_string()
}

/// Build a simple text message payload.
///
/// Returns the JSON string `{"text": "<content>"}` suitable for Feishu's
/// `msg_type: "text"` send-message API.
pub fn build_text_payload(content: &str) -> String {
    json!({"text": content}).to_string()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // build_post_payload
    // ------------------------------------------------------------------

    #[test]
    fn test_build_post_payload_empty() {
        let payload = build_post_payload("");
        assert!(payload.contains(r#""tag":"md""#));
        assert!(payload.contains(r#""text":"""#));
        let v: Value = serde_json::from_str(&payload).unwrap();
        let content = &v["zh_cn"]["content"];
        assert_eq!(content.as_array().unwrap().len(), 1);
    }

    #[test]
    fn test_build_post_payload_plain_text() {
        let payload = build_post_payload("Hello world");
        assert!(payload.contains("Hello world"));
        assert!(payload.contains(r#""tag":"md""#));
        let v: Value = serde_json::from_str(&payload).unwrap();
        let rows = v["zh_cn"]["content"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_build_post_payload_with_markdown_formatting() {
        let payload = build_post_payload("Hello **bold** and *italic*");
        assert!(payload.contains("Hello **bold** and *italic*"));
    }

    #[test]
    fn test_build_post_payload_single_code_block() {
        let input = "Intro text\n\n```rust\nfn main() {\n    println!(\"hi\");\n}\n```\n\nOutro";
        let payload = build_post_payload(input);
        let v: Value = serde_json::from_str(&payload).unwrap();
        let rows = v["zh_cn"]["content"].as_array().unwrap();

        // Should produce 3 rows: md, code_block, md
        assert_eq!(
            rows.len(),
            3,
            "expected 3 rows, got {}: {}",
            rows.len(),
            payload
        );
        assert_eq!(rows[0][0]["tag"], "md");
        assert_eq!(rows[1][0]["tag"], "code_block");
        assert_eq!(rows[2][0]["tag"], "md");
    }

    #[test]
    fn test_build_post_payload_code_block_strips_language() {
        let input = "```python\nprint(1+1)\n```";
        let payload = build_post_payload(input);
        let v: Value = serde_json::from_str(&payload).unwrap();
        let rows = v["zh_cn"]["content"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        let code_text = rows[0][0]["text"].as_str().unwrap();
        assert_eq!(code_text, "print(1+1)");
        // "python" language identifier must NOT appear
        assert!(!code_text.contains("python"));
    }

    #[test]
    fn test_build_post_payload_multiple_code_blocks() {
        let input = "A\n```\ncode1\n```\nB\n```\ncode2\n```\nC";
        let payload = build_post_payload(input);
        let v: Value = serde_json::from_str(&payload).unwrap();
        let rows = v["zh_cn"]["content"].as_array().unwrap();

        // 5 rows: md(A), code_block(code1), md(B), code_block(code2), md(C)
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0][0]["tag"], "md");
        assert_eq!(rows[0][0]["text"], "A");
        assert_eq!(rows[1][0]["tag"], "code_block");
        assert_eq!(rows[1][0]["text"], "code1");
        assert_eq!(rows[2][0]["tag"], "md");
        assert_eq!(rows[2][0]["text"], "B");
        assert_eq!(rows[3][0]["tag"], "code_block");
        assert_eq!(rows[3][0]["text"], "code2");
        assert_eq!(rows[4][0]["tag"], "md");
        assert_eq!(rows[4][0]["text"], "C");
    }

    #[test]
    fn test_build_post_payload_no_fences_single_row() {
        let payload = build_post_payload("Just text, no code");
        let v: Value = serde_json::from_str(&payload).unwrap();
        let rows = v["zh_cn"]["content"].as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0]["tag"], "md");
    }

    // ------------------------------------------------------------------
    // parse_post_payload
    // ------------------------------------------------------------------

    #[test]
    fn test_parse_post_payload_empty_json() {
        let result = parse_post_payload("{}");
        assert!(result.text_content.is_empty());
        assert!(result.image_keys.is_empty());
        assert!(result.media_refs.is_empty());
    }

    #[test]
    fn test_parse_post_payload_invalid_json() {
        let result = parse_post_payload("not json");
        assert!(result.text_content.is_empty());
    }

    #[test]
    fn test_parse_post_payload_plain_text() {
        let payload = r#"{"zh_cn":{"content":[[{"tag":"text","text":"Hello"}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.text_content, "Hello");
    }

    #[test]
    fn test_parse_post_payload_md_tag() {
        let payload = r#"{"zh_cn":{"content":[[{"tag":"md","text":"Hello **world**"}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.text_content, "Hello **world**");
    }

    #[test]
    fn test_parse_post_payload_bold_style() {
        let payload = r#"{"zh_cn":{"content":[[{"tag":"text","text":"bold","style":["bold"]}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.text_content, "**bold**");
    }

    #[test]
    fn test_parse_post_payload_italic_style() {
        let payload =
            r#"{"zh_cn":{"content":[[{"tag":"text","text":"italic","style":["italic"]}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.text_content, "*italic*");
    }

    #[test]
    fn test_parse_post_payload_strikethrough_style() {
        let payload =
            r#"{"zh_cn":{"content":[[{"tag":"text","text":"strike","style":["strikethrough"]}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.text_content, "~~strike~~");
    }

    #[test]
    fn test_parse_post_payload_code_style() {
        let payload = r#"{"zh_cn":{"content":[[{"tag":"text","text":"code","style":["code"]}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.text_content, "`code`");
    }

    #[test]
    fn test_parse_post_payload_combined_styles() {
        // bold + italic → ***text***
        let payload =
            r#"{"zh_cn":{"content":[[{"tag":"text","text":"both","style":["bold","italic"]}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.text_content, "***both***");
    }

    #[test]
    fn test_parse_post_payload_image_key() {
        let payload = r#"{"zh_cn":{"content":[[{"tag":"img","image_key":"img_abc123"}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.image_keys, vec!["img_abc123"]);
    }

    #[test]
    fn test_parse_post_payload_media_file() {
        let payload = r#"{"zh_cn":{"content":[[{"tag":"file","file_key":"file_xyz","file_name":"doc.pdf"}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.media_refs.len(), 1);
        assert_eq!(result.media_refs[0].file_key, "file_xyz");
        assert_eq!(result.media_refs[0].file_name, "doc.pdf");
        assert_eq!(result.media_refs[0].resource_type, "file");
    }

    #[test]
    fn test_parse_post_payload_at_mention_with_name() {
        let payload =
            r#"{"zh_cn":{"content":[[{"tag":"at","user_id":"ou_1","user_name":"Alice"}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.text_content, "@Alice");
    }

    #[test]
    fn test_parse_post_payload_at_mention_from_map() {
        let payload = r#"{"zh_cn":{"content":[[{"tag":"at","user_id":"ou_2"}]],"mentions_map":{"ou_2":"Bob"}}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.text_content, "@Bob");
    }

    #[test]
    fn test_parse_post_payload_en_us_locale() {
        let payload = r#"{"en_us":{"content":[[{"tag":"text","text":"English"}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.text_content, "English");
    }

    #[test]
    fn test_parse_post_payload_link_element() {
        let payload =
            r#"{"zh_cn":{"content":[[{"tag":"a","text":"click","href":"https://example.com"}]]}}"#;
        let result = parse_post_payload(payload);
        assert_eq!(result.text_content, "[click](https://example.com)");
    }

    #[test]
    fn test_parse_post_payload_code_block() {
        let payload = r#"{"zh_cn":{"content":[[{"tag":"code_block","text":"fn main() {}"}]]}}"#;
        let result = parse_post_payload(payload);
        assert!(result.text_content.contains("```"));
        assert!(result.text_content.contains("fn main() {}"));
    }

    // ------------------------------------------------------------------
    // build_card_payload
    // ------------------------------------------------------------------

    #[test]
    fn test_build_card_payload_basic() {
        let payload = build_card_payload("My Title", "Hello world", &[]);
        let v: Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(v["config"]["wide_screen_mode"], true);
        assert_eq!(v["header"]["title"]["content"], "My Title");
        assert_eq!(v["header"]["template"], "blue");
        assert_eq!(v["elements"][0]["tag"], "markdown");
        assert_eq!(v["elements"][0]["content"], "Hello world");
    }

    #[test]
    fn test_build_card_payload_with_actions() {
        let actions = vec![
            CardActionDef {
                label: "OK".into(),
                action_id: "ok".into(),
                style: "primary".into(),
            },
            CardActionDef {
                label: "Cancel".into(),
                action_id: "cancel".into(),
                style: "danger".into(),
            },
        ];
        let payload = build_card_payload("Confirm", "Are you sure?", &actions);
        let v: Value = serde_json::from_str(&payload).unwrap();
        let elements = v["elements"].as_array().unwrap();
        // markdown + action row = 2 elements
        assert_eq!(elements.len(), 2);
        let action_el = &elements[1];
        assert_eq!(action_el["tag"], "action");
        let buttons = action_el["actions"].as_array().unwrap();
        assert_eq!(buttons.len(), 2);
        assert_eq!(buttons[0]["text"]["content"], "OK");
        assert_eq!(buttons[0]["type"], "primary");
        assert_eq!(buttons[1]["text"]["content"], "Cancel");
        assert_eq!(buttons[1]["type"], "danger");
    }

    #[test]
    fn test_build_card_payload_empty_content() {
        let actions = vec![CardActionDef {
            label: "Go".into(),
            action_id: "go".into(),
            style: "default".into(),
        }];
        let payload = build_card_payload("Title", "", &actions);
        let v: Value = serde_json::from_str(&payload).unwrap();
        let elements = v["elements"].as_array().unwrap();
        // only action row, no markdown element
        assert_eq!(elements.len(), 1);
        assert_eq!(elements[0]["tag"], "action");
    }

    // ------------------------------------------------------------------
    // strip_markdown
    // ------------------------------------------------------------------

    #[test]
    fn test_strip_markdown_bold() {
        assert_eq!(strip_markdown("**bold**"), "bold");
    }

    #[test]
    fn test_strip_markdown_italic() {
        assert_eq!(strip_markdown("*italic*"), "italic");
    }

    #[test]
    fn test_strip_markdown_strikethrough() {
        assert_eq!(strip_markdown("~~strike~~"), "strike");
    }

    #[test]
    fn test_strip_markdown_inline_code() {
        assert_eq!(strip_markdown("use `foo` bar"), "use foo bar");
    }

    #[test]
    fn test_strip_markdown_link_keeps_url() {
        assert_eq!(
            strip_markdown("see [docs](https://x.com)"),
            "see docs (https://x.com)"
        );
    }

    #[test]
    fn test_strip_markdown_image() {
        assert_eq!(strip_markdown("![alt](img.png)"), "alt");
    }

    #[test]
    fn test_strip_markdown_heading() {
        assert_eq!(strip_markdown("# Title"), "Title");
        assert_eq!(strip_markdown("### Sub"), "Sub");
    }

    #[test]
    fn test_strip_markdown_blockquote() {
        assert_eq!(strip_markdown("> quoted"), "quoted");
    }

    #[test]
    fn test_strip_markdown_unordered_list() {
        assert_eq!(strip_markdown("- item"), "item");
        assert_eq!(strip_markdown("* item"), "item");
    }

    #[test]
    fn test_strip_markdown_ordered_list() {
        assert_eq!(strip_markdown("1. first"), "first");
    }

    #[test]
    fn test_strip_markdown_fenced_code_removed() {
        let input = "before\n```rust\nlet x = 1;\n```\nafter";
        let result = strip_markdown(input);
        assert!(!result.contains("```"));
        assert!(!result.contains("let x = 1;"));
        assert!(result.contains("before"));
        assert!(result.contains("after"));
    }

    #[test]
    fn test_strip_markdown_horizontal_rule() {
        let input = "above\n---\nbelow";
        let result = strip_markdown(input);
        assert!(!result.contains("---"));
        assert!(result.contains("above"));
        assert!(result.contains("below"));
    }

    #[test]
    fn test_strip_markdown_mixed_formatting() {
        let input = "**Bold** and *italic* and ~~strike~~ and `code`";
        let result = strip_markdown(input);
        assert_eq!(result, "Bold and italic and strike and code");
    }

    // ------------------------------------------------------------------
    // build_text_payload
    // ------------------------------------------------------------------

    #[test]
    fn test_build_text_payload_simple() {
        let payload = build_text_payload("Hello");
        assert_eq!(payload, r#"{"text":"Hello"}"#);
    }

    #[test]
    fn test_build_text_payload_empty() {
        let payload = build_text_payload("");
        assert_eq!(payload, r#"{"text":""}"#);
    }

    // ------------------------------------------------------------------
    // Round-trip: build → parse
    // ------------------------------------------------------------------

    #[test]
    fn test_roundtrip_markdown_simple() {
        let original = "Hello **world** and *more*";
        let payload = build_post_payload(original);
        let result = parse_post_payload(&payload);
        // md tag passes markdown through verbatim
        assert_eq!(result.text_content, original);
    }

    #[test]
    fn test_roundtrip_code_blocks() {
        let original = "Intro\n```\nfn main() {}\n```\nOutro";
        let payload = build_post_payload(original);
        let result = parse_post_payload(&payload);
        assert!(result.text_content.contains("Intro"));
        assert!(result.text_content.contains("fn main() {}"));
        assert!(result.text_content.contains("Outro"));
    }
}
