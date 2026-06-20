//! Feishu API request/response types.
//!
//! Comprehensive struct definitions for all Feishu (Lark) Open API endpoints
//! used by the Hermes platform adapter. All structs follow Feishu's `snake_case`
//! JSON naming convention and include both serialization and deserialization support.
//!
//! Response structs use `#[serde(default)]` to gracefully handle missing fields.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Request body for tenant access token authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct TenantTokenRequest {
    pub app_id: String,
    pub app_secret: String,
}

/// Response from tenant access token endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct TenantTokenResponse {
    pub code: i32,
    pub msg: String,
    pub tenant_access_token: Option<String>,
    pub expire: Option<i64>,
}

impl Default for TenantTokenResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
            tenant_access_token: None,
            expire: None,
        }
    }
}

// ---------------------------------------------------------------------------
// IM Messages
// ---------------------------------------------------------------------------

/// Request body for sending a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SendMessageRequest {
    pub receive_id: String,
    pub msg_type: String,
    pub content: String,
}

/// Response from sending a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct SendMessageResponse {
    pub code: i32,
    pub msg: String,
    pub data: Option<SendMessageData>,
}

impl Default for SendMessageResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
            data: None,
        }
    }
}

/// Data payload in message send/update responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct SendMessageData {
    pub message_id: Option<String>,
    pub chat_id: Option<String>,
}

/// Request body for replying to a message (path includes the message_id).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReplyMessageRequest {
    pub msg_type: String,
    pub content: String,
}

/// Response from the reply endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct ReplyMessageResponse {
    pub code: i32,
    pub msg: String,
    pub data: Option<SendMessageData>,
}

impl Default for ReplyMessageResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
            data: None,
        }
    }
}

/// Request body for updating an existing message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct UpdateMessageRequest {
    pub content: String,
    pub msg_type: String,
}

/// Response from the update message endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct UpdateMessageResponse {
    pub code: i32,
    pub msg: String,
}

impl Default for UpdateMessageResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
        }
    }
}

/// Empty request for getting a message (path-based, no body).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GetMessageRequest {}

/// Response from the get message endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct GetMessageResponse {
    pub code: i32,
    pub msg: String,
    pub data: Option<GetMessageData>,
}

impl Default for GetMessageResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
            data: None,
        }
    }
}

/// Data payload for retrieved messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GetMessageData {
    pub items: Option<Vec<FeishuMessage>>,
}

/// Full representation of a Feishu message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FeishuMessage {
    pub message_id: String,
    pub chat_id: String,
    pub chat_type: String,
    pub msg_type: String,
    pub content: String,
    pub sender: Option<MessageSender>,
    pub create_time: Option<String>,
    pub parent_id: Option<String>,
    pub root_id: Option<String>,
}

/// Sender information within a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct MessageSender {
    pub id: Option<String>,
    pub id_type: Option<String>,
    pub sender_type: Option<String>,
}

// ---------------------------------------------------------------------------
// IM Images
// ---------------------------------------------------------------------------

/// Empty request for creating an image (multipart upload, no JSON body).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateImageRequest {}

/// Response from the create image endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct CreateImageResponse {
    pub code: i32,
    pub msg: String,
    pub data: Option<CreateImageData>,
}

impl Default for CreateImageResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
            data: None,
        }
    }
}

/// Data payload for the created image.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateImageData {
    pub image_key: Option<String>,
}

// ---------------------------------------------------------------------------
// IM Files
// ---------------------------------------------------------------------------

/// Empty request for creating a file (multipart upload, no JSON body).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateFileRequest {}

/// Response from the create file endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct CreateFileResponse {
    pub code: i32,
    pub msg: String,
    pub data: Option<CreateFileData>,
}

impl Default for CreateFileResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
            data: None,
        }
    }
}

/// Data payload for the created file.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateFileData {
    pub file_key: Option<String>,
}

// ---------------------------------------------------------------------------
// IM Reactions
// ---------------------------------------------------------------------------

/// Reaction type specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ReactionType {
    pub emoji_type: String,
}

/// Request body for creating a reaction on a message.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateReactionRequest {
    pub reaction_type: ReactionType,
}

/// Response from the create reaction endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct CreateReactionResponse {
    pub code: i32,
    pub msg: String,
    pub data: Option<CreateReactionData>,
}

impl Default for CreateReactionResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
            data: None,
        }
    }
}

/// Data payload for the created reaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CreateReactionData {
    pub reaction_id: Option<String>,
}

/// Empty request for deleting a reaction (path-based, reaction_id in URL).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DeleteReactionRequest {}

/// Response from the delete reaction endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct DeleteReactionResponse {
    pub code: i32,
    pub msg: String,
}

impl Default for DeleteReactionResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// IM Chats
// ---------------------------------------------------------------------------

/// Empty request for getting chat info (path-based, chat_id in URL).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GetChatRequest {}

/// Response from the get chat info endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct GetChatResponse {
    pub code: i32,
    pub msg: String,
    pub data: Option<GetChatData>,
}

impl Default for GetChatResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
            data: None,
        }
    }
}

/// Chat information data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct GetChatData {
    pub chat_type: Option<String>,
    pub name: Option<String>,
    pub chat_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// Empty request for listing events (GET, no body).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EventListRequest {}

/// Response from the event list endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", default)]
pub struct EventListResponse {
    pub code: i32,
    pub msg: String,
    pub data: Option<EventListData>,
}

impl Default for EventListResponse {
    fn default() -> Self {
        Self {
            code: 0,
            msg: String::new(),
            data: None,
        }
    }
}

/// Event list data payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EventListData {
    pub items: Option<Vec<serde_json::Value>>,
}

// ---------------------------------------------------------------------------
// Webhook / Event Wrappers
// ---------------------------------------------------------------------------

/// Top-level webhook event envelope received from Feishu.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WebhookEvent {
    pub schema: Option<String>,
    pub header: WebhookHeader,
    #[serde(rename = "event")]
    pub event: Option<serde_json::Value>,
    #[serde(rename = "message")]
    pub message: Option<serde_json::Value>,
}

/// Webhook event header with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct WebhookHeader {
    pub event_id: String,
    pub event_type: String,
    pub create_time: Option<String>,
    pub token: Option<String>,
    pub app_id: Option<String>,
    pub tenant_key: Option<String>,
}

/// Challenge response for webhook URL verification.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct ChallengeResponse {
    pub challenge: Option<String>,
}

/// Encrypted event payload envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct EncryptedPayload {
    pub encrypt: Option<String>,
}

// ---------------------------------------------------------------------------
// Card Messages (Interactive)
// ---------------------------------------------------------------------------

/// An interactive card message sent to Feishu.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct InteractiveCard {
    pub config: Option<CardConfig>,
    pub header: Option<CardHeader>,
    pub elements: Vec<serde_json::Value>,
}

/// Card-level configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CardConfig {
    pub wide_screen_mode: Option<bool>,
}

/// Card header section.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CardHeader {
    pub title: CardTextContent,
    pub template: Option<String>,
}

/// Text content with tag descriptor (used in card titles, buttons, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CardTextContent {
    pub tag: String,
    pub content: String,
}

/// Base card element identified by tag type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CardElement {
    pub tag: String,
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- Auth -----------------------------------------------------------------

    #[test]
    fn roundtrip_tenant_token_request() {
        let req = TenantTokenRequest {
            app_id: "cli_abc123".into(),
            app_secret: "secret_xyz".into(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let parsed: TenantTokenRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.app_id, "cli_abc123");
        assert_eq!(parsed.app_secret, "secret_xyz");
    }

    #[test]
    fn roundtrip_tenant_token_response_success() {
        let raw = r#"{
            "code": 0,
            "msg": "ok",
            "tenant_access_token": "t-g1041baH7QBDEJNG3UMWN3SKGQ23GNLKNZ7IVARG",
            "expire": 7200
        }"#;
        let parsed: TenantTokenResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        assert_eq!(parsed.msg, "ok");
        assert_eq!(
            parsed.tenant_access_token.as_deref(),
            Some("t-g1041baH7QBDEJNG3UMWN3SKGQ23GNLKNZ7IVARG")
        );
        assert_eq!(parsed.expire, Some(7200));
    }

    #[test]
    fn roundtrip_tenant_token_response_error() {
        let raw = r#"{"code": 99991663, "msg": "invalid app_id"}"#;
        let parsed: TenantTokenResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 99991663);
        assert_eq!(parsed.msg, "invalid app_id");
        assert!(parsed.tenant_access_token.is_none());
        assert!(parsed.expire.is_none());
    }

    // -- IM Messages ----------------------------------------------------------

    #[test]
    fn roundtrip_send_message_request() {
        let req = SendMessageRequest {
            receive_id: "ou_abc123".into(),
            msg_type: "text".into(),
            content: r#"{"text":"hello world"}"#.into(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert_eq!(value["receive_id"], "ou_abc123");
        assert_eq!(value["msg_type"], "text");
        assert!(value["content"].as_str().is_some());

        let parsed: SendMessageRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.receive_id, "ou_abc123");
        assert_eq!(parsed.msg_type, "text");
        assert_eq!(parsed.content, r#"{"text":"hello world"}"#);
    }

    #[test]
    fn roundtrip_send_message_response_success() {
        let raw = r#"{
            "code": 0,
            "msg": "success",
            "data": {
                "message_id": "om_abc123def",
                "chat_id": "oc_xyz789"
            }
        }"#;
        let parsed: SendMessageResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        assert_eq!(parsed.msg, "success");
        let data = parsed.data.expect("data present");
        assert_eq!(data.message_id.as_deref(), Some("om_abc123def"));
        assert_eq!(data.chat_id.as_deref(), Some("oc_xyz789"));
    }

    #[test]
    fn roundtrip_send_message_response_no_data() {
        let raw = r#"{"code": 0, "msg": "success"}"#;
        let parsed: SendMessageResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        assert_eq!(parsed.msg, "success");
        assert!(parsed.data.is_none());
    }

    #[test]
    fn roundtrip_reply_message_request() {
        let req = ReplyMessageRequest {
            msg_type: "text".into(),
            content: r#"{"text":"reply content"}"#.into(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let parsed: ReplyMessageRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.msg_type, "text");
        assert_eq!(parsed.content, r#"{"text":"reply content"}"#);
    }

    #[test]
    fn roundtrip_update_message_request() {
        let req = UpdateMessageRequest {
            content: r#"{"text":"updated text"}"#.into(),
            msg_type: "text".into(),
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let parsed: UpdateMessageRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.content, r#"{"text":"updated text"}"#);
        assert_eq!(parsed.msg_type, "text");
    }

    #[test]
    fn roundtrip_get_message_response() {
        let raw = r#"{
            "code": 0,
            "msg": "success",
            "data": {
                "items": [
                    {
                        "message_id": "om_msg001",
                        "chat_id": "oc_chat001",
                        "chat_type": "p2p",
                        "msg_type": "text",
                        "content": "{\"text\":\"hello\"}",
                        "sender": {
                            "id": "ou_user001",
                            "id_type": "open_id",
                            "sender_type": "user"
                        },
                        "create_time": "1700000000000",
                        "parent_id": "om_parent001",
                        "root_id": "om_root001"
                    }
                ]
            }
        }"#;
        let parsed: GetMessageResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        let data = parsed.data.expect("data present");
        let items = data.items.expect("items present");
        assert_eq!(items.len(), 1);
        let msg = &items[0];
        assert_eq!(msg.message_id, "om_msg001");
        assert_eq!(msg.chat_id, "oc_chat001");
        assert_eq!(msg.chat_type, "p2p");
        assert_eq!(msg.msg_type, "text");
        let sender = msg.sender.as_ref().expect("sender present");
        assert_eq!(sender.id.as_deref(), Some("ou_user001"));
        assert_eq!(sender.id_type.as_deref(), Some("open_id"));
        assert_eq!(msg.parent_id.as_deref(), Some("om_parent001"));
        assert_eq!(msg.root_id.as_deref(), Some("om_root001"));
    }

    // -- IM Images ------------------------------------------------------------

    #[test]
    fn roundtrip_create_image_response() {
        let raw = r#"{
            "code": 0,
            "msg": "success",
            "data": {
                "image_key": "img_abc123"
            }
        }"#;
        let parsed: CreateImageResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        let data = parsed.data.expect("data present");
        assert_eq!(data.image_key.as_deref(), Some("img_abc123"));
    }

    // -- IM Files -------------------------------------------------------------

    #[test]
    fn roundtrip_create_file_response() {
        let raw = r#"{
            "code": 0,
            "msg": "success",
            "data": {
                "file_key": "file_xyz789"
            }
        }"#;
        let parsed: CreateFileResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        let data = parsed.data.expect("data present");
        assert_eq!(data.file_key.as_deref(), Some("file_xyz789"));
    }

    // -- IM Reactions ---------------------------------------------------------

    #[test]
    fn roundtrip_create_reaction_request() {
        let req = CreateReactionRequest {
            reaction_type: ReactionType {
                emoji_type: "THUMBSUP".into(),
            },
        };
        let json = serde_json::to_string(&req).expect("serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert_eq!(value["reaction_type"]["emoji_type"], "THUMBSUP");

        let parsed: CreateReactionRequest = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.reaction_type.emoji_type, "THUMBSUP");
    }

    #[test]
    fn roundtrip_create_reaction_response() {
        let raw = r#"{
            "code": 0,
            "msg": "success",
            "data": {
                "reaction_id": "reaction_abc123"
            }
        }"#;
        let parsed: CreateReactionResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        let data = parsed.data.expect("data present");
        assert_eq!(data.reaction_id.as_deref(), Some("reaction_abc123"));
    }

    #[test]
    fn roundtrip_delete_reaction_response() {
        let raw = r#"{"code": 0, "msg": "success"}"#;
        let parsed: DeleteReactionResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        assert_eq!(parsed.msg, "success");
    }

    // -- IM Chats -------------------------------------------------------------

    #[test]
    fn roundtrip_get_chat_response() {
        let raw = r#"{
            "code": 0,
            "msg": "success",
            "data": {
                "chat_type": "group",
                "name": "Project Alpha",
                "chat_id": "oc_chat123"
            }
        }"#;
        let parsed: GetChatResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        let data = parsed.data.expect("data present");
        assert_eq!(data.chat_type.as_deref(), Some("group"));
        assert_eq!(data.name.as_deref(), Some("Project Alpha"));
        assert_eq!(data.chat_id.as_deref(), Some("oc_chat123"));
    }

    // -- Webhook / Events -----------------------------------------------------

    #[test]
    fn roundtrip_webhook_event_message() {
        let raw = r#"{
            "schema": "2.0",
            "header": {
                "event_id": "evt_abc123",
                "event_type": "im.message.receive_v1",
                "create_time": "1700000000000",
                "token": "verification_token_123",
                "app_id": "cli_app001",
                "tenant_key": "tenant_key_001"
            },
            "event": null,
            "message": {
                "message_id": "om_msg001",
                "chat_id": "oc_chat001",
                "content": "{\"text\":\"hello\"}"
            }
        }"#;
        let parsed: WebhookEvent = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.schema.as_deref(), Some("2.0"));
        assert_eq!(parsed.header.event_id, "evt_abc123");
        assert_eq!(parsed.header.event_type, "im.message.receive_v1");
        assert_eq!(parsed.header.create_time.as_deref(), Some("1700000000000"));
        assert_eq!(parsed.header.app_id.as_deref(), Some("cli_app001"));
        assert_eq!(parsed.header.tenant_key.as_deref(), Some("tenant_key_001"));
        assert!(parsed.event.is_none());
        assert!(parsed.message.is_some());
    }

    #[test]
    fn roundtrip_challenge_response() {
        let raw = r#"{"challenge": "challenge_token_12345"}"#;
        let parsed: ChallengeResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.challenge.as_deref(), Some("challenge_token_12345"));

        // Roundtrip
        let resp = ChallengeResponse {
            challenge: Some("response_token".into()),
        };
        let json = serde_json::to_string(&resp).expect("serialize");
        let back: ChallengeResponse = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.challenge.as_deref(), Some("response_token"));
    }

    #[test]
    fn roundtrip_encrypted_payload() {
        let raw = r#"{"encrypt": "base64encrypteddatahere"}"#;
        let parsed: EncryptedPayload = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.encrypt.as_deref(), Some("base64encrypteddatahere"));

        let payload = EncryptedPayload {
            encrypt: Some("encrypted_string".into()),
        };
        let json = serde_json::to_string(&payload).expect("serialize");
        let back: EncryptedPayload = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back.encrypt.as_deref(), Some("encrypted_string"));
    }

    // -- Card Messages --------------------------------------------------------

    #[test]
    fn roundtrip_interactive_card() {
        let card = InteractiveCard {
            config: Some(CardConfig {
                wide_screen_mode: Some(true),
            }),
            header: Some(CardHeader {
                title: CardTextContent {
                    tag: "plain_text".into(),
                    content: "Hello Feishu".into(),
                },
                template: Some("blue".into()),
            }),
            elements: vec![
                serde_json::json!({"tag": "markdown", "content": "## Welcome"}),
                serde_json::json!({"tag": "action", "actions": []}),
            ],
        };

        let json = serde_json::to_string(&card).expect("serialize");
        let value: serde_json::Value = serde_json::from_str(&json).expect("parse");

        // Verify structure
        assert_eq!(value["config"]["wide_screen_mode"], true);
        assert_eq!(value["header"]["title"]["tag"], "plain_text");
        assert_eq!(value["header"]["title"]["content"], "Hello Feishu");
        assert_eq!(value["header"]["template"], "blue");
        assert_eq!(value["elements"].as_array().map(|a| a.len()), Some(2));

        // Verify roundtrip
        let back: InteractiveCard = serde_json::from_str(&json).expect("deserialize");
        let cfg = back.config.expect("config present");
        assert_eq!(cfg.wide_screen_mode, Some(true));
        let hdr = back.header.expect("header present");
        assert_eq!(hdr.title.tag, "plain_text");
        assert_eq!(hdr.title.content, "Hello Feishu");
        assert_eq!(hdr.template.as_deref(), Some("blue"));
        assert_eq!(back.elements.len(), 2);
    }

    #[test]
    fn roundtrip_card_text_content() {
        let content = CardTextContent {
            tag: "lark_md".into(),
            content: "**bold** and _italic_".into(),
        };
        let json = serde_json::to_string(&content).expect("serialize");
        let parsed: CardTextContent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.tag, "lark_md");
        assert_eq!(parsed.content, "**bold** and _italic_");
    }

    #[test]
    fn roundtrip_card_element() {
        let el = CardElement {
            tag: "markdown".into(),
        };
        let json = serde_json::to_string(&el).expect("serialize");
        let parsed: CardElement = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.tag, "markdown");

        // Verify minimal JSON shape
        let value: serde_json::Value = serde_json::from_str(&json).expect("parse");
        assert_eq!(value["tag"], "markdown");
    }

    // -- Edge cases -----------------------------------------------------------

    #[test]
    fn response_default_on_empty_json() {
        // Simulates Feishu returning an empty object for error responses
        let parsed: SendMessageResponse = serde_json::from_str("{}").expect("deserialize");
        assert_eq!(parsed.code, 0);
        assert_eq!(parsed.msg, "");
        assert!(parsed.data.is_none());
    }

    #[test]
    fn response_default_on_partial_json() {
        // Missing `code` field — should default to 0
        let parsed: SendMessageResponse =
            serde_json::from_str(r#"{"msg": "timeout"}"#).expect("deserialize");
        assert_eq!(parsed.code, 0);
        assert_eq!(parsed.msg, "timeout");
        assert!(parsed.data.is_none());
    }

    #[test]
    fn feishu_message_all_optional_fields_missing() {
        let raw = r#"{
            "message_id": "msg_min",
            "chat_id": "chat_min",
            "chat_type": "p2p",
            "msg_type": "text",
            "content": "hello"
        }"#;
        let parsed: FeishuMessage = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.message_id, "msg_min");
        assert!(parsed.sender.is_none());
        assert!(parsed.create_time.is_none());
        assert!(parsed.parent_id.is_none());
        assert!(parsed.root_id.is_none());
    }

    #[test]
    fn event_list_response_empty_items() {
        let raw = r#"{"code": 0, "msg": "ok", "data": {}}"#;
        let parsed: EventListResponse = serde_json::from_str(raw).expect("deserialize");
        assert_eq!(parsed.code, 0);
        let data = parsed.data.expect("data present");
        assert!(data.items.is_none());
    }
}
