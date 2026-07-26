use std::collections::BTreeMap;
#[cfg(test)]
use std::path::Path;
use std::path::PathBuf;

use runtime::{ConversationMessage, JsonValue, Session};

/// Build Runtime's in-memory session from the canonical durable record.
///
/// Gateway's lazy runtime activation uses this conversion so a Surface cannot
/// appear to restore history while the model carrier is initialized with a
/// different (or empty) transcript.
pub(crate) fn hydrated_runtime_session(
    record: session::SessionRecord,
    mut stored_messages: Vec<session::SessionMessage>,
) -> Result<Session, String> {
    stored_messages.sort_by_key(|message| message.sequence);
    for (expected_sequence, stored) in stored_messages.iter().enumerate() {
        if stored.sequence != expected_sequence {
            return Err(format!(
                "session {} transcript is not contiguous: expected sequence {}, found {} ({})",
                record.session_id, expected_sequence, stored.sequence, stored.stable_message_id
            ));
        }
    }
    // Sequence is the immutable physical append cursor. Terminal transcript
    // rows may be persisted after a later queued ingress, so Runtime restores
    // logical turn order from explicit metadata without ever renumbering rows
    // already observed by a Surface.
    let mut turn_ingress_sequence = BTreeMap::<String, usize>::new();
    let mut turn_metadata = BTreeMap::<String, (Option<String>, Option<String>)>::new();
    for stored in &stored_messages {
        let parsed = serde_json::from_str::<Vec<serde_json::Value>>(&stored.content_json).map_err(
            |error| {
                format!(
                    "stored message {} at sequence {} has invalid blocks: {error}",
                    stored.stable_message_id, stored.sequence
                )
            },
        )?;
        let turn_id = parsed.iter().find_map(|block| {
            block
                .get("cowd_turn_id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(ToOwned::to_owned)
        });
        let ingress_message_id = parsed.iter().find_map(|block| {
            block
                .get("cowd_turn_ingress_message_id")
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .map(ToOwned::to_owned)
        });
        if stored.role == "user"
            && ingress_message_id.as_deref() == Some(stored.stable_message_id.as_str())
        {
            if let Some(turn_id) = turn_id.as_ref() {
                turn_ingress_sequence.insert(turn_id.clone(), stored.sequence);
            }
        }
        turn_metadata.insert(
            stored.stable_message_id.clone(),
            (turn_id, ingress_message_id),
        );
    }
    stored_messages.sort_by_key(|message| {
        let (turn_id, ingress_message_id) = turn_metadata
            .get(&message.stable_message_id)
            .cloned()
            .unwrap_or_default();
        let anchor = turn_id
            .as_ref()
            .and_then(|turn_id| turn_ingress_sequence.get(turn_id))
            .copied()
            .unwrap_or(message.sequence);
        let is_ingress = ingress_message_id.as_deref() == Some(message.stable_message_id.as_str());
        (anchor, usize::from(!is_ingress), message.sequence)
    });
    let mut messages = Vec::with_capacity(stored_messages.len());
    for stored in stored_messages {
        if stored.session_id != record.session_id {
            return Err(format!(
                "stored message {} belongs to session {}, expected {}",
                stored.stable_message_id, stored.session_id, record.session_id
            ));
        }
        let blocks = JsonValue::parse(&stored.content_json).map_err(|error| {
            format!(
                "stored message {} at sequence {} has invalid blocks: {error}",
                stored.stable_message_id, stored.sequence
            )
        })?;
        let mut object = BTreeMap::new();
        object.insert("role".to_string(), JsonValue::String(stored.role.clone()));
        object.insert("blocks".to_string(), blocks);
        if let Some(usage_json) = stored.token_usage_json.as_deref() {
            object.insert(
                "usage".to_string(),
                JsonValue::parse(usage_json).map_err(|error| {
                    format!(
                        "stored message {} at sequence {} has invalid usage: {error}",
                        stored.stable_message_id, stored.sequence
                    )
                })?,
            );
        }
        messages.push(
            ConversationMessage::from_json(&JsonValue::Object(object)).map_err(|error| {
                format!(
                    "stored message {} at sequence {} cannot hydrate Runtime: {error}",
                    stored.stable_message_id, stored.sequence
                )
            })?,
        );
    }
    let metadata = record
        .metadata_json
        .as_deref()
        .map(serde_json::from_str::<serde_json::Value>)
        .transpose()
        .map_err(|error| {
            format!(
                "session {} has invalid durable metadata: {error}",
                record.session_id
            )
        })?;
    let workspace_root = metadata
        .as_ref()
        .and_then(|value| value.get("workspace_root"))
        .and_then(serde_json::Value::as_str)
        .map(PathBuf::from);
    let parent_session_id = metadata
        .as_ref()
        .and_then(|value| value.get("parent_session_id"))
        .and_then(serde_json::Value::as_str)
        .map(std::string::ToString::to_string);
    let branch_name = metadata
        .as_ref()
        .and_then(|value| value.get("branch_name"))
        .and_then(serde_json::Value::as_str)
        .map(std::string::ToString::to_string);

    let created_at_ms = chrono::DateTime::parse_from_rfc3339(&record.created_at)
        .map(|timestamp| timestamp.timestamp_millis().max(0) as u64)
        .map_err(|error| {
            format!(
                "session {} has invalid created_at: {error}",
                record.session_id
            )
        })?;
    let updated_at_ms = chrono::DateTime::parse_from_rfc3339(&record.last_activity)
        .map(|timestamp| timestamp.timestamp_millis().max(0) as u64)
        .map_err(|error| {
            format!(
                "session {} has invalid last_activity: {error}",
                record.session_id
            )
        })?;
    let mut session = Session::new();
    session.session_id = record.session_id;
    session.model = record.model;
    session.replace_messages(messages);
    // Replacing the in-memory transcript updates activity time. Durable
    // hydration must restore the persisted timestamps after that mutation so
    // a cold attach cannot masquerade as new user activity.
    session.created_at_ms = created_at_ms;
    session.updated_at_ms = updated_at_ms;
    session.workspace_root = workspace_root;
    session.fork = parent_session_id.map(|parent_session_id| runtime::SessionFork {
        parent_session_id,
        branch_name,
    });
    session.closed = record.status.eq_ignore_ascii_case("closed");

    Ok(session)
}

#[cfg(test)]
mod hydration_tests {
    use super::*;

    fn record() -> session::SessionRecord {
        session::SessionRecord {
            session_id: "hydrate-session".to_string(),
            platform: "test".to_string(),
            chat_id: "hydrate-session".to_string(),
            user_id: None,
            model: Some("effective-model".to_string()),
            created_at: "2026-07-24T01:00:00Z".to_string(),
            last_activity: "2026-07-24T02:00:00Z".to_string(),
            message_count: 2,
            reset_policy: "none".to_string(),
            metadata_json: Some(
                serde_json::json!({
                    "workspace_root": "/workspace/project",
                    "parent_session_id": "parent-session",
                    "branch_name": "audit"
                })
                .to_string(),
            ),
            input_tokens: 12,
            output_tokens: 3,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        }
    }

    fn message(sequence: usize, role: &str, text: &str) -> session::SessionMessage {
        session::SessionMessage {
            stable_message_id: format!("message-{sequence}"),
            session_id: "hydrate-session".to_string(),
            sequence,
            role: role.to_string(),
            content_json: serde_json::json!([{"type": "text", "text": text}]).to_string(),
            blocks_count: 1,
            tool_use_id: None,
            tool_name: None,
            token_usage_json: (role == "assistant").then(|| {
                serde_json::json!({
                    "input_tokens": 12,
                    "output_tokens": 3,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0
                })
                .to_string()
            }),
            created_at_ms: 1_000 + sequence as u64,
        }
    }

    #[test]
    fn durable_hydration_restores_order_model_usage_workspace_and_lineage() {
        let session = hydrated_runtime_session(
            record(),
            vec![
                message(1, "assistant", "historical answer"),
                message(0, "user", "historical question"),
            ],
        )
        .expect("durable transcript should hydrate");

        assert_eq!(session.session_id, "hydrate-session");
        assert_eq!(session.model.as_deref(), Some("effective-model"));
        assert_eq!(session.message_count(), 2);
        assert_eq!(
            session.message(0).expect("user").role,
            runtime::MessageRole::User
        );
        assert_eq!(
            session.message(1).expect("assistant").role,
            runtime::MessageRole::Assistant
        );
        assert_eq!(
            session
                .message(1)
                .expect("assistant")
                .usage
                .as_ref()
                .map(|usage| (usage.input_tokens, usage.output_tokens)),
            Some((12, 3))
        );
        assert_eq!(
            session.workspace_root.as_deref(),
            Some(Path::new("/workspace/project"))
        );
        assert_eq!(
            session
                .fork
                .as_ref()
                .map(|fork| (fork.parent_session_id.as_str(), fork.branch_name.as_deref())),
            Some(("parent-session", Some("audit")))
        );
        assert_eq!(session.created_at_ms, 1_784_854_800_000);
        assert_eq!(session.updated_at_ms, 1_784_858_400_000);
    }

    #[test]
    fn durable_hydration_rejects_transcript_gaps_instead_of_forgetting_turns() {
        let error = hydrated_runtime_session(record(), vec![message(1, "assistant", "gap")])
            .expect_err("a non-contiguous transcript must fail closed");

        assert!(error.contains("expected sequence 0"));
    }

    #[test]
    fn durable_hydration_uses_turn_causality_without_mutating_physical_cursor_order() {
        let causal_message =
            |sequence: usize, id: &str, role: &str, text: &str, turn: &str, ingress: &str| {
                session::SessionMessage {
                    stable_message_id: id.to_string(),
                    session_id: "hydrate-session".to_string(),
                    sequence,
                    role: role.to_string(),
                    content_json: serde_json::json!([{
                        "type": "text",
                        "text": text,
                        "cowd_turn_id": turn,
                        "cowd_turn_ingress_message_id": ingress,
                    }])
                    .to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: sequence as u64 + 1,
                }
            };
        let session = hydrated_runtime_session(
            record(),
            vec![
                causal_message(0, "user-1", "user", "first", "turn-1", "user-1"),
                causal_message(1, "user-2", "user", "second", "turn-2", "user-2"),
                causal_message(
                    2,
                    "assistant-1",
                    "assistant",
                    "first answer",
                    "turn-1",
                    "user-1",
                ),
            ],
        )
        .expect("contiguous physical rows with causal metadata should hydrate");

        let text = session
            .messages()
            .map(|message| match message.blocks.first() {
                Some(runtime::ContentBlock::Text { text }) => text.as_str(),
                other => panic!("expected text block, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(text, vec!["first", "first answer", "second"]);
        assert_eq!(
            session
                .messages()
                .map(|message| message.role)
                .collect::<Vec<_>>(),
            vec![
                runtime::MessageRole::User,
                runtime::MessageRole::Assistant,
                runtime::MessageRole::User,
            ]
        );
    }
}
