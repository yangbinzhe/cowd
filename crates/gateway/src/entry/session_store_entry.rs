use crate::{gateway_storage::GatewayStorage, CliOutputFormat, SHARED_RT};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use runtime::{ConversationMessage, JsonValue, Session};
use serde_json::json;

#[derive(Debug, Clone)]
pub(crate) struct SessionHandle {
    pub(crate) id: String,
    pub(crate) path: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct ManagedSessionSummary {
    pub(crate) id: String,
    pub(crate) path: PathBuf,
    pub(crate) updated_at_ms: u64,
    pub(crate) modified_epoch_millis: u128,
    pub(crate) message_count: usize,
    pub(crate) parent_session_id: Option<String>,
    pub(crate) branch_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalSessionImportCandidate {
    pub(crate) path: PathBuf,
    pub(crate) session_id: String,
}

/// Opens a scoped CLI repository. The daemon owns and injects its own store
/// through RuntimeServices; a process-global CLI cache would create a second
/// lifecycle authority and leak state across homes/workspaces.
pub(crate) fn get_unified_store() -> Result<memory::UnifiedSessionStore, Box<dyn std::error::Error>>
{
    GatewayStorage::open_unified_session_store(runtime::cowd_dirs::config_home_dir())
}

pub(crate) fn jsonl_sessions_dir() -> PathBuf {
    runtime::cowd_dirs::config_home_dir().join("sessions")
}

pub(crate) fn session_db_path() -> PathBuf {
    GatewayStorage::session_db_path(runtime::cowd_dirs::config_home_dir())
}

pub(crate) fn discover_local_session_import_candidates() -> Vec<LocalSessionImportCandidate> {
    let base = jsonl_sessions_dir();
    let mut roots = vec![base.clone(), base.join("global"), base.join("projects")];
    let mut candidates = Vec::new();

    while let Some(root) = roots.pop() {
        let entries = match fs::read_dir(&root) {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                roots.push(path);
                continue;
            }
            let ext = path
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or("");
            if ext != "jsonl" && ext != "json" {
                continue;
            }
            let Some(session_id) = path
                .file_stem()
                .and_then(|value| value.to_str())
                .map(std::string::ToString::to_string)
            else {
                continue;
            };
            candidates.push(LocalSessionImportCandidate { path, session_id });
        }
    }
    candidates.sort_by(|a, b| a.path.cmp(&b.path));
    candidates
}

fn migrate_session_messages(
    store: &memory::UnifiedSessionStore,
    session_id: &str,
    jsonl_path: &Path,
) -> Result<usize, Box<dyn std::error::Error>> {
    use std::io::{BufRead, BufReader};

    let file = fs::File::open(jsonl_path)?;
    let reader = BufReader::new(file);
    let mut batch = Vec::with_capacity(100);
    let mut total = 0usize;
    let mut sequence = 0usize;

    for line in reader.lines() {
        let line = line?;
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        if line.contains(r#""type":"session_meta""#) || line.contains(r#""type":"compaction""#) {
            continue;
        }

        if let Ok(value) = JsonValue::parse(&line) {
            if let Some(message_val) = value.as_object().and_then(|obj| obj.get("message")) {
                if let Ok(msg) = ConversationMessage::from_json(message_val) {
                    let record = msg.to_session_message(session_id, sequence);
                    batch.push(record);
                    sequence += 1;
                    total += 1;
                }
            }
        }

        if batch.len() >= 100 {
            SHARED_RT.block_on(store.insert_messages_batch(&batch))?;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        SHARED_RT.block_on(store.insert_messages_batch(&batch))?;
    }

    tracing::info!(
        session_id,
        count = total,
        "migrated session messages to SQLite"
    );
    Ok(total)
}

pub(crate) fn import_local_session_file(
    store: &memory::UnifiedSessionStore,
    path: &Path,
) -> Result<(String, usize), Box<dyn std::error::Error>> {
    if !path.exists() {
        return Err(format!("session file not found: {}", path.display()).into());
    }
    let ext = path
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("");
    if ext != "jsonl" && ext != "json" {
        return Err(format!(
            "unsupported session import format: {} (expected .jsonl or .json)",
            path.display()
        )
        .into());
    }

    let session = Session::load_from_path(path)?;
    let record = session_to_record(&session, path);
    let session_id = record.session_id.clone();
    SHARED_RT.block_on(async {
        if store.get_session(&session_id).await?.is_some() {
            store.update_session(&record).await?;
            store.delete_messages_from(&session_id, 0).await?;
            store
                .delete_events_by_type_from(&session_id, "message_appended", 0)
                .await?;
        } else {
            store.create_session(&record).await?;
        }
        Ok::<(), memory::MemoryError>(())
    })?;
    let imported_messages = migrate_session_messages(store, &session_id, path)?;
    Ok((session_id, imported_messages))
}

pub(crate) fn run_import_session(
    path: &Path,
    output_format: CliOutputFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let store = get_unified_store()?;
    let (session_id, imported_messages) = import_local_session_file(&store, path)?;
    match output_format {
        CliOutputFormat::Text => {
            println!(
                "Session imported\n  Session          {session_id}\n  Messages         {imported_messages}\n  Store            {}",
                session_db_path().display()
            );
        }
        CliOutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "kind": "session-import",
                    "session_id": session_id,
                    "messages": imported_messages,
                    "store": session_db_path(),
                }))?
            );
        }
    }
    Ok(())
}

fn session_to_record(session: &Session, path: &Path) -> memory::store::session::SessionRecord {
    use memory::store::session::SessionRecord;

    let id = session.session_id.clone();
    let now = chrono::Utc::now().to_rfc3339();
    let metadata = json!({
        "workspace_root": session.workspace_root().map(|p| p.display().to_string()),
        "parent_session_id": session.fork.as_ref().map(|f| f.parent_session_id.clone()),
        "branch_name": session.fork.as_ref().and_then(|f| f.branch_name.clone()),
        "legacy_path": path.display().to_string(),
    });

    SessionRecord {
        session_id: id,
        platform: "cli".to_string(),
        chat_id: path.display().to_string(),
        user_id: None,
        model: session.model.clone(),
        created_at: now.clone(),
        last_activity: now,
        message_count: session.message_count() as i64,
        reset_policy: "none".to_string(),
        metadata_json: Some(metadata.to_string()),
        input_tokens: 0,
        output_tokens: 0,
        estimated_cost_usd: 0.0,
        status: "active".to_string(),
    }
}

pub(crate) fn sync_cli_session_to_unified_store(
    store: &memory::UnifiedSessionStore,
    handle: &SessionHandle,
    model: Option<&str>,
    session: &Session,
) -> Result<(), Box<dyn std::error::Error>> {
    let now = chrono::Utc::now().to_rfc3339();
    let existing = SHARED_RT.block_on(store.get_session(&session.session_id))?;
    let created_at = existing
        .as_ref()
        .map(|record| record.created_at.clone())
        .unwrap_or_else(|| now.clone());
    let metadata = json!({
        "workspace_root": session.workspace_root().map(|p| p.display().to_string()),
        "parent_session_id": session.fork.as_ref().map(|f| f.parent_session_id.clone()),
        "branch_name": session.fork.as_ref().and_then(|f| f.branch_name.clone()),
        "session_path": handle.path.display().to_string(),
    });

    let record = memory::store::session::SessionRecord {
        session_id: session.session_id.clone(),
        platform: "cli".to_string(),
        chat_id: session.session_id.clone(),
        user_id: None,
        model: session
            .model
            .clone()
            .or_else(|| model.map(std::string::ToString::to_string)),
        created_at,
        last_activity: now,
        message_count: session.message_count() as i64,
        reset_policy: existing
            .as_ref()
            .map(|record| record.reset_policy.clone())
            .unwrap_or_else(|| "none".to_string()),
        metadata_json: Some(metadata.to_string()),
        input_tokens: session
            .messages()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| i64::from(usage.input_tokens))
            .sum(),
        output_tokens: session
            .messages()
            .filter_map(|message| message.usage.as_ref())
            .map(|usage| i64::from(usage.output_tokens))
            .sum(),
        estimated_cost_usd: existing
            .as_ref()
            .map(|record| record.estimated_cost_usd)
            .unwrap_or(0.0),
        status: "active".to_string(),
    };

    let existed = existing.is_some();
    SHARED_RT.block_on(async {
        if existed {
            store.update_session(&record).await?;
        } else {
            store.create_session(&record).await?;
        }
        store.delete_messages_from(&session.session_id, 0).await?;
        store
            .delete_events_by_type_from(&session.session_id, "message_appended", 0)
            .await?;

        let messages = session
            .messages()
            .enumerate()
            .map(|(sequence, message)| message.to_session_message(&session.session_id, sequence))
            .collect::<Vec<_>>();
        if !messages.is_empty() {
            store.insert_messages_batch(&messages).await?;
        }

        for (sequence, message) in session.messages().enumerate() {
            let message_json =
                serde_json::from_str::<serde_json::Value>(&message.to_json().render())
                    .unwrap_or(serde_json::Value::Null);
            let event = memory::SessionEvent {
                session_id: session.session_id.clone(),
                event_type: "message_appended".to_string(),
                event_json: json!({
                    "type": "message_appended",
                    "sequence": sequence,
                    "role": message.role.role_str(),
                    "message": message_json,
                })
                .to_string(),
                sequence,
                created_at_ms: messages
                    .get(sequence)
                    .map(|message| message.created_at_ms)
                    .unwrap_or(0),
            };
            store.append_event(&event).await?;
        }

        Ok::<(), memory::MemoryError>(())
    })?;

    Ok(())
}

pub(crate) fn hydrate_session_from_unified_store(
    store: &memory::UnifiedSessionStore,
    handle: &SessionHandle,
) -> Result<Option<Session>, Box<dyn std::error::Error>> {
    let Some(record) = SHARED_RT.block_on(store.get_session(&handle.id))? else {
        return Ok(None);
    };
    let stored_messages = SHARED_RT.block_on(store.get_all_messages(&record.session_id))?;
    hydrated_runtime_session(record, stored_messages)
        .map(Some)
        .map_err(Into::into)
}

/// Build Runtime's in-memory session from the canonical durable record.
///
/// Both the legacy CLI resume path and Gateway's lazy runtime activation use
/// this exact conversion. Keeping one parser is important: a Surface must not
/// appear to restore history while the model carrier is initialized with a
/// different (or empty) transcript.
pub(crate) fn hydrated_runtime_session(
    record: memory::SessionRecord,
    mut stored_messages: Vec<memory::SessionMessage>,
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

    fn record() -> memory::SessionRecord {
        memory::SessionRecord {
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

    fn message(sequence: usize, role: &str, text: &str) -> memory::SessionMessage {
        memory::SessionMessage {
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
                memory::SessionMessage {
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

pub(crate) fn sessions_dir() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(session_db_path())
}

pub(crate) fn new_cli_session() -> Result<Session, Box<dyn std::error::Error>> {
    Ok(Session::new().with_workspace_root(env::current_dir()?))
}

pub(crate) fn load_or_create_live_session(
    session_id: Option<String>,
) -> Result<(SessionHandle, Session), Box<dyn std::error::Error>> {
    let Some(session_id) = session_id else {
        let session_state = new_cli_session()?;
        let handle = create_managed_session_handle(&session_state.session_id)?;
        return Ok((handle, session_state));
    };

    match load_session_reference(&session_id) {
        Ok((handle, session)) => Ok((handle, session)),
        Err(error) if error.to_string().contains("session not found") => {
            let mut session_state = new_cli_session()?;
            session_state.session_id = session_id.clone();
            let handle = create_managed_session_handle(&session_id)?;
            Ok((handle, session_state))
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn create_managed_session_handle(
    session_id: &str,
) -> Result<SessionHandle, Box<dyn std::error::Error>> {
    let path = session_db_path();
    let workspace_root = env::current_dir()?;

    if let Ok(store) = get_unified_store() {
        let now = chrono::Utc::now().to_rfc3339();
        let metadata = json!({
            "workspace_root": workspace_root.display().to_string(),
        });
        let record = memory::store::session::SessionRecord {
            session_id: session_id.to_string(),
            platform: "cli".to_string(),
            chat_id: session_id.to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "none".to_string(),
            metadata_json: Some(metadata.to_string()),
            input_tokens: 0,
            output_tokens: 0,
            estimated_cost_usd: 0.0,
            status: "active".to_string(),
        };
        let _ = SHARED_RT.block_on(store.create_session(&record));
        let _ = SHARED_RT.block_on(store.upsert_session(&record));
    }

    Ok(SessionHandle {
        id: session_id.to_string(),
        path,
    })
}

pub(crate) fn resolve_session_reference(
    reference: &str,
) -> Result<SessionHandle, Box<dyn std::error::Error>> {
    if reference.eq_ignore_ascii_case("latest")
        || reference.eq_ignore_ascii_case("last")
        || reference.eq_ignore_ascii_case("recent")
    {
        let store = get_unified_store()?;
        let workspace_records = list_workspace_session_records(&store)?;
        let record = workspace_records
            .iter()
            .find(|record| record.message_count > 0)
            .cloned()
            .or_else(|| workspace_records.into_iter().next())
            .or_else(|| {
                SHARED_RT
                    .block_on(store.list_sessions())
                    .ok()
                    .and_then(|records| {
                        records
                            .iter()
                            .find(|record| record.message_count > 0)
                            .cloned()
                            .or_else(|| records.into_iter().next())
                    })
            })
            .ok_or_else(|| -> Box<dyn std::error::Error> { "no managed sessions found".into() })?;
        return Ok(SessionHandle {
            id: record.session_id,
            path: session_db_path(),
        });
    }

    let direct = PathBuf::from(reference);
    let candidate = if direct.is_absolute() {
        direct.clone()
    } else {
        env::current_dir()?.join(&direct)
    };
    let looks_like_path = direct.extension().is_some() || direct.components().count() > 1;

    if candidate.exists() {
        let id = candidate
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(reference)
            .to_string();
        return Ok(SessionHandle {
            id,
            path: candidate,
        });
    }

    if looks_like_path {
        return Err(format!("session file not found: {reference}").into());
    }

    let path = resolve_managed_session_path(reference)?;
    let id = if path == session_db_path() {
        reference.to_string()
    } else {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(reference)
            .to_string()
    };
    Ok(SessionHandle { id, path })
}

fn resolve_managed_session_path(session_id: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if let Ok(store) = get_unified_store() {
        if let Ok(Some(_record)) = SHARED_RT.block_on(store.get_session(session_id)) {
            return Ok(session_db_path());
        }
    }

    Err(format!("session not found: {session_id}").into())
}

pub(crate) fn list_managed_sessions(
) -> Result<Vec<ManagedSessionSummary>, Box<dyn std::error::Error>> {
    let store = get_unified_store()?;
    let records = list_workspace_session_records(&store)?;
    Ok(records.into_iter().map(record_to_summary).collect())
}

fn list_workspace_session_records(
    store: &memory::UnifiedSessionStore,
) -> Result<Vec<memory::store::session::SessionRecord>, Box<dyn std::error::Error>> {
    let workspace_root = env::current_dir()?;
    SHARED_RT
        .block_on(
            store.list_sessions_by_workspace_root(workspace_root.display().to_string().as_str()),
        )
        .map_err(|e| -> Box<dyn std::error::Error> {
            format!("failed to list workspace sessions: {e}").into()
        })
}

fn record_to_summary(record: memory::store::session::SessionRecord) -> ManagedSessionSummary {
    let path = session_db_path();
    let last_activity_ms = chrono::DateTime::parse_from_rfc3339(&record.last_activity)
        .ok()
        .map(|dt| dt.timestamp_millis().max(0) as u64)
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(&record.last_activity, "%Y-%m-%dT%H:%M:%S%.fZ")
                .ok()
                .map(|dt| dt.and_utc().timestamp_millis().max(0) as u64)
        })
        .unwrap_or(0);

    let (parent_session_id, branch_name) = record
        .metadata_json
        .as_deref()
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
        .map(|v| {
            (
                v.get("parent_session_id")
                    .and_then(|s| s.as_str())
                    .map(String::from),
                v.get("branch_name")
                    .and_then(|s| s.as_str())
                    .map(String::from),
            )
        })
        .unwrap_or((None, None));

    ManagedSessionSummary {
        id: record.session_id,
        path,
        updated_at_ms: last_activity_ms,
        modified_epoch_millis: u128::from(last_activity_ms),
        message_count: record.message_count.max(0) as usize,
        parent_session_id,
        branch_name,
    }
}

pub(crate) fn latest_managed_session() -> Result<ManagedSessionSummary, Box<dyn std::error::Error>>
{
    list_managed_sessions()?
        .into_iter()
        .next()
        .ok_or_else(|| -> Box<dyn std::error::Error> { "no managed sessions found".into() })
}

pub(crate) fn load_session_reference(
    reference: &str,
) -> Result<(SessionHandle, Session), Box<dyn std::error::Error>> {
    let handle = resolve_session_reference(reference)?;
    let session = if let Ok(store) = get_unified_store() {
        if let Some(hydrated) = hydrate_session_from_unified_store(&store, &handle)? {
            hydrated
        } else if handle.path.exists() {
            return Err(format!(
                "local session file is not imported: {}. Import it explicitly before resume.",
                handle.path.display()
            )
            .into());
        } else {
            return Err(format!("session not found: {}", handle.id).into());
        }
    } else if handle.path.exists() {
        return Err(format!(
            "local session file is not imported: {}. Import it explicitly before resume.",
            handle.path.display()
        )
        .into());
    } else {
        return Err(format!("session not found: {}", handle.id).into());
    };

    if let Some(ref session_workspace) = session.workspace_root {
        let current_dir = env::current_dir()?;
        if *session_workspace != current_dir {
            tracing::warn!(
                session_workspace = %session_workspace.display(),
                current_workspace = %current_dir.display(),
                session_id = %session.session_id,
                "session workspace mismatch: session was created in '{}' but current workspace is '{}'",
                session_workspace.display(),
                current_dir.display()
            );
        }
    }

    Ok((handle, session))
}

pub(crate) fn delete_managed_session(session_id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let store = get_unified_store()?;
    SHARED_RT.block_on(store.delete_session(session_id))?;
    Ok(())
}

pub(crate) fn confirm_session_deletion(session_id: &str) -> bool {
    print!("Delete session '{session_id}'? This cannot be undone. [y/N]: ");
    io::stdout().flush().unwrap_or(());
    let mut answer = String::new();
    if io::stdin().read_line(&mut answer).is_err() {
        return false;
    }
    matches!(answer.trim(), "y" | "Y" | "yes" | "Yes" | "YES")
}

pub(crate) fn render_session_list(
    active_session_id: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let sessions = list_managed_sessions()?;
    let import_candidates = discover_local_session_import_candidates();
    let mut lines = vec![
        "Sessions".to_string(),
        format!("  Store             {}", session_db_path().display()),
    ];
    if !import_candidates.is_empty() {
        lines.push(format!(
            "  Local imports     {} legacy session file(s) available; import explicitly to use them.",
            import_candidates.len()
        ));
    }
    if sessions.is_empty() {
        lines.push("  No managed sessions saved yet.".to_string());
        return Ok(lines.join("\n"));
    }
    for session in sessions {
        let marker = if session.id == active_session_id {
            "● current"
        } else {
            "○ saved"
        };
        let lineage = match (
            session.branch_name.as_deref(),
            session.parent_session_id.as_deref(),
        ) {
            (Some(branch_name), Some(parent_session_id)) => {
                format!(" branch={branch_name} from={parent_session_id}")
            }
            (None, Some(parent_session_id)) => format!(" from={parent_session_id}"),
            (Some(branch_name), None) => format!(" branch={branch_name}"),
            (None, None) => String::new(),
        };
        lines.push(format!(
            "  {id:<20} {marker:<10} msgs={msgs:<4} updated={modified}{lineage} store={path}",
            id = session.id,
            msgs = session.message_count,
            modified = format_session_modified_age(session.modified_epoch_millis),
            lineage = lineage,
            path = session.path.display(),
        ));
    }
    Ok(lines.join("\n"))
}

fn format_session_modified_age(modified_epoch_millis: u128) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map_or(modified_epoch_millis, |duration| duration.as_millis());
    let delta_seconds = now
        .saturating_sub(modified_epoch_millis)
        .checked_div(1_000)
        .unwrap_or_default();
    match delta_seconds {
        0..=4 => "just-now".to_string(),
        5..=59 => format!("{delta_seconds}s-ago"),
        60..=3_599 => format!("{}m-ago", delta_seconds / 60),
        3_600..=86_399 => format!("{}h-ago", delta_seconds / 3_600),
        _ => format!("{}d-ago", delta_seconds / 86_400),
    }
}

pub(crate) fn write_session_clear_backup(
    session: &Session,
    session_path: &Path,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let backup_path = session_clear_backup_path(session_path);
    if let Some(parent) = backup_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&backup_path, session.export_jsonl()?)?;
    Ok(backup_path)
}

pub(crate) fn session_clear_backup_path(session_path: &Path) -> PathBuf {
    let timestamp = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map_or(0, |duration| duration.as_millis());
    let file_name = session_path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("session");
    session_path.with_file_name(format!("{file_name}.before-clear-{timestamp}.jsonl"))
}
