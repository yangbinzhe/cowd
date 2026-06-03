//! SQLite-backed [`PersistenceProtocol`] implementation.
//!
//! Wraps [`memory::sqlite_persistence::SqlitePersistence`] and converts
//! between the runtime's session types and the memory crate's raw data
//! types, keeping the crate graph acyclic.

use std::path::Path;
use async_trait::async_trait;
use tokio::task::spawn_blocking;

use rusqlite::params;

use memory::sqlite_persistence::{
    self as mem_sqlite, BlockData, CleanupConfig as MemCleanupConfig, MessageData,
    SessionRecordData, SqlitePersistence as RawPersistence,
};

use crate::persistence::{CleanupConfig, PersistenceProtocol, Result, StoreStats};
use crate::session::{ContentBlock, ConversationMessage, MessageRole, SessionRecord};
use crate::usage::TokenUsage;

// ---------------------------------------------------------------------------
// SqlitePersistence (newtype wrapper)
// ---------------------------------------------------------------------------

/// SQLite-backed persistence implementing [`PersistenceProtocol`].
///
/// Delegates all SQL operations to [`memory::sqlite_persistence::SqlitePersistence`]
/// and handles type conversion between runtime session types and raw storage types.
pub struct SqlitePersistence(RawPersistence);

impl std::fmt::Debug for SqlitePersistence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl SqlitePersistence {
    /// Open (or create) a persistent database at `path`.
    pub fn open(
        path: &Path,
        cleanup_config: CleanupConfig,
    ) -> Result<Self> {
        let mem_config = MemCleanupConfig {
            max_sessions: cleanup_config.max_sessions,
            max_days: cleanup_config.max_days,
        };
        let raw = RawPersistence::open(path, mem_config)?;
        Ok(Self(raw))
    }

    /// Open an in-memory database (useful for testing).
    pub fn open_in_memory(cleanup_config: CleanupConfig) -> Result<Self> {
        let mem_config = MemCleanupConfig {
            max_sessions: cleanup_config.max_sessions,
            max_days: cleanup_config.max_days,
        };
        let raw = RawPersistence::open_in_memory(mem_config)?;
        Ok(Self(raw))
    }
}

// ---------------------------------------------------------------------------
// Type conversion helpers
// ---------------------------------------------------------------------------

fn message_to_message_data(msg: &ConversationMessage) -> MessageData {
    MessageData {
        role: match msg.role {
            MessageRole::User => "user",
            MessageRole::Assistant => "assistant",
            MessageRole::System => "system",
            MessageRole::Tool => "tool",
        }
        .to_string(),
        blocks: msg.blocks.iter().map(block_to_block_data).collect(),
        usage_input: msg
            .usage
            .as_ref()
            .map(|u| i64::from(u.input_tokens))
            .unwrap_or(0),
        usage_output: msg
            .usage
            .as_ref()
            .map(|u| i64::from(u.output_tokens))
            .unwrap_or(0),
    }
}

fn block_to_block_data(block: &ContentBlock) -> BlockData {
    match block {
        ContentBlock::Text { text } => BlockData {
            block_type: "text".into(),
            text: Some(text.clone()),
            signature: None,
            tool_id: None,
            tool_name: None,
            tool_input: None,
            tool_output: None,
            is_error: false,
        },
        ContentBlock::Thinking {
            thinking,
            signature,
        } => BlockData {
            block_type: "thinking".into(),
            text: Some(thinking.clone()),
            signature: signature.clone(),
            tool_id: None,
            tool_name: None,
            tool_input: None,
            tool_output: None,
            is_error: false,
        },
        ContentBlock::ToolUse { id, name, input } => BlockData {
            block_type: "tool_use".into(),
            text: None,
            signature: None,
            tool_id: Some(id.clone()),
            tool_name: Some(name.clone()),
            tool_input: Some(input.clone()),
            tool_output: None,
            is_error: false,
        },
        ContentBlock::ToolResult {
            tool_use_id,
            tool_name,
            output,
            is_error,
        } => BlockData {
            block_type: "tool_result".into(),
            text: None,
            signature: None,
            tool_id: Some(tool_use_id.clone()),
            tool_name: Some(tool_name.clone()),
            tool_input: None,
            tool_output: Some(output.clone()),
            is_error: *is_error,
        },
    }
}

fn message_data_to_message(data: &MessageData) -> ConversationMessage {
    let role = match data.role.as_str() {
        "user" => MessageRole::User,
        "assistant" => MessageRole::Assistant,
        "system" => MessageRole::System,
        _ => MessageRole::Tool,
    };
    let blocks: Vec<ContentBlock> = data
        .blocks
        .iter()
        .map(|b| match b.block_type.as_str() {
            "text" => ContentBlock::Text {
                text: b.text.clone().unwrap_or_default(),
            },
            "thinking" => ContentBlock::Thinking {
                thinking: b.text.clone().unwrap_or_default(),
                signature: b.signature.clone(),
            },
            "tool_use" => ContentBlock::ToolUse {
                id: b.tool_id.clone().unwrap_or_default(),
                name: b.tool_name.clone().unwrap_or_default(),
                input: b.tool_input.clone().unwrap_or_default(),
            },
            "tool_result" => ContentBlock::ToolResult {
                tool_use_id: b.tool_id.clone().unwrap_or_default(),
                tool_name: b.tool_name.clone().unwrap_or_default(),
                output: b.tool_output.clone().unwrap_or_default(),
                is_error: b.is_error,
            },
            _ => ContentBlock::Text {
                text: "[unknown block]".into(),
            },
        })
        .collect();
    let usage = if data.usage_input > 0 || data.usage_output > 0 {
        Some(TokenUsage {
            input_tokens: data.usage_input as u32,
            output_tokens: data.usage_output as u32,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
        })
    } else {
        None
    };
    ConversationMessage { role, blocks, usage }
}

fn session_record_to_data(record: &SessionRecord) -> SessionRecordData {
    SessionRecordData {
        session_id: record.session_id.clone(),
        title: record.title.clone(),
        model: record.model.clone(),
        message_count: record.message_count,
        created_at_ms: record.created_at_ms,
        last_activity: record.last_activity,
    }
}

fn session_record_from_data(data: &SessionRecordData) -> SessionRecord {
    SessionRecord {
        session_id: data.session_id.clone(),
        title: data.title.clone(),
        model: data.model.clone(),
        message_count: data.message_count,
        created_at_ms: data.created_at_ms,
        last_activity: data.last_activity,
    }
}

// ---------------------------------------------------------------------------
// PersistenceProtocol impl
// ---------------------------------------------------------------------------

#[async_trait]
impl PersistenceProtocol for SqlitePersistence {
    async fn create_session(&self, record: &SessionRecord) -> Result<()> {
        let pool = self.0.write_pool.clone();
        let rec = session_record_to_data(record);
        let sid = rec.session_id.clone();
        spawn_blocking(move || {
            let conn = pool.get()?;
            mem_sqlite::SqlitePersistence::insert_session(&conn, &sid, &rec)
        })
        .await??;
        Ok(())
    }

    async fn get_session(&self, id: &str) -> Result<Option<SessionRecord>> {
        let pool = self.0.read_pool.clone();
        let sid = id.to_string();
        spawn_blocking(move || {
            let conn = pool.get()?;
            mem_sqlite::SqlitePersistence::get_session(&conn, &sid)
                .map(|opt| opt.as_ref().map(session_record_from_data))
        })
        .await?
    }

    async fn list_sessions(&self) -> Result<Vec<SessionRecord>> {
        let pool = self.0.read_pool.clone();
        spawn_blocking(move || {
            let conn = pool.get()?;
            let records = mem_sqlite::SqlitePersistence::list_sessions(&conn)?;
            Ok(records.iter().map(session_record_from_data).collect())
        })
        .await?
    }

    async fn update_session(&self, id: &str, record: &SessionRecord) -> Result<()> {
        let pool = self.0.write_pool.clone();
        let sid = id.to_string();
        let rec = session_record_to_data(record);
        spawn_blocking(move || {
            let conn = pool.get()?;
            mem_sqlite::SqlitePersistence::update_session(&conn, &sid, &rec)
        })
        .await??;
        Ok(())
    }

    async fn delete_session(&self, id: &str) -> Result<()> {
        let pool = self.0.write_pool.clone();
        let sid = id.to_string();
        spawn_blocking(move || {
            let conn = pool.get()?;
            mem_sqlite::SqlitePersistence::delete_session(&conn, &sid)
        })
        .await??;
        Ok(())
    }

    async fn append_message(&self, session_id: &str, msg: &ConversationMessage) -> Result<()> {
        let pool = self.0.write_pool.clone();
        let sid = session_id.to_string();
        let data = message_to_message_data(msg);
        // Fire-and-forget: spawn, don't await JoinHandle
        spawn_blocking(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let conn = pool.get()?;
                mem_sqlite::SqlitePersistence::insert_message(&conn, &sid, &data)?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
            }));
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!("append_message failed: {e}"),
                Err(_panic) => tracing::warn!("append_message panicked"),
            }
        });
        Ok(())
    }

    async fn append_messages(
        &self,
        session_id: &str,
        msgs: &[ConversationMessage],
    ) -> Result<()> {
        let pool = self.0.write_pool.clone();
        let sid = session_id.to_string();
        let data: Vec<MessageData> = msgs.iter().map(message_to_message_data).collect();
        spawn_blocking(move || -> Result<()> {
            let mut conn = pool.get()?;
            let txn = conn.transaction()?;
            for msg in &data {
                mem_sqlite::SqlitePersistence::insert_message(&txn, &sid, msg)?;
            }
            txn.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn get_messages(
        &self,
        session_id: &str,
    ) -> Result<Vec<ConversationMessage>> {
        let pool = self.0.read_pool.clone();
        let sid = session_id.to_string();
        spawn_blocking(move || {
            let conn = pool.get()?;
            let data = mem_sqlite::SqlitePersistence::get_messages(&conn, &sid)?;
            Ok(data.iter().map(message_data_to_message).collect())
        })
        .await?
    }

    async fn get_messages_range(
        &self,
        session_id: &str,
        from: usize,
        limit: usize,
    ) -> Result<Vec<ConversationMessage>> {
        let pool = self.0.read_pool.clone();
        let sid = session_id.to_string();
        spawn_blocking(move || {
            let conn = pool.get()?;
            let all = mem_sqlite::SqlitePersistence::get_messages(&conn, &sid)?;
            Ok(all
                .iter()
                .skip(from)
                .take(limit)
                .map(message_data_to_message)
                .collect())
        })
        .await?
    }

    async fn get_message_count(&self, session_id: &str) -> Result<usize> {
        let pool = self.0.read_pool.clone();
        let sid = session_id.to_string();
        spawn_blocking(move || {
            let conn = pool.get()?;
            mem_sqlite::SqlitePersistence::get_message_count(&conn, &sid)
        })
        .await?
    }

    async fn delete_messages_from(&self, session_id: &str, sequence: usize) -> Result<()> {
        let pool = self.0.write_pool.clone();
        let sid = session_id.to_string();
        spawn_blocking(move || {
            let conn = pool.get()?;
            mem_sqlite::SqlitePersistence::delete_messages_from(&conn, &sid, sequence)
        })
        .await??;
        Ok(())
    }

    async fn search_messages(
        &self,
        query: &str,
    ) -> Result<Vec<ConversationMessage>> {
        let pool = self.0.read_pool.clone();
        let q = query.to_string();
        spawn_blocking(move || {
            let conn = pool.get()?;
            let session_ids =
                mem_sqlite::SqlitePersistence::search_messages_session_ids(&conn, &q)?;
            let mut all = Vec::new();
            for sid in session_ids {
                if let Ok(msgs) = mem_sqlite::SqlitePersistence::get_messages(&conn, &sid) {
                    all.extend(msgs.iter().map(message_data_to_message));
                }
            }
            Ok(all)
        })
        .await?
    }

    async fn search_sessions(&self, query: &str) -> Result<Vec<SessionRecord>> {
        let pool = self.0.read_pool.clone();
        let q = query.to_string();
        spawn_blocking(move || {
            let conn = pool.get()?;
            let records = mem_sqlite::SqlitePersistence::search_sessions(&conn, &q)?;
            Ok(records.iter().map(session_record_from_data).collect())
        })
        .await?
    }

    async fn save_snapshot(
        &self,
        session_id: &str,
        messages: &[ConversationMessage],
    ) -> Result<()> {
        let pool = self.0.write_pool.clone();
        let sid = session_id.to_string();
        let data: Vec<MessageData> = messages.iter().map(message_to_message_data).collect();
        spawn_blocking(move || -> Result<()> {
            let mut conn = pool.get()?;
            let txn = conn.transaction()?;
            txn.execute("DELETE FROM messages WHERE session_id=?1", params![sid])?;
            for msg in &data {
                mem_sqlite::SqlitePersistence::insert_message(&txn, &sid, msg)?;
            }
            txn.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn get_latest_snapshot(
        &self,
        _session_id: &str,
    ) -> Result<Option<Vec<ConversationMessage>>> {
        // Snapshots are implicit: the messages table IS the snapshot.
        // For consistent restore, callers should use get_messages().
        Ok(None)
    }

    async fn cleanup(&self) -> Result<usize> {
        let pool = self.0.write_pool.clone();
        let config = self.0.cleanup_config.clone();
        spawn_blocking(move || {
            let conn = pool.get()?;
            mem_sqlite::SqlitePersistence::cleanup_with_config(&conn, &config)
        })
        .await?
    }

    async fn flush(&self) -> Result<()> {
        let pool = self.0.write_pool.clone();
        spawn_blocking(move || {
            let conn = pool.get()?;
            mem_sqlite::SqlitePersistence::wal_checkpoint(&conn)
        })
        .await??;
        Ok(())
    }

    async fn stats(&self) -> Result<StoreStats> {
        let pool = self.0.read_pool.clone();
        let path = self.0.db_path();
        spawn_blocking(move || {
            let conn = pool.get()?;
            let s = mem_sqlite::SqlitePersistence::compute_stats(&conn, &path)?;
            Ok(StoreStats {
                session_count: s.session_count,
                message_count: s.message_count,
                db_size_bytes: s.db_size_bytes,
            })
        })
        .await?
    }
}
