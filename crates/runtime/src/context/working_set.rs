//! Model-owned working preferences; source bytes stay in Memory/ArtifactStore.
//! A bounded latest-state record uses the existing PostgreSQL Runtime journal.
use crate::{
    ContextAuthority, ContextItem, ContextRole, ContextSourceKind, ContextVisibility,
    RuntimeEventStore, RuntimeServices,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Arc};

const MAX_PINS: usize = 64;
const STATE_BYTES: usize = 64 * 1024;
const WINDOW_BYTES: usize = 64 * 1024;
const PAGE_BYTES: u64 = 8 * 1024;
const EVENT_KIND: &str = "context.working_set_updated";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkingSource {
    Artifact { content_ref: String },
    Memory { memory_id: String },
}
impl WorkingSource {
    fn key(&self) -> String {
        match self {
            Self::Artifact { content_ref } => content_ref.clone(),
            Self::Memory { memory_id } => format!("memory:{memory_id}"),
        }
    }
    fn read_request(&self) -> Value {
        match self {
            Self::Artifact { content_ref } => {
                json!({"name":"evidence_retrieve","input":{"evidence_ref":content_ref}})
            }
            Self::Memory { memory_id } => {
                json!({"name":"context_retrieve","input":{"source":"memory","memory_id":memory_id}})
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkingContextInput {
    Pin {
        source: WorkingSource,
    },
    Expand {
        source: WorkingSource,
        source_hash: String,
        offset: u64,
    },
    Unpin {
        source: WorkingSource,
    },
    List {
        #[serde(default)]
        cursor: Option<String>,
    },
}
/// These are Agent-authored notes, never independently verified facts.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrivateNoteKind {
    Observation,
    Hypothesis,
    Question,
    Decision,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PrivateNoteInput {
    pub kind: PrivateNoteKind,
    pub title: String,
    pub content: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct WorkingPin {
    source: WorkingSource,
    source_hash: String,
    offset: u64,
    lease_digest: String,
}
#[derive(Default, Serialize, Deserialize)]
struct WorkingState {
    pins: BTreeMap<String, WorkingPin>,
}
fn digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}
fn binding(context: &memory::MemoryTurnContext) -> Result<(String, String), String> {
    if context.session_id.trim().is_empty() || context.agent_id.trim().is_empty() {
        return Err("working_context requires an immutable Session and Agent binding".into());
    }
    let identity =
        serde_json::to_vec(&(&context.session_id, &context.agent_id)).map_err(|e| e.to_string())?;
    let lease = serde_json::to_vec(context).map_err(|e| e.to_string())?;
    Ok((
        format!("context-working:{}", digest(&identity)),
        digest(&lease),
    ))
}
fn read_state(store: &RuntimeEventStore, stream: &str) -> Result<(u64, WorkingState), String> {
    let revision = store.stream_revision(stream).map_err(|e| e.to_string())?;
    if revision == 0 {
        return Ok((0, WorkingState::default()));
    }
    let events = store.list_stream_after(stream, revision - 1, revision, 1, STATE_BYTES * 2)?;
    let event = events
        .first()
        .filter(|e| e.sequence == revision && e.kind == EVENT_KIND)
        .ok_or("working context latest state is unavailable")?;
    let state =
        serde_json::from_value(event.payload["state"].clone()).map_err(|e| e.to_string())?;
    Ok((revision, state))
}
struct SourcePage {
    hash: String,
    total: u64,
    bytes: Vec<u8>,
    media_type: String,
}
impl RuntimeServices {
    /// Persist a private Agent report through the original Memory owner.
    pub async fn private_note_command(
        &self,
        context: &memory::MemoryTurnContext,
        action_id: &str,
        input: PrivateNoteInput,
    ) -> Result<Value, String> {
        let (identity, lease) = binding(context)?;
        if action_id.trim().is_empty()
            || input.title.trim().is_empty()
            || input.content.trim().is_empty()
        {
            return Err(
                "private_note requires a Runtime invocation identity, title and content".into(),
            );
        }
        if input.title.len() > 1024 {
            return Err("private note title exceeds metadata capacity".into());
        }
        let manager = self
            .memory_manager()
            .ok_or("private note Memory owner unavailable")?;
        let command_hash =
            digest(&serde_json::to_vec(&(&lease, &input)).map_err(|e| e.to_string())?);
        let seed =
            Sha256::digest(serde_json::to_vec(&(&identity, action_id)).map_err(|e| e.to_string())?);
        let mut id_bytes = [0u8; 16];
        id_bytes.copy_from_slice(&seed[..16]);
        let id = uuid::Uuid::from_bytes(id_bytes);
        let stream = format!("private-note:{id}");
        let actor = context.agent_id.clone();
        let record = |stage: &'static str| {
            let store = Arc::clone(self.event_store());
            let stream = stream.clone();
            let actor = actor.clone();
            let command_hash = command_hash.clone();
            async move {
                tokio::task::spawn_blocking(move || store.with_stream_lock(&stream, || {
                    if let Some(event) = store.event_by_idempotency_key(&stream, stage).map_err(|e|e.to_string())? {
                        if event.payload["command_hash"].as_str() != Some(command_hash.as_str()) {
                            return Err("private note invocation reused with different input or binding".to_string());
                        }
                        return Ok(event);
                    }
                    let revision = store.stream_revision(&stream).map_err(|e|e.to_string())?;
                    store.append_transaction_locked(crate::AppendTransactionRequest {
                        transaction_id: format!("{stream}:{stage}"),
                        expected_streams: vec![crate::ExpectedStreamRevision {stream_id:stream.clone(), expected_revision:revision}],
                        events: vec![crate::RuntimeTransactionEventInput {
                            event: crate::RuntimeEventInput {stream_id:stream.clone(), scope:crate::RuntimeEventScope::Session,
                                kind:format!("memory.private_note_{stage}"), status:Some(stage.into()), actor:Some(actor), refs:vec![],
                                payload:json!({"command_hash":command_hash,"memory_id":id.to_string()})},
                            idempotency_key:Some(stage.into()),schema_version:1,
                        }],
                    }).map_err(|e|e.to_string())?;
                    store.event_by_idempotency_key(&stream, stage).map_err(|e|e.to_string())?
                        .ok_or_else(||"private note journal receipt unavailable".into())
                })).await.map_err(|e|e.to_string())?
            }
        };
        let intent = record("intent").await?;
        let store = Arc::clone(self.event_store());
        let stream_copy = stream.clone();
        let completed = tokio::task::spawn_blocking(move || {
            store
                .event_by_idempotency_key(&stream_copy, "completed")
                .map_err(|e| e.to_string())
        })
        .await
        .map_err(|e| e.to_string())??
        .is_some();
        let kernel = memory::MemoryKernel::new(manager);
        let kind = serde_json::to_value(&input.kind).map_err(|e| e.to_string())?;
        let body = format!(
            "Agent-reported {} (not independently verified).\n\n{}",
            kind.as_str().unwrap_or("note"),
            input.content
        );
        let existing = kernel
            .retrieve_visible_entry(context, id)
            .await
            .map_err(|e| e.to_string())?;
        if existing.is_none() {
            if completed {
                return Err("completed private note source is unavailable; old invocation cannot recreate it".into());
            }
            let time = chrono::DateTime::from_timestamp_millis(
                intent
                    .created_at_ms
                    .try_into()
                    .map_err(|_| "invalid private note timestamp")?,
            )
            .ok_or("invalid private note timestamp")?;
            kernel
                .remember(
                    context,
                    memory::MemoryEntry {
                        id,
                        layer: memory::MemoryLayer::L3,
                        category: memory::MemoryCategory::Reference,
                        priority: memory::Priority::Normal,
                        source: memory::MemorySource::AutoExtracted,
                        title: input.title.clone(),
                        content: body.clone(),
                        embedding: None,
                        tags: vec![
                            "agent-private-note".into(),
                            format!("note-kind:{}", kind.as_str().unwrap_or("note")),
                            format!("note-invocation:{id}"),
                        ],
                        relations: vec![],
                        confidence: 0.5,
                        access_count: 0,
                        staleness: 0.0,
                        created_at: time,
                        updated_at: time,
                        last_accessed_at: None,
                        scope: memory::MemoryScope::AgentInstance(context.agent_id.clone()),
                        session_id: Some(context.session_id.clone()),
                        source_agent: Some(context.agent_id.clone()),
                        visibility: memory::AgentVisibility::Private,
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
        }
        let saved = kernel
            .retrieve_visible_entry(context, id)
            .await
            .map_err(|e| e.to_string())?
            .ok_or("private note was not persisted in the current binding")?;
        if saved.content != body
            || saved.title != input.title
            || saved.source != memory::MemorySource::AutoExtracted
            || saved.visibility != memory::AgentVisibility::Private
            || saved.source_agent.as_deref() != Some(context.agent_id.as_str())
            || saved.scope != memory::MemoryScope::AgentInstance(context.agent_id.clone())
        {
            return Err(
                "private note source changed; existing content will not be overwritten".into(),
            );
        }
        record("completed").await?;
        let source = WorkingSource::Memory {
            memory_id: id.to_string(),
        };
        Ok(
            json!({"kind":"runtime.private_note","status":"persisted","memory_id":id.to_string(),
            "note_kind":kind,"epistemic_status":"agent_reported","independently_verified":false,
            "visibility":"private","read_request":source.read_request(),
            "pin_request":{"name":"working_context","input":{"operation":"pin","source":source}}}),
        )
    }

    async fn working_source_page(
        &self,
        context: &memory::MemoryTurnContext,
        source: &WorkingSource,
        offset: u64,
    ) -> Result<SourcePage, String> {
        if source.key().len() > 1024 {
            return Err("working source reference exceeds metadata capacity".into());
        }
        match source {
            WorkingSource::Artifact { content_ref } => {
                let store = Arc::clone(self.artifact_store());
                let content_ref = content_ref.clone();
                let scope = format!("session:{}", context.session_id);
                tokio::task::spawn_blocking(move || {
                    let artifact = store.resolve(&content_ref).map_err(|e| e.to_string())?;
                    if offset > artifact.bytes {
                        return Err("working source offset exceeds original bytes".into());
                    }
                    let bytes = store
                        .read_blocking(
                            &artifact,
                            &scope,
                            Some(offset..offset.saturating_add(PAGE_BYTES).min(artifact.bytes)),
                        )
                        .map_err(|e| e.to_string())?;
                    Ok(SourcePage {
                        hash: artifact.sha256,
                        total: artifact.bytes,
                        bytes,
                        media_type: artifact.media_type,
                    })
                })
                .await
                .map_err(|e| e.to_string())?
            }
            WorkingSource::Memory { memory_id } => {
                let id = uuid::Uuid::parse_str(memory_id).map_err(|e| e.to_string())?;
                let manager = self
                    .memory_manager()
                    .ok_or("working context memory manager unavailable")?;
                let entry = memory::MemoryKernel::new(manager)
                    .retrieve_visible_entry(context, id)
                    .await
                    .map_err(|e| e.to_string())?
                    .ok_or("working memory source is unavailable in the current binding")?;
                let mut revision = Sha256::new();
                revision.update(entry.id.as_bytes());
                revision.update(entry.updated_at.to_rfc3339().as_bytes());
                revision.update([0]);
                revision.update(entry.content.as_bytes());
                let total = entry.content.len() as u64;
                if offset > total {
                    return Err("working source offset exceeds original bytes".into());
                }
                let end = offset.saturating_add(PAGE_BYTES).min(total) as usize;
                Ok(SourcePage {
                    hash: format!("sha256:{:x}", revision.finalize()),
                    total,
                    bytes: entry.content.as_bytes()[offset as usize..end].to_vec(),
                    media_type: "text/markdown; charset=utf-8".into(),
                })
            }
        }
    }

    /// Pin/expand changes only this Agent's working selection. Every read,
    /// including an idempotent retry, resolves the current source authority.
    pub async fn working_context_command(
        &self,
        context: &memory::MemoryTurnContext,
        action_id: &str,
        command: WorkingContextInput,
    ) -> Result<Value, String> {
        let (stream, lease_digest) = binding(context)?;
        if let WorkingContextInput::List { cursor } = &command {
            return self
                .working_context_window_page(context, cursor.as_deref())
                .await;
        }
        if action_id.trim().is_empty() {
            return Err("working context mutation requires a Runtime invocation identity".into());
        }
        let command_digest = digest(&serde_json::to_vec(&command).map_err(|e| e.to_string())?);
        let (source, pin) = match &command {
            WorkingContextInput::Pin { source } => {
                let page = self.working_source_page(context, source, 0).await?;
                (
                    source.clone(),
                    Some(WorkingPin {
                        source: source.clone(),
                        source_hash: page.hash,
                        offset: 0,
                        lease_digest,
                    }),
                )
            }
            WorkingContextInput::Expand {
                source,
                source_hash,
                offset,
            } => {
                let page = self.working_source_page(context, source, *offset).await?;
                if &page.hash != source_hash {
                    return Err(
                        "working source changed; inspect and pin its current version".into(),
                    );
                }
                (
                    source.clone(),
                    Some(WorkingPin {
                        source: source.clone(),
                        source_hash: page.hash,
                        offset: *offset,
                        lease_digest,
                    }),
                )
            }
            WorkingContextInput::Unpin { source } => (source.clone(), None),
            WorkingContextInput::List { .. } => unreachable!(),
        };
        let store = Arc::clone(self.event_store());
        let actor = context.agent_id.clone();
        let action_key = format!("working-command:{}", digest(action_id.as_bytes()));
        tokio::task::spawn_blocking(move || store.with_stream_lock(&stream,|| {
            if let Some(previous)=store.event_by_idempotency_key(&stream,&action_key).map_err(|e|e.to_string())? {
                if previous.payload["command_digest"].as_str()!=Some(command_digest.as_str()) {
                    return Err("working context invocation ID was reused with different input".to_string());
                }
                return Ok(());
            }
            let (revision,mut state)=read_state(&store,&stream)?;
            if let Some(pin)=pin {state.pins.insert(source.key(),pin);} else {state.pins.remove(&source.key());}
            if state.pins.len()>MAX_PINS || serde_json::to_vec(&state).map_err(|e|e.to_string())?.len()>STATE_BYTES {
                return Err("working context capacity reached; unpin inactive materials, originals remain retrievable".into());
            }
            store.append_transaction_locked(crate::AppendTransactionRequest {
                transaction_id:format!("{stream}:{action_key}"),
                expected_streams:vec![crate::ExpectedStreamRevision {stream_id:stream.clone(),expected_revision:revision}],
                events:vec![crate::RuntimeTransactionEventInput {
                    event:crate::RuntimeEventInput {stream_id:stream.clone(),scope:crate::RuntimeEventScope::Session,
                        kind:EVENT_KIND.into(),status:Some("applied".into()),actor:Some(actor),refs:vec![],
                        payload:json!({"command_digest":command_digest,"state":state})},
                    idempotency_key:Some(action_key),schema_version:1,
                }],
            }).map_err(|e|e.to_string())?;
            Ok(())
        })).await.map_err(|e|e.to_string())??;
        self.working_context_window(context).await
    }

    pub async fn working_context_window(
        &self,
        context: &memory::MemoryTurnContext,
    ) -> Result<Value, String> {
        self.working_context_window_page(context, None).await
    }

    async fn working_context_window_page(
        &self,
        context: &memory::MemoryTurnContext,
        cursor: Option<&str>,
    ) -> Result<Value, String> {
        let (stream, lease_digest) = binding(context)?;
        let store = Arc::clone(self.event_store());
        let (revision, state) = tokio::task::spawn_blocking(move || read_state(&store, &stream))
            .await
            .map_err(|e| e.to_string())??;
        let after_ref = if let Some(cursor) = cursor {
            let value: Value =
                serde_json::from_str(cursor).map_err(|_| "invalid working directory cursor")?;
            if value["revision"].as_u64() != Some(revision)
                || value["lease_digest"].as_str() != Some(lease_digest.as_str())
            {
                return Err("working directory cursor source or binding changed".into());
            }
            let after = value["after_ref"]
                .as_str()
                .ok_or("working cursor has no position")?
                .to_string();
            if !state.pins.contains_key(&after) {
                return Err("working cursor position is unavailable".into());
            }
            Some(after)
        } else {
            None
        };
        let total = state.pins.len();
        let mut last_ref = after_ref.clone();
        let mut remaining = false;
        let mut entries = Vec::new();
        let mut used = 0usize;
        for (reference, pin) in &state.pins {
            if after_ref.as_ref().is_some_and(|after| reference <= after) {
                continue;
            }
            let mut entry = json!({"source":pin.source,"source_hash":pin.source_hash,"offset":pin.offset,"read_request":pin.source.read_request()});
            if pin.lease_digest != lease_digest {
                entry["omission"] =
                    json!("Runtime binding changed; explicitly repin under the current lease");
            } else if used.saturating_add(PAGE_BYTES as usize * 2) > WINDOW_BYTES {
                entry["omission"]=json!("working window capacity; use read_request or expand after unpinning inactive materials");
            } else {
                match self
                    .working_source_page(context, &pin.source, pin.offset)
                    .await
                {
                    Ok(page) if page.hash == pin.source_hash => {
                        let next = pin.offset + page.bytes.len() as u64;
                        let (encoding, content) = match String::from_utf8(page.bytes.clone()) {
                            Ok(text) => ("utf8", text),
                            Err(_) => {
                                use base64::Engine;
                                (
                                    "base64",
                                    base64::engine::general_purpose::STANDARD.encode(&page.bytes),
                                )
                            }
                        };
                        entry["content"] = json!(content);
                        entry["encoding"] = json!(encoding);
                        entry["media_type"] = json!(page.media_type);
                        entry["total_bytes"] = json!(page.total);
                        entry["end_offset"] = json!(next);
                        entry["complete"] = json!(pin.offset == 0 && next == page.total);
                        if next < page.total {
                            entry["next_request"] = json!({"name":"working_context","input":{"operation":"expand","source":pin.source,"source_hash":page.hash,"offset":next}});
                        }
                    }
                    Ok(_) => {
                        entry["omission"] =
                            json!("source changed; inspect and repin current version")
                    }
                    Err(error) => entry["omission"] = json!(error),
                }
            }
            let size = serde_json::to_vec(&entry).map_err(|e| e.to_string())?.len();
            if used.saturating_add(size) > WINDOW_BYTES {
                remaining = true;
                break;
            }
            used += size;
            last_ref = Some(reference.clone());
            entries.push(entry);
        }
        let next_cursor = remaining
            .then(|| {
                serde_json::to_string(
                    &json!({"revision":revision,"lease_digest":lease_digest,"after_ref":last_ref}),
                )
            })
            .transpose()
            .map_err(|e| e.to_string())?;
        Ok(
            json!({"kind":"runtime.working_context","revision":revision,"entries":entries,
            "coverage":{"active_references":total,"listed_references":entries.len(),"window_bytes":used,"capacity_bytes":WINDOW_BYTES,"complete_directory":!remaining,"continued":after_ref.is_some()},
            "list_request":{"name":"working_context","input":{"operation":"list"}},
            "next_request":next_cursor.map(|cursor|json!({"name":"working_context","input":{"operation":"list","cursor":cursor}})),
            "meaning":"private working selection; neither source authority nor provider semantic observation"}),
        )
    }

    pub(crate) async fn working_context_item(
        &self,
        context: &memory::MemoryTurnContext,
    ) -> Result<Option<ContextItem>, String> {
        let window = self.working_context_window(context).await?;
        if window["coverage"]["active_references"].as_u64() == Some(0) {
            return Ok(None);
        }
        let (stream, _) = binding(context)?;
        let mut item = ContextItem::new(
            stream,
            ContextSourceKind::Memory,
            ContextRole::TaskState,
            window.to_string(),
        );
        item.authority = ContextAuthority::Derived;
        item.visibility = ContextVisibility::Private;
        item.source_version = Some(window["revision"].to_string());
        Ok(Some(item))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    async fn artifact(
        services: &RuntimeServices,
        context: &memory::MemoryTurnContext,
        bytes: &[u8],
    ) -> harness_contract::context::ArtifactRef {
        services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/plain".into(),
                    visibility_scope: format!("session:{}", context.session_id),
                    expected_bytes: Some(bytes.len() as u64),
                    original_name: None,
                },
                bytes,
            )
            .await
            .unwrap()
    }
    fn page_bytes(window: &Value) -> Vec<u8> {
        let entry = &window["entries"][0];
        let text = entry["content"].as_str().unwrap();
        if entry["encoding"] == "base64" {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(text)
                .unwrap()
        } else {
            text.as_bytes().to_vec()
        }
    }
    #[tokio::test]
    async fn pinned_original_expands_losslessly_and_unpin_survives_fresh_state_reads() {
        let services = RuntimeServices::in_memory().unwrap();
        let context = memory::MemoryTurnContext::new("working-session", "agent-one");
        let body = format!("BEGIN{}END", "中文🙂".repeat(10_000));
        let stored = artifact(&services, &context, body.as_bytes()).await;
        let source = WorkingSource::Artifact {
            content_ref: stored.selector.clone(),
        };
        let command = WorkingContextInput::Pin {
            source: source.clone(),
        };
        let mut page = services
            .working_context_command(&context, "pin-1", command.clone())
            .await
            .unwrap();
        let repeated = services
            .working_context_command(&context, "pin-1", command)
            .await
            .unwrap();
        assert_eq!(page["revision"], repeated["revision"]);
        assert!(services
            .working_context_command(
                &context,
                "pin-1",
                WorkingContextInput::Unpin {
                    source: source.clone()
                }
            )
            .await
            .unwrap_err()
            .contains("different input"));
        assert!(services
            .working_context_command(
                &context,
                "bad-hash",
                WorkingContextInput::Expand {
                    source: source.clone(),
                    source_hash: "wrong".into(),
                    offset: 0
                }
            )
            .await
            .unwrap_err()
            .contains("source changed"));
        let mut restored = Vec::new();
        let mut index = 0;
        loop {
            restored.extend(page_bytes(&page));
            let next = &page["entries"][0]["next_request"]["input"];
            if next.is_null() {
                break;
            }
            index += 1;
            page = services
                .working_context_command(
                    &context,
                    &format!("expand-{index}"),
                    serde_json::from_value(next.clone()).unwrap(),
                )
                .await
                .unwrap();
        }
        assert_eq!(restored, body.as_bytes());
        let other = memory::MemoryTurnContext::new("working-session", "agent-two");
        assert_eq!(
            services.working_context_window(&other).await.unwrap()["coverage"]["active_references"],
            0
        );
        let changed = context.clone().with_task_id(Some("changed-lease".into()));
        let denied = services.working_context_window(&changed).await.unwrap();
        assert!(denied["entries"][0]["content"].is_null());
        assert!(denied["entries"][0]["omission"]
            .as_str()
            .unwrap()
            .contains("binding changed"));
        let unpinned = services
            .working_context_command(&context, "unpin", WorkingContextInput::Unpin { source })
            .await
            .unwrap();
        assert_eq!(unpinned["coverage"]["active_references"], 0);
        assert!(services
            .working_context_item(&context)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            services
                .artifact_store()
                .read(&stored, "session:working-session", None)
                .await
                .unwrap(),
            body.as_bytes()
        );
    }
    #[tokio::test]
    async fn concurrent_working_updates_preserve_both_sources_and_deleted_source_never_replays() {
        let services = RuntimeServices::in_memory().unwrap();
        let context = memory::MemoryTurnContext::new("working-concurrent", "agent");
        let first = artifact(&services, &context, b"first source").await;
        let second = artifact(&services, &context, b"second source").await;
        let (a, b) = tokio::join!(
            services.working_context_command(
                &context,
                "one",
                WorkingContextInput::Pin {
                    source: WorkingSource::Artifact {
                        content_ref: first.selector.clone()
                    }
                }
            ),
            services.working_context_command(
                &context,
                "two",
                WorkingContextInput::Pin {
                    source: WorkingSource::Artifact {
                        content_ref: second.selector.clone()
                    }
                }
            ),
        );
        a.unwrap();
        b.unwrap();
        let window = services.working_context_window(&context).await.unwrap();
        assert_eq!(window["entries"].as_array().unwrap().len(), 2);
        services
            .artifact_store()
            .delete(&first, "session:working-concurrent")
            .unwrap();
        let window = services.working_context_window(&context).await.unwrap();
        assert!(!window.to_string().contains("first source"));
        assert!(window["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["omission"].is_string()));
        let item = services
            .working_context_item(&context)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.visibility, ContextVisibility::Private);
        assert_eq!(item.authority, ContextAuthority::Derived);
    }
    #[tokio::test]
    async fn full_working_directory_is_resumable_and_cursor_rejects_changed_selection() {
        let services = RuntimeServices::in_memory().unwrap();
        let context = memory::MemoryTurnContext::new("working-directory", "agent");
        let mut expected = std::collections::BTreeSet::new();
        let body = vec![0; PAGE_BYTES as usize];
        for index in 0..MAX_PINS {
            let stored = artifact(&services, &context, &body).await;
            expected.insert(stored.selector.clone());
            services
                .working_context_command(
                    &context,
                    &format!("directory-pin-{index}"),
                    WorkingContextInput::Pin {
                        source: WorkingSource::Artifact {
                            content_ref: stored.selector,
                        },
                    },
                )
                .await
                .unwrap();
        }
        let first = services.working_context_window(&context).await.unwrap();
        assert!(
            first["next_request"].is_object(),
            "escaped raw pages must exercise directory continuation"
        );
        let saved_next = first["next_request"]["input"].clone();
        let mut page = first;
        let mut actual = std::collections::BTreeSet::new();
        loop {
            for entry in page["entries"].as_array().unwrap() {
                assert!(actual.insert(entry["source"]["content_ref"].as_str().unwrap().to_string()));
            }
            let next = page["next_request"]["input"].clone();
            if next.is_null() {
                break;
            }
            page = services
                .working_context_command(
                    &context,
                    "directory-read",
                    serde_json::from_value(next).unwrap(),
                )
                .await
                .unwrap();
        }
        assert_eq!(actual, expected);
        services
            .working_context_command(
                &context,
                "release-one",
                WorkingContextInput::Unpin {
                    source: WorkingSource::Artifact {
                        content_ref: expected.first().unwrap().clone(),
                    },
                },
            )
            .await
            .unwrap();
        assert!(services
            .working_context_command(
                &context,
                "old-page",
                serde_json::from_value(saved_next).unwrap()
            )
            .await
            .unwrap_err()
            .contains("cursor source or binding changed"));
    }
}
