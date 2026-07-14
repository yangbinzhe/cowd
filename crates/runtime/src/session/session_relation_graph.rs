//! Event-sourced cross-session relation projection.
//!
//! Relations and compact proxies are runtime metadata, not a second Session
//! store. The durable stream is the sole authority; the in-memory map only
//! accelerates reads and is rebuilt on Runtime startup.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::runtime_event_store::RuntimeTransactionEventInput;
use crate::{RuntimeEventInput, RuntimeEventScope, RuntimeEventStore};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionRelationKind {
    References,
    DependsOn,
    Blocks,
    Reviews,
    ConflictsWith,
    Supersedes,
    ContributesTo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRelation {
    pub relation_id: String,
    pub from_session_id: String,
    pub to_session_id: String,
    pub kind: SessionRelationKind,
    pub summary: String,
    pub evidence_refs: Vec<String>,
    pub created_at_ms: u64,
}

/// A deliberately compact, non-secret view that may be addressed by another
/// session. Full history and raw evidence remain in the owning Session store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionProxy {
    pub session_id: String,
    pub summary: String,
    pub evidence_refs: Vec<String>,
    pub decisions: Vec<String>,
    pub open_questions: Vec<String>,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRouteCommand {
    pub from_session_id: String,
    pub target_ref: String,
    pub command: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionRouteReceipt {
    pub from_session_id: String,
    pub target_ref: String,
    pub resolved_session_id: Option<String>,
    pub resolved_agent_id: Option<String>,
    pub status: String,
    pub message: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct SessionRelationState {
    relations: BTreeMap<String, SessionRelation>,
    proxies: BTreeMap<String, SessionProxy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionRelationStateEvent {
    state: SessionRelationState,
}

#[derive(Debug)]
pub struct SessionRelationGraph {
    state: Mutex<SessionRelationState>,
    event_store: Option<Arc<RuntimeEventStore>>,
    stream_id: Option<String>,
}

impl Default for SessionRelationGraph {
    fn default() -> Self {
        Self {
            state: Mutex::new(SessionRelationState::default()),
            event_store: None,
            stream_id: None,
        }
    }
}

impl SessionRelationGraph {
    /// Test-only/in-memory projection. RuntimeServices always uses
    /// `event_sourced`, so a process map is never production truth.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn event_sourced(
        event_store: Arc<RuntimeEventStore>,
        workspace_key: impl Into<String>,
    ) -> Result<Self, String> {
        let stream_id = format!("session-relations:{}", workspace_key.into());
        Ok(Self {
            state: Mutex::new(load_state(&event_store, &stream_id)?),
            event_store: Some(event_store),
            stream_id: Some(stream_id),
        })
    }

    pub fn add_relation(
        &self,
        from_session_id: impl Into<String>,
        to_session_id: impl Into<String>,
        kind: SessionRelationKind,
        summary: impl Into<String>,
        evidence_refs: Vec<String>,
    ) -> Result<SessionRelation, String> {
        let from_session_id = from_session_id.into();
        let to_session_id = to_session_id.into();
        let summary = summary.into();
        if from_session_id.trim().is_empty() || to_session_id.trim().is_empty() {
            return Err("relation session ids must not be empty".to_string());
        }
        if summary.trim().is_empty() {
            return Err("relation summary must not be empty".to_string());
        }
        let relation = SessionRelation {
            relation_id: format!("session-relation-{}", uuid::Uuid::new_v4()),
            from_session_id,
            to_session_id,
            kind,
            summary,
            evidence_refs,
            created_at_ms: now_ms(),
        };
        self.mutate("session.relation.added.v1", |state| {
            state
                .relations
                .insert(relation.relation_id.clone(), relation.clone());
            Ok(relation.clone())
        })
    }

    pub fn upsert_proxy(&self, proxy: SessionProxy) -> Result<SessionProxy, String> {
        if proxy.session_id.trim().is_empty() {
            return Err("proxy session_id must not be empty".to_string());
        }
        self.mutate("session.proxy.upserted.v1", |state| {
            state
                .proxies
                .insert(proxy.session_id.clone(), proxy.clone());
            Ok(proxy)
        })
    }

    #[must_use]
    pub fn relations_for(&self, session_id: &str) -> Vec<SessionRelation> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .relations
            .values()
            .filter(|relation| {
                relation.from_session_id == session_id || relation.to_session_id == session_id
            })
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn proxy(&self, session_id: &str) -> Option<SessionProxy> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .proxies
            .get(session_id)
            .cloned()
    }

    /// Route resolution is a pure lookup. It never dispatches a turn; callers
    /// must compile the returned reference into a typed SessionHandoff graph.
    pub fn route(&self, command: SessionRouteCommand) -> SessionRouteReceipt {
        let target = command.target_ref.trim_start_matches('@').to_string();
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.proxies.contains_key(&target) {
            return SessionRouteReceipt {
                from_session_id: command.from_session_id,
                target_ref: command.target_ref,
                resolved_session_id: Some(target),
                resolved_agent_id: None,
                status: "resolved".to_string(),
                message: "target resolved from durable session proxy".to_string(),
            };
        }
        SessionRouteReceipt {
            from_session_id: command.from_session_id,
            target_ref: command.target_ref,
            resolved_session_id: None,
            resolved_agent_id: Some(target),
            status: "unresolved_session".to_string(),
            message:
                "reference is not a durable session proxy; compile an AgentTask if appropriate"
                    .to_string(),
        }
    }

    pub fn projection(&self) -> serde_json::Value {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        serde_json::json!({
            "kind": "runtime.session_relations",
            "durable": self.event_store.is_some(),
            "stream_id": self.stream_id,
            "relation_count": state.relations.len(),
            "proxy_count": state.proxies.len(),
            "relations": state.relations.values().cloned().collect::<Vec<_>>(),
            "proxies": state.proxies.values().cloned().collect::<Vec<_>>(),
        })
    }

    /// Durable relation aggregate revision used by the mission command
    /// boundary for optimistic concurrency checks.
    pub fn revision(&self) -> Result<u64, String> {
        match (&self.event_store, &self.stream_id) {
            (Some(event_store), Some(stream_id)) => event_store
                .stream_revision(stream_id)
                .map_err(|error| error.to_string()),
            _ => Ok(self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .relations
                .len() as u64),
        }
    }

    fn mutate<T>(
        &self,
        event_kind: &str,
        operation: impl FnOnce(&mut SessionRelationState) -> Result<T, String>,
    ) -> Result<T, String> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = state.clone();
        let result = operation(&mut state)?;
        if let Err(error) = self.commit(&state, event_kind) {
            *state = previous;
            return Err(error);
        }
        Ok(result)
    }

    fn commit(&self, state: &SessionRelationState, event_kind: &str) -> Result<(), String> {
        let (Some(event_store), Some(stream_id)) = (&self.event_store, &self.stream_id) else {
            return Ok(());
        };
        let revision = event_store
            .stream_revision(stream_id)
            .map_err(|error| error.to_string())?;
        event_store
            .append_batch_if_revision(
                stream_id.clone(),
                revision,
                format!(
                    "session-relations:{stream_id}:{}",
                    revision.saturating_add(1)
                ),
                vec![RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: stream_id.clone(),
                        scope: RuntimeEventScope::Session,
                        kind: event_kind.to_string(),
                        status: Some("committed".to_string()),
                        actor: Some("session_relation_projection".to_string()),
                        refs: Vec::new(),
                        payload: serde_json::to_value(SessionRelationStateEvent {
                            state: state.clone(),
                        })
                        .map_err(|error| error.to_string())?,
                    },
                    idempotency_key: Some(format!(
                        "relations-revision:{}",
                        revision.saturating_add(1)
                    )),
                    schema_version: 1,
                }],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }
}

fn load_state(
    event_store: &RuntimeEventStore,
    stream_id: &str,
) -> Result<SessionRelationState, String> {
    event_store
        .list_stream(stream_id)?
        .into_iter()
        .rev()
        .find_map(|event| event.kind.starts_with("session.").then_some(event))
        .map(|event| {
            serde_json::from_value::<SessionRelationStateEvent>(event.payload)
                .map(|event| event.state)
                .map_err(|error| error.to_string())
        })
        .transpose()
        .map(|state| state.unwrap_or_default())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_sourced_projection_rebuilds_after_runtime_restart() {
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));
        let relations = SessionRelationGraph::event_sourced(Arc::clone(&store), "workspace-a")
            .expect("relation projection");
        let relation = relations
            .add_relation(
                "session-a",
                "session-b",
                SessionRelationKind::Reviews,
                "A reviews B",
                vec!["evidence:1".to_string()],
            )
            .expect("relation");
        relations
            .upsert_proxy(SessionProxy {
                session_id: "session-b".to_string(),
                summary: "B summary".to_string(),
                evidence_refs: vec!["evidence:1".to_string()],
                decisions: vec!["ship".to_string()],
                open_questions: vec!["risk?".to_string()],
                updated_at_ms: now_ms(),
            })
            .expect("proxy");

        let rebuilt =
            SessionRelationGraph::event_sourced(store, "workspace-a").expect("rebuilt projection");
        assert_eq!(rebuilt.relations_for("session-a"), vec![relation]);
        assert!(rebuilt.proxy("session-b").is_some());
        assert_eq!(rebuilt.projection()["durable"], true);
    }

    #[test]
    fn unresolved_reference_is_not_misrepresented_as_a_session_route() {
        let graph = SessionRelationGraph::new();
        let receipt = graph.route(SessionRouteCommand {
            from_session_id: "session-a".to_string(),
            target_ref: "@agent-reviewer".to_string(),
            command: "review".to_string(),
        });
        assert_eq!(receipt.status, "unresolved_session");
        assert!(receipt.resolved_session_id.is_none());
        assert_eq!(receipt.resolved_agent_id.as_deref(), Some("agent-reviewer"));
    }
}
