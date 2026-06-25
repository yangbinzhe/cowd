//! Cross-session relation graph and routing contracts.

use std::collections::BTreeMap;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

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

#[derive(Debug, Default)]
pub struct SessionRelationGraph {
    relations: Mutex<BTreeMap<String, SessionRelation>>,
    proxies: Mutex<BTreeMap<String, SessionProxy>>,
}

impl SessionRelationGraph {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
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
        self.relations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(relation.relation_id.clone(), relation.clone());
        Ok(relation)
    }

    pub fn upsert_proxy(&self, proxy: SessionProxy) -> Result<SessionProxy, String> {
        if proxy.session_id.trim().is_empty() {
            return Err("proxy session_id must not be empty".to_string());
        }
        self.proxies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(proxy.session_id.clone(), proxy.clone());
        Ok(proxy)
    }

    #[must_use]
    pub fn relations_for(&self, session_id: &str) -> Vec<SessionRelation> {
        self.relations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .filter(|relation| {
                relation.from_session_id == session_id || relation.to_session_id == session_id
            })
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn proxy(&self, session_id: &str) -> Option<SessionProxy> {
        self.proxies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .cloned()
    }

    pub fn route(&self, command: SessionRouteCommand) -> SessionRouteReceipt {
        let target = command.target_ref.trim_start_matches('@').to_string();
        let proxies = self
            .proxies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if proxies.contains_key(&target) {
            return SessionRouteReceipt {
                from_session_id: command.from_session_id,
                target_ref: command.target_ref,
                resolved_session_id: Some(target),
                resolved_agent_id: None,
                status: "routed".to_string(),
                message: "command routed to session proxy".to_string(),
            };
        }
        SessionRouteReceipt {
            from_session_id: command.from_session_id,
            target_ref: command.target_ref,
            resolved_session_id: None,
            resolved_agent_id: Some(target),
            status: "routed".to_string(),
            message: "command routed to agent reference".to_string(),
        }
    }

    pub fn projection(&self) -> serde_json::Value {
        let relations = self
            .relations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let proxies = self
            .proxies
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        serde_json::json!({
            "kind": "runtime.session_relations",
            "relation_count": relations.len(),
            "proxy_count": proxies.len(),
            "relations": relations,
            "proxies": proxies,
        })
    }
}

pub fn global_session_relation_graph() -> &'static SessionRelationGraph {
    static GRAPH: OnceLock<SessionRelationGraph> = OnceLock::new();
    GRAPH.get_or_init(SessionRelationGraph::new)
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
    fn session_relation_graph_tracks_relations_proxies_and_routes() {
        let graph = SessionRelationGraph::new();
        let relation = graph
            .add_relation(
                "session-a",
                "session-b",
                SessionRelationKind::Reviews,
                "A reviews B",
                vec!["evidence:1".to_string()],
            )
            .expect("relation");
        assert_eq!(relation.kind, SessionRelationKind::Reviews);
        graph
            .upsert_proxy(SessionProxy {
                session_id: "session-b".to_string(),
                summary: "B summary".to_string(),
                evidence_refs: vec!["evidence:1".to_string()],
                decisions: vec!["ship".to_string()],
                open_questions: vec!["risk?".to_string()],
                updated_at_ms: now_ms(),
            })
            .expect("proxy");

        assert_eq!(graph.relations_for("session-a").len(), 1);
        assert!(graph.proxy("session-b").is_some());
        let receipt = graph.route(SessionRouteCommand {
            from_session_id: "session-a".to_string(),
            target_ref: "@session-b".to_string(),
            command: "review".to_string(),
        });
        assert_eq!(receipt.resolved_session_id.as_deref(), Some("session-b"));
        assert_eq!(graph.projection()["relation_count"], 1);
    }
}
