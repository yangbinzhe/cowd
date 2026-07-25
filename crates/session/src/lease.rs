use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SessionLease {
    pub session_id: String,
    pub owner: String,
    pub mode: String,
    pub acquired_at_ms: u64,
}

#[derive(Default)]
pub struct SessionLeaseRegistry {
    leases: RwLock<HashMap<String, HashMap<String, SessionLease>>>,
}

impl SessionLeaseRegistry {
    pub async fn acquire(&self, session_id: &str, owner: &str, mode: &str) -> serde_json::Value {
        if session_id.trim().is_empty() || owner.trim().is_empty() {
            return serde_json::json!({
                "ok": false,
                "error": "session_id and owner are required",
            });
        }

        let normalized_mode = match mode {
            "exclusive" | "collaborative" | "takeover" => mode,
            _ => "collaborative",
        };

        let mut leases = self.leases.write().await;
        if let Some(existing) = leases.get(session_id) {
            let same_owner = existing.contains_key(owner);
            let compatible = normalized_mode == "collaborative"
                && existing.values().all(|lease| lease.mode == "collaborative");
            let takeover = normalized_mode == "takeover";
            if !same_owner && !compatible && !takeover {
                if let Some(holder) = existing.values().next() {
                    return serde_json::json!({
                        "ok": false,
                        "error": "session lease is held by another owner",
                        "session_id": session_id,
                        "owner": holder.owner,
                        "mode": holder.mode,
                    });
                }
            }
            if same_owner
                && normalized_mode == "exclusive"
                && existing
                    .keys()
                    .any(|existing_owner| existing_owner != owner)
            {
                return serde_json::json!({
                    "ok": false,
                    "error": "exclusive session lease requires takeover while collaborators remain",
                    "session_id": session_id,
                });
            }
        }

        let effective_mode = if normalized_mode == "takeover" {
            "exclusive"
        } else {
            normalized_mode
        };
        let lease = SessionLease {
            session_id: session_id.to_string(),
            owner: owner.to_string(),
            mode: effective_mode.to_string(),
            acquired_at_ms: current_epoch_ms(),
        };
        let owners = leases.entry(session_id.to_string()).or_default();
        if normalized_mode == "takeover" {
            owners.clear();
        }
        owners.insert(owner.to_string(), lease.clone());

        serde_json::json!({
            "ok": true,
            "session_id": lease.session_id,
            "owner": lease.owner,
            "mode": lease.mode,
            "acquired_at_ms": lease.acquired_at_ms,
        })
    }

    /// Atomically admit a writer without weakening an already-held lease.
    ///
    /// Existing owners keep their exact mode; a new writer may join only a
    /// collaborative group. This is the enforcement boundary used by message
    /// ingress, so a Surface cannot keep writing after its explicit lease
    /// acquisition was rejected.
    pub async fn ensure_writer(&self, session_id: &str, owner: &str) -> serde_json::Value {
        if session_id.trim().is_empty() || owner.trim().is_empty() {
            return serde_json::json!({
                "ok": false,
                "error": "session_id and owner are required",
            });
        }

        let mut leases = self.leases.write().await;
        if let Some(existing) = leases.get_mut(session_id) {
            if let Some(lease) = existing.get(owner) {
                return serde_json::json!({
                    "ok": true,
                    "session_id": lease.session_id,
                    "owner": lease.owner,
                    "mode": lease.mode,
                    "acquired_at_ms": lease.acquired_at_ms,
                    "existing": true,
                });
            }
            if !existing.values().all(|lease| lease.mode == "collaborative") {
                if let Some(holder) = existing.values().next() {
                    return serde_json::json!({
                        "ok": false,
                        "error": "session lease is held exclusively by another owner",
                        "session_id": session_id,
                        "owner": holder.owner,
                        "mode": holder.mode,
                    });
                }
            }
            let lease = SessionLease {
                session_id: session_id.to_string(),
                owner: owner.to_string(),
                mode: "collaborative".to_string(),
                acquired_at_ms: current_epoch_ms(),
            };
            existing.insert(owner.to_string(), lease.clone());
            return serde_json::json!({
                "ok": true,
                "session_id": lease.session_id,
                "owner": lease.owner,
                "mode": lease.mode,
                "acquired_at_ms": lease.acquired_at_ms,
                "existing": false,
            });
        }

        let lease = SessionLease {
            session_id: session_id.to_string(),
            owner: owner.to_string(),
            mode: "collaborative".to_string(),
            acquired_at_ms: current_epoch_ms(),
        };
        leases.insert(
            session_id.to_string(),
            HashMap::from([(owner.to_string(), lease.clone())]),
        );
        serde_json::json!({
            "ok": true,
            "session_id": lease.session_id,
            "owner": lease.owner,
            "mode": lease.mode,
            "acquired_at_ms": lease.acquired_at_ms,
            "existing": false,
        })
    }

    pub async fn release(&self, session_id: &str, owner: &str) -> serde_json::Value {
        let mut leases = self.leases.write().await;
        let mut remove_empty_group = false;
        let result = match leases.get_mut(session_id) {
            Some(existing) => {
                let released = existing.remove(owner).is_some();
                remove_empty_group = released && existing.is_empty();
                if released {
                    serde_json::json!({
                        "ok": true,
                        "session_id": session_id,
                        "released": true,
                    })
                } else if let Some(holder) = existing.values().next() {
                    serde_json::json!({
                        "ok": false,
                        "error": "session lease is held by another owner",
                        "session_id": session_id,
                        "owner": holder.owner,
                        "mode": holder.mode,
                    })
                } else {
                    remove_empty_group = true;
                    serde_json::json!({
                        "ok": true,
                        "session_id": session_id,
                        "released": false,
                    })
                }
            }
            None => serde_json::json!({
                "ok": true,
                "session_id": session_id,
                "released": false,
            }),
        };
        if remove_empty_group {
            leases.remove(session_id);
        }
        result
    }

    pub async fn list(&self) -> Vec<SessionLease> {
        let leases = self.leases.read().await;
        let mut items = leases
            .values()
            .flat_map(|owners| owners.values().cloned())
            .collect::<Vec<_>>();
        items.sort_by(|left, right| {
            left.session_id
                .cmp(&right.session_id)
                .then_with(|| left.owner.cmp(&right.owner))
        });
        items
    }

    /// Return sessions with at least one process-local writer lease.
    ///
    /// Writer leases are intentionally not restart durable. Durable Surface
    /// attachment state is tracked by the Session lifecycle kernel instead.
    pub async fn active_session_ids(&self) -> Vec<String> {
        let leases = self.leases.read().await;
        let mut session_ids = leases
            .iter()
            .filter_map(|(session_id, owners)| (!owners.is_empty()).then_some(session_id.clone()))
            .collect::<Vec<_>>();
        session_ids.sort();
        session_ids
    }
}

fn current_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn lease_rejects_conflicting_owner() {
        let registry = SessionLeaseRegistry::default();
        assert_eq!(registry.acquire("s1", "tui", "exclusive").await["ok"], true);
        assert_eq!(
            registry.acquire("s1", "webui", "exclusive").await["ok"],
            false
        );
    }

    #[tokio::test]
    async fn collaborative_leases_can_share() {
        let registry = SessionLeaseRegistry::default();
        assert_eq!(
            registry.acquire("s1", "tui", "collaborative").await["ok"],
            true
        );
        assert_eq!(
            registry.acquire("s1", "webui", "collaborative").await["ok"],
            true
        );
        assert_eq!(registry.list().await.len(), 2);
        assert_eq!(registry.release("s1", "tui").await["released"], true);
        let remaining = registry.list().await;
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].owner, "webui");
    }

    #[tokio::test]
    async fn message_writer_admission_preserves_exclusive_owner() {
        let registry = SessionLeaseRegistry::default();
        assert_eq!(
            registry.acquire("s1", "tui:a", "exclusive").await["ok"],
            true
        );
        let existing = registry.ensure_writer("s1", "tui:a").await;
        assert_eq!(existing["ok"], true);
        assert_eq!(existing["mode"], "exclusive");
        assert_eq!(registry.ensure_writer("s1", "webui:b").await["ok"], false);
        let leases = registry.list().await;
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].mode, "exclusive");
    }

    #[tokio::test]
    async fn active_session_ids_track_process_local_leases() {
        let registry = SessionLeaseRegistry::default();
        registry.acquire("s2", "web", "collaborative").await;
        registry.acquire("s1", "tui", "collaborative").await;
        registry.acquire("s1", "web", "collaborative").await;
        assert_eq!(registry.active_session_ids().await, vec!["s1", "s2"]);

        registry.release("s1", "tui").await;
        assert_eq!(registry.active_session_ids().await, vec!["s1", "s2"]);
        registry.release("s1", "web").await;
        assert_eq!(registry.active_session_ids().await, vec!["s2"]);
    }
}
