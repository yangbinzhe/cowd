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
    leases: RwLock<HashMap<String, SessionLease>>,
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
            let same_owner = existing.owner == owner;
            let compatible = existing.mode == "collaborative" && normalized_mode == "collaborative";
            let takeover = normalized_mode == "takeover";
            if !same_owner && !compatible && !takeover {
                return serde_json::json!({
                    "ok": false,
                    "error": "session lease is held by another owner",
                    "session_id": session_id,
                    "owner": existing.owner,
                    "mode": existing.mode,
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
        leases.insert(session_id.to_string(), lease.clone());

        serde_json::json!({
            "ok": true,
            "session_id": lease.session_id,
            "owner": lease.owner,
            "mode": lease.mode,
            "acquired_at_ms": lease.acquired_at_ms,
        })
    }

    pub async fn release(&self, session_id: &str, owner: &str) -> serde_json::Value {
        let mut leases = self.leases.write().await;
        match leases.get(session_id) {
            Some(existing) if existing.owner == owner => {
                leases.remove(session_id);
                serde_json::json!({
                    "ok": true,
                    "session_id": session_id,
                    "released": true,
                })
            }
            Some(existing) => serde_json::json!({
                "ok": false,
                "error": "session lease is held by another owner",
                "session_id": session_id,
                "owner": existing.owner,
                "mode": existing.mode,
            }),
            None => serde_json::json!({
                "ok": true,
                "session_id": session_id,
                "released": false,
            }),
        }
    }

    pub async fn list(&self) -> Vec<SessionLease> {
        let leases = self.leases.read().await;
        let mut items = leases.values().cloned().collect::<Vec<_>>();
        items.sort_by(|left, right| left.session_id.cmp(&right.session_id));
        items
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
    }
}
