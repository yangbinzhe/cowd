//! One-shot import for the removed pre-V5 Team state file.
//!
//! The old file did not contain the graph/node/AgentRuntime bindings required
//! to resume a Team safely. It is therefore preserved as durable audit events
//! and every non-terminal legacy item is explicitly dispositioned as blocked.
//! No imported record becomes an executable Team without a canonical graph.

use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runtime_event_store::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeTransactionEventInput,
};
use crate::{RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyTeamImportReport {
    pub source_path: String,
    pub source_hash: String,
    pub duplicate: bool,
    pub imported_team_ids: Vec<String>,
    pub blocked_team_ids: Vec<String>,
    pub source_removed: bool,
}

#[derive(Debug, Deserialize)]
struct LegacyTeamStateFile {
    #[serde(default)]
    runs: std::collections::BTreeMap<String, LegacyTeamRecord>,
}

#[derive(Debug, Deserialize)]
struct LegacyTeamRecord {
    snapshot: serde_json::Value,
}

pub fn import_legacy_team_state_file(
    event_store: Arc<RuntimeEventStore>,
    path: &Path,
) -> Result<Option<LegacyTeamImportReport>, String> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read(path).map_err(|error| error.to_string())?;
    let source_hash = format!("{:x}", Sha256::digest(&raw));
    let marker_stream = format!("upgrade:team-runtime-legacy:{source_hash}");
    if !event_store
        .list_stream(&marker_stream)
        .map_err(|error| error.to_string())?
        .is_empty()
    {
        // The durable import committed but source retirement was interrupted.
        // Never replay it; finish the only remaining retirement operation.
        std::fs::remove_file(path).map_err(|error| error.to_string())?;
        return Ok(Some(LegacyTeamImportReport {
            source_path: path.display().to_string(),
            source_hash,
            duplicate: true,
            source_removed: true,
            ..LegacyTeamImportReport::default()
        }));
    }
    let legacy: LegacyTeamStateFile =
        serde_json::from_slice(&raw).map_err(|error| error.to_string())?;
    let mut events = Vec::new();
    let mut expected_streams = Vec::new();
    let mut imported_team_ids = Vec::new();
    let mut blocked_team_ids = Vec::new();
    let mut seen_team_ids = std::collections::BTreeSet::new();
    for (map_key, record) in legacy.runs {
        let team_id = record
            .snapshot
            .get("team_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(map_key.as_str())
            .to_string();
        if !seen_team_ids.insert(team_id.clone()) {
            return Err(format!(
                "legacy team state contains duplicate team identity {team_id}; refusing partial import"
            ));
        }
        let status = record
            .snapshot
            .get("status")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let terminal = matches!(status, "completed" | "cancelled" | "failed");
        // Legacy snapshots have no graph binding and cannot be merged with a
        // live canonical Team stream. Keep the import in a hash-scoped audit
        // stream, with the original identity in the payload, so no Team state
        // can be accidentally resurrected or double-written.
        let stream_id = format!("upgrade:team-runtime-legacy:{source_hash}:team:{team_id}");
        expected_streams.push(ExpectedStreamRevision {
            stream_id: stream_id.clone(),
            expected_revision: 0,
        });
        events.push(RuntimeTransactionEventInput::from(RuntimeEventInput {
            stream_id,
            scope: RuntimeEventScope::Team,
            kind: "team.legacy_imported".to_string(),
            status: Some(if terminal { status } else { "blocked" }.to_string()),
            actor: Some("team_legacy_importer".to_string()),
            refs: vec![RuntimeEventRef {
                kind: "legacy_team_state".to_string(),
                id: source_hash.clone(),
            }],
            payload: serde_json::json!({
                "team_id": team_id,
                "legacy_status": status,
                "disposition": if terminal { "audit_only" } else { "blocked_unbound" },
                "reason": if terminal {
                    "legacy terminal state is retained as audit history; canonical graph result is required for projection"
                } else {
                    "legacy Team state lacks canonical ExecutionGraph and AgentRuntime bindings; it cannot resume"
                },
                "snapshot": record.snapshot,
            }),
        }));
        if !terminal {
            blocked_team_ids.push(team_id.clone());
        }
        imported_team_ids.push(team_id);
    }
    expected_streams.push(ExpectedStreamRevision {
        stream_id: marker_stream.clone(),
        expected_revision: 0,
    });
    events.push(RuntimeTransactionEventInput::from(RuntimeEventInput {
        stream_id: marker_stream,
        scope: RuntimeEventScope::Recovery,
        kind: "upgrade.legacy_team_imported".to_string(),
        status: Some("completed".to_string()),
        actor: Some("team_legacy_importer".to_string()),
        refs: Vec::new(),
        payload: serde_json::json!({
            "source_hash": source_hash,
            "team_count": imported_team_ids.len(),
            "blocked_count": blocked_team_ids.len(),
        }),
    }));
    event_store
        .append_transaction(AppendTransactionRequest {
            transaction_id: format!("legacy-team-import:{source_hash}"),
            expected_streams,
            events,
        })
        .map_err(|error| error.to_string())?;
    std::fs::remove_file(path).map_err(|error| error.to_string())?;
    Ok(Some(LegacyTeamImportReport {
        source_path: path.display().to_string(),
        source_hash,
        duplicate: false,
        imported_team_ids,
        blocked_team_ids,
        source_removed: true,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn imports_legacy_active_state_as_blocked_audit_and_removes_source() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.json");
        std::fs::write(
            &path,
            r#"{"runs":{"legacy-team":{"snapshot":{"team_id":"legacy-team","status":"running"}}}}"#,
        )
        .unwrap();
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        let report = import_legacy_team_state_file(Arc::clone(&store), &path)
            .unwrap()
            .unwrap();
        assert_eq!(report.blocked_team_ids, vec!["legacy-team"]);
        assert!(report.source_removed);
        assert!(!path.exists());
        let events = store
            .list_stream(&format!(
                "upgrade:team-runtime-legacy:{}:team:legacy-team",
                report.source_hash
            ))
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status.as_deref(), Some("blocked"));
        assert_eq!(events[0].payload["disposition"], "blocked_unbound");
    }

    #[test]
    fn duplicate_marker_finishes_source_retirement_without_reimporting() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.json");
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
        std::fs::write(
            &path,
            r#"{"runs":{"legacy-team":{"snapshot":{"team_id":"legacy-team","status":"completed"}}}}"#,
        )
        .unwrap();
        let first = import_legacy_team_state_file(Arc::clone(&store), &path)
            .unwrap()
            .unwrap();
        std::fs::write(
            &path,
            r#"{"runs":{"legacy-team":{"snapshot":{"team_id":"legacy-team","status":"completed"}}}}"#,
        )
        .unwrap();
        let second = import_legacy_team_state_file(store, &path)
            .unwrap()
            .unwrap();
        assert!(second.duplicate);
        assert_eq!(second.source_hash, first.source_hash);
        assert!(second.source_removed);
        assert!(!path.exists());
    }
}
