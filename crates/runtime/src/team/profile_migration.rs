//! Explicit retirement of the removed Gateway Team Profile file.
//!
//! Legacy profiles contained mutable UI role names, not the pinned Agent
//! Definition revisions and contracts required by a runnable TeamTemplate.
//! They are therefore never silently converted into executable Teams.  The
//! source is moved into a hash-addressed archive and a durable event records
//! the operator action required: recreate it as a Draft TeamTemplate, validate
//! it, then publish a stable revision through the canonical Definition flow.

use std::path::Path;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::runtime_event_store::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeTransactionEventInput,
};
use crate::{RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeEventStore};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LegacyTeamProfileMigrationReport {
    pub source_path: String,
    pub archive_path: String,
    pub source_hash: String,
    pub duplicate: bool,
    pub profile_count: usize,
    pub parse_status: String,
    pub source_retired: bool,
}

pub fn archive_legacy_team_profile_file(
    event_store: Arc<RuntimeEventStore>,
    source_path: &Path,
    archive_root: &Path,
) -> Result<Option<LegacyTeamProfileMigrationReport>, String> {
    if !source_path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read(source_path).map_err(|error| error.to_string())?;
    let source_hash = format!("{:x}", Sha256::digest(&raw));
    let marker_stream = format!("upgrade:team-profile-legacy:{source_hash}");
    let archive_path = archive_root.join(format!("team-profiles-{source_hash}.json"));
    if !event_store
        .list_stream(&marker_stream)
        .map_err(|error| error.to_string())?
        .is_empty()
    {
        retire_source(source_path, &archive_path)?;
        return Ok(Some(LegacyTeamProfileMigrationReport {
            source_path: source_path.display().to_string(),
            archive_path: archive_path.display().to_string(),
            source_hash,
            duplicate: true,
            source_retired: true,
            ..LegacyTeamProfileMigrationReport::default()
        }));
    }

    let (profile_count, parse_status) = inspect_profile_file(&raw);
    event_store
        .append_transaction(AppendTransactionRequest {
            transaction_id: format!("legacy-team-profile-archive:{source_hash}"),
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: marker_stream.clone(),
                expected_revision: 0,
            }],
            events: vec![RuntimeTransactionEventInput::from(RuntimeEventInput {
                stream_id: marker_stream,
                scope: RuntimeEventScope::Team,
                kind: "team.legacy_profile_archived.v1".to_string(),
                status: Some("requires_human_recreate".to_string()),
                actor: Some("team_profile_migrator".to_string()),
                refs: vec![RuntimeEventRef {
                    kind: "legacy_team_profile_file".to_string(),
                    id: source_hash.clone(),
                }],
                payload: serde_json::json!({
                    "source_path": source_path.display().to_string(),
                    "archive_path": archive_path.display().to_string(),
                    "source_hash": source_hash,
                    "profile_count": profile_count,
                    "parse_status": parse_status,
                    "disposition": "archived_non_runnable_requires_draft_team_template_recreation",
                    "reason": "legacy Team Profiles lack immutable Agent Definition revision bindings, role contracts, and validation evidence",
                }),
            })],
        })
        .map_err(|error| error.to_string())?;
    retire_source(source_path, &archive_path)?;
    Ok(Some(LegacyTeamProfileMigrationReport {
        source_path: source_path.display().to_string(),
        archive_path: archive_path.display().to_string(),
        source_hash,
        duplicate: false,
        profile_count,
        parse_status,
        source_retired: true,
    }))
}

fn retire_source(source_path: &Path, archive_path: &Path) -> Result<(), String> {
    if let Some(parent) = archive_path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    if archive_path.exists() {
        std::fs::remove_file(source_path).map_err(|error| error.to_string())?;
    } else {
        std::fs::rename(source_path, archive_path).map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn inspect_profile_file(raw: &[u8]) -> (usize, String) {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(raw) else {
        return (0, "unparseable_json_archived".to_string());
    };
    let profiles = value.get("profiles").or_else(|| value.get("team_profiles"));
    let count = match profiles {
        Some(serde_json::Value::Array(entries)) => entries.len(),
        Some(serde_json::Value::Object(entries)) => entries.len(),
        _ => 0,
    };
    (count, "parsed_archived_requires_human_recreate".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn archives_legacy_profiles_with_a_durable_human_recreate_receipt() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let source = directory.path().join("team-profiles.json");
        let archive_root = directory.path().join("archives");
        std::fs::write(
            &source,
            r#"{"profiles":[{"id":"legacy","leader":"planner","members":["executor"]}]}"#,
        )
        .expect("legacy profile file");
        let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("event store"));

        let report = archive_legacy_team_profile_file(store.clone(), &source, &archive_root)
            .expect("archive")
            .expect("report");

        assert!(report.source_retired);
        assert_eq!(report.profile_count, 1);
        assert!(!source.exists());
        assert!(PathBuf::from(&report.archive_path).exists());
        let events = store
            .list_stream(&format!(
                "upgrade:team-profile-legacy:{}",
                report.source_hash
            ))
            .expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].status.as_deref(), Some("requires_human_recreate"));
    }
}
