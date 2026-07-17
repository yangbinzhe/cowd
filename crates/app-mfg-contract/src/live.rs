use chrono::{DateTime, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::version::MfgContractVersion;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema, Default)]
pub struct MfgLiveSnapshotStateV1 {
    #[serde(default)]
    pub cockpit: serde_json::Value,
    #[serde(default)]
    pub alerts: serde_json::Value,
    #[serde(default)]
    pub assignments: serde_json::Value,
    #[serde(default)]
    pub incidents: serde_json::Value,
    #[serde(default)]
    pub executions: serde_json::Value,
    #[serde(default)]
    pub reports: serde_json::Value,
    #[serde(default)]
    pub reviews: serde_json::Value,
    #[serde(default)]
    pub receipts: serde_json::Value,
    #[serde(default)]
    pub data_compute: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgLiveEventV1 {
    pub event_type: String,
    pub subject_ref: String,
    pub revision: u64,
    pub occurred_at: DateTime<Utc>,
    #[serde(default)]
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgLiveSnapshotV1 {
    pub view_epoch: String,
    pub cursor: String,
    pub generated_at: DateTime<Utc>,
    pub contract_version: MfgContractVersion,
    pub state: MfgLiveSnapshotStateV1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct MfgLiveDeltaV1 {
    pub view_epoch: String,
    pub base_cursor: String,
    pub target_cursor: String,
    #[serde(default)]
    pub events: Vec<MfgLiveEventV1>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgLiveResyncV1 {
    pub previous_view_epoch: String,
    pub reason: String,
    pub snapshot_url: String,
    pub latest_cursor: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MfgLiveHeartbeatV1 {
    pub view_epoch: String,
    pub cursor: String,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MfgLiveEnvelopeV1 {
    Snapshot(MfgLiveSnapshotV1),
    Delta(MfgLiveDeltaV1),
    Resync(MfgLiveResyncV1),
    Heartbeat(MfgLiveHeartbeatV1),
}

/// Canonical observer-queue priority shared by Gateway and TUI.
///
/// Lower values have stronger retention guarantees:
/// P0 terminal/review/auth, P1 receipt/conflict/recovery, P2 domain state,
/// and P3 metric/heartbeat.
#[must_use]
pub fn mfg_live_event_priority(event_type: &str, payload: &serde_json::Value) -> u8 {
    if event_type.starts_with("report_review.")
        || event_type.contains("resync")
        || event_type.contains("auth")
    {
        return 0;
    }
    if event_type.contains("receipt")
        || event_type.contains("conflict")
        || event_type.contains("recovery")
    {
        return 1;
    }
    let terminal_family = [
        "assignment.",
        "alert.",
        "incident.",
        "workflow.",
        "execution.",
        "skill_run.",
        "report.",
        "compute_job.",
    ]
    .iter()
    .any(|prefix| event_type.starts_with(prefix));
    if terminal_family
        && (event_type.ends_with(".complete")
            || event_type.ends_with(".completed")
            || event_type.ends_with(".resolve")
            || event_type.ends_with(".resolved")
            || event_type.ends_with(".unassign")
            || event_type.ends_with(".cancel")
            || event_type.ends_with(".failed")
            || event_type.ends_with(".closed")
            || payload_has_terminal_status(payload))
    {
        return 0;
    }
    if event_type.starts_with("metric_")
        || event_type.starts_with("metric.")
        || event_type.contains("heartbeat")
    {
        3
    } else {
        2
    }
}

fn payload_has_terminal_status(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(object) => {
            let terminal = ["status", "state", "lifecycle"]
                .into_iter()
                .filter_map(|field| object.get(field))
                .filter_map(serde_json::Value::as_str)
                .any(|status| {
                    matches!(
                        status,
                        "complete"
                            | "completed"
                            | "resolved"
                            | "closed"
                            | "failed"
                            | "rejected"
                            | "cancelled"
                            | "canceled"
                            | "abandoned"
                            | "unassigned"
                            | "dead_lettered"
                            | "blocked"
                            | "terminal"
                            | "succeeded"
                    )
                });
            terminal || object.values().any(payload_has_terminal_status)
        }
        serde_json::Value::Array(values) => values.iter().any(payload_has_terminal_status),
        _ => false,
    }
}

#[cfg(test)]
mod priority_tests {
    use super::mfg_live_event_priority;

    #[test]
    fn terminal_business_state_is_p0_without_relying_on_event_name_substrings() {
        assert_eq!(
            mfg_live_event_priority(
                "assignment.unassign",
                &serde_json::json!({"assignment": {"status": "unassigned"}}),
            ),
            0
        );
        assert_eq!(
            mfg_live_event_priority(
                "execution.updated",
                &serde_json::json!({"execution": {"status": "completed"}}),
            ),
            0
        );
        assert_eq!(
            mfg_live_event_priority(
                "receipt.completed",
                &serde_json::json!({"status": "completed"})
            ),
            1
        );
        assert_eq!(
            mfg_live_event_priority("metric_state.updated", &serde_json::json!({})),
            3
        );
    }
}
