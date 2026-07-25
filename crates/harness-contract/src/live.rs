//! Transport-neutral contracts for one multiplexed Surface live connection.
//!
//! A subscription selects authoritative Runtime sources. Gateway owns the
//! physical connection and recovery checkpoint; Surfaces own only reducers.

use std::collections::BTreeMap;
use std::fmt::Write;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::projection::ProjectionDetailScope;

pub const LIVE_CONTRACT_SCHEMA_VERSION: u32 = 1;
pub const LIVE_ENVELOPE_CANONICAL_FIXTURE_JSON: &str =
    include_str!("../tests/fixtures/live-envelope-v1.json");

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LiveSourceKind {
    Session,
    Execution,
    Mission,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveSourceSelector {
    pub kind: LiveSourceKind,
    pub id: String,
    #[serde(default)]
    pub cursor: u64,
    #[serde(default)]
    pub detail_scope: ProjectionDetailScope,
}

impl LiveSourceSelector {
    pub fn key(&self) -> String {
        let kind = match self.kind {
            LiveSourceKind::Session => "session",
            LiveSourceKind::Execution => "execution",
            LiveSourceKind::Mission => "mission",
        };
        format!("{kind}:{}", self.id)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveSelector {
    #[serde(default)]
    pub sources: Vec<LiveSourceSelector>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateLiveSubscriptionRequest {
    pub surface_instance: String,
    pub selector: LiveSelector,
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatchLiveSubscriptionRequest {
    pub expected_revision: u64,
    pub idempotency_key: String,
    pub selector: LiveSelector,
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveSubscription {
    pub schema_version: u32,
    pub id: String,
    pub surface_instance: String,
    pub revision: u64,
    pub selector: LiveSelector,
    pub selector_hash: String,
    pub expires_at_ms: u64,
    pub stream_url: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryClass {
    Durable,
    SnapshotReconstructable,
    EphemeralPreview,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceHealth {
    Baseline,
    Live,
    ResyncRequired,
    Revoked,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LiveEnvelope {
    pub schema_version: u32,
    pub subscription_id: String,
    pub subscription_revision: u64,
    pub source_kind: String,
    pub source_id: String,
    pub detail_scope: ProjectionDetailScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_cursor: Option<u64>,
    pub delivery_class: DeliveryClass,
    pub source_health: SourceHealth,
    pub event: String,
    #[serde(default)]
    pub payload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mission_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositeCheckpoint {
    pub schema_version: u32,
    pub subscription_id: String,
    pub subscription_revision: u64,
    pub selector_hash: String,
    pub principal_hash: String,
    pub surface_instance_hash: String,
    pub issued_at_ms: u64,
    pub expires_at_ms: u64,
    pub key_revision: u64,
    #[serde(default)]
    pub source_cursors: BTreeMap<String, u64>,
}

pub fn live_envelope_json_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "schema_version", "subscription_id", "subscription_revision",
            "source_kind", "source_id", "detail_scope", "delivery_class", "source_health",
            "event", "payload"
        ],
        "properties": {
            "schema_version": {"type": "integer", "const": LIVE_CONTRACT_SCHEMA_VERSION},
            "subscription_id": {"type": "string", "minLength": 1},
            "subscription_revision": {"type": "integer", "minimum": 1},
            "source_kind": {
                "type": "string",
                "enum": ["session", "execution", "mission", "subscription"]
            },
            "source_id": {"type": "string", "minLength": 1},
            "detail_scope": {"type": "string", "enum": ["summary", "full"]},
            "source_cursor": {"type": ["integer", "null"], "minimum": 0},
            "delivery_class": {
                "type": "string",
                "enum": ["durable", "snapshot_reconstructable", "ephemeral_preview"]
            },
            "source_health": {
                "type": "string",
                "enum": ["baseline", "live", "resync_required", "revoked"]
            },
            "event": {"type": "string", "minLength": 1},
            "payload": {},
            "session_id": {"type": ["string", "null"]},
            "execution_id": {"type": ["string", "null"]},
            "mission_id": {"type": ["string", "null"]},
            "agent_id": {"type": ["string", "null"]},
            "stream_revision": {"type": ["integer", "null"], "minimum": 0},
            "start_bytes": {"type": ["integer", "null"], "minimum": 0},
            "end_bytes": {"type": ["integer", "null"], "minimum": 0}
        },
        "additionalProperties": false
    })
}

pub fn canonical_live_envelope_fixture() -> LiveEnvelope {
    serde_json::from_str(LIVE_ENVELOPE_CANONICAL_FIXTURE_JSON)
        .expect("canonical live envelope fixture must remain valid")
}

pub fn live_envelope_schema_hash() -> String {
    let mut hasher = Sha256::new();
    hasher.update(canonical_json(&live_envelope_json_schema()).as_bytes());
    format!("{:x}", hasher.finalize())
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            serde_json::to_string(value).expect("JSON scalar serialization cannot fail")
        }
        Value::Array(values) => {
            let mut output = String::from("[");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&canonical_json(value));
            }
            output.push(']');
            output
        }
        Value::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            let mut output = String::from("{");
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write!(
                    output,
                    "{}:{}",
                    serde_json::to_string(key).expect("JSON object key serialization cannot fail"),
                    canonical_json(&values[key])
                )
                .expect("writing canonical JSON into a String cannot fail");
            }
            output.push('}');
            output
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_source_keys_are_stable_and_namespaced() {
        let source = LiveSourceSelector {
            kind: LiveSourceKind::Execution,
            id: "same-id".to_string(),
            cursor: 0,
            detail_scope: ProjectionDetailScope::Full,
        };
        assert_eq!(source.key(), "execution:same-id");
    }

    #[test]
    fn mission_sources_have_an_independent_namespace() {
        let source = LiveSourceSelector {
            kind: LiveSourceKind::Mission,
            id: "workspace".to_string(),
            cursor: 4,
            detail_scope: ProjectionDetailScope::Summary,
        };
        assert_eq!(source.key(), "mission:workspace");
    }

    #[test]
    fn delivery_classes_are_the_only_wire_values() {
        assert_eq!(
            serde_json::to_string(&DeliveryClass::Durable).unwrap(),
            "\"durable\""
        );
        assert_eq!(
            serde_json::to_string(&DeliveryClass::SnapshotReconstructable).unwrap(),
            "\"snapshot_reconstructable\""
        );
        assert_eq!(
            serde_json::to_string(&DeliveryClass::EphemeralPreview).unwrap(),
            "\"ephemeral_preview\""
        );
    }

    #[test]
    fn canonical_fixture_covers_every_optional_wire_field() {
        let envelope = canonical_live_envelope_fixture();
        assert_eq!(envelope.schema_version, LIVE_CONTRACT_SCHEMA_VERSION);
        assert_eq!(envelope.subscription_revision, 7);
        assert_eq!(envelope.source_cursor, Some(42));
        assert_eq!(envelope.session_id.as_deref(), Some("session-contract"));
        assert_eq!(envelope.execution_id.as_deref(), Some("execution-contract"));
        assert_eq!(envelope.mission_id.as_deref(), Some("mission-contract"));
        assert_eq!(envelope.agent_id.as_deref(), Some("agent-contract"));
        assert_eq!(envelope.stream_revision, Some(3));
        assert_eq!(envelope.start_bytes, Some(128));
        assert_eq!(envelope.end_bytes, Some(256));
    }

    #[test]
    fn live_envelope_schema_hash_is_stable() {
        assert_eq!(
            live_envelope_schema_hash(),
            "53ccc1bb8fb6896f1e648035dad6985aba8754b2e5d88e47b7687ddc492a346c"
        );
    }
}
