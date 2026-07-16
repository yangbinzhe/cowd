use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgAlertRuleInput {
    #[serde(default)]
    pub rule_id: Option<String>,
    pub owner_ref: String,
    pub name: String,
    #[serde(default)]
    pub metric_refs: Vec<String>,
    #[serde(default)]
    pub entity_refs: Vec<String>,
    #[serde(default)]
    pub condition: Value,
    #[serde(default = "default_alert_severity")]
    pub severity: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgAlertRule {
    pub rule_id: String,
    pub owner_ref: String,
    pub name: String,
    #[serde(default)]
    pub metric_refs: Vec<String>,
    #[serde(default)]
    pub entity_refs: Vec<String>,
    #[serde(default)]
    pub condition: Value,
    pub severity: String,
    pub enabled: bool,
    pub revision: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MfgAlertRule {
    #[must_use]
    pub fn from_input(input: MfgAlertRuleInput) -> Self {
        let now = Utc::now();
        Self {
            rule_id: input
                .rule_id
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| format!("alert-rule-{}", uuid::Uuid::new_v4())),
            owner_ref: input.owner_ref,
            name: input.name,
            metric_refs: input.metric_refs,
            entity_refs: input.entity_refs,
            condition: input.condition,
            severity: input.severity,
            enabled: input.enabled,
            revision: 1,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgAlertOccurrence {
    pub occurrence_id: String,
    pub rule_id: String,
    #[serde(default)]
    pub attention_ref: Option<String>,
    #[serde(default)]
    pub incident_ref: Option<String>,
    pub status: String,
    pub severity: String,
    pub summary: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub revision: u64,
    #[serde(default)]
    pub snoozed_until: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgAlertSubscriptionInput {
    #[serde(default)]
    pub subscription_id: Option<String>,
    pub rule_id: String,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgAlertSubscription {
    pub subscription_id: String,
    pub rule_id: String,
    pub subscriber_ref: String,
    #[serde(default)]
    pub channels: Vec<String>,
    pub enabled: bool,
    pub revision: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MfgAlertSubscription {
    #[must_use]
    pub fn from_input(input: MfgAlertSubscriptionInput, subscriber_ref: String) -> Self {
        let now = Utc::now();
        Self {
            subscription_id: input
                .subscription_id
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| format!("alert-subscription-{}", uuid::Uuid::new_v4())),
            rule_id: input.rule_id,
            subscriber_ref,
            channels: input.channels,
            enabled: input.enabled,
            revision: 1,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgAlertCommand {
    Acknowledge,
    Snooze,
    Resolve,
    Escalate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgAlertCommandInput {
    pub command: MfgAlertCommand,
    pub actor_ref: String,
    pub expected_revision: u64,
    pub idempotency_key: String,
    #[serde(default)]
    pub until: Option<DateTime<Utc>>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgAssignmentInput {
    #[serde(default)]
    pub assignment_id: Option<String>,
    pub task_ref: String,
    #[serde(default)]
    pub workflow_id: Option<String>,
    #[serde(default)]
    pub workflow_node_id: Option<String>,
    #[serde(default)]
    pub incident_id: Option<String>,
    pub assignee_ref: String,
    #[serde(default = "default_assignee_kind")]
    pub assignee_kind: String,
    #[serde(default)]
    pub watcher_refs: Vec<String>,
    #[serde(default = "default_priority")]
    pub priority: String,
    #[serde(default)]
    pub due_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub sla_minutes: Option<u64>,
    #[serde(default)]
    pub notification_targets: Vec<MfgSurfaceNotificationTarget>,
    #[serde(default = "default_visibility")]
    pub visibility: String,
    #[serde(default)]
    pub expected_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgAssignment {
    pub assignment_id: String,
    pub task_ref: String,
    #[serde(default)]
    pub workflow_id: Option<String>,
    #[serde(default)]
    pub workflow_node_id: Option<String>,
    #[serde(default)]
    pub incident_id: Option<String>,
    pub assignee_ref: String,
    pub assignee_kind: String,
    #[serde(default)]
    pub watcher_refs: Vec<String>,
    pub priority: String,
    #[serde(default)]
    pub due_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub sla_minutes: Option<u64>,
    #[serde(default)]
    pub notification_targets: Vec<MfgSurfaceNotificationTarget>,
    pub status: String,
    pub visibility: String,
    pub revision: u64,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MfgAssignment {
    #[must_use]
    pub fn from_input(input: MfgAssignmentInput, actor_ref: String) -> Self {
        let now = Utc::now();
        Self {
            assignment_id: input
                .assignment_id
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| format!("assignment-{}", uuid::Uuid::new_v4())),
            task_ref: input.task_ref,
            workflow_id: input.workflow_id,
            workflow_node_id: input.workflow_node_id,
            incident_id: input.incident_id,
            assignee_ref: input.assignee_ref,
            assignee_kind: input.assignee_kind,
            watcher_refs: input.watcher_refs,
            priority: input.priority,
            due_at: input.due_at,
            sla_minutes: input.sla_minutes,
            notification_targets: input.notification_targets,
            status: "assigned".to_string(),
            visibility: input.visibility,
            revision: 1,
            created_by: actor_ref,
            created_at: now,
            updated_at: now,
        }
    }
}

/// A delivery address owned by the Surface boundary.  It deliberately carries
/// no provider token or task state; Surface owns transport and recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MfgSurfaceNotificationTarget {
    pub surface: String,
    pub recipient: String,
    #[serde(default)]
    pub thread: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MfgAssignmentCommand {
    Assign,
    Claim,
    Transfer,
    Unassign,
    Watch,
    RequestUpdate,
    Escalate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgAssignmentCommandInput {
    pub command: MfgAssignmentCommand,
    pub actor_ref: String,
    pub expected_revision: u64,
    pub idempotency_key: String,
    #[serde(default)]
    pub target_ref: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgCommandReceipt {
    pub receipt_id: String,
    pub domain: String,
    pub subject_ref: String,
    pub command: String,
    pub actor_ref: String,
    pub idempotency_key: String,
    pub idempotent_replay: bool,
    pub previous_revision: u64,
    pub current_revision: u64,
    pub audit_ref: String,
    #[serde(default)]
    pub notification_refs: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgForecastSignal {
    pub signal_ref: String,
    pub label: String,
    pub direction: String,
    pub weight: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgForecastProjection {
    pub forecast_id: String,
    pub metric_ref: String,
    #[serde(default)]
    pub entity_ref: Option<String>,
    pub status: String,
    pub horizon: String,
    pub interval: String,
    #[serde(default)]
    pub confidence: Option<f32>,
    pub method: String,
    pub generated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    #[serde(default)]
    pub leading_signals: Vec<MfgForecastSignal>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub points: Vec<Value>,
    #[serde(default)]
    pub unavailable_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgLiveProjectionEvent {
    pub cursor: u64,
    pub event_type: String,
    pub subject_ref: String,
    #[serde(default)]
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgLiveProjection {
    pub kind: String,
    pub cursor: u64,
    pub recoverable: bool,
    #[serde(default)]
    pub snapshot: Value,
    #[serde(default)]
    pub events: Vec<MfgLiveProjectionEvent>,
    #[serde(default)]
    pub resync_reason: Option<String>,
}

fn default_true() -> bool {
    true
}
fn default_alert_severity() -> String {
    "warning".to_string()
}
fn default_assignee_kind() -> String {
    "user".to_string()
}
fn default_priority() -> String {
    "normal".to_string()
}
fn default_visibility() -> String {
    "team".to_string()
}
