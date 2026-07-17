use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgAlertCommand {
    Acknowledge,
    Snooze,
    Resolve,
    Escalate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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
    #[serde(default)]
    pub completion_ref: Option<String>,
    #[serde(default)]
    pub lifecycle_correlation_id: Option<String>,
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
            completion_ref: None,
            lifecycle_correlation_id: None,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgSurfaceNotificationTarget {
    pub surface: String,
    pub recipient: String,
    #[serde(default)]
    pub thread: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MfgAssignmentCommand {
    Assign,
    Claim,
    Transfer,
    Unassign,
    Watch,
    RequestUpdate,
    Escalate,
    Start,
    Complete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgAssignmentCommandInput {
    pub command: MfgAssignmentCommand,
    pub actor_ref: String,
    pub expected_revision: u64,
    pub idempotency_key: String,
    #[serde(default)]
    pub target_ref: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    pub correlation_id: String,
    #[serde(default)]
    pub completion_evidence: Option<app_mfg_contract::MfgAssignmentCompletionEvidenceV1>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgCommandReceipt {
    pub receipt_id: String,
    pub domain: String,
    pub subject_ref: String,
    pub command: String,
    #[serde(default)]
    pub action_id: String,
    pub actor_ref: String,
    pub idempotency_key: String,
    #[serde(default)]
    pub payload_digest: String,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default = "default_mfg_contract_version")]
    pub contract_version: String,
    pub idempotent_replay: bool,
    pub previous_revision: u64,
    pub current_revision: u64,
    pub audit_ref: String,
    #[serde(default)]
    pub notification_refs: Vec<String>,
    #[serde(default)]
    pub response_snapshot: Value,
    pub created_at: DateTime<Utc>,
}

impl MfgCommandReceipt {
    pub fn canonical_receipt(
        &self,
    ) -> Result<app_mfg_contract::MfgReceiptV1, app_mfg_contract::MfgApiErrorV1> {
        let action_id = app_mfg_contract::MfgActionId::parse(&self.action_id).ok_or_else(|| {
            app_mfg_contract::MfgApiErrorV1 {
                code: app_mfg_contract::MfgErrorCode::ContractMismatch,
                message: format!(
                    "legacy MFG receipt action is absent from the canonical contract: {}",
                    self.action_id
                ),
                http_status: 500,
                details: serde_json::json!({"receipt_id": self.receipt_id}),
                retryable: false,
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                recovery_actions: Vec::new(),
                request_id: None,
                receipt_ref: Some(self.receipt_id.clone()),
            }
        })?;
        let is_create = app_mfg_contract::mfg_action_contracts()
            .into_iter()
            .find(|contract| contract.action_id == action_id)
            .is_some_and(|contract| {
                matches!(contract.class, app_mfg_contract::MfgMutationClass::Create)
            });
        Ok(app_mfg_contract::MfgReceiptV1 {
            receipt_id: self.receipt_id.clone(),
            idempotency_key: self.idempotency_key.clone(),
            actor_principal: self.actor_ref.clone(),
            action_id,
            resource_ref: self.subject_ref.clone(),
            expected_revision: (!is_create).then_some(self.previous_revision),
            result_revision: Some(self.current_revision),
            payload_digest: self.payload_digest.clone(),
            correlation_id: self.correlation_id.clone(),
            status: if self.idempotent_replay {
                app_mfg_contract::MfgReceiptStatus::Replayed
            } else {
                app_mfg_contract::MfgReceiptStatus::Completed
            },
            response: serde_json::to_value(self).unwrap_or(serde_json::Value::Null),
            contract_version: app_mfg_contract::MfgContractVersion(self.contract_version.clone()),
            created_at: self.created_at,
            updated_at: self.created_at,
        })
    }
}

fn default_mfg_contract_version() -> String {
    app_mfg_contract::MFG_CONTRACT_VERSION.to_string()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgForecastSignal {
    pub signal_ref: String,
    pub label: String,
    pub direction: String,
    pub weight: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgLiveProjectionEvent {
    pub cursor: u64,
    pub event_type: String,
    pub subject_ref: String,
    #[serde(default)]
    pub payload: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgLiveEpoch {
    pub epoch_id: String,
    pub contract_version: String,
    pub schema_version: u32,
    pub created_at: DateTime<Utc>,
    pub rotation_reason: String,
    pub retention_low_cursor: u64,
    pub retention_high_cursor: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgLiveSnapshotRead {
    pub epoch: MfgLiveEpoch,
    pub high_cursor: u64,
    pub state: app_mfg_contract::MfgLiveSnapshotStateV1,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct MfgLiveDeltaRead {
    pub epoch: MfgLiveEpoch,
    pub base_cursor: u64,
    pub high_cursor: u64,
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
