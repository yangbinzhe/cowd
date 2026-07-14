//! Declarative contracts for long-lived, Runtime-managed Agent work.
//!
//! A managed Agent is not a process.  It is a versioned definition that asks
//! Runtime to create a fresh, immutable Binding/Run for each accepted
//! trigger.  The contracts deliberately exclude executor handles, worker
//! state, graph nodes, and effect receipts.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::agent::{AgentCapability, AgentDefinitionId, RevisionSelector, ValidationError};
use crate::mission::ScheduleTrigger;
use crate::team::{TeamTemplateDefinitionId, TeamTemplateSelector};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ManagedAgentTarget {
    Agent {
        definition_id: AgentDefinitionId,
        selector: RevisionSelector,
    },
    Team {
        template_id: TeamTemplateDefinitionId,
        selector: TeamTemplateSelector,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedAgentEventTrigger {
    pub source_id: String,
    pub source_kind: String,
    pub event_type: String,
    /// Source capabilities are transport facts supplied by the trusted Edge
    /// adapter. Runtime compares them here instead of letting each adapter
    /// decide which automation is allowed to start.
    #[serde(default)]
    pub required_source_capabilities: Vec<String>,
    /// Exact, structured attribute predicates.  Connector/Gateway only
    /// normalizes an event; Runtime evaluates these predicates consistently.
    #[serde(default)]
    pub required_attributes: BTreeMap<String, String>,
    /// Events older than this are rejected instead of silently triggering
    /// stale automation. `None` allows durable replay under idempotency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maximum_age_ms: Option<u64>,
    #[serde(default)]
    pub out_of_order_policy: ManagedAgentEventOrderPolicy,
}

/// How a versioned Managed Agent definition treats source sequences. Source
/// adapters may replay events; the default accepts them because invocation
/// idempotency is already durable. Stateful feeds can opt into rejecting an
/// older sequence for the same source/subject partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ManagedAgentEventOrderPolicy {
    #[default]
    AcceptAny,
    RejectOlderSequence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ManagedAgentTrigger {
    Manual,
    Schedule { trigger: ScheduleTrigger },
    Event(ManagedAgentEventTrigger),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum ManagedAgentOverlapPolicy {
    Forbid,
    AllowParallel { max_concurrent: u16 },
    Replace,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedAgentRetryPolicy {
    pub max_attempts: u16,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for ManagedAgentRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff_ms: 1_000,
            max_backoff_ms: 60_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedAgentHealthPolicy {
    pub max_consecutive_failures: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_run_age_ms: Option<u64>,
}

impl Default for ManagedAgentHealthPolicy {
    fn default() -> Self {
        Self {
            max_consecutive_failures: 3,
            max_run_age_ms: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedAgentDefinition {
    pub managed_agent_id: String,
    pub revision: u64,
    pub target: ManagedAgentTarget,
    pub trigger: ManagedAgentTrigger,
    pub session_id: String,
    pub objective: String,
    #[serde(default)]
    pub acceptance: Vec<String>,
    pub permission_lease: String,
    pub model_lease: String,
    /// Runtime intersects these direct-Agent grants with the selected
    /// Definition's capability ceiling. An empty list never expands to the
    /// Definition's full ceiling.
    #[serde(default)]
    pub granted_capabilities: Vec<AgentCapability>,
    /// Explicit direct-Agent tool and Skill ceilings. Team targets receive
    /// their per-role ceilings only from the selected Template.
    #[serde(default)]
    pub allowed_tool_contract_refs: Vec<String>,
    #[serde(default)]
    pub allowed_skill_refs: Vec<String>,
    #[serde(default)]
    pub resource_scopes: Vec<String>,
    pub overlap_policy: ManagedAgentOverlapPolicy,
    #[serde(default)]
    pub retry_policy: ManagedAgentRetryPolicy,
    #[serde(default)]
    pub health_policy: ManagedAgentHealthPolicy,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedAgentTriggerEvent {
    pub event_id: String,
    pub source_id: String,
    pub source_kind: String,
    pub event_type: String,
    pub subject: String,
    pub payload_ref: String,
    pub payload_digest: String,
    pub occurred_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_sequence: Option<u64>,
    pub idempotency_key: String,
    /// Declared by a trusted source adapter after it has authenticated the
    /// external origin. Callers cannot turn a missing capability into a
    /// Runtime match by placing a free-form attribute in the payload.
    #[serde(default)]
    pub source_capabilities: Vec<String>,
    #[serde(default)]
    pub attributes: BTreeMap<String, String>,
    #[serde(default)]
    pub trace_refs: Vec<String>,
}

/// Immutable Runtime-issued fence carried by a task created for a Managed
/// Agent invocation.  It contains no executor or adapter details; it merely
/// lets the Runtime tool boundary prove that an external effect belongs to
/// the still-current invocation attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedAgentInvocationFence {
    pub managed_agent_id: String,
    pub definition_revision: u64,
    pub invocation_id: String,
    pub attempt_no: u16,
    pub fence_generation: u64,
    pub dispatcher_id: String,
}

impl ManagedAgentInvocationFence {
    pub fn validate(&self) -> Result<(), ValidationError> {
        for (field, value) in [
            (
                "managed_invocation.managed_agent_id",
                self.managed_agent_id.as_str(),
            ),
            (
                "managed_invocation.invocation_id",
                self.invocation_id.as_str(),
            ),
            (
                "managed_invocation.dispatcher_id",
                self.dispatcher_id.as_str(),
            ),
        ] {
            if value.trim().is_empty() {
                return Err(ValidationError::MissingField {
                    field: field.to_string(),
                });
            }
        }
        if self.definition_revision == 0 || self.attempt_no == 0 || self.fence_generation == 0 {
            return Err(ValidationError::InvalidContract {
                message: "managed invocation fence revisions and attempt must be positive"
                    .to_string(),
            });
        }
        Ok(())
    }
}

fn enabled_by_default() -> bool {
    true
}

impl ManagedAgentDefinition {
    pub fn validate(&self) -> Result<(), ValidationError> {
        for (field, value) in [
            ("managed_agent_id", self.managed_agent_id.as_str()),
            ("session_id", self.session_id.as_str()),
            ("objective", self.objective.as_str()),
            ("permission_lease", self.permission_lease.as_str()),
            ("model_lease", self.model_lease.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(ValidationError::MissingField {
                    field: field.to_string(),
                });
            }
        }
        if self.revision == 0 {
            return Err(ValidationError::InvalidContract {
                message: "managed Agent revision must be positive".to_string(),
            });
        }
        let mut capabilities = std::collections::BTreeSet::new();
        for capability in &self.granted_capabilities {
            if !capabilities.insert(*capability) {
                return Err(ValidationError::DuplicateValue {
                    field: "managed_agent.granted_capabilities".to_string(),
                    value: capability.as_str().to_string(),
                });
            }
        }
        for (field, values) in [
            (
                "managed_agent.allowed_tool_contract_refs",
                &self.allowed_tool_contract_refs,
            ),
            ("managed_agent.allowed_skill_refs", &self.allowed_skill_refs),
        ] {
            let mut seen = std::collections::BTreeSet::new();
            for value in values {
                if value.trim().is_empty() {
                    return Err(ValidationError::MissingField {
                        field: field.to_string(),
                    });
                }
                if !seen.insert(value.as_str()) {
                    return Err(ValidationError::DuplicateValue {
                        field: field.to_string(),
                        value: value.clone(),
                    });
                }
            }
        }
        match self.target {
            ManagedAgentTarget::Agent { .. } if self.granted_capabilities.is_empty() => {
                return Err(ValidationError::MissingField {
                    field: "managed_agent.granted_capabilities".to_string(),
                });
            }
            ManagedAgentTarget::Team { .. }
                if !self.granted_capabilities.is_empty()
                    || !self.allowed_tool_contract_refs.is_empty()
                    || !self.allowed_skill_refs.is_empty() =>
            {
                return Err(ValidationError::InvalidContract {
                    message: "managed Team targets must use Template role grants rather than direct Agent grants"
                        .to_string(),
                });
            }
            _ => {}
        }
        match &self.trigger {
            ManagedAgentTrigger::Manual => {}
            ManagedAgentTrigger::Schedule { trigger } => match trigger {
                ScheduleTrigger::At { at_ms } if *at_ms == 0 => {
                    return Err(ValidationError::InvalidContract {
                        message: "managed Agent schedule time must be positive".to_string(),
                    });
                }
                ScheduleTrigger::Interval { every_ms } if *every_ms == 0 => {
                    return Err(ValidationError::InvalidContract {
                        message: "managed Agent interval must be positive".to_string(),
                    });
                }
                ScheduleTrigger::Cron {
                    expression,
                    timezone,
                } if expression.trim().is_empty() || timezone.trim().is_empty() => {
                    return Err(ValidationError::InvalidContract {
                        message: "managed Agent cron requires expression and timezone".to_string(),
                    });
                }
                _ => {}
            },
            ManagedAgentTrigger::Event(event) => {
                for (field, value) in [
                    ("managed_agent.event.source_id", event.source_id.as_str()),
                    (
                        "managed_agent.event.source_kind",
                        event.source_kind.as_str(),
                    ),
                    ("managed_agent.event.event_type", event.event_type.as_str()),
                ] {
                    if value.trim().is_empty() {
                        return Err(ValidationError::MissingField {
                            field: field.to_string(),
                        });
                    }
                }
                let mut source_capabilities = std::collections::BTreeSet::new();
                for capability in &event.required_source_capabilities {
                    if capability.trim().is_empty() {
                        return Err(ValidationError::MissingField {
                            field: "managed_agent.event.required_source_capabilities".to_string(),
                        });
                    }
                    if !source_capabilities.insert(capability.as_str()) {
                        return Err(ValidationError::DuplicateValue {
                            field: "managed_agent.event.required_source_capabilities".to_string(),
                            value: capability.clone(),
                        });
                    }
                }
            }
        }
        match self.overlap_policy {
            ManagedAgentOverlapPolicy::AllowParallel { max_concurrent } if max_concurrent == 0 => {
                return Err(ValidationError::InvalidContract {
                    message: "managed Agent parallel overlap requires positive max_concurrent"
                        .to_string(),
                });
            }
            _ => {}
        }
        if self.retry_policy.max_attempts == 0
            || self.retry_policy.initial_backoff_ms == 0
            || self.retry_policy.max_backoff_ms < self.retry_policy.initial_backoff_ms
        {
            return Err(ValidationError::InvalidContract {
                message: "managed Agent retry policy is invalid".to_string(),
            });
        }
        if self.health_policy.max_consecutive_failures == 0 {
            return Err(ValidationError::InvalidContract {
                message: "managed Agent health policy requires a positive failure threshold"
                    .to_string(),
            });
        }
        Ok(())
    }
}

impl ManagedAgentTriggerEvent {
    pub fn validate(&self) -> Result<(), ValidationError> {
        for (field, value) in [
            ("managed_agent.event.event_id", self.event_id.as_str()),
            ("managed_agent.event.source_id", self.source_id.as_str()),
            ("managed_agent.event.source_kind", self.source_kind.as_str()),
            ("managed_agent.event.event_type", self.event_type.as_str()),
            ("managed_agent.event.subject", self.subject.as_str()),
            ("managed_agent.event.payload_ref", self.payload_ref.as_str()),
            (
                "managed_agent.event.payload_digest",
                self.payload_digest.as_str(),
            ),
            (
                "managed_agent.event.idempotency_key",
                self.idempotency_key.as_str(),
            ),
        ] {
            if value.trim().is_empty() {
                return Err(ValidationError::MissingField {
                    field: field.to_string(),
                });
            }
        }
        let mut source_capabilities = std::collections::BTreeSet::new();
        for capability in &self.source_capabilities {
            if capability.trim().is_empty() {
                return Err(ValidationError::MissingField {
                    field: "managed_agent.event.source_capabilities".to_string(),
                });
            }
            if !source_capabilities.insert(capability.as_str()) {
                return Err(ValidationError::DuplicateValue {
                    field: "managed_agent.event.source_capabilities".to_string(),
                    value: capability.clone(),
                });
            }
        }
        Ok(())
    }
}
