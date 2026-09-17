//! Stable Agent-first collaboration actions.
//!
//! Model-facing inputs intentionally contain only semantic intent and durable
//! references. Runtime identity, authorization, revisions, leases and physical
//! execution topology are bound by the trusted host in [`AgentActionEnvelope`].

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const STATE_INSPECT_TOOL_ID: &str = "state_inspect";
pub const TEAM_CREATE_TOOL_ID: &str = "team_create";
pub const AGENT_INVITE_TOOL_ID: &str = "agent_invite";
pub const TASK_PUBLISH_TOOL_ID: &str = "task_publish";
pub const TASK_CLAIM_TOOL_ID: &str = "task_claim";
pub const TASK_RELEASE_TOOL_ID: &str = "task_release";
pub const TASK_SUPERSEDE_TOOL_ID: &str = "task_supersede";
pub const TASK_SUBMIT_TOOL_ID: &str = "task_submit";
pub const TASK_REVIEW_TOOL_ID: &str = "task_review";
pub const MESSAGE_PUBLISH_TOOL_ID: &str = "message_publish";
pub const ARTIFACT_COMMIT_TOOL_ID: &str = "artifact_commit";
pub const OBJECTIVE_COMPLETE_REQUEST_TOOL_ID: &str = "objective_complete_request";
pub const OBJECTIVE_UPDATE_TOOL_ID: &str = "objective_update";
pub const OBJECTIVE_REVIEW_TOOL_ID: &str = "objective_review";
pub const TASK_WITHDRAW_TOOL_ID: &str = "task_withdraw";
pub const MEMBERSHIP_UPDATE_TOOL_ID: &str = "membership_update";
pub const TEAM_UPDATE_TOOL_ID: &str = "team_update";

pub const AGENT_ACTION_TOOL_IDS: &[&str] = &[
    STATE_INSPECT_TOOL_ID,
    TEAM_CREATE_TOOL_ID,
    AGENT_INVITE_TOOL_ID,
    TASK_PUBLISH_TOOL_ID,
    TASK_CLAIM_TOOL_ID,
    TASK_RELEASE_TOOL_ID,
    TASK_SUPERSEDE_TOOL_ID,
    TASK_SUBMIT_TOOL_ID,
    TASK_REVIEW_TOOL_ID,
    MESSAGE_PUBLISH_TOOL_ID,
    ARTIFACT_COMMIT_TOOL_ID,
    OBJECTIVE_COMPLETE_REQUEST_TOOL_ID,
    OBJECTIVE_UPDATE_TOOL_ID,
    OBJECTIVE_REVIEW_TOOL_ID,
    TASK_WITHDRAW_TOOL_ID,
    MEMBERSHIP_UPDATE_TOOL_ID,
    TEAM_UPDATE_TOOL_ID,
];

/// Stable Objective identity for one conversational turn. The trusted host
/// and Gateway derive this independently from Runtime-owned lineage so model
/// JSON never carries it.
#[must_use]
pub fn root_objective_id(session_id: &str, turn_id: &str) -> String {
    let digest = Sha256::digest(format!("{session_id}|{turn_id}").as_bytes());
    format!("objective:{digest:x}")
}

#[must_use]
pub fn program_id_for_objective(objective_id: &str) -> String {
    let digest = Sha256::digest(objective_id.as_bytes());
    format!("program:{digest:x}")
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StateInspectInput {
    /// Focused metadata search over the current Program directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// Root only: yield to active workers after inspecting state. Leave false
    /// to keep planning, publishing dependent tasks or doing useful work.
    #[serde(default)]
    pub wait_for_workers: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TeamCreateInput {
    pub name: String,
    pub mission: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub objective: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentInviteInput {
    pub team_ref: String,
    pub role: String,
    pub mission: String,
    /// Optional semantic capability hints. Prefer Runtime effect names
    /// `read`, `search`, `write`, `test`, and `network` when the effect is
    /// known. Domain labels such as `formal_methods` or `literature_review`
    /// remain useful matching hints; Runtime, not the model, translates them
    /// into a concrete Agent definition and least-privilege tool grant.
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    /// Existing configured identity to attach to this Program. Runtime still
    /// creates a distinct membership/run scope, never a second hidden Agent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub existing_agent_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub definition_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_profile_ref: Option<String>,
    #[serde(default)]
    pub expertise_hints: Vec<String>,
    #[serde(default)]
    pub execution_requirements: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskPurpose {
    Delivery,
    Exploration,
}

impl Default for TaskPurpose {
    fn default() -> Self {
        Self::Delivery
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskPublishInput {
    pub team_ref: String,
    pub title: String,
    pub objective: String,
    pub acceptance: String,
    /// Optional machine-checkable acceptance hints (advisory). Prose
    /// `acceptance` stays authoritative; checks guide worker self-check and
    /// reviewer evidence citation.
    #[serde(default)]
    pub acceptance_checks: Vec<String>,
    /// Optional semantic capability hints. The five portable execution
    /// effects are `read`, `search`, `write`, `test`, and `network`. Unknown
    /// domain labels are never treated as trusted permissions and never make
    /// an otherwise runnable Task permanently undispatchable.
    #[serde(default)]
    pub required_capabilities: Vec<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub obligation_refs: Vec<String>,
    #[serde(default)]
    pub purpose: TaskPurpose,
    #[serde(default)]
    pub execution_requirements: Vec<String>,
    #[serde(default)]
    pub expertise_hints: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskClaimInput {
    pub task_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskReleaseInput {
    pub task_ref: String,
    pub reason: String,
}

/// Retire failed or invalidated work in favor of one or more concrete successor
/// Tasks. A single successor is a replacement; multiple successors are an
/// explicit split. Runtime keeps the original Task and its failure history as
/// durable audit facts rather than pretending it completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskSupersedeInput {
    pub task_ref: String,
    pub replacement_task_refs: Vec<String>,
    pub reason: String,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskWithdrawInput {
    pub task_ref: String,
    /// Durable explanation of the withdrawal.  A free-form string here used
    /// to let a model claim a plan change without a readable record.
    pub reason_ref: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

/// Runtime-only result of a physical Agent attempt. It uses the same durable
/// Program journal but is intentionally not registered as a model tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentAttemptMode {
    Execute,
    Review,
    Coordination,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskAttemptFailInput {
    pub task_ref: String,
    pub execution_id: String,
    pub mode: AgentAttemptMode,
    pub reason: String,
    pub retryable: bool,
}

/// Runtime-only admission record for one physical Agent graph. This closes
/// the interval between executor admission and the Agent's first semantic
/// action, so cancellation and recovery can always find the exact effect.
/// It is not exposed as a model tool.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskAttemptDispatchInput {
    pub task_ref: String,
    pub execution_id: String,
    pub agent_ref: String,
    pub membership_id: String,
    pub mode: AgentAttemptMode,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskSubmitInput {
    pub task_ref: String,
    /// Collaboration artifact references returned as `changed_refs` by
    /// `artifact_commit`. Runtime binds their durable content automatically;
    /// do not duplicate storage selectors in `evidence_refs`.
    #[serde(default)]
    pub artifact_refs: Vec<String>,
    /// Supporting durable observations such as source, command, test, or file
    /// tool receipts. The committed artifact content is attached by Runtime.
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    /// Known limitations, risks, or follow-up opportunities disclosed to the
    /// independent reviewer. A reviewer may accept them as non-blocking; its
    /// accept verdict is the sole Task completion authority.
    #[serde(default)]
    pub unresolved: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskReviewDecision {
    Accept,
    Challenge,
    Rework,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskReviewInput {
    pub task_ref: String,
    pub decision: TaskReviewDecision,
    pub reason: String,
    /// Durable observations used for the independent verdict, normally the
    /// receipt returned after retrieving/inspecting the submitted artifact.
    /// Runtime already knows which artifacts belong to the Task.
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

/// Model-authored treatment of a source-bound issue. This classifies evidence;
/// it does not turn Task acceptance into a factual truth assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum IssueDispositionKind {
    MustResolve,
    Disclose,
    Resolved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct IssueDisposition {
    pub issue_ref: String,
    pub disposition: IssueDispositionKind,
    pub reason_ref: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MessagePublishInput {
    pub topic_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_ref: Option<String>,
    #[serde(default)]
    pub refs: Vec<String>,
    #[serde(default)]
    pub recipients: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intent: Option<TaskIntent>,
    /// Root adjudication of source-bound issues returned by state_inspect.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issue_dispositions: Vec<IssueDisposition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TaskIntentKind {
    Offer,
    Decline,
    RequestHelp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TaskIntent {
    pub task_ref: String,
    pub kind: TaskIntentKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_ref: Option<String>,
    #[serde(default)]
    pub requested_capability_refs: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveUpdateOperation {
    Add,
    Replace,
    Retire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveUpdateInput {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub criterion_ref: Option<String>,
    pub operation: ObjectiveUpdateOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_ref: Option<String>,
    #[serde(default)]
    pub source_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_ref: Option<String>,
    #[serde(default)]
    pub evidence_requirements: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ObjectiveReviewDecision {
    Satisfied,
    Gap,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveReviewInput {
    /// Exact Goal criterion or obligation reference returned by state_inspect.
    pub criterion_ref: String,
    pub decision: ObjectiveReviewDecision,
    #[serde(default)]
    pub result_refs: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub reason_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MembershipOperation {
    Join,
    Leave,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MembershipUpdateInput {
    pub agent_ref: String,
    pub team_ref: String,
    pub operation: MembershipOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TeamUpdateInput {
    pub team_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mission_ref: Option<String>,
    #[serde(default)]
    pub request_retire: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_ref: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactCommitInput {
    /// An immutable artifact:// selector from artifact_publish, or
    /// current_message_block:<zero-based-index> selecting an explicit Text
    /// block in this model response. The Host persists only that block.
    pub content_ref: String,
    pub kind: String,
    pub title: String,
    /// Additional semantic relations. For an executing Agent, Runtime always
    /// appends the actor's currently claimed Task from its attested execution
    /// binding; the model does not need to repeat that Task ID here.
    #[serde(default)]
    pub relates_to: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObjectiveCompleteRequestInput {
    /// Result references are typed by their durable selector prefix. Runtime
    /// may derive a display artifact, but a report artifact is not mandatory
    /// for effects, structured data, or a user decision.
    #[serde(default)]
    pub result_refs: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    /// Objective-level delivery blockers only. Accepted Task disclosures stay
    /// visible in the Program projection but are not duplicated here.
    #[serde(default)]
    pub unresolved: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentActorKind {
    Root,
    TeamLead,
    Agent,
    Supervisor,
}

/// Trusted identity injected by Runtime. It is never accepted from model JSON.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentActorBinding {
    pub objective_id: String,
    pub program_id: String,
    pub session_id: String,
    pub turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_execution_id: Option<String>,
    /// Runtime-frozen collaboration cardinality for this Objective. It is
    /// inherited by delegated actors and never accepted in model tool JSON.
    #[serde(default)]
    pub required_team_count: u8,
    /// Trusted immutable preview of the user Objective. It is persisted when
    /// the Program opens so recovery and completion never validate activity
    /// counts without the business goal that gave them meaning.
    #[serde(default)]
    pub objective_summary: String,
    #[serde(default)]
    pub model_lease: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission_ceiling: Option<crate::policy::PermissionMode>,
    #[serde(default)]
    pub resource_scopes: Vec<String>,
    pub actor_id: String,
    pub kind: AgentActorKind,
    /// Trusted physical execution fence for delegated actors. Runtime binds
    /// this from the parent graph; model-authored JSON never supplies it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", content = "input", rename_all = "snake_case")]
pub enum AgentAction {
    StateInspect(StateInspectInput),
    TeamCreate(TeamCreateInput),
    AgentInvite(AgentInviteInput),
    TaskPublish(TaskPublishInput),
    TaskClaim(TaskClaimInput),
    TaskRelease(TaskReleaseInput),
    TaskSupersede(TaskSupersedeInput),
    TaskWithdraw(TaskWithdrawInput),
    TaskAttemptDispatch(TaskAttemptDispatchInput),
    TaskAttemptFail(TaskAttemptFailInput),
    TaskSubmit(TaskSubmitInput),
    TaskReview(TaskReviewInput),
    MessagePublish(MessagePublishInput),
    ArtifactCommit(ArtifactCommitInput),
    ObjectiveUpdate(ObjectiveUpdateInput),
    ObjectiveReview(ObjectiveReviewInput),
    MembershipUpdate(MembershipUpdateInput),
    TeamUpdate(TeamUpdateInput),
    ObjectiveCompleteRequest(ObjectiveCompleteRequestInput),
}

impl AgentAction {
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::StateInspect(_) => STATE_INSPECT_TOOL_ID,
            Self::TeamCreate(_) => TEAM_CREATE_TOOL_ID,
            Self::AgentInvite(_) => AGENT_INVITE_TOOL_ID,
            Self::TaskPublish(_) => TASK_PUBLISH_TOOL_ID,
            Self::TaskClaim(_) => TASK_CLAIM_TOOL_ID,
            Self::TaskRelease(_) => TASK_RELEASE_TOOL_ID,
            Self::TaskSupersede(_) => TASK_SUPERSEDE_TOOL_ID,
            Self::TaskWithdraw(_) => TASK_WITHDRAW_TOOL_ID,
            Self::TaskAttemptDispatch(_) => "task_attempt_dispatch_internal",
            Self::TaskAttemptFail(_) => "task_attempt_fail_internal",
            Self::TaskSubmit(_) => TASK_SUBMIT_TOOL_ID,
            Self::TaskReview(_) => TASK_REVIEW_TOOL_ID,
            Self::MessagePublish(_) => MESSAGE_PUBLISH_TOOL_ID,
            Self::ArtifactCommit(_) => ARTIFACT_COMMIT_TOOL_ID,
            Self::ObjectiveUpdate(_) => OBJECTIVE_UPDATE_TOOL_ID,
            Self::ObjectiveReview(_) => OBJECTIVE_REVIEW_TOOL_ID,
            Self::MembershipUpdate(_) => MEMBERSHIP_UPDATE_TOOL_ID,
            Self::TeamUpdate(_) => TEAM_UPDATE_TOOL_ID,
            Self::ObjectiveCompleteRequest(_) => OBJECTIVE_COMPLETE_REQUEST_TOOL_ID,
        }
    }

    pub fn validate(&self) -> Result<(), AgentActionValidationError> {
        match self {
            Self::StateInspect(input) => {
                optional_nonempty("scope_ref", input.scope_ref.as_deref())?;
                optional_nonempty("entry_ref", input.entry_ref.as_deref())?;
                optional_nonempty("page_cursor", input.page_cursor.as_deref())?;
                optional_nonempty("query", input.query.as_deref())?;
                if (input.page_cursor.is_some() || input.query.is_some())
                    && (input.scope_ref.is_some() || input.entry_ref.is_some())
                {
                    return Err(AgentActionValidationError::Invalid(
                        "page_cursor_with_exact_ref",
                    ));
                }
            }
            Self::TeamCreate(input) => {
                required("name", &input.name)?;
                required("mission", &input.mission)?;
                optional_nonempty("objective", input.objective.as_deref())?;
            }
            Self::AgentInvite(input) => {
                required("team_ref", &input.team_ref)?;
                required("role", &input.role)?;
                required("mission", &input.mission)?;
                unique_nonempty("required_capabilities", &input.required_capabilities)?;
            }
            Self::TaskPublish(input) => {
                required("team_ref", &input.team_ref)?;
                required("title", &input.title)?;
                required("objective", &input.objective)?;
                required("acceptance", &input.acceptance)?;
                unique_nonempty("required_capabilities", &input.required_capabilities)?;
                unique_nonempty("depends_on", &input.depends_on)?;
            }
            Self::TaskClaim(input) => {
                required("task_ref", &input.task_ref)?;
                optional_nonempty("reason", input.reason.as_deref())?;
            }
            Self::TaskRelease(input) => {
                required("task_ref", &input.task_ref)?;
                required("reason", &input.reason)?;
            }
            Self::TaskSupersede(input) => {
                required("task_ref", &input.task_ref)?;
                required("reason", &input.reason)?;
                if input.replacement_task_refs.is_empty() {
                    return Err(AgentActionValidationError::Missing("replacement_task_refs"));
                }
                if input.evidence_refs.is_empty() {
                    return Err(AgentActionValidationError::Missing("evidence_refs"));
                }
                unique_nonempty("replacement_task_refs", &input.replacement_task_refs)?;
                unique_nonempty("evidence_refs", &input.evidence_refs)?;
                if input
                    .replacement_task_refs
                    .iter()
                    .any(|replacement| replacement == &input.task_ref)
                {
                    return Err(AgentActionValidationError::Duplicate(
                        "task_ref_in_replacement_task_refs",
                    ));
                }
            }
            Self::TaskWithdraw(input) => {
                required("task_ref", &input.task_ref)?;
                required("reason_ref", &input.reason_ref)?;
                unique_nonempty("evidence_refs", &input.evidence_refs)?;
            }
            Self::TaskAttemptDispatch(input) => {
                required("task_ref", &input.task_ref)?;
                required("execution_id", &input.execution_id)?;
                required("agent_ref", &input.agent_ref)?;
                required("membership_id", &input.membership_id)?;
            }
            Self::TaskAttemptFail(input) => {
                required("task_ref", &input.task_ref)?;
                required("execution_id", &input.execution_id)?;
                required("reason", &input.reason)?;
            }
            Self::TaskSubmit(input) => {
                required("task_ref", &input.task_ref)?;
                if input.artifact_refs.is_empty() {
                    return Err(AgentActionValidationError::Missing("artifact_refs"));
                }
                unique_nonempty("artifact_refs", &input.artifact_refs)?;
                unique_nonempty("evidence_refs", &input.evidence_refs)?;
                unique_nonempty("unresolved", &input.unresolved)?;
            }
            Self::TaskReview(input) => {
                required("task_ref", &input.task_ref)?;
                required("reason", &input.reason)?;
                unique_nonempty("evidence_refs", &input.evidence_refs)?;
            }
            Self::MessagePublish(input) => {
                required("topic_ref", &input.topic_ref)?;
                optional_nonempty("summary", input.summary.as_deref())?;
                optional_nonempty("content_ref", input.content_ref.as_deref())?;
                if input.summary.is_none() && input.content_ref.is_none() {
                    return Err(AgentActionValidationError::Missing(
                        "summary_or_content_ref",
                    ));
                }
                unique_nonempty("refs", &input.refs)?;
                unique_nonempty("recipients", &input.recipients)?;
                let issue_refs = input
                    .issue_dispositions
                    .iter()
                    .map(|item| item.issue_ref.clone())
                    .collect::<Vec<_>>();
                unique_nonempty("issue_dispositions.issue_ref", &issue_refs)?;
                for item in &input.issue_dispositions {
                    required("issue_dispositions.issue_ref", &item.issue_ref)?;
                    required("issue_dispositions.reason_ref", &item.reason_ref)?;
                    unique_nonempty("issue_dispositions.evidence_refs", &item.evidence_refs)?;
                    if item.evidence_refs.is_empty() {
                        return Err(AgentActionValidationError::Missing(
                            "issue_dispositions.evidence_refs",
                        ));
                    }
                }
                if let Some(intent) = &input.intent {
                    required("intent.task_ref", &intent.task_ref)?;
                    optional_nonempty("intent.reason_ref", intent.reason_ref.as_deref())?;
                    unique_nonempty(
                        "intent.requested_capability_refs",
                        &intent.requested_capability_refs,
                    )?;
                }
            }
            Self::ArtifactCommit(input) => {
                required("content_ref", &input.content_ref)?;
                required("kind", &input.kind)?;
                required("title", &input.title)?;
                unique_nonempty("relates_to", &input.relates_to)?;
            }
            Self::ObjectiveUpdate(input) => {
                optional_nonempty("criterion_ref", input.criterion_ref.as_deref())?;
                optional_nonempty("statement_ref", input.statement_ref.as_deref())?;
                optional_nonempty("reason_ref", input.reason_ref.as_deref())?;
                unique_nonempty("source_refs", &input.source_refs)?;
                unique_nonempty("evidence_requirements", &input.evidence_requirements)?;
                if matches!(input.operation, ObjectiveUpdateOperation::Add)
                    && input.statement_ref.is_none()
                {
                    return Err(AgentActionValidationError::Missing("statement_ref"));
                }
                if matches!(input.operation, ObjectiveUpdateOperation::Add)
                    && input.source_refs.is_empty()
                {
                    return Err(AgentActionValidationError::Missing("source_refs"));
                }
                if matches!(
                    input.operation,
                    ObjectiveUpdateOperation::Replace | ObjectiveUpdateOperation::Retire
                ) && input.criterion_ref.is_none()
                {
                    return Err(AgentActionValidationError::Missing("criterion_ref"));
                }
                if matches!(
                    input.operation,
                    ObjectiveUpdateOperation::Replace | ObjectiveUpdateOperation::Retire
                ) && input.source_refs.is_empty()
                {
                    return Err(AgentActionValidationError::Missing("source_refs"));
                }
                if matches!(
                    input.operation,
                    ObjectiveUpdateOperation::Replace | ObjectiveUpdateOperation::Retire
                ) && input.reason_ref.is_none()
                {
                    return Err(AgentActionValidationError::Missing("reason_ref"));
                }
            }
            Self::ObjectiveReview(input) => {
                required("criterion_ref", &input.criterion_ref)?;
                required("reason_ref", &input.reason_ref)?;
                unique_nonempty("result_refs", &input.result_refs)?;
                unique_nonempty("evidence_refs", &input.evidence_refs)?;
                if input.result_refs.is_empty() && input.evidence_refs.is_empty() {
                    return Err(AgentActionValidationError::Missing(
                        "result_refs_or_evidence_refs",
                    ));
                }
            }
            Self::MembershipUpdate(input) => {
                required("agent_ref", &input.agent_ref)?;
                required("team_ref", &input.team_ref)?;
                optional_nonempty("reason_ref", input.reason_ref.as_deref())?;
            }
            Self::TeamUpdate(input) => {
                required("team_ref", &input.team_ref)?;
                optional_nonempty("mission_ref", input.mission_ref.as_deref())?;
                optional_nonempty("reason_ref", input.reason_ref.as_deref())?;
            }
            Self::ObjectiveCompleteRequest(input) => {
                if input.result_refs.is_empty() {
                    return Err(AgentActionValidationError::Missing("result_refs"));
                }
                unique_nonempty("result_refs", &input.result_refs)?;
                unique_nonempty("evidence_refs", &input.evidence_refs)?;
                unique_nonempty("unresolved", &input.unresolved)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentActionEnvelope {
    pub action_id: String,
    pub actor: AgentActorBinding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_revision: Option<u64>,
    pub action: AgentAction,
}

impl AgentActionEnvelope {
    pub fn validate(&self) -> Result<(), AgentActionValidationError> {
        required("action_id", &self.action_id)?;
        required("objective_id", &self.actor.objective_id)?;
        required("program_id", &self.actor.program_id)?;
        required("session_id", &self.actor.session_id)?;
        required("turn_id", &self.actor.turn_id)?;
        optional_nonempty("root_execution_id", self.actor.root_execution_id.as_deref())?;
        required("actor_id", &self.actor.actor_id)?;
        optional_nonempty("team_id", self.actor.team_id.as_deref())?;
        optional_nonempty("agent_id", self.actor.agent_id.as_deref())?;
        if matches!(
            self.actor.kind,
            AgentActorKind::Agent | AgentActorKind::TeamLead
        ) && (self.actor.team_id.is_none() || self.actor.agent_id.is_none())
        {
            return Err(AgentActionValidationError::Missing(
                "managed_actor_team_and_agent",
            ));
        }
        self.action.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentActionStatus {
    Applied,
    Observed,
    Rejected,
    AwaitingResource,
    Blocked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentActionObservation {
    pub receipt_id: String,
    pub action_id: String,
    pub action: String,
    pub program_id: String,
    pub revision: u64,
    pub status: AgentActionStatus,
    /// True when Runtime returned the receipt of an already committed action.
    /// Consumers must not repeat dispatch or other derived effects.
    #[serde(default)]
    pub duplicate: bool,
    #[serde(default)]
    pub changed_refs: Vec<String>,
    #[serde(default)]
    pub actionable: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<AgentActionErrorObservation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AgentActionErrorObservation {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub recoverable: bool,
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum AgentActionValidationError {
    #[error("agent action field `{0}` is required")]
    Missing(&'static str),
    #[error("agent action field `{0}` cannot be empty")]
    Empty(&'static str),
    #[error("agent action field `{0}` contains duplicate values")]
    Duplicate(&'static str),
    #[error("agent action field `{0}` has an invalid canonical form")]
    Invalid(&'static str),
}

fn required(field: &'static str, value: &str) -> Result<(), AgentActionValidationError> {
    if value.trim().is_empty() {
        Err(AgentActionValidationError::Empty(field))
    } else {
        Ok(())
    }
}

fn optional_nonempty(
    field: &'static str,
    value: Option<&str>,
) -> Result<(), AgentActionValidationError> {
    if value.is_some_and(|value| value.trim().is_empty()) {
        Err(AgentActionValidationError::Empty(field))
    } else {
        Ok(())
    }
}

fn unique_nonempty(
    field: &'static str,
    values: &[String],
) -> Result<(), AgentActionValidationError> {
    let mut unique = BTreeSet::new();
    for value in values {
        required(field, value)?;
        if !unique.insert(value.trim()) {
            return Err(AgentActionValidationError::Duplicate(field));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_inspection_does_not_implicitly_yield_the_planner() {
        let inspect: StateInspectInput = serde_json::from_str("{}").expect("ordinary inspection");
        assert!(!inspect.wait_for_workers);
        let wait: StateInspectInput =
            serde_json::from_str(r#"{"wait_for_workers":true}"#).expect("explicit worker wait");
        assert!(wait.wait_for_workers);
        assert!(
            serde_json::from_str::<StateInspectInput>(r#"{"wait_for_workers":"true"}"#).is_err()
        );
    }

    #[test]
    fn long_content_is_not_part_of_the_artifact_action_contract() {
        let schema = schemars::schema_for!(ArtifactCommitInput);
        let encoded = serde_json::to_value(schema).expect("schema").to_string();
        assert!(encoded.contains("content_ref"));
        assert!(!encoded.contains("\"content\""));
    }

    #[test]
    fn managed_actor_identity_is_runtime_bound() {
        let envelope = AgentActionEnvelope {
            action_id: "action-1".to_string(),
            actor: AgentActorBinding {
                objective_id: "objective-1".to_string(),
                program_id: "program-1".to_string(),
                session_id: "session-1".to_string(),
                turn_id: "turn-1".to_string(),
                root_execution_id: None,
                required_team_count: 1,
                objective_summary: "test objective".to_string(),
                model_lease: "test".to_string(),
                permission_ceiling: Some(crate::policy::PermissionMode::ReadOnly),
                resource_scopes: Vec::new(),
                actor_id: "agent-1".to_string(),
                kind: AgentActorKind::Agent,
                execution_id: Some("execution-1".to_string()),
                team_id: None,
                agent_id: None,
            },
            expected_revision: None,
            action: AgentAction::StateInspect(StateInspectInput {
                query: None,
                wait_for_workers: false,
                scope_ref: None,
                after_revision: None,
                page_cursor: None,
                entry_ref: None,
            }),
        };
        assert_eq!(
            envelope.validate(),
            Err(AgentActionValidationError::Missing(
                "managed_actor_team_and_agent"
            ))
        );
    }

    #[test]
    fn task_submission_requires_a_real_artifact_reference() {
        let action = AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: "task-1".to_string(),
            artifact_refs: Vec::new(),
            evidence_refs: Vec::new(),
            unresolved: Vec::new(),
        });
        assert_eq!(
            action.validate(),
            Err(AgentActionValidationError::Missing("artifact_refs"))
        );
    }

    #[test]
    fn semantic_capability_names_are_not_capped_by_the_action_schema() {
        let action = AgentAction::AgentInvite(AgentInviteInput {
            team_ref: "team-1".to_string(),
            role: "Domain operator".to_string(),
            mission: "use a workspace-defined capability".to_string(),
            required_capabilities: vec!["custom_domain_operation".to_string()],
            existing_agent_ref: None,
            definition_ref: None,
            model_profile_ref: None,
            expertise_hints: Vec::new(),
            execution_requirements: Vec::new(),
        });

        assert_eq!(action.validate(), Ok(()));
    }

    #[test]
    fn task_supersede_contract_requires_concrete_successors_and_evidence() {
        let schema = schemars::schema_for!(TaskSupersedeInput);
        let encoded = serde_json::to_value(schema).expect("schema").to_string();
        assert!(encoded.contains("replacement_task_refs"));
        assert!(encoded.contains("evidence_refs"));

        let missing_successor = AgentAction::TaskSupersede(TaskSupersedeInput {
            task_ref: "task:failed".to_string(),
            replacement_task_refs: Vec::new(),
            reason: "split the failed approach".to_string(),
            evidence_refs: vec!["tool://failure-receipt".to_string()],
        });
        assert_eq!(
            missing_successor.validate(),
            Err(AgentActionValidationError::Missing("replacement_task_refs"))
        );

        let split = AgentAction::TaskSupersede(TaskSupersedeInput {
            task_ref: "task:failed".to_string(),
            replacement_task_refs: vec!["task:part-a".to_string(), "task:part-b".to_string()],
            reason: "replace the failed approach with two independently reviewable parts"
                .to_string(),
            evidence_refs: vec!["tool://failure-receipt".to_string()],
        });
        assert_eq!(split.validate(), Ok(()));
    }

    #[test]
    fn objective_scope_changes_need_evidence_and_a_reason() {
        let mut replace = ObjectiveUpdateInput {
            criterion_ref: Some("criterion:derived".to_string()),
            operation: ObjectiveUpdateOperation::Replace,
            statement_ref: Some("artifact://replacement".to_string()),
            source_refs: Vec::new(),
            reason_ref: None,
            evidence_requirements: Vec::new(),
        };
        assert_eq!(
            AgentAction::ObjectiveUpdate(replace.clone()).validate(),
            Err(AgentActionValidationError::Missing("source_refs"))
        );
        replace.source_refs = vec!["tool://scope-change-evidence".to_string()];
        assert_eq!(
            AgentAction::ObjectiveUpdate(replace.clone()).validate(),
            Err(AgentActionValidationError::Missing("reason_ref"))
        );
        replace.reason_ref = Some("artifact://scope-change-rationale".to_string());
        assert_eq!(AgentAction::ObjectiveUpdate(replace).validate(), Ok(()));
    }
}
