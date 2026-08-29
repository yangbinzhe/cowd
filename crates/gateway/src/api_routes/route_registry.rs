//! Typed registration for stable Gateway routes.
//!
//! A route spec is the single stable identity for an Axum registration and
//! its externally visible schema. The execution projection family is the
//! first migration boundary; other families can move here without creating a
//! second manifest/OpenAPI list.

use std::{marker::PhantomData, sync::Arc};

use axum::{
    routing::{get, post},
    Router,
};
use harness_contract::{
    live::{
        CreateLiveSubscriptionRequest, LiveEnvelope, LiveSubscription, PatchLiveSubscriptionRequest,
    },
    mission::{MissionCommand, MissionMaterializedSnapshot, MissionProjectionDelta},
    projection::{
        ExecutionActivityDetailProjection, ExecutionCommandReceipt, ExecutionCommandRequest,
        ExecutionLiveUpdate, ExecutionProjection, SessionEvidenceProjection,
        SessionExecutionIndexProjection, SessionExecutionIndicesProjection,
        SessionHistoryIndexProjection, TurnEvidenceProjection,
    },
    turn::{SessionInputProjection, TurnInboxSnapshot},
};

use super::{runtime_routes, AppState};

pub(crate) struct TypedRouteSpec<Request, Query, Response> {
    pub(crate) method: &'static str,
    pub(crate) path: &'static str,
    pub(crate) operation_id: &'static str,
    pub(crate) _request: PhantomData<Request>,
    pub(crate) _query: PhantomData<Query>,
    pub(crate) _response: PhantomData<Response>,
}

impl<Request, Query, Response> TypedRouteSpec<Request, Query, Response> {
    const fn new(
        route: surface::gateway_api::GatewayRouteSpec,
        operation_id: &'static str,
    ) -> Self {
        Self {
            method: route.method().as_str(),
            path: route.path().template(),
            operation_id,
            _request: PhantomData,
            _query: PhantomData,
            _response: PhantomData,
        }
    }

    fn metadata(
        &self,
        request_schema: Option<&'static str>,
        response_schema: &'static str,
        streaming: bool,
    ) -> StableRouteMetadata {
        StableRouteMetadata {
            method: self.method,
            path: self.path.to_string(),
            operation_id: self.operation_id.to_string(),
            request_schema: request_schema.map(str::to_string),
            response_schema: response_schema.to_string(),
            streaming,
            request_required: request_schema.is_some(),
            session_writer: SessionWriterPolicy::None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SessionWriterPolicy {
    None,
    Required,
    Conditional,
}

impl SessionWriterPolicy {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Required => "required",
            Self::Conditional => "conditional",
        }
    }
}

/// Non-generic metadata consumed by capability/OpenAPI generation. It is
/// derived from a `TypedRouteSpec`; no feature-specific string matching is
/// permitted outside this registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StableRouteMetadata {
    pub(crate) method: &'static str,
    pub(crate) path: String,
    pub(crate) operation_id: String,
    pub(crate) request_schema: Option<String>,
    pub(crate) response_schema: String,
    pub(crate) streaming: bool,
    pub(crate) request_required: bool,
    pub(crate) session_writer: SessionWriterPolicy,
}

impl StableRouteMetadata {
    fn with_writer(mut self, policy: SessionWriterPolicy) -> Self {
        self.session_writer = policy;
        self
    }

    fn with_optional_body(mut self) -> Self {
        self.request_required = false;
        self
    }
}

fn execution_projection_snapshot_spec(
) -> TypedRouteSpec<(), runtime_routes::ExecutionProjectionQuery, ExecutionProjection> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_RUNTIME_EXECUTIONS_BY_ID,
        "runtime_execution_projection_get",
    )
}

fn execution_projection_command_spec(
) -> TypedRouteSpec<ExecutionCommandRequest, (), ExecutionCommandReceipt> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::POST_API_RUNTIME_EXECUTIONS_BY_ID_COMMANDS,
        "runtime_execution_projection_command",
    )
}

fn execution_activity_detail_spec(
) -> TypedRouteSpec<(), runtime_routes::ExecutionActivityQuery, ExecutionActivityDetailProjection> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_RUNTIME_EXECUTIONS_BY_ID_ACTIVITY,
        "runtime_execution_activity_get",
    )
}

fn send_message_spec() -> TypedRouteSpec<(), (), ()> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::POST_API_SESSIONS_BY_ID_MESSAGES,
        "session_message_send",
    )
}

fn session_input_projection_spec() -> TypedRouteSpec<(), (), SessionInputProjection> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_SESSIONS_BY_ID_INPUT_PROJECTION,
        "session_input_projection_get",
    )
}

fn session_turn_inbox_spec() -> TypedRouteSpec<(), (), TurnInboxSnapshot> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_SESSIONS_BY_ID_TURN_INBOX,
        "session_turn_inbox_get",
    )
}

fn session_turn_inbox_by_turn_spec() -> TypedRouteSpec<(), (), TurnInboxSnapshot> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_SESSIONS_BY_ID_TURNS_BY_TURN_ID_INBOX,
        "session_turn_inbox_by_turn_get",
    )
}

fn session_input_cancel_spec() -> TypedRouteSpec<(), (), ()> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::POST_API_SESSIONS_BY_ID_INPUTS_BY_INPUT_ID_CANCEL,
        "session_input_cancel",
    )
}

fn session_input_reclassify_spec() -> TypedRouteSpec<(), (), ()> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::POST_API_SESSIONS_BY_ID_INPUTS_BY_INPUT_ID_RECLASSIFY,
        "session_input_reclassify",
    )
}

fn session_cancel_spec() -> TypedRouteSpec<(), (), ()> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::POST_API_SESSIONS_BY_ID_CANCEL,
        "session_turn_cancel",
    )
}

fn session_compact_spec() -> TypedRouteSpec<(), (), ()> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::POST_API_SESSIONS_BY_ID_COMPACT,
        "session_compact",
    )
}

fn slash_dispatch_spec() -> TypedRouteSpec<(), (), ()> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::POST_API_SLASH_DISPATCH,
        "slash_dispatch",
    )
}

fn auth_verify_spec() -> TypedRouteSpec<(), (), ()> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_AUTH_VERIFY,
        "auth_verify",
    )
}

fn session_execution_indices_spec() -> TypedRouteSpec<(), (), SessionExecutionIndicesProjection> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_SESSIONS_EXECUTIONS,
        "session_execution_indices_get",
    )
}

fn session_execution_index_spec() -> TypedRouteSpec<(), (), SessionExecutionIndexProjection> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_SESSIONS_BY_ID_EXECUTION,
        "session_execution_index_get",
    )
}

fn session_execution_live_spec() -> TypedRouteSpec<(), (), ExecutionLiveUpdate> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_SESSIONS_BY_ID_EXECUTION_LIVE,
        "session_execution_live_get",
    )
}

fn session_execution_policy_get_spec(
) -> TypedRouteSpec<(), (), harness_contract::policy::SessionExecutionPolicyResponse> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_SESSIONS_BY_ID_EXECUTION_POLICY,
        "session_execution_policy_get",
    )
}

fn session_execution_policy_put_spec() -> TypedRouteSpec<
    harness_contract::policy::UpdateSessionExecutionPolicyRequest,
    (),
    harness_contract::policy::SessionExecutionPolicyResponse,
> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::PUT_API_SESSIONS_BY_ID_EXECUTION_POLICY,
        "session_execution_policy_put",
    )
}

fn approval_pending_spec() -> TypedRouteSpec<(), super::approval_routes::ApprovalPendingQuery, ()> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_APPROVAL_PENDING,
        "approval_pending_get",
    )
}

fn approval_exact_spec() -> TypedRouteSpec<(), (), ()> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_APPROVAL_BY_ID,
        "approval_exact_get",
    )
}

fn approval_respond_spec() -> TypedRouteSpec<(), (), ()> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::POST_API_APPROVAL_RESPOND,
        "approval_respond_post",
    )
}

fn session_history_index_spec() -> TypedRouteSpec<
    (),
    super::session_routes::SessionHistoryIndexQuery,
    SessionHistoryIndexProjection,
> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_SESSIONS_BY_ID_HISTORY_INDEX,
        "session_history_index_get",
    )
}

fn session_evidence_spec() -> TypedRouteSpec<(), (), SessionEvidenceProjection> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_SESSIONS_BY_ID_EVIDENCE,
        "session_evidence_get",
    )
}

fn turn_evidence_spec() -> TypedRouteSpec<(), (), TurnEvidenceProjection> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_SESSIONS_BY_ID_TURNS_BY_TURN_ID_EVIDENCE,
        "session_turn_evidence_get",
    )
}

fn live_create_spec() -> TypedRouteSpec<CreateLiveSubscriptionRequest, (), LiveSubscription> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::POST_API_RUNTIME_LIVE_SUBSCRIPTIONS,
        "runtime_live_subscription_create",
    )
}

fn live_patch_spec() -> TypedRouteSpec<PatchLiveSubscriptionRequest, (), LiveSubscription> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::PATCH_API_RUNTIME_LIVE_SUBSCRIPTIONS_BY_ID,
        "runtime_live_subscription_patch",
    )
}

fn live_delete_spec() -> TypedRouteSpec<(), (), ()> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::DELETE_API_RUNTIME_LIVE_SUBSCRIPTIONS_BY_ID,
        "runtime_live_subscription_delete",
    )
}

fn live_stream_spec() -> TypedRouteSpec<(), (), LiveEnvelope> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_RUNTIME_LIVE_BY_ID,
        "runtime_live_stream_get",
    )
}

fn mission_control_snapshot_spec() -> TypedRouteSpec<(), (), MissionMaterializedSnapshot> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_MISSION_CONTROL,
        "mission_control_get",
    )
}

fn mission_control_command_spec() -> TypedRouteSpec<MissionCommand, (), MissionMaterializedSnapshot>
{
    TypedRouteSpec::new(
        surface::gateway_api::routes::POST_API_MISSION_CONTROL,
        "mission_control_command",
    )
}

fn mission_control_delta_spec() -> TypedRouteSpec<(), (), MissionProjectionDelta> {
    TypedRouteSpec::new(
        surface::gateway_api::routes::GET_API_MISSION_CONTROL_DELTA,
        "mission_control_delta_get",
    )
}

pub(crate) fn typed_route_metadata() -> Vec<StableRouteMetadata> {
    vec![
        execution_projection_snapshot_spec().metadata(None, "ExecutionProjection", false),
        execution_activity_detail_spec().metadata(None, "ExecutionActivityDetailProjection", false),
        execution_projection_command_spec()
            .metadata(
                Some("ExecutionCommandRequest"),
                "ExecutionCommandReceipt",
                false,
            )
            .with_writer(SessionWriterPolicy::Required),
        send_message_spec()
            .metadata(Some("SendMessageRequest"), "SendMessageReceipt", false)
            .with_writer(SessionWriterPolicy::Required),
        session_input_projection_spec().metadata(None, "SessionInputProjection", false),
        session_turn_inbox_spec().metadata(None, "TurnInboxSnapshot", false),
        session_turn_inbox_by_turn_spec().metadata(None, "TurnInboxSnapshot", false),
        session_input_cancel_spec()
            .metadata(
                Some("SessionInputCancelRequest"),
                "SessionInputMutationReceipt",
                false,
            )
            .with_writer(SessionWriterPolicy::Required),
        session_input_reclassify_spec()
            .metadata(
                Some("SessionInputReclassifyRequest"),
                "SessionInputMutationReceipt",
                false,
            )
            .with_writer(SessionWriterPolicy::Required),
        session_cancel_spec()
            .metadata(
                Some("CancelSessionTurnRequest"),
                "CancelSessionTurnReceipt",
                false,
            )
            .with_writer(SessionWriterPolicy::Required),
        session_compact_spec()
            .metadata(Some("Empty"), "ContextCompactionResult", false)
            .with_optional_body()
            .with_writer(SessionWriterPolicy::Required),
        slash_dispatch_spec()
            .metadata(Some("SlashDispatchRequest"), "SlashDispatchReceipt", false)
            .with_writer(SessionWriterPolicy::Conditional),
        auth_verify_spec().metadata(None, "AuthVerifyResponse", false),
        session_execution_indices_spec().metadata(None, "SessionExecutionIndicesProjection", false),
        session_execution_index_spec().metadata(None, "SessionExecutionIndexProjection", false),
        session_execution_live_spec().metadata(None, "ExecutionLiveUpdate", false),
        session_execution_policy_get_spec().metadata(None, "SessionExecutionPolicyResponse", false),
        session_execution_policy_put_spec()
            .metadata(
                Some("UpdateSessionExecutionPolicyRequest"),
                "SessionExecutionPolicyResponse",
                false,
            )
            .with_writer(SessionWriterPolicy::Required),
        approval_pending_spec().metadata(None, "ApprovalPendingResponse", false),
        approval_exact_spec().metadata(None, "ApprovalExactResponse", false),
        approval_respond_spec().metadata(None, "ApprovalRespondReceipt", false),
        session_history_index_spec().metadata(None, "SessionHistoryIndexProjection", false),
        session_evidence_spec().metadata(None, "SessionEvidenceProjection", false),
        turn_evidence_spec().metadata(None, "TurnEvidenceProjection", false),
        live_create_spec().metadata(
            Some("CreateLiveSubscriptionRequest"),
            "LiveSubscription",
            false,
        ),
        live_patch_spec().metadata(
            Some("PatchLiveSubscriptionRequest"),
            "LiveSubscription",
            false,
        ),
        live_delete_spec().metadata(None, "Empty", false),
        live_stream_spec().metadata(None, "LiveEnvelope", true),
        mission_control_snapshot_spec().metadata(None, "MissionControlResponse", false),
        mission_control_command_spec()
            .metadata(Some("MissionCommand"), "MissionCommandResponse", false)
            .with_writer(SessionWriterPolicy::Conditional),
        mission_control_delta_spec().metadata(None, "MissionProjectionDelta", false),
        TypedRouteSpec::<(), (), ()>::new(surface::gateway_api::routes::GET_API_TASKS, "task_list")
            .metadata(None, "TaskListResponse", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_START,
            "task_start",
        )
        .metadata(Some("StartTaskRequest"), "TaskAggregate", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::GET_API_TASKS_BY_ID,
            "task_detail",
        )
        .metadata(None, "TaskDetailResponse", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::GET_API_TASKS_BY_ID_TURNS,
            "task_turns",
        )
        .metadata(None, "TaskTurnsResponse", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_BY_ID_FOCUS,
            "task_focus",
        )
        .metadata(Some("TaskFocusRequest"), "SessionFocusReceipt", false)
        .with_writer(SessionWriterPolicy::Required),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_MISSION_PREVIEW,
            "task_mission_batch_preview",
        )
        .metadata(
            Some("TaskMissionRequest"),
            "TaskMissionPreviewResponse",
            false,
        ),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_MISSION_COMMIT,
            "task_mission_batch_commit",
        )
        .metadata(
            Some("TaskMissionRequest"),
            "TaskMissionCommitResponse",
            false,
        ),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_BY_ID_MISSION_PREVIEW,
            "task_mission_preview",
        )
        .metadata(
            Some("TaskMissionRequest"),
            "TaskMissionPreviewResponse",
            false,
        ),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_BY_ID_MISSION_COMMIT,
            "task_mission_commit",
        )
        .metadata(
            Some("TaskMissionRequest"),
            "TaskMissionCommitResponse",
            false,
        ),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::GET_API_TASKS_MISSION_ORGANIZATION,
            "mission_organization_list",
        )
        .metadata(None, "MissionOrganizationResponse", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_BY_ID_PHASES,
            "task_phase_start",
        )
        .metadata(Some("StartTaskPhaseRequest"), "TaskAggregate", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_BY_ID_PHASES_BY_PHASE_ID_ARTIFACTS,
            "task_phase_artifact_record",
        )
        .metadata(Some("TaskPhaseArtifactRequest"), "TaskAggregate", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_BY_ID_PHASES_BY_PHASE_ID_REVIEW,
            "task_phase_review",
        )
        .metadata(Some("TaskPhaseReviewRequest"), "TaskAggregate", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_BY_ID_CANCEL,
            "task_cancel",
        )
        .metadata(Some("TaskTransitionRequest"), "TaskAggregate", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_BY_ID_COMPLETE,
            "task_complete",
        )
        .metadata(Some("TaskTransitionRequest"), "TaskAggregate", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::POST_API_TASKS_BY_ID_FAILURE,
            "task_failure",
        )
        .metadata(Some("TaskFailureRequest"), "TaskAggregate", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::GET_API_SESSIONS_BY_ID_TASK_FOCUS,
            "session_task_focus_get",
        )
        .metadata(None, "TaskFocusProjection", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::PUT_API_SESSIONS_BY_ID_TASK_FOCUS,
            "session_task_focus_set",
        )
        .metadata(
            Some("SessionTaskFocusRequest"),
            "SessionFocusReceipt",
            false,
        )
        .with_writer(SessionWriterPolicy::Required),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::DELETE_API_SESSIONS_BY_ID_TASK_FOCUS,
            "session_task_focus_clear",
        )
        .metadata(
            Some("SessionFocusClearRequest"),
            "SessionFocusReceipt",
            false,
        )
        .with_writer(SessionWriterPolicy::Required),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::GET_API_SESSIONS_BY_ID_MISSION_FOCUS,
            "session_mission_focus_get",
        )
        .metadata(None, "MissionFocusProjection", false),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::PUT_API_SESSIONS_BY_ID_MISSION_FOCUS,
            "session_mission_focus_set",
        )
        .metadata(
            Some("SessionMissionFocusRequest"),
            "SessionFocusReceipt",
            false,
        )
        .with_writer(SessionWriterPolicy::Required),
        TypedRouteSpec::<(), (), ()>::new(
            surface::gateway_api::routes::DELETE_API_SESSIONS_BY_ID_MISSION_FOCUS,
            "session_mission_focus_clear",
        )
        .metadata(
            Some("SessionFocusClearRequest"),
            "SessionFocusReceipt",
            false,
        )
        .with_writer(SessionWriterPolicy::Required),
    ]
}

pub(crate) fn stable_route_metadata(method: &str, path: &str) -> Option<StableRouteMetadata> {
    typed_route_metadata()
        .into_iter()
        .find(|spec| spec.method == method && spec.path == path)
}

pub(super) fn register_execution_projection_routes(
    router: Router<Arc<AppState>>,
) -> Router<Arc<AppState>> {
    let snapshot = execution_projection_snapshot_spec();
    let activity = execution_activity_detail_spec();
    let command = execution_projection_command_spec();
    router
        .route(snapshot.path, get(runtime_routes::get_execution_projection))
        .route(activity.path, get(runtime_routes::get_execution_activity))
        .route(
            command.path,
            post(runtime_routes::execute_projection_command),
        )
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn typed_specs_cover_projection_and_live_contracts() {
        let specs = typed_route_metadata();
        let operation_ids = specs
            .iter()
            .map(|spec| spec.operation_id.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(operation_ids.len(), specs.len());
        let spec = |operation_id: &str| {
            specs
                .iter()
                .find(|spec| spec.operation_id == operation_id)
                .unwrap_or_else(|| panic!("missing typed route metadata for {operation_id}"))
        };

        assert_eq!(
            spec("runtime_execution_projection_get").response_schema,
            "ExecutionProjection"
        );
        assert_eq!(
            spec("runtime_execution_activity_get").response_schema,
            "ExecutionActivityDetailProjection"
        );
        let execution_command = spec("runtime_execution_projection_command");
        assert_eq!(
            execution_command.request_schema,
            Some("ExecutionCommandRequest".to_string())
        );
        assert_eq!(execution_command.response_schema, "ExecutionCommandReceipt");
        assert_eq!(
            execution_command.session_writer,
            SessionWriterPolicy::Required
        );
        let send_message = spec("session_message_send");
        assert_eq!(
            send_message.request_schema.as_deref(),
            Some("SendMessageRequest")
        );
        assert_eq!(send_message.session_writer, SessionWriterPolicy::Required);
        assert_eq!(
            spec("slash_dispatch").session_writer,
            SessionWriterPolicy::Conditional
        );
        assert_eq!(spec("auth_verify").response_schema, "AuthVerifyResponse");
        assert_eq!(
            spec("runtime_live_subscription_create").request_schema,
            Some("CreateLiveSubscriptionRequest".to_string())
        );
        let live_stream = spec("runtime_live_stream_get");
        assert_eq!(live_stream.response_schema, "LiveEnvelope");
        assert!(live_stream.streaming);
        assert_eq!(
            spec("mission_control_get").response_schema,
            "MissionControlResponse"
        );
        assert_eq!(
            spec("mission_control_command").request_schema.as_deref(),
            Some("MissionCommand")
        );
        assert_eq!(
            spec("mission_control_command").session_writer,
            SessionWriterPolicy::Conditional
        );
        assert_eq!(
            spec("mission_control_delta_get").response_schema,
            "MissionProjectionDelta"
        );
        assert_eq!(
            spec("session_history_index_get").response_schema,
            "SessionHistoryIndexProjection"
        );
    }
}
