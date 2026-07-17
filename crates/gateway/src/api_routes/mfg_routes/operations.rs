use std::{convert::Infallible, time::Instant};

use axum::{
    extract::Extension,
    http::HeaderMap,
    response::{sse::Event, Response, Sse},
};

use crate::api_routes::{principal_actor_id, AuthenticatedPrincipal};
use surface::SurfaceSendRequest;

use super::*;

#[derive(Debug, Deserialize, JsonSchema)]
pub(super) struct MfgAlertListQuery {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct MfgAlertRuleUpsertRequest {
    #[serde(default)]
    idempotency_key: Option<String>,
    rule: MfgAlertRuleInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct MfgAlertSubscriptionUpsertRequest {
    #[serde(default)]
    idempotency_key: Option<String>,
    subscription: MfgAlertSubscriptionInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct MfgAlertCommandRequest {
    command: MfgAlertCommand,
    expected_revision: u64,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    until: Option<chrono::DateTime<chrono::Utc>>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(super) struct MfgForecastQuery {
    #[serde(default)]
    metric_refs: Option<String>,
    #[serde(default = "default_forecast_horizon")]
    horizon: String,
    #[serde(default)]
    limit: Option<usize>,
}

fn default_forecast_horizon() -> String {
    "next_period".to_string()
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(super) struct MfgAssignmentListQuery {
    #[serde(default)]
    assignee_ref: Option<String>,
    #[serde(default)]
    incident_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct MfgAssignmentUpsertRequest {
    #[serde(default)]
    idempotency_key: Option<String>,
    assignment: MfgAssignmentInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct MfgAssignmentCommandRequest {
    command: MfgAssignmentCommand,
    expected_revision: u64,
    #[serde(default)]
    idempotency_key: Option<String>,
    #[serde(default)]
    target_ref: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub(super) struct MfgLiveQuery {
    #[serde(default)]
    limit: Option<usize>,
}

pub(super) async fn mfg_alert_rule_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgAlertListQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let actor = principal_actor_id(&principal);
    let rules = state
        .services
        .mfg
        .list_alert_rules(
            &state.config_home,
            Some(&actor),
            query.limit.unwrap_or(100).clamp(1, 500),
        )
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(
        serde_json::json!({ "kind": "mfg.alert_rule_list", "items": rules }),
    ))
}

pub(super) async fn mfg_alert_rule_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(mut request): Json<MfgAlertRuleUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let expected_revision = request.rule.expected_revision;
    let actor = principal_actor_id(&principal);
    let idempotency_key = mfg_idempotency_key(&headers, request.idempotency_key.take())
        .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?;
    request
        .rule
        .rule_id
        .get_or_insert_with(|| stable_mfg_resource_id("alert-rule", &idempotency_key));
    let existing_rule = state
        .services
        .mfg
        .list_alert_rules(&state.config_home, None, 500)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .find(|rule| rule.rule_id == request.rule.rule_id.as_deref().unwrap_or_default());
    if existing_rule.is_some_and(|rule| rule.owner_ref != actor) {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "alert rule was not found in the verified principal scope",
        ));
    }
    request.rule.owner_ref = actor.clone();
    let rule = MfgAlertRule::from_input(request.rule);
    let (rule, receipt) = state
        .services
        .mfg
        .upsert_alert_rule_receipted(
            &state.config_home,
            &rule,
            expected_revision,
            &actor,
            &idempotency_key,
        )
        .map_err(mfg_mutation_error)?;
    let receipt = receipt
        .canonical_receipt()
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.message))?;
    Ok(Json(
        serde_json::json!({ "kind": "mfg.alert_rule", "rule": rule, "business_receipt": receipt }),
    ))
}

pub(super) async fn mfg_alert_occurrence_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgAlertListQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let actor = principal_actor_id(&principal);
    let visible_rule_ids = state
        .services
        .mfg
        .list_alert_rules(&state.config_home, Some(&actor), 500)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .map(|rule| rule.rule_id)
        .collect::<std::collections::BTreeSet<_>>();
    let items = state
        .services
        .mfg
        .list_alert_occurrences(
            &state.config_home,
            query.status.as_deref(),
            query.limit.unwrap_or(100).clamp(1, 500),
        )
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .filter(|occurrence| visible_rule_ids.contains(&occurrence.rule_id))
        .collect::<Vec<_>>();
    Ok(Json(
        serde_json::json!({ "kind": "mfg.alert_occurrence_list", "items": items }),
    ))
}

pub(super) async fn mfg_alert_subscription_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgAlertListQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let actor = principal_actor_id(&principal);
    let items = state
        .services
        .mfg
        .list_alert_subscriptions(
            &state.config_home,
            Some(&actor),
            query.limit.unwrap_or(100).clamp(1, 500),
        )
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(
        serde_json::json!({ "kind": "mfg.alert_subscription_list", "items": items }),
    ))
}

pub(super) async fn mfg_alert_subscription_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(mut request): Json<MfgAlertSubscriptionUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let expected_revision = request.subscription.expected_revision;
    let actor = principal_actor_id(&principal);
    let idempotency_key = mfg_idempotency_key(&headers, request.idempotency_key.take())
        .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?;
    request
        .subscription
        .subscription_id
        .get_or_insert_with(|| stable_mfg_resource_id("alert-subscription", &idempotency_key));
    let existing_subscription = state
        .services
        .mfg
        .list_alert_subscriptions(&state.config_home, None, 500)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .find(|subscription| {
            subscription.subscription_id
                == request
                    .subscription
                    .subscription_id
                    .as_deref()
                    .unwrap_or_default()
        });
    if existing_subscription.is_some_and(|subscription| subscription.subscriber_ref != actor) {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "alert subscription was not found in the verified principal scope",
        ));
    }
    let owns_rule = state
        .services
        .mfg
        .list_alert_rules(&state.config_home, Some(&actor), 500)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .iter()
        .any(|rule| rule.rule_id == request.subscription.rule_id);
    if !owns_rule {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "alert rule was not found in the verified principal scope",
        ));
    }
    let subscription = MfgAlertSubscription::from_input(request.subscription, actor.clone());
    let (subscription, receipt) = state
        .services
        .mfg
        .upsert_alert_subscription_receipted(
            &state.config_home,
            &subscription,
            expected_revision,
            &actor,
            &idempotency_key,
        )
        .map_err(mfg_mutation_error)?;
    let receipt = receipt
        .canonical_receipt()
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.message))?;
    Ok(Json(
        serde_json::json!({ "kind": "mfg.alert_subscription", "subscription": subscription, "business_receipt": receipt }),
    ))
}

pub(super) async fn mfg_alert_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(request): Json<MfgAlertCommandRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_mfg_capability(&principal, "mfg.alert.respond")?;
    let idempotency_key = mfg_idempotency_key(&headers, request.idempotency_key)
        .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?;
    let actor = principal_actor_id(&principal);
    let visible_rule_ids = state
        .services
        .mfg
        .list_alert_rules(&state.config_home, Some(&actor), 500)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .map(|rule| rule.rule_id)
        .collect::<std::collections::BTreeSet<_>>();
    let occurrence = state
        .services
        .mfg
        .list_alert_occurrences(&state.config_home, None, 500)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .find(|item| item.occurrence_id == id);
    if !occurrence.is_some_and(|item| visible_rule_ids.contains(&item.rule_id)) {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "alert occurrence was not found in the verified principal scope",
        ));
    }
    let (occurrence, receipt) = state
        .services
        .mfg
        .command_alert(
            &state.config_home,
            &id,
            MfgAlertCommandInput {
                command: request.command,
                actor_ref: actor,
                expected_revision: request.expected_revision,
                idempotency_key,
                until: request.until,
                reason: request.reason,
            },
        )
        .map_err(mfg_mutation_error)?;
    let receipt = receipt
        .canonical_receipt()
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.message))?;
    Ok(Json(
        serde_json::json!({ "kind": "mfg.alert_command_receipt", "occurrence": occurrence, "business_receipt": receipt }),
    ))
}

pub(super) async fn mfg_forecast_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<MfgForecastQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let metric_refs = query
        .metric_refs
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    let items = state
        .services
        .mfg
        .forecasts(
            &state.config_home,
            &metric_refs,
            &query.horizon,
            query.limit.unwrap_or(50).clamp(1, 200),
        )
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(
        serde_json::json!({ "kind": "mfg.forecast_list", "horizon": query.horizon, "items": items }),
    ))
}

pub(super) async fn mfg_assignment_upsert_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(mut request): Json<MfgAssignmentUpsertRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    require_mfg_capability(&principal, "mfg.assignment.manage")?;
    let expected_revision = request.assignment.expected_revision;
    let actor = principal_actor_id(&principal);
    let idempotency_key = mfg_idempotency_key(&headers, request.idempotency_key.take())
        .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?;
    request
        .assignment
        .assignment_id
        .get_or_insert_with(|| stable_mfg_resource_id("assignment", &idempotency_key));
    if let Some(existing) = state
        .services
        .mfg
        .get_assignment(
            &state.config_home,
            request
                .assignment
                .assignment_id
                .as_deref()
                .unwrap_or_default(),
        )
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    {
        if existing.created_by != actor {
            return Err(mfg_api_error(
                StatusCode::NOT_FOUND,
                "assignment was not found in the verified principal scope",
            ));
        }
    }
    let task_id = request
        .assignment
        .task_ref
        .trim()
        .trim_start_matches("task://")
        .trim_start_matches("task:");
    let task_exists = state
        .services
        .task
        .list_records()
        .map_err(|error| mfg_api_error(StatusCode::SERVICE_UNAVAILABLE, error))?
        .iter()
        .any(|task| task.id == task_id);
    if !task_exists {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "assignment task_ref does not resolve to an existing task",
        ));
    }
    let assignment = MfgAssignment::from_input(request.assignment, actor.clone());
    let (assignment, receipt) = state
        .services
        .mfg
        .upsert_assignment_receipted(
            &state.config_home,
            &assignment,
            expected_revision,
            &actor,
            &idempotency_key,
        )
        .map_err(mfg_mutation_error)?;
    let receipt = receipt
        .canonical_receipt()
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.message))?;
    Ok(Json(
        serde_json::json!({ "kind": "mfg.assignment", "assignment": assignment, "business_receipt": receipt }),
    ))
}

pub(super) async fn mfg_assignment_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgAssignmentListQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let assignments = state
        .services
        .mfg
        .list_assignments(
            &state.config_home,
            query.assignee_ref.as_deref(),
            query.incident_id.as_deref(),
            query.limit.unwrap_or(100).clamp(1, 500),
        )
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .into_iter()
        .filter(|item| assignment_visible_to(item, &principal))
        .collect::<Vec<_>>();
    let items = assignments
        .iter()
        .map(|assignment| assignment_with_notification_refs(&state, assignment))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(
        serde_json::json!({ "kind": "mfg.assignment_list", "items": items }),
    ))
}

pub(super) async fn mfg_assignment_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let assignment = state
        .services
        .mfg
        .get_assignment(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG assignment not found"))?;
    if !assignment_visible_to(&assignment, &principal) {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "assignment was not found in the verified principal scope",
        ));
    }
    let assignment = assignment_with_notification_refs(&state, &assignment)?;
    Ok(Json(
        serde_json::json!({ "kind": "mfg.assignment", "assignment": assignment }),
    ))
}

fn assignment_with_notification_refs(
    state: &AppState,
    assignment: &MfgAssignment,
) -> Result<serde_json::Value, (StatusCode, Json<ErrorResponse>)> {
    let mut value = serde_json::to_value(assignment).map_err(|error| {
        mfg_api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to serialize assignment: {error}"),
        )
    })?;
    let notification_refs = state
        .services
        .mfg
        .command_notification_refs_for_resource(
            &state.config_home,
            &format!("mfg:assignment:{}", assignment.assignment_id),
        )
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "notification_refs".to_string(),
            serde_json::to_value(notification_refs).unwrap_or_default(),
        );
    }
    Ok(value)
}

pub(super) async fn mfg_assignment_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(request): Json<MfgAssignmentCommandRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let capability = if matches!(
        request.command,
        MfgAssignmentCommand::Start | MfgAssignmentCommand::Complete
    ) {
        "mfg.assignment.lifecycle"
    } else {
        "mfg.assignment.manage"
    };
    require_mfg_capability(&principal, capability)?;
    let assignment = state
        .services
        .mfg
        .get_assignment(&state.config_home, &id)
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| mfg_api_error(StatusCode::NOT_FOUND, "MFG assignment not found"))?;
    if !assignment_visible_to(&assignment, &principal) {
        return Err(mfg_api_error(
            StatusCode::NOT_FOUND,
            "assignment was not found in the verified principal scope",
        ));
    }
    let idempotency_key = mfg_idempotency_key(&headers, request.idempotency_key)
        .map_err(|error| mfg_api_error(StatusCode::BAD_REQUEST, error.message))?;
    let correlation_id = headers
        .get("x-cowd-correlation-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("mfg-assignment:{idempotency_key}"));
    let actor_ref = principal_actor_id(&principal);
    let action_id = assignment_command_action_id(&request.command);
    let resource_ref = format!("mfg:assignment:{id}");
    if let Some(receipt) = state
        .services
        .mfg
        .native_command_receipt_by_identity(
            &state.config_home,
            &idempotency_key,
            &actor_ref,
            action_id,
            &resource_ref,
        )
        .map_err(mfg_mutation_error)?
    {
        let replayed_assignment =
            serde_json::from_value::<MfgAssignment>(receipt.response_snapshot.clone())
                .unwrap_or(assignment);
        let receipt = deliver_assignment_notifications(
            &state,
            &replayed_assignment,
            &receipt,
            &idempotency_key,
        )
        .await?
        .canonical_receipt()
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.message))?;
        return Ok(Json(serde_json::json!({
            "kind": "mfg.assignment_command_receipt",
            "assignment": replayed_assignment,
            "business_receipt": receipt
        })));
    }
    let completion_resume = request.command == MfgAssignmentCommand::Complete
        && assignment.status == "completion_pending"
        && assignment.lifecycle_correlation_id.as_deref() == Some(correlation_id.as_str());
    if !completion_resume && request.expected_revision != assignment.revision {
        return Err(mfg_typed_api_error(assignment_revision_conflict(
            &assignment,
            request.expected_revision,
        )));
    }
    let mut command_expected_revision = request.expected_revision;
    let completion_evidence = if request.command == MfgAssignmentCommand::Complete {
        let terminal = probe_assignment_completion_terminal(&state, &assignment)
            .await
            .map_err(mfg_typed_api_error)?;
        if completion_resume {
            command_expected_revision = assignment.revision;
        } else {
            let reserved = state
                .services
                .mfg
                .reserve_assignment_completion(
                    &state.config_home,
                    &id,
                    request.expected_revision,
                    &actor_ref,
                    &correlation_id,
                )
                .map_err(mfg_mutation_error)?;
            command_expected_revision = reserved.revision;
        }
        Some(
            record_assignment_completion_evidence(&state, &assignment, &correlation_id, terminal)
                .map_err(mfg_typed_api_error)?,
        )
    } else {
        None
    };
    let (assignment, receipt) = state
        .services
        .mfg
        .command_assignment(
            &state.config_home,
            &id,
            MfgAssignmentCommandInput {
                command: request.command,
                actor_ref,
                expected_revision: command_expected_revision,
                idempotency_key: idempotency_key.clone(),
                target_ref: request.target_ref,
                reason: request.reason,
                correlation_id,
                completion_evidence,
            },
        )
        .map_err(mfg_mutation_error)?;
    // Reconcile every target on replay as well. SurfaceSendRequest uses one
    // deterministic key per target, so a completed delivery is replayed from
    // the durable Surface outbox while a missing/partial delivery is resumed.
    let receipt =
        deliver_assignment_notifications(&state, &assignment, &receipt, &idempotency_key).await?;
    let receipt = receipt
        .canonical_receipt()
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.message))?;
    Ok(Json(
        serde_json::json!({ "kind": "mfg.assignment_command_receipt", "assignment": assignment, "business_receipt": receipt }),
    ))
}

fn assignment_command_action_id(command: &MfgAssignmentCommand) -> &'static str {
    match command {
        MfgAssignmentCommand::Assign => "mfg.assignment.assign",
        MfgAssignmentCommand::Claim => "mfg.assignment.claim",
        MfgAssignmentCommand::Transfer => "mfg.assignment.transfer",
        MfgAssignmentCommand::Unassign => "mfg.assignment.unassign",
        MfgAssignmentCommand::Watch => "mfg.assignment.watch",
        MfgAssignmentCommand::RequestUpdate => "mfg.assignment.request_update",
        MfgAssignmentCommand::Escalate => "mfg.assignment.escalate",
        MfgAssignmentCommand::Start => "mfg.assignment.start",
        MfgAssignmentCommand::Complete => "mfg.assignment.complete",
    }
}

struct AssignmentCompletionTerminal {
    task_id: String,
    workflow_node_id: Option<String>,
    terminal_status: String,
    source_receipt_ref: String,
}

async fn probe_assignment_completion_terminal(
    state: &AppState,
    assignment: &MfgAssignment,
) -> Result<AssignmentCompletionTerminal, app_mfg_contract::MfgApiErrorV1> {
    let task_id = assignment
        .task_ref
        .trim()
        .strip_prefix("task://")
        .or_else(|| assignment.task_ref.trim().strip_prefix("task:"))
        .unwrap_or(assignment.task_ref.trim());
    let task = state
        .services
        .task
        .list_records()
        .map_err(|message| assignment_completion_internal_error(message))?
        .into_iter()
        .find(|task| task.id == task_id)
        .ok_or_else(|| {
            app_mfg_contract::MfgApiErrorV1::assignment_task_transition_required(
                assignment.task_ref.clone(),
                assignment.workflow_node_id.clone(),
            )
        })?;

    if let Some(node_id) = assignment.workflow_node_id.as_deref() {
        let projection = state
            .services
            .task
            .execution_graph(&task.id)
            .await
            .map_err(assignment_completion_internal_error)?
            .ok_or_else(|| {
                app_mfg_contract::MfgApiErrorV1::assignment_task_transition_required(
                    assignment.task_ref.clone(),
                    assignment.workflow_node_id.clone(),
                )
            })?;
        let node = projection
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .ok_or_else(|| {
                app_mfg_contract::MfgApiErrorV1::assignment_task_transition_required(
                    assignment.task_ref.clone(),
                    assignment.workflow_node_id.clone(),
                )
            })?;
        if !node.status.is_terminal() {
            return Err(
                app_mfg_contract::MfgApiErrorV1::assignment_task_transition_required(
                    assignment.task_ref.clone(),
                    assignment.workflow_node_id.clone(),
                ),
            );
        }
        let terminal_status = format!("{:?}", node.status).to_ascii_lowercase();
        let source_receipt_ref = node.result_ref.clone().unwrap_or_else(|| {
            format!(
                "execution://{}/nodes/{}?revision={}",
                projection.graph_id, node_id, projection.revision
            )
        });
        return Ok(AssignmentCompletionTerminal {
            task_id: task.id,
            workflow_node_id: Some(node_id.to_string()),
            terminal_status,
            source_receipt_ref,
        });
    }

    let completion_receipt = state
        .services
        .task
        .latest_terminal_runtime_receipt(&task.id)
        .map_err(assignment_completion_internal_error)?;
    if let Some(receipt) = completion_receipt {
        let terminal_status = receipt
            .payload
            .get("status")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                assignment_completion_internal_error(
                    "canonical Runtime terminal receipt omitted its terminal status",
                )
            })?;
        let source_receipt_ref = format!(
            "runtime-event://{}?cursor={}&transaction={}",
            receipt.event_id, receipt.commit_cursor, receipt.transaction_id
        );
        return Ok(AssignmentCompletionTerminal {
            task_id: task.id,
            workflow_node_id: None,
            terminal_status: terminal_status.to_string(),
            source_receipt_ref,
        });
    }
    if matches!(
        task.status,
        crate::task_kernel::TaskStatus::Completed
            | crate::task_kernel::TaskStatus::Blocked
            | crate::task_kernel::TaskStatus::Cancelled
            | crate::task_kernel::TaskStatus::Failed
    ) {
        let source_receipt_ref = format!("task://{}?status={}", task.id, task.status.as_str());
        return Ok(AssignmentCompletionTerminal {
            task_id: task.id,
            workflow_node_id: None,
            terminal_status: task.status.as_str().to_string(),
            source_receipt_ref,
        });
    }
    Err(
        app_mfg_contract::MfgApiErrorV1::assignment_task_transition_required(
            assignment.task_ref.clone(),
            assignment.workflow_node_id.clone(),
        ),
    )
}

fn record_assignment_completion_evidence(
    state: &AppState,
    assignment: &MfgAssignment,
    correlation_id: &str,
    terminal: AssignmentCompletionTerminal,
) -> Result<app_mfg_contract::MfgAssignmentCompletionEvidenceV1, app_mfg_contract::MfgApiErrorV1> {
    let observation = state
        .services
        .task
        .record_assignment_terminal_observation(
            &terminal.task_id,
            &terminal.terminal_status,
            &terminal.source_receipt_ref,
            correlation_id,
        )
        .map_err(assignment_completion_internal_error)?;
    Ok(app_mfg_contract::MfgAssignmentCompletionEvidenceV1 {
        correlation_id: correlation_id.to_string(),
        owner_kind: "runtime_assignment_terminal_observation".to_string(),
        task_ref: assignment.task_ref.clone(),
        workflow_node_id: terminal.workflow_node_id,
        terminal_status: terminal.terminal_status,
        receipt_ref: format!(
            "runtime-event://{}?cursor={}&transaction={}",
            observation.event_id, observation.commit_cursor, observation.transaction_id
        ),
    })
}

fn assignment_revision_conflict(
    assignment: &MfgAssignment,
    expected_revision: u64,
) -> app_mfg_contract::MfgApiErrorV1 {
    app_mfg_contract::MfgApiErrorV1 {
        code: app_mfg_contract::MfgErrorCode::RevisionConflict,
        message: format!(
            "assignment revision conflict: expected {expected_revision}, actual {}",
            assignment.revision
        ),
        http_status: 409,
        details: serde_json::json!({
            "assignment_id": assignment.assignment_id,
            "expected_revision": expected_revision,
            "actual_revision": assignment.revision,
        }),
        retryable: false,
        contract_version: app_mfg_contract::MfgContractVersion::default(),
        recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
            kind: app_mfg_contract::MfgRecoveryActionKind::Compare,
            label: "Refresh and compare the assignment".to_string(),
            target: Some(format!("/mfg/assignments/{}", assignment.assignment_id)),
            enabled: true,
        }],
        request_id: None,
        receipt_ref: None,
    }
}

fn assignment_completion_internal_error(
    message: impl Into<String>,
) -> app_mfg_contract::MfgApiErrorV1 {
    app_mfg_contract::MfgApiErrorV1 {
        code: app_mfg_contract::MfgErrorCode::Internal,
        message: message.into(),
        http_status: 503,
        details: serde_json::Value::Null,
        retryable: true,
        contract_version: app_mfg_contract::MfgContractVersion::default(),
        recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
            kind: app_mfg_contract::MfgRecoveryActionKind::RetrySameIntent,
            label: "Retry the same assignment intent".to_string(),
            target: None,
            enabled: true,
        }],
        request_id: None,
        receipt_ref: None,
    }
}

fn assignment_visible_to(assignment: &MfgAssignment, principal: &AuthenticatedPrincipal) -> bool {
    let actor = principal_actor_id(principal);
    if assignment.created_by == actor
        || assignment.assignee_ref == actor
        || assignment.watcher_refs.contains(&actor)
    {
        return true;
    }
    match assignment.visibility.as_str() {
        "public" => true,
        "team"
            if matches!(
                assignment.assignee_kind.as_str(),
                "team" | "role" | "organization"
            ) =>
        {
            let qualified = format!("{}:{}", assignment.assignee_kind, assignment.assignee_ref);
            principal
                .0
                .claims()
                .scopes
                .iter()
                .any(|scope| scope == &assignment.assignee_ref || scope == &qualified)
        }
        _ => false,
    }
}

async fn deliver_assignment_notifications(
    state: &Arc<AppState>,
    assignment: &MfgAssignment,
    receipt: &app_mfg::MfgCommandReceipt,
    idempotency_key: &str,
) -> Result<app_mfg::MfgCommandReceipt, (StatusCode, Json<ErrorResponse>)> {
    let mut notification_refs = Vec::new();
    for (index, target) in assignment.notification_targets.iter().enumerate() {
        let delivery_key = format!("{idempotency_key}:surface:{index}");
        let result = state.services.surface.send(SurfaceSendRequest {
            surface: target.surface.clone(), recipient: target.recipient.clone(), thread: target.thread.clone(),
            text: format!("MFG assignment {} is {} (revision {})", assignment.assignment_id, assignment.status, assignment.revision),
            idempotency_key: Some(delivery_key.clone()),
            metadata: serde_json::json!({ "kind": "mfg.assignment_notification", "assignment_ref": format!("mfg:assignment:{}", assignment.assignment_id), "receipt_id": receipt.receipt_id, "audit_ref": receipt.audit_ref }),
        }).await;
        // The Surface outbox owns the stable delivery identity even when the
        // provider call fails. Never persist a result kind or a synthetic
        // "failed" path as if it were a resolvable object.
        if let Some(delivery) = state
            .services
            .surface
            .all_outbox()
            .into_iter()
            .find(|delivery| delivery.idempotency_key == delivery_key)
        {
            notification_refs.push(format!(
                "surface://{}/delivery/{}",
                delivery.surface, delivery.delivery_id
            ));
        } else if let Err(error) = result {
            tracing::warn!(
                assignment_id = %assignment.assignment_id,
                surface = %target.surface,
                error = %error,
                "assignment notification failed before a durable Surface delivery was queued"
            );
        }
    }
    state
        .services
        .mfg
        .record_command_notifications(&state.config_home, idempotency_key, notification_refs)
        .map_err(mfg_mutation_error)
}

const MFG_LIVE_OBSERVER_QUEUE_CAPACITY: usize = 64;
const MFG_LIVE_OBSERVER_EVENT_CAPACITY: usize = 512;

enum MfgLiveQueuedItem {
    Envelope(app_mfg_contract::MfgLiveEnvelopeV1),
    Error(app_mfg_contract::MfgApiErrorV1),
}

struct MfgLiveObserverQueue {
    connection_id: String,
    observer_id: String,
    surface_id: String,
    items: tokio::sync::Mutex<std::collections::VecDeque<MfgLiveQueuedItem>>,
    notify: tokio::sync::Notify,
    receiver_closed: std::sync::atomic::AtomicBool,
    producer_done: std::sync::atomic::AtomicBool,
    peak: std::sync::atomic::AtomicUsize,
    peak_events: std::sync::atomic::AtomicUsize,
    coalesced: std::sync::atomic::AtomicUsize,
}

impl Drop for MfgLiveObserverQueue {
    fn drop(&mut self) {
        tracing::info!(
            connection_id = %self.connection_id,
            observer_id = %self.observer_id,
            surface_id = %self.surface_id,
            queue_capacity = MFG_LIVE_OBSERVER_QUEUE_CAPACITY,
            event_capacity = MFG_LIVE_OBSERVER_EVENT_CAPACITY,
            queue_peak = self.peak.load(std::sync::atomic::Ordering::Relaxed),
            event_peak = self.peak_events.load(std::sync::atomic::Ordering::Relaxed),
            coalesced = self.coalesced.load(std::sync::atomic::Ordering::Relaxed),
            receiver_closed = self
                .receiver_closed
                .load(std::sync::atomic::Ordering::Relaxed),
            producer_done = self
                .producer_done
                .load(std::sync::atomic::Ordering::Relaxed),
            "MFG live observer telemetry"
        );
    }
}

impl MfgLiveObserverQueue {
    fn new(observer_id: impl Into<String>, surface_id: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            connection_id: uuid::Uuid::new_v4().to_string(),
            observer_id: observer_id.into(),
            surface_id: surface_id.into(),
            items: tokio::sync::Mutex::new(std::collections::VecDeque::new()),
            notify: tokio::sync::Notify::new(),
            receiver_closed: std::sync::atomic::AtomicBool::new(false),
            producer_done: std::sync::atomic::AtomicBool::new(false),
            peak: std::sync::atomic::AtomicUsize::new(0),
            peak_events: std::sync::atomic::AtomicUsize::new(0),
            coalesced: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    async fn push_envelope(&self, envelope: app_mfg_contract::MfgLiveEnvelopeV1) -> bool {
        let mut items = self.items.lock().await;
        if let Some(MfgLiveQueuedItem::Envelope(existing)) = items.back_mut() {
            match merge_mfg_live_envelope(existing, &envelope) {
                MfgLiveMergeResult::Merged => {
                    self.coalesced
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.peak_events.fetch_max(
                        queued_mfg_live_event_count(&items),
                        std::sync::atomic::Ordering::Relaxed,
                    );
                    return true;
                }
                MfgLiveMergeResult::CapacityExceeded => return false,
                MfgLiveMergeResult::NotMergeable => {}
            }
        }
        if items.len() >= MFG_LIVE_OBSERVER_QUEUE_CAPACITY {
            return false;
        }
        items.push_back(MfgLiveQueuedItem::Envelope(envelope));
        self.peak
            .fetch_max(items.len(), std::sync::atomic::Ordering::Relaxed);
        self.peak_events.fetch_max(
            queued_mfg_live_event_count(&items),
            std::sync::atomic::Ordering::Relaxed,
        );
        drop(items);
        self.notify.notify_one();
        true
    }

    async fn finish_with(&self, item: MfgLiveQueuedItem) {
        let mut items = self.items.lock().await;
        items.clear();
        items.push_back(item);
        self.producer_done
            .store(true, std::sync::atomic::Ordering::Release);
        drop(items);
        self.notify.notify_waiters();
    }

    async fn pop(&self) -> Option<MfgLiveQueuedItem> {
        loop {
            let notified = self.notify.notified();
            if let Some(item) = self.items.lock().await.pop_front() {
                return Some(item);
            }
            if self
                .producer_done
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return None;
            }
            notified.await;
        }
    }

    fn close_receiver(&self) {
        self.receiver_closed
            .store(true, std::sync::atomic::Ordering::Release);
        self.notify.notify_waiters();
    }

    fn receiver_is_closed(&self) -> bool {
        self.receiver_closed
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MfgLiveMergeResult {
    Merged,
    NotMergeable,
    CapacityExceeded,
}

fn merge_mfg_live_envelope(
    existing: &mut app_mfg_contract::MfgLiveEnvelopeV1,
    incoming: &app_mfg_contract::MfgLiveEnvelopeV1,
) -> MfgLiveMergeResult {
    match (existing, incoming) {
        (
            app_mfg_contract::MfgLiveEnvelopeV1::Delta(existing),
            app_mfg_contract::MfgLiveEnvelopeV1::Delta(incoming),
        ) if existing.view_epoch == incoming.view_epoch
            && existing.target_cursor == incoming.base_cursor =>
        {
            let mut merged = existing.events.clone();
            for event in &incoming.events {
                if app_mfg_contract::mfg_live_event_priority(&event.event_type, &event.payload) >= 2
                {
                    merged.retain(|queued| {
                        queued.event_type != event.event_type
                            || queued.subject_ref != event.subject_ref
                    });
                }
                merged.push(event.clone());
                if merged.len() > MFG_LIVE_OBSERVER_EVENT_CAPACITY {
                    return MfgLiveMergeResult::CapacityExceeded;
                }
            }
            existing.events = merged;
            existing.target_cursor.clone_from(&incoming.target_cursor);
            MfgLiveMergeResult::Merged
        }
        (
            app_mfg_contract::MfgLiveEnvelopeV1::Heartbeat(existing),
            app_mfg_contract::MfgLiveEnvelopeV1::Heartbeat(incoming),
        ) if existing.view_epoch == incoming.view_epoch => {
            *existing = incoming.clone();
            MfgLiveMergeResult::Merged
        }
        _ => MfgLiveMergeResult::NotMergeable,
    }
}

fn queued_mfg_live_event_count(items: &std::collections::VecDeque<MfgLiveQueuedItem>) -> usize {
    items
        .iter()
        .map(|item| match item {
            MfgLiveQueuedItem::Envelope(app_mfg_contract::MfgLiveEnvelopeV1::Delta(delta)) => {
                delta.events.len()
            }
            MfgLiveQueuedItem::Envelope(_) | MfgLiveQueuedItem::Error(_) => 1,
        })
        .sum()
}

struct MfgLiveObserverReceiver {
    queue: Arc<MfgLiveObserverQueue>,
    close_after_emit: bool,
}

struct MfgLiveProducerGuard(Arc<MfgLiveObserverQueue>);

impl Drop for MfgLiveProducerGuard {
    fn drop(&mut self) {
        self.0
            .producer_done
            .store(true, std::sync::atomic::Ordering::Release);
        self.0.notify.notify_waiters();
    }
}

impl Drop for MfgLiveObserverReceiver {
    fn drop(&mut self) {
        self.queue.close_receiver();
    }
}

pub(super) async fn mfg_live_snapshot_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let principal = mfg_live_principal_context(&principal);
    if state.auth_token.is_some() {
        if let Some(error) = state
            .services
            .mfg
            .live_authorization_error_async(state.config_home.clone(), principal.clone())
            .await
        {
            return Err(mfg_typed_api_error(error));
        }
    }
    state
        .services
        .mfg
        .live_snapshot_envelope_async(state.config_home.clone(), principal)
        .await
        .map(Json)
        .map_err(mfg_live_service_error)
}

pub(super) async fn mfg_live_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgLiveQuery>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let surface_id = headers
        .get("x-cowd-surface-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown")
        .chars()
        .take(64)
        .collect::<String>();
    let observer_id = headers
        .get("x-cowd-observer-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(128).collect::<String>())
        .unwrap_or_else(|| format!("{surface_id}:{}", uuid::Uuid::new_v4()));
    let principal = mfg_live_principal_context(&principal);
    let validate_authorization = state.auth_token.is_some();
    if validate_authorization {
        if let Some(error) = state
            .services
            .mfg
            .live_authorization_error_async(state.config_home.clone(), principal.clone())
            .await
        {
            return Err(mfg_typed_api_error(error));
        }
    }
    let supplied_cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let supplied_epoch = headers
        .get("x-mfg-view-epoch")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let pending = if let Some(cursor) = supplied_cursor.as_deref() {
        state
            .services
            .mfg
            .live_delta_envelope_async(
                state.config_home.clone(),
                principal.clone(),
                supplied_epoch.clone().unwrap_or_default(),
                cursor.to_string(),
                limit,
            )
            .await
            .map_err(mfg_live_service_error)?
    } else {
        Some(
            state
                .services
                .mfg
                .live_snapshot_envelope_async(state.config_home.clone(), principal.clone())
                .await
                .map_err(mfg_live_service_error)?,
        )
    };
    let (cursor, view_epoch) = if let Some(seed) = pending.as_ref() {
        mfg_live_envelope_position(seed)
    } else {
        state
            .services
            .mfg
            .live_heartbeat_envelope_async(
                state.config_home.clone(),
                principal.clone(),
                supplied_cursor.clone().unwrap_or_default(),
            )
            .await
            .map_err(mfg_live_service_error)?;
        (
            supplied_cursor.clone().unwrap_or_default(),
            supplied_epoch.clone().unwrap_or_default(),
        )
    };
    let queue = MfgLiveObserverQueue::new(observer_id, surface_id);
    tokio::spawn(run_mfg_live_observer_producer(
        Arc::clone(&state),
        principal,
        cursor,
        view_epoch,
        limit,
        pending,
        validate_authorization,
        Arc::clone(&queue),
    ));
    let stream = futures::stream::unfold(
        MfgLiveObserverReceiver {
            queue,
            close_after_emit: false,
        },
        |mut receiver| async move {
            if receiver.close_after_emit {
                return None;
            }
            let item = receiver.queue.pop().await?;
            let (event, terminal) = match item {
                MfgLiveQueuedItem::Envelope(envelope) => {
                    let terminal =
                        matches!(envelope, app_mfg_contract::MfgLiveEnvelopeV1::Resync(_));
                    let (cursor, _) = mfg_live_envelope_position(&envelope);
                    let event = Event::default()
                        .id(cursor)
                        .event("mfg_live")
                        .json_data(envelope)
                        .unwrap_or_else(|_| Event::default().event("mfg_live_error"));
                    (event, terminal)
                }
                MfgLiveQueuedItem::Error(error) => {
                    let event = Event::default()
                        .event("mfg_live_error")
                        .json_data(error)
                        .unwrap_or_else(|_| Event::default().event("mfg_live_error"));
                    (event, true)
                }
            };
            receiver.close_after_emit = terminal;
            Some((Ok::<Event, Infallible>(event), receiver))
        },
    );
    Ok(Sse::new(stream)
        .keep_alive(
            axum::response::sse::KeepAlive::new()
                .interval(std::time::Duration::from_secs(15))
                .text("keepalive"),
        )
        .into_response())
}

#[allow(clippy::too_many_arguments)]
async fn run_mfg_live_observer_producer(
    app: Arc<AppState>,
    principal: crate::services::MfgLivePrincipalContext,
    mut cursor: String,
    mut view_epoch: String,
    limit: usize,
    mut pending: Option<app_mfg_contract::MfgLiveEnvelopeV1>,
    validate_authorization: bool,
    queue: Arc<MfgLiveObserverQueue>,
) {
    let _producer_guard = MfgLiveProducerGuard(Arc::clone(&queue));
    let mut last_heartbeat = Instant::now();
    let mut last_authorization_check = Instant::now();
    let mut last_telemetry = (usize::MAX, usize::MAX, usize::MAX);
    loop {
        let telemetry = (
            queue.peak.load(std::sync::atomic::Ordering::Relaxed),
            queue.peak_events.load(std::sync::atomic::Ordering::Relaxed),
            queue.coalesced.load(std::sync::atomic::Ordering::Relaxed),
        );
        if telemetry != last_telemetry {
            tracing::info!(
                connection_id = %queue.connection_id,
                observer_id = %queue.observer_id,
                surface_id = %queue.surface_id,
                queue_capacity = MFG_LIVE_OBSERVER_QUEUE_CAPACITY,
                event_capacity = MFG_LIVE_OBSERVER_EVENT_CAPACITY,
                queue_peak = telemetry.0,
                event_peak = telemetry.1,
                coalesced = telemetry.2,
                "MFG live observer telemetry sample"
            );
            last_telemetry = telemetry;
        }
        if queue.receiver_is_closed() {
            tracing::debug!(
                connection_id = %queue.connection_id,
                observer_id = %queue.observer_id,
                surface_id = %queue.surface_id,
                queue_peak = queue.peak.load(std::sync::atomic::Ordering::Relaxed),
                event_peak = queue.peak_events.load(std::sync::atomic::Ordering::Relaxed),
                coalesced = queue.coalesced.load(std::sync::atomic::Ordering::Relaxed),
                "MFG live observer released"
            );
            return;
        }
        if validate_authorization && last_authorization_check.elapsed().as_secs() >= 2 {
            if let Some(error) = app
                .services
                .mfg
                .live_authorization_error_async(app.config_home.clone(), principal.clone())
                .await
            {
                queue.finish_with(MfgLiveQueuedItem::Error(error)).await;
                return;
            }
            last_authorization_check = Instant::now();
        }
        let envelope = if let Some(pending) = pending.take() {
            Some(pending)
        } else {
            match app
                .services
                .mfg
                .live_delta_envelope_async(
                    app.config_home.clone(),
                    principal.clone(),
                    view_epoch.clone(),
                    cursor.clone(),
                    limit,
                )
                .await
            {
                Ok(Some(envelope)) => Some(envelope),
                Ok(None) if last_heartbeat.elapsed().as_secs() >= 5 => {
                    match app
                        .services
                        .mfg
                        .live_heartbeat_envelope_async(
                            app.config_home.clone(),
                            principal.clone(),
                            cursor.clone(),
                        )
                        .await
                    {
                        Ok(envelope) => Some(envelope),
                        Err(error) => {
                            queue
                                .finish_with(MfgLiveQueuedItem::Error(
                                    mfg_live_service_contract_error(error),
                                ))
                                .await;
                            return;
                        }
                    }
                }
                Ok(None) => None,
                Err(error) => {
                    queue
                        .finish_with(MfgLiveQueuedItem::Error(mfg_live_service_contract_error(
                            error,
                        )))
                        .await;
                    return;
                }
            }
        };
        let Some(envelope) = envelope else {
            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            continue;
        };
        let terminal = matches!(envelope, app_mfg_contract::MfgLiveEnvelopeV1::Resync(_));
        let (next_cursor, next_epoch) = mfg_live_envelope_position(&envelope);
        if terminal {
            queue
                .finish_with(MfgLiveQueuedItem::Envelope(envelope))
                .await;
            return;
        }
        if !queue.push_envelope(envelope).await {
            let reason = "observer_queue_capacity_exceeded";
            match app
                .services
                .mfg
                .live_resync_envelope_async(
                    app.config_home.clone(),
                    principal.clone(),
                    view_epoch.clone(),
                    reason.to_string(),
                )
                .await
            {
                Ok(resync) => {
                    tracing::warn!(
                        connection_id = %queue.connection_id,
                        observer_id = %queue.observer_id,
                        surface_id = %queue.surface_id,
                        queue_capacity = MFG_LIVE_OBSERVER_QUEUE_CAPACITY,
                        event_capacity = MFG_LIVE_OBSERVER_EVENT_CAPACITY,
                        queue_peak = queue.peak.load(std::sync::atomic::Ordering::Relaxed),
                        event_peak = queue.peak_events.load(std::sync::atomic::Ordering::Relaxed),
                        coalesced = queue.coalesced.load(std::sync::atomic::Ordering::Relaxed),
                        reason,
                        "MFG live slow observer disconnected with typed resync"
                    );
                    queue.finish_with(MfgLiveQueuedItem::Envelope(resync)).await;
                }
                Err(error) => {
                    queue
                        .finish_with(MfgLiveQueuedItem::Error(mfg_live_service_contract_error(
                            error,
                        )))
                        .await;
                }
            }
            return;
        }
        cursor = next_cursor;
        view_epoch = next_epoch;
        last_heartbeat = Instant::now();
    }
}

fn mfg_live_principal_context(
    principal: &AuthenticatedPrincipal,
) -> crate::services::MfgLivePrincipalContext {
    crate::services::MfgLivePrincipalContext {
        principal_id: principal.0.claims().principal_id.clone(),
        profile_revision: principal.0.claims().profile_revision,
        scopes: principal.0.claims().scopes.clone(),
        capabilities: principal.0.claims().capabilities.clone(),
        credential_epoch: principal.0.claims().credential_epoch,
        expires_at_ms: principal.0.claims().expires_at_ms,
    }
}

fn mfg_live_envelope_position(envelope: &app_mfg_contract::MfgLiveEnvelopeV1) -> (String, String) {
    match envelope {
        app_mfg_contract::MfgLiveEnvelopeV1::Snapshot(snapshot) => {
            (snapshot.cursor.clone(), snapshot.view_epoch.clone())
        }
        app_mfg_contract::MfgLiveEnvelopeV1::Delta(delta) => {
            (delta.target_cursor.clone(), delta.view_epoch.clone())
        }
        app_mfg_contract::MfgLiveEnvelopeV1::Heartbeat(heartbeat) => {
            (heartbeat.cursor.clone(), heartbeat.view_epoch.clone())
        }
        app_mfg_contract::MfgLiveEnvelopeV1::Resync(resync) => {
            (resync.latest_cursor.clone(), String::new())
        }
    }
}

fn mfg_live_service_contract_error(
    error: crate::services::MfgLiveServiceError,
) -> app_mfg_contract::MfgApiErrorV1 {
    match error {
        crate::services::MfgLiveServiceError::InvalidCursorKey(message) => {
            app_mfg_contract::MfgApiErrorV1 {
                code: app_mfg_contract::MfgErrorCode::MfgLiveCursorKeyInvalid,
                message,
                http_status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                details: serde_json::json!({
                    "key_file": "mfg-live-cursor.key",
                    "required_mode": "0600",
                    "required_bytes": 32,
                }),
                retryable: false,
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
                    kind: app_mfg_contract::MfgRecoveryActionKind::RequestManualReview,
                    label: "Repair the live cursor key securely".to_string(),
                    target: None,
                    enabled: true,
                }],
                request_id: None,
                receipt_ref: None,
            }
        }
        crate::services::MfgLiveServiceError::CursorKeyIo(message) => {
            app_mfg_contract::MfgApiErrorV1 {
                code: app_mfg_contract::MfgErrorCode::Internal,
                message,
                http_status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                details: serde_json::Value::Null,
                retryable: true,
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
                    kind: app_mfg_contract::MfgRecoveryActionKind::RetrySameIntent,
                    label: "Reconnect MFG live updates".to_string(),
                    target: Some("/api/apps/mfg/live/snapshot".to_string()),
                    enabled: true,
                }],
                request_id: None,
                receipt_ref: None,
            }
        }
        crate::services::MfgLiveServiceError::Repository(error) => {
            app_mfg_contract::MfgApiErrorV1 {
                code: app_mfg_contract::MfgErrorCode::Internal,
                message: error.to_string(),
                http_status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                details: serde_json::Value::Null,
                retryable: true,
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                recovery_actions: Vec::new(),
                request_id: None,
                receipt_ref: None,
            }
        }
    }
}

fn mfg_live_service_error(
    error: crate::services::MfgLiveServiceError,
) -> (StatusCode, Json<ErrorResponse>) {
    match error {
        crate::services::MfgLiveServiceError::InvalidCursorKey(message) => {
            mfg_typed_api_error(app_mfg_contract::MfgApiErrorV1 {
                code: app_mfg_contract::MfgErrorCode::MfgLiveCursorKeyInvalid,
                message,
                http_status: StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                details: serde_json::json!({
                    "key_file": "mfg-live-cursor.key",
                    "required_mode": "0600",
                    "required_bytes": 32,
                }),
                retryable: false,
                contract_version: app_mfg_contract::MfgContractVersion::default(),
                recovery_actions: vec![app_mfg_contract::MfgRecoveryAction {
                    kind: app_mfg_contract::MfgRecoveryActionKind::RequestManualReview,
                    label: "Repair the live cursor key securely".to_string(),
                    target: None,
                    enabled: true,
                }],
                request_id: None,
                receipt_ref: None,
            })
        }
        crate::services::MfgLiveServiceError::CursorKeyIo(message) => {
            mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, message)
        }
        crate::services::MfgLiveServiceError::Repository(error) => {
            mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
        }
    }
}

#[cfg(test)]
mod live_observer_queue_tests {
    use super::*;

    fn event(event_type: &str, subject_ref: &str) -> app_mfg_contract::MfgLiveEventV1 {
        app_mfg_contract::MfgLiveEventV1 {
            event_type: event_type.to_string(),
            subject_ref: subject_ref.to_string(),
            revision: 1,
            occurred_at: chrono::Utc::now(),
            payload: serde_json::json!({"subject_ref": subject_ref}),
        }
    }

    fn delta(
        base: impl Into<String>,
        target: impl Into<String>,
        events: Vec<app_mfg_contract::MfgLiveEventV1>,
    ) -> app_mfg_contract::MfgLiveEnvelopeV1 {
        app_mfg_contract::MfgLiveEnvelopeV1::Delta(app_mfg_contract::MfgLiveDeltaV1 {
            view_epoch: "view-1".to_string(),
            base_cursor: base.into(),
            target_cursor: target.into(),
            events,
        })
    }

    fn resync(index: usize) -> app_mfg_contract::MfgLiveEnvelopeV1 {
        app_mfg_contract::MfgLiveEnvelopeV1::Resync(app_mfg_contract::MfgLiveResyncV1 {
            previous_view_epoch: "view-1".to_string(),
            reason: "test".to_string(),
            snapshot_url: "/api/apps/mfg/live/snapshot".to_string(),
            latest_cursor: format!("cursor-{index}"),
        })
    }

    #[test]
    fn coalescing_preserves_p0_and_p1_and_replaces_only_same_subject_domain_events() {
        let mut queued = delta(
            "cursor-0",
            "cursor-1",
            vec![
                event("report_review.decided", "review:1"),
                event("receipt.recorded", "receipt:1"),
                event("entity.updated", "entity:1"),
            ],
        );
        let incoming = delta(
            "cursor-1",
            "cursor-2",
            vec![
                event("entity.updated", "entity:1"),
                event("metric.snapshot", "metric:1"),
            ],
        );
        assert_eq!(
            merge_mfg_live_envelope(&mut queued, &incoming),
            MfgLiveMergeResult::Merged
        );
        let app_mfg_contract::MfgLiveEnvelopeV1::Delta(queued) = queued else {
            panic!("delta")
        };
        assert_eq!(queued.target_cursor, "cursor-2");
        assert_eq!(
            queued
                .events
                .iter()
                .filter(|event| event.event_type == "report_review.decided")
                .count(),
            1
        );
        assert_eq!(
            queued
                .events
                .iter()
                .filter(|event| event.event_type == "receipt.recorded")
                .count(),
            1
        );
        assert_eq!(
            queued
                .events
                .iter()
                .filter(|event| {
                    event.event_type == "entity.updated" && event.subject_ref == "entity:1"
                })
                .count(),
            1
        );
    }

    #[test]
    fn merged_delta_has_a_hard_event_bound_and_is_not_partially_mutated_on_overflow() {
        let events = (0..MFG_LIVE_OBSERVER_EVENT_CAPACITY)
            .map(|index| event("entity.updated", &format!("entity:{index}")))
            .collect::<Vec<_>>();
        let mut queued = delta("cursor-0", "cursor-1", events);
        let original = queued.clone();
        let incoming = delta(
            "cursor-1",
            "cursor-2",
            vec![event("entity.updated", "entity:overflow")],
        );
        assert_eq!(
            merge_mfg_live_envelope(&mut queued, &incoming),
            MfgLiveMergeResult::CapacityExceeded
        );
        assert_eq!(queued, original);
    }

    #[tokio::test]
    async fn observer_queue_is_bounded_and_closes_only_the_slow_receiver() {
        let queue = MfgLiveObserverQueue::new("test-slow", "test");
        for index in 0..MFG_LIVE_OBSERVER_QUEUE_CAPACITY {
            assert!(queue.push_envelope(resync(index)).await);
        }
        assert!(
            !queue
                .push_envelope(resync(MFG_LIVE_OBSERVER_QUEUE_CAPACITY))
                .await
        );
        assert_eq!(
            queue.peak.load(std::sync::atomic::Ordering::Relaxed),
            MFG_LIVE_OBSERVER_QUEUE_CAPACITY
        );
        assert!(!queue.receiver_is_closed());
        queue.close_receiver();
        assert!(queue.receiver_is_closed());

        let independent = MfgLiveObserverQueue::new("test-independent", "test");
        assert!(independent.push_envelope(resync(1)).await);
        assert!(!independent.receiver_is_closed());
    }

    #[tokio::test]
    async fn producer_drop_wakes_the_receiver_instead_of_leaving_a_zombie_stream() {
        let queue = MfgLiveObserverQueue::new("test-producer", "test");
        {
            let _guard = MfgLiveProducerGuard(Arc::clone(&queue));
        }
        assert!(queue.pop().await.is_none());
    }
}
