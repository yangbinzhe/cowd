use std::convert::Infallible;

use axum::{
    extract::Extension,
    http::{header, HeaderMap},
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
    cursor: Option<u64>,
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

pub(super) async fn mfg_live_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(_principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgLiveQuery>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .or(query.cursor);
    let limit = query.limit.unwrap_or(100).clamp(1, 500);
    let wants_sse = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.contains("text/event-stream"));
    if !wants_sse {
        let projection = state
            .services
            .mfg
            .live_projection(&state.config_home, cursor, limit)
            .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        return Ok(Json(projection).into_response());
    }
    let stream = futures::stream::unfold(
        (state, cursor, limit),
        |(state, mut cursor, limit)| async move {
            loop {
                match state
                    .services
                    .mfg
                    .live_projection(&state.config_home, cursor, limit)
                {
                    Ok(projection)
                        if projection.kind == "snapshot"
                            || projection.kind == "resync"
                            || !projection.events.is_empty() =>
                    {
                        cursor = Some(projection.cursor);
                        let event_type = match projection.kind.as_str() {
                            "snapshot" => "mfg_snapshot",
                            "resync" => "mfg_resync",
                            _ => "mfg_delta",
                        };
                        let event = Event::default()
                            .id(projection.cursor.to_string())
                            .event(event_type)
                            .json_data(projection)
                            .unwrap_or_else(|_| Event::default().event("mfg_projection_error"));
                        return Some((Ok::<Event, Infallible>(event), (state, cursor, limit)));
                    }
                    Ok(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
                    Err(error) => {
                        let event = Event::default().event("mfg_resync").data(serde_json::json!({ "reason": error.to_string(), "snapshot_url": "/api/apps/mfg/live" }).to_string());
                        return Some((Ok(event), (state, None, limit)));
                    }
                }
            }
        },
    );
    Ok(Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::default())
        .into_response())
}
