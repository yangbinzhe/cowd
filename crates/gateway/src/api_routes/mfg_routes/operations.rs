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
        serde_json::json!({ "kind": "mfg.alert_rule", "rule": rule, "receipt": receipt }),
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
        serde_json::json!({ "kind": "mfg.alert_subscription", "subscription": subscription, "receipt": receipt }),
    ))
}

pub(super) async fn mfg_alert_command_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    headers: HeaderMap,
    Json(request): Json<MfgAlertCommandRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
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
        serde_json::json!({ "kind": "mfg.alert_command_receipt", "occurrence": occurrence, "receipt": receipt }),
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
        serde_json::json!({ "kind": "mfg.assignment", "assignment": assignment, "receipt": receipt }),
    ))
}

pub(super) async fn mfg_assignment_list_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Extension(principal): Extension<AuthenticatedPrincipal>,
    Query(query): Query<MfgAssignmentListQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let items = state
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
    Ok(Json(
        serde_json::json!({ "kind": "mfg.assignment", "assignment": assignment }),
    ))
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
    let (assignment, receipt) = state
        .services
        .mfg
        .command_assignment(
            &state.config_home,
            &id,
            MfgAssignmentCommandInput {
                command: request.command,
                actor_ref: principal_actor_id(&principal),
                expected_revision: request.expected_revision,
                idempotency_key: idempotency_key.clone(),
                target_ref: request.target_ref,
                reason: request.reason,
            },
        )
        .map_err(mfg_mutation_error)?;
    let receipt = if receipt.idempotent_replay {
        receipt
    } else {
        deliver_assignment_notifications(&state, &assignment, &receipt, &idempotency_key).await?
    };
    let receipt = receipt
        .canonical_receipt()
        .map_err(|error| mfg_api_error(StatusCode::INTERNAL_SERVER_ERROR, error.message))?;
    Ok(Json(
        serde_json::json!({ "kind": "mfg.assignment_command_receipt", "assignment": assignment, "receipt": receipt }),
    ))
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
        let result = state.services.surface.send(SurfaceSendRequest {
            surface: target.surface.clone(), recipient: target.recipient.clone(), thread: target.thread.clone(),
            text: format!("MFG assignment {} is {} (revision {})", assignment.assignment_id, assignment.status, assignment.revision),
            idempotency_key: Some(format!("{idempotency_key}:surface:{index}")),
            metadata: serde_json::json!({ "kind": "mfg.assignment_notification", "assignment_ref": format!("mfg:assignment:{}", assignment.assignment_id), "receipt_id": receipt.receipt_id, "audit_ref": receipt.audit_ref }),
        }).await;
        match result {
            Ok(result) => {
                let message_ref = result
                    .message_id
                    .clone()
                    .unwrap_or_else(|| result.kind.clone());
                notification_refs.push(format!(
                    "surface://{}/{}?status={}",
                    result.surface, message_ref, result.status
                ));
            }
            Err(error) => notification_refs.push(format!(
                "surface://{}/failed?reason={}",
                target.surface, error
            )),
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
