use std::{collections::BTreeSet, sync::OnceLock};

use serde::Serialize;
use serde_json::{json, Map, Value};

use super::{
    route_manifest::{
        gateway_route_manifest, gateway_route_manifest_for_apps, GatewayRouteManifestEntry,
    },
    route_registry::{stable_route_metadata, SessionWriterPolicy, StableRouteMetadata},
};

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayCapabilityContract {
    kind: &'static str,
    schema_version: u32,
    owner: &'static str,
    source: &'static str,
    route_count: usize,
    capability_count: usize,
    coverage: GatewayCapabilityCoverage,
    capabilities: Vec<GatewayCapability>,
}

#[derive(Debug, Clone, Serialize)]
struct GatewayCapabilityCoverage {
    route_count: usize,
    capability_count: usize,
    p1_count: usize,
    webui_required_count: usize,
    tui_required_count: usize,
    ai_tool_count: usize,
    openapi_path_count: usize,
    route_contract_parity: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayCapability {
    id: String,
    domain: String,
    title: String,
    description: String,
    http: GatewayCapabilityHttp,
    auth: String,
    risk: String,
    side_effects: Vec<String>,
    idempotency: String,
    streaming: String,
    availability: GatewayCapabilityAvailability,
    discoverability: GatewayCapabilityDiscoverability,
    consumed_by: Vec<String>,
    verified_by: Vec<String>,
    ai_affordance: GatewayCapabilityAiAffordance,
    input_schema: Value,
    output_schema: Value,
    tests: Vec<String>,
    #[serde(skip)]
    app: Option<super::route_manifest::GatewayAppSemanticMetadata>,
}

#[derive(Debug, Clone, Serialize)]
struct GatewayCapabilityHttp {
    method: String,
    path: String,
    handler: String,
    source: String,
    stability: String,
    criticality: String,
}

#[derive(Debug, Clone, Serialize)]
struct GatewayCapabilityAvailability {
    available: bool,
    executable: bool,
}

#[derive(Debug, Clone, Serialize)]
struct GatewayCapabilityDiscoverability {
    http: bool,
    openapi: bool,
    ai_tool: bool,
}

#[derive(Debug, Clone, Serialize)]
struct GatewayCapabilityAiAffordance {
    expose_as_tool: bool,
    tool_name: Option<String>,
    when_to_use: String,
    cautions: Vec<String>,
}

pub(crate) fn gateway_capability_contract() -> GatewayCapabilityContract {
    gateway_capability_contract_from_routes(gateway_route_manifest())
}

pub(crate) fn gateway_capability_contract_for_apps() -> GatewayCapabilityContract {
    static CONTRACT: OnceLock<GatewayCapabilityContract> = OnceLock::new();
    CONTRACT
        .get_or_init(gateway_capability_contract_for_apps_uncached)
        .clone()
}

fn gateway_capability_contract_for_apps_uncached() -> GatewayCapabilityContract {
    gateway_capability_contract_from_routes(gateway_route_manifest_for_apps())
}

fn gateway_capability_contract_from_routes(
    routes: Vec<GatewayRouteManifestEntry>,
) -> GatewayCapabilityContract {
    let capabilities = routes.iter().map(route_capability).collect::<Vec<_>>();
    let openapi_path_count = capabilities
        .iter()
        .map(|capability| openapi_path(&capability.http.path))
        .collect::<BTreeSet<_>>()
        .len();
    let coverage = GatewayCapabilityCoverage {
        route_count: routes.len(),
        capability_count: capabilities.len(),
        p1_count: capabilities
            .iter()
            .filter(|capability| capability.http.criticality == "p1")
            .count(),
        webui_required_count: capabilities
            .iter()
            .filter(|capability| capability.consumed_by.iter().any(|item| item == "webui"))
            .count(),
        tui_required_count: capabilities
            .iter()
            .filter(|capability| capability.consumed_by.iter().any(|item| item == "tui"))
            .count(),
        ai_tool_count: capabilities
            .iter()
            .filter(|capability| capability.discoverability.ai_tool)
            .count(),
        openapi_path_count,
        route_contract_parity: routes.len() == capabilities.len(),
    };

    GatewayCapabilityContract {
        kind: "gateway.capability_contract",
        schema_version: 2,
        owner: "gateway",
        source: "crates/gateway/src/api_routes/capability_contract.rs",
        route_count: routes.len(),
        capability_count: capabilities.len(),
        coverage,
        capabilities,
    }
}

pub(crate) fn gateway_openapi_document() -> Value {
    static DOCUMENT: OnceLock<Value> = OnceLock::new();
    DOCUMENT
        .get_or_init(|| {
            gateway_openapi_document_from_contract(gateway_capability_contract(), Map::new())
        })
        .clone()
}

pub(crate) fn gateway_openapi_document_for_apps() -> Value {
    static DOCUMENT: OnceLock<Value> = OnceLock::new();
    DOCUMENT
        .get_or_init(gateway_openapi_document_for_apps_uncached)
        .clone()
}

fn gateway_openapi_document_for_apps_uncached() -> Value {
    gateway_openapi_document_from_contract(
        gateway_capability_contract_for_apps_uncached(),
        Map::new(),
    )
}

pub(crate) fn benchmark_openapi_document(cached: bool) -> Value {
    if cached {
        gateway_openapi_document_for_apps()
    } else {
        gateway_openapi_document_for_apps_uncached()
    }
}

fn gateway_openapi_document_from_contract(
    contract: GatewayCapabilityContract,
    app_components: Map<String, Value>,
) -> Value {
    let mut paths = Map::new();
    for capability in &contract.capabilities {
        let path_entry = paths
            .entry(openapi_path(&capability.http.path))
            .or_insert_with(|| Value::Object(Map::new()));
        let Some(path_object) = path_entry.as_object_mut() else {
            continue;
        };
        path_object.insert(
            capability.http.method.to_ascii_lowercase(),
            openapi_operation(capability),
        );
    }
    let mut schemas = Map::new();
    schemas.insert(
        "GatewayError".to_string(),
        json!({
            "type": "object",
            "properties": {
                "error": {"type": "string"}
            },
            "required": ["error"]
        }),
    );
    for (name, schema) in [
        ("SendMessageRequest", send_message_request_schema()),
        ("SendMessageReceipt", send_message_receipt_schema()),
        ("SessionInputCursor", session_input_cursor_schema()),
        (
            "SessionInputApplicationReceipt",
            session_input_application_receipt_schema(),
        ),
        ("TurnInboxItem", turn_inbox_item_schema()),
        ("TurnInboxSnapshot", turn_inbox_snapshot_schema()),
        ("SessionInputProjection", session_input_projection_schema()),
        (
            "SessionInputCancelRequest",
            session_input_cancel_request_schema(),
        ),
        (
            "SessionInputReclassifyRequest",
            session_input_reclassify_request_schema(),
        ),
        (
            "SessionInputMutationReceipt",
            session_input_mutation_receipt_schema(),
        ),
        (
            "CancelSessionTurnRequest",
            cancel_session_turn_request_schema(),
        ),
        (
            "CancelSessionTurnReceipt",
            cancel_session_turn_receipt_schema(),
        ),
        (
            "ContextCompactionResult",
            context_compaction_result_schema(),
        ),
        ("SlashDispatchRequest", slash_dispatch_request_schema()),
        ("SlashDispatchReceipt", slash_dispatch_receipt_schema()),
        (
            "HumanEntitlementProjection",
            human_entitlement_projection_schema(),
        ),
        ("AuthVerifyResponse", auth_verify_response_schema()),
        (
            "ApprovalPendingResponse",
            approval_pending_response_schema(),
        ),
        ("ApprovalExactResponse", approval_exact_response_schema()),
        ("ApprovalRespondReceipt", approval_respond_receipt_schema()),
        (
            "CreateLiveSubscriptionRequest",
            live_create_request_schema(),
        ),
        ("PatchLiveSubscriptionRequest", live_patch_request_schema()),
        ("LiveSubscription", live_subscription_schema()),
        ("LiveEnvelope", live_envelope_schema()),
        ("EvidenceRef", evidence_ref_schema()),
        ("MissionCommandTarget", mission_command_target_schema()),
        ("MissionCommand", mission_command_schema()),
        ("MissionCommandReceipt", mission_command_receipt_schema()),
        ("MissionCommandSagaRecord", mission_command_saga_schema()),
        (
            "MissionWorkspaceProjection",
            mission_workspace_projection_schema(),
        ),
        ("MissionControlSummary", mission_control_summary_schema()),
        (
            "MissionControlMissionSummary",
            mission_control_mission_summary_schema(),
        ),
        (
            "MissionControlReadiness",
            mission_control_readiness_schema(),
        ),
        (
            "MissionControlSessionNode",
            mission_control_session_node_schema(),
        ),
        ("MissionControlTaskNode", mission_control_task_node_schema()),
        ("MissionControlTeamNode", mission_control_team_node_schema()),
        (
            "MissionControlAgentNode",
            mission_control_agent_node_schema(),
        ),
        (
            "MissionControlGraphNode",
            mission_control_graph_node_schema(),
        ),
        (
            "MissionControlGraphEdge",
            mission_control_graph_edge_schema(),
        ),
        (
            "MissionControlGraphProjection",
            mission_control_graph_projection_schema(),
        ),
        (
            "MissionControlApprovalNode",
            mission_control_approval_node_schema(),
        ),
        (
            "MissionControlEventLine",
            mission_control_event_line_schema(),
        ),
        (
            "MissionControlProjection",
            mission_control_projection_schema(),
        ),
        (
            "MissionMaterializedSnapshot",
            mission_materialized_snapshot_schema(),
        ),
        ("MissionProjectionDelta", mission_projection_delta_schema()),
        ("MissionControlResponse", mission_control_response_schema()),
        ("MissionCommandResponse", mission_command_response_schema()),
        ("TaskListResponse", task_list_response_schema()),
        ("TaskDetailResponse", task_detail_response_schema()),
        ("TaskTurnsResponse", task_turns_response_schema()),
        ("TaskFocusProjection", task_focus_projection_schema()),
        ("MissionFocusProjection", mission_focus_projection_schema()),
        (
            "SessionTaskFocusRequest",
            session_task_focus_request_schema(),
        ),
        (
            "SessionMissionFocusRequest",
            session_mission_focus_request_schema(),
        ),
        (
            "SessionFocusClearRequest",
            session_focus_clear_request_schema(),
        ),
        ("TaskFocusRequest", task_focus_request_schema()),
        ("TaskMissionRequest", task_mission_request_schema()),
        (
            "TaskMissionPreviewResponse",
            task_mission_preview_response_schema(),
        ),
        (
            "TaskMissionCommitResponse",
            task_mission_commit_response_schema(),
        ),
        (
            "MissionOrganizationResponse",
            mission_organization_response_schema(),
        ),
        ("StartTaskRequest", start_task_request_schema()),
        ("StartTaskPhaseRequest", start_task_phase_request_schema()),
        (
            "TaskPhaseArtifactRequest",
            task_phase_artifact_request_schema(),
        ),
        ("TaskPhaseReviewRequest", task_phase_review_request_schema()),
        ("TaskTransitionRequest", task_transition_request_schema()),
        ("TaskFailureRequest", task_failure_request_schema()),
        ("Empty", json!({"type": "object", "maxProperties": 0})),
    ] {
        schemas.insert(name.to_string(), schema);
    }
    insert_canonical_schema::<harness_contract::projection::ExecutionProjection>(
        &mut schemas,
        "ExecutionProjection",
    );
    insert_canonical_schema::<harness_contract::policy::UpdateSessionExecutionPolicyRequest>(
        &mut schemas,
        "UpdateSessionExecutionPolicyRequest",
    );
    insert_canonical_schema::<harness_contract::policy::SessionExecutionPolicyResponse>(
        &mut schemas,
        "SessionExecutionPolicyResponse",
    );
    insert_canonical_schema::<harness_contract::projection::ExecutionActivityDetailProjection>(
        &mut schemas,
        "ExecutionActivityDetailProjection",
    );
    insert_canonical_schema::<harness_contract::projection::ProjectionDelta>(
        &mut schemas,
        "ProjectionDelta",
    );
    insert_canonical_schema::<harness_contract::projection::ExecutionCommandRequest>(
        &mut schemas,
        "ExecutionCommandRequest",
    );
    insert_canonical_schema::<harness_contract::projection::ExecutionCommandReceipt>(
        &mut schemas,
        "ExecutionCommandReceipt",
    );
    insert_canonical_schema::<harness_contract::projection::ExecutionLiveUpdate>(
        &mut schemas,
        "ExecutionLiveUpdate",
    );
    insert_canonical_schema::<harness_contract::projection::SessionExecutionIndicesProjection>(
        &mut schemas,
        "SessionExecutionIndicesProjection",
    );
    insert_canonical_schema::<harness_contract::projection::SessionEvidenceProjection>(
        &mut schemas,
        "SessionEvidenceProjection",
    );
    insert_canonical_schema::<harness_contract::projection::SessionHistoryIndexProjection>(
        &mut schemas,
        "SessionHistoryIndexProjection",
    );
    insert_canonical_schema::<harness_contract::task::TaskAggregate>(&mut schemas, "TaskAggregate");
    insert_canonical_schema::<harness_contract::task::TaskTurnBinding>(
        &mut schemas,
        "TaskTurnBinding",
    );
    insert_canonical_schema::<harness_contract::task::SessionRoutingFocus>(
        &mut schemas,
        "SessionRoutingFocus",
    );
    insert_canonical_schema::<harness_contract::task::SessionFocusReceipt>(
        &mut schemas,
        "SessionFocusReceipt",
    );
    insert_canonical_schema::<harness_contract::mission::TaskMissionAssignmentCommand>(
        &mut schemas,
        "TaskMissionAssignmentCommand",
    );
    insert_canonical_schema::<harness_contract::mission::TaskMissionAssignmentPreview>(
        &mut schemas,
        "TaskMissionAssignmentPreview",
    );
    insert_canonical_schema::<harness_contract::mission::TaskMissionAssignmentReceipt>(
        &mut schemas,
        "TaskMissionAssignmentReceipt",
    );
    insert_canonical_schema::<harness_contract::mission::MissionOrganizationDecision>(
        &mut schemas,
        "MissionOrganizationDecision",
    );
    if let Some(entity) = schemas.get("ProjectionEntity").cloned() {
        schemas.insert("ExecutionProjectionEntity".to_string(), entity);
    }
    schemas.extend(app_components);

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Cowd Gateway API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Generated from Gateway Capability Contract. This is a lightweight OpenAPI projection without external Swagger dependencies."
        },
        "servers": [{"url": "/"}],
        "security": [{"bearerAuth": []}],
        "components": {
            "securitySchemes": {
                "bearerAuth": {
                    "type": "http",
                    "scheme": "bearer"
                }
            },
            "schemas": Value::Object(schemas)
        },
        "paths": Value::Object(paths),
        "x-cowd-contract": {
            "kind": contract.kind,
            "schema_version": contract.schema_version,
            "route_count": contract.route_count,
            "capability_count": contract.capability_count,
            "coverage": contract.coverage
        },
        "x-cowd-route-catalog-digest": surface::gateway_api::gateway_route_catalog_digest(),
        "x-cowd-projection-v3-golden": projection_v3_golden()
    })
}

fn projection_v3_golden() -> Value {
    serde_json::from_str(include_str!(
        "../../../harness-contract/tests/fixtures/projection-v3/materialization.json"
    ))
    .expect("canonical projection v3 fixture must be valid JSON")
}

fn insert_canonical_schema<T: schemars::JsonSchema>(schemas: &mut Map<String, Value>, name: &str) {
    let mut root = serde_json::to_value(schemars::schema_for!(T))
        .expect("canonical harness contract schema must serialize");
    let definitions = root
        .as_object_mut()
        .and_then(|object| object.remove("$defs"))
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    rewrite_schema_refs(&mut root);
    for (definition_name, mut definition) in definitions {
        rewrite_schema_refs(&mut definition);
        schemas.insert(definition_name, definition);
    }
    schemas.insert(name.to_string(), root);
}

fn rewrite_schema_refs(value: &mut Value) {
    match value {
        Value::Object(object) => {
            let rewritten = object
                .get("$ref")
                .and_then(Value::as_str)
                .and_then(|reference| reference.strip_prefix("#/$defs/"))
                .map(|name| format!("#/components/schemas/{name}"));
            if let Some(reference) = rewritten {
                object.insert("$ref".to_string(), Value::String(reference));
            }
            for child in object.values_mut() {
                rewrite_schema_refs(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                rewrite_schema_refs(child);
            }
        }
        _ => {}
    }
}

pub(crate) fn gateway_openai_tools(tool_catalog: &tools::ToolCatalog) -> Value {
    gateway_openai_tools_from_catalog(tool_catalog)
}

fn gateway_openai_tools_from_catalog(tool_catalog: &tools::ToolCatalog) -> Value {
    let tools = tool_catalog
        .definitions(None)
        .into_iter()
        .map(|definition| {
            json!({
                "type": "function",
                "function": {
                    "name": definition.name,
                    "description": definition.description.unwrap_or_default(),
                    "parameters": definition.input_schema,
                }
            })
        })
        .collect::<Vec<_>>();

    json!({
        "kind": "gateway.openai_tools",
        "schema_version": 2,
        "source": "runtime.tool_catalog",
        "tool_count": tools.len(),
        "tools": tools,
    })
}

fn route_capability(route: &GatewayRouteManifestEntry) -> GatewayCapability {
    let domain = capability_domain(route);
    let auth = auth_mode(&route.path);
    let risk = risk_level(route);
    let side_effects = side_effects(route, &risk);
    let idempotency = idempotency(route, &risk);
    let streaming = streaming_mode(&route.path);
    let id = capability_id(&domain, route);
    let title = capability_title(route, &domain);
    let description = capability_description(route, &domain);
    let ai_affordance = GatewayCapabilityAiAffordance {
        expose_as_tool: false,
        tool_name: None,
        when_to_use: when_to_use(route, &domain),
        cautions: cautions(route, &risk, &side_effects),
    };
    let input_schema = input_schema(route);
    let output_schema = output_schema(route);
    let tests = test_hints(route);

    GatewayCapability {
        id,
        domain,
        title,
        description,
        http: GatewayCapabilityHttp {
            method: route.method.to_string(),
            path: route.path.clone(),
            handler: route.handler.clone(),
            source: route.source.clone(),
            stability: route.stability.to_string(),
            criticality: route.criticality.to_string(),
        },
        auth: auth.to_string(),
        risk,
        side_effects,
        idempotency,
        streaming,
        availability: GatewayCapabilityAvailability {
            available: true,
            executable: true,
        },
        discoverability: GatewayCapabilityDiscoverability {
            http: true,
            openapi: true,
            ai_tool: false,
        },
        consumed_by: explicit_surface_consumers(route),
        verified_by: vec!["gateway.route_manifest.handler_registered".to_string()],
        ai_affordance,
        input_schema,
        output_schema,
        tests,
        app: route.app.clone(),
    }
}

/// Production Surface consumers are declared explicitly. This list is
/// intentionally conservative: an HTTP route, generated client method, or
/// matching URL name is not evidence that a Surface actually consumes it.
fn explicit_surface_consumers(route: &GatewayRouteManifestEntry) -> Vec<String> {
    const WEBUI: &[(&str, &str)] = &[
        ("GET", "/api/sessions"),
        ("GET", "/api/sessions/search"),
        ("GET", "/api/sessions/:id"),
        ("GET", "/api/sessions/:id/execution-policy"),
        ("PUT", "/api/sessions/:id/execution-policy"),
        ("POST", "/api/sessions/:id/messages"),
        ("GET", "/api/approval/pending"),
        ("GET", "/api/mission/control"),
        ("POST", "/api/mission/control"),
        ("GET", "/api/mission/control/teams/:team_id/execution"),
        ("GET", "/api/mission/control/teams/:team_id/evidence"),
        ("GET", "/api/mission/schedules"),
        ("POST", "/api/mission/schedules"),
        ("PATCH", "/api/mission/schedules/:id"),
        ("GET", "/api/harness-eval/reports/:id"),
        ("GET", "/api/harness-eval/reports/:id/artifacts"),
        ("GET", "/api/harness-eval/reports/:id/gate"),
        ("GET", "/api/evolution/missions/:id/detail"),
        ("GET", "/api/evolution/collaboration-patterns"),
        ("GET", "/api/evolution/chain/:id"),
        ("POST", "/api/evolution/reviews"),
        ("GET", "/api/evolution/reviews/:id"),
    ];
    const TUI: &[(&str, &str)] = &[
        ("GET", "/api/sessions"),
        ("GET", "/api/sessions/:id"),
        ("POST", "/api/sessions/:id/messages"),
        ("GET", "/api/sessions/:id/execution"),
        ("GET", "/api/sessions/:id/stats"),
        ("GET", "/api/sessions/:id/execution-policy"),
        ("PUT", "/api/sessions/:id/execution-policy"),
        ("GET", "/api/approval/pending"),
        ("GET", "/api/sessions/:id/input-projection"),
        ("POST", "/api/slash/dispatch"),
        ("GET", "/api/mission/control"),
    ];
    let mut consumers = Vec::new();
    if WEBUI.contains(&(route.method.as_str(), route.path.as_str())) {
        consumers.push("webui".to_string());
    }
    if TUI.contains(&(route.method.as_str(), route.path.as_str())) {
        consumers.push("tui".to_string());
    }
    consumers
}

fn capability_domain(route: &GatewayRouteManifestEntry) -> String {
    match route.group.as_str() {
        "system" => "tool".to_string(),
        "message" => "session.message".to_string(),
        "cross_plane" => "cross_plane".to_string(),
        "harness_eval" => "harness_eval".to_string(),
        // Managed Agent is a Runtime scheduling capability, not a separate
        // Gateway business domain. Keeping this mapping prevents API
        // discovery, TUI visibility and model affordances from drifting from
        // the Runtime owner.
        "managed_agent" => "runtime".to_string(),
        "public" => "public".to_string(),
        other => other.to_string(),
    }
}

fn auth_mode(path: &str) -> &'static str {
    if path == "/health"
        || path == "/healthz"
        || path == "/readyz"
        || path == "/api/webui/manifest"
        || matches!(
            path,
            "/api/gateway/route-manifest"
                | "/api/gateway/capability-contract"
                | "/api/gateway/openapi.json"
                | "/api/gateway/openai-tools"
        )
        || path.starts_with("/api/auth/")
    {
        "public"
    } else {
        "bearer"
    }
}

fn risk_level(route: &GatewayRouteManifestEntry) -> String {
    let path = route.path.to_ascii_lowercase();
    if route.method == "DELETE"
        || path.contains("/delete")
        || path.contains("/stop")
        || path.contains("/cancel")
        || path.contains("/logout")
    {
        "destructive".to_string()
    } else if path.contains("/cross-plane")
        || path.contains("/channels/")
        || path.contains("/surfaces/")
        || path.contains("/connectors/")
        || path.contains("/resources")
        || path.contains("/upload")
        || path.contains("/send")
        || path.contains("/dispatch")
    {
        "external".to_string()
    } else if path.contains("/config")
        || path.contains("/reload")
        || path.contains("/repair")
        || path.contains("/start")
        || path.contains("/release-gate")
        || path.contains("/approval/respond")
    {
        "admin".to_string()
    } else if route.method == "GET" {
        "read".to_string()
    } else {
        "write".to_string()
    }
}

fn side_effects(route: &GatewayRouteManifestEntry, risk: &str) -> Vec<String> {
    let path = route.path.to_ascii_lowercase();
    let mut effects = Vec::new();
    if route.method != "GET" {
        effects.push("mutates_gateway_or_runtime_state".to_string());
    }
    if path.contains("/workspace") || path.contains("/file") || path.contains("/upload") {
        effects.push("may_read_or_write_workspace_files".to_string());
    }
    if path.contains("/channels") || path.contains("/surfaces") || path.contains("/connectors") {
        effects.push("may_call_or_control_external_surface".to_string());
    }
    if path.contains("/runtime") || path.contains("/sessions") || path.contains("/mission") {
        effects.push("may_change_ai_harness_execution_state".to_string());
    }
    if risk == "read" && effects.is_empty() {
        effects.push("none".to_string());
    }
    effects
}

fn idempotency(route: &GatewayRouteManifestEntry, risk: &str) -> String {
    if route.method == "GET" {
        "safe".to_string()
    } else if risk == "destructive" || risk == "external" {
        "non_idempotent".to_string()
    } else if route.method == "PUT" || route.method == "PATCH" {
        "idempotent_by_resource".to_string()
    } else {
        "unknown".to_string()
    }
}

fn streaming_mode(path: &str) -> String {
    if path.ends_with("/stream") {
        "sse".to_string()
    } else if path.starts_with("/s/") {
        "static".to_string()
    } else {
        "none".to_string()
    }
}

fn capability_id(domain: &str, route: &GatewayRouteManifestEntry) -> String {
    format!(
        "gateway.{}.{}.{}",
        sanitize_segment(domain),
        route.method.to_ascii_lowercase(),
        sanitize_segment(route.path.trim_start_matches('/'))
    )
}

fn sanitize_segment(value: &str) -> String {
    let mut out = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if ch == ':' {
            out.push_str("by_");
        } else if ch == '*' {
            out.push_str("wildcard");
        } else if !out.ends_with('_') {
            out.push('_');
        }
    }
    out.trim_matches('_').to_string()
}

fn capability_title(route: &GatewayRouteManifestEntry, domain: &str) -> String {
    format!("{} {} {}", domain, route.method, route.path)
}

fn capability_description(route: &GatewayRouteManifestEntry, domain: &str) -> String {
    let action = match route.method.as_str() {
        "GET" => "Query",
        "POST" => "Invoke or create",
        "PUT" => "Replace",
        "PATCH" => "Update",
        "DELETE" => "Delete",
        _ => "Call",
    };
    format!(
        "{action} Gateway {domain} capability through `{}` handled by `{}`.",
        route.path, route.handler
    )
}

fn when_to_use(route: &GatewayRouteManifestEntry, domain: &str) -> String {
    match domain {
        "runtime" => "Use when inspecting, controlling, or submitting AI Harness runtime work.".to_string(),
        "session" | "session.message" => {
            "Use when creating, reading, branching, compacting, or messaging within sessions."
                .to_string()
        }
        "mission" => {
            "Use when coordinating multi-session, multi-agent Mission Runtime work.".to_string()
        }
        "memory" => "Use when recalling, explaining, maintaining, or inspecting memory and knowledge.".to_string(),
        "reality" => "Use when inspecting fact flow, promotions, evidence, and Reality Core state.".to_string(),
        "matrix" => "Use when working with structured facts, source packs, metrics, changes, and evidence packets.".to_string(),
        "tool" => "Use when discovering or planning tool execution through Gateway.".to_string(),
        "skill" => "Use when discovering, validating, planning, or reading skill capabilities.".to_string(),
        "surface" | "edge" | "connector" => {
            "Use when inspecting or operating external surfaces, edge packages, or connectors.".to_string()
        }
        "workspace" | "resource" => {
            "Use when reading workspace state or registering resources for runtime context.".to_string()
        }
        _ => format!("Use for Gateway `{}` capability operations.", route.path),
    }
}

fn cautions(route: &GatewayRouteManifestEntry, risk: &str, side_effects: &[String]) -> Vec<String> {
    let mut cautions = Vec::new();
    if risk != "read" {
        cautions.push(format!(
            "risk={risk}; require policy and approval checks when configured"
        ));
    }
    if route.method != "GET" {
        cautions.push(
            "non-GET call may mutate state; prefer dry-run or plan endpoint when available"
                .to_string(),
        );
    }
    if side_effects
        .iter()
        .any(|effect| effect.contains("external_surface"))
    {
        cautions.push("may interact with external surface or connector".to_string());
    }
    if cautions.is_empty() {
        cautions.push("read-only observation; still respect auth and data visibility".to_string());
    }
    cautions
}

fn input_schema(route: &GatewayRouteManifestEntry) -> Value {
    let params = path_param_schema(&route.path);
    let mut properties = Map::new();
    if params["required"]
        .as_array()
        .is_some_and(|required| !required.is_empty())
    {
        properties.insert("path".to_string(), params.clone());
    }
    if route.method == "GET" {
        properties.insert(
            "query".to_string(),
            json!({
                "type": "object",
                "additionalProperties": true,
                "description": "Query parameters accepted by the handler-specific Params struct."
            }),
        );
    } else {
        properties.insert(
            "body".to_string(),
            json!({
                "type": "object",
                "additionalProperties": true,
                "description": "Request JSON or multipart body. See handler request type in source file."
            }),
        );
    }
    json!({
        "type": "object",
        "properties": properties,
        "additionalProperties": false
    })
}

fn output_schema(route: &GatewayRouteManifestEntry) -> Value {
    if route.path.ends_with("/stream") {
        json!({
            "type": "string",
            "format": "event-stream",
            "description": "Server-Sent Events stream."
        })
    } else if route.path.starts_with("/s/")
        || route.path.contains("/download")
        || route.path.contains("/raw")
    {
        json!({
            "type": "string",
            "format": "binary",
            "description": "Binary or static response."
        })
    } else {
        json!({
            "type": "object",
            "additionalProperties": true,
            "description": "JSON response returned by the Gateway handler."
        })
    }
}

fn path_param_schema(path: &str) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::new();
    for segment in path.split('/') {
        if let Some(name) = segment.strip_prefix(':') {
            properties.insert(
                name.to_string(),
                json!({
                    "type": "string",
                    "description": format!("Path parameter `{name}`")
                }),
            );
            required.push(Value::String(name.to_string()));
        } else if segment.starts_with('*') {
            let name = segment.trim_start_matches('*');
            properties.insert(
                name.to_string(),
                json!({
                    "type": "string",
                    "description": format!("Wildcard path parameter `{name}`")
                }),
            );
            required.push(Value::String(name.to_string()));
        }
    }
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}

fn test_hints(route: &GatewayRouteManifestEntry) -> Vec<String> {
    let mut hints = vec!["cargo test -p gateway route_manifest -- --nocapture".to_string()];
    if route.path.starts_with("/api/gateway/") {
        hints.push("cargo test -p gateway capability_contract -- --nocapture".to_string());
    }
    if route.criticality == "p1" {
        hints.push("cargo check -p gateway --all-targets".to_string());
    }
    hints
}

fn openapi_path(path: &str) -> String {
    path.split('/')
        .map(|segment| {
            if let Some(name) = segment.strip_prefix(':') {
                format!("{{{name}}}")
            } else if let Some(name) = segment.strip_prefix('*') {
                format!("{{{name}}}")
            } else {
                segment.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn openapi_operation(capability: &GatewayCapability) -> Value {
    let mut operation = Map::new();
    let stable_metadata = stable_route_metadata(&capability.http.method, &capability.http.path);
    let app_metadata = capability.app.as_ref();
    operation.insert(
        "operationId".to_string(),
        Value::String(
            stable_metadata
                .as_ref()
                .map(|metadata| metadata.operation_id.clone())
                .unwrap_or_else(|| openapi_operation_id(&capability.id)),
        ),
    );
    operation.insert(
        "summary".to_string(),
        Value::String(capability.title.clone()),
    );
    operation.insert(
        "description".to_string(),
        Value::String(format!(
            "{}\n\nRisk: {}. Side effects: {}.",
            capability.description,
            capability.risk,
            capability.side_effects.join(", ")
        )),
    );
    operation.insert(
        "tags".to_string(),
        Value::Array(vec![Value::String(capability.domain.clone())]),
    );
    if capability.auth == "public" {
        operation.insert("security".to_string(), Value::Array(vec![]));
    }
    let parameters = openapi_parameters(
        &capability.http.method,
        &capability.http.path,
        stable_metadata.as_ref(),
    );
    if !parameters.is_empty() {
        operation.insert("parameters".to_string(), Value::Array(parameters));
    }
    if capability.http.method != "GET" {
        let request_schema = app_metadata
            .map(|metadata| {
                json!({"$ref": format!("#/components/schemas/{}", metadata.request_schema)})
            })
            .or_else(|| stable_request_schema(capability))
            .unwrap_or_else(|| capability.input_schema.clone());
        let mut request_content = Map::new();
        request_content.insert(
            "application/json".to_string(),
            json!({"schema": request_schema.clone()}),
        );
        if app_metadata.is_none() {
            request_content.insert(
                "multipart/form-data".to_string(),
                json!({"schema": request_schema}),
            );
        }
        operation.insert(
            "requestBody".to_string(),
            json!({
                "required": stable_metadata
                    .as_ref()
                    .is_some_and(|metadata| metadata.request_required),
                "content": Value::Object(request_content)
            }),
        );
    }
    let response_schema = app_metadata
        .map(|metadata| {
            json!({"$ref": format!("#/components/schemas/{}", metadata.response_schema)})
        })
        .or_else(|| stable_response_schema(capability))
        .unwrap_or_else(|| capability.output_schema.clone());
    let mut content = Map::new();
    content.insert(
        "application/json".to_string(),
        json!({"schema": response_schema}),
    );
    if app_metadata
        .map(|metadata| metadata.emits_live_event)
        .or_else(|| stable_metadata.as_ref().map(|metadata| metadata.streaming))
        .unwrap_or(false)
    {
        let event_schema = app_metadata
            .map(|metadata| metadata.response_schema.as_str())
            .or_else(|| {
                stable_metadata
                    .as_ref()
                    .map(|metadata| metadata.response_schema.as_str())
            })
            .unwrap_or("ProjectionDelta");
        content.insert(
            "text/event-stream".to_string(),
            json!({
                "schema": {"type": "string", "format": "event-stream"},
                "x-cowd-event-schema": {"$ref": format!("#/components/schemas/{event_schema}")}
            }),
        );
    }
    let writer_policy = stable_metadata
        .as_ref()
        .map_or(SessionWriterPolicy::None, |metadata| {
            metadata.session_writer
        });
    let responses = if let Some(metadata) = app_metadata {
        json!({
            "200": {
                "description": "Successful Gateway response",
                "content": Value::Object(content)
            },
            "400": app_openapi_error_response("Bad request", metadata.auth_error_schema.as_deref()),
            "401": app_openapi_error_response("Unauthorized", metadata.auth_error_schema.as_deref()),
            "403": app_openapi_error_response("Capability or scope denied", metadata.auth_error_schema.as_deref()),
            "404": app_openapi_error_response("Resource is outside the verified scope", metadata.auth_error_schema.as_deref()),
            "409": app_openapi_error_response("Revision or idempotency conflict", metadata.auth_error_schema.as_deref()),
            "429": app_openapi_error_response("Rate limited", metadata.auth_error_schema.as_deref()),
            "500": app_openapi_error_response("Gateway internal error", metadata.auth_error_schema.as_deref())
        })
    } else {
        let mut responses = json!({
            "200": {
                "description": "Successful Gateway response",
                "content": Value::Object(content)
            },
            "400": {"description": "Bad request"},
            "401": {"description": "Unauthorized"},
            "500": {"description": "Gateway internal error"}
        });
        if writer_policy != SessionWriterPolicy::None {
            responses["403"] = json!({
                "description": "Missing, invalid, unattached, or read-only x-cowd-observer-id"
            });
            responses["409"] = json!({
                "description": "Writer lease conflict"
            });
        }
        responses
    };
    operation.insert("responses".to_string(), responses);
    operation.insert(
        "x-cowd".to_string(),
        json!({
            "capability_id": capability.id,
            "risk": capability.risk,
            "idempotency": capability.idempotency,
            "side_effects": capability.side_effects,
            "source": capability.http.source,
            "handler": capability.http.handler,
            "ai_tool": capability.discoverability.ai_tool,
            "session_writer": writer_policy.as_str(),
        }),
    );
    Value::Object(operation)
}

fn app_openapi_error_response(description: &str, error_schema: Option<&str>) -> Value {
    json!({
        "description": description,
        "content": {
            "application/json": {
                "schema": {"$ref": format!("#/components/schemas/{}", error_schema.unwrap_or("GatewayError"))}
            }
        }
    })
}

fn stable_request_schema(capability: &GatewayCapability) -> Option<Value> {
    stable_route_metadata(&capability.http.method, &capability.http.path)
        .and_then(|metadata| metadata.request_schema)
        .map(|schema| json!({"$ref": format!("#/components/schemas/{schema}")}))
}

fn stable_response_schema(capability: &GatewayCapability) -> Option<Value> {
    stable_route_metadata(&capability.http.method, &capability.http.path).map(
        |metadata| json!({"$ref": format!("#/components/schemas/{}", metadata.response_schema)}),
    )
}

fn live_source_selector_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "id"],
        "properties": {
            "kind": {"type": "string", "enum": ["session", "execution", "mission"]},
            "id": {"type": "string", "minLength": 1, "maxLength": 256},
            "cursor": {"type": "integer", "minimum": 0},
            "revision": {"type": "integer", "minimum": 0},
            "detail_scope": {"type": "string", "enum": ["summary", "full"]}
        },
        "additionalProperties": false
    })
}

fn live_selector_schema() -> Value {
    json!({
        "type": "object",
        "required": ["sources"],
        "properties": {
            "sources": {
                "type": "array",
                "maxItems": 32,
                "items": live_source_selector_schema()
            }
        },
        "additionalProperties": false
    })
}

fn live_create_request_schema() -> Value {
    json!({
        "type": "object",
        "required": ["surface_instance", "selector"],
        "properties": {
            "surface_instance": {"type": "string", "minLength": 1, "maxLength": 128},
            "selector": live_selector_schema(),
            "ttl_seconds": {"type": "integer", "minimum": 1, "maximum": 86400},
            "idempotency_key": {"type": "string", "maxLength": 256}
        },
        "additionalProperties": false
    })
}

fn live_patch_request_schema() -> Value {
    json!({
        "type": "object",
        "required": ["expected_revision", "idempotency_key", "selector"],
        "properties": {
            "expected_revision": {"type": "integer", "minimum": 1},
            "idempotency_key": {"type": "string", "minLength": 1, "maxLength": 256},
            "selector": live_selector_schema(),
            "ttl_seconds": {"type": "integer", "minimum": 1, "maximum": 86400}
        },
        "additionalProperties": false
    })
}

fn live_subscription_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "schema_version", "id", "surface_instance", "revision", "selector",
            "selector_hash", "expires_at_ms", "stream_url"
        ],
        "properties": {
            "schema_version": {"type": "integer", "const": 1},
            "id": {"type": "string"},
            "surface_instance": {"type": "string"},
            "revision": {"type": "integer", "minimum": 1},
            "selector": live_selector_schema(),
            "selector_hash": {"type": "string"},
            "expires_at_ms": {"type": "integer", "minimum": 0},
            "stream_url": {"type": "string"}
        },
        "additionalProperties": false
    })
}

fn live_envelope_schema() -> Value {
    let mut schema = harness_contract::live::live_envelope_json_schema();
    let Some(object) = schema.as_object_mut() else {
        return json!({
            "type": "object",
            "x-cowd-schema-error": "canonical live envelope schema is not an object",
        });
    };
    object.insert(
        "x-cowd-schema-hash".to_string(),
        json!(harness_contract::live::live_envelope_schema_hash()),
    );
    object.insert(
        "example".to_string(),
        serde_json::to_value(harness_contract::live::canonical_live_envelope_fixture())
            .unwrap_or(serde_json::Value::Null),
    );
    schema
}

fn evidence_ref_schema() -> Value {
    json!({
        "type": "object",
        "required": ["ref_type", "id", "boundary"],
        "properties": {
            "ref_type": {"type": "string"},
            "id": {"type": "string"},
            "source": {"type": ["string", "null"]},
            "boundary": {"type": "string", "enum": ["observed", "inferred", "simulated", "hypothetical", "conflict"]},
            "confidence_bp": {"type": ["integer", "null"], "minimum": 0, "maximum": 10000}
        },
        "additionalProperties": false
    })
}

fn mission_command_target_schema() -> Value {
    let variants = [
        ("mission", "mission_id"),
        ("session", "session_id"),
        ("task", "task_id"),
        ("graph", "graph_id"),
        ("team", "team_id"),
        ("agent", "agent_id"),
        ("approval", "approval_id"),
        ("relation", "relation_id"),
    ]
    .into_iter()
    .map(|(kind, id)| {
        let mut variant = json!({
            "type": "object",
            "required": ["kind", id],
            "properties": {
                "kind": {"type": "string", "const": kind}
            },
            "additionalProperties": false
        });
        variant["properties"]
            .as_object_mut()
            .expect("Mission target properties are an object")
            .insert(id.to_string(), json!({"type": "string", "minLength": 1}));
        variant
    })
    .collect::<Vec<_>>();
    json!({"oneOf": variants, "discriminator": {"propertyName": "kind"}})
}

fn mission_command_schema() -> Value {
    json!({
        "type": "object",
        "required": ["command_id", "action", "target"],
        "properties": {
            "command_id": {"type": "string", "minLength": 1},
            "action": {
                "type": "string",
                "enum": [
                    "create", "activate", "background", "pause", "resume", "cancel",
                    "close", "input", "continue", "branch", "approve", "reject",
                    "replan", "link", "unlink"
                ]
            },
            "target": {"$ref": "#/components/schemas/MissionCommandTarget"},
            "actor": {"type": "string"},
            "expected_revision": {"type": ["integer", "null"], "minimum": 0},
            "correlation_id": {"type": "string"},
            "payload": {},
            "evidence_refs": {"type": "array", "items": {"$ref": "#/components/schemas/EvidenceRef"}}
        },
        "additionalProperties": false
    })
}

fn mission_command_receipt_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "command_id", "action", "target", "accepted_revision", "status",
            "evidence_refs", "result"
        ],
        "properties": {
            "command_id": {"type": "string"},
            "action": {
                "type": "string",
                "enum": [
                    "create", "activate", "background", "pause", "resume", "cancel",
                    "close", "input", "continue", "branch", "approve", "reject",
                    "replan", "link", "unlink"
                ]
            },
            "target": {"$ref": "#/components/schemas/MissionCommandTarget"},
            "accepted_revision": {"type": "integer", "minimum": 0},
            "status": {"type": "string"},
            "reason": {"type": ["string", "null"]},
            "evidence_refs": {"type": "array", "items": {"$ref": "#/components/schemas/EvidenceRef"}},
            "result": {}
        },
        "additionalProperties": false
    })
}

fn mission_command_saga_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "schema_version", "command", "phase", "revision",
            "reserved_target_revision", "updated_at_ms"
        ],
        "properties": {
            "schema_version": {"type": "integer", "minimum": 1},
            "command": {"$ref": "#/components/schemas/MissionCommand"},
            "phase": {
                "type": "string",
                "enum": [
                    "reserved", "effect_committed", "receipt_committed",
                    "finalized", "rejected", "reconciliation_required"
                ]
            },
            "revision": {"type": "integer", "minimum": 1},
            "reserved_target_revision": {"type": "integer", "minimum": 0},
            "effect_result": {},
            "receipt": {
                "oneOf": [
                    {"$ref": "#/components/schemas/MissionCommandReceipt"},
                    {"type": "null"}
                ]
            },
            "error": {"type": ["string", "null"]},
            "updated_at_ms": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn mission_workspace_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "workspace_id", "title", "session_count", "running_agent_count",
            "pending_approval_count", "recovery_required_count"
        ],
        "properties": {
            "workspace_id": {"type": "string"},
            "title": {"type": "string"},
            "active_session_id": {"type": ["string", "null"]},
            "session_count": {"type": "integer", "minimum": 0},
            "running_agent_count": {"type": "integer", "minimum": 0},
            "pending_approval_count": {"type": "integer", "minimum": 0},
            "recovery_required_count": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn mission_control_summary_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "session_count", "background_session_count", "paused_session_count",
            "closed_session_count", "task_count", "team_count", "agent_count",
            "pending_approval_count", "recovery_required_count", "pending_organization_count"
        ],
        "properties": {
            "session_count": {"type": "integer", "minimum": 0},
            "active_session_id": {"type": ["string", "null"]},
            "background_session_count": {"type": "integer", "minimum": 0},
            "paused_session_count": {"type": "integer", "minimum": 0},
            "closed_session_count": {"type": "integer", "minimum": 0},
            "task_count": {"type": "integer", "minimum": 0},
            "team_count": {"type": "integer", "minimum": 0},
            "agent_count": {"type": "integer", "minimum": 0},
            "pending_approval_count": {"type": "integer", "minimum": 0},
            "recovery_required_count": {"type": "integer", "minimum": 0},
            "pending_organization_count": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn mission_control_mission_summary_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "mission_id", "objective", "status", "revision", "session_count",
            "task_count", "graph_count", "team_count", "agent_count",
            "created_at_ms", "updated_at_ms"
        ],
        "properties": {
            "mission_id": {"type": "string"},
            "objective": {"type": "string"},
            "status": {"type": "string"},
            "revision": {"type": "integer", "minimum": 0},
            "session_count": {"type": "integer", "minimum": 0},
            "task_count": {"type": "integer", "minimum": 0},
            "graph_count": {"type": "integer", "minimum": 0},
            "team_count": {"type": "integer", "minimum": 0},
            "agent_count": {"type": "integer", "minimum": 0},
            "created_at_ms": {"type": "integer", "minimum": 0},
            "updated_at_ms": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn mission_control_readiness_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "ready_count", "blocked_count", "actions"],
        "properties": {
            "kind": {"type": "string"},
            "ready_count": {"type": "integer", "minimum": 0},
            "blocked_count": {"type": "integer", "minimum": 0},
            "actions": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": [
                        "action", "available", "reason", "requires_approval",
                        "target_count"
                    ],
                    "properties": {
                        "action": {"type": "string"},
                        "available": {"type": "boolean"},
                        "reason": {"type": "string"},
                        "requires_approval": {"type": "boolean"},
                        "policy_marker": {"type": ["string", "null"]},
                        "target_count": {"type": "integer", "minimum": 0}
                    },
                    "additionalProperties": false
                }
            }
        },
        "additionalProperties": false
    })
}

fn mission_control_session_node_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "session_id", "title", "status", "lifecycle", "hydration", "active",
            "attachment_count", "team_count", "agent_count", "contributing_task_count",
            "contributing_task_ids", "created_at_ms", "updated_at_ms"
        ],
        "properties": {
            "session_id": {"type": "string"},
            "title": {"type": "string"},
            "status": {"type": "string"},
            "lifecycle": {"type": "string"},
            "hydration": {"type": "string"},
            "active": {"type": "boolean"},
            "attachment_count": {"type": "integer", "minimum": 0},
            "team_count": {"type": "integer", "minimum": 0},
            "agent_count": {"type": "integer", "minimum": 0},
            "contributing_task_count": {"type": "integer", "minimum": 0},
            "contributing_task_ids": {"type": "array", "items": {"type": "string"}},
            "created_at_ms": {"type": "integer", "minimum": 0},
            "updated_at_ms": {"type": "integer", "minimum": 0},
            "last_error": {"type": ["string", "null"]}
        },
        "additionalProperties": false
    })
}

fn mission_control_task_node_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "task_id", "mission_id", "kind", "root_task_id", "origin_session_id",
            "objective", "status", "revision", "phase_count", "graph_count",
            "turn_count", "assignment_source", "failure_count",
            "created_at_ms", "updated_at_ms"
        ],
        "properties": {
            "task_id": {"type": "string"},
            "mission_id": {"type": "string"},
            "kind": {"type": "string", "enum": ["root", "delegated"]},
            "root_task_id": {"type": "string"},
            "parent_task_id": {"type": ["string", "null"]},
            "origin_session_id": {"type": "string"},
            "objective": {"type": "string"},
            "status": {"type": "string"},
            "revision": {"type": "integer", "minimum": 0},
            "current_phase_id": {"type": ["string", "null"]},
            "phase_count": {"type": "integer", "minimum": 0},
            "graph_count": {"type": "integer", "minimum": 0},
            "turn_count": {"type": "integer", "minimum": 0},
            "assignment_source": {"type": "string"},
            "failure_count": {"type": "integer", "minimum": 0},
            "blocker_reason": {"type": ["string", "null"]},
            "created_at_ms": {"type": "integer", "minimum": 0},
            "updated_at_ms": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn mission_control_team_node_schema() -> Value {
    json!({
        "type": "object",
        "required": ["team_id", "graph_id", "agent_count", "detail"],
        "properties": {
            "team_id": {"type": "string"},
            "graph_id": {"type": "string"},
            "mission_id": {"type": ["string", "null"]},
            "task_id": {"type": ["string", "null"]},
            "session_id": {"type": ["string", "null"]},
            "status": {"type": ["string", "null"]},
            "agent_count": {"type": "integer", "minimum": 0},
            "detail": {"type": "object", "additionalProperties": true}
        },
        "additionalProperties": false
    })
}

fn mission_control_agent_node_schema() -> Value {
    json!({
        "type": "object",
        "required": ["agent_id", "detail"],
        "properties": {
            "agent_id": {"type": "string"},
            "mission_id": {"type": ["string", "null"]},
            "task_id": {"type": ["string", "null"]},
            "execution_id": {"type": ["string", "null"]},
            "team_id": {"type": ["string", "null"]},
            "session_id": {"type": ["string", "null"]},
            "status": {"type": ["string", "null"]},
            "backend": {"type": ["string", "null"]},
            "detail": {"type": "object", "additionalProperties": true},
            "display_label": {"type": ["string", "null"]},
            "display_role_label": {"type": ["string", "null"]},
            "display_focus_label": {"type": ["string", "null"]},
            "display_provenance": {"type": ["string", "null"]},
            "display_digest": {"type": ["string", "null"]}
        },
        "additionalProperties": false
    })
}

fn mission_control_graph_node_schema() -> Value {
    json!({
        "type": "object",
        "required": ["node_id", "kind", "label", "status", "mission_id"],
        "properties": {
            "node_id": {"type": "string"},
            "kind": {"type": "string"},
            "label": {"type": "string"},
            "status": {"type": "string"},
            "mission_id": {"type": "string"},
            "session_id": {"type": ["string", "null"]},
            "task_id": {"type": ["string", "null"]},
            "execution_id": {"type": ["string", "null"]},
            "team_id": {"type": ["string", "null"]},
            "agent_id": {"type": ["string", "null"]},
            "display_label": {"type": ["string", "null"]},
            "display_role_label": {"type": ["string", "null"]},
            "display_focus_label": {"type": ["string", "null"]},
            "display_provenance": {"type": ["string", "null"]},
            "display_digest": {"type": ["string", "null"]}
        },
        "additionalProperties": false
    })
}

fn mission_control_graph_edge_schema() -> Value {
    json!({
        "type": "object",
        "required": ["edge_id", "kind", "from_node_id", "to_node_id"],
        "properties": {
            "edge_id": {"type": "string"},
            "kind": {"type": "string"},
            "from_node_id": {"type": "string"},
            "to_node_id": {"type": "string"}
        },
        "additionalProperties": false
    })
}

fn mission_control_graph_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["schema_version", "mission_id", "nodes", "edges"],
        "properties": {
            "schema_version": {"type": "integer", "minimum": 1},
            "mission_id": {"type": "string"},
            "nodes": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlGraphNode"}},
            "edges": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlGraphEdge"}}
        },
        "additionalProperties": false
    })
}

fn mission_control_approval_node_schema() -> Value {
    json!({
        "type": "object",
        "required": ["approval_id", "status", "detail"],
        "properties": {
            "approval_id": {"type": "string"},
            "status": {"type": "string"},
            "action": {"type": ["string", "null"]},
            "source_session_id": {"type": ["string", "null"]},
            "detail": {"type": "object", "additionalProperties": true}
        },
        "additionalProperties": false
    })
}

fn mission_control_event_line_schema() -> Value {
    json!({
        "type": "object",
        "required": ["event_id", "stream_id", "cursor", "transaction_index", "scope", "kind", "created_at_ms"],
        "properties": {
            "event_id": {"type": "string"},
            "stream_id": {"type": "string"},
            "cursor": {"type": "integer", "minimum": 0},
            "transaction_index": {"type": "integer", "minimum": 0},
            "scope": {"type": "string"},
            "kind": {"type": "string"},
            "status": {"type": ["string", "null"]},
            "actor": {"type": ["string", "null"]},
            "created_at_ms": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn mission_control_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "schema_version", "kind", "workspace", "summary", "control_readiness",
            "selected_mission_id", "missions", "mission", "sessions", "tasks", "teams", "agents", "approvals", "organization_decisions", "mission_graph", "relations",
            "execution_graphs", "conflicts", "evidence", "capabilities",
            "event_digest", "health"
        ],
        "properties": {
            "schema_version": {"type": "integer", "minimum": 1},
            "kind": {"type": "string", "const": "mission_control.projection"},
            "workspace": {"$ref": "#/components/schemas/MissionWorkspaceProjection"},
            "summary": {"$ref": "#/components/schemas/MissionControlSummary"},
            "control_readiness": {"$ref": "#/components/schemas/MissionControlReadiness"},
            "selected_mission_id": {"type": "string"},
            "missions": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlMissionSummary"}},
            "mission": {},
            "sessions": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlSessionNode"}},
            "tasks": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlTaskNode"}},
            "teams": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlTeamNode"}},
            "agents": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlAgentNode"}},
            "approvals": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlApprovalNode"}},
            "organization_decisions": {"type": "array", "items": {"$ref": "#/components/schemas/MissionOrganizationDecision"}},
            "mission_graph": {"$ref": "#/components/schemas/MissionControlGraphProjection"},
            "relations": {},
            "execution_graphs": {},
            "conflicts": {},
            "evidence": {},
            "capabilities": {},
            "event_digest": {
                "type": "object",
                "required": ["total_recent_events", "scope_counts", "latest_errors", "recovery_required", "latest"],
                "properties": {
                    "total_recent_events": {"type": "integer", "minimum": 0},
                    "scope_counts": {"type": "object", "additionalProperties": {"type": "integer", "minimum": 0}},
                    "latest_errors": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlEventLine"}},
                    "recovery_required": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlEventLine"}},
                    "latest": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlEventLine"}}
                },
                "additionalProperties": false
            },
            "health": {}
        },
        "additionalProperties": false
    })
}

fn mission_materialized_snapshot_schema() -> Value {
    json!({
        "type": "object",
        "required": ["schema_version", "kind", "cursor", "revision", "needs_resync", "projection"],
        "properties": {
            "schema_version": {"type": "integer", "minimum": 1},
            "kind": {"type": "string", "const": "mission_control.materialized_snapshot"},
            "cursor": {"type": "integer", "minimum": 0},
            "revision": {"type": "integer", "minimum": 1},
            "needs_resync": {"type": "boolean"},
            "projection": {"$ref": "#/components/schemas/MissionControlProjection"}
        },
        "additionalProperties": false
    })
}

fn mission_projection_delta_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "schema_version", "kind", "from_cursor", "to_cursor", "revision",
            "needs_resync", "changed_domains", "events", "patch"
        ],
        "properties": {
            "schema_version": {"type": "integer", "minimum": 1},
            "kind": {"type": "string", "const": "mission_control.projection_delta"},
            "from_cursor": {"type": "integer", "minimum": 0},
            "from_revision": {"type": ["integer", "null"], "minimum": 1},
            "to_cursor": {"type": "integer", "minimum": 0},
            "revision": {"type": "integer", "minimum": 1},
            "needs_resync": {"type": "boolean"},
            "changed_domains": {"type": "array", "items": {"type": "string"}},
            "events": {"type": "array", "items": {"$ref": "#/components/schemas/MissionControlEventLine"}},
            "patch": {"type": "object", "additionalProperties": true}
        },
        "additionalProperties": false
    })
}

fn mission_control_response_schema() -> Value {
    json!({
        "type": "object",
        "required": ["ok", "snapshot"],
        "properties": {
            "envelope": {},
            "ok": {"type": "boolean"},
            "snapshot": {"$ref": "#/components/schemas/MissionMaterializedSnapshot"}
        },
        "additionalProperties": false
    })
}

fn mission_command_response_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "ok", "receipt", "saga", "snapshot"],
        "properties": {
            "envelope": {},
            "kind": {"type": "string", "const": "mission_control.command_result"},
            "ok": {"type": "boolean"},
            "receipt": {"$ref": "#/components/schemas/MissionCommandReceipt"},
            "saga": {"$ref": "#/components/schemas/MissionCommandSagaRecord"},
            "snapshot": {"$ref": "#/components/schemas/MissionMaterializedSnapshot"}
        },
        "additionalProperties": false
    })
}

fn send_message_request_schema() -> Value {
    json!({
        "type": "object",
        "required": ["content"],
        "properties": {
            "content": {"type": "string"},
            "resource_ids": {"type": "array", "items": {"type": "string"}},
            "idempotency_key": {"type": ["string", "null"]},
            "client_message_id": {"type": ["string", "null"]}
        },
        "additionalProperties": false
    })
}

fn send_message_receipt_schema() -> Value {
    json!({
        "type": "object",
        "required": ["session_id", "run_id", "status", "execution", "mode", "input", "materialized"],
        "properties": {
            "session_id": {"type": "string"},
            "run_id": {"type": "string"},
            "status": {"type": "string", "const": "accepted"},
            "execution": {
                "type": "object",
                "required": ["graph_id", "turn_id", "terminal_id", "status", "materialization"],
                "additionalProperties": true
            },
            "mode": {"type": "string", "enum": ["attached_to_active_turn", "queued_new_turn"]},
            "input": {"type": "object", "additionalProperties": true},
            "materialized": {"type": ["object", "null"], "additionalProperties": true},
            "input_projection": {"oneOf": [{"$ref": "#/components/schemas/SessionInputProjection"}, {"type": "null"}]},
            "turn_inbox": {"oneOf": [{"$ref": "#/components/schemas/TurnInboxSnapshot"}, {"type": "null"}]}
        },
        "additionalProperties": false
    })
}

fn session_input_cursor_schema() -> Value {
    json!({
        "type": "object",
        "required": ["generation", "sequence"],
        "properties": {
            "generation": {"type": "integer", "format": "uint64", "minimum": 0},
            "sequence": {"type": "integer", "format": "uint64", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn session_input_application_receipt_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "disposition_id", "leader_input_id", "input_ids", "action", "relation",
            "state", "objective", "required", "attempts", "summary", "task_ids", "team_ids",
            "agent_ids", "execution_ids", "target_session_created", "revision", "updated_at_ms"
        ],
        "properties": {
            "disposition_id": {"type": "string"},
            "leader_input_id": {"type": "string"},
            "input_ids": {"type": "array", "items": {"type": "string"}, "minItems": 1},
            "action": {
                "type": "string",
                "enum": [
                    "amend_current_turn", "replan_current_graph", "replace_current_task",
                    "add_required_task", "add_background_task", "add_team_lane",
                    "add_task_with_team", "dispatch_session", "progress_or_control", "clarify"
                ]
            },
            "relation": {
                "type": "string",
                "enum": [
                    "supplement", "replan", "progress", "background", "new_task",
                    "new_session", "subtask", "cross_session"
                ]
            },
            "state": {"type": "string", "enum": ["prepared", "materializing", "applied", "failed"]},
            "objective": {"type": "string"},
            "required": {"type": "boolean"},
            "attempts": {"type": "integer", "format": "uint16", "minimum": 1, "maximum": 2},
            "summary": {"type": "string"},
            "task_ids": {"type": "array", "items": {"type": "string"}},
            "team_ids": {"type": "array", "items": {"type": "string"}},
            "agent_ids": {"type": "array", "items": {"type": "string"}},
            "execution_ids": {"type": "array", "items": {"type": "string"}},
            "target_session_id": {"type": ["string", "null"]},
            "target_session_created": {"type": "boolean"},
            "error": {"type": ["string", "null"]},
            "revision": {"type": "integer", "format": "uint64", "minimum": 0},
            "updated_at_ms": {"type": "integer", "format": "uint64", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn turn_inbox_item_schema() -> Value {
    json!({
        "type": "object",
        "required": ["input_id", "session_id", "status", "decision", "content_preview", "created_at"],
        "properties": {
            "input_id": {"type": "string"},
            "session_id": {"type": "string"},
            "status": {
                "type": "string",
                "enum": [
                    "received", "persisted", "classified", "attached_to_turn", "queued_next",
                    "interrupt_requested", "dispatched_subtask", "dispatched_session",
                    "new_session_created", "control_resolved", "consumed", "cancelled", "failed",
                    "rejected_duplicate", "rejected_policy", "superseded"
                ]
            },
            "decision": {
                "type": "string",
                "enum": [
                    "start_new_turn", "supplement_current_turn", "interrupt_and_replan",
                    "enqueue_next_step", "spawn_subtask", "route_cross_session",
                    "create_new_session", "control_or_approval", "reject_duplicate", "reject_policy"
                ]
            },
            "relation_proposal": {"type": ["object", "null"], "additionalProperties": true},
            "content_preview": {"type": "string"},
            "checkpoint": {
                "type": ["string", "null"],
                "enum": [
                    "turn_start", "ingress_dispatched", "before_provider_request",
                    "after_provider_response", "after_tool_result", "before_final_answer",
                    "before_compaction", null
                ]
            },
            "created_at": {"type": "string", "format": "date-time"},
            "consumed_at": {"type": ["string", "null"], "format": "date-time"},
            "cursor": {"oneOf": [{"$ref": "#/components/schemas/SessionInputCursor"}, {"type": "null"}]},
            "failure_class": {"type": ["string", "null"]},
            "last_error": {"type": ["string", "null"]},
            "application_receipt": {
                "oneOf": [
                    {"$ref": "#/components/schemas/SessionInputApplicationReceipt"},
                    {"type": "null"}
                ]
            }
        },
        "additionalProperties": false
    })
}

fn turn_inbox_snapshot_schema() -> Value {
    json!({
        "type": "object",
        "required": ["session_id", "pending_count", "consumed_count", "items", "updated_at"],
        "properties": {
            "session_id": {"type": "string"},
            "turn_id": {"type": ["string", "null"]},
            "pending_count": {"type": "integer", "minimum": 0},
            "consumed_count": {"type": "integer", "minimum": 0},
            "admitted_cursor": {"oneOf": [{"$ref": "#/components/schemas/SessionInputCursor"}, {"type": "null"}]},
            "consumed_cursor": {"oneOf": [{"$ref": "#/components/schemas/SessionInputCursor"}, {"type": "null"}]},
            "items": {"type": "array", "items": {"$ref": "#/components/schemas/TurnInboxItem"}},
            "updated_at": {"type": "string", "format": "date-time"}
        },
        "additionalProperties": false
    })
}

fn session_input_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "session_id", "total", "pending_count", "queued_next_count", "consumed_count",
            "inputs", "updated_at"
        ],
        "properties": {
            "session_id": {"type": "string"},
            "active_turn_id": {"type": ["string", "null"]},
            "total": {"type": "integer", "minimum": 0},
            "pending_count": {"type": "integer", "minimum": 0},
            "queued_next_count": {"type": "integer", "minimum": 0},
            "consumed_count": {"type": "integer", "minimum": 0},
            "admitted_cursor": {"oneOf": [{"$ref": "#/components/schemas/SessionInputCursor"}, {"type": "null"}]},
            "consumed_cursor": {"oneOf": [{"$ref": "#/components/schemas/SessionInputCursor"}, {"type": "null"}]},
            "last_decision": {"type": ["string", "null"]},
            "inputs": {"type": "array", "items": {"$ref": "#/components/schemas/TurnInboxItem"}},
            "updated_at": {"type": "string", "format": "date-time"}
        },
        "additionalProperties": false
    })
}

fn session_input_cancel_request_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"reason": {"type": ["string", "null"]}},
        "additionalProperties": false
    })
}

fn session_input_reclassify_request_schema() -> Value {
    json!({
        "type": "object",
        "required": ["decision"],
        "properties": {
            "decision": {
                "type": "string",
                "enum": [
                    "start_new_turn", "supplement_current_turn", "interrupt_and_replan",
                    "enqueue_next_step", "spawn_subtask", "route_cross_session",
                    "create_new_session", "control_or_approval", "reject_duplicate",
                    "reject_policy"
                ]
            },
            "reason": {"type": ["string", "null"]}
        },
        "additionalProperties": false
    })
}

fn session_input_mutation_receipt_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "session_id", "input"],
        "properties": {
            "kind": {"type": "string", "enum": ["session_input.cancel", "session_input.reclassify"]},
            "session_id": {"type": "string"},
            "input": {"type": "object", "additionalProperties": true},
            "input_projection": {"oneOf": [{"$ref": "#/components/schemas/SessionInputProjection"}, {"type": "null"}]},
            "turn_inbox": {"oneOf": [{"$ref": "#/components/schemas/TurnInboxSnapshot"}, {"type": "null"}]}
        },
        "additionalProperties": false
    })
}

fn cancel_session_turn_request_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "reason": {"type": ["string", "null"]},
            "cancellation_id": {"type": ["string", "null"], "maxLength": 256},
            "requested_at_ms": {"type": ["integer", "null"], "minimum": 1},
            "expected_execution_id": {"type": ["string", "null"]},
            "expected_turn_id": {"type": ["string", "null"]}
        },
        "additionalProperties": false
    })
}

fn cancel_session_turn_receipt_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "cancellation_id", "session_id", "turn_id", "execution_id", "actor_id",
            "cause", "requested_at_ms", "status", "journal_sequence", "projection_revision"
        ],
        "properties": {
            "cancellation_id": {"type": "string"},
            "session_id": {"type": "string"},
            "turn_id": {"type": "string"},
            "execution_id": {"type": "string"},
            "actor_id": {"type": "string"},
            "cause": {"type": "string", "enum": ["user_requested", "system", "parent", "deadline", "lease_lost"]},
            "reason": {"type": ["string", "null"]},
            "requested_at_ms": {"type": "integer", "minimum": 0},
            "effective_at_ms": {"type": ["integer", "null"], "minimum": 0},
            "status": {"type": "string", "enum": ["requested", "cancelled", "already_terminal"]},
            "journal_sequence": {"type": "integer", "minimum": 0},
            "projection_revision": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn context_compaction_result_schema() -> Value {
    json!({
        "type": "object",
        "required": ["summary", "formatted_summary", "removed_message_count", "source_message_start", "source_message_end"],
        "properties": {
            "summary": {"type": "string"},
            "formatted_summary": {"type": "string"},
            "compacted_session": {"type": "object", "additionalProperties": true},
            "removed_message_count": {"type": "integer", "minimum": 0},
            "source_message_start": {"type": "integer", "minimum": 0},
            "source_message_end": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn slash_dispatch_request_schema() -> Value {
    json!({
        "type": "object",
        "required": ["command"],
        "properties": {
            "command": {"type": "string", "minLength": 1},
            "args": {"type": ["object", "null"], "additionalProperties": true}
        },
        "additionalProperties": false
    })
}

fn slash_dispatch_receipt_schema() -> Value {
    json!({
        "type": "object",
        "required": ["ok", "slash", "id", "action", "status", "data", "executed_at_ms"],
        "properties": {
            "ok": {"type": "boolean"},
            "slash": {"type": "string"},
            "id": {"type": "string"},
            "action": {"type": ["string", "object"]},
            "status": {"type": "string"},
            "data": {},
            "executed_at_ms": {"type": "integer"}
        },
        "additionalProperties": false
    })
}

fn human_entitlement_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "core_profile_id", "app_profiles", "profile_revision", "credential_epoch",
            "ceiling", "granted", "denied"
        ],
        "properties": {
            "core_profile_id": {"type": "string"},
            "app_profiles": {
                "type": "object",
                "additionalProperties": {"type": "string"}
            },
            "profile_revision": {"type": "integer", "minimum": 1},
            "credential_epoch": {"type": "integer", "minimum": 1},
            "ceiling": {"type": "array", "items": {"type": "string"}},
            "granted": {"type": "array", "items": {"type": "string"}},
            "denied": {"type": "array", "items": {"type": "string"}}
        },
        "additionalProperties": false
    })
}

fn auth_verify_response_schema() -> Value {
    json!({
        "type": "object",
        "required": ["valid", "auth_required"],
        "properties": {
            "valid": {"type": "boolean"},
            "auth_required": {"type": "boolean"},
            "transport": {"type": "string", "enum": ["bearer", "browser_session"]},
            "entitlement": {"$ref": "#/components/schemas/HumanEntitlementProjection"}
        },
        "additionalProperties": false
    })
}

fn approval_pending_response_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "filter", "pending", "groups", "pending_count", "approvals"],
        "properties": {
            "kind": {"type": "string", "const": "gateway.unified_approval_pending"},
            "filter": {
                "type": "object",
                "properties": {
                    "session_id": {"type": ["string", "null"]},
                    "domain": {
                        "type": ["string", "null"],
                        "enum": ["execution", "knowledge", "skill", "evolution", "application", "system", null]
                    },
                    "blocks_execution": {"type": ["boolean", "null"]}
                },
                "additionalProperties": false
            },
            "pending": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["approval_id", "domain", "blocks_execution", "status", "action", "summary", "risk", "source", "context", "deadline_elapsed"],
                    "properties": {
                        "approval_id": {"type": "string"},
                        "domain": {"type": "string"},
                        "blocks_execution": {"type": "boolean"},
                        "status": {"type": "string"},
                        "action": {"type": "string"},
                        "summary": {"type": "string"},
                        "risk": {"type": "string"},
                        "deadline_elapsed": {"type": "boolean"},
                        "source": {"type": "object", "additionalProperties": true},
                        "context": {"type": "object", "additionalProperties": true}
                    },
                    "additionalProperties": false
                }
            },
            "groups": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["equivalence_key", "approval_ids", "count", "batch_token", "batch_decision_supported"],
                    "properties": {
                        "equivalence_key": {"type": "object", "additionalProperties": true},
                        "approval_ids": {"type": "array", "items": {"type": "string"}},
                        "count": {"type": "integer", "minimum": 1},
                        "batch_token": {"type": "string"},
                        "batch_decision_supported": {"type": "boolean"}
                    },
                    "additionalProperties": false
                }
            },
            "pending_count": {"type": "integer", "minimum": 0},
            "approvals": {"type": ["object", "null"], "additionalProperties": true}
        },
        "additionalProperties": false
    })
}

fn approval_exact_response_schema() -> Value {
    json!({
        "type": "object",
        "required": ["approval"],
        "properties": {
            "approval": {
                "type": "object",
                "required": ["approval_id", "status", "blocks_execution", "deadline_elapsed", "action", "summary", "risk", "domain"],
                "properties": {
                    "approval_id": {"type": "string"},
                    "status": {"type": "string"},
                    "blocks_execution": {"type": "boolean"},
                    "deadline_elapsed": {"type": "boolean"},
                    "action": {"type": "string"},
                    "summary": {"type": "string"},
                    "risk": {"type": "string"},
                    "domain": {"type": "string"}
                },
                "additionalProperties": false
            }
        },
        "additionalProperties": false
    })
}

fn approval_respond_receipt_schema() -> Value {
    json!({
        "type": "object",
        "required": ["approval_id", "status", "route_back"],
        "properties": {
            "approval_id": {"type": "string"},
            "status": {"type": "string"},
            "route_back": {"type": "object", "additionalProperties": true}
        },
        "additionalProperties": false
    })
}

fn task_list_response_schema() -> Value {
    json!({
        "type": "object", "required": ["tasks"],
        "properties": {"tasks": {"type": "array", "items": {"$ref": "#/components/schemas/TaskAggregate"}}},
        "additionalProperties": false
    })
}

fn task_detail_response_schema() -> Value {
    json!({
        "type": "object", "required": ["task", "turns"],
        "properties": {
            "task": {"$ref": "#/components/schemas/TaskAggregate"},
            "turns": {"type": "array", "items": {"$ref": "#/components/schemas/TaskTurnBinding"}}
        },
        "additionalProperties": false
    })
}

fn task_turns_response_schema() -> Value {
    json!({
        "type": "object", "required": ["task_id", "turns"],
        "properties": {
            "task_id": {"type": "string", "minLength": 1},
            "turns": {"type": "array", "items": {"$ref": "#/components/schemas/TaskTurnBinding"}}
        },
        "additionalProperties": false
    })
}

fn task_focus_projection_schema() -> Value {
    json!({
        "type": "object", "required": ["session_id", "revision", "task_focus"],
        "properties": {
            "session_id": {"type": "string", "minLength": 1},
            "revision": {"type": "integer", "minimum": 0},
            "task_focus": {"oneOf": [
                {"$ref": "#/components/schemas/SessionTaskFocus"}, {"type": "null"}
            ]}
        },
        "additionalProperties": false
    })
}

fn mission_focus_projection_schema() -> Value {
    json!({
        "type": "object", "required": ["session_id", "revision", "mission_focus"],
        "properties": {
            "session_id": {"type": "string", "minLength": 1},
            "revision": {"type": "integer", "minimum": 0},
            "mission_focus": {"oneOf": [
                {"$ref": "#/components/schemas/SessionMissionFocus"}, {"type": "null"}
            ]}
        },
        "additionalProperties": false
    })
}

fn session_task_focus_request_schema() -> Value {
    json!({
        "type": "object", "required": ["task_id", "expected_revision"],
        "properties": {
            "task_id": {"type": "string", "minLength": 1},
            "expected_revision": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn session_mission_focus_request_schema() -> Value {
    json!({
        "type": "object", "required": ["mission_id", "expected_revision"],
        "properties": {
            "mission_id": {"type": "string", "minLength": 1},
            "expected_revision": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn session_focus_clear_request_schema() -> Value {
    json!({
        "type": "object", "required": ["expected_revision"],
        "properties": {"expected_revision": {"type": "integer", "minimum": 0}},
        "additionalProperties": false
    })
}

fn task_focus_request_schema() -> Value {
    json!({
        "type": "object", "required": ["session_id", "expected_revision"],
        "properties": {
            "session_id": {"type": "string", "minLength": 1},
            "expected_revision": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn task_mission_request_schema() -> Value {
    json!({
        "type": "object",
        "required": ["operation_id", "target_mission_id", "assignment", "expected_task_revisions"],
        "properties": {
            "operation_id": {"type": "string", "minLength": 1},
            "task_ids": {"type": "array", "items": {"type": "string", "minLength": 1}},
            "target_mission_id": {"type": "string", "minLength": 1},
            "assignment": {"$ref": "#/components/schemas/TaskMissionAssignment"},
            "expected_task_revisions": {"type": "object", "additionalProperties": {"type": "integer", "minimum": 1}},
            "evidence_refs": {"type": "array", "items": {"$ref": "#/components/schemas/EvidenceRef"}},
            "confirmed": {"type": "boolean", "default": false}
        },
        "additionalProperties": false
    })
}

fn task_mission_preview_response_schema() -> Value {
    json!({
        "type": "object", "required": ["command", "preview"],
        "properties": {
            "command": {"$ref": "#/components/schemas/TaskMissionAssignmentCommand"},
            "preview": {"$ref": "#/components/schemas/TaskMissionAssignmentPreview"}
        },
        "additionalProperties": false
    })
}

fn task_mission_commit_response_schema() -> Value {
    json!({
        "type": "object", "required": ["preview", "receipt"],
        "properties": {
            "preview": {"$ref": "#/components/schemas/TaskMissionAssignmentPreview"},
            "receipt": {"$ref": "#/components/schemas/TaskMissionAssignmentReceipt"}
        },
        "additionalProperties": false
    })
}

fn mission_organization_response_schema() -> Value {
    json!({
        "type": "object", "required": ["decisions"],
        "properties": {"decisions": {"type": "array", "items": {"$ref": "#/components/schemas/MissionOrganizationDecision"}}},
        "additionalProperties": false
    })
}

fn start_task_request_schema() -> Value {
    json!({
        "type": "object",
        "required": ["task_id", "mission_id", "origin_session_id", "origin_turn_id", "objective"],
        "properties": {
            "task_id": {"type": "string", "minLength": 1},
            "mission_id": {"type": "string", "minLength": 1},
            "origin_session_id": {"type": "string", "minLength": 1},
            "origin_turn_id": {"type": "string", "minLength": 1},
            "objective": {"type": "string", "minLength": 1},
            "evidence_refs": {"type": "array", "items": {"$ref": "#/components/schemas/EvidenceRef"}}
        },
        "additionalProperties": false
    })
}

fn start_task_phase_request_schema() -> Value {
    json!({
        "type": "object", "required": ["expected_revision", "name", "objective"],
        "properties": {
            "expected_revision": {"type": "integer", "minimum": 1},
            "name": {"type": "string", "minLength": 1},
            "objective": {"type": "string", "minLength": 1},
            "plan": {"type": "array", "items": {"type": "string"}},
            "acceptance": {"type": "array", "items": {"type": "string"}},
            "test_commands": {"type": "array", "items": {"type": "string"}},
            "evidence_refs": {"type": "array", "items": {"$ref": "#/components/schemas/EvidenceRef"}}
        },
        "additionalProperties": false
    })
}

fn task_phase_artifact_request_schema() -> Value {
    json!({
        "type": "object", "required": ["expected_revision", "label", "value"],
        "properties": {
            "expected_revision": {"type": "integer", "minimum": 1},
            "kind": {"type": "string", "default": "note"},
            "label": {"type": "string", "minLength": 1},
            "value": {"type": "string"},
            "evidence_refs": {"type": "array", "items": {"$ref": "#/components/schemas/EvidenceRef"}}
        },
        "additionalProperties": false
    })
}

fn task_phase_review_request_schema() -> Value {
    json!({
        "type": "object", "required": ["expected_revision", "result"],
        "properties": {
            "expected_revision": {"type": "integer", "minimum": 1},
            "result": {"type": "string", "minLength": 1},
            "completed": {"type": "boolean", "default": false},
            "evidence_refs": {"type": "array", "items": {"$ref": "#/components/schemas/EvidenceRef"}}
        },
        "additionalProperties": false
    })
}

fn task_transition_request_schema() -> Value {
    json!({
        "type": "object", "required": ["expected_revision", "note", "evidence_refs"],
        "properties": {
            "expected_revision": {"type": "integer", "minimum": 1},
            "note": {"type": "string"},
            "evidence_refs": {"type": "array", "items": {"$ref": "#/components/schemas/EvidenceRef"}}
        },
        "additionalProperties": false
    })
}

fn task_failure_request_schema() -> Value {
    json!({
        "type": "object", "required": ["expected_revision", "reason"],
        "properties": {
            "expected_revision": {"type": "integer", "minimum": 1},
            "reason": {"type": "string", "minLength": 1},
            "evidence_refs": {"type": "array", "items": {"$ref": "#/components/schemas/EvidenceRef"}}
        },
        "additionalProperties": false
    })
}

fn openapi_parameters(
    method: &str,
    path: &str,
    stable_metadata: Option<&StableRouteMetadata>,
) -> Vec<Value> {
    let mut params = Vec::new();
    for segment in path.split('/') {
        if let Some(name) = segment.strip_prefix(':') {
            params.push(json!({
                "name": name,
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            }));
        } else if let Some(name) = segment.strip_prefix('*') {
            params.push(json!({
                "name": name,
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            }));
        }
    }
    if path.starts_with("/api/runtime/live") {
        params.push(json!({
            "name": "x-cowd-observer-id",
            "in": "header",
            "required": path != "/api/runtime/live/:id",
            "description": "Surface instance binding. Must match the subscription owner.",
            "schema": {"type": "string", "maxLength": 128}
        }));
    }
    if let Some(policy) = stable_metadata
        .map(|metadata| metadata.session_writer)
        .filter(|policy| *policy != SessionWriterPolicy::None)
    {
        params.push(json!({
            "name": "x-cowd-observer-id",
            "in": "header",
            "required": policy == SessionWriterPolicy::Required,
            "description": if policy == SessionWriterPolicy::Conditional {
                "Required when the operation resolves to a mutation of an authoritative Session."
            } else {
                "Exact attached writer Surface identity. The same observer must own a compatible session lease."
            },
            "schema": {"type": "string", "minLength": 1, "maxLength": 128}
        }));
    }
    if method == "GET" && path == "/api/runtime/live/:id" {
        params.push(json!({
            "name": "surface_instance",
            "in": "query",
            "required": false,
            "description": "Browser EventSource Surface binding. Required when x-cowd-observer-id cannot be sent.",
            "schema": {"type": "string", "maxLength": 128}
        }));
    }
    if method == "GET" && path == "/api/runtime/executions/:id" {
        params.push(json!({
            "name": "detail_scope",
            "in": "query",
            "required": false,
            "description": "Summary is suitable for the chat timeline; full adds audit entities for an opened inspector.",
            "schema": {"type": "string", "enum": ["summary", "full"], "default": "summary"}
        }));
    }
    if method == "GET" && path == "/api/runtime/executions/:id/activity" {
        params.push(json!({
            "name": "activity_id",
            "in": "query",
            "required": true,
            "description": "Stable canonical activity identity from ExecutionProjection.activities.",
            "schema": {"type": "string", "minLength": 1}
        }));
    }
    if method == "GET" && matches!(path, "/api/mission/control" | "/api/mission/control/delta") {
        params.push(json!({
            "name": "mission_id",
            "in": "query",
            "required": false,
            "description": "Mission aggregate selected for this materialized projection.",
            "schema": {"type": "string", "minLength": 1}
        }));
    }
    if method == "GET" && path == "/api/mission/control/delta" {
        params.push(json!({
            "name": "cursor",
            "in": "query",
            "required": false,
            "description": "Last applied Runtime event commit cursor.",
            "schema": {"type": "integer", "minimum": 0, "default": 0}
        }));
        params.push(json!({
            "name": "revision",
            "in": "query",
            "required": false,
            "description": "Last applied Mission materialized-view revision.",
            "schema": {"type": "integer", "minimum": 1}
        }));
    }
    params
}

fn openapi_operation_id(id: &str) -> String {
    sanitize_segment(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema_contains_ref(schema: &Value, expected: &str) -> bool {
        match schema {
            Value::Object(object) => {
                object.get("$ref").and_then(Value::as_str) == Some(expected)
                    || object
                        .values()
                        .any(|value| schema_contains_ref(value, expected))
            }
            Value::Array(values) => values
                .iter()
                .any(|value| schema_contains_ref(value, expected)),
            _ => false,
        }
    }

    #[test]
    fn capability_contract_covers_every_route() {
        let manifest = gateway_route_manifest();
        let contract = gateway_capability_contract();
        assert_eq!(contract.route_count, manifest.len());
        assert_eq!(contract.capability_count, manifest.len());
        assert!(contract.coverage.route_contract_parity);
        assert!(contract.coverage.p1_count > 0);
        assert_eq!(contract.coverage.webui_required_count, 22);
        assert_eq!(contract.coverage.tui_required_count, 11);
        assert_eq!(contract.coverage.ai_tool_count, 0);
        assert!(contract.capabilities.iter().any(|capability| {
            capability.http.path == "/api/gateway/capability-contract"
                && capability.domain == "public"
                && capability.auth == "public"
                && capability.availability.available
                && capability.availability.executable
        }));
        assert!(contract.capabilities.iter().any(|capability| {
            capability.http.path == "/api/cross-plane/summary"
                && capability.domain == "cross_plane"
                && capability.http.criticality == "p1"
                && capability.discoverability.openapi
                && capability.consumed_by.is_empty()
        }));
        assert!(contract.capabilities.iter().any(|capability| {
            capability.http.path == "/api/runtime/managed-agents"
                && capability.domain == "runtime"
                && capability.http.criticality == "p1"
        }));
        assert!(contract.capabilities.iter().any(|capability| {
            capability.http.path == "/api/sessions/search"
                && capability.http.method == "GET"
                && capability.consumed_by == ["webui"]
        }));
        assert!(contract.capabilities.iter().any(|capability| {
            capability.http.path == "/api/sessions/:id/messages"
                && capability.http.method == "POST"
                && capability.consumed_by == ["webui", "tui"]
        }));
    }

    #[test]
    fn openapi_document_is_derived_from_contract() {
        let document = gateway_openapi_document();
        assert_eq!(document["openapi"], "3.1.0");
        assert!(document["paths"]["/api/gateway/capability-contract"]["get"].is_object());
        assert!(document["paths"]["/api/sessions/{id}/messages"]["post"].is_object());
        assert_eq!(
            document["paths"]["/api/sessions/{id}/history-index"]["get"]["operationId"],
            "session_history_index_get"
        );
        assert!(document["components"]["schemas"]["SessionHistoryIndexProjection"].is_object());
        assert!(document["paths"]["/api/runtime/managed-agents"]["get"].is_object());
        assert!(document["paths"]["/api/runtime/managed-agents/{id}/trigger"]["post"].is_object());
        assert!(document["paths"]["/api/surfaces/{id}/trigger-events"]["get"].is_object());
        assert!(document["paths"]["/api/surfaces/{id}/trigger-events/retry"]["post"].is_object());
        assert!(document["paths"]["/api/sessions/:id/messages"].is_null());
        assert_eq!(
            document["x-cowd-contract"]["route_count"],
            document["x-cowd-contract"]["capability_count"]
        );
        assert_eq!(
            document["x-cowd-route-catalog-digest"],
            surface::gateway_api::gateway_route_catalog_digest()
        );
    }

    #[test]
    fn openapi_schema_golden_is_stable() {
        let document = gateway_openapi_document();
        let schemas = document["components"]["schemas"]
            .as_object()
            .expect("OpenAPI schemas");
        let exceptional = surface::gateway_api::EXCEPTIONAL_GATEWAY_SCHEMAS;
        assert_eq!(exceptional.owner, "gateway.api-contract");
        assert_eq!(exceptional.schema_names.len(), 67);
        for name in exceptional.schema_names {
            assert!(
                schemas.contains_key(*name),
                "missing exceptional schema {name}"
            );
        }
    }

    #[test]
    fn session_input_routes_publish_one_typed_failure_projection_contract() {
        let document = gateway_openapi_document();
        for path in [
            "/api/sessions/{id}/input-projection",
            "/api/sessions/{id}/turn-inbox",
            "/api/sessions/{id}/turns/{turn_id}/inbox",
        ] {
            assert!(document["paths"][path]["get"].is_object(), "missing {path}");
        }
        assert_eq!(
            document["paths"]["/api/sessions/{id}/input-projection"]["get"]["responses"]["200"]
                ["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/SessionInputProjection"
        );
        assert_eq!(
            document["paths"]["/api/sessions/{id}/turn-inbox"]["get"]["responses"]["200"]
                ["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/TurnInboxSnapshot"
        );
        let item = &document["components"]["schemas"]["TurnInboxItem"];
        assert_eq!(item["properties"]["failure_class"]["type"][0], "string");
        assert_eq!(item["properties"]["last_error"]["type"][0], "string");
        assert!(schema_contains_ref(
            &item["properties"]["application_receipt"],
            "#/components/schemas/SessionInputApplicationReceipt"
        ));
        assert_eq!(
            document["components"]["schemas"]["SessionInputApplicationReceipt"]["properties"]
                ["state"]["enum"][2],
            "applied"
        );
        assert_eq!(
            document["components"]["schemas"]["SessionInputApplicationReceipt"]["properties"]
                ["target_session_created"]["type"],
            "boolean"
        );
        assert_eq!(
            document["components"]["schemas"]["SessionInputProjection"]["properties"]["inputs"]
                ["items"]["$ref"],
            "#/components/schemas/TurnInboxItem"
        );
    }

    #[test]
    fn disabled_app_is_not_published_to_http_or_ai_contracts() {
        let contract = gateway_capability_contract_for_apps();
        let openapi = gateway_openapi_document_for_apps();

        assert!(contract
            .capabilities
            .iter()
            .all(|capability| !capability.http.path.starts_with("/api/apps/mfg")));
        assert!(openapi["paths"]["/api/apps/mfg/projects"].is_null());
        assert!(openapi["components"]["schemas"]["MfgApiErrorV1"].is_null());
    }

    #[test]
    fn execution_projection_openapi_uses_typed_route_schema_metadata() {
        let document = gateway_openapi_document();
        let snapshot = &document["paths"]["/api/runtime/executions/{id}"]["get"];
        assert_eq!(snapshot["operationId"], "runtime_execution_projection_get");
        assert_eq!(
            snapshot["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ExecutionProjection"
        );
        assert_eq!(
            document["paths"]["/api/sessions/{id}/execution/live"]["get"]["responses"]["200"]
                ["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ExecutionLiveUpdate"
        );
        assert!(document["components"]["schemas"]["ExecutionLiveUpdate"].is_object());
        assert_eq!(
            document["components"]["schemas"]["ExecutionProjection"]["properties"]
                ["child_executions"]["items"]["$ref"],
            "#/components/schemas/ChildExecutionProjection"
        );
        assert!(
            schema_contains_ref(
                &document["components"]["schemas"]["ExecutionProjection"]["properties"]["strategy"],
                "#/components/schemas/StrategyDecisionProjection",
            ),
            "canonical nullable strategy schema must reference StrategyDecisionProjection"
        );
        let collaboration_program = &document["components"]["schemas"]["CollaborationProgram"];
        assert!(
            schema_contains_ref(
                &document["components"]["schemas"]["ExecutionOrchestrationMetadata"]["properties"]
                    ["collaboration_program"],
                "#/components/schemas/CollaborationProgram",
            ),
            "execution projection must expose the typed collaboration program"
        );
        assert_eq!(
            document["components"]["schemas"]["ExecutionOrchestrationMetadata"]["properties"]
                ["collaboration_escalations"]["items"]["$ref"],
            "#/components/schemas/CollaborationEscalationReceipt",
            "execution projection must expose applied escalation receipts as typed facts"
        );
        assert_eq!(
            document["components"]["schemas"]["ExecutionWorkProjection"]["properties"]
                ["scheduling_priority"]["type"],
            "integer",
            "execution projection must expose the durable soft scheduling priority"
        );
        for field in ["control", "semantic_node_instances"] {
            assert!(
                collaboration_program["properties"][field].is_object(),
                "collaboration program must retain {field} in the public projection schema"
            );
        }
        let collaboration_edge = &document["components"]["schemas"]["CollaborationProgramEdge"];
        for field in [
            "input_contract",
            "state",
            "delivery_receipt",
            "claim_receipt",
        ] {
            assert!(
                collaboration_edge["properties"][field].is_object(),
                "collaboration edge must retain {field} in the public projection schema"
            );
        }
        let control = &document["components"]["schemas"]["CollaborationProgramControlState"];
        for field in [
            "lifecycle",
            "obligations",
            "resource_ledger",
            "waiting_relation",
            "blocker_ref",
            "next_action",
        ] {
            assert!(
                control["properties"][field].is_object(),
                "collaboration control must retain {field} in the public projection schema"
            );
        }
        let strategy_required = document["components"]["schemas"]["StrategyDecisionProjection"]
            ["required"]
            .as_array()
            .expect("strategy required fields");
        for field in ["id", "kind", "revision"] {
            assert!(
                strategy_required.iter().any(|value| value == field),
                "strategy schema must require {field}"
            );
        }
        assert_eq!(
            document["components"]["schemas"]["StrategyDecisionProjection"]["properties"]
                ["evidence_refs"]["default"],
            serde_json::json!([])
        );
        assert_eq!(
            document["components"]["schemas"]["StrategyDecisionProjection"]["properties"]
                ["schema_version"]["default"],
            1
        );
        assert!(
            schema_contains_ref(
                &document["components"]["schemas"]["ExecutionGraphProjection"]["properties"]
                    ["parent_execution"],
                "#/components/schemas/ExecutionParentBinding",
            ),
            "canonical nullable parent schema must reference ExecutionParentBinding"
        );
        assert_eq!(
            document["components"]["schemas"]["ExecutionGraphProjection"]["properties"]["edges"]
                ["items"]["$ref"],
            "#/components/schemas/ExecutionEdgeProjection"
        );
        let delta_schema = &document["components"]["schemas"]["ProjectionDelta"];
        assert_eq!(
            delta_schema["properties"]["operations"]["default"],
            serde_json::json!([])
        );
        assert!(schema_contains_ref(
            &delta_schema["properties"]["operations"],
            "#/components/schemas/ProjectionOperation"
        ));
        let golden = &document["x-cowd-projection-v3-golden"];
        assert_eq!(
            golden["delta"]["schema_version"],
            harness_contract::projection::EXECUTION_PROJECTION_SCHEMA_VERSION
        );
        assert_eq!(
            golden["delta"]["reducer_version"],
            harness_contract::projection::EXECUTION_PROJECTION_REDUCER_VERSION
        );

        let events = &document["paths"]["/api/runtime/live/{id}"]["get"];
        assert_eq!(events["operationId"], "runtime_live_stream_get");
        assert_eq!(
            events["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/LiveEnvelope"
        );
        assert_eq!(
            events["responses"]["200"]["content"]["text/event-stream"]["x-cowd-event-schema"]
                ["$ref"],
            "#/components/schemas/LiveEnvelope"
        );
        let live_schema = &document["components"]["schemas"]["LiveEnvelope"];
        assert_eq!(
            live_schema["x-cowd-schema-hash"],
            harness_contract::live::live_envelope_schema_hash()
        );
        assert_eq!(
            live_schema["example"],
            serde_json::to_value(harness_contract::live::canonical_live_envelope_fixture())
                .expect("canonical live fixture")
        );
        assert_eq!(live_schema["additionalProperties"], false);
        assert_eq!(
            live_schema["properties"]["source_kind"]["enum"],
            serde_json::json!(["session", "execution", "mission", "subscription"])
        );

        let command = &document["paths"]["/api/runtime/executions/{id}/commands"]["post"];
        assert_eq!(
            command["operationId"],
            "runtime_execution_projection_command"
        );
        assert_eq!(
            command["requestBody"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ExecutionCommandRequest"
        );
        assert_eq!(
            command["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ExecutionCommandReceipt"
        );
        assert_eq!(command["requestBody"]["required"], true);
        assert!(command["parameters"]
            .as_array()
            .expect("command parameters")
            .iter()
            .any(|parameter| {
                parameter["name"] == "x-cowd-observer-id" && parameter["required"] == true
            }));
        assert_eq!(command["x-cowd"]["session_writer"], "required");
    }

    #[test]
    fn session_writer_and_auth_contracts_are_exact_and_generated_from_route_metadata() {
        let document = gateway_openapi_document();
        for path in [
            "/api/sessions/{id}/messages",
            "/api/sessions/{id}/inputs/{input_id}/cancel",
            "/api/sessions/{id}/inputs/{input_id}/reclassify",
            "/api/sessions/{id}/cancel",
            "/api/sessions/{id}/compact",
            "/api/runtime/executions/{id}/commands",
        ] {
            let operation = &document["paths"][path]["post"];
            let observer = operation["parameters"]
                .as_array()
                .expect("writer parameters")
                .iter()
                .find(|parameter| parameter["name"] == "x-cowd-observer-id")
                .expect("writer observer parameter");
            assert_eq!(observer["required"], true, "{path}");
            assert_eq!(operation["x-cowd"]["session_writer"], "required", "{path}");
            assert!(operation["responses"]["403"].is_object(), "{path}");
            assert!(operation["responses"]["409"].is_object(), "{path}");
        }

        let slash = &document["paths"]["/api/slash/dispatch"]["post"];
        let observer = slash["parameters"]
            .as_array()
            .expect("slash parameters")
            .iter()
            .find(|parameter| parameter["name"] == "x-cowd-observer-id")
            .expect("conditional slash observer parameter");
        assert_eq!(observer["required"], false);
        assert_eq!(slash["x-cowd"]["session_writer"], "conditional");

        let verify = &document["paths"]["/api/auth/verify"]["get"];
        assert_eq!(
            verify["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/AuthVerifyResponse"
        );
        assert_eq!(
            document["components"]["schemas"]["AuthVerifyResponse"]["properties"]["entitlement"]
                ["$ref"],
            "#/components/schemas/HumanEntitlementProjection"
        );
    }

    #[test]
    fn dynamic_app_openapi_advertises_only_generic_gateway_paths() {
        let document = gateway_openapi_document_for_apps();
        let paths = document["paths"].as_object().expect("OpenAPI paths");
        assert!(paths.contains_key("/api/apps"));
        assert!(paths.contains_key("/api/apps/{app_id}"));
        assert!(paths.contains_key("/api/apps/{app_id}/logs"));
        assert!(paths.contains_key("/api/apps/{app_id}/restart"));
        assert!(paths.contains_key("/api/apps/{app_id}/operations/{operation_id}/invoke"));
        assert!(paths.keys().all(|path| !path.starts_with("/api/apps/mfg")));
        assert!(document["components"]["schemas"]["MfgApiErrorV1"].is_null());
    }

    #[test]
    fn cached_openapi_projection_is_exactly_equal_to_the_uncached_authority() {
        let authority = gateway_openapi_document_for_apps_uncached();
        let first = gateway_openapi_document_for_apps();
        let second = gateway_openapi_document_for_apps();
        assert_eq!(first, authority);
        assert_eq!(second, authority);
        assert_eq!(
            first["x-cowd-route-catalog-digest"],
            authority["x-cowd-route-catalog-digest"]
        );
        assert_eq!(
            first["paths"].as_object().map(|paths| paths.len()),
            Some(441)
        );
    }

    #[test]
    fn openai_tools_are_the_runtime_tool_catalog() {
        let catalog = tools::ToolCatalog::builtin();
        let tools = gateway_openai_tools(&catalog);
        let tool_list = tools["tools"].as_array().expect("tools array");
        let definitions = catalog.definitions(None);
        assert_eq!(tool_list.len(), definitions.len());
        assert_eq!(tools["tool_count"], tool_list.len());
        assert_eq!(tools["source"], "runtime.tool_catalog");
        assert!(tool_list.iter().all(|tool| tool["type"] == "function"));
        assert!(tool_list.iter().all(|tool| {
            tool["function"]["name"].as_str().is_some()
                && tool["function"]["parameters"]["type"] == "object"
        }));
        let names = tool_list
            .iter()
            .filter_map(|tool| tool["function"]["name"].as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), tool_list.len());
        let expected = definitions
            .iter()
            .map(|definition| definition.name.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(names, expected);
    }
}
