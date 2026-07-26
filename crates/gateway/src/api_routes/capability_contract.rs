use std::{
    collections::BTreeSet,
    hash::{Hash, Hasher},
};

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
    ai_visible_count: usize,
    openapi_path_count: usize,
    openai_tool_count: usize,
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
    surface_visibility: GatewayCapabilityVisibility,
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
struct GatewayCapabilityVisibility {
    webui: bool,
    tui: bool,
    llm: bool,
    edge: bool,
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

pub(crate) fn gateway_capability_contract_for_apps(
    app_registry: &cowd_app_host::AppRegistry,
) -> GatewayCapabilityContract {
    gateway_capability_contract_from_routes(gateway_route_manifest_for_apps(app_registry))
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
    let openai_tool_count = capabilities
        .iter()
        .filter(|capability| capability.ai_affordance.expose_as_tool)
        .count();
    let coverage = GatewayCapabilityCoverage {
        route_count: routes.len(),
        capability_count: capabilities.len(),
        p1_count: capabilities
            .iter()
            .filter(|capability| capability.http.criticality == "p1")
            .count(),
        ai_visible_count: capabilities
            .iter()
            .filter(|capability| capability.surface_visibility.llm)
            .count(),
        openapi_path_count,
        openai_tool_count,
        route_contract_parity: routes.len() == capabilities.len(),
    };

    GatewayCapabilityContract {
        kind: "gateway.capability_contract",
        schema_version: 1,
        owner: "gateway",
        source: "crates/gateway/src/api_routes/capability_contract.rs",
        route_count: routes.len(),
        capability_count: capabilities.len(),
        coverage,
        capabilities,
    }
}

pub(crate) fn gateway_openapi_document() -> Value {
    let contract = gateway_capability_contract();
    gateway_openapi_document_from_contract(contract, Map::new())
}

pub(crate) fn gateway_openapi_document_for_apps(
    app_registry: &cowd_app_host::AppRegistry,
) -> Value {
    let contract = gateway_capability_contract_for_apps(app_registry);
    gateway_openapi_document_from_contract(
        contract,
        app_registry.openapi_components().into_iter().collect(),
    )
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
        (
            "ExecutionProjectionEntity",
            execution_projection_entity_schema(),
        ),
        (
            "StrategyCandidateEstimate",
            strategy_candidate_estimate_schema(),
        ),
        (
            "StrategyResourceSnapshot",
            strategy_resource_snapshot_schema(),
        ),
        (
            "StrategyEvidenceScopeProjection",
            strategy_evidence_scope_schema(),
        ),
        ("StrategyTransitionProjection", strategy_transition_schema()),
        ("StrategyActualProjection", strategy_actual_schema()),
        (
            "StrategyDecisionProjection",
            strategy_decision_projection_schema(),
        ),
        (
            "ExecutionNodeProjection",
            execution_node_projection_schema(),
        ),
        (
            "ExecutionEdgeProjection",
            execution_edge_projection_schema(),
        ),
        ("ExecutionParentBinding", execution_parent_binding_schema()),
        (
            "ExecutionGraphProjection",
            execution_graph_projection_schema(),
        ),
        (
            "ChildExecutionProjection",
            child_execution_projection_schema(),
        ),
        ("ContextComponentUsage", context_component_usage_schema()),
        ("ContextUsageProjection", context_usage_projection_schema()),
        ("RunMetricsProjection", run_metrics_projection_schema()),
        ("ExecutionLiveState", execution_live_state_schema()),
        ("ExecutionProjection", execution_projection_schema()),
        (
            "SessionExecutionIndexProjection",
            session_execution_index_projection_schema(),
        ),
        (
            "SessionExecutionIndicesProjection",
            session_execution_indices_projection_schema(),
        ),
        ("EvidenceFreshness", evidence_freshness_schema()),
        ("TurnEvidenceProjection", turn_evidence_projection_schema()),
        (
            "SessionEvidenceProjection",
            session_evidence_projection_schema(),
        ),
        ("ProjectionEvent", projection_event_schema()),
        ("ProjectionDelta", projection_delta_schema()),
        (
            "ExecutionCommandRequest",
            execution_command_request_schema(),
        ),
        (
            "ExecutionCommandReceipt",
            execution_command_receipt_schema(),
        ),
        ("SendMessageRequest", send_message_request_schema()),
        ("SendMessageReceipt", send_message_receipt_schema()),
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
            "CreateLiveSubscriptionRequest",
            live_create_request_schema(),
        ),
        ("PatchLiveSubscriptionRequest", live_patch_request_schema()),
        ("LiveSubscription", live_subscription_schema()),
        ("LiveEnvelope", live_envelope_schema()),
        ("Empty", json!({"type": "object", "maxProperties": 0})),
    ] {
        schemas.insert(name.to_string(), schema);
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
        }
    })
}

pub(crate) fn gateway_openai_tools() -> Value {
    let contract = gateway_capability_contract();
    gateway_openai_tools_from_contract(contract)
}

pub(crate) fn gateway_openai_tools_for_apps(app_registry: &cowd_app_host::AppRegistry) -> Value {
    let contract = gateway_capability_contract_for_apps(app_registry);
    gateway_openai_tools_from_contract(contract)
}

fn gateway_openai_tools_from_contract(contract: GatewayCapabilityContract) -> Value {
    let mut seen_names = BTreeSet::new();
    let tools = contract
        .capabilities
        .iter()
        .filter(|capability| capability.ai_affordance.expose_as_tool)
        .map(|capability| {
            let mut tool = openai_tool(capability);
            let base_name = tool["function"]["name"]
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| openai_tool_name(&capability.id));
            let unique_name = unique_openai_tool_name(&base_name, &capability.id, &mut seen_names);
            if let Some(function) = tool["function"].as_object_mut() {
                function.insert("name".to_string(), Value::String(unique_name));
            }
            tool
        })
        .collect::<Vec<_>>();

    json!({
        "kind": "gateway.openai_tools",
        "schema_version": 1,
        "source": "gateway.capability_contract",
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
    let visibility = surface_visibility(route, &risk);
    let id = capability_id(&domain, route);
    let title = capability_title(route, &domain);
    let description = capability_description(route, &domain);
    let tool_name = openai_tool_name(&id);
    let expose_as_tool = expose_as_tool(route, &risk, &visibility);
    let ai_affordance = GatewayCapabilityAiAffordance {
        expose_as_tool,
        tool_name: expose_as_tool.then_some(tool_name),
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
        surface_visibility: visibility,
        ai_affordance,
        input_schema,
        output_schema,
        tests,
        app: route.app.clone(),
    }
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

fn surface_visibility(
    route: &GatewayRouteManifestEntry,
    risk: &str,
) -> GatewayCapabilityVisibility {
    let domain = capability_domain(route);
    let webui = route.path.starts_with("/api/") || route.path.starts_with("/s/");
    let tui = matches!(
        domain.as_str(),
        "runtime"
            | "session"
            | "session.message"
            | "mission"
            | "agent"
            | "task"
            | "context"
            | "memory"
            | "reality"
            | "matrix"
            | "tool"
            | "skill"
            | "surface"
            | "edge"
            | "connector"
            | "cross_plane"
            | "resource"
            | "approval"
            | "workspace"
            | "profile"
            | "public"
            | "slash"
    );
    let llm = matches!(
        domain.as_str(),
        "runtime"
            | "session"
            | "session.message"
            | "mission"
            | "agent"
            | "task"
            | "context"
            | "memory"
            | "reality"
            | "matrix"
            | "tool"
            | "skill"
            | "surface"
            | "edge"
            | "connector"
            | "cross_plane"
            | "resource"
            | "workspace"
            | "approval"
            | "slash"
    ) && risk != "destructive";
    let edge = matches!(
        domain.as_str(),
        "surface" | "edge" | "connector" | "resource"
    );

    GatewayCapabilityVisibility {
        webui,
        tui,
        llm,
        edge,
    }
}

fn expose_as_tool(
    route: &GatewayRouteManifestEntry,
    risk: &str,
    visibility: &GatewayCapabilityVisibility,
) -> bool {
    if !visibility.llm
        || risk == "destructive"
        || risk == "admin"
        || route.path.starts_with("/s/")
        || route.path.ends_with("/stream")
        || route.path.contains("/upload")
        || route.path.contains("/download")
        || route.path.contains("/raw")
    {
        return false;
    }
    route.method == "GET"
        || matches!(
            route.path.as_str(),
            "/api/sessions/:id/messages"
                | "/api/runtime/turns"
                | "/api/tools/batch-readonly"
                | "/api/tools/intent-plan"
                | "/api/tools/context-fanout-plan"
                | "/api/slash/resolve"
                | "/api/mission/route"
                | "/api/skills/:id/actions/validate"
                | "/api/skills/:id/actions/plan"
        )
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

fn openai_tool_name(id: &str) -> String {
    let name = sanitize_segment(id);
    if name.len() <= 64 {
        return name;
    }
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut hasher);
    let suffix = hasher.finish().to_string();
    let prefix_len = 64usize.saturating_sub(suffix.len() + 1);
    format!("{}_{}", &name[..prefix_len], suffix)
}

fn unique_openai_tool_name(base: &str, id: &str, seen: &mut BTreeSet<String>) -> String {
    if seen.insert(base.to_string()) {
        return base.to_string();
    }

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    id.hash(&mut hasher);
    let suffix = hasher.finish().to_string();
    let prefix_len = 64usize.saturating_sub(suffix.len() + 1);
    let candidate = format!("{}_{}", &base[..base.len().min(prefix_len)], suffix);
    if seen.insert(candidate.clone()) {
        return candidate;
    }

    let mut index = 2usize;
    loop {
        let indexed_suffix = format!("{suffix}_{index}");
        let prefix_len = 64usize.saturating_sub(indexed_suffix.len() + 1);
        let candidate = format!("{}_{}", &base[..base.len().min(prefix_len)], indexed_suffix);
        if seen.insert(candidate.clone()) {
            return candidate;
        }
        index += 1;
    }
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
    if capability.http.method != "GET" && capability.http.method != "DELETE" {
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
            "ai_visible": capability.surface_visibility.llm,
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

fn execution_projection_entity_schema() -> Value {
    json!({
        "type": "object",
        "required": ["id", "kind", "revision", "evidence_refs"],
        "properties": {
            "id": {"type": "string"},
            "kind": {"type": "string"},
            "revision": {"type": "integer", "minimum": 0},
            "status": {"type": ["string", "null"]},
            "summary": {"type": ["string", "null"]},
            "evidence_refs": {"type": "array", "items": {"type": "string"}},
            "detail": {"type": ["object", "array", "string", "number", "boolean", "null"]}
        },
        "additionalProperties": false
    })
}

fn strategy_candidate_estimate_schema() -> Value {
    json!({
        "type": "object",
        "required": ["candidate", "eligible", "estimated_serial_ms", "estimated_critical_path_ms", "startup_overhead_ms", "context_duplication_tokens", "merge_cost_ms", "evidence_overlap_penalty_bp", "provider_concurrency_penalty_bp", "risk_approval_penalty_bp", "expected_quality_lift_bp", "duration_calibration_source", "duration_sample_count", "quality_calibration_source", "quality_sample_count", "duration_provenance", "token_provenance", "quality_provenance", "risk_provenance", "reasons"],
        "properties": {
            "candidate": {"type": "string", "enum": ["direct", "parallel_tools", "team"]},
            "eligible": {"type": "boolean"},
            "estimated_serial_ms": {"type": "integer", "minimum": 0},
            "estimated_critical_path_ms": {"type": "integer", "minimum": 0},
            "startup_overhead_ms": {"type": "integer", "minimum": 0},
            "context_duplication_tokens": {"type": "integer", "minimum": 0},
            "merge_cost_ms": {"type": "integer", "minimum": 0},
            "evidence_overlap_penalty_bp": {"type": "integer", "minimum": 0, "maximum": 10000},
            "provider_concurrency_penalty_bp": {"type": "integer", "minimum": 0, "maximum": 10000},
            "risk_approval_penalty_bp": {"type": "integer", "minimum": 0, "maximum": 10000},
            "expected_quality_lift_bp": {"type": "integer"},
            "duration_calibration_source": {"type": "string"},
            "duration_sample_count": {"type": "integer", "minimum": 0},
            "quality_calibration_source": {"type": "string"},
            "quality_sample_count": {"type": "integer", "minimum": 0},
            "duration_provenance": {"type": "string", "enum": ["observed", "calibrated", "assumed", "unknown"]},
            "token_provenance": {"type": "string", "enum": ["observed", "calibrated", "assumed", "unknown"]},
            "quality_provenance": {"type": "string", "enum": ["observed", "calibrated", "assumed", "unknown"]},
            "risk_provenance": {"type": "string", "enum": ["observed", "calibrated", "assumed", "unknown"]},
            "reasons": {"type": "array", "items": {"type": "string"}}
        },
        "additionalProperties": false
    })
}

fn strategy_resource_snapshot_schema() -> Value {
    json!({
        "type": "object",
        "required": ["version", "provider_available", "tools_available", "team_available", "provider_concurrency", "tool_concurrency", "team_slots", "provider_concurrency_penalty_bp", "sample_source", "sample_count", "provenance"],
        "properties": {
            "version": {"type": "string"},
            "provider_available": {"type": "boolean"},
            "tools_available": {"type": "boolean"},
            "team_available": {"type": "boolean"},
            "provider_concurrency": {"type": "integer", "minimum": 0},
            "tool_concurrency": {"type": "integer", "minimum": 0},
            "team_slots": {"type": "integer", "minimum": 0},
            "provider_concurrency_penalty_bp": {"type": "integer", "minimum": 0, "maximum": 10000},
            "sample_source": {"type": "string"},
            "sample_count": {"type": "integer", "minimum": 0},
            "provenance": {"type": "string", "enum": ["observed", "calibrated", "assumed", "unknown"]}
        },
        "additionalProperties": false
    })
}

fn strategy_evidence_scope_schema() -> Value {
    json!({
        "type": "object",
        "required": ["role_id", "focus_id", "responsibility_summary", "capability_cropped_refs", "scope_hash", "overlap_budget_bp", "novelty_target_bp"],
        "properties": {
            "role_id": {"type": "string"},
            "focus_id": {"type": "string"},
            "responsibility_summary": {"type": "string"},
            "capability_cropped_refs": {"type": "array", "items": {"type": "string"}},
            "scope_hash": {"type": "string"},
            "overlap_budget_bp": {"type": "integer", "minimum": 0, "maximum": 10000},
            "novelty_target_bp": {"type": "integer", "minimum": 0, "maximum": 10000}
        },
        "additionalProperties": false
    })
}

fn strategy_transition_schema() -> Value {
    json!({
        "type": "object",
        "required": ["revision", "kind", "status", "summary"],
        "properties": {
            "revision": {"type": "integer", "minimum": 0},
            "kind": {"type": "string", "enum": ["runtime.strategy.downgraded", "runtime.strategy.early_stopped"]},
            "status": {"type": "string"},
            "summary": {"type": "string"}
        },
        "additionalProperties": false
    })
}

fn strategy_actual_schema() -> Value {
    json!({
        "type": "object",
        "required": ["duration_ms", "input_tokens", "output_tokens", "cached_tokens", "tool_calls", "duplicate_tool_calls", "max_tool_concurrency_observed", "parallel_tool_batches", "write_attempt_refs", "evidence_overlap_bp", "evidence_overlap_observed", "working_state_verified", "merge_cost_ms", "parent_merge_count", "evaluation_token_limit", "evaluation_tokens_consumed", "evaluation_budget_observed", "evaluation_budget_breached", "terminal_reason"],
        "properties": {
            "duration_ms": {"type": "integer", "minimum": 0},
            "input_tokens": {"type": "integer", "minimum": 0},
            "output_tokens": {"type": "integer", "minimum": 0},
            "cached_tokens": {"type": "integer", "minimum": 0},
            "tool_calls": {"type": "integer", "minimum": 0},
            "duplicate_tool_calls": {"type": "integer", "minimum": 0},
            "max_tool_concurrency_observed": {"type": "integer", "minimum": 0},
            "parallel_tool_batches": {"type": "integer", "minimum": 0},
            "write_attempt_refs": {"type": "array", "items": {"type": "string"}},
            "evidence_overlap_bp": {"type": "integer", "minimum": 0, "maximum": 10000},
            "evidence_overlap_observed": {"type": "boolean"},
            "working_state_verified": {"type": "boolean"},
            "merge_cost_ms": {"type": "integer", "minimum": 0},
            "parent_merge_count": {"type": "integer", "minimum": 0},
            "evaluation_token_limit": {"type": "integer", "minimum": 0},
            "evaluation_tokens_consumed": {"type": "integer", "minimum": 0},
            "evaluation_budget_observed": {"type": "boolean"},
            "evaluation_budget_breached": {"type": "boolean"},
            "quality_score_bp": {"type": ["integer", "null"], "minimum": 0, "maximum": 10000},
            "actual_speedup_ratio_bp": {"type": ["integer", "null"], "minimum": 0},
            "terminal_reason": {"type": "string"}
        },
        "additionalProperties": false
    })
}

fn strategy_decision_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["id", "kind", "revision", "evidence_refs"],
        "properties": {
            "schema_version": {"type": "integer", "const": 1},
            "id": {"type": "string"},
            "kind": {"type": "string"},
            "revision": {"type": "integer", "minimum": 0},
            "status": {"type": ["string", "null"]},
            "summary": {"type": ["string", "null"]},
            "evidence_refs": {"type": "array", "items": {"type": "string"}},
            "detail": {"type": ["object", "array", "string", "number", "boolean", "null"]},
            "decision_id": {"type": ["string", "null"]},
            "execution_id": {"type": ["string", "null"]},
            "session_id": {"type": ["string", "null"]},
            "turn_id": {"type": ["string", "null"]},
            "selected_candidate": {"type": ["string", "null"], "enum": ["direct", "parallel_tools", "team", null]},
            "selected_pattern": {"type": ["string", "null"], "enum": ["direct", "explore", "execute", "deliberate", "collaborate", "supervise", null]},
            "candidate_estimates": {"type": "array", "items": {"$ref": "#/components/schemas/StrategyCandidateEstimate"}},
            "benefit_reason": {"type": "array", "items": {"type": "string"}},
            "cost_reason": {"type": "array", "items": {"type": "string"}},
            "evidence_scopes": {"type": "array", "items": {"$ref": "#/components/schemas/StrategyEvidenceScopeProjection"}},
            "downgrade": {"type": "array", "items": {"$ref": "#/components/schemas/StrategyTransitionProjection"}},
            "early_stop": {"type": "array", "items": {"$ref": "#/components/schemas/StrategyTransitionProjection"}},
            "estimated": {"oneOf": [{"$ref": "#/components/schemas/StrategyCandidateEstimate"}, {"type": "null"}]},
            "actual": {"oneOf": [{"$ref": "#/components/schemas/StrategyActualProjection"}, {"type": "null"}]},
            "resource_snapshot": {"oneOf": [{"$ref": "#/components/schemas/StrategyResourceSnapshot"}, {"type": "null"}]},
            "policy_version": {"type": ["string", "null"]},
            "source": {"type": ["string", "null"], "enum": ["deterministic", "model_validated", "experience_adapted", "resource_adapted", null]},
            "confidence": {"type": ["integer", "null"], "minimum": 0, "maximum": 100},
            "proof_status": {"type": ["string", "null"], "enum": ["not_proven", "calibrated", null]},
            "actual_status": {"type": ["string", "null"], "enum": ["unknown", "observed", null]},
            "team_id": {"type": ["string", "null"]},
            "team_execution_id": {"type": ["string", "null"]}
        },
        "additionalProperties": false
    })
}

fn execution_node_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["node_id", "kind", "status", "executor_kind", "evidence_refs"],
        "properties": {
            "node_id": {"type": "string"},
            "kind": {"type": "string"},
            "status": {"type": "string"},
            "executor_kind": {"type": "string"},
            "result_ref": {"type": ["string", "null"]},
            "evidence_refs": {"type": "array", "items": {"type": "string"}}
        },
        "additionalProperties": false
    })
}

fn execution_graph_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["graph_id", "revision", "objective", "nodes", "edges", "commit_cursor"],
        "properties": {
            "graph_id": {"type": "string"},
            "revision": {"type": "integer", "minimum": 0},
            "objective": {"type": "string"},
            "parent_execution": {"oneOf": [{"$ref": "#/components/schemas/ExecutionParentBinding"}, {"type": "null"}]},
            "nodes": {"type": "array", "items": {"$ref": "#/components/schemas/ExecutionNodeProjection"}},
            "edges": {"type": "array", "items": {"$ref": "#/components/schemas/ExecutionEdgeProjection"}},
            "commit_cursor": {"type": "integer", "minimum": 0},
            "terminal_result_ref": {"type": ["string", "null"]}
        },
        "additionalProperties": false
    })
}

fn execution_edge_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["from", "to", "kind"],
        "properties": {
            "from": {"type": "string"},
            "to": {"type": "string"},
            "kind": {"type": "string", "enum": ["depends_on", "verifies", "produces"]}
        },
        "additionalProperties": false
    })
}

fn execution_parent_binding_schema() -> Value {
    json!({
        "type": "object",
        "required": ["execution_id", "node_id"],
        "properties": {
            "execution_id": {"type": "string"},
            "node_id": {"type": "string"}
        },
        "additionalProperties": false
    })
}

fn child_execution_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["execution_id", "parent_execution_id", "parent_node_id", "revision", "cursor", "status", "objective"],
        "properties": {
            "execution_id": {"type": "string"},
            "parent_execution_id": {"type": "string"},
            "parent_node_id": {"type": "string"},
            "revision": {"type": "integer", "minimum": 0},
            "cursor": {"type": "integer", "minimum": 0},
            "status": {"type": "string"},
            "objective": {"type": "string"}
        },
        "additionalProperties": false
    })
}

fn context_component_usage_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "tokens", "occurrences"],
        "properties": {
            "kind": {"type": "string"},
            "tokens": {"type": "integer", "minimum": 0},
            "occurrences": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn context_usage_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["components"],
        "properties": {
            "model": {"type": ["string", "null"]},
            "window_tokens": {"type": ["integer", "null"], "minimum": 0},
            "window_source": {"type": ["string", "null"]},
            "input_tokens": {"type": ["integer", "null"], "minimum": 0},
            "input_source": {"type": ["string", "null"]},
            "remaining_tokens": {"type": ["integer", "null"], "minimum": 0},
            "usage_percent_bp": {"type": ["integer", "null"], "minimum": 0, "maximum": 10000},
            "request_sequence": {"type": ["integer", "null"], "minimum": 0},
            "components": {"type": "array", "items": {"$ref": "#/components/schemas/ContextComponentUsage"}}
        },
        "additionalProperties": false
    })
}

fn run_metrics_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["tool_calls", "memory_recalls", "memory_evidence", "approvals", "context_items", "files_touched", "input_tokens", "output_tokens", "total_tokens"],
        "properties": {
            "tool_calls": {"type": "integer", "minimum": 0},
            "memory_recalls": {"type": "integer", "minimum": 0},
            "memory_evidence": {"type": "integer", "minimum": 0},
            "approvals": {"type": "integer", "minimum": 0},
            "context_items": {"type": "integer", "minimum": 0},
            "files_touched": {"type": "integer", "minimum": 0},
            "input_tokens": {"type": "integer", "minimum": 0},
            "output_tokens": {"type": "integer", "minimum": 0},
            "total_tokens": {"type": "integer", "minimum": 0}
        },
        "additionalProperties": false
    })
}

fn execution_live_state_schema() -> Value {
    json!({
        "type": "object",
        "required": ["revision", "status", "started_at_ms", "updated_at_ms", "last_progress_at_ms", "metrics"],
        "properties": {
            "revision": {"type": "integer", "minimum": 0},
            "status": {"type": "string", "enum": ["queued", "preparing_context", "calling_model", "thinking", "calling_tool", "waiting_approval", "finalizing", "complete", "cancelled", "error"]},
            "status_detail": {"type": ["string", "null"]},
            "turn_id": {"type": ["string", "null"]},
            "started_at_ms": {"type": "integer", "minimum": 0},
            "updated_at_ms": {"type": "integer", "minimum": 0},
            "last_progress_at_ms": {"type": "integer", "minimum": 0},
            "context_usage": {"oneOf": [{"$ref": "#/components/schemas/ContextUsageProjection"}, {"type": "null"}]},
            "metrics": {"$ref": "#/components/schemas/RunMetricsProjection"},
            "output_preview": {"type": ["string", "null"]},
            "terminal_ref": {"type": ["string", "null"]},
            "error": {"type": ["string", "null"]}
        },
        "additionalProperties": false
    })
}

fn session_execution_index_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["session_id", "active_execution_ids"],
        "properties": {
            "session_id": {"type": "string"},
            "active_execution_ids": {"type": "array", "items": {"type": "string"}},
            "latest_execution_id": {"type": ["string", "null"]},
            "latest_status": {"type": ["string", "null"], "enum": ["queued", "preparing_context", "calling_model", "thinking", "calling_tool", "waiting_approval", "finalizing", "complete", "cancelled", "error", null]},
            "latest_live_revision": {"type": ["integer", "null"], "minimum": 0},
            "last_progress_at_ms": {"type": ["integer", "null"], "minimum": 0},
            "terminal_ref": {"type": ["string", "null"]}
        },
        "additionalProperties": false
    })
}

fn session_execution_indices_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["items"],
        "properties": {"items": {"type": "array", "items": {"$ref": "#/components/schemas/SessionExecutionIndexProjection"}}},
        "additionalProperties": false
    })
}

fn evidence_freshness_schema() -> Value {
    json!({"type": "string", "enum": ["live", "durable", "unavailable"]})
}

fn turn_evidence_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["session_id", "turn_id", "input_message_id", "execution_id", "evidence_refs", "freshness"],
        "properties": {
            "session_id": {"type": "string"},
            "turn_id": {"type": "string"},
            "input_message_id": {"type": "string"},
            "execution_id": {"type": "string"},
            "terminal_ref": {"type": ["string", "null"]},
            "assistant_message_id": {"type": ["string", "null"]},
            "context_report_id": {"type": ["string", "null"]},
            "evidence_refs": {"type": "array", "items": {"type": "string"}},
            "freshness": {"$ref": "#/components/schemas/EvidenceFreshness"}
        },
        "additionalProperties": false
    })
}

fn session_evidence_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["session_id", "evidence_refs", "turns", "freshness"],
        "properties": {
            "session_id": {"type": "string"},
            "evidence_refs": {"type": "array", "items": {"type": "string"}},
            "turns": {"type": "array", "items": {"$ref": "#/components/schemas/TurnEvidenceProjection"}},
            "freshness": {"$ref": "#/components/schemas/EvidenceFreshness"}
        },
        "additionalProperties": false
    })
}

fn execution_projection_schema() -> Value {
    json!({
        "type": "object",
        "required": ["schema_version", "execution_id", "revision", "cursor", "graph", "child_executions", "goals", "agents", "teams", "relations", "approvals", "interventions", "usage", "context", "evidence", "health", "recovery", "available_commands"],
        "properties": {
            "schema_version": {"type": "integer", "const": 1},
            "execution_id": {"type": "string"},
            "revision": {"type": "integer", "minimum": 0},
            "cursor": {"type": "integer", "minimum": 0},
            "session_id": {"type": ["string", "null"]},
            "mission_id": {"type": ["string", "null"]},
            "strategy": {"oneOf": [{"$ref": "#/components/schemas/StrategyDecisionProjection"}, {"type": "null"}]},
            "graph": {"$ref": "#/components/schemas/ExecutionGraphProjection"},
            "child_executions": {"type": "array", "items": {"$ref": "#/components/schemas/ChildExecutionProjection"}},
            "goals": projection_entity_list_schema(),
            "agents": projection_entity_list_schema(),
            "teams": projection_entity_list_schema(),
            "relations": projection_entity_list_schema(),
            "approvals": projection_entity_list_schema(),
            "interventions": projection_entity_list_schema(),
            "usage": projection_entity_list_schema(),
            "context": projection_entity_list_schema(),
            "evidence": projection_entity_list_schema(),
            "health": projection_entity_list_schema(),
            "recovery": projection_entity_list_schema(),
            "live": {"oneOf": [{"$ref": "#/components/schemas/ExecutionLiveState"}, {"type": "null"}]},
            "available_commands": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["command", "available"],
                    "properties": {
                        "command": {"type": "string", "enum": ["pause", "resume", "cancel", "replan"]},
                        "available": {"type": "boolean"},
                        "reason": {"type": ["string", "null"]}
                    },
                    "additionalProperties": false
                }
            }
        },
        "additionalProperties": false
    })
}

fn projection_entity_list_schema() -> Value {
    json!({"type": "array", "items": {"$ref": "#/components/schemas/ExecutionProjectionEntity"}})
}

fn projection_event_schema() -> Value {
    json!({
        "type": "object",
        "required": ["commit_cursor", "transaction_index", "event_id", "kind"],
        "properties": {
            "commit_cursor": {"type": "integer", "minimum": 0},
            "transaction_index": {"type": "integer", "minimum": 0},
            "event_id": {"type": "string"},
            "kind": {"type": "string"},
            "entity": {"oneOf": [{"$ref": "#/components/schemas/ExecutionProjectionEntity"}, {"type": "null"}]}
        },
        "additionalProperties": false
    })
}

fn projection_delta_schema() -> Value {
    json!({
        "type": "object",
        "required": ["schema_version", "execution_id", "base_cursor", "target_cursor", "events"],
        "properties": {
            "schema_version": {"type": "integer", "const": 1},
            "execution_id": {"type": "string"},
            "base_cursor": {"type": "integer", "minimum": 0},
            "target_cursor": {"type": "integer", "minimum": 0},
            "events": {"type": "array", "items": {"$ref": "#/components/schemas/ProjectionEvent"}}
        },
        "additionalProperties": false
    })
}

fn execution_command_request_schema() -> Value {
    json!({
        "type": "object",
        "required": ["command_id", "expected_revision", "command", "payload"],
        "properties": {
            "command_id": {"type": "string", "minLength": 1},
            "expected_revision": {"type": "integer", "minimum": 0},
            "command": {"type": "string", "enum": ["pause", "resume", "cancel", "replan"]},
            "payload": {}
        },
        "additionalProperties": false
    })
}

fn execution_command_receipt_schema() -> Value {
    json!({
        "type": "object",
        "required": ["command_id", "accepted_revision", "status"],
        "properties": {
            "command_id": {"type": "string"},
            "accepted_revision": {"type": "integer", "minimum": 0},
            "status": {"type": "string", "enum": ["accepted", "rejected_stale_revision"]},
            "reason": {"type": ["string", "null"]}
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
            "input_projection": {"type": ["object", "null"], "additionalProperties": true},
            "turn_inbox": {"type": ["object", "null"], "additionalProperties": true}
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
            "input_projection": {"type": ["object", "null"], "additionalProperties": true},
            "turn_inbox": {"type": ["object", "null"], "additionalProperties": true}
        },
        "additionalProperties": false
    })
}

fn cancel_session_turn_request_schema() -> Value {
    json!({
        "type": "object",
        "properties": {"reason": {"type": ["string", "null"]}},
        "additionalProperties": false
    })
}

fn cancel_session_turn_receipt_schema() -> Value {
    json!({
        "type": "object",
        "required": ["ok", "session_id", "status", "actor_id", "reason", "aborted", "execution_ids"],
        "properties": {
            "ok": {"type": "boolean", "const": true},
            "session_id": {"type": "string"},
            "status": {"type": "string", "const": "cancel_requested"},
            "actor_id": {"type": "string"},
            "reason": {"type": "string"},
            "aborted": {"type": "boolean"},
            "run_id": {"type": ["string", "null"]},
            "execution_ids": {"type": "array", "items": {"type": "string"}}
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
                "Required when slash dispatch resolves to a mutating command with an authoritative session_id."
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
    params
}

fn openapi_operation_id(id: &str) -> String {
    sanitize_segment(id)
}

fn openai_tool(capability: &GatewayCapability) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": capability.ai_affordance.tool_name.clone().unwrap_or_else(|| openai_tool_name(&capability.id)),
            "description": format!(
                "{} Use `{}` {}. {}",
                capability.ai_affordance.when_to_use,
                capability.http.method,
                capability.http.path,
                capability.ai_affordance.cautions.join(" ")
            ),
            "parameters": capability.input_schema,
        },
        "x-cowd": {
            "capability_id": capability.id,
            "domain": capability.domain,
            "risk": capability.risk,
            "http": capability.http,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capability_contract_covers_every_route() {
        let manifest = gateway_route_manifest();
        let contract = gateway_capability_contract();
        assert_eq!(contract.route_count, manifest.len());
        assert_eq!(contract.capability_count, manifest.len());
        assert!(contract.coverage.route_contract_parity);
        assert!(contract.coverage.p1_count > 0);
        assert!(contract.coverage.ai_visible_count > 0);
        assert!(contract.capabilities.iter().any(|capability| {
            capability.http.path == "/api/gateway/capability-contract"
                && capability.domain == "public"
                && capability.auth == "public"
        }));
        assert!(contract.capabilities.iter().any(|capability| {
            capability.http.path == "/api/cross-plane/summary"
                && capability.domain == "cross_plane"
                && capability.http.criticality == "p1"
                && capability.surface_visibility.llm
        }));
        assert!(contract.capabilities.iter().any(|capability| {
            capability.http.path == "/api/runtime/managed-agents"
                && capability.domain == "runtime"
                && capability.http.criticality == "p1"
        }));
    }

    #[test]
    fn openapi_document_is_derived_from_contract() {
        let document = gateway_openapi_document();
        assert_eq!(document["openapi"], "3.1.0");
        assert!(document["paths"]["/api/gateway/capability-contract"]["get"].is_object());
        assert!(document["paths"]["/api/sessions/{id}/messages"]["post"].is_object());
        assert!(document["paths"]["/api/runtime/managed-agents"]["get"].is_object());
        assert!(document["paths"]["/api/runtime/managed-agents/{id}/trigger"]["post"].is_object());
        assert!(document["paths"]["/api/surfaces/{id}/trigger-events"]["get"].is_object());
        assert!(document["paths"]["/api/surfaces/{id}/trigger-events/retry"]["post"].is_object());
        assert!(document["paths"]["/api/sessions/:id/messages"].is_null());
        assert_eq!(
            document["x-cowd-contract"]["route_count"],
            document["x-cowd-contract"]["capability_count"]
        );
    }

    #[test]
    fn disabled_app_is_not_published_to_http_or_ai_contracts() {
        let app_registry = cowd_app_host::AppRegistry::default();
        let contract = gateway_capability_contract_for_apps(&app_registry);
        let openapi = gateway_openapi_document_for_apps(&app_registry);
        let tools = gateway_openai_tools_for_apps(&app_registry);

        assert!(contract
            .capabilities
            .iter()
            .all(|capability| !capability.http.path.starts_with("/api/apps/mfg")));
        assert!(openapi["paths"]["/api/apps/mfg/projects"].is_null());
        assert!(openapi["components"]["schemas"]["MfgApiErrorV1"].is_null());
        assert!(tools["tools"]
            .as_array()
            .expect("tools array")
            .iter()
            .all(|tool| !tool["x-cowd"]["http"]["path"]
                .as_str()
                .is_some_and(|path| path.starts_with("/api/apps/mfg"))));
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
            document["components"]["schemas"]["ExecutionProjection"]["properties"]
                ["child_executions"]["items"]["$ref"],
            "#/components/schemas/ChildExecutionProjection"
        );
        assert_eq!(
            document["components"]["schemas"]["ExecutionProjection"]["properties"]["strategy"]
                ["oneOf"][0]["$ref"],
            "#/components/schemas/StrategyDecisionProjection"
        );
        assert_eq!(
            document["components"]["schemas"]["StrategyDecisionProjection"]["required"],
            serde_json::json!(["id", "kind", "revision", "evidence_refs"])
        );
        assert_eq!(
            document["components"]["schemas"]["StrategyDecisionProjection"]["properties"]
                ["schema_version"]["const"],
            1
        );
        assert_eq!(
            document["components"]["schemas"]["ExecutionGraphProjection"]["properties"]
                ["parent_execution"]["oneOf"][0]["$ref"],
            "#/components/schemas/ExecutionParentBinding"
        );
        assert_eq!(
            document["components"]["schemas"]["ExecutionGraphProjection"]["properties"]["edges"]
                ["items"]["$ref"],
            "#/components/schemas/ExecutionEdgeProjection"
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
    fn registered_app_openapi_uses_only_named_contract_components() {
        let services = crate::services::GatewayServices::baseline();
        let document = gateway_openapi_document_for_apps(services.app_registry.as_ref());
        let schemas = document["components"]["schemas"]
            .as_object()
            .expect("OpenAPI schemas");
        let active = services
            .app_registry
            .route_metadata()
            .into_iter()
            .filter(|registered| registered.app_id.as_str() == "mfg" && registered.route.active)
            .collect::<Vec<_>>();
        let route_ids = active
            .iter()
            .map(|registered| registered.route.route_id.as_str())
            .collect::<BTreeSet<_>>();
        assert!(!active.is_empty());
        assert_eq!(route_ids.len(), active.len());

        for registered in active {
            let route = registered.route;
            let path = openapi_path(&route.path);
            let method = route.method.to_ascii_lowercase();
            let operation = &document["paths"][&path][&method];
            assert!(operation.is_object(), "missing {} {}", route.method, path);
            let response_ref = operation["responses"]["200"]["content"]["application/json"]
                ["schema"]["$ref"]
                .as_str()
                .expect("MFG response must use a named schema");
            assert_eq!(
                response_ref,
                format!("#/components/schemas/{}", route.response_schema)
            );
            assert!(
                schemas.contains_key(&route.response_schema),
                "missing response component {}",
                route.response_schema
            );
            let response_alias = &schemas[&route.response_schema];
            assert!(
                response_alias.get("$ref").is_some(),
                "response alias must not be an anonymous object: {}",
                route.response_schema
            );

            assert!(
                schemas.contains_key(&route.request_schema),
                "missing request component {}",
                route.request_schema
            );
            let request_alias = &schemas[&route.request_schema];
            assert!(
                request_alias.get("$ref").is_some(),
                "request alias must not be an anonymous object: {}",
                route.request_schema
            );
            if route.method != "GET" && route.method != "DELETE" {
                assert_eq!(
                    operation["requestBody"]["content"]["application/json"]["schema"]["$ref"],
                    format!("#/components/schemas/{}", route.request_schema)
                );
                assert!(
                    operation["requestBody"]["content"]
                        .get("multipart/form-data")
                        .is_none(),
                    "MFG route must not advertise an unwired multipart transport"
                );
            }
            for status in ["400", "401", "403", "404", "409", "429", "500"] {
                assert_eq!(
                    operation["responses"][status]["content"]["application/json"]["schema"]["$ref"],
                    "#/components/schemas/MfgApiErrorV1"
                );
            }
        }
    }

    #[test]
    fn openai_tools_are_safe_subset_with_function_schema() {
        let tools = gateway_openai_tools();
        let tool_list = tools["tools"].as_array().expect("tools array");
        assert!(!tool_list.is_empty());
        assert_eq!(tools["tool_count"], tool_list.len());
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
        assert!(names.iter().all(|name| name.len() <= 64));
        assert!(tool_list.iter().all(|tool| {
            let path = tool["x-cowd"]["http"]["path"].as_str().unwrap_or_default();
            let risk = tool["x-cowd"]["risk"].as_str().unwrap_or_default();
            risk != "destructive"
                && risk != "admin"
                && !path.starts_with("/s/")
                && !path.ends_with("/stream")
                && !path.contains("/upload")
                && !path.contains("/download")
                && !path.contains("/raw")
        }));
        assert!(tool_list.iter().any(|tool| {
            tool["x-cowd"]["capability_id"]
                .as_str()
                .is_some_and(|id| id.contains("runtime"))
        }));
    }
}
