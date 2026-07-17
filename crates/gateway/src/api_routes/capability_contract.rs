use std::{
    collections::BTreeSet,
    hash::{Hash, Hasher},
};

use serde::Serialize;
use serde_json::{json, Map, Value};

use super::{
    route_manifest::{gateway_route_manifest, GatewayRouteManifestEntry},
    route_registry::stable_route_metadata,
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
    let routes = gateway_route_manifest();
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
    ] {
        schemas.insert(name.to_string(), schema);
    }
    schemas.extend(mfg_openapi_components());

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

fn mfg_openapi_components() -> Map<String, Value> {
    let mut registry = app_mfg_contract::MfgOpenApiSchemaRegistry::canonical();
    super::mfg_routes::register_mfg_openapi_schemas(&mut registry);
    registry.register_type::<app_mfg::MfgApplicationDescriptor>("MfgApplicationDescriptor");
    registry.register_type::<app_mfg::MfgIncident>("MfgIncident");
    registry.register_type::<app_mfg::MfgOperationalAnalysis>("MfgOperationalAnalysis");
    registry.register_type::<app_mfg::MfgActionExecution>("MfgActionExecution");
    registry.register_type::<app_mfg::MfgActionFeedback>("MfgActionFeedback");
    registry.register_type::<app_mfg::MfgCockpitProfile>("MfgCockpitProfile");
    registry.register_type::<app_mfg::MfgCockpitProjection>("MfgCockpitProjection");
    registry.register_type::<app_mfg::MfgCockpitWidgetProjection>("MfgCockpitWidgetProjection");
    registry.register_type::<app_mfg::MfgCockpitReportSnapshot>("MfgCockpitReportSnapshot");
    registry
        .register_type::<app_mfg::MfgCockpitReportDeliveryState>("MfgCockpitReportDeliveryState");
    registry.register_type::<app_mfg::MfgAlertRule>("MfgAlertRule");
    registry.register_type::<app_mfg::MfgAlertOccurrence>("MfgAlertOccurrence");
    registry.register_type::<app_mfg::MfgAlertSubscription>("MfgAlertSubscription");
    registry.register_type::<app_mfg::MfgAssignment>("MfgAssignment");
    registry.register_type::<app_mfg::MfgForecastProjection>("MfgForecastProjection");
    registry.register_type::<app_mfg::MfgSkillManifest>("MfgSkillManifest");
    registry.register_type::<app_mfg::MfgSkillRun>("MfgSkillRun");
    registry.register_type::<app_mfg::MfgMemoryCase>("MfgMemoryCase");
    registry.register_type::<app_mfg::MfgPlaybook>("MfgPlaybook");
    registry.register_type::<matrix_core::MatrixDataPlaneHealth>("MatrixDataPlaneHealth");
    registry.register_type::<matrix_core::MatrixDataPlaneIngestPlan>("MatrixDataPlaneIngestPlan");
    registry.register_type::<matrix_core::MatrixSourcePack>("MatrixSourcePack");
    registry.register_type::<matrix_core::MatrixSourcePackValidation>("MatrixSourcePackValidation");
    registry.register_type::<matrix_core::MatrixSourceDeltaPlan>("MatrixSourceDeltaPlan");
    registry.register_type::<matrix_core::MatrixConnectorRun>("MatrixConnectorRun");
    registry.register_type::<matrix_core::MatrixMetricDefinition>("MatrixMetricDefinition");
    registry.register_type::<matrix_core::MatrixMetricSnapshot>("MatrixMetricSnapshot");
    registry.register_type::<matrix_core::MatrixMetricLineage>("MatrixMetricLineage");
    registry.register_type::<matrix_core::MatrixMetricAttentionPlan>("MatrixMetricAttentionPlan");
    registry.register_type::<matrix_core::MatrixComputeJob>("MatrixComputeJob");
    registry.register_type::<matrix_core::MatrixEntity>("MatrixEntity");
    registry.register_type::<matrix_core::MatrixRelation>("MatrixRelation");
    registry.register_type::<matrix_core::MatrixFact>("MatrixFact");
    registry.register_type::<matrix_core::MatrixChangeEvent>("MatrixChangeEvent");
    registry.register_type::<matrix_core::MatrixAttentionItem>("MatrixAttentionItem");
    registry.register_type::<matrix_core::MatrixEvidencePacket>("MatrixEvidencePacket");
    registry.register_type::<matrix_core::MatrixQualityGateDecision>("MatrixQualityGateDecision");

    for route in app_mfg_contract::mfg_route_contracts() {
        let response_ref = mfg_response_schema_ref(&route);
        registry.register_schema(
            route.response_schema,
            json!({"$ref": format!("#/components/schemas/{response_ref}")}),
        );
        let request_ref = super::mfg_routes::mfg_request_schema_component(route.route_id);
        registry.register_schema(
            route.request_schema,
            json!({"$ref": format!("#/components/schemas/{request_ref}")}),
        );
    }
    registry.into_components().into_iter().collect()
}

fn mfg_response_schema_ref(route: &app_mfg_contract::MfgRouteContract) -> &'static str {
    use app_mfg_contract::MfgRouteId as R;
    match route.route_id {
        R::ContractGet => "MfgFrontendContractV1",
        R::AppGet => "MfgApplicationDescriptor",
        R::ReportReviewList => "MfgReportDeliveryReviewCollection",
        R::ReportReviewGet => "MfgReportDeliveryReview",
        R::LiveSnapshot => "MfgLiveEnvelopeV1",
        _ if matches!(route.class, app_mfg_contract::MfgMutationClass::Read) => "MfgReadResponseV1",
        _ => "MfgMutationResponseV1",
    }
}

pub(crate) fn gateway_openai_tools() -> Value {
    let contract = gateway_capability_contract();
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
    let action = match route.method {
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
    let parameters = openapi_parameters(&capability.http.path);
    if !parameters.is_empty() {
        operation.insert("parameters".to_string(), Value::Array(parameters));
    }
    if capability.http.method != "GET" && capability.http.method != "DELETE" {
        let request_schema =
            stable_request_schema(capability).unwrap_or_else(|| capability.input_schema.clone());
        let mut request_content = Map::new();
        request_content.insert(
            "application/json".to_string(),
            json!({"schema": request_schema.clone()}),
        );
        if !capability.http.path.starts_with("/api/apps/mfg/") {
            request_content.insert(
                "multipart/form-data".to_string(),
                json!({"schema": request_schema}),
            );
        }
        operation.insert(
            "requestBody".to_string(),
            json!({
                "required": false,
                "content": Value::Object(request_content)
            }),
        );
    }
    let response_schema =
        stable_response_schema(capability).unwrap_or_else(|| capability.output_schema.clone());
    let mut content = Map::new();
    content.insert(
        "application/json".to_string(),
        json!({"schema": response_schema}),
    );
    if stable_metadata
        .as_ref()
        .is_some_and(|metadata| metadata.streaming)
    {
        let event_schema = stable_metadata
            .as_ref()
            .map(|metadata| metadata.response_schema.as_str())
            .unwrap_or("ProjectionDelta");
        content.insert(
            "text/event-stream".to_string(),
            json!({
                "schema": {"type": "string", "format": "event-stream"},
                "x-cowd-event-schema": {"$ref": format!("#/components/schemas/{event_schema}")}
            }),
        );
    }
    let responses = if capability.http.path.starts_with("/api/apps/mfg/") {
        json!({
            "200": {
                "description": "Successful Gateway response",
                "content": Value::Object(content)
            },
            "400": mfg_openapi_error_response("Bad request"),
            "401": mfg_openapi_error_response("Unauthorized"),
            "403": mfg_openapi_error_response("Capability or scope denied"),
            "404": mfg_openapi_error_response("Resource is outside the verified scope"),
            "409": mfg_openapi_error_response("Revision or idempotency conflict"),
            "429": mfg_openapi_error_response("Rate limited"),
            "500": mfg_openapi_error_response("Gateway internal error")
        })
    } else {
        json!({
            "200": {
                "description": "Successful Gateway response",
                "content": Value::Object(content)
            },
            "400": {"description": "Bad request"},
            "401": {"description": "Unauthorized"},
            "500": {"description": "Gateway internal error"}
        })
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
        }),
    );
    Value::Object(operation)
}

fn mfg_openapi_error_response(description: &str) -> Value {
    json!({
        "description": description,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/MfgApiErrorV1"}
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
            "strategy": {"oneOf": [{"$ref": "#/components/schemas/ExecutionProjectionEntity"}, {"type": "null"}]},
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

fn openapi_parameters(path: &str) -> Vec<Value> {
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
            document["components"]["schemas"]["ExecutionGraphProjection"]["properties"]
                ["parent_execution"]["oneOf"][0]["$ref"],
            "#/components/schemas/ExecutionParentBinding"
        );
        assert_eq!(
            document["components"]["schemas"]["ExecutionGraphProjection"]["properties"]["edges"]
                ["items"]["$ref"],
            "#/components/schemas/ExecutionEdgeProjection"
        );

        let events = &document["paths"]["/api/runtime/executions/{id}/events"]["get"];
        assert_eq!(events["operationId"], "runtime_execution_projection_events");
        assert_eq!(
            events["responses"]["200"]["content"]["application/json"]["schema"]["$ref"],
            "#/components/schemas/ProjectionDelta"
        );
        assert_eq!(
            events["responses"]["200"]["content"]["text/event-stream"]["x-cowd-event-schema"]
                ["$ref"],
            "#/components/schemas/ProjectionDelta"
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
    }

    #[test]
    fn active_mfg_openapi_uses_only_named_contract_components() {
        let document = gateway_openapi_document();
        let schemas = document["components"]["schemas"]
            .as_object()
            .expect("OpenAPI schemas");
        let active = app_mfg_contract::mfg_route_contracts()
            .into_iter()
            .filter(|route| route.availability == app_mfg_contract::MfgActionAvailability::Active)
            .collect::<Vec<_>>();
        assert_eq!(active.len(), 104);

        for route in active {
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
