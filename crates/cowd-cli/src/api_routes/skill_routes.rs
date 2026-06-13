use std::sync::Arc;

use axum::{
    extract::{Path as AxumPath, Query, State as AxumState},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use commands::{SkillInfo, SkillRegistry, SkillRouter};
use runtime::{server_manufacturing_skill_pack, IaccSkillManifest};
use serde::{Deserialize, Serialize};

use super::{api_error, AppState, ErrorResponse};

pub(super) fn router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/skills/catalog", get(skills_catalog_handler))
        .route("/api/skills/projection", get(skills_projection_handler))
        .route("/api/skills/:id", get(skill_get_handler))
}

#[derive(Debug, Deserialize)]
struct SkillCatalogQuery {
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SkillProjectionQuery {
    #[serde(default)]
    surface: Option<String>,
    #[serde(default)]
    query: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SkillCatalogItem {
    id: String,
    name: String,
    description: Option<String>,
    scope: String,
    source: String,
    domain: Option<String>,
    status: String,
    risk: String,
    tags: Vec<String>,
    tools: Vec<String>,
    required_evidence: Vec<String>,
    capabilities: Vec<String>,
    path: Option<String>,
    shadowed_by: Option<String>,
}

#[derive(Debug, Serialize)]
struct SkillAction {
    id: &'static str,
    label: &'static str,
    surface: &'static str,
    mutation: bool,
}

#[derive(Debug, Serialize)]
struct SkillProjection {
    kind: &'static str,
    surface: String,
    catalog_count: usize,
    capabilities: Vec<&'static str>,
    actions: Vec<SkillAction>,
    facets: SkillProjectionFacets,
    queue: SkillProjectionQueue,
    governance: SkillProjectionGovernance,
    diagnostics: Vec<String>,
    activation: Option<serde_json::Value>,
    items: Vec<SkillCatalogItem>,
}

#[derive(Debug, Serialize)]
struct SkillProjectionFacets {
    scopes: Vec<String>,
    domains: Vec<String>,
    tags: Vec<String>,
    risks: Vec<String>,
    statuses: Vec<String>,
}

#[derive(Debug, Serialize)]
struct SkillProjectionQueue {
    source: &'static str,
    run_list_endpoint: &'static str,
    supports_watch: bool,
}

#[derive(Debug, Serialize)]
struct SkillProjectionGovernance {
    evidence_model: &'static str,
    tool_fact_model: &'static str,
    approval_model: &'static str,
}

async fn skills_catalog_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<SkillCatalogQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let items = collect_skill_catalog(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let items = filter_scope(items, query.scope.as_deref());

    Ok(Json(serde_json::json!({
        "kind": "skills.catalog",
        "schema_version": 1,
        "items": items,
    })))
}

async fn skills_projection_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    Query(query): Query<SkillProjectionQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let surface = normalize_surface(query.surface.as_deref());
    let items = collect_skill_catalog(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let activation = activation_projection(&state, query.query.as_deref()).map_err(|error| {
        api_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("skill activation failed: {error}"),
        )
    })?;
    let projection = SkillProjection {
        kind: "skills.projection",
        surface: surface.clone(),
        catalog_count: items.len(),
        capabilities: projection_capabilities(&surface),
        actions: projection_actions(&surface),
        facets: projection_facets(&items),
        queue: SkillProjectionQueue {
            source: "iacc.skill_runs",
            run_list_endpoint: "/api/iacc/incidents/:incident_id/skills",
            supports_watch: surface != "cli",
        },
        governance: SkillProjectionGovernance {
            evidence_model: "iacc.evidence.packet + agent_evidence + tool_invocation",
            tool_fact_model: "tool.execution_plan + tool.invocation.runtime_event",
            approval_model: "quality_gate + cross_plane_policy",
        },
        diagnostics: projection_diagnostics(&items),
        activation,
        items,
    };

    Ok(Json(projection))
}

async fn skill_get_handler(
    AxumState(state): AxumState<Arc<AppState>>,
    AxumPath(id): AxumPath<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let items = collect_skill_catalog(&state)
        .map_err(|error| api_error(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let item = items
        .into_iter()
        .find(|item| item.id == id || item.name.eq_ignore_ascii_case(&id))
        .ok_or_else(|| api_error(StatusCode::NOT_FOUND, "skill not found"))?;

    Ok(Json(serde_json::json!({
        "kind": "skills.detail",
        "schema_version": 1,
        "skill": item,
    })))
}

fn collect_skill_catalog(state: &AppState) -> std::io::Result<Vec<SkillCatalogItem>> {
    let mut items = server_manufacturing_skill_pack()
        .into_iter()
        .map(iacc_skill_catalog_item)
        .collect::<Vec<_>>();

    let registry = SkillRegistry::discover(&state.workspace_root);
    for skill in registry.list()? {
        items.push(local_skill_catalog_item(skill));
    }

    items.sort_by(|left, right| {
        left.scope
            .cmp(&right.scope)
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(items)
}

fn iacc_skill_catalog_item(skill: IaccSkillManifest) -> SkillCatalogItem {
    let risk = iacc_risk(&skill).to_string();
    let tags = vec![skill.domain.clone(), "iacc".to_string()];
    let capabilities = skill
        .output_actions
        .iter()
        .chain(skill.input_fact_types.iter())
        .cloned()
        .collect();
    SkillCatalogItem {
        id: format!("iacc:{}", skill.skill_id),
        name: skill.skill_id,
        description: Some(skill.role),
        scope: "iacc".to_string(),
        source: "runtime.iacc.server_manufacturing".to_string(),
        domain: Some(skill.domain),
        status: "ready".to_string(),
        risk,
        tags,
        tools: skill.tools,
        required_evidence: skill.required_evidence,
        capabilities,
        path: None,
        shadowed_by: None,
    }
}

fn local_skill_catalog_item(skill: SkillInfo) -> SkillCatalogItem {
    SkillCatalogItem {
        id: format!("local:{}", skill.name),
        name: skill.name,
        description: skill.description,
        scope: "local".to_string(),
        source: format!("{:?}", skill.source),
        domain: None,
        status: if skill.shadowed_by.is_some() {
            "shadowed".to_string()
        } else {
            "ready".to_string()
        },
        risk: "operator_review".to_string(),
        tags: skill.tags,
        tools: Vec::new(),
        required_evidence: Vec::new(),
        capabilities: skill.related_skills,
        path: Some(skill.path.display().to_string()),
        shadowed_by: skill.shadowed_by.map(|source| format!("{source:?}")),
    }
}

fn iacc_risk(skill: &IaccSkillManifest) -> &'static str {
    if skill
        .output_actions
        .iter()
        .any(|action| action.contains("dispatch") || action.contains("escalation"))
    {
        "controlled"
    } else if skill.tools.iter().any(|tool| tool.contains("cross_plane")) {
        "governed"
    } else {
        "review"
    }
}

fn filter_scope(items: Vec<SkillCatalogItem>, scope: Option<&str>) -> Vec<SkillCatalogItem> {
    match scope.unwrap_or("all") {
        "all" => items,
        scope => items
            .into_iter()
            .filter(|item| item.scope == scope || item.source.contains(scope))
            .collect(),
    }
}

fn normalize_surface(surface: Option<&str>) -> String {
    match surface.unwrap_or("webui").to_ascii_lowercase().as_str() {
        "tui" => "tui".to_string(),
        "cli" => "cli".to_string(),
        _ => "webui".to_string(),
    }
}

fn projection_capabilities(surface: &str) -> Vec<&'static str> {
    match surface {
        "cli" => vec![
            "catalog.read",
            "skill.view",
            "skill.import",
            "diagnostics.read",
        ],
        "tui" => vec![
            "catalog.read",
            "skill.view",
            "skill.validate",
            "skill.plan",
            "skill.run",
            "run.watch",
            "evidence.summary",
            "governance.queue",
        ],
        _ => vec![
            "catalog.read",
            "skill.view",
            "skill.validate",
            "skill.plan",
            "skill.run",
            "run.watch",
            "evidence.timeline",
            "evidence.diff",
            "governance.bulk",
            "imports.guided",
            "telemetry.dashboard",
        ],
    }
}

fn projection_actions(surface: &str) -> Vec<SkillAction> {
    let mut actions = vec![
        SkillAction {
            id: "view",
            label: "View",
            surface: "all",
            mutation: false,
        },
        SkillAction {
            id: "validate",
            label: "Validate",
            surface: "webui,tui",
            mutation: false,
        },
    ];
    if surface != "cli" {
        actions.extend([
            SkillAction {
                id: "plan",
                label: "Plan",
                surface: "webui,tui",
                mutation: false,
            },
            SkillAction {
                id: "run",
                label: "Run",
                surface: "webui,tui",
                mutation: true,
            },
            SkillAction {
                id: "watch",
                label: "Watch",
                surface: "webui,tui",
                mutation: false,
            },
        ]);
    }
    if surface == "webui" {
        actions.push(SkillAction {
            id: "bulk_govern",
            label: "Bulk Govern",
            surface: "webui",
            mutation: true,
        });
    }
    actions
}

fn projection_facets(items: &[SkillCatalogItem]) -> SkillProjectionFacets {
    let mut scopes = Vec::new();
    let mut domains = Vec::new();
    let mut tags = Vec::new();
    let mut risks = Vec::new();
    let mut statuses = Vec::new();
    for item in items {
        push_unique(&mut scopes, item.scope.clone());
        if let Some(domain) = &item.domain {
            push_unique(&mut domains, domain.clone());
        }
        for tag in &item.tags {
            push_unique(&mut tags, tag.clone());
        }
        push_unique(&mut risks, item.risk.clone());
        push_unique(&mut statuses, item.status.clone());
    }
    SkillProjectionFacets {
        scopes,
        domains,
        tags,
        risks,
        statuses,
    }
}

fn projection_diagnostics(items: &[SkillCatalogItem]) -> Vec<String> {
    let mut diagnostics = Vec::new();
    if !items.iter().any(|item| item.scope == "iacc") {
        diagnostics.push("iacc_skill_pack_unavailable".to_string());
    }
    if !items.iter().any(|item| item.scope == "local") {
        diagnostics.push("local_skill_registry_empty".to_string());
    }
    diagnostics
}

fn activation_projection(
    state: &AppState,
    query: Option<&str>,
) -> std::io::Result<Option<serde_json::Value>> {
    let Some(query) = query.filter(|value| !value.trim().is_empty()) else {
        return Ok(None);
    };
    let registry = SkillRegistry::discover(&state.workspace_root);
    let result = SkillRouter::new(registry).suggest(query)?;
    Ok(Some(serde_json::json!({
        "kind": "skills.activation",
        "query": result.query,
        "selected": result.selected,
        "candidates": result.candidates,
    })))
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.contains(&value) {
        values.push(value);
    }
}
