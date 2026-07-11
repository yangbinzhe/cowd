//! Runtime capability manifest and prompt primer.
//!
//! The manifest makes Cowd's higher-level harness affordances visible to the
//! model without coupling tool implementations back into runtime.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::context_runtime::ContextProfile;
use crate::evidence_planner::EvidencePlan;
use crate::execution_core::{
    action_selection_report_for_decision, build_runtime_execution_decision,
    execution_pattern_catalog_response, rewoo_plan_for_intent_with_evidence_plan,
    runtime_orchestration_action_guidance, runtime_orchestration_actions, tool_dag_from_rewoo,
    RuntimeExecutionDecision,
};
use crate::EvolutionAppliedCapabilityRecord;
use crate::TeamTemplateRegistry;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapability {
    pub id: String,
    pub summary: String,
    pub recommended_tools: Vec<String>,
    pub when_to_use: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilityManifest {
    pub name: String,
    pub capabilities: Vec<RuntimeCapability>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeCapabilityCatalog {
    pub name: String,
    pub templates: Vec<RuntimeTemplateSummary>,
    pub operation_groups: Vec<RuntimeOperationGroup>,
    pub action_contracts: Vec<RuntimeActionContract>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeTemplateSummary {
    pub template_id: String,
    pub protocol_id: String,
    pub protocol_version: u32,
    pub availability: String,
    pub requires_review: bool,
    pub best_for: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOperationGroup {
    pub id: String,
    pub summary: String,
    pub operations: Vec<RuntimeOperation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOperation {
    pub id: String,
    pub owner: String,
    pub summary: String,
    pub model_intent: String,
    pub validation_gate: String,
    pub output_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeActionContract {
    pub runtime_action: String,
    pub tool_action: String,
    pub when_to_use: String,
    pub required_intent_fields: Vec<String>,
    pub validation: String,
    pub expected_projection: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveEvolutionCapabilityOverlay {
    pub active_count: usize,
    pub rolled_back_count: usize,
    pub capabilities: Vec<ActiveEvolutionCapabilitySummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveEvolutionCapabilitySummary {
    pub version_id: String,
    pub candidate_id: String,
    pub kind: String,
    pub adapter: String,
    pub owner: String,
    pub status: String,
    pub goal_ids: Vec<String>,
    pub policy_effects: Vec<String>,
    pub evidence_refs: Vec<String>,
}

impl ActiveEvolutionCapabilityOverlay {
    #[must_use]
    pub fn from_records(records: &[EvolutionAppliedCapabilityRecord]) -> Self {
        let capabilities = records
            .iter()
            .map(|record| ActiveEvolutionCapabilitySummary {
                version_id: record.version_id.clone(),
                candidate_id: record.candidate_id.clone(),
                kind: record.kind.clone(),
                adapter: record.adapter.clone(),
                owner: record.owner.clone(),
                status: record.status.clone(),
                goal_ids: record.goal_ids.clone(),
                policy_effects: record.policy_effects.clone(),
                evidence_refs: record.artifact_refs.clone(),
            })
            .collect::<Vec<_>>();
        Self {
            active_count: capabilities
                .iter()
                .filter(|capability| capability.status == "active")
                .count(),
            rolled_back_count: capabilities
                .iter()
                .filter(|capability| capability.status == "rolled_back")
                .count(),
            capabilities,
        }
    }

    #[must_use]
    pub fn active_summaries(&self, limit: usize) -> Vec<String> {
        self.capabilities
            .iter()
            .filter(|capability| capability.status == "active")
            .take(limit)
            .map(|capability| {
                format!(
                    "{}:{} via {} effects={}",
                    capability.kind,
                    capability.version_id,
                    capability.adapter,
                    if capability.policy_effects.is_empty() {
                        "none".to_string()
                    } else {
                        capability.policy_effects.join("|")
                    }
                )
            })
            .collect()
    }
}

impl RuntimeCapabilityCatalog {
    #[must_use]
    pub fn current() -> Self {
        let templates = TeamTemplateRegistry::all()
            .into_iter()
            .map(|template| RuntimeTemplateSummary {
                template_id: template.template_id.as_str().to_string(),
                protocol_id: template.protocol_id.to_string(),
                protocol_version: template.version,
                availability: match template.availability {
                    harness_contract::team::TeamTemplateAvailability::Available => "available",
                    harness_contract::team::TeamTemplateAvailability::Unavailable => "unavailable",
                }
                .to_string(),
                requires_review: template.requires_review,
                best_for: template_best_for(template.template_id),
            })
            .collect();
        Self {
            name: "cowd-runtime-capability-catalog".to_string(),
            templates,
            operation_groups: vec![
                operation_group(
                    "execution_graph",
                    "Build, validate, schedule, and inspect DAG-based team work.",
                    vec![
                        operation(
                            "build_execution_graph",
                            "runtime.team_builder",
                            "Convert a collaboration template into a validated ExecutionGraph with verify and synthesis nodes.",
                            "I need ordered or parallel team work with visible dependencies.",
                            "ExecutionGraph quality must be DAG-valid and policy-approved before dispatch.",
                            &["execution_graph_id", "team_id", "agent_task_bindings"],
                        ),
                        operation(
                            "inspect_execution_graph",
                            "runtime.execution_graph_runner",
                            "Read canonical team progress; ExecutionGraphRunner alone advances ready nodes.",
                            "The team already exists and progress or terminal evidence is needed.",
                            "No second team scheduler may mutate graph state.",
                            &["execution_graph_id", "node_statuses", "terminal_result_ref"],
                        ),
                    ],
                ),
                operation_group(
                    "team",
                    "Start, inspect, control, and synthesize runtime-owned agent teams.",
                    vec![
                        operation(
                            "use_team_template",
                            "runtime.team_runtime",
                            "Start the lightest suitable built-in collaboration template.",
                            "The task has independent roles, review needs, or multi-domain work.",
                            "Runtime strategy, risk, session binding, and policy gates must accept the request.",
                            &["team_id", "template_id", "agent_runs", "control_actions"],
                        ),
                        operation(
                            "request_verification",
                            "runtime.team_runtime",
                            "Attach reviewer/background verification without turning tools into lifecycle owners.",
                            "The result needs independent review or regression evidence.",
                            "Read/write permissions are role-scoped through AgentCapabilityResolver.",
                            &["review_required", "review_reason", "agent_runs"],
                        ),
                    ],
                ),
                operation_group(
                    "session",
                    "Coordinate parallel sessions, background turns, route commands, and bridge evidence.",
                    vec![
                        operation(
                            "dispatch_session",
                            "runtime.session_execution",
                            "Turn a pending mission command into a runtime turn dispatch request.",
                            "A background or parallel session should continue executing real work.",
                            "Session lease state must be active/background and command idempotency must hold.",
                            &["command_id", "turn_request", "session_command_summary"],
                        ),
                        operation(
                            "link_sessions",
                            "runtime.session_relation_graph",
                            "Create or use session relations/proxies for cross-session collaboration.",
                            "The answer needs to reference, review, or route work between sessions.",
                            "ConflictsWith/Blocks relations are routed through ConflictArbiter.",
                            &["relations", "proxies", "route_receipt"],
                        ),
                    ],
                ),
                operation_group(
                    "conflict_approval_steward",
                    "Handle conflicts, approvals, and long-running stewardship without silent drift.",
                    vec![
                        operation(
                            "request_arbiter",
                            "runtime.conflict_arbiter",
                            "Record and resolve a conflict from ExecutionGraph, session, tool, memory, approval, or agent evidence.",
                            "Evidence disagrees or a dependency/permission state blocks progress.",
                            "Severity determines continue, review, pause, or approval.",
                            &["conflict_id", "decision", "mission_evidence"],
                        ),
                        operation(
                            "ask_approval",
                            "runtime.approval",
                            "Escalate high-risk or externally visible actions before execution.",
                            "The next step is high risk, destructive, or requires human authority.",
                            "Approval policy decides pending, timeout, auto-continue, or block.",
                            &["approval_projection", "policy_gates"],
                        ),
                    ],
                ),
                operation_group(
                    "tool_strategy",
                    "Batch and schedule model-requested tools while keeping tools as hands and feet.",
                    vec![
                        operation(
                            "parallel_tool_batch",
                            "runtime.tool_scheduler",
                            "Batch independent read-only tools and serialize write/destructive tools.",
                            "Several evidence items can be gathered independently.",
                            "Tool safety classes and dependency edges determine the schedule.",
                            &["tool_dag", "schedule", "tool_evidence_refs"],
                        ),
                        operation(
                            "continue_single",
                            "runtime.conversation",
                            "Continue as one executor when collaboration overhead is not justified.",
                            "The task is simple, low risk, and evidence needs are small.",
                            "Runtime still records trace, evidence, and policy decisions.",
                            &["turn_summary", "runtime_trace"],
                        ),
                    ],
                ),
            ],
            action_contracts: vec![
                action_contract(
                    "use_team_template",
                    "request_team",
                    "Complex implementation, audit, research, debate, incident, or long-running work benefits from role split.",
                    &["intent", "template_hint optional", "reason optional"],
                    &["team_projection", "agent_projection", "execution_graph"],
                ),
                action_contract(
                    "build_execution_graph",
                    "request_team",
                    "The model wants explicit dependencies, parallel lanes, review, and synthesis for a team.",
                    &["intent", "template_hint optional"],
                    &["execution_graph", "execution_graph_quality", "ready_node_ids"],
                ),
                action_contract(
                    "dispatch_session",
                    "request_session_link",
                    "A background or related session should run or receive work from the current mission.",
                    &["intent", "session_id or target_ref"],
                    &["session_commands", "relation_projection"],
                ),
                action_contract(
                    "request_arbiter",
                    "request_risk_gate",
                    "Evidence conflicts, graph blocks, or policy disagreement require explicit resolution.",
                    &["intent", "risk optional", "evidence_refs optional"],
                    &["conflict_projection", "approval_projection"],
                ),
                action_contract(
                    "parallel_tool_batch",
                    "request_parallel_tools",
                    "Independent read-only evidence can be gathered faster as a scheduler batch.",
                    &["intent", "capabilities optional"],
                    &["tool_dag", "schedule"],
                ),
                action_contract(
                    "ask_approval",
                    "request_risk_gate",
                    "The next action is high-risk, destructive, external, or requires human authority.",
                    &["intent", "risk", "reason optional"],
                    &["approval_projection", "policy_gates"],
                ),
                action_contract(
                    "continue_single",
                    "plan_only",
                    "The task is simple enough that team/session overhead would reduce efficiency.",
                    &["intent"],
                    &["execution_decision"],
                ),
            ],
        }
    }
}

impl RuntimeCapabilityManifest {
    #[must_use]
    pub fn current() -> Self {
        Self {
            name: "cowd-runtime-capabilities".to_string(),
            capabilities: vec![
                capability(
                    "batch_readonly_evidence",
                    "Batch read-only evidence gathering with ordered results.",
                    &[
                        "workspace_snapshot",
                        "read_many",
                        "grep_many",
                        "glob_many",
                        "tool_batch_readonly",
                    ],
                    &[
                        "Use for README, docs, source review, bug investigation, release checks, and any task that needs several independent read-only facts.",
                        "Prefer this over repeated read_file calls on the same file.",
                    ],
                ),
                capability(
                    "parallel_tool_scheduler",
                    "Runtime can schedule model-requested tool calls by safety class: parallel read-only, limited network/write, serial destructive.",
                    &["tool_batch_readonly", "read_many", "grep_many"],
                    &[
                        "Use by requesting multiple independent read-only tool calls in one turn.",
                        "Keep write/destructive calls separate and permission-aware.",
                    ],
                ),
                capability(
                    "evidence_planning",
                    "Runtime can recommend an evidence acquisition mode and a compact tool plan for the current task.",
                    &["runtime_capabilities", "ToolSearch"],
                    &[
                        "Use when unsure whether to full-read, batch-read, search, summarize, or delegate.",
                        "Use before deep source/document review to avoid slow range-by-range reading.",
                    ],
                ),
                capability(
                    "agent_collaboration",
                    "Runtime owns subagent, team, collaboration, mission/session, and verification affordances for complex tasks; these are orchestration affordances, not always direct provider tools.",
                    &["runtime_capabilities"],
                    &[
                        "Use when independent domains can be explored in parallel.",
                        "When no direct callable team tool is present, structure the plan so runtime/gateway orchestration or the user can attach the right collaborators.",
                    ],
                ),
                capability(
                    "runtime_orchestration",
                    "Model-visible runtime control-plane for plan_only, teams, subagents, verification, ReWOO, Tool DAG, deliberation, reflexion, risk gates, and session links.",
                    &["runtime_capabilities", "runtime_orchestrate"],
                    &[
                        "Use plan_only before complex implementation, architecture, or multi-agent work.",
                        "Use request_team/request_parallel_tools/request_rewoo_evidence/request_deliberation/request_reflexion_retry when the task clearly benefits from higher-order runtime execution.",
                    ],
                ),
                capability(
                    "tool_trace_and_evidence",
                    "Tool calls are recorded as summaries plus raw evidence references so the model can reason from evidence without flooding context.",
                    &["runtime_capabilities"],
                    &[
                        "Use evidence refs and summaries instead of repeatedly rereading large outputs.",
                        "When evidence is enough, answer with checked facts and remaining risks.",
                    ],
                ),
                capability(
                    "surface_stage_reply",
                    "External surfaces need visible staged progress; long-running turns should produce an evidence-backed partial answer instead of silence.",
                    &["runtime_capabilities"],
                    &[
                        "Use for Feishu, WebUI, TUI, or any surface where user-visible latency matters.",
                        "If time or evidence is insufficient, report checked evidence, current judgment, and next steps.",
                    ],
                ),
                capability(
                    "context_memory_governance",
                    "Runtime reconciles recalled memory and active knowledge against the current user turn before injecting them into context.",
                    &["runtime_capabilities"],
                    &[
                        "Use when old user preferences, knowledge rules, or memory recalls appear to conflict with the current explicit instruction.",
                        "Treat the current user turn as the deciding instruction for this turn; suppressed memories remain stored but should not steer this answer.",
                    ],
                ),
            ],
        }
    }
}

#[must_use]
pub fn runtime_capability_primer() -> String {
    runtime_capability_primer_with_overlay(&ActiveEvolutionCapabilityOverlay::default())
}

#[must_use]
pub fn runtime_capability_primer_with_overlay(
    active_evolution: &ActiveEvolutionCapabilityOverlay,
) -> String {
    let manifest = RuntimeCapabilityManifest::current();
    let catalog = RuntimeCapabilityCatalog::current();
    let mut lines = vec![
        "# Runtime capability awareness".to_string(),
        "You run inside Cowd AI Harness. You are not limited to naive one-tool-at-a-time ReAct."
            .to_string(),
        "Actively choose the most efficient available runtime capability for the task.".to_string(),
        String::new(),
        "Core capabilities:".to_string(),
    ];

    for capability in &manifest.capabilities {
        lines.push(format!("- `{}`: {}", capability.id, capability.summary));
    }
    if active_evolution.active_count > 0 {
        lines.push(String::new());
        lines.push("Active self-evolution overlays:".to_string());
        for summary in active_evolution.active_summaries(5) {
            lines.push(format!("- {summary}"));
        }
    }

    lines.extend([
        String::new(),
        "Runtime action contract:".to_string(),
        "When higher-order execution is useful, inspect options through read-only `runtime_capabilities`; use `runtime_orchestrate` only for stateful runtime orchestration that may create teams, session commands, approvals, or evidence records.".to_string(),
    ]);
    for action in &catalog.action_contracts {
        lines.push(format!(
            "- runtime_action=`{}` -> runtime_orchestrate action=`{}`: {}",
            action.runtime_action, action.tool_action, action.when_to_use
        ));
    }

    lines.extend([
        String::new(),
        "Operational guidance:".to_string(),
        "- For README/docs/code review, prefer `workspace_snapshot`, `git diff` evidence, `read_many`, `grep_many`, or `tool_batch_readonly` before repeated `read_file`.".to_string(),
        "- For independent read-only facts, request them together so the runtime can batch or parallelize them.".to_string(),
        "- Distinguish model-callable tools from runtime-owned affordances: subagent/team/mission collaboration may be orchestrated by runtime even when it is not exposed as a direct tool.".to_string(),
        "- For complex architecture or validation work, shape the task so runtime-owned collaboration can be used when independent evidence domains exist.".to_string(),
        "- If a path becomes slow or repetitive, switch strategy: batch evidence, narrow scope, delegate, or give an evidence-backed staged answer.".to_string(),
        "- Slow model output is acceptable when it keeps producing useful evidence; treat no-progress idle, repeated low-novelty tool paths, and missing synthesis as the signals to re-plan.".to_string(),
        "- For long or expensive work, produce an early staged answer with checked facts, then continue with batched evidence and runtime orchestration instead of serial probing.".to_string(),
        "- Prefer full-file or batched reads when the context window can hold the evidence; avoid artificial tiny range reads unless the target is genuinely large.".to_string(),
        "- Current user instructions override conflicting recalled memory or knowledge rules for this turn; if a recalled preference conflicts with the explicit current request, suppress that memory and follow the current request.".to_string(),
        "- Use `runtime_capabilities` when you need a compact, read-only recommendation for available runtime affordances.".to_string(),
        "- Use `runtime_capabilities` with detail=`execution_patterns`, `team_templates`, `agent_catalog`, `orchestration_options`, or `budget_controls` when deciding how to solve complex work.".to_string(),
        "- Use `runtime_orchestrate` only when a runtime state change is intended; it is not a read-only query.".to_string(),
        format!("- {}", runtime_orchestration_action_guidance()),
    ]);

    lines.join("\n")
}

#[must_use]
pub fn runtime_capabilities_response(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
) -> Value {
    runtime_capabilities_response_with_detail(intent, surface, profile, None)
}

#[must_use]
pub fn runtime_capabilities_response_with_detail(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
    detail: Option<&str>,
) -> Value {
    runtime_capabilities_response_with_detail_and_overlay(
        intent,
        surface,
        profile,
        detail,
        &ActiveEvolutionCapabilityOverlay::default(),
    )
}

#[must_use]
pub fn runtime_capabilities_response_with_detail_and_overlay(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
    detail: Option<&str>,
    active_evolution: &ActiveEvolutionCapabilityOverlay,
) -> Value {
    runtime_capabilities_response_with_leased_decision(
        intent,
        surface,
        profile,
        detail,
        active_evolution,
        None,
    )
}

#[must_use]
pub fn runtime_capabilities_response_with_leased_decision(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
    detail: Option<&str>,
    active_evolution: &ActiveEvolutionCapabilityOverlay,
    leased_decision: Option<&RuntimeExecutionDecision>,
) -> Value {
    let available_tools = runtime_model_callable_tools()
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    runtime_capabilities_response_with_leased_decision_and_tools(
        intent,
        surface,
        profile,
        detail,
        active_evolution,
        leased_decision,
        &available_tools,
    )
}

#[must_use]
pub fn runtime_capabilities_response_with_leased_decision_and_tools(
    intent: &str,
    surface: Option<&str>,
    profile: Option<&str>,
    detail: Option<&str>,
    active_evolution: &ActiveEvolutionCapabilityOverlay,
    leased_decision: Option<&RuntimeExecutionDecision>,
    available_tool_names: &[String],
) -> Value {
    let mut manifest = RuntimeCapabilityManifest::current();
    let catalog = RuntimeCapabilityCatalog::current();
    let context_profile = profile.and_then(parse_context_profile);
    let execution_decision = leased_decision
        .cloned()
        .unwrap_or_else(|| build_runtime_execution_decision(intent, context_profile));
    let evidence_plan: EvidencePlan = crate::evidence_planner::plan_evidence_with_understanding(
        intent,
        &execution_decision.strategy.understanding,
    );
    let action_selection =
        action_selection_report_for_decision(&execution_decision, context_profile);
    let rewoo_plan = rewoo_plan_for_intent_with_evidence_plan(intent, evidence_plan.clone());
    let tool_dag = tool_dag_from_rewoo(&rewoo_plan);
    let detail_value = detail.unwrap_or("summary");
    let backend_capabilities =
        backend_capabilities(detail_value, &execution_decision, &action_selection);
    let runtime_orchestrate_enabled = available_tool_names
        .iter()
        .any(|name| name == "runtime_orchestrate");
    let runtime_orchestrate_available =
        execution_decision.executable && runtime_orchestrate_enabled;
    let model_callable_tools = runtime_model_callable_tools()
        .into_iter()
        .filter(|name| {
            available_tool_names
                .iter()
                .any(|available| available == name)
        })
        .collect::<Vec<_>>();
    for capability in &mut manifest.capabilities {
        capability.recommended_tools.retain(|tool| {
            available_tool_names
                .iter()
                .any(|available| available == tool)
        });
    }
    let mut orchestration_blocked_reasons = execution_decision.blocked_reasons.clone();
    if !runtime_orchestrate_enabled {
        orchestration_blocked_reasons.push("runtime_orchestrate_not_enabled".to_string());
    }
    let action_plane = runtime_action_plane(
        &execution_decision.recommended_actions,
        runtime_orchestrate_available,
        &orchestration_blocked_reasons,
    );
    json!({
        "type": "runtime_capabilities",
        "manifest": manifest,
        "runtime_capability_catalog": catalog,
        "intent": intent,
        "surface": surface,
        "profile": profile,
        "detail": detail_value,
        "active_evolution_capabilities": active_evolution,
        "available_tool_names": available_tool_names,
        "evidence_plan": evidence_plan,
        "execution_decision": execution_decision,
        "action_selection": action_selection,
        "backend_capabilities": backend_capabilities,
        "runtime_orchestrate": {
            "available": runtime_orchestrate_available,
            "blocked_reasons": orchestration_blocked_reasons,
            "required_permission": "workspace-write",
            "side_effects": ["mission_evidence", "team_runtime", "session_command", "approval_orchestration"],
            "actions": runtime_orchestration_actions(),
            "runtime_action_contract": RuntimeCapabilityCatalog::current().action_contracts,
            "recommendation": execution_decision.recommended_actions,
            "action_selection": action_selection
        },
        "action_plane": action_plane,
        "runtime_action_contract": RuntimeCapabilityCatalog::current().action_contracts,
        "model_router": runtime_model_router_capability(),
        "budget_controls": runtime_budget_controls(profile),
        "strategy": {
            "prefer_batch_readonly": true,
            "prefer_full_or_batch_read_for_small_docs": true,
            "model_callable_tools": model_callable_tools,
            "runtime_owned_affordances": ["execution_patterns", "rewoo_evidence", "tool_dag", "subagent", "team", "mission", "session", "verification", "deliberation", "reflexion"],
            "runtime_orchestrate_is_stateful": true,
            "use_or_request_subagents_for_independent_domains": true,
            "avoid_repeated_overlapping_reads": true,
            "current_turn_overrides_conflicting_memory": true,
            "fallback_when_stalled": "switch execution pattern by querying runtime_capabilities first, then use runtime_orchestrate(request_reflexion_retry/request_parallel_tools) only when stateful runtime orchestration is intended, or answer with checked evidence, current judgment, remaining risks, and next best step"
        },
        "advanced_execution": {
            "rewoo_candidate": rewoo_plan,
            "tool_dag_candidate": tool_dag,
        }
    })
}

fn runtime_action_plane<T: Serialize>(
    recommended_actions: &[T],
    runtime_orchestrate_available: bool,
    blocked_reasons: &[String],
) -> Value {
    json!({
        "recommended_next_tool": if runtime_orchestrate_available {
            "runtime_capabilities_or_runtime_orchestrate"
        } else {
            "runtime_capabilities"
        },
        "can_execute_now": runtime_orchestrate_available,
        "blocked_reasons": blocked_reasons,
        "session_id_bound": "gateway_api_sessions_auto_bind_session_id",
        "permissions": {
            "runtime_capabilities": "read-only",
            "runtime_orchestrate": "workspace-write"
        },
        "required_args": {
            "runtime_capabilities": ["intent"],
            "runtime_orchestrate": ["intent", "action"],
        },
        "expected_events": [
            "RuntimeRun",
            "RunModelTelemetry",
            "ToolStart",
            "ToolComplete",
            "TurnComplete"
        ],
        "recipes": if runtime_orchestrate_available { json!([
            {
                "name": "plan_then_execute_team",
                "when": "complex work has independent domains, reviewers, or parallel evidence needs",
                "steps": [
                    {"tool": "runtime_capabilities", "args": {"detail": "team_templates"}},
                    {"tool": "runtime_orchestrate", "args": {"action": "request_team"}, "session_id": "auto_bound_by_gateway"}
                ]
            },
            {
                "name": "parallel_readonly_evidence",
                "when": "several read-only facts can be gathered independently",
                "steps": [
                    {"tool": "runtime_orchestrate", "args": {"action": "request_parallel_tools"}},
                    {"tool": "tool_batch_readonly", "args": "execute returned independent reads when available"}
                ]
            },
            {
                "name": "rewoo_evidence_plan",
                "when": "the task needs explicit evidence variables before synthesis",
                "steps": [
                    {"tool": "runtime_orchestrate", "args": {"action": "request_rewoo_evidence"}}
                ]
            },
            {
                "name": "reflexion_on_stall",
                "when": "tool path repeats, evidence novelty drops, or answer quality is blocked",
                "steps": [
                    {"tool": "runtime_orchestrate", "args": {"action": "request_reflexion_retry"}}
                ]
            }
        ]) } else { json!([]) },
        "recommended_actions": recommended_actions,
    })
}

fn runtime_model_callable_tools() -> Vec<&'static str> {
    vec![
        "workspace_snapshot",
        "read_many",
        "grep_many",
        "glob_many",
        "tool_batch_readonly",
        "runtime_capabilities",
        "runtime_orchestrate",
        "ToolSearch",
    ]
}

fn backend_capabilities(
    detail: &str,
    execution_decision: &RuntimeExecutionDecision,
    action_selection: &crate::execution_core::RuntimeActionSelectionReport,
) -> Value {
    let catalog = RuntimeCapabilityCatalog::current();
    let templates = catalog
        .templates
        .iter()
        .map(|template| {
            json!({
                "id": template.template_id,
                "protocol_id": template.protocol_id,
                "protocol_version": template.protocol_version,
                "availability": template.availability,
                "requires_review": template.requires_review,
                "best_for": template.best_for,
            })
        })
        .collect::<Vec<_>>();
    match detail {
        "execution_patterns" => execution_pattern_catalog_response(),
        "team_templates" => json!({ "collaboration_templates": templates }),
        "agent_catalog" => json!({
            "role_intents": ["planner", "researcher", "executor", "reviewer", "merger", "memory_curator", "human"],
            "execution_profiles": [
                {"role": "planner", "tool_mode": "read_only", "purpose": "plan and decompose"},
                {"role": "researcher", "tool_mode": "read_only", "purpose": "parallel evidence gathering"},
                {"role": "executor", "tool_mode": "write_workspace", "purpose": "apply bounded changes"},
                {"role": "reviewer", "tool_mode": "read_only", "purpose": "independent verification"}
            ]
        }),
        "orchestration_options" => json!({
            "decision": execution_decision,
            "action_selection": action_selection,
            "execution_patterns": execution_pattern_catalog_response(),
            "collaboration_templates": templates,
            "runtime_action_contract": catalog.action_contracts,
        }),
        "action_selection" => json!(action_selection),
        "capability_catalog" => json!(catalog),
        "runtime_action_contract" => json!({
            "contracts": catalog.action_contracts,
            "operation_groups": catalog.operation_groups,
        }),
        "model_router" => runtime_model_router_capability(),
        "budget_controls" => runtime_budget_controls(None),
        "policy_gates" => json!({
            "risk": ["risk_gate", "human_confirm"],
            "parallelism": "runtime validator caps max_parallel_agents",
            "writes": "write/destructive actions require permission and scheduler gating"
        }),
        _ => json!({
            "summary": "Use detail=execution_patterns/team_templates/agent_catalog/orchestration_options/model_router/budget_controls/policy_gates for concrete runtime affordances.",
            "execution_patterns": execution_pattern_catalog_response()["execution_patterns"],
            "collaboration_template_count": templates.len(),
        }),
    }
}

fn runtime_model_router_capability() -> Value {
    json!({
        "owner": "runtime.provider_usage",
        "registry": "ModelPerformanceRegistry",
        "decision": "ModelRouteDecision",
        "intents": ["quick", "standard", "deep", "recovery"],
        "signals": [
            "first_token_latency_ms",
            "wall_tokens_per_second",
            "active_tokens_per_second",
            "tokens_per_second",
            "usage_source",
            "quality_score",
            "failure_rate"
        ],
        "policies": {
            "quick": "favor high throughput and low first-token latency for simple or interactive turns",
            "standard": "balance speed, quality, and reliability for normal turns",
            "deep": "favor quality and reliability for architecture, refactor, audit, and complex synthesis",
            "recovery": "favor reliable models after stalled, failed, or repetitive execution"
        },
        "telemetry_source": "RunModelTelemetry",
        "fallback_behavior": "cold-start configured models remain routable even before telemetry samples exist"
    })
}

fn runtime_budget_controls(profile: Option<&str>) -> Value {
    json!({
        "profile": profile.unwrap_or("auto"),
        "turn": {
            "adaptive_wall_clock_seconds": {
                "direct_or_quick": 240,
                "standard": 480,
                "deep_or_yolo": 900
            },
            "stream_idle_seconds": {
                "direct_or_quick": 240,
                "standard": 360,
                "deep_or_yolo": 600
            },
            "max_iterations": {
                "direct_or_quick": 12,
                "standard": 32,
                "deep_or_yolo": 64
            },
            "partial_answer_preserved": true
        },
        "supervision": {
            "slow_but_productive_output_allowed": true,
            "low_novelty_tool_loop": "supervisor first asks for fallback synthesis; if ignored and tools continue, runtime stops the loop on the next iteration",
            "preferred_recovery": ["batch_evidence", "narrow_scope", "delegate_independent_domains", "staged_answer_with_remaining_risks"]
        },
        "tools": {
            "per_tool_timeout_registry": true,
            "parallel_readonly_scheduler": true,
            "serial_destructive_scheduler": true,
            "large_result_policy": "summaries plus raw evidence refs; avoid reinjecting huge raw payloads into prompt context"
        },
        "subagents": {
            "timeout_secs_supported": true,
            "max_turns_supported": true,
            "budget_tokens_supported": true,
            "default_max_turns": 10,
            "default_budget_tokens": 20000,
            "peer_visibility_supported": true
        },
        "context_memory": {
            "current_turn_overrides_conflicting_memory": true,
            "suppressed_memory_stays_stored": true,
            "suppression_scope": "current_turn_only",
            "known_conflict_classes": ["tool_orchestration_ban", "required_tool_orchestration_rule", "code_evidence_count_rule", "defer_work_rule"]
        }
    })
}

fn parse_context_profile(value: &str) -> Option<ContextProfile> {
    match value {
        "MainTurn" | "main_turn" | "default" => Some(ContextProfile::MainTurn),
        "SoloGoal" | "solo_goal" => Some(ContextProfile::SoloGoal),
        "YoloGoal" | "yolo_goal" => Some(ContextProfile::YoloGoal),
        "SubAgent" | "sub_agent" => Some(ContextProfile::SubAgent),
        "Collaboration" | "collaboration" => Some(ContextProfile::Collaboration),
        "Review" | "review" => Some(ContextProfile::Review),
        "Resume" | "resume" => Some(ContextProfile::Resume),
        "Cron" | "cron" => Some(ContextProfile::Cron),
        "SurfaceQuickReply" | "surface_quick_reply" => Some(ContextProfile::SurfaceQuickReply),
        "SurfaceTaskIntake" | "surface_task_intake" => Some(ContextProfile::SurfaceTaskIntake),
        "DeepInvestigation" | "deep_investigation" | "deep" => {
            Some(ContextProfile::DeepInvestigation)
        }
        _ => None,
    }
}

fn capability(
    id: &str,
    summary: &str,
    recommended_tools: &[&str],
    when_to_use: &[&str],
) -> RuntimeCapability {
    RuntimeCapability {
        id: id.to_string(),
        summary: summary.to_string(),
        recommended_tools: recommended_tools
            .iter()
            .map(|item| item.to_string())
            .collect(),
        when_to_use: when_to_use.iter().map(|item| item.to_string()).collect(),
    }
}

fn template_best_for(template_id: harness_contract::team::TeamTemplateId) -> Vec<String> {
    match template_id {
        harness_contract::team::TeamTemplateId::SingleExecutor => {
            vec!["direct bounded work".into()]
        }
        harness_contract::team::TeamTemplateId::ExecuteReview => {
            vec!["execution with independent review".into()]
        }
        harness_contract::team::TeamTemplateId::FanoutResearchSynthesis => {
            vec!["independent evidence gathering".into(), "synthesis".into()]
        }
        harness_contract::team::TeamTemplateId::DebateConsensus => {
            vec!["competing proposals".into()]
        }
        harness_contract::team::TeamTemplateId::ImplementationReviewFix => {
            vec!["implementation, review, and repair".into()]
        }
        harness_contract::team::TeamTemplateId::IncidentResponse => {
            vec!["incident triage and remediation".into()]
        }
        harness_contract::team::TeamTemplateId::LongRunningProject => {
            vec!["durable mission execution".into()]
        }
    }
}

fn operation_group(
    id: &str,
    summary: &str,
    operations: Vec<RuntimeOperation>,
) -> RuntimeOperationGroup {
    RuntimeOperationGroup {
        id: id.to_string(),
        summary: summary.to_string(),
        operations,
    }
}

fn operation(
    id: &str,
    owner: &str,
    summary: &str,
    model_intent: &str,
    validation_gate: &str,
    output_refs: &[&str],
) -> RuntimeOperation {
    RuntimeOperation {
        id: id.to_string(),
        owner: owner.to_string(),
        summary: summary.to_string(),
        model_intent: model_intent.to_string(),
        validation_gate: validation_gate.to_string(),
        output_refs: output_refs.iter().map(|item| (*item).to_string()).collect(),
    }
}

fn action_contract(
    runtime_action: &str,
    tool_action: &str,
    when_to_use: &str,
    required_intent_fields: &[&str],
    expected_projection: &[&str],
) -> RuntimeActionContract {
    RuntimeActionContract {
        runtime_action: runtime_action.to_string(),
        tool_action: tool_action.to_string(),
        when_to_use: when_to_use.to_string(),
        required_intent_fields: required_intent_fields
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
        validation: "Runtime validator applies strategy, risk, permission, session binding, and policy gates before changing state.".to_string(),
        expected_projection: expected_projection
            .iter()
            .map(|item| (*item).to_string())
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primer_exposes_batch_and_agent_capabilities() {
        let primer = runtime_capability_primer();

        assert!(primer.contains("read_many"));
        assert!(primer.contains("tool_batch_readonly"));
        assert!(primer.contains("subagent, team"));
        assert!(primer.contains("runtime-owned"));
        assert!(primer.contains("runtime_capabilities"));
        assert!(primer.contains("Runtime action contract"));
        assert!(primer.contains("use_team_template"));
        assert!(primer.contains("dispatch_session"));
        assert!(primer.contains("Slow model output is acceptable"));
        assert!(primer.contains("early staged answer"));
        assert!(primer.contains("batched reads"));
        assert!(primer.contains("Current user instructions override conflicting recalled memory"));
    }

    #[test]
    fn capabilities_response_contains_evidence_plan() {
        let response = runtime_capabilities_response(
            "检查 README 是否反映最新架构",
            Some("feishu"),
            Some("DeepInvestigation"),
        );

        assert_eq!(response["type"], "runtime_capabilities");
        assert_eq!(response["surface"], "feishu");
        assert!(response["evidence_plan"]["recommended_calls"].is_array());
        assert!(response["runtime_orchestrate"]["available"]
            .as_bool()
            .unwrap_or(false));
        assert!(response["runtime_capability_catalog"]["operation_groups"].is_array());
        assert!(response["runtime_action_contract"]
            .as_array()
            .is_some_and(|items| items
                .iter()
                .any(|item| item["runtime_action"] == "use_team_template")));
        assert_eq!(
            response["action_plane"]["recommended_next_tool"],
            "runtime_capabilities_or_runtime_orchestrate"
        );
        assert!(response["action_plane"]["recipes"].is_array());
        assert!(response["execution_decision"]["strategy"]["pattern"].is_string());
        assert!(response["execution_decision"]["lease"]["lease_id"].is_string());
        assert!(response["budget_controls"]["turn"]["max_iterations"].is_object());
        assert_eq!(
            response["model_router"]["registry"],
            "ModelPerformanceRegistry"
        );
        assert!(response["model_router"]["signals"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == "wall_tokens_per_second")));
        assert!(
            response["strategy"]["current_turn_overrides_conflicting_memory"]
                .as_bool()
                .unwrap_or(false)
        );
    }

    #[test]
    fn unavailable_runtime_is_not_projected_as_executable() {
        let decision = crate::StrategyDecisionEngine.decide_with_input(
            harness_contract::strategy::StrategyInput::from_prompt("解释这个名称"),
            None,
            crate::StrategyResourceHealth {
                provider_available: false,
                observed: true,
                ..crate::StrategyResourceHealth::default()
            },
        );
        let response = runtime_capabilities_response_with_leased_decision(
            "解释这个名称",
            None,
            None,
            None,
            &ActiveEvolutionCapabilityOverlay::default(),
            Some(&decision),
        );

        assert_eq!(response["runtime_orchestrate"]["available"], false);
        assert_eq!(response["action_plane"]["can_execute_now"], false);
        assert_eq!(
            response["action_plane"]["recommended_next_tool"],
            "runtime_capabilities"
        );
        assert!(response["runtime_orchestrate"]["blocked_reasons"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));
    }

    #[test]
    fn disabled_orchestration_tool_is_not_advertised_as_callable() {
        let response = runtime_capabilities_response_with_leased_decision_and_tools(
            "并行检查代码",
            None,
            None,
            None,
            &ActiveEvolutionCapabilityOverlay::default(),
            None,
            &["runtime_capabilities".to_string(), "read_many".to_string()],
        );

        assert_eq!(response["runtime_orchestrate"]["available"], false);
        assert_eq!(response["action_plane"]["can_execute_now"], false);
        assert!(response["action_plane"]["recipes"]
            .as_array()
            .is_some_and(Vec::is_empty));
        assert!(response["strategy"]["model_callable_tools"]
            .as_array()
            .is_some_and(|items| items.iter().all(|item| item != "runtime_orchestrate")));
        assert!(response["manifest"]["capabilities"]
            .as_array()
            .is_some_and(|capabilities| capabilities
                .iter()
                .all(|capability| capability["recommended_tools"]
                    .as_array()
                    .is_some_and(|tools| tools.iter().all(|tool| tool != "runtime_orchestrate")))));
    }

    #[test]
    fn capabilities_response_accepts_explicit_active_evolution_overlay() {
        let overlay = ActiveEvolutionCapabilityOverlay {
            active_count: 1,
            rolled_back_count: 0,
            capabilities: vec![ActiveEvolutionCapabilitySummary {
                version_id: "version-runtime-policy".to_string(),
                candidate_id: "candidate-runtime-policy".to_string(),
                kind: "runtime_policy".to_string(),
                adapter: "runtime_policy_overlay".to_string(),
                owner: "runtime".to_string(),
                status: "active".to_string(),
                goal_ids: vec!["runtime_efficiency".to_string()],
                policy_effects: vec!["prefer_parallel_tool_batch".to_string()],
                evidence_refs: vec!["artifact-policy".to_string()],
            }],
        };
        let response = runtime_capabilities_response_with_detail_and_overlay(
            "需要自进化策略影响后续运行",
            Some("webui"),
            Some("DeepInvestigation"),
            Some("summary"),
            &overlay,
        );

        assert_eq!(response["active_evolution_capabilities"]["active_count"], 1);
        assert_eq!(
            response["active_evolution_capabilities"]["capabilities"][0]["policy_effects"][0],
            "prefer_parallel_tool_batch"
        );
        let primer = runtime_capability_primer_with_overlay(&overlay);
        assert!(primer.contains("Active self-evolution overlays"));
        assert!(primer.contains("prefer_parallel_tool_batch"));
    }

    #[test]
    fn capabilities_response_exposes_execution_patterns_and_templates() {
        let modes = runtime_capabilities_response_with_detail(
            "全盘架构分析",
            None,
            None,
            Some("execution_patterns"),
        );
        assert!(modes["backend_capabilities"]["execution_patterns"].is_array());

        let templates = runtime_capabilities_response_with_detail(
            "需要多 Agent 协同实现并审查",
            None,
            None,
            Some("team_templates"),
        );
        assert!(templates["backend_capabilities"]["collaboration_templates"]
            .as_array()
            .is_some_and(|items| items.len() >= 7));

        let budget = runtime_capabilities_response_with_detail(
            "慢模型复杂分析",
            Some("feishu"),
            Some("DeepInvestigation"),
            Some("budget_controls"),
        );
        assert!(budget["backend_capabilities"]["turn"]["adaptive_wall_clock_seconds"].is_object());
        assert!(
            budget["backend_capabilities"]["subagents"]["timeout_secs_supported"]
                .as_bool()
                .unwrap_or(false)
        );

        let router = runtime_capabilities_response_with_detail(
            "复杂任务需要选择合适模型",
            None,
            None,
            Some("model_router"),
        );
        assert_eq!(
            router["backend_capabilities"]["owner"],
            "runtime.provider_usage"
        );
        assert!(router["backend_capabilities"]["intents"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == "deep")));

        let contract = runtime_capabilities_response_with_detail(
            "需要模型主动选择团队和并行工具",
            None,
            None,
            Some("runtime_action_contract"),
        );
        assert!(contract["backend_capabilities"]["contracts"]
            .as_array()
            .is_some_and(|items| {
                items
                    .iter()
                    .any(|item| item["runtime_action"] == "parallel_tool_batch")
            }));
    }

    #[test]
    fn runtime_capability_catalog_exposes_model_visible_actions() {
        let catalog = RuntimeCapabilityCatalog::current();
        assert!(catalog.templates.len() >= 7);
        assert!(catalog
            .operation_groups
            .iter()
            .any(|group| group.id == "execution_graph"));
        assert!(catalog
            .operation_groups
            .iter()
            .any(|group| group.operations.iter().any(|op| op.id == "request_arbiter")));
        assert!(catalog.action_contracts.iter().any(|contract| {
            contract.runtime_action == "use_team_template" && contract.tool_action == "request_team"
        }));
        assert!(catalog.action_contracts.iter().any(|contract| {
            contract.runtime_action == "dispatch_session"
                && contract
                    .expected_projection
                    .iter()
                    .any(|item| item == "session_commands")
        }));
    }
}
