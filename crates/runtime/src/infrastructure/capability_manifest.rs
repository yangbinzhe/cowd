//! Runtime capability manifest and prompt primer.
//!
//! The manifest makes Cowd's higher-level harness affordances visible to the
//! model without coupling tool implementations back into runtime.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::collaboration_template::CollaborationTemplateCatalog;
use crate::context_runtime::ContextProfile;
use crate::evidence_planner::{plan_evidence, EvidencePlan};
use crate::execution_core::{
    build_runtime_execution_decision, execution_mode_catalog_response, rewoo_plan_for_intent,
    runtime_orchestration_action_guidance, runtime_orchestration_actions, tool_dag_from_rewoo,
};

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
    let manifest = RuntimeCapabilityManifest::current();
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
        "- Use `runtime_capabilities` when you need a compact recommendation for available runtime affordances.".to_string(),
        "- Use `runtime_capabilities` with detail=`execution_modes`, `team_templates`, `agent_catalog`, `orchestration_options`, or `budget_controls` when deciding how to solve complex work.".to_string(),
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
    let manifest = RuntimeCapabilityManifest::current();
    let evidence_plan: EvidencePlan = plan_evidence(intent);
    let execution_decision =
        build_runtime_execution_decision(intent, profile.and_then(parse_context_profile));
    let rewoo_plan = rewoo_plan_for_intent(intent);
    let tool_dag = tool_dag_from_rewoo(&rewoo_plan);
    let detail_value = detail.unwrap_or("summary");
    let backend_capabilities = backend_capabilities(detail_value, intent);
    let action_plane = runtime_action_plane(&execution_decision.recommended_actions);
    json!({
        "type": "runtime_capabilities",
        "manifest": manifest,
        "intent": intent,
        "surface": surface,
        "profile": profile,
        "detail": detail_value,
        "evidence_plan": evidence_plan,
        "execution_decision": execution_decision,
        "backend_capabilities": backend_capabilities,
        "runtime_orchestrate": {
            "available": true,
            "actions": runtime_orchestration_actions(),
            "recommendation": execution_decision.recommended_actions
        },
        "action_plane": action_plane,
        "model_router": runtime_model_router_capability(),
        "budget_controls": runtime_budget_controls(profile),
        "strategy": {
            "prefer_batch_readonly": true,
            "prefer_full_or_batch_read_for_small_docs": true,
            "model_callable_tools": ["workspace_snapshot", "read_many", "grep_many", "glob_many", "tool_batch_readonly", "runtime_capabilities", "runtime_orchestrate", "ToolSearch"],
            "runtime_owned_affordances": ["execution_modes", "rewoo_evidence", "tool_dag", "subagent", "team", "mission", "session", "verification", "deliberation", "reflexion"],
            "use_or_request_subagents_for_independent_domains": true,
            "avoid_repeated_overlapping_reads": true,
            "current_turn_overrides_conflicting_memory": true,
            "fallback_when_stalled": "switch execution mode through runtime_orchestrate plan_only/request_reflexion_retry or answer with checked evidence, current judgment, remaining risks, and next best step"
        },
        "advanced_execution": {
            "rewoo_candidate": rewoo_plan,
            "tool_dag_candidate": tool_dag,
        }
    })
}

fn runtime_action_plane<T: Serialize>(recommended_actions: &[T]) -> Value {
    json!({
        "recommended_next_tool": "runtime_orchestrate",
        "can_execute_now": true,
        "session_id_bound": "gateway_api_sessions_auto_bind_session_id",
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
        "recipes": [
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
        ],
        "recommended_actions": recommended_actions,
    })
}

fn backend_capabilities(detail: &str, intent: &str) -> Value {
    let templates = CollaborationTemplateCatalog::built_in()
        .templates()
        .iter()
        .map(|template| {
            json!({
                "id": template.template_id.as_str(),
                "name": template.label,
                "agent_roles": template.agent_roles.iter().map(|role| json!({
                    "role_id": role.role_id,
                    "responsibility": role.responsibility,
                    "allowed_tools": role.allowed_tools,
                    "evidence_duties": role.evidence_duties,
                })).collect::<Vec<_>>(),
                "max_parallelism": template.max_parallelism,
                "review_contract": template.review_contract,
                "merge_contract": template.merge_contract,
                "human_approval_points": template.human_approval_points,
            })
        })
        .collect::<Vec<_>>();
    match detail {
        "execution_modes" => execution_mode_catalog_response(),
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
            "decision": build_runtime_execution_decision(intent, None),
            "execution_modes": execution_mode_catalog_response(),
            "collaboration_templates": templates,
        }),
        "model_router" => runtime_model_router_capability(),
        "budget_controls" => runtime_budget_controls(None),
        "policy_gates" => json!({
            "risk": ["risk_gate", "human_confirm"],
            "parallelism": "runtime validator caps max_parallel_agents",
            "writes": "write/destructive actions require permission and scheduler gating"
        }),
        _ => json!({
            "summary": "Use detail=execution_modes/team_templates/agent_catalog/orchestration_options/model_router/budget_controls/policy_gates for concrete runtime affordances.",
            "execution_modes": execution_mode_catalog_response()["execution_modes"],
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
        assert_eq!(
            response["action_plane"]["recommended_next_tool"],
            "runtime_orchestrate"
        );
        assert!(response["action_plane"]["recipes"].is_array());
        assert!(response["execution_decision"]["recommended_mode"].is_string());
        assert!(response["budget_controls"]["turn"]["max_iterations"].is_object());
        assert_eq!(
            response["model_router"]["registry"],
            "ModelPerformanceRegistry"
        );
        assert!(response["model_router"]["signals"]
            .as_array()
            .is_some_and(|items| items.iter().any(|item| item == "tokens_per_second")));
        assert!(
            response["strategy"]["current_turn_overrides_conflicting_memory"]
                .as_bool()
                .unwrap_or(false)
        );
    }

    #[test]
    fn capabilities_response_exposes_execution_modes_and_templates() {
        let modes = runtime_capabilities_response_with_detail(
            "全盘架构分析",
            None,
            None,
            Some("execution_modes"),
        );
        assert!(modes["backend_capabilities"]["execution_modes"].is_array());

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
    }
}
