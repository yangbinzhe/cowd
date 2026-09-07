use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::{ComplexHarnessScenarioReport, E2eScenarioMatrixItem, StableAiHealthReport};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum HarnessEvalLevel {
    #[default]
    Quick,
    Full,
    Deep,
}

impl HarnessEvalLevel {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Quick => "quick",
            Self::Full => "full",
            Self::Deep => "deep",
        }
    }

    #[must_use]
    pub fn from_str(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "quick" | "smoke" | "lightweight" => Some(Self::Quick),
            "full" => Some(Self::Full),
            "deep" | "real" | "deep-real" | "deep_real" => Some(Self::Deep),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessEvalRunStatus {
    Queued,
    Running,
    CancelRequested,
    Completed,
    Gated,
    Cancelled,
    Failed,
}

impl HarnessEvalRunStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::CancelRequested => "cancel_requested",
            Self::Completed => "completed",
            Self::Gated => "gated",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEvalUsageSummary {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub cache_creation_input_tokens: u32,
    pub cache_read_input_tokens: u32,
    pub total_tokens: u32,
    pub usage_source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEvalReportSummary {
    pub id: String,
    pub level: String,
    pub status: String,
    pub provider: Option<String>,
    pub budget: Option<String>,
    pub report_path: String,
    pub markdown_path: Option<String>,
    pub result_package_dir: Option<String>,
    pub created_at_ms: u128,
    pub total_elapsed_ms: Option<u128>,
    pub provider_rounds: usize,
    pub runtime_actions: usize,
    pub tool_calls: usize,
    pub total_tokens: u32,
    pub scenario_count: usize,
    pub failed_capabilities: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HarnessEvalReportDetail {
    pub summary: HarnessEvalReportSummary,
    pub report: Value,
    pub artifacts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEvalRunRecord {
    pub run_id: String,
    pub level: String,
    pub status: String,
    pub requested_at_ms: u128,
    pub finished_at_ms: Option<u128>,
    pub authorized_real_model: bool,
    pub provider: Option<String>,
    pub budget: Option<String>,
    pub report_id: Option<String>,
    pub report_path: Option<String>,
    pub result_package_dir: Option<String>,
    pub total_elapsed_ms: Option<u128>,
    pub provider_rounds: usize,
    pub tool_calls: usize,
    pub total_tokens: u32,
    pub scenario_count: usize,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEvalRunRequest {
    #[serde(default)]
    pub level: HarnessEvalLevel,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub budget: Option<String>,
    #[serde(default)]
    pub allow_real_model: bool,
    #[serde(default)]
    pub actor: Option<String>,
    #[serde(default)]
    pub objective: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEvalReportGateItem {
    pub name: String,
    pub status: String,
    pub required: bool,
    pub evidence: String,
    pub repair_hint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEvalReportGate {
    pub kind: String,
    pub status: String,
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub items: Vec<HarnessEvalReportGateItem>,
}

impl HarnessEvalReportGateItem {
    fn new(
        name: impl Into<String>,
        passed: bool,
        required: bool,
        evidence: impl Into<String>,
        repair_hint: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            status: if passed { "passed" } else { "failed" }.to_string(),
            required,
            evidence: evidence.into(),
            repair_hint: repair_hint.into(),
        }
    }
}

impl HarnessEvalReportGate {
    fn from_items(items: Vec<HarnessEvalReportGateItem>) -> Self {
        let total = items.len();
        let passed = items.iter().filter(|item| item.status == "passed").count();
        let failed = items
            .iter()
            .filter(|item| item.required && item.status != "passed")
            .count();
        Self {
            kind: "harness_eval.report_gate".to_string(),
            status: if failed == 0 { "passed" } else { "failed" }.to_string(),
            total,
            passed,
            failed,
            items,
        }
    }
}

/// Evaluate the canonical Agent-first closure projection.  This deliberately
/// consumes only durable projection facts; a model's claimed status or a
/// summary counter is never sufficient to pass the gate.
#[must_use]
pub fn evaluate_agentic_program_closure(value: &Value) -> Value {
    let projection = value.get("projection").unwrap_or(value);
    let mut checks = Vec::new();
    let mut missing = Vec::new();
    let mut check = |name: &str, passed: bool, evidence: Value| {
        if !passed {
            missing.push(name.to_string());
        }
        checks.push(json!({"name": name, "passed": passed, "evidence": evidence}));
    };

    let nonempty = has_nonempty_string;
    let (program_identity, program_identity_evidence) = program_identity_check(projection);
    check(
        "program_identity_bound",
        program_identity,
        program_identity_evidence,
    );

    let teams = projection
        .get("teams")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let agents = projection
        .get("agents")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let tasks = projection
        .get("tasks")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let artifacts = projection
        .get("artifacts")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let topics = projection
        .get("topics")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let required_team_count = projection
        .get("required_team_count")
        .and_then(Value::as_u64)
        .unwrap_or_default() as usize;
    check(
        "teams_materialized",
        required_team_count > 0 && teams.len() >= required_team_count,
        json!({"required": required_team_count, "observed": teams.len()}),
    );

    let teams_well_formed = !teams.is_empty()
        && teams.iter().all(|(team_id, team)| {
            team.get("team_id").and_then(Value::as_str) == Some(team_id)
                && nonempty(team.get("name"))
                && nonempty(team.get("topic_ref"))
                && !team
                    .get("member_ids")
                    .and_then(Value::as_array)
                    .is_none_or(Vec::is_empty)
                && !team
                    .get("task_ids")
                    .and_then(Value::as_array)
                    .is_none_or(Vec::is_empty)
        });
    check(
        "team_projection_integrity",
        teams_well_formed,
        json!({"teams": teams.len()}),
    );

    let agents_well_formed = !agents.is_empty()
        && agents.iter().all(|(agent_id, agent)| {
            let team_id = agent.get("team_id").and_then(Value::as_str);
            agent.get("agent_id").and_then(Value::as_str) == Some(agent_id)
                && team_id.is_some_and(|team_id| teams.contains_key(team_id))
                && team_id.is_some_and(|team_id| {
                    teams
                        .get(team_id)
                        .and_then(|team| team.get("member_ids"))
                        .and_then(Value::as_array)
                        .is_some_and(|members| {
                            members
                                .iter()
                                .any(|member| member.as_str() == Some(agent_id))
                        })
                })
        });
    check(
        "agent_projection_integrity",
        agents_well_formed,
        json!({"agents": agents.len()}),
    );

    let active_tasks = tasks
        .iter()
        .filter(|(_, task)| task.get("status").and_then(Value::as_str) != Some("superseded"))
        .collect::<Vec<_>>();
    let tasks_well_formed = !active_tasks.is_empty()
        && active_tasks.iter().all(|(task_id, task)| {
            let team_id = task.get("team_id").and_then(Value::as_str);
            let claimant = task.get("claimant").and_then(Value::as_str);
            let reviewer = task.get("reviewed_by").and_then(Value::as_str);
            task.get("task_id").and_then(Value::as_str) == Some(task_id)
                && team_id.is_some_and(|team_id| teams.contains_key(team_id))
                && team_id.is_some_and(|team_id| {
                    teams
                        .get(team_id)
                        .and_then(|team| team.get("task_ids"))
                        .and_then(Value::as_array)
                        .is_some_and(|task_ids| {
                            task_ids.iter().any(|id| id.as_str() == Some(task_id))
                        })
                })
                && task.get("status").and_then(Value::as_str) == Some("accepted")
                && claimant.is_some_and(|agent_id| agents.contains_key(agent_id))
                && reviewer.is_some_and(|agent_id| agents.contains_key(agent_id))
                && claimant != reviewer
                && !task
                    .get("artifact_refs")
                    .and_then(Value::as_array)
                    .is_none_or(Vec::is_empty)
                && !task
                    .get("evidence_refs")
                    .and_then(Value::as_array)
                    .is_none_or(Vec::is_empty)
        });
    check(
        "tasks_accepted_and_independently_verified",
        tasks_well_formed,
        json!({"active_tasks": active_tasks.len()}),
    );

    let dependencies_verified = active_tasks.iter().all(|(_, task)| {
        task.get("depends_on")
            .and_then(Value::as_array)
            .is_none_or(|dependencies| {
                dependencies.iter().all(|dependency| {
                    dependency
                        .as_str()
                        .and_then(|id| tasks.get(id))
                        .is_some_and(|dependency| {
                            dependency.get("status").and_then(Value::as_str) == Some("accepted")
                        })
                })
            })
    });
    check(
        "task_dependencies_verified",
        dependencies_verified,
        json!({"active_tasks": active_tasks.len()}),
    );

    let artifacts_well_formed = !artifacts.is_empty()
        && artifacts.iter().all(|(artifact_ref, artifact)| {
            artifact.get("artifact_ref").and_then(Value::as_str) == Some(artifact_ref)
                && nonempty(artifact.get("content_ref"))
                && artifact
                    .get("committed_by")
                    .and_then(Value::as_str)
                    .is_some_and(|agent_id| agents.contains_key(agent_id))
        })
        && active_tasks.iter().all(|(_, task)| {
            task.get("artifact_refs")
                .and_then(Value::as_array)
                .is_some_and(|refs| {
                    refs.iter().all(|reference| {
                        reference
                            .as_str()
                            .is_some_and(|reference| artifacts.contains_key(reference))
                    })
                })
        });
    check(
        "artifacts_are_durable_and_bound",
        artifacts_well_formed,
        json!({"artifacts": artifacts.len()}),
    );

    let topics_well_formed = !topics.is_empty()
        && teams.iter().all(|(_, team)| {
            team.get("topic_ref")
                .and_then(Value::as_str)
                .is_some_and(|topic_ref| {
                    topics
                        .get(topic_ref)
                        .and_then(Value::as_array)
                        .is_some_and(|entries| {
                            !entries.is_empty()
                                && entries.iter().all(|entry| {
                                    nonempty(entry.get("entry_id"))
                                        && entry
                                            .get("actor_id")
                                            .and_then(Value::as_str)
                                            .is_some_and(|agent_id| agents.contains_key(agent_id))
                                        && !entry
                                            .get("refs")
                                            .and_then(Value::as_array)
                                            .is_none_or(Vec::is_empty)
                                })
                        })
                })
        });
    check(
        "topics_carry_referenced_coordination",
        topics_well_formed,
        json!({"topics": topics.len()}),
    );

    let final_artifact_ref = projection.get("final_artifact_ref").and_then(Value::as_str);
    let completion = projection.get("completion_request");
    let verdict = projection.get("objective_verdict");
    let terminal_verified = terminal_projection_verified(projection, &artifacts);
    check(
        "terminal_projection_verified",
        terminal_verified,
        json!({
            "status": projection.get("status"),
            "final_artifact_ref": final_artifact_ref,
            "completion_request": completion.is_some(),
            "objective_verdict": verdict.is_some()
        }),
    );

    agentic_closure_result(
        projection,
        checks,
        missing,
        [
            teams.len(),
            agents.len(),
            active_tasks.len(),
            artifacts.len(),
            topics.len(),
        ],
        terminal_verified,
    )
}

fn agentic_closure_result(
    projection: &Value,
    checks: Vec<Value>,
    missing: Vec<String>,
    counts: [usize; 5],
    terminal_verified: bool,
) -> Value {
    let [teams, agents, active_tasks, artifacts, topics] = counts;
    json!({
        "kind": "harness_eval.agentic_program_closure",
        "status": if missing.is_empty() { "passed" } else { "failed" },
        "checks": checks,
        "missing": missing,
        "facts": {
            "program_id": projection.get("program_id"),
            "teams": teams,
            "agents": agents,
            "active_tasks": active_tasks,
            "artifacts": artifacts,
            "topics": topics,
            "verified": terminal_verified
        }
    })
}

fn has_nonempty_string(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
}

fn program_identity_check(projection: &Value) -> (bool, Value) {
    let passed = [
        "program_id",
        "objective_id",
        "session_id",
        "turn_id",
        "root_execution_id",
    ]
    .into_iter()
    .all(|field| has_nonempty_string(projection.get(field)));
    (
        passed,
        json!({
            "program_id": projection.get("program_id"),
            "objective_id": projection.get("objective_id"),
            "session_id": projection.get("session_id"),
            "turn_id": projection.get("turn_id"),
            "root_execution_id": projection.get("root_execution_id")
        }),
    )
}

fn terminal_projection_verified(
    projection: &Value,
    artifacts: &serde_json::Map<String, Value>,
) -> bool {
    let final_artifact_ref = projection.get("final_artifact_ref").and_then(Value::as_str);
    let completion = projection.get("completion_request");
    let verdict = projection.get("objective_verdict");
    projection.get("status").and_then(Value::as_str) == Some("verified")
        && has_nonempty_string(projection.get("final_artifact_ref"))
        && final_artifact_ref.is_some_and(|reference| artifacts.contains_key(reference))
        && completion.is_some_and(|completion| {
            has_nonempty_string(completion.get("action_id"))
                && has_nonempty_string(completion.get("requested_by"))
                && completion.get("final_artifact_ref").and_then(Value::as_str)
                    == final_artifact_ref
                && !completion
                    .get("evidence_refs")
                    .and_then(Value::as_array)
                    .is_none_or(Vec::is_empty)
        })
        && verdict.is_some_and(|verdict| {
            verdict.get("kind").and_then(Value::as_str) == Some("satisfied")
                && has_nonempty_string(verdict.get("terminal_fence"))
                && verdict
                    .get("authority_revision")
                    .and_then(Value::as_u64)
                    .is_some_and(|revision| revision > 0)
        })
        && projection
            .get("unresolved")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
}

fn agentic_program_closure_claimed(report: &Value, scenarios: &[Value]) -> bool {
    report
        .get("agentic_program_closure")
        .is_some_and(|closure| !closure.is_null())
        || scenarios.iter().any(|scenario| {
            scenario.get("capability").and_then(Value::as_str) == Some("agentic_program_closure")
        })
        || report
            .pointer("/live_gateway_scenarios/scenarios")
            .and_then(Value::as_array)
            .is_some_and(|scenarios| {
                scenarios.iter().any(|scenario| {
                    scenario
                        .pointer("/production_trace/agentic_program")
                        .is_some_and(|projection| !projection.is_null())
                })
            })
}

fn agentic_closure_gate_evidence(closure: &Value) -> String {
    let facts = closure.get("facts").unwrap_or(&Value::Null);
    format!(
        "status={}, program={}, teams={}, agents={}, tasks={}, artifacts={}, topics={}, verified={}",
        closure.get("status").and_then(Value::as_str).unwrap_or("missing"),
        facts.get("program_id").and_then(Value::as_str).unwrap_or("missing"),
        facts.get("teams").and_then(Value::as_u64).unwrap_or_default(),
        facts.get("agents").and_then(Value::as_u64).unwrap_or_default(),
        facts.get("active_tasks").and_then(Value::as_u64).unwrap_or_default(),
        facts.get("artifacts").and_then(Value::as_u64).unwrap_or_default(),
        facts.get("topics").and_then(Value::as_u64).unwrap_or_default(),
        facts.get("verified").and_then(Value::as_bool).unwrap_or(false),
    )
}

#[must_use]
pub fn evaluate_report_gate(report: &Value) -> HarnessEvalReportGate {
    let level = report
        .get("level")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let trace = report.get("execution_trace").unwrap_or(&Value::Null);
    let total_tokens = trace
        .get("total_usage")
        .and_then(|value| value.get("total_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let usage_source = trace
        .get("total_usage")
        .and_then(|value| value.get("usage_source"))
        .and_then(Value::as_str)
        .unwrap_or("unavailable");
    let tool_calls = trace
        .get("tool_calls")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let tool_log_count = trace
        .get("tool_call_log")
        .and_then(Value::as_array)
        .map_or(0, Vec::len) as u64;
    let runtime_actions = trace
        .get("runtime_actions")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let scenarios = report
        .get("scenarios")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let complex = report.get("complex_scenarios").unwrap_or(&Value::Null);
    let real_tools = report.get("real_tool_scenarios").unwrap_or(&Value::Null);
    let parity = report
        .get("event_observation_parity")
        .unwrap_or(&Value::Null);
    let reality_context = report.get("reality_context_eval").unwrap_or(&Value::Null);
    let next_gen = report
        .get("next_gen_harness_closure")
        .unwrap_or(&Value::Null);
    let next_gen_scenarios = next_gen
        .get("scenarios")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let evidence_manifest = report.get("evidence_manifest").unwrap_or(&Value::Null);
    let package = report.get("report_package").unwrap_or(&Value::Null);
    let is_quick = level == "quick";
    let claims_tool_validation = next_gen_claims(&next_gen_scenarios, "claims_tool_validation");
    let claims_orchestration = next_gen_claims(&next_gen_scenarios, "claims_orchestration");
    let claims_memory_context = next_gen_claims(&next_gen_scenarios, "claims_memory_context");
    let claims_replay = next_gen_claims(&next_gen_scenarios, "claims_replay");
    let claims_external_access = next_gen_claims(&next_gen_scenarios, "claims_external_access");
    let real_model_claimed = report
        .get("authorized_real_model")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || report
            .get("stable_ai")
            .and_then(|value| value.get("real_provider_enabled"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
    let live = report.get("live_gateway_scenarios").unwrap_or(&Value::Null);
    let focused_live_claim =
        level == "deep" && live.get("claim_scope").and_then(Value::as_str) == Some("focused");
    let live_provider_rounds = live_gateway_provider_rounds(live);
    let live_total_tokens = live_gateway_total_tokens(live);
    let cache = trace.get("provider_cache").unwrap_or(&Value::Null);
    let fallback_miss = trace
        .pointer("/total_usage/input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let fallback_creation = trace
        .pointer("/total_usage/cache_creation_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let fallback_read = trace
        .pointer("/total_usage/cache_read_input_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let fallback_prompt = fallback_miss
        .saturating_add(fallback_creation)
        .saturating_add(fallback_read);
    let cache_prompt_tokens = cache
        .get("provider_prompt_tokens")
        .and_then(Value::as_u64)
        .unwrap_or(fallback_prompt);
    let cache_hit_ratio_bp = cache
        .get("cache_hit_ratio_bp")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| {
            (cache_prompt_tokens > 0)
                .then(|| fallback_read.saturating_mul(10_000) / cache_prompt_tokens)
                .unwrap_or(0)
        });
    let cache_dimensions_declared_known = cache
        .get("cache_dimensions_known")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let cache_physical_attempts = cache
        .get("physical_provider_attempts")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let cache_dimensions_unknown_attempts = cache
        .get("cache_dimensions_unknown_attempts")
        .and_then(Value::as_u64)
        .unwrap_or(cache_physical_attempts);
    let cache_provider_rounds = cache
        .get("provider_rounds")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| runtime_provider_rounds(trace));
    let cache_usage_known = cache_dimensions_declared_known
        && cache_dimensions_unknown_attempts == 0
        && cache_physical_attempts >= cache_provider_rounds;
    let structural_prompt_bytes = cache
        .get("model_visible_prompt_bytes")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let structural_reuse_ratio_bp = cache
        .get("structural_reuse_ratio_bp")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let warm_provider_prompt_tokens = cache
        .get("warm_provider_prompt_tokens")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let warm_cache_hit_ratio_bp = cache
        .get("warm_cache_hit_ratio_bp")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let warm_structural_reuse_ratio_bp = cache
        .get("warm_structural_reuse_ratio_bp")
        .and_then(Value::as_u64)
        .unwrap_or_default();

    let mut items = Vec::new();
    items.push(HarnessEvalReportGateItem::new(
        "scenario_capability_status",
        !scenarios.is_empty()
            && scenarios.iter().all(|item| {
                item.get("status").and_then(Value::as_str) == Some("passed")
                    || ((level != "deep" || focused_live_claim)
                        && item.get("capability").and_then(Value::as_str)
                            == Some("next_gen_harness_closure")
                        && item.get("status").and_then(Value::as_str) == Some("not_observed"))
            }),
        true,
        format!("scenario_capabilities={}", scenarios.len()),
        "fix failed capability rows before accepting the report",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "runtime_actions_recorded",
        runtime_actions >= 3,
        true,
        format!("runtime_actions={runtime_actions}"),
        "record capability, coverage, knowledge, and complex action traces",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "report_package_layout",
        package
            .get("required_dirs")
            .and_then(Value::as_array)
            .is_some_and(|dirs| dirs.len() >= 6),
        true,
        format!("package={}", package.get("status").and_then(Value::as_str).unwrap_or("missing")),
        "write summary/full report and requests/responses/events/run-evidence/model-speed artifacts",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "knowledge_memory_governance",
        scenario_status(&scenarios, "knowledge_fabric_context_governance") == Some("passed"),
        true,
        "knowledge fabric scenario must pass",
        "repair memory namespace, conflict, and activation evaluation",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "reality_context_eval_complete",
        scenario_status(&scenarios, "reality_context_eval") == Some("passed")
            && reality_context
                .get("failed")
                .and_then(Value::as_u64)
                .is_some_and(|failed| failed == 0)
            && reality_context
                .get("evidence_ref_total")
                .and_then(Value::as_u64)
                .is_some_and(|count| count > 0),
        true,
        format!(
            "failed={}, evidence_refs={}",
            reality_context
                .get("failed")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            reality_context
                .get("evidence_ref_total")
                .and_then(Value::as_u64)
                .unwrap_or_default()
        ),
        "repair RecallReport/ContextEnvelope scenario evidence before accepting the report",
    ));
    let agentic_closure_required = agentic_program_closure_claimed(report, &scenarios);
    let agentic_closure = report
        .get("agentic_program_closure")
        .map(evaluate_agentic_program_closure)
        .unwrap_or_else(
            || json!({"status": "not_observed", "checks": [], "missing": ["projection"]}),
        );
    items.push(HarnessEvalReportGateItem::new(
        "agentic_program_closure_verified",
        !agentic_closure_required
            || agentic_closure.get("status").and_then(Value::as_str) == Some("passed"),
        agentic_closure_required,
        agentic_closure_gate_evidence(&agentic_closure),
        "provide the durable AgenticProgram projection and satisfy Program/Team/Agent/Task/Artifact/Topic, independent verification, and terminal-fence checks",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "next_gen_harness_closure_complete",
        (scenario_status(&scenarios, "next_gen_harness_closure") == Some("passed")
            && next_gen.get("status").and_then(Value::as_str) == Some("passed")
            || ((level != "deep" || focused_live_claim)
                && scenario_status(&scenarios, "next_gen_harness_closure")
                    == Some("not_observed")
                && next_gen.get("status").and_then(Value::as_str) == Some("not_observed")))
            && next_gen
                .get("failed")
                .and_then(Value::as_u64)
                .is_some_and(|failed| failed == 0)
            && next_gen_scenarios.len() >= 7,
        true,
        format!(
            "status={}, scenarios={}, failed={}",
            next_gen
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("missing"),
            next_gen_scenarios.len(),
            next_gen
                .get("failed")
                .and_then(Value::as_u64)
                .unwrap_or_default()
        ),
        "run the next_gen_harness_closure suite and keep all terminal scenarios evidenced",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "claimed_orchestration_has_runtime_actions",
        !claims_orchestration || runtime_actions >= 3,
        true,
        format!("claims_orchestration={claims_orchestration}, runtime_actions={runtime_actions}"),
        "reports that claim orchestration must include runtime action trace evidence",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "claimed_tool_validation_has_tool_evidence",
        !claims_tool_validation || tool_calls > 0,
        true,
        format!("claims_tool_validation={claims_tool_validation}, tool_calls={tool_calls}"),
        "reports that claim tool validation must include real local tool execution evidence",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "claimed_memory_context_has_context_report",
        !claims_memory_context
            || (reality_context
                .get("evidence_ref_total")
                .and_then(Value::as_u64)
                .is_some_and(|count| count > 0)
                && next_gen_evidence_refs_total(&next_gen_scenarios) > 0),
        true,
        format!(
            "claims_memory_context={}, reality_evidence_refs={}, next_gen_refs={}",
            claims_memory_context,
            reality_context
                .get("evidence_ref_total")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            next_gen_evidence_refs_total(&next_gen_scenarios)
        ),
        "memory/context claims must include RecallReport/ContextEnvelope evidence refs",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "claimed_replay_has_evidence_refs",
        !claims_replay || next_gen_claimed_evidence_refs(&next_gen_scenarios, "claims_replay") > 0,
        true,
        format!(
            "claims_replay={}, replay_refs={}",
            claims_replay,
            next_gen_claimed_evidence_refs(&next_gen_scenarios, "claims_replay")
        ),
        "replay/recovery claims must include replay, conflict, or recovery evidence refs",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "claimed_external_access_has_health_evidence",
        !claims_external_access || manifest_has_external_health(evidence_manifest),
        true,
        format!(
            "claims_external_access={}, source={}, sidecar={}, db={}",
            claims_external_access,
            evidence_manifest
                .get("source_fixture_status")
                .and_then(Value::as_str)
                .unwrap_or("missing"),
            evidence_manifest
                .get("sidecar_fixture_status")
                .and_then(Value::as_str)
                .unwrap_or("missing"),
            evidence_manifest
                .get("db_fixture_status")
                .and_then(Value::as_str)
                .unwrap_or("missing")
        ),
        "external source/sidecar claims must include explicit connected or healthy fixture evidence",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "real_model_claim_has_provider_rounds",
        !real_model_claimed || runtime_provider_rounds(trace) > 0 || live_provider_rounds > 0,
        true,
        format!(
            "real_model_claimed={}, trace_provider_rounds={}, live_provider_rounds={}",
            real_model_claimed,
            runtime_provider_rounds(trace),
            live_provider_rounds
        ),
        "deep/real reports may not claim real model evidence with zero provider rounds",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "real_provider_cache_usage_known",
        !real_model_claimed || cache_usage_known,
        true,
        format!(
            "real_model_claimed={real_model_claimed}, prompt_tokens={cache_prompt_tokens}, physical_attempts={cache_physical_attempts}, unknown_cache_attempts={cache_dimensions_unknown_attempts}, cache_dimensions_known={cache_usage_known}"
        ),
        "persist canonical hit/miss usage for every real Provider run",
    ));
    let cache_slo_applicable =
        real_model_claimed && runtime_provider_rounds(trace).max(live_provider_rounds) >= 3;
    items.push(HarnessEvalReportGateItem::new(
        "real_provider_cache_cold_inclusive_90pct",
        !cache_slo_applicable || cache_hit_ratio_bp >= 9_000,
        true,
        format!(
            "applicable={cache_slo_applicable}, cache_hit_ratio_bp={cache_hit_ratio_bp}, prompt_tokens={cache_prompt_tokens}"
        ),
        "fix request prefix layout and cold-cohort coordination before deep paid acceptance",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "real_provider_cache_structure_95pct",
        !cache_slo_applicable
            || (structural_prompt_bytes > 0 && structural_reuse_ratio_bp >= 9_500),
        true,
        format!(
            "applicable={cache_slo_applicable}, structural_reuse_ratio_bp={structural_reuse_ratio_bp}, prompt_bytes={structural_prompt_bytes}"
        ),
        "preserve exact model-visible prefixes and persist request-level wire evidence",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "real_provider_cache_warm_95pct",
        !cache_slo_applicable
            || (warm_provider_prompt_tokens > 0
                && warm_cache_hit_ratio_bp >= 9_500
                && warm_structural_reuse_ratio_bp >= 9_500),
        true,
        format!(
            "applicable={cache_slo_applicable}, warm_cache_hit_ratio_bp={warm_cache_hit_ratio_bp}, warm_structural_reuse_ratio_bp={warm_structural_reuse_ratio_bp}, warm_prompt_tokens={warm_provider_prompt_tokens}"
        ),
        "keep warm requests append-only and investigate Provider cache-unit misses",
    ));
    push_manifest_gate_item(&mut items, evidence_manifest, real_model_claimed);

    push_execution_level_gate_items(
        &mut items,
        ExecutionLevelGateContext {
            is_quick,
            complex,
            real_tools,
            parity,
            tool_calls,
            tool_log_count,
            total_tokens,
            live_total_tokens,
            usage_source,
        },
    );

    push_deep_live_gate_item(&mut items, level, live);

    HarnessEvalReportGate::from_items(items)
}

fn push_deep_live_gate_item(items: &mut Vec<HarnessEvalReportGateItem>, level: &str, live: &Value) {
    if level != "deep" {
        return;
    }
    let live_scenarios = live
        .get("scenarios")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let selected_scenario_count = live
        .get("selected_scenario_ids")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    // A full deep suite requires broad coverage. A focused costly run is
    // judged against exactly its selected set; an unknown selection retains
    // the broad three-scenario floor.
    let required_live_scenarios = if selected_scenario_count > 0 {
        selected_scenario_count
    } else {
        3
    };
    let all_traces_complete = live_scenarios.iter().all(|scenario| {
        scenario.get("status").and_then(Value::as_str) == Some("passed")
            && ["session_id", "terminal_id", "execution_id"]
                .iter()
                .all(|field| {
                    scenario
                        .pointer(&format!("/production_trace/{field}"))
                        .and_then(Value::as_str)
                        .is_some_and(|value| !value.is_empty())
                })
    });
    let all_observations_complete = live_scenarios.iter().all(|scenario| {
        let observation = scenario
            .get("observation_integrity")
            .unwrap_or(&Value::Null);
        observation.get("status").and_then(Value::as_str) == Some("passed")
            && observation.get("cursor_monotonic").and_then(Value::as_bool) == Some(true)
            && observation.get("timeline_drained").and_then(Value::as_bool) == Some(true)
            && observation
                .get("message_pages_drained")
                .and_then(Value::as_bool)
                == Some(true)
            && observation.get("omitted_changes").and_then(Value::as_u64) == Some(0)
            && observation.get("stall_detected").and_then(Value::as_bool) == Some(false)
    });
    items.push(HarnessEvalReportGateItem::new(
        "deep_live_gateway_scenarios",
        live.get("status").and_then(Value::as_str) == Some("passed")
            && live_scenarios.len() >= required_live_scenarios
            && all_traces_complete
            && all_observations_complete,
        true,
        format!(
            "live_status={}, scenarios={}, required_scenarios={}, complete_traces={}, complete_observations={}",
            live.get("status")
                .and_then(Value::as_str)
                .unwrap_or("missing"),
            live_scenarios.len(),
            required_live_scenarios,
            all_traces_complete,
            all_observations_complete
        ),
        "run deep evaluation against an explicit isolated COWD_EVAL_GATEWAY_URL and retain durable session, terminal, execution and cursor traces for every live scenario",
    ));
}

struct ExecutionLevelGateContext<'a> {
    is_quick: bool,
    complex: &'a Value,
    real_tools: &'a Value,
    parity: &'a Value,
    tool_calls: u64,
    tool_log_count: u64,
    total_tokens: u64,
    live_total_tokens: u64,
    usage_source: &'a str,
}

fn push_execution_level_gate_items(
    items: &mut Vec<HarnessEvalReportGateItem>,
    context: ExecutionLevelGateContext<'_>,
) {
    if context.is_quick {
        items.push(HarnessEvalReportGateItem::new(
            "quick_smoke_declares_no_tool_lane",
            context.tool_calls == 0 && context.usage_source == "deterministic_smoke",
            true,
            format!(
                "tool_calls={}, usage_source={}",
                context.tool_calls, context.usage_source
            ),
            "quick must either stay explicitly deterministic or be promoted to full eval",
        ));
        return;
    }

    let complex_ok = context
        .complex
        .get("failed")
        .and_then(Value::as_u64)
        .is_some_and(|failed| failed == 0)
        && context
            .complex
            .get("average_score")
            .and_then(Value::as_f64)
            .is_some_and(|score| score >= 0.9);
    let real_tool_calls = context
        .real_tools
        .get("tool_calls")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    items.push(HarnessEvalReportGateItem::new(
        "complex_scenarios_passed",
        complex_ok,
        true,
        format!(
            "failed={}, average={}",
            context
                .complex
                .get("failed")
                .and_then(Value::as_u64)
                .unwrap_or_default(),
            context
                .complex
                .get("average_score")
                .and_then(Value::as_f64)
                .unwrap_or_default()
        ),
        "full/deep eval must execute the complex scenario suite",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "complex_tool_calls_nonzero",
        context.tool_calls > 0 && real_tool_calls > 0,
        true,
        format!(
            "trace_tool_calls={}, real_tool_calls={real_tool_calls}",
            context.tool_calls
        ),
        "full/deep eval cannot pass complex scenarios with zero real tool evidence",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "token_usage_nonzero_or_estimated",
        (context.total_tokens > 0 || context.live_total_tokens > 0)
            && context.usage_source != "unavailable",
        true,
        format!(
            "trace_total_tokens={}, live_total_tokens={}, usage_source={}",
            context.total_tokens, context.live_total_tokens, context.usage_source
        ),
        "record provider usage or explicit deterministic/estimated fallback",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "events_observations_parity",
        context.parity.get("status").and_then(Value::as_str) == Some("passed")
            && context.tool_calls == context.tool_log_count,
        true,
        format!(
            "parity={}, tool_calls={}, tool_log_count={}",
            context
                .parity
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("missing"),
            context.tool_calls,
            context.tool_log_count
        ),
        "tool events, observations, and report trace must share one count",
    ));
}

fn push_manifest_gate_item(
    items: &mut Vec<HarnessEvalReportGateItem>,
    manifest: &Value,
    real_model_claimed: bool,
) {
    let commit = manifest
        .get("commit")
        .and_then(Value::as_str)
        .unwrap_or("missing");
    let source_sha256 = manifest
        .get("candidate_source_sha256")
        .and_then(Value::as_str);
    let dirty_state = manifest
        .get("target_repo_dirty_state")
        .and_then(Value::as_str)
        .unwrap_or("missing");
    let complete = manifest.get("kind").and_then(Value::as_str)
        == Some("harness_eval.evidence_manifest")
        && manifest
            .get("repo")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
        && valid_git_commit(commit)
        && manifest
            .get("version")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
        && manifest
            .get("command")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
        && (!real_model_claimed
            || (source_sha256.is_some_and(valid_sha256) && dirty_state == "clean"));
    items.push(HarnessEvalReportGateItem::new(
        "evidence_manifest_complete",
        complete,
        true,
        format!(
            "repo={}, commit={}, source_sha256={}, dirty_state={}, version={}",
            manifest
                .get("repo")
                .and_then(Value::as_str)
                .unwrap_or("missing"),
            commit,
            source_sha256.unwrap_or("missing"),
            dirty_state,
            manifest
                .get("version")
                .and_then(Value::as_str)
                .unwrap_or("missing")
        ),
        "write a complete evidence manifest before accepting the report package",
    ));
}

fn valid_git_commit(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn scenario_status<'a>(scenarios: &'a [Value], capability: &str) -> Option<&'a str> {
    scenarios
        .iter()
        .find(|item| item.get("capability").and_then(Value::as_str) == Some(capability))
        .and_then(|item| item.get("status"))
        .and_then(Value::as_str)
}

fn next_gen_claims(scenarios: &[Value], field: &str) -> bool {
    scenarios.iter().any(|scenario| {
        scenario
            .get(field)
            .and_then(Value::as_bool)
            .unwrap_or(false)
    })
}

fn next_gen_evidence_refs_total(scenarios: &[Value]) -> u64 {
    scenarios
        .iter()
        .map(|scenario| {
            scenario
                .get("evidence_refs")
                .and_then(Value::as_array)
                .map_or(0, Vec::len) as u64
        })
        .sum()
}

fn next_gen_claimed_evidence_refs(scenarios: &[Value], claim_field: &str) -> u64 {
    scenarios
        .iter()
        .filter(|scenario| {
            scenario
                .get(claim_field)
                .and_then(Value::as_bool)
                .unwrap_or(false)
        })
        .map(|scenario| {
            scenario
                .get("evidence_refs")
                .and_then(Value::as_array)
                .map_or(0, Vec::len) as u64
        })
        .sum()
}

fn manifest_has_external_health(manifest: &Value) -> bool {
    [
        "source_fixture_status",
        "sidecar_fixture_status",
        "db_fixture_status",
    ]
    .iter()
    .any(|key| {
        manifest
            .get(*key)
            .and_then(Value::as_str)
            .is_some_and(|value| matches!(value, "connected" | "healthy" | "passed"))
    })
}

fn runtime_provider_rounds(trace: &Value) -> u64 {
    trace
        .get("provider_rounds")
        .and_then(Value::as_u64)
        .unwrap_or_default()
}

fn live_gateway_provider_rounds(live: &Value) -> u64 {
    live.get("scenarios")
        .and_then(Value::as_array)
        .map(|scenarios| {
            scenarios
                .iter()
                .map(|scenario| {
                    scenario
                        .pointer("/metrics/model_rounds")
                        .and_then(Value::as_u64)
                        .unwrap_or_default()
                })
                .sum()
        })
        .unwrap_or_default()
}

fn live_gateway_total_tokens(live: &Value) -> u64 {
    live.get("scenarios")
        .and_then(Value::as_array)
        .map(|scenarios| {
            scenarios
                .iter()
                .map(|scenario| {
                    scenario
                        .pointer("/metrics/total_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or_default()
                })
                .sum()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod gate_tests {
    use super::*;
    use serde_json::json;

    fn real_manifest_report(commit: &str, source_sha256: Option<&str>, dirty: &str) -> Value {
        json!({
            "level": "deep",
            "authorized_real_model": true,
            "evidence_manifest": {
                "kind": "harness_eval.evidence_manifest",
                "repo": "/workspace/cowd",
                "commit": commit,
                "candidate_source_sha256": source_sha256,
                "target_repo_dirty_state": dirty,
                "version": "0.9.711",
                "command": "harness-eval deep --allow-real-model"
            }
        })
    }

    fn gate_item<'a>(gate: &'a HarnessEvalReportGate, name: &str) -> &'a HarnessEvalReportGateItem {
        gate.items
            .iter()
            .find(|item| item.name == name)
            .expect("gate item")
    }

    fn verified_agentic_program_closure() -> Value {
        json!({
            "projection": {
                "program_id": "program-test",
                "objective_id": "objective-test",
                "session_id": "session-test",
                "turn_id": "turn-test",
                "root_execution_id": "execution-test",
                "required_team_count": 1,
                "status": "verified",
                "teams": {
                    "team-test": {
                        "team_id": "team-test",
                        "name": "Research",
                        "topic_ref": "topic-test",
                        "member_ids": ["agent-claimant", "agent-reviewer"],
                        "task_ids": ["task-test"]
                    }
                },
                "agents": {
                    "agent-claimant": {"agent_id": "agent-claimant", "team_id": "team-test"},
                    "agent-reviewer": {"agent_id": "agent-reviewer", "team_id": "team-test"}
                },
                "tasks": {
                    "task-test": {
                        "task_id": "task-test",
                        "team_id": "team-test",
                        "status": "accepted",
                        "claimant": "agent-claimant",
                        "reviewed_by": "agent-reviewer",
                        "artifact_refs": ["artifact-test"],
                        "evidence_refs": ["evidence:test"],
                        "depends_on": []
                    }
                },
                "artifacts": {
                    "artifact-test": {
                        "artifact_ref": "artifact-test",
                        "content_ref": "content:test",
                        "committed_by": "agent-claimant"
                    }
                },
                "topics": {
                    "topic-test": [{
                        "entry_id": "entry-test",
                        "actor_id": "agent-claimant",
                        "refs": ["artifact-test"]
                    }]
                },
                "final_artifact_ref": "artifact-test",
                "completion_request": {
                    "action_id": "complete-test",
                    "requested_by": "agent-claimant",
                    "program_revision": 2,
                    "final_artifact_ref": "artifact-test",
                    "evidence_refs": ["evidence:test"]
                },
                "objective_verdict": {
                    "kind": "satisfied",
                    "terminal_fence": "terminal:test",
                    "authority_revision": 2
                },
                "unresolved": []
            }
        })
    }

    #[test]
    fn gate_accepts_real_gateway_model_metrics_without_report_reviewer() {
        let report = json!({
            "level": "deep",
            "authorized_real_model": true,
            "execution_trace": {
                "provider_rounds": 0,
                "total_usage": {"total_tokens": 0, "usage_source": "deterministic_smoke"}
            },
            "live_gateway_scenarios": {
                "status": "passed",
                "scenarios": [{
                    "status": "passed",
                    "metrics": {"model_rounds": 2, "total_tokens": 321}
                }]
            }
        });

        let gate = evaluate_report_gate(&report);
        let item = |name: &str| {
            gate.items
                .iter()
                .find(|item| item.name == name)
                .expect("gate item")
        };
        assert_eq!(
            item("real_model_claim_has_provider_rounds").status,
            "passed"
        );
        assert_eq!(item("token_usage_nonzero_or_estimated").status, "passed");
    }

    #[test]
    fn agentic_program_closure_requires_all_durable_entity_facts() {
        let closure = verified_agentic_program_closure();
        let result = evaluate_agentic_program_closure(&closure);
        assert_eq!(result["status"], "passed");
        assert_eq!(result["facts"]["teams"], 1);
        assert_eq!(result["facts"]["agents"], 2);
        assert_eq!(result["facts"]["active_tasks"], 1);
        assert_eq!(result["facts"]["artifacts"], 1);
        assert_eq!(result["facts"]["topics"], 1);

        let mut tampered = closure;
        tampered["projection"]["tasks"]["task-test"]["reviewed_by"] = json!("agent-claimant");
        let result = evaluate_agentic_program_closure(&tampered);
        assert_eq!(result["status"], "failed");
        assert!(result["missing"]
            .as_array()
            .expect("missing checks")
            .iter()
            .any(|item| item == "tasks_accepted_and_independently_verified"));
    }

    #[test]
    fn agentic_program_claim_cannot_pass_from_summary_status_alone() {
        let report = json!({
            "level": "deep",
            "scenarios": [{"capability": "agentic_program_closure", "status": "passed"}],
            "agentic_program_closure": {"status": "passed", "facts": {"teams": 9}}
        });
        let gate = evaluate_report_gate(&report);
        assert_eq!(
            gate_item(&gate, "agentic_program_closure_verified").status,
            "failed"
        );
    }

    #[test]
    fn real_multi_round_cache_gate_uses_cold_inclusive_provider_denominator() {
        let mut report = real_manifest_report(
            "0123456789abcdef0123456789abcdef01234567",
            Some("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
            "clean",
        );
        report["execution_trace"] = json!({
            "provider_rounds": 3,
            "total_usage": {
                "input_tokens": 10,
                "output_tokens": 1,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 90,
                "total_tokens": 101,
                "usage_source": "provider_event"
            },
            "provider_cache": {
                "provider_prompt_tokens": 100,
                "cache_hit_ratio_bp": 9000,
                "provider_rounds": 3,
                "physical_provider_attempts": 3,
                "usage_known": true,
                "cache_dimensions_known": true,
                "cache_dimensions_unknown_attempts": 0,
                "model_visible_prompt_bytes": 10000,
                "structural_reuse_ratio_bp": 9500,
                "warm_provider_prompt_tokens": 80,
                "warm_cache_hit_ratio_bp": 9500,
                "warm_structural_reuse_ratio_bp": 9900
            }
        });
        let passed = evaluate_report_gate(&report);
        assert_eq!(
            gate_item(&passed, "real_provider_cache_cold_inclusive_90pct").status,
            "passed"
        );

        report["execution_trace"]["provider_cache"]["cache_hit_ratio_bp"] = json!(8_999);
        let failed = evaluate_report_gate(&report);
        assert_eq!(
            gate_item(&failed, "real_provider_cache_cold_inclusive_90pct").status,
            "failed"
        );

        report["execution_trace"]["provider_rounds"] = json!(0);
        report["execution_trace"]["provider_cache"]["provider_rounds"] = json!(0);
        report["execution_trace"]["provider_cache"]["physical_provider_attempts"] = json!(1);
        report["execution_trace"]["provider_cache"]["cache_dimensions_unknown_attempts"] = json!(1);
        report["execution_trace"]["provider_cache"]["cache_dimensions_known"] = json!(true);
        let unknown_cache = evaluate_report_gate(&report);
        assert_eq!(
            gate_item(&unknown_cache, "real_provider_cache_usage_known").status,
            "failed",
            "a declared known flag must not override an unknown physical attempt"
        );
    }

    #[test]
    fn deep_gate_accepts_a_complete_explicitly_selected_live_scenario() {
        let report = json!({
            "level": "deep",
            "live_gateway_scenarios": {
                "status": "passed",
                "selected_scenario_ids": ["live_group_theory_ai_research_simulation"],
                "scenarios": [{
                    "status": "passed",
                    "production_trace": {
                        "session_id": "session-1",
                        "terminal_id": "terminal-1",
                        "execution_id": "execution-1"
                    },
                    "observation_integrity": {
                        "status": "passed",
                        "cursor_monotonic": true,
                        "timeline_drained": true,
                        "message_pages_drained": true,
                        "omitted_changes": 0,
                        "stall_detected": false
                    }
                }]
            }
        });

        let gate = evaluate_report_gate(&report);
        let item = gate
            .items
            .iter()
            .find(|item| item.name == "deep_live_gateway_scenarios")
            .expect("gate item");
        assert_eq!(item.status, "passed");
        assert!(item.evidence.contains("required_scenarios=1"));
        assert!(item.evidence.contains("complete_observations=true"));
    }

    #[test]
    fn focused_deep_claim_does_not_claim_unselected_next_gen_scenarios() {
        let next_gen_scenarios = (0..7)
            .map(|index| json!({"scenario_id": format!("scenario-{index}")}))
            .collect::<Vec<_>>();
        let mut report = json!({
            "level": "deep",
            "scenarios": [{
                "capability": "next_gen_harness_closure",
                "status": "not_observed"
            }],
            "next_gen_harness_closure": {
                "status": "not_observed",
                "failed": 0,
                "scenarios": next_gen_scenarios
            },
            "live_gateway_scenarios": {"claim_scope": "focused"}
        });

        let focused = evaluate_report_gate(&report);
        assert_eq!(
            gate_item(&focused, "scenario_capability_status").status,
            "passed"
        );
        assert_eq!(
            gate_item(&focused, "next_gen_harness_closure_complete").status,
            "passed"
        );

        report["live_gateway_scenarios"]["claim_scope"] = json!("release-certification");
        let release = evaluate_report_gate(&report);
        assert_eq!(
            gate_item(&release, "next_gen_harness_closure_complete").status,
            "failed"
        );
    }

    #[test]
    fn deep_gate_rejects_a_live_scenario_without_complete_observation_evidence() {
        let report = json!({
            "level": "deep",
            "live_gateway_scenarios": {
                "status": "passed",
                "selected_scenario_ids": ["live_group_theory_ai_research_simulation"],
                "scenarios": [{
                    "status": "passed",
                    "production_trace": {
                        "session_id": "session-1",
                        "terminal_id": "terminal-1",
                        "execution_id": "execution-1"
                    },
                    "observation_integrity": {
                        "status": "passed",
                        "cursor_monotonic": true,
                        "timeline_drained": false,
                        "message_pages_drained": true,
                        "omitted_changes": 0,
                        "stall_detected": false
                    }
                }]
            }
        });

        let gate = evaluate_report_gate(&report);
        let item = gate_item(&gate, "deep_live_gateway_scenarios");
        assert_eq!(item.status, "failed");
        assert!(item.evidence.contains("complete_observations=false"));
    }

    #[test]
    fn real_model_manifest_rejects_unknown_or_unbound_candidate_identity() {
        let unknown = evaluate_report_gate(&real_manifest_report(
            "unknown",
            Some(&"a".repeat(64)),
            "clean",
        ));
        assert_eq!(
            gate_item(&unknown, "evidence_manifest_complete").status,
            "failed"
        );

        let unchecked = evaluate_report_gate(&real_manifest_report(
            &"b".repeat(40),
            Some(&"c".repeat(64)),
            "not_checked_by_library_runner",
        ));
        assert_eq!(
            gate_item(&unchecked, "evidence_manifest_complete").status,
            "failed"
        );

        let unarchived =
            evaluate_report_gate(&real_manifest_report(&"d".repeat(40), None, "clean"));
        assert_eq!(
            gate_item(&unarchived, "evidence_manifest_complete").status,
            "failed"
        );
    }

    #[test]
    fn real_model_manifest_accepts_clean_archived_candidate_identity() {
        let gate = evaluate_report_gate(&real_manifest_report(
            &"e".repeat(40),
            Some(&"f".repeat(64)),
            "clean",
        ));
        assert_eq!(
            gate_item(&gate, "evidence_manifest_complete").status,
            "passed"
        );
    }
}

#[derive(Debug, Serialize)]
pub struct CapabilityResult {
    pub capability: &'static str,
    pub status: &'static str,
    pub evidence: String,
    pub notes: String,
}

#[derive(Debug, Serialize)]
pub struct HarnessMetric {
    pub name: &'static str,
    pub value: String,
    pub notes: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct UsageSummary {
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub cache_creation_input_tokens: u32,
    pub cache_read_input_tokens: u32,
    pub total_tokens: u32,
    pub usage_source: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderRoundSummary {
    pub round_index: usize,
    pub name: String,
    pub model: String,
    pub status: String,
    pub elapsed_ms: u128,
    pub usage: UsageSummary,
    pub text_delta_count: usize,
    pub tool_use_count: usize,
    pub request_summary: String,
    pub response_summary: String,
    pub detail_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProviderRoundDetail {
    pub summary: ProviderRoundSummary,
    pub request: Value,
    pub events: Vec<Value>,
    pub response_text: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolCallSummary {
    pub call_index: usize,
    pub scenario_id: String,
    pub name: String,
    pub status: String,
    pub elapsed_ms: u128,
    pub input_summary: String,
    pub output_summary: String,
    pub detail_path: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolCallDetail {
    pub summary: ToolCallSummary,
    pub input: Value,
    pub output: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RealToolScenarioResult {
    pub scenario_id: String,
    pub title: String,
    pub status: String,
    pub tool_calls: usize,
    pub runtime_evidence: Vec<String>,
    pub matrix_evidence: Vec<String>,
    pub memory_evidence: Vec<String>,
    pub changed_files: Vec<String>,
    pub diff_summary: String,
    pub conclusion: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RealToolScenarioReport {
    pub kind: &'static str,
    pub target_repo: String,
    pub total: usize,
    pub passed: usize,
    pub tool_calls: usize,
    pub scenarios: Vec<RealToolScenarioResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RuntimeActionTrace {
    pub index: usize,
    pub action: String,
    pub evidence: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionTrace {
    pub kind: &'static str,
    pub started_at_ms: u128,
    pub finished_at_ms: Option<u128>,
    pub total_elapsed_ms: Option<u128>,
    pub provider_rounds: usize,
    pub runtime_actions: usize,
    pub tool_calls: usize,
    pub total_usage: UsageSummary,
    pub rounds: Vec<ProviderRoundSummary>,
    pub tool_call_log: Vec<ToolCallSummary>,
    pub runtime_action_log: Vec<RuntimeActionTrace>,
}

impl ExecutionTrace {
    #[must_use]
    pub fn start() -> Self {
        Self {
            kind: "harness_eval.execution_trace",
            started_at_ms: now_ms_u128(),
            finished_at_ms: None,
            total_elapsed_ms: None,
            provider_rounds: 0,
            runtime_actions: 0,
            tool_calls: 0,
            total_usage: UsageSummary {
                usage_source: "unavailable".to_string(),
                ..UsageSummary::default()
            },
            rounds: Vec::new(),
            tool_call_log: Vec::new(),
            runtime_action_log: Vec::new(),
        }
    }

    pub fn record_runtime_action(
        &mut self,
        action: impl Into<String>,
        evidence: impl Into<String>,
    ) {
        let index = self.runtime_action_log.len() + 1;
        self.runtime_action_log.push(RuntimeActionTrace {
            index,
            action: action.into(),
            evidence: evidence.into(),
        });
        self.runtime_actions = self.runtime_action_log.len();
    }

    pub fn add_provider_round(&mut self, summary: ProviderRoundSummary) {
        self.total_usage.input_tokens = self
            .total_usage
            .input_tokens
            .saturating_add(summary.usage.input_tokens);
        self.total_usage.output_tokens = self
            .total_usage
            .output_tokens
            .saturating_add(summary.usage.output_tokens);
        self.total_usage.cache_creation_input_tokens = self
            .total_usage
            .cache_creation_input_tokens
            .saturating_add(summary.usage.cache_creation_input_tokens);
        self.total_usage.cache_read_input_tokens = self
            .total_usage
            .cache_read_input_tokens
            .saturating_add(summary.usage.cache_read_input_tokens);
        self.total_usage.total_tokens = self
            .total_usage
            .total_tokens
            .saturating_add(summary.usage.total_tokens);
        if summary.usage.usage_source == "provider_event" {
            self.total_usage.usage_source = "provider_event".to_string();
        }
        self.rounds.push(summary);
        self.provider_rounds = self.rounds.len();
    }

    pub fn add_tool_call(&mut self, summary: ToolCallSummary) {
        self.tool_call_log.push(summary);
        self.tool_calls = self.tool_call_log.len();
    }

    pub fn finish(&mut self, started: Instant) {
        self.finished_at_ms = Some(now_ms_u128());
        self.total_elapsed_ms = Some(started.elapsed().as_millis());
    }
}

#[derive(Debug, Serialize)]
pub struct MissionHarnessEvalReport {
    pub kind: &'static str,
    pub level: HarnessEvalLevel,
    pub status: String,
    pub provider: Option<String>,
    pub budget: Option<String>,
    pub gateway_process: bool,
    pub scenario_matrix: Vec<E2eScenarioMatrixItem>,
    pub stable_ai: StableAiHealthReport,
    pub scenarios: Vec<CapabilityResult>,
    pub metrics: Vec<HarnessMetric>,
    pub complex_scenarios: Option<ComplexHarnessScenarioReport>,
    pub real_tool_scenarios: Option<RealToolScenarioReport>,
    pub execution_trace: ExecutionTrace,
    pub result_package_dir: Option<String>,
    #[serde(skip)]
    pub provider_round_details: Vec<ProviderRoundDetail>,
    #[serde(skip)]
    pub tool_call_details: Vec<ToolCallDetail>,
}

impl HarnessEvalReportSummary {
    #[must_use]
    pub fn from_report_json(
        id: String,
        report_path: String,
        markdown_path: Option<String>,
        report: &Value,
    ) -> Self {
        let trace = report.get("execution_trace").unwrap_or(&Value::Null);
        let scenarios = report
            .get("scenarios")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let total_usage = trace.get("total_usage").unwrap_or(&Value::Null);
        Self {
            id,
            level: report
                .get("level")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            status: report
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
            provider: report
                .get("provider")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            budget: report
                .get("budget")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            report_path,
            markdown_path,
            result_package_dir: report
                .get("result_package_dir")
                .and_then(Value::as_str)
                .map(ToString::to_string),
            created_at_ms: trace
                .get("started_at_ms")
                .and_then(Value::as_u64)
                .map(u128::from)
                .unwrap_or_default(),
            total_elapsed_ms: trace
                .get("total_elapsed_ms")
                .and_then(Value::as_u64)
                .map(u128::from),
            provider_rounds: trace
                .get("provider_rounds")
                .and_then(Value::as_u64)
                .unwrap_or_default() as usize,
            runtime_actions: trace
                .get("runtime_actions")
                .and_then(Value::as_u64)
                .unwrap_or_default() as usize,
            tool_calls: trace
                .get("tool_calls")
                .and_then(Value::as_u64)
                .unwrap_or_default() as usize,
            total_tokens: total_usage
                .get("total_tokens")
                .and_then(Value::as_u64)
                .unwrap_or_default() as u32,
            scenario_count: scenarios.len(),
            failed_capabilities: scenarios
                .iter()
                .filter(|item| item.get("status").and_then(Value::as_str) != Some("passed"))
                .count(),
        }
    }
}

fn now_ms_u128() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
