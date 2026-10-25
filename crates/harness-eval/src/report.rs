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
    pub total_tokens: Option<u32>,
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
    pub total_tokens: Option<u32>,
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

    /// A gate that cannot be evaluated at this level says so. Printing the evaluated
    /// object's failure state as evidence for a pass tells an auditor nothing about
    /// why it passed.
    fn not_required(
        name: impl Into<String>,
        evidence: impl Into<String>,
        repair_hint: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            status: "not_required".to_string(),
            required: false,
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
    // Idempotence is load-bearing here, not defensive style. This evaluator runs
    // at two layers: `runner` evaluates the raw Program projection into a
    // closure, and the report gate evaluates `report.agentic_program_closure`
    // again. The second input is the first one's OUTPUT, which has `checks`,
    // `facts`, `status` and `missing` but none of the projection's `teams` /
    // `agents` / `tasks` maps - so a second pass read every one of them as empty
    // and produced an all-zero evaluation. The verdict happened to agree
    // ("failed" both times), which is exactly what made the defect invisible:
    // the gate evidence read `program=missing, teams=0, agents=0` while the
    // report body carried teams=1, agents=4, tasks=3. Anyone debugging from the
    // gate evidence hunts a Program that never existed.
    //
    // An already-evaluated closure is returned as it stands. Re-deriving it from
    // its own summary is the disconnect, not a cheaper path to the same answer.
    if value.get("checks").and_then(Value::as_array).is_some()
        && value.get("status").and_then(Value::as_str).is_some()
    {
        return value.clone();
    }
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
        // An autonomous objective may legitimately use zero Teams; only an
        // operator-named requirement has to be met.
        required_team_count == 0 || teams.len() >= required_team_count,
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
            // Membership lives on the team side: AgentMemberProjection carries
            // no team_id field (its fields are agent_id, role, mission,
            // membership_ids, ...), so reading agent.team_id here compared None
            // against every team and the check could never pass on a real
            // projection. An agent is well-formed when its id matches its key
            // and some team lists it as a member, which is where the projection
            // records the relationship.
            agent.get("agent_id").and_then(Value::as_str) == Some(agent_id)
                && teams.values().any(|team| {
                    team.get("member_ids")
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
            let status = task.get("status").and_then(Value::as_str);
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
                && status.is_some_and(|status| {
                    // A withdrawn task is a settled task: the work was
                    // intentionally retracted, a legitimate terminal state.
                    // Mon9 closed with four accepted and two withdrawn tasks
                    // and the check failed on the two.
                    matches!(status, "accepted" | "withdrawn")
                })
                // A withdrawn task is an empty shell: no claimant, no reviewer,
                // no artifacts. Only the accepted path requires the full
                // independent-verification shape.
                && (status == Some("withdrawn")
                    || (claimant.is_some_and(|agent_id| agents.contains_key(agent_id))
                        && reviewer.is_some_and(|agent_id| agents.contains_key(agent_id))
                        && claimant != reviewer
                        && !task
                            .get("artifact_refs")
                            .and_then(Value::as_array)
                            .is_none_or(Vec::is_empty)
                        && !task
                            .get("evidence_refs")
                            .and_then(Value::as_array)
                            .is_none_or(Vec::is_empty)))
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
                                            .is_some_and(|actor| {
                                                // The root publishes to topics
                                                // too: mon9's second entry is
                                                // the root's issue disposition,
                                                // and requiring an agent actor
                                                // judged legitimate root
                                                // coordination as malformed.
                                                agents.contains_key(actor)
                                                    || actor.starts_with("root:")
                                            })
                                        && topic_entry_references_something(entry)
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

/// A topic entry references something when any reference-bearing field is
/// non-empty. Mon9's root entry carried its references in
/// `issue_dispositions[].evidence_refs`, not in `refs`, so reading only `refs`
/// judged real coordination as reference-free.
fn topic_entry_references_something(entry: &Value) -> bool {
    let nonempty = |value: Option<&Value>| {
        value
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty())
    };
    if nonempty(entry.get("refs")) || nonempty(entry.get("recipients")) {
        return true;
    }
    entry
        .get("issue_dispositions")
        .and_then(Value::as_array)
        .is_some_and(|items| items.iter().any(|item| nonempty(item.get("evidence_refs"))))
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
                && completion
                    .get("primary_artifact_ref")
                    .and_then(Value::as_str)
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
        closure
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("missing"),
        facts
            .get("program_id")
            .and_then(Value::as_str)
            .unwrap_or("missing"),
        facts
            .get("teams")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        facts
            .get("agents")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        facts
            .get("active_tasks")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        facts
            .get("artifacts")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        facts
            .get("topics")
            .and_then(Value::as_u64)
            .unwrap_or_default(),
        facts
            .get("verified")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    )
}

/// Terminal tool statuses. A `running` record and its terminal record are the same
/// call, so counting both inflates the number by the number of in-flight records.
/// Terminal tool statuses. `refused` is deliberately absent: a refusal is not an execution
/// attempt, so it must not change the count. It is still recorded as an event with a status,
/// so the evidence stays readable - the count and the record are different things.
pub(crate) const TERMINAL_TOOL_STATUSES: [&str; 4] = ["completed", "failed", "denied", "timed_out"];

/// The single counting rule: terminal tool events in a record set.
///
/// The metric and the gate must read this, not two versions of it. One live run held
/// 497 payloads with a tool name while exactly 95 were terminal, and the durable store
/// agreed with 95 while the report read 3 - the report and the evidence were reading two
/// different record paths.
#[must_use]
pub(crate) fn terminal_tool_events_in_events(events: &[Value]) -> u64 {
    events
        .iter()
        .filter_map(|event| event.get("payload"))
        .filter(|payload| payload.get("tool_name").is_some())
        .filter(|payload| {
            payload
                .get("status")
                .and_then(Value::as_str)
                .is_some_and(|status| TERMINAL_TOOL_STATUSES.contains(&status))
        })
        .count() as u64
}

/// Durable terminal tool events across every recorded live scenario.
///
/// This is the evidence lane, and it is not the harness's own tool-batch counter: the
/// evidence stream also carries the governed actions of delegated Agents, which the
/// local counter never sees. A deep run that presents only the local counter reads as
/// though it did almost no work - one live run presented 3 tool calls while its
/// evidence held 35 terminal events. Both lanes are reported, and the counting rule
/// itself stays shared with the gate.
#[must_use]
pub fn terminal_tool_events(report: &Value) -> u64 {
    report
        .get("live_gateway_scenarios")
        .and_then(|live| live.get("scenarios"))
        .and_then(Value::as_array)
        .map(|scenarios| {
            scenarios
                .iter()
                .map(|scenario| scenario_tool_call_count_evidence(scenario))
                .sum()
        })
        .unwrap_or_default()
}

fn scenario_tool_call_count_evidence(scenario: &Value) -> u64 {
    scenario
        .pointer("/failure_diagnostics/timeline/events")
        .and_then(Value::as_array)
        .map_or(0, |events| terminal_tool_events_in_events(events))
}

/// One scenario's tool-call count, from one rule shared by the metric and the gate.
///
/// The reported number is used when the scenario reached a terminal state; otherwise the
/// scenario's own durable record is the authority. Keeping these as two paths is what let a
/// live run report 0 tool calls for a scenario whose evidence held 124 terminal tool events.
#[must_use]
pub(crate) fn scenario_tool_call_count(scenario: &Value) -> u64 {
    scenario
        .pointer("/metrics/tool_calls")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| {
            scenario
                .pointer("/failure_diagnostics/timeline/events")
                .and_then(Value::as_array)
                .map_or(0, |events| terminal_tool_events_in_events(events))
        })
}

/// The tool-lane gates, from one counting rule.
///
/// The assertion is one-directional on purpose. The report headline and the evidence are not
/// the same lane: the headline is the harness's own local tool-batch count (`runner.rs:155`),
/// while the evidence is the durable event stream, which includes the governed actions of
/// delegated Agents. Asserting equality between them would fail every multi-agent run - that
/// is over-strictness, not a judgement. What is a defect is the report presenting a number
/// below its own record, which is what one live run did: it presented 0 tool calls for a
/// scenario whose evidence held 124 terminal tool events.
///
/// Because the metric takes the maximum over its sources, this gate cannot fail while the
/// counting rule is intact. It is a guard against the rule regressing, not a detector; the
/// independent check against the durable store stays a script, because the harness does not
/// hold the database in-process - the isolated run creates it from the shell wrapper.
fn push_tool_evidence_gate_items(
    items: &mut Vec<HarnessEvalReportGateItem>,
    report: &Value,
    tool_calls: u64,
    claims_tool_validation: bool,
) {
    let scenarios = report
        .get("live_gateway_scenarios")
        .and_then(|live| live.get("scenarios"))
        .and_then(Value::as_array)
        .map_or(&[][..], |scenarios| scenarios.as_slice());

    let mut checked_scenarios = 0_usize;
    let mut understated = Vec::new();
    let mut total_terminal_events = 0_u64;
    for scenario in scenarios {
        let events = scenario
            .pointer("/failure_diagnostics/timeline/events")
            .and_then(Value::as_array)
            .map_or(&[][..], |events| events.as_slice());
        let terminal = terminal_tool_events_in_events(events);
        if terminal == 0 {
            continue;
        }
        total_terminal_events = total_terminal_events.saturating_add(terminal);
        let reported = scenario_tool_call_count(scenario);
        checked_scenarios += 1;
        if reported < terminal {
            understated.push(format!(
                "{}: reported {reported}, evidence {terminal}",
                scenario
                    .get("scenario_id")
                    .and_then(Value::as_str)
                    .unwrap_or("scenario")
            ));
        }
    }

    // Real tool execution may be recorded by either path: the harness's own captured calls,
    // or the durable event stream. A claim gate that only accepted the event stream would
    // fail a deterministic run whose tool evidence is the harness's own calls.
    items.push(HarnessEvalReportGateItem::new(
        "claimed_tool_validation_has_tool_evidence",
        !claims_tool_validation || tool_calls.max(total_terminal_events) > 0,
        true,
        format!(
            "claims_tool_validation={claims_tool_validation}, tool_calls={tool_calls}, terminal_tool_events={total_terminal_events}"
        ),
        "reports that claim tool validation must include real local tool execution evidence",
    ));

    items.push(if checked_scenarios == 0 {
        HarnessEvalReportGateItem::not_required(
            "report_tool_counts_do_not_understate_the_evidence",
            format!(
                "terminal tool events in evidence: {total_terminal_events}; no scenario recorded tool events, so there is nothing to compare"
            ),
            "not applicable at this level; the comparison is only meaningful once a scenario records tool events and reaches a terminal state",
        )
    } else {
        HarnessEvalReportGateItem::new(
            "report_tool_counts_do_not_understate_the_evidence",
            understated.is_empty(),
            true,
            if understated.is_empty() {
                format!("{checked_scenarios} scenario(s) agree with their evidence")
            } else {
                understated.join("; ")
            },
            "the report must not report fewer tool calls than its own durable record; a shortfall is a counting-source problem, not a behaviour change",
        )
    });
}

#[must_use]
pub(crate) fn observed_usage_source(source: &str) -> bool {
    !source.is_empty()
        && source.split('+').all(|part| {
            matches!(
                part,
                "deterministic_smoke"
                    | "live_gateway_canonical_usage"
                    | "provider"
                    | "provider_event"
                    | "real_provider"
            )
        })
}

pub(crate) fn execution_observation_ready(
    observation: &Value,
    execution_id: &str,
    session_id: &str,
    turn_id: &str,
) -> bool {
    use harness_contract::projection::{
        ExecutionTerminalAxes, TerminalEffectSettlement, TerminalExecutionPhase,
    };

    for (field, expected) in [
        ("execution_id", execution_id),
        ("session_id", session_id),
        ("turn_id", turn_id),
    ] {
        if expected.trim().is_empty()
            || observation.get(field).and_then(Value::as_str) != Some(expected)
        {
            return false;
        }
    }
    let revision = observation.get("revision").and_then(Value::as_u64);
    let cursor = observation.get("cursor").and_then(Value::as_u64);
    let Some(cursor) = cursor.filter(|cursor| *cursor > 0) else {
        return false;
    };
    if !revision.is_some_and(|revision| revision > 0) {
        return false;
    }
    let Some(axes) = observation
        .get("terminal_axes")
        .and_then(|value| serde_json::from_value::<ExecutionTerminalAxes>(value.clone()).ok())
    else {
        return false;
    };
    axes.execution.phase == TerminalExecutionPhase::Terminal
        && axes.execution.settlement == TerminalEffectSettlement::ProvenSettled
        && axes.execution.coverage_complete
        && !axes.execution.evidence_refs.is_empty()
        && axes
            .execution
            .evidence_refs
            .iter()
            .all(|reference| !reference.trim().is_empty())
        && !axes.source_race
        && axes.source_cursor > 0
        && axes.source_cursor <= cursor
}

pub(crate) fn complete_metric_tokens(metrics: &Value) -> Option<u32> {
    let attempts = metrics.get("provider_attempt_count")?.as_u64()?;
    let records = metrics.get("provider_usage_records")?.as_u64()?;
    let rounds = metrics.get("model_rounds")?.as_u64()?;
    if records != attempts
        || attempts < rounds
        || metrics.get("usage_unknown_attempts")?.as_u64()? != 0
        || metrics
            .get("provider_attempt_cache_dimensions_unknown_count")?
            .as_u64()?
            != 0
    {
        return None;
    }
    let input = u32::try_from(metrics.get("input_tokens")?.as_u64()?).ok()?;
    let output = u32::try_from(metrics.get("output_tokens")?.as_u64()?).ok()?;
    let cache_creation =
        u32::try_from(metrics.get("cache_creation_input_tokens")?.as_u64()?).ok()?;
    let cache_read = u32::try_from(metrics.get("cache_read_input_tokens")?.as_u64()?).ok()?;
    let total = u32::try_from(metrics.get("total_tokens")?.as_u64()?).ok()?;
    (input
        .checked_add(output)?
        .checked_add(cache_creation)?
        .checked_add(cache_read)?
        == total)
        .then_some(total)
}

fn timeline_coverage_complete(observation: &Value) -> bool {
    observation.get("timeline_drained").and_then(Value::as_bool) == Some(true)
        && observation.get("cursor_monotonic").and_then(Value::as_bool) == Some(true)
        && observation.get("omitted_changes").and_then(Value::as_u64) == Some(0)
}

pub(crate) fn canonical_total_tokens(report: &Value) -> Option<u32> {
    if report["kind"] == "harness_eval.u5_internal" {
        let leaves = report
            .pointer("/live_gateway_scenarios/scenarios/0/sessions")?
            .as_array()?;
        let usage = u5_stage_usage(leaves).ok()?;
        if usage != report["u5_usage"] {
            return None;
        }
        return u32::try_from(usage["total_tokens"].as_u64()?).ok();
    }

    if report.get("kind").and_then(Value::as_str)
        == Some("harness_eval.paired_public_api_performance")
    {
        let mut samples = Vec::new();
        let pairs = report.get("pairs")?.as_array()?;
        if pairs.is_empty() {
            return None;
        }
        for pair in pairs {
            for endpoint in ["baseline", "candidate"] {
                samples.push(pair.get(endpoint)?);
            }
        }
        for endpoint in ["baseline", "candidate"] {
            samples.push(report.get("warmup")?.get(endpoint)?);
        }
        let mut seen = std::collections::BTreeSet::new();
        let mut total = 0_u32;
        for sample in samples {
            let identity = (
                sample.get("endpoint")?.as_str()?,
                sample.get("execution_id")?.as_str()?,
            );
            let metrics = sample.get("metrics")?;
            if !seen.insert(identity)
                || metrics.get("provider_attempt_count")?.as_u64()? == 0
                || !timeline_coverage_complete(sample.get("usage_observation_integrity")?)
            {
                return None;
            }
            total = total.checked_add(complete_metric_tokens(metrics)?)?;
        }
        if let Some(review_trace) = report.get("execution_trace") {
            let reviewer_total = canonical_total_tokens(&json!({"execution_trace":review_trace}))?;
            total = total.checked_add(reviewer_total)?;
        }
        return Some(total);
    }
    let trace = report.get("execution_trace")?;
    let usage = trace.get("total_usage")?;
    let source = usage.get("usage_source")?.as_str()?;
    if !observed_usage_source(source) {
        return None;
    }
    let total = u32::try_from(usage.get("total_tokens")?.as_u64()?).ok()?;
    let rounds = trace.get("provider_rounds")?.as_u64()?;
    let live = report
        .pointer("/live_gateway_scenarios/scenarios")
        .and_then(Value::as_array);
    if source.split('+').all(|part| part == "deterministic_smoke")
        && rounds == 0
        && live.is_none_or(Vec::is_empty)
    {
        return (total == 0
            && trace.get("rounds")?.as_array()?.is_empty()
            && trace
                .pointer("/provider_cache/physical_provider_attempts")?
                .as_u64()?
                == 0
            && trace
                .pointer("/provider_cache/usage_unknown_attempts")?
                .as_u64()?
                == 0)
            .then_some(0);
    }
    let mut observed_rounds = 0_u64;
    let mut attempts = 0_u64;
    let mut observed_total = 0_u32;
    if let Some(scenarios) = live {
        for scenario in scenarios {
            if !timeline_coverage_complete(scenario.get("observation_integrity")?) {
                return None;
            }
            let metrics = scenario.get("metrics")?;
            observed_total = observed_total.checked_add(complete_metric_tokens(metrics)?)?;
            attempts = attempts.checked_add(metrics.get("provider_attempt_count")?.as_u64()?)?;
            observed_rounds =
                observed_rounds.checked_add(metrics.get("model_rounds")?.as_u64()?)?;
        }
    }
    for round in trace.get("rounds")?.as_array()? {
        if round.get("round_kind").and_then(Value::as_str) == Some("live_gateway_scenario") {
            continue;
        }
        let usage = round.get("usage")?;
        if !observed_usage_source(usage.get("usage_source")?.as_str()?)
            || usage.get("usage_source")?.as_str()? == "deterministic_smoke"
        {
            return None;
        }
        for field in ["input_tokens", "output_tokens"] {
            u32::try_from(usage.get(field)?.as_u64()?).ok()?;
        }
        let metrics = round.get("metrics")?;
        let round_total = complete_metric_tokens(metrics)?;
        if u32::try_from(usage.get("total_tokens")?.as_u64()?).ok()? != round_total {
            return None;
        }
        observed_total = observed_total.checked_add(round_total)?;
        observed_rounds = observed_rounds.checked_add(metrics.get("model_rounds")?.as_u64()?)?;
        attempts = attempts.checked_add(metrics.get("provider_attempt_count")?.as_u64()?)?;
    }
    let cache = trace.get("provider_cache")?;
    if observed_rounds != rounds
        || attempts == 0
        || cache.get("physical_provider_attempts")?.as_u64()? != attempts
        || cache.get("usage_unknown_attempts")?.as_u64()? != 0
        || observed_total != total
    {
        return None;
    }
    Some(total)
}

#[must_use]
/// Exact physical call IDs are additive; missing outcomes/usage never mean zero.
pub(crate) fn u5_stage_usage(leaves: &[Value]) -> Result<Value, String> {
    let mut calls = std::collections::BTreeMap::<String, Value>::new();
    let mut packed = std::collections::BTreeMap::new();
    for leaf in leaves {
        let events = leaf["u5_provider_events"]
            .as_array()
            .ok_or("U5 Provider source missing")?;
        let mut leaf_ids = std::collections::BTreeSet::new();
        for event in events {
            let payload = &event["payload"];
            let id = payload["request_id"]
                .as_str()
                .filter(|s| !s.is_empty())
                .ok_or("U5 call identity missing")?;
            match event["kind"].as_str() {
                Some("context.provider_request_packed") => {
                    if packed
                        .insert(id.to_owned(), payload.clone())
                        .is_some_and(|old| old != *payload)
                    {
                        return Err("U5 conflicting packed call".into());
                    }
                    leaf_ids.insert(id.to_owned());
                }
                Some("context.provider_attempt_outcome") => {
                    leaf_ids.insert(id.to_owned());
                    if calls
                        .insert(id.to_owned(), payload.clone())
                        .is_some_and(|old| old != *payload)
                    {
                        return Err("U5 conflicting call outcome".into());
                    }
                }
                _ => return Err("U5 unknown Provider source".into()),
            }
        }
        if leaf_ids.is_empty() {
            return Err("U5 leaf has no physical call evidence".into());
        }
    }
    if calls.is_empty()
        || packed.keys().any(|id| !calls.contains_key(id))
        || calls.keys().any(|id| !packed.contains_key(id))
    {
        return Err("U5 physical call usage unknown".into());
    }
    let mut result = serde_json::Map::new();
    for key in [
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
    ] {
        let total = calls.values().try_fold(0u64, |sum, call| {
            if call["usage_status"] != "known" {
                return None;
            }
            if key.starts_with("cache_") && call["cache_dimensions_status"] != "known" {
                return None;
            }
            sum.checked_add(call["usage"][key].as_u64()?)
        });
        if matches!(key, "input_tokens" | "output_tokens") && total.is_none() {
            return Err("U5 required usage unknown".into());
        }
        result.insert(key.to_owned(), json!(total));
    }
    let total = [
        "input_tokens",
        "output_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
    ]
    .iter()
    .try_fold(0u64, |sum, key| sum.checked_add(result[*key].as_u64()?));
    result.insert("total_tokens".into(), json!(total));
    result.insert("physical_calls".into(), json!(calls.len()));
    result.insert(
        "source".into(),
        json!("original_provider_call_id_deduplicated"),
    );
    Ok(Value::Object(result))
}

fn u5_stage_gate(report: &Value) -> HarnessEvalReportGate {
    let live = &report["live_gateway_scenarios"];
    let pairs = live["scenarios"].as_array();
    let pair = pairs.and_then(|p| p.first()).unwrap_or(&Value::Null);
    let leaves = pair["sessions"].as_array();
    let leaf_ok = leaves.is_some_and(|l| {
        l.len() == 2
            && l.iter().all(|v| {
                v["status"] == "passed"
                    && v["metrics"]["tool_calls"].as_u64().is_some()
                    && v.pointer("/acceptance/passed") == Some(&Value::Bool(true))
                    && v.pointer("/actor_cleanup/passed") == Some(&Value::Bool(true))
                    && v.pointer("/acceptance/checks")
                        .and_then(Value::as_array)
                        .is_some_and(|checks| {
                            !checks.is_empty() && checks.iter().all(|c| c["passed"] == true)
                        })
            })
    });
    let identities = leaves.is_some_and(|l| {
        l.len() == 2
            && ["session_id", "turn_id", "execution_id"].iter().all(|key| {
                l[0][key].as_str().is_some_and(|a| {
                    !a.is_empty() && l[1][key].as_str().is_some_and(|b| !b.is_empty() && a != b)
                })
            })
    });
    let usage = leaves.and_then(|l| u5_stage_usage(l).ok());
    let checks = [
        (
            "u5_exact_focused_selection",
            report["level"] == "deep"
                && report["authorized_real_model"] == true
                && live["status"] == "pending_outer_cleanup"
                && live["claim_scope"] == "focused"
                && live["health_status"] == "passed"
                && live["selection_status"] == "passed"
                && live["selected_scenario_ids"] == json!(["live_u5_shared_session_fairness"])
                && pairs.is_some_and(|p| p.len() == 1)
                && pair["scenario_id"] == "live_u5_shared_session_fairness",
        ),
        (
            "u5_two_original_leaf_business_permissions_and_ACK",
            leaf_ok && identities,
        ),
        (
            "u5_original_capacity_witness",
            pair["status"] == "pending_outer_cleanup"
                && pair.pointer("/acceptance/checks/internal_passed") == Some(&Value::Bool(true))
                && [
                    "identity",
                    "selected_witnesses_complete",
                    "left_business",
                    "right_business",
                    "capacity_competition",
                ]
                .iter()
                .all(|k| pair["acceptance"]["checks"][k] == true),
        ),
        (
            "u5_original_usage_complete",
            usage
                .as_ref()
                .is_some_and(|u| u["total_tokens"].as_u64().is_some())
                && usage.as_ref().is_some_and(|u| {
                    [
                        "input_tokens",
                        "output_tokens",
                        "cache_creation_input_tokens",
                        "cache_read_input_tokens",
                        "total_tokens",
                    ]
                    .iter()
                    .all(|key| u[*key] == report["execution_trace"]["total_usage"][*key])
                        && u["physical_calls"] == report["execution_trace"]["provider_rounds"]
                })
                && usage == Some(report["u5_usage"].clone()),
        ),
        (
            "u5_host_binding_present",
            report["u5_host"]["nonce"]
                .as_str()
                .is_some_and(|s| s.len() == 64)
                && report["u5_host"]["candidate"]
                    .as_str()
                    .is_some_and(|s| s.len() == 40),
        ),
    ];
    let items = checks
        .iter()
        .map(|(name, ok)| {
            HarnessEvalReportGateItem::new(
                *name,
                *ok,
                true,
                "original U5 inputs",
                "restore exact source evidence",
            )
        })
        .collect::<Vec<_>>();
    let failed = checks.iter().filter(|(_, ok)| !*ok).count();
    HarnessEvalReportGate {
        kind: "harness_eval.u5_internal_gate".into(),
        status: if failed == 0 {
            "pending_outer_cleanup"
        } else {
            "failed"
        }
        .into(),
        total: items.len(),
        passed: items.len() - failed,
        failed,
        items,
    }
}

pub fn evaluate_report_gate(report: &Value) -> HarnessEvalReportGate {
    if report["kind"] == "harness_eval.u5_internal" {
        return u5_stage_gate(report);
    }
    if report.get("kind").and_then(Value::as_str)
        == Some("harness_eval.paired_public_api_performance")
    {
        return paired_report_gate(report);
    }
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
    push_tool_evidence_gate_items(&mut items, report, tool_calls, claims_tool_validation);
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
    let usage_required = real_model_claimed
        || runtime_provider_rounds(trace) > 0
        || cache
            .get("physical_provider_attempts")
            .and_then(Value::as_u64)
            .is_some_and(|count| count > 0)
        || live_provider_rounds > 0;
    items.push(if usage_required {
        HarnessEvalReportGateItem::new(
            "real_provider_usage_complete",
            canonical_total_tokens(report).is_some(),
            true,
            format!("canonical_total_tokens={:?}, usage_source={usage_source}", canonical_total_tokens(report)),
            "retain observed token dimensions and complete physical-attempt coverage; estimates are not observed usage",
        )
    } else {
        HarnessEvalReportGateItem::not_required(
            "real_provider_usage_complete",
            "no real-provider claim",
            "deterministic subtotals do not establish real-provider usage",
        )
    });

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

pub(crate) fn business_observation_ready(scenario: &Value) -> bool {
    let observation = scenario.get("business_observation").unwrap_or(&Value::Null);
    let identity_matches = ["execution_id", "session_id", "turn_id"]
        .into_iter()
        .all(|field| {
            let expected = scenario
                .get(field)
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty());
            expected.is_some() && observation.get(field).and_then(Value::as_str) == expected
        });
    let goal_outcome = scenario.pointer("/terminal_observation/terminal_axes/outcome/outcome");
    identity_matches
        && observation.get("kind").and_then(Value::as_str) == Some("runtime.outcome.recorded.v1")
        && observation.get("execution_scope").and_then(Value::as_str) == Some("session")
        && observation.get("event_status").and_then(Value::as_str) == Some("succeeded")
        && observation.get("terminal_class").and_then(Value::as_str) == Some("succeeded")
        && goal_outcome.is_some_and(|value| value.is_null() || value.as_str() == Some("satisfied"))
}

pub(crate) fn delivery_observation_ready(scenario: &Value) -> bool {
    let delivery = scenario.get("delivery_observation").unwrap_or(&Value::Null);
    let ack = delivery.get("ack").unwrap_or(&Value::Null);
    let current = delivery.get("current_context").unwrap_or(&Value::Null);
    for field in [
        "session_id",
        "message_id",
        "runtime_commit_cursor",
        "content_digest",
        "client_binding_sha256",
    ] {
        if delivery.get(field).is_none()
            || delivery.get(field) != ack.get(field)
            || delivery.get(field) != current.get(field)
        {
            return false;
        }
    }
    if current.get("receipt") != Some(ack)
        || ack.get("received_at_ms") != delivery.get("received_at_ms")
        || current.get("turn_id") != delivery.get("turn_id")
        || current.get("execution_id") != delivery.get("execution_id")
    {
        return false;
    }
    for (observed, expected) in [
        ("session_id", "session_id"),
        ("message_id", "terminal_id"),
        ("turn_id", "turn_id"),
    ] {
        let expected = scenario
            .get(expected)
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        if expected.is_none() || delivery.get(observed).and_then(Value::as_str) != expected {
            return false;
        }
    }
    match delivery.get("execution_id") {
        Some(Value::Null) => {}
        Some(Value::String(execution))
            if scenario.get("execution_id").and_then(Value::as_str) == Some(execution.as_str()) => {
        }
        _ => return false,
    }
    let Some(cursor) = delivery
        .get("runtime_commit_cursor")
        .and_then(Value::as_str)
        .and_then(|value| harness_contract::live::ReceiptCursor::parse(value).ok())
    else {
        return false;
    };
    let request = harness_contract::live::SessionMessageAckRequest {
        runtime_commit_cursor: cursor,
        content_digest: delivery
            .get("content_digest")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
    };
    delivery.get("status").and_then(Value::as_str) == Some("confirmed")
        && delivery
            .get("fresh_context_confirmed")
            .and_then(Value::as_bool)
            == Some(true)
        && delivery
            .get("client_binding_sha256")
            .and_then(Value::as_str)
            .is_some_and(valid_sha256)
        && delivery
            .get("received_at_ms")
            .and_then(Value::as_u64)
            .is_some()
        && request.validate().is_ok()
}

fn push_terminal_gate_items(
    items: &mut Vec<HarnessEvalReportGateItem>,
    scenarios: &[Value],
    prefix: &str,
) {
    let physically_settled = !scenarios.is_empty()
        && scenarios.iter().all(|scenario| {
            let identity = |field| {
                scenario
                    .get(field)
                    .and_then(Value::as_str)
                    .unwrap_or_default()
            };
            execution_observation_ready(
                scenario.get("terminal_observation").unwrap_or(&Value::Null),
                identity("execution_id"),
                identity("session_id"),
                identity("turn_id"),
            )
        });
    let delivered = !scenarios.is_empty() && scenarios.iter().all(delivery_observation_ready);
    let cleaned = !scenarios.is_empty()
        && scenarios.iter().all(|scenario| {
            scenario
                .pointer("/session_actor_cleanup/status")
                .and_then(Value::as_str)
                == Some("passed")
        });
    let business_succeeded =
        !scenarios.is_empty() && scenarios.iter().all(business_observation_ready);
    for (suffix, passed, hint) in [
        (
            "business_outcome",
            business_succeeded,
            "retain the exact original root Runtime successful outcome independently of physical settlement",
        ),
        (
            "execution_physically_settled",
            physically_settled,
            "retain exact Runtime terminal axes and complete physical coverage",
        ),
        (
            "current_public_ack",
            delivered,
            "consume the exact public payload, ACK it, and re-read its current receipt",
        ),
        (
            "actor_cleanup",
            cleaned,
            "retain the original client lease-release and detach outcome",
        ),
    ] {
        items.push(HarnessEvalReportGateItem::new(
            format!("{prefix}_{suffix}"),
            passed,
            true,
            format!("scenarios={}", scenarios.len()),
            hint,
        ));
    }
}

fn paired_report_gate(report: &Value) -> HarnessEvalReportGate {
    let pairs = report
        .get("pairs")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let mut scenarios = Vec::new();
    let mut exact_pairs = !pairs.is_empty();
    for (index, pair) in pairs.iter().enumerate() {
        exact_pairs &= pair.get("pair_index").and_then(Value::as_u64) == Some(index as u64);
        for endpoint in ["baseline", "candidate"] {
            let sample = pair.get(endpoint).unwrap_or(&Value::Null);
            exact_pairs &= sample.get("endpoint").and_then(Value::as_str) == Some(endpoint)
                && sample.get("pair_index").and_then(Value::as_u64) == Some(index as u64);
            scenarios.push(sample.clone());
        }
    }
    for endpoint in ["baseline", "candidate"] {
        let warmup = report
            .pointer(&format!("/warmup/{endpoint}"))
            .unwrap_or(&Value::Null);
        exact_pairs &= warmup.get("endpoint").and_then(Value::as_str) == Some(endpoint);
        scenarios.push(warmup.clone());
    }
    let mut observed_executions = std::collections::BTreeSet::new();
    exact_pairs &= scenarios.iter().all(|sample| {
        let endpoint = sample.get("endpoint").and_then(Value::as_str);
        let execution = sample
            .get("execution_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty());
        endpoint
            .zip(execution)
            .is_some_and(|identity| observed_executions.insert(identity))
    });
    let mut items = Vec::new();
    items.push(HarnessEvalReportGateItem::new(
        "paired_sample_identity",
        exact_pairs,
        true,
        format!("pairs={}", pairs.len()),
        "retain exact endpoint and pair indices, including warmups",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "paired_independent_business_and_surface",
        !scenarios.is_empty()
            && scenarios.iter().all(|sample| {
                sample.get("status").and_then(Value::as_str) == Some("passed")
                    && sample
                        .pointer("/acceptance/passed")
                        .and_then(Value::as_bool)
                        == Some(true)
                    && sample
                        .get("first_surface_delta_ms")
                        .and_then(Value::as_u64)
                        .is_some()
            }),
        true,
        format!("samples={}", scenarios.len()),
        "answer and observed Surface delta remain independent requirements",
    ));
    items.push(HarnessEvalReportGateItem::new(
        "paired_statistics_and_slo",
        crate::paired_performance::paired_statistics_passed(report),
        true,
        "recomputed from original retained pairs and sampling options",
        "preserve the original statistical and SLO gate",
    ));
    push_terminal_gate_items(&mut items, &scenarios, "paired");
    items.push(HarnessEvalReportGateItem::new(
        "paired_provider_usage_complete",
        canonical_total_tokens(report).is_some(),
        true,
        "actual per-attempt metrics including both endpoints and warmups",
        "missing/partial usage is unavailable, not known zero",
    ));
    push_manifest_gate_item(
        &mut items,
        report.get("evidence_manifest").unwrap_or(&Value::Null),
        true,
    );
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
    let observed_ids = live_scenarios
        .iter()
        .filter_map(|scenario| scenario.get("scenario_id").and_then(Value::as_str))
        .collect::<std::collections::BTreeSet<_>>();
    let selected_ids = live
        .get("selected_scenario_ids")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .collect::<std::collections::BTreeSet<_>>()
        });
    let exact_selection = observed_ids.len() == live_scenarios.len()
        && selected_ids.as_ref().is_none_or(|selected| {
            !selected.is_empty()
                && selected.len() == selected_scenario_count
                && selected == &observed_ids
        });
    items.push(HarnessEvalReportGateItem::new(
        "deep_selected_scenarios_exact",
        exact_selection,
        true,
        format!(
            "selected={selected_scenario_count}, observed={}",
            live_scenarios.len()
        ),
        "retain exactly the selected unique scenario identities",
    ));
    push_terminal_gate_items(items, &live_scenarios, "deep");
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
            "trace_usage_subtotal={}, live_usage_subtotal={}, usage_source={}",
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
    let expected = manifest.get("expected_candidate").unwrap_or(&Value::Null);
    let candidate_matches = complete_candidate_binding(manifest, expected);
    items.push(if real_model_claimed {
        HarnessEvalReportGateItem::new("candidate_matches_frozen_expectation", candidate_matches, true, "frozen-launcher observation and separately supplied expectation", "bind observed source/commit to the independent expected candidate; unknown cannot certify")
    } else {
        HarnessEvalReportGateItem::not_required("candidate_matches_frozen_expectation", "no real-model candidate claim", "real candidate identity is checked when that claim is made")
    });
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

fn complete_candidate_binding(observed: &Value, expected: &Value) -> bool {
    observed.get("identity_source").and_then(Value::as_str) == Some("frozen_launcher")
        && observed
            .get("execution_key")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty())
        && observed
            .get("target_repo_dirty_state")
            .and_then(Value::as_str)
            == Some("clean")
        && ["commit", "candidate_source_sha256"]
            .into_iter()
            .all(|field| {
                let actual = observed.get(field).and_then(Value::as_str);
                let expected = expected.get(field).and_then(Value::as_str);
                actual.is_some_and(|actual| {
                    if field == "commit" {
                        valid_git_commit(actual)
                    } else {
                        valid_sha256(actual)
                    }
                }) && actual == expected
            })
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
                .fold(0_u64, u64::saturating_add)
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
                .fold(0_u64, u64::saturating_add)
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod gate_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn physical_execution_observation_uses_original_axes_not_business_outcome() {
        use harness_contract::projection::{
            ExecutionTerminalAxes, TerminalEffectSettlement, TerminalExecutionPhase,
        };
        let mut axes = ExecutionTerminalAxes::default();
        axes.execution.phase = TerminalExecutionPhase::Terminal;
        axes.execution.settlement = TerminalEffectSettlement::ProvenSettled;
        axes.execution.coverage_complete = true;
        axes.execution.evidence_refs = vec!["effect:settled".to_string()];
        axes.source_cursor = 7;
        let mut observation = json!({
            "execution_id": "execution-1", "session_id": "session-1", "turn_id": "turn-1",
            "revision": 2, "cursor": 8, "terminal_axes": axes
        });
        let ready = |value: &Value| {
            execution_observation_ready(value, "execution-1", "session-1", "turn-1")
        };
        assert!(ready(&observation));
        axes.outcome.outcome = Some(harness_contract::goal::ObjectiveTerminalKind::Failed);
        observation["terminal_axes"] = serde_json::to_value(axes).expect("axes");
        assert!(ready(&observation));
        for (pointer, value) in [
            ("/terminal_axes/execution/phase", json!("draining")),
            ("/terminal_axes/execution/phase", json!("unknown")),
            ("/terminal_axes/execution/settlement", json!("unknown")),
            ("/terminal_axes/execution/coverage_complete", json!(false)),
            ("/terminal_axes/execution/evidence_refs", json!([])),
            ("/terminal_axes/source_race", json!(true)),
            ("/terminal_axes/source_cursor", json!(0)),
            ("/terminal_axes/source_cursor", json!(9)),
            ("/execution_id", json!("foreign")),
            ("/session_id", json!("foreign")),
            ("/turn_id", json!("foreign")),
            ("/revision", json!(0)),
            ("/cursor", json!(0)),
            ("/terminal_axes", Value::Null),
        ] {
            let mut invalid = observation.clone();
            *invalid.pointer_mut(pointer).expect("existing field") = value;
            assert!(!ready(&invalid), "{pointer}");
        }
        assert!(!execution_observation_ready(
            &observation,
            "",
            "session-1",
            "turn-1"
        ));
    }

    fn terminal_sample(endpoint: &str, index: u64) -> Value {
        let session = format!("session-{endpoint}-{index}");
        let execution = format!("execution-{endpoint}-{index}");
        let turn = format!("turn-{endpoint}-{index}");
        let message = format!("message-{endpoint}-{index}");
        let digest = format!("session-public-v1:sha256:{}", "a".repeat(64));
        let ack = json!({"session_id":session,"message_id":message,"runtime_commit_cursor":"9007199254740993","content_digest":digest,"client_binding_sha256":"b".repeat(64),"received_at_ms":100});
        let current = json!({"session_id":session,"message_id":message,"execution_id":execution,"turn_id":turn,"runtime_commit_cursor":"9007199254740993","content_digest":digest,"client_binding_sha256":"b".repeat(64),"receipt":ack});
        json!({
            "endpoint":endpoint,"pair_index":index,"status":"passed",
            "session_id":session,"execution_id":execution,"turn_id":turn,"terminal_id":message,
            "acceptance":{"passed":true},"first_surface_delta_ms":1000,"first_durable_assistant_ms":1100,"terminal_wall_ms":2000,
            "session_actor_cleanup":{"status":"passed"},
            "terminal_observation":{
                "execution_id":execution,"session_id":session,"turn_id":turn,"revision":2,"cursor":10,
                "terminal_axes":{
                    "outcome":{"goal_id":null,"goal_revision":null,"outcome":null,"terminal_fence":null},
                    "execution":{"phase":"terminal","settlement":"proven_settled","evidence_refs":["receipt:original"],"coverage_complete":true},
                    "delivery":{"runtime_terminal_ref":"terminal:original","session_client_evidence":"requires_fresh_context"},
                    "source_cursor":10,"source_race":false
                }
            },
            "delivery_observation":{
                "status":"confirmed","session_id":session,"message_id":message,"execution_id":execution,"turn_id":turn,
                "runtime_commit_cursor":"9007199254740993","content_digest":digest,"client_binding_sha256":"b".repeat(64),"received_at_ms":100,
                "fresh_context_confirmed":true,"ack":ack,"current_context":current
            },
            "business_observation":{"kind":"runtime.outcome.recorded.v1","execution_scope":"session","execution_id":execution,"session_id":session,"turn_id":turn,"event_status":"succeeded","terminal_class":"succeeded"},
            "usage_observation_integrity":{"timeline_drained":true,"cursor_monotonic":true,"omitted_changes":0},
            "metrics":{"provider_attempt_count":1,"provider_usage_records":1,"usage_unknown_attempts":0,"provider_attempt_cache_dimensions_unknown_count":0,"model_rounds":1,"input_tokens":1,"output_tokens":1,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"total_tokens":2}
        })
    }

    fn complete_paired_report() -> Value {
        let pairs = (0..5).map(|index| json!({"pair_index":index,"baseline":terminal_sample("baseline", index),"candidate":terminal_sample("candidate", index)})).collect::<Vec<_>>();
        json!({
            "kind":"harness_eval.paired_public_api_performance",
            "pairs":pairs,"warmup":{"baseline":terminal_sample("baseline",u64::MAX),"candidate":terminal_sample("candidate",u64::MAX)},
            "environment":{"min_pairs":5,"max_pairs":5,"target_relative_ci_half_width_bp":500,"poll_interval_ms":20},
            "evidence_manifest":{
                "kind":"harness_eval.evidence_manifest","repo":"/workspace/cowd","version":"fixture","command":"paired-performance",
                "commit":"a".repeat(40),"candidate_source_sha256":"b".repeat(64),"target_repo_dirty_state":"clean","identity_source":"frozen_launcher","execution_key":"original-run",
                "expected_candidate":{"commit":"a".repeat(40),"candidate_source_sha256":"b".repeat(64)}
            }
        })
    }

    #[test]
    fn canonical_paired_gate_keeps_business_execution_receipt_and_usage_independent() {
        let valid = complete_paired_report();
        assert_eq!(evaluate_report_gate(&valid).status, "passed");
        assert_eq!(canonical_total_tokens(&valid), Some(24));
        let mut optional_outbox = valid.clone();
        optional_outbox["pairs"][0]["candidate"]["delivery_observation"]["execution_id"] =
            Value::Null;
        optional_outbox["pairs"][0]["candidate"]["delivery_observation"]["current_context"]
            ["execution_id"] = Value::Null;
        assert_eq!(evaluate_report_gate(&optional_outbox).status, "passed");
        let mut reviewed = valid.clone();
        reviewed["execution_trace"] = json!({"provider_rounds":1,"rounds":[{"usage":{"total_tokens":50,"usage_source":"provider_event"}}],"total_usage":{"total_tokens":50,"usage_source":"provider_event+partial"}});
        assert_eq!(canonical_total_tokens(&reviewed), None);
        assert_eq!(
            gate_item(
                &evaluate_report_gate(&reviewed),
                "paired_provider_usage_complete"
            )
            .status,
            "failed"
        );
        for (pointer, replacement, item) in [
            (
                "/pairs/0/candidate/terminal_observation/terminal_axes/execution/phase",
                json!("draining"),
                "paired_execution_physically_settled",
            ),
            (
                "/pairs/0/candidate/acceptance/passed",
                json!(false),
                "paired_independent_business_and_surface",
            ),
            (
                "/pairs/0/candidate/business_observation/terminal_class",
                json!("failed"),
                "paired_business_outcome",
            ),
            (
                "/pairs/0/candidate/business_observation/execution_scope",
                json!("agent"),
                "paired_business_outcome",
            ),
            (
                "/pairs/0/candidate/delivery_observation/runtime_commit_cursor",
                json!("1"),
                "paired_current_public_ack",
            ),
            (
                "/pairs/0/candidate/delivery_observation/current_context/receipt",
                Value::Null,
                "paired_current_public_ack",
            ),
            (
                "/pairs/0/candidate/metrics/output_tokens",
                Value::Null,
                "paired_provider_usage_complete",
            ),
            (
                "/pairs/0/candidate/metrics/cache_read_input_tokens",
                Value::Null,
                "paired_provider_usage_complete",
            ),
            (
                "/pairs/0/candidate/usage_observation_integrity/timeline_drained",
                json!(false),
                "paired_provider_usage_complete",
            ),
            (
                "/evidence_manifest/commit",
                json!("c".repeat(40)),
                "candidate_matches_frozen_expectation",
            ),
            (
                "/evidence_manifest/identity_source",
                json!("unattested_library"),
                "candidate_matches_frozen_expectation",
            ),
        ] {
            let mut invalid = valid.clone();
            *invalid.pointer_mut(pointer).unwrap() = replacement;
            let gate = evaluate_report_gate(&invalid);
            assert_eq!(gate.status, "failed", "{pointer}");
            assert_eq!(gate_item(&gate, item).status, "failed", "{pointer}");
            if matches!(
                item,
                "paired_independent_business_and_surface" | "paired_business_outcome"
            ) {
                assert_eq!(
                    gate_item(&gate, "paired_execution_physically_settled").status,
                    "passed"
                );
            }
        }
        let mut replayed = valid.clone();
        replayed["pairs"][1]["candidate"] = valid["pairs"][0]["candidate"].clone();
        replayed["pairs"][1]["candidate"]["pair_index"] = json!(1);
        assert_eq!(
            gate_item(&evaluate_report_gate(&replayed), "paired_sample_identity").status,
            "failed"
        );
        let mut empty = valid.clone();
        empty["pairs"] = json!([]);
        assert_eq!(evaluate_report_gate(&empty).status, "failed");
    }

    #[test]
    fn focused_selection_rejects_equal_count_foreign_and_duplicate_scenarios() {
        let mut scenario = terminal_sample("candidate", 0);
        scenario["scenario_id"] = json!("observed");
        let live = json!({"selected_scenario_ids":["expected"],"scenarios":[scenario]});
        let mut items = Vec::new();
        push_deep_live_gate_item(&mut items, "deep", &live);
        assert_eq!(
            items
                .iter()
                .find(|item| item.name == "deep_selected_scenarios_exact")
                .unwrap()
                .status,
            "failed"
        );
        let mut duplicate = live.clone();
        duplicate["selected_scenario_ids"] = json!(["observed", "observed"]);
        duplicate["scenarios"] = json!([live["scenarios"][0], live["scenarios"][0]]);
        let mut items = Vec::new();
        push_deep_live_gate_item(&mut items, "deep", &duplicate);
        assert_eq!(
            items
                .iter()
                .find(|item| item.name == "deep_selected_scenarios_exact")
                .unwrap()
                .status,
            "failed"
        );
    }

    fn complete_usage_report(total: u32) -> Value {
        json!({
            "level": "deep", "authorized_real_model": true,
            "execution_trace": {
                "provider_rounds": 1, "rounds": [],
                "total_usage": {"total_tokens": total, "usage_source": "live_gateway_canonical_usage"},
                "provider_cache": {"physical_provider_attempts": 1, "usage_unknown_attempts": 0}
            },
            "live_gateway_scenarios": {"scenarios": [{"observation_integrity":{"timeline_drained":true,"cursor_monotonic":true,"omitted_changes":0},"metrics": {
                "provider_attempt_count": 1, "provider_usage_records": 1,
                "usage_unknown_attempts": 0, "model_rounds": 1,
                "provider_attempt_cache_dimensions_unknown_count":0,
                "cache_creation_input_tokens":0,"cache_read_input_tokens":0,
                "input_tokens": total, "output_tokens": 0, "total_tokens": total
            }}]}
        })
    }

    #[test]
    fn canonical_read_total_distinguishes_known_zero_missing_partial_and_overflow() {
        let summary = |report: &Value| {
            HarnessEvalReportSummary::from_report_json(
                "report".to_string(),
                "report.json".to_string(),
                None,
                report,
            )
        };
        for total in [0, 17, u32::MAX] {
            let report = complete_usage_report(total);
            assert_eq!(summary(&report).total_tokens, Some(total));
            assert_eq!(
                gate_item(
                    &evaluate_report_gate(&report),
                    "real_provider_usage_complete"
                )
                .status,
                "passed"
            );
        }
        let known = complete_usage_report(17);
        for value in [
            Value::Null,
            json!(4294967296_u64),
            json!(-1),
            json!(1.5),
            json!("17"),
        ] {
            let mut report = known.clone();
            report["execution_trace"]["total_usage"]["total_tokens"] = value;
            assert_eq!(summary(&report).total_tokens, None);
        }
        let mut missing = known.clone();
        missing["execution_trace"]["total_usage"]
            .as_object_mut()
            .expect("usage")
            .remove("total_tokens");
        assert_eq!(summary(&missing).total_tokens, None);
        let mut reviewer_without_attempt_coverage = known.clone();
        reviewer_without_attempt_coverage["execution_trace"]["provider_rounds"] = json!(2);
        reviewer_without_attempt_coverage["execution_trace"]["provider_cache"]
            ["physical_provider_attempts"] = json!(2);
        reviewer_without_attempt_coverage["execution_trace"]["total_usage"] = json!({"total_tokens": 67, "usage_source": "live_gateway_canonical_usage+provider_event"});
        reviewer_without_attempt_coverage["execution_trace"]["rounds"] = json!([{"usage": {"input_tokens": 40, "output_tokens": 10, "total_tokens": 50, "usage_source": "provider_event"}}]);
        assert_eq!(
            summary(&reviewer_without_attempt_coverage).total_tokens,
            None
        );
        for (pointer, value) in [
            (
                "/live_gateway_scenarios/scenarios/0/metrics/provider_attempt_count",
                json!(2),
            ),
            (
                "/live_gateway_scenarios/scenarios/0/metrics/provider_usage_records",
                Value::Null,
            ),
            (
                "/live_gateway_scenarios/scenarios/0/metrics/usage_unknown_attempts",
                json!(1),
            ),
            (
                "/live_gateway_scenarios/scenarios/0/metrics/output_tokens",
                Value::Null,
            ),
            (
                "/live_gateway_scenarios/scenarios/0/metrics/total_tokens",
                json!(4294967296_u64),
            ),
            (
                "/execution_trace/total_usage/usage_source",
                json!("deterministic_tool_estimate+provider_event"),
            ),
            (
                "/execution_trace/provider_cache/physical_provider_attempts",
                json!(2),
            ),
        ] {
            let mut report = known.clone();
            *report.pointer_mut(pointer).expect("existing field") = value;
            assert_eq!(summary(&report).total_tokens, None, "{pointer}");
            assert_eq!(
                gate_item(
                    &evaluate_report_gate(&report),
                    "real_provider_usage_complete"
                )
                .status,
                "failed",
                "{pointer}"
            );
        }
        let mut partial = known;
        partial["live_gateway_scenarios"]["scenarios"][0]["metrics"]["provider_attempt_count"] =
            json!(2);
        partial["live_gateway_scenarios"]["scenarios"][0]["metrics"]["usage_unknown_attempts"] =
            json!(1);
        partial["execution_trace"]["provider_rounds"] = json!(2);
        partial["execution_trace"]["provider_cache"]["physical_provider_attempts"] = json!(3);
        partial["execution_trace"]["total_usage"] = json!({"total_tokens": 67, "usage_source": "live_gateway_canonical_usage+provider_event"});
        partial["execution_trace"]["rounds"] = json!([{"usage": {"input_tokens": 40, "output_tokens": 10, "total_tokens": 50, "usage_source": "provider_event"}}]);
        assert_eq!(summary(&partial).total_tokens, None);
        assert_eq!(
            gate_item(
                &evaluate_report_gate(&partial),
                "real_provider_usage_complete"
            )
            .status,
            "failed"
        );
    }

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
                "version": env!("CARGO_PKG_VERSION"),
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
                    "primary_artifact_ref": "artifact-test",
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

    /// Mon9's real projection shape, as a fixture. Every field name comes from
    /// the projection structs: agents carry no team_id (membership lives in the
    /// team's member_ids), withdrawn tasks are empty shells, and the root
    /// publishes topic entries whose references sit in issue_dispositions.
    fn mon9_shaped_projection() -> Value {
        let mut agents = serde_json::Map::new();
        for index in 0..8 {
            let agent_id = format!("agent:{index:04}");
            agents.insert(
                agent_id.clone(),
                json!({
                    "agent_id": agent_id,
                    "display_name": format!("Worker {index}"),
                    "role": "Executor",
                    "mission": "produce one verifiable fact",
                    "required_capabilities": ["read"],
                    "invited_by": "root",
                    "membership_ids": ["membership:1"]
                }),
            );
        }
        let mut tasks = serde_json::Map::new();
        for index in 0..4 {
            let task_id = format!("task:accepted-{index}");
            tasks.insert(
                task_id.clone(),
                json!({
                    "task_id": task_id,
                    "team_id": "team:1",
                    "status": "accepted",
                    "claimant": "agent:0000",
                    "reviewed_by": "agent:0001",
                    "artifact_refs": [format!("artifact:{index}")],
                    "evidence_refs": [format!("evidence:{index}")],
                    "depends_on": []
                }),
            );
        }
        for index in 0..2 {
            let task_id = format!("task:withdrawn-{index}");
            tasks.insert(
                task_id.clone(),
                json!({
                    "task_id": task_id,
                    "team_id": "team:1",
                    "status": "withdrawn",
                    "claimant": null,
                    "reviewed_by": null,
                    "artifact_refs": [],
                    "evidence_refs": [],
                    "depends_on": []
                }),
            );
        }
        json!({
            "program_id": "program:mon9",
            "objective_id": "objective:mon9",
            "session_id": "session:mon9",
            "turn_id": "turn:mon9",
            "root_execution_id": "root:mon9",
            "required_team_count": 1,
            "status": "verified",
            "teams": {
                "team:1": {
                    "team_id": "team:1",
                    "name": "Verification",
                    "topic_ref": "topic:1",
                    "member_ids": (0..8).map(|i| format!("agent:{i:04}")).collect::<Vec<_>>(),
                    "task_ids": (0..4).map(|i| format!("task:accepted-{i}"))
                        .chain((0..2).map(|i| format!("task:withdrawn-{i}")))
                        .collect::<Vec<_>>()
                }
            },
            "agents": agents,
            "tasks": tasks,
            "artifacts": {
                "artifact:0": {"artifact_ref": "artifact:0", "content_ref": "content:0", "committed_by": "agent:0000"},
                "artifact:1": {"artifact_ref": "artifact:1", "content_ref": "content:1", "committed_by": "agent:0000"},
                "artifact:2": {"artifact_ref": "artifact:2", "content_ref": "content:2", "committed_by": "agent:0000"},
                "artifact:3": {"artifact_ref": "artifact:3", "content_ref": "content:3", "committed_by": "agent:0000"}
            },
            "topics": {
                "topic:1": [
                    {
                        "entry_id": "message:agent-entry",
                        "actor_id": "agent:0002",
                        "refs": [],
                        "recipients": ["agent:0003"]
                    },
                    {
                        "entry_id": "message:root-entry",
                        "actor_id": "root:mon9",
                        "refs": [],
                        "issue_dispositions": [{
                            "disposition": "disclose",
                            "evidence_refs": ["task:accepted-0"]
                        }]
                    }
                ]
            },
            "final_artifact_ref": "artifact:0",
            "completion_request": {
                "action_id": "complete:1",
                "requested_by": "root:mon9",
                "program_revision": 2,
                "primary_artifact_ref": "artifact:0",
                "evidence_refs": ["evidence:final"]
            },
            "objective_verdict": {
                "kind": "satisfied",
                "terminal_fence": "fence:1",
                "authority_revision": 78
            },
            "unresolved": []
        })
    }

    #[test]
    fn a_real_program_shape_passes_the_closure_gate() {
        let result = evaluate_agentic_program_closure(&mon9_shaped_projection());
        assert_eq!(result["status"], "passed", "{}", result["missing"]);
        assert_eq!(result["facts"]["agents"], 8);
        assert_eq!(result["facts"]["active_tasks"], 6);
    }

    #[test]
    fn a_withdrawn_task_is_not_a_defect_but_an_unsettled_one_is() {
        let mut withdrawn = mon9_shaped_projection();
        withdrawn["tasks"]["task:withdrawn-0"]["status"] = json!("claimed");
        let result = evaluate_agentic_program_closure(&withdrawn);
        assert_eq!(result["status"], "failed");
        assert!(result["missing"]
            .as_array()
            .expect("missing checks")
            .iter()
            .any(|item| item == "tasks_accepted_and_independently_verified"));
    }

    #[test]
    fn re_evaluating_a_closure_keeps_the_facts_the_gate_evidence_reads() {
        // The gate evaluates `report.agentic_program_closure`, which `runner`
        // has already evaluated from the raw Program projection. Without
        // idempotence the second pass read the projection's `teams` / `agents` /
        // `tasks` maps out of a value that only carries `checks` / `facts` /
        // `status`, so every field came back empty and the gate evidence read
        // `program=missing, teams=0, agents=0, tasks=0` while the report body
        // carried teams=1, agents=2, tasks=1. The verdict agreed by luck, which
        // is why nothing caught it: a wrong reason and a right verdict look
        // identical from outside.
        let projection = verified_agentic_program_closure();
        let once = evaluate_agentic_program_closure(&projection);
        let twice = evaluate_agentic_program_closure(&once);
        assert_eq!(once["facts"], twice["facts"]);
        assert_eq!(once["status"], twice["status"]);
        assert_eq!(twice["facts"]["teams"], 1);
        assert_eq!(twice["facts"]["agents"], 2);
        assert_eq!(twice["facts"]["active_tasks"], 1);
        // The evidence string the gate prints must name the Program the report
        // actually holds, not an empty one.
        let evidence = agentic_closure_gate_evidence(&twice);
        assert!(
            !evidence.contains("program=missing"),
            "gate evidence must not claim the Program is absent: {evidence}"
        );
        assert!(
            evidence.contains("teams=1") && evidence.contains("agents=2"),
            "gate evidence must be derivable from the report body: {evidence}"
        );
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

    /// The one gate added for the tool/harness layer: the report and the evidence must
    /// The gate added for the tool/harness layer: the report must not report fewer tool
    /// calls than its own durable record.
    ///
    /// The contradiction it asserts was observed, not imagined - one live run reported 3
    /// tool calls while its own evidence held 95 terminal tool events and the durable store
    /// agreed with 95.
    ///
    /// The assertion is one-directional because the two numbers are not the same lane: the
    /// headline is the harness's own local tool-batch count (`runner.rs:155`), the evidence is
    /// the durable event stream including delegated Agents' governed actions. Equality would
    /// fail every multi-agent run. A level, or a scenario, that records no tool lane says
    /// `not_required` instead of passing on an empty set.
    #[test]
    fn the_report_must_not_understate_its_own_durable_record() {
        let evidence = json!([
            {"payload": {"tool_name": "task_review", "status": "failed"}},
            {"payload": {"tool_name": "task_review", "status": "running"}},
            {"payload": {"tool_name": "artifact_publish", "status": "refused"}}
        ]);
        let gate_name = "report_tool_counts_do_not_understate_the_evidence";

        // A refusal is recorded but not counted: it is not an execution attempt. The record
        // and the count are different things, and the gate must not confuse them.
        assert_eq!(
            terminal_tool_events_in_events(evidence.as_array().expect("evidence is an array")),
            1,
            "the refusal is in the record but does not change the count"
        );

        // The observed defect shape: the report reads fewer than its own record.
        let understated = json!({
            "level": "deep",
            "next_gen_scenarios": [{ "claims_tool_validation": true }],
            "execution_trace": {
                "tool_calls": 0,
                "total_usage": {"total_tokens": 1, "usage_source": "real_provider"}
            },
            "live_gateway_scenarios": {"scenarios": [{
                "scenario_id": "live_one_team_execution",
                "metrics": {"tool_calls": 0},
                "failure_diagnostics": {"timeline": {"events": evidence}}
            }]}
        });
        let gate = evaluate_report_gate(&understated);
        let item = gate_item(&gate, gate_name);
        assert_eq!(
            item.status, "failed",
            "reported 0 against evidence 1: {item:?}"
        );

        // A larger reported number is not a defect. The headline counts the harness's own
        // captured calls; the evidence counts the durable event stream, which includes
        // delegated Agents' governed actions. Equality would fail every multi-agent run.
        let agrees = json!({
            "level": "deep",
            "next_gen_scenarios": [{ "claims_tool_validation": true }],
            "execution_trace": {
                "tool_calls": 3,
                "total_usage": {"total_tokens": 1, "usage_source": "real_provider"}
            },
            "live_gateway_scenarios": {"scenarios": [{
                "scenario_id": "live_one_team_execution",
                "metrics": {"tool_calls": 3},
                "failure_diagnostics": {"timeline": {"events": evidence}}
            }]}
        });
        assert_eq!(
            gate_item(&evaluate_report_gate(&agrees), gate_name).status,
            "passed",
            "3 >= 1 is a scope difference, not a shortfall"
        );

        // A scenario with no measured number takes its count from its own record, so the
        // number the report presents for it cannot be below the record.
        let no_terminal_state = json!({
            "level": "deep",
            "execution_trace": {
                "tool_calls": 0,
                "total_usage": {"total_tokens": 1, "usage_source": "real_provider"}
            },
            "live_gateway_scenarios": {"scenarios": [{
                "scenario_id": "live_one_team_execution",
                "failure_diagnostics": {"timeline": {"events": evidence}}
            }]}
        });
        assert_eq!(
            gate_item(&evaluate_report_gate(&no_terminal_state), gate_name).status,
            "passed",
            "the number presented for this scenario is the record itself"
        );

        // The claim gate reads the same rule, so it cannot pass on a headline counter that
        // the evidence contradicts.
        assert_eq!(
            gate_item(&gate, "claimed_tool_validation_has_tool_evidence").status,
            "passed",
            "the evidence holds the calls even when the headline counter reads zero"
        );

        let quick = json!({
            "level": "quick",
            "execution_trace": {
                "tool_calls": 0,
                "total_usage": {"total_tokens": 0, "usage_source": "deterministic_smoke"}
            },
            "live_gateway_scenarios": {"scenarios": []}
        });
        assert_eq!(
            gate_item(&evaluate_report_gate(&quick), gate_name).status,
            "not_required",
            "a level with no tool lane must say so, not pass on an empty set"
        );
    }

    #[test]
    fn deep_gate_accepts_a_complete_explicitly_selected_live_scenario() {
        let mut scenario = terminal_sample("candidate", 0);
        scenario["scenario_id"] = json!("live_group_theory_ai_research_simulation");
        scenario["production_trace"] = json!({"session_id":scenario["session_id"],"terminal_id":scenario["terminal_id"],"execution_id":scenario["execution_id"]});
        scenario["observation_integrity"] = json!({"status":"passed","cursor_monotonic":true,"timeline_drained":true,"message_pages_drained":true,"omitted_changes":0,"stall_detected":false});
        let report = json!({
            "level": "deep",
            "live_gateway_scenarios": {
                "status": "passed",
                "selected_scenario_ids": ["live_group_theory_ai_research_simulation"],
                "scenarios": [scenario]
            }
        });

        let gate = evaluate_report_gate(&report);
        let item = gate
            .items
            .iter()
            .find(|item| item.name == "deep_live_gateway_scenarios")
            .expect("gate item");
        assert_eq!(item.status, "passed");
        for name in [
            "deep_selected_scenarios_exact",
            "deep_execution_physically_settled",
            "deep_business_outcome",
            "deep_current_public_ack",
            "deep_actor_cleanup",
        ] {
            assert_eq!(gate_item(&gate, name).status, "passed", "{name}");
        }
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
        let overflow = [
            self.total_usage
                .input_tokens
                .checked_add(summary.usage.input_tokens),
            self.total_usage
                .output_tokens
                .checked_add(summary.usage.output_tokens),
            self.total_usage
                .cache_creation_input_tokens
                .checked_add(summary.usage.cache_creation_input_tokens),
            self.total_usage
                .cache_read_input_tokens
                .checked_add(summary.usage.cache_read_input_tokens),
            self.total_usage
                .total_tokens
                .checked_add(summary.usage.total_tokens),
        ]
        .iter()
        .any(Option::is_none);
        self.total_usage.usage_source = if self.provider_rounds == 0 {
            summary.usage.usage_source.clone()
        } else {
            format!(
                "{}+{}",
                self.total_usage.usage_source, summary.usage.usage_source
            )
        };
        if overflow {
            self.total_usage.usage_source.push_str("+overflow");
        }
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
            total_tokens: canonical_total_tokens(report),
            scenario_count: scenarios.len(),
            failed_capabilities: scenarios
                .iter()
                .filter(|item| {
                    item.get("status").and_then(Value::as_str) != Some("passed")
                        && !(report["kind"] == "harness_eval.u5_internal"
                            && item["status"] == "pending_outer_cleanup")
                })
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

#[cfg(test)]
pub(crate) mod u5_stage_tests {
    use super::*;
    fn leaf(id: &str, input: u64, output: u64) -> Value {
        json!({"session_id":format!("session-{id}"),"turn_id":format!("turn-{id}"),"execution_id":format!("root-{id}"),
            "status":"passed","metrics":{"tool_calls":1},"acceptance":{"passed":true,"checks":[{"name":"original_read_and_ACK","passed":true}]},"actor_cleanup":{"passed":true},
            "u5_provider_events":[{"kind":"context.provider_request_packed","payload":{"request_id":id}},
            {"kind":"context.provider_attempt_outcome","payload":{"request_id":id,"usage_status":"known","cache_dimensions_status":"known","usage":{"input_tokens":input,"output_tokens":output,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}}}]})
    }
    pub(crate) fn original_stage() -> Value {
        json!({"kind":"harness_eval.u5_internal","level":"deep","authorized_real_model":true,
            "u5_host":{"nonce":"a".repeat(64),"candidate":"b".repeat(40)},
            "scenarios":[{"capability":"live_u5_shared_session_fairness","status":"pending_outer_cleanup"}],
            "execution_trace":{"provider_rounds":2,"tool_calls":2,"total_usage":{"input_tokens":4,"output_tokens":6,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"total_tokens":10,"usage_source":"live_gateway_canonical_usage"}},
            "u5_usage":{"total_tokens":10,"physical_calls":2,"source":"original_provider_call_id_deduplicated","input_tokens":4,"output_tokens":6,"cache_creation_input_tokens":0,"cache_read_input_tokens":0},
            "live_gateway_scenarios":{"status":"pending_outer_cleanup","health_status":"passed","selection_status":"passed","claim_scope":"focused","selected_scenario_ids":["live_u5_shared_session_fairness"],
            "scenarios":[{"scenario_id":"live_u5_shared_session_fairness","status":"pending_outer_cleanup",
                "acceptance":{"checks":{"internal_passed":true,"identity":true,"selected_witnesses_complete":true,"left_business":true,"right_business":true,"capacity_competition":true}},
                "sessions":[leaf("a",1,2),leaf("b",3,4)]}]}})
    }
    #[test]
    fn u5_pending_requires_original_two_leaf_hardgates_and_typed_usage() {
        let good = original_stage();
        let gate = evaluate_report_gate(&good);
        assert_eq!(gate.status, "pending_outer_cleanup");
        assert_eq!(gate.failed, 0);
        for (path,value) in [
            ("/level",json!("quick")),("/authorized_real_model",json!(false)),
            ("/live_gateway_scenarios/selected_scenario_ids",json!(["live_u5_shared_session_fairness","live_tool_evidence"])),
            ("/live_gateway_scenarios/health_status",json!("failed")),
            ("/live_gateway_scenarios/scenarios/0/sessions/0/status",json!("cancelled")),
            ("/live_gateway_scenarios/scenarios/0/sessions/0/actor_cleanup/passed",json!(false)),
            ("/live_gateway_scenarios/scenarios/0/sessions/0/acceptance/checks/0/passed",json!(false)),
            ("/live_gateway_scenarios/scenarios/0/sessions/0/turn_id",json!("turn-b")),
            ("/live_gateway_scenarios/scenarios/0/acceptance/checks/capacity_competition",json!(false)),
            ("/live_gateway_scenarios/scenarios/0/sessions/0/u5_provider_events/1/payload/usage/input_tokens",Value::Null),
            ("/u5_usage/input_tokens",json!(0)),
        ] {
            let mut bad=good.clone();*bad.pointer_mut(path).expect("independent fixture field")=value;
            assert_eq!(evaluate_report_gate(&bad).status,"failed","{path}");
        }
        let mut ordinary = good;
        ordinary["kind"] = json!("ordinary");
        ordinary["status"] = json!("pending_outer_cleanup");
        assert_ne!(
            evaluate_report_gate(&ordinary).status,
            "pending_outer_cleanup"
        );
    }
    #[test]
    fn u5_call_identity_dedup_rejects_unknown_conflicts_and_preserves_unknown_cache() {
        let first = leaf("a", 1, 2);
        let second = leaf("b", 3, 4);
        let expected = json!({"total_tokens":10,"physical_calls":2,"source":"original_provider_call_id_deduplicated","input_tokens":4,"output_tokens":6,"cache_creation_input_tokens":0,"cache_read_input_tokens":0});
        assert_eq!(
            u5_stage_usage(&[first.clone(), second.clone()]).unwrap(),
            expected
        );
        assert_eq!(
            u5_stage_usage(&[first.clone(), second, first.clone()]).unwrap(),
            expected
        );
        for value in [Value::Null, json!("1"), json!(-1)] {
            let mut bad = first.clone();
            bad["u5_provider_events"][1]["payload"]["usage"]["input_tokens"] = value;
            assert!(u5_stage_usage(&[bad]).is_err());
        }
        let mut missing = first.clone();
        missing["u5_provider_events"].as_array_mut().unwrap().pop();
        assert!(u5_stage_usage(&[missing]).is_err());
        let mut conflict = first.clone();
        conflict["u5_provider_events"][1]["payload"]["usage"]["output_tokens"] = json!(99);
        assert!(u5_stage_usage(&[first, conflict]).is_err());
        let mut unknown = leaf("unknown", 1, 2);
        unknown["u5_provider_events"][1]["payload"]["cache_dimensions_status"] = json!("unknown");
        let partial = u5_stage_usage(&[unknown]).unwrap();
        assert_eq!(partial["cache_read_input_tokens"], Value::Null);
        assert_eq!(partial["total_tokens"], Value::Null);
        let zero = leaf("zero", 0, 0);
        assert_eq!(u5_stage_usage(&[zero]).unwrap()["input_tokens"], 0);
    }
}
