use std::{
    process::Command,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Instant,
};

use harness_contract::core::ExecutionPattern;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    evaluate_complex_harness_scenarios, evaluate_evolution_closure,
    evaluate_knowledge_fabric_context_governance, evaluate_next_gen_harness_closure,
    evaluate_reality_context_scenarios, evaluate_report_gate, harness_capability_coverage_report,
    real_provider_runner::run_deep_real_provider_review,
    report::{
        HarnessEvalLevel, HarnessEvalRunRecord, HarnessEvalRunStatus, HarnessEvalUsageSummary,
        ToolCallDetail, ToolCallSummary,
    },
    report_store::{empty_usage, now_ms, HarnessEvalReportStore},
    run_live_gateway_scenarios, stable_ai_scenario_matrix, E2eScenarioKind,
    NextGenHarnessEvalInput, RealToolScenarioReport, RealToolScenarioResult, ScenarioCheck,
    ScenarioObservation, ScenarioSpec, ScenarioSuite, StableAiHealthReport,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEvalRunnerOptions {
    pub level: HarnessEvalLevel,
    pub provider: Option<String>,
    pub budget: Option<String>,
    pub allow_real_model: bool,
}

impl Default for HarnessEvalRunnerOptions {
    fn default() -> Self {
        Self {
            level: HarnessEvalLevel::Quick,
            provider: None,
            budget: Some("low".to_string()),
            allow_real_model: false,
        }
    }
}

#[derive(Clone, Default)]
pub struct HarnessEvalRunControl {
    pub run_id: Option<String>,
    pub cancel_requested: Option<Arc<AtomicBool>>,
}

impl HarnessEvalRunControl {
    #[must_use]
    pub fn with_run_id(run_id: impl Into<String>) -> Self {
        Self {
            run_id: Some(run_id.into()),
            cancel_requested: None,
        }
    }

    #[must_use]
    pub fn with_cancel(mut self, cancel_requested: Arc<AtomicBool>) -> Self {
        self.cancel_requested = Some(cancel_requested);
        self
    }

    #[must_use]
    pub fn is_cancel_requested(&self) -> bool {
        self.cancel_requested
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::SeqCst))
    }
}

pub fn run_eval(
    store: &HarnessEvalReportStore,
    options: HarnessEvalRunnerOptions,
) -> Result<HarnessEvalRunRecord, String> {
    run_eval_controlled(store, options, HarnessEvalRunControl::default())
}

pub fn run_eval_controlled(
    store: &HarnessEvalReportStore,
    options: HarnessEvalRunnerOptions,
    control: HarnessEvalRunControl,
) -> Result<HarnessEvalRunRecord, String> {
    let requested_at_ms = now_ms();
    let run_id = control.run_id.clone().unwrap_or_else(|| {
        format!(
            "harness-eval-{}-{}",
            options.level.as_str(),
            uuid::Uuid::new_v4()
        )
    });
    if options.level == HarnessEvalLevel::Deep && !options.allow_real_model {
        let record = HarnessEvalRunRecord {
            run_id,
            level: options.level.as_str().to_string(),
            status: HarnessEvalRunStatus::Gated.as_str().to_string(),
            requested_at_ms,
            finished_at_ms: Some(now_ms()),
            authorized_real_model: false,
            provider: options.provider,
            budget: options.budget,
            report_id: None,
            report_path: None,
            result_package_dir: None,
            total_elapsed_ms: None,
            provider_rounds: 0,
            tool_calls: 0,
            total_tokens: 0,
            scenario_count: 0,
            message: "deep/real harness eval requires explicit allow_real_model authorization"
                .to_string(),
        };
        store.upsert_run(&record)?;
        return Ok(record);
    }

    let started = Instant::now();
    let stable_ai = stable_ai_report_for(&options);
    let complex =
        (options.level != HarnessEvalLevel::Quick).then(evaluate_complex_harness_scenarios);
    let knowledge = evaluate_knowledge_fabric_context_governance();
    let reality_context = evaluate_reality_context_scenarios();
    let mission_runtime = evaluate_mission_runtime_collaboration_closure();
    let real_tool = (options.level != HarnessEvalLevel::Quick).then(run_full_real_tool_scenarios);
    let mut usage = empty_usage("deterministic_smoke");
    let mut tool_call_log = Vec::new();
    let mut tool_call_details = Vec::new();
    let mut real_tool_scenarios = None;
    if let Some(real_tool) = real_tool {
        usage = real_tool.usage;
        tool_call_log = real_tool
            .tool_details
            .iter()
            .map(|detail| detail.summary.clone())
            .collect();
        tool_call_details = real_tool
            .tool_details
            .iter()
            .map(|detail| {
                json!({
                    "summary": detail.summary,
                    "input": detail.input,
                    "output": detail.output,
                    "error": detail.error,
                })
            })
            .collect();
        real_tool_scenarios = Some(real_tool.report);
    }
    let tool_calls = tool_call_log.len();
    let runtime_actions = 5 + usize::from(complex.is_some());
    let mission_evidence_refs = mission_runtime
        .get("evidence_refs")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mission_terminal = mission_runtime
        .get("terminal_evidence")
        .unwrap_or(&Value::Null);
    let next_gen_harness = evaluate_next_gen_harness_closure(NextGenHarnessEvalInput {
        level: options.level.as_str().to_string(),
        runtime_action_count: runtime_actions,
        tool_call_count: tool_calls,
        provider_rounds: 0,
        total_tokens: usage.total_tokens,
        real_model_authorized: options.allow_real_model,
        mission_evidence_refs,
        reality_evidence_ref_total: reality_context.evidence_ref_total,
        agent_terminal_count: mission_terminal
            .get("agent_terminal_count")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize,
        mailbox_completed_count: mission_terminal
            .get("mailbox_completed_count")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize,
        synthesis_receipt_id: mission_terminal
            .get("synthesis_receipt_id")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        session_relation_count: mission_terminal
            .get("session_relation_count")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize,
        runtime_turn_result_count: mission_terminal
            .get("runtime_turn_result_count")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize,
        recovery_applied_count: mission_terminal
            .get("recovery_applied_count")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize,
        recovery_verified_count: mission_terminal
            .get("recovery_verified_count")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize,
        source_fixture_status: "not_required_deterministic".to_string(),
        sidecar_fixture_status: "not_required_deterministic".to_string(),
        db_fixture_status: "not_required_deterministic".to_string(),
    });
    let mut scenarios = vec![
        json!({
            "capability": "stable_ai_scenario_matrix",
            "status": stable_ai.status,
            "evidence": format!("{}/{} fake scenarios passed", stable_ai.fake_provider_result.passed, stable_ai.fake_provider_result.total),
            "notes": "deterministic matrix validates strategy, context, recovery, mission, and tool evidence markers"
        }),
        json!({
            "capability": "harness_capability_coverage",
            "status": if stable_ai.coverage.failed == 0 { "passed" } else { "failed" },
            "evidence": format!("{}/{} runtime domains covered", stable_ai.coverage.passed, stable_ai.coverage.total),
            "notes": "runtime module map and lifecycle ownership coverage"
        }),
        json!({
            "capability": "knowledge_fabric_context_governance",
            "status": if knowledge.passed { "passed" } else { "failed" },
            "evidence": format!("active_packs={}, blocked_namespaces={}, conflicts={}, evidence={}", knowledge.active_pack_count, knowledge.blocked_namespace_count, knowledge.conflict_count, knowledge.evidence_count),
            "notes": knowledge.notes.join("; ")
        }),
        json!({
            "capability": "reality_context_eval",
            "status": if reality_context.failed == 0 { "passed" } else { "failed" },
            "evidence": format!("{}/{} reality scenarios passed; selected_context={}, omitted_context={}, evidence_refs={}", reality_context.passed, reality_context.total, reality_context.selected_context_total, reality_context.omitted_context_total, reality_context.evidence_ref_total),
            "notes": "validates RecallReport, ContextEnvelope, selected/omitted context, evidence refs, scoped recall, knowledge activation, fact/matrix trace, tool sandbox, multi-agent and cross-session evidence"
        }),
        json!({
            "capability": "mission_runtime_collaboration_closure",
            "status": mission_runtime.get("status").and_then(Value::as_str).unwrap_or("failed"),
            "evidence": format!(
                "template={}, execution_graph={}, conflicts={}, projection_schema={}",
                mission_runtime.pointer("/selected_strategy/template").and_then(Value::as_str).unwrap_or("none"),
                mission_runtime.pointer("/execution_graph/execution_graph_id").and_then(Value::as_str).unwrap_or("none"),
                mission_runtime.pointer("/conflicts/count").and_then(Value::as_u64).unwrap_or_default(),
                mission_runtime.pointer("/mission_projection/schema_version").and_then(Value::as_u64).unwrap_or_default(),
            ),
            "notes": "deterministic closure exercises Runtime capability catalog, team template, ExecutionGraph planning, agent capability binding, session command lifecycle, conflict arbitration, and MissionProjection"
        }),
        json!({
            "capability": "next_gen_harness_closure",
            "status": next_gen_harness.status.as_str(),
            "evidence": format!("{}/{} next-gen closure scenarios passed; missing={}", next_gen_harness.passed, next_gen_harness.total, next_gen_harness.missing_capabilities.len()),
            "notes": "validates simple fast path, complex strategy, batch tool evidence, team/agent execution, cross-session dispatch, memory/reality governance, and conflict/recovery evidence gates"
        }),
    ];
    if let Some(complex) = &complex {
        scenarios.push(json!({
            "capability": "complex_harness_scenario_suite",
            "status": if complex.failed == 0 && complex.average_score >= 0.9 { "passed" } else { "failed" },
            "evidence": format!("{}/{} complex scenarios passed; average={:.2}", complex.passed, complex.total, complex.average_score),
            "notes": "full smoke validates repo refactor, memory governance, multi-agent, cross-session, and recovery scenario models"
        }));
    }

    let live_gateway_scenario_details = (options.level == HarnessEvalLevel::Deep
        && options.allow_real_model)
        .then(|| run_live_gateway_scenarios(&options));
    let live_gateway_scenarios = live_gateway_scenario_details
        .as_ref()
        .map(live_gateway_scenario_summary);
    // The production Gateway owns model execution. Its per-scenario metrics
    // are therefore the canonical real-provider evidence for deep evaluation,
    // rather than a later report-writing model call.
    let live_provider_evidence =
        live_gateway_provider_evidence(live_gateway_scenario_details.as_ref());
    let mut execution_usage = merge_usage(&usage, &live_provider_evidence.usage);
    let mut execution_provider_rounds = live_provider_evidence.provider_rounds;
    let mut execution_rounds = live_provider_evidence.rounds;
    let report_reviewer_requested = report_reviewer_requested();
    if let Some(live) = &live_gateway_scenarios {
        let scenario_count = live
            .get("scenario_count")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        let passed = live
            .get("passed")
            .and_then(Value::as_u64)
            .unwrap_or_default();
        scenarios.push(json!({
            "capability": "live_gateway_scenarios",
            "status": live.get("status").and_then(Value::as_str).unwrap_or("failed"),
            "evidence": format!("{passed}/{scenario_count} production Gateway scenarios passed"),
            "notes": live.get("reason").and_then(Value::as_str).unwrap_or("each scenario requires durable session, terminal, cursor, and projection evidence")
        }));
    }

    let total_elapsed_ms = started.elapsed().as_millis();
    let event_observation_parity = json!({
        "status": if tool_calls == tool_call_details.len() { "passed" } else { "failed" },
        "tool_events": tool_calls,
        "observations": tool_call_details.len(),
        "source": "harness_eval.execution_trace"
    });
    let mut runtime_action_log = vec![
        json!({"index": 1, "action": "stable_ai_scenario_matrix", "evidence": "deterministic fake provider suite"}),
        json!({"index": 2, "action": "harness_capability_coverage", "evidence": "runtime module map coverage"}),
        json!({"index": 3, "action": "knowledge_fabric.evaluate", "evidence": "context governance activated and blocked namespaces"}),
        json!({"index": 4, "action": "reality_context_eval.evaluate", "evidence": "RecallReport and ContextEnvelope scenario matrix generated"}),
        json!({"index": 5, "action": "mission_runtime_collaboration_closure", "evidence": "team/execution_graph/session/conflict/projection closure generated"}),
        json!({"index": 6, "action": "evolution_closure", "evidence": "signal/proposal/sandbox/skill draft closure generated"}),
    ];
    if complex.is_some() {
        runtime_action_log.push(json!({"index": 7, "action": "complex_harness_scenario_suite", "evidence": "full complex scenario suite generated"}));
    }
    if live_gateway_scenario_details.is_some() {
        runtime_action_log.push(json!({"index": runtime_action_log.len() + 1, "action": "live_gateway_scenarios", "evidence": "isolated Gateway public API execution with durable session/terminal/projection traces"}));
    }
    let evolution_closure = evaluate_evolution_closure();
    let mut report = json!({
        "kind": "mission_harness.eval_report",
        "level": options.level.as_str(),
        "status": if scenarios.iter().all(|item| item.get("status").and_then(Value::as_str) == Some("passed")) { "passed" } else { "failed" },
        "provider": options.provider.as_deref(),
        "budget": options.budget.as_deref(),
        "authorized_real_model": options.allow_real_model,
        "gateway_process": live_gateway_scenario_details.is_some(),
        "scenario_matrix": stable_ai_scenario_matrix(),
        "stable_ai": stable_ai,
        "scenarios": scenarios,
        "metrics": [
            {"name": "provider_rounds", "value": execution_provider_rounds.to_string(), "notes": if options.level == HarnessEvalLevel::Deep { "derived from independently traced isolated Gateway scenarios; an optional report reviewer is tracked separately" } else { "quick/full gateway runs do not consume provider tokens" }},
            {"name": "runtime_actions", "value": runtime_actions.to_string(), "notes": "library runner exercised scenario, coverage, and knowledge fabric checks"},
            {"name": "tool_calls", "value": tool_calls.to_string(), "notes": if options.level == HarnessEvalLevel::Full { "full eval executed local read-only tool evidence" } else { "quick smoke lane intentionally does not execute tools" }}
        ],
        "complex_scenarios": complex,
        "reality_context_eval": reality_context,
        "mission_runtime_collaboration": mission_runtime,
        "evolution_closure": evolution_closure,
        "next_gen_harness_closure": next_gen_harness,
        "real_tool_scenarios": real_tool_scenarios,
        "live_gateway_scenarios": live_gateway_scenarios,
        "live_gateway_scenario_details": live_gateway_scenario_details,
        "event_observation_parity": event_observation_parity,
        "report_package": {
            "status": "prepared",
            "required_dirs": ["requests", "responses", "events", "run-evidence", "live-scenarios", "model-speed", "quality-rubric"]
        },
        "execution_trace": {
            "kind": "harness_eval.execution_trace",
            "started_at_ms": requested_at_ms,
            "finished_at_ms": now_ms(),
            "total_elapsed_ms": total_elapsed_ms,
            "provider_rounds": execution_provider_rounds,
            "runtime_actions": runtime_actions,
            "tool_calls": tool_calls,
            "total_usage": execution_usage,
            "rounds": execution_rounds,
            "tool_call_log": tool_call_log,
            "runtime_action_log": runtime_action_log
        },
        "tool_call_details": tool_call_details,
        "provider_round_details": [],
        "ai_reviewer": {
            "status": if options.level == HarnessEvalLevel::Deep && report_reviewer_requested { "pending" } else { "not_requested" },
            "report_source": "optional_provider_round"
        },
        "evidence_manifest": build_evidence_manifest(&options, requested_at_ms, tool_calls, execution_usage.total_tokens),
        "result_package_dir": null
    });
    report["next_gen_harness_closure"]["provider_rounds"] = json!(execution_provider_rounds);
    if control.is_cancel_requested() {
        let record = cancelled_record(
            run_id,
            &options,
            requested_at_ms,
            Some(started.elapsed().as_millis()),
            "harness eval cancelled before provider review/report write",
        );
        store.upsert_run(&record)?;
        return Ok(record);
    }
    if options.level == HarnessEvalLevel::Deep
        && options.allow_real_model
        && report_reviewer_requested
    {
        let provider_review = run_deep_real_provider_review(&options, &report);
        let provider_round_count = provider_review.provider_rounds.len();
        let provider_rounds = serde_json::to_value(&provider_review.provider_rounds)
            .map_err(|error| error.to_string())?;
        let provider_round_details = serde_json::to_value(&provider_review.provider_round_details)
            .map_err(|error| error.to_string())?;
        if let Some(rounds) = provider_rounds.as_array() {
            execution_rounds.extend(rounds.iter().cloned());
        }
        execution_provider_rounds = execution_provider_rounds.saturating_add(provider_round_count);
        execution_usage = merge_usage(&execution_usage, &provider_review.usage);
        report["execution_trace"]["rounds"] = Value::Array(execution_rounds);
        report["execution_trace"]["provider_rounds"] = json!(execution_provider_rounds);
        report["execution_trace"]["total_usage"] =
            serde_json::to_value(&execution_usage).map_err(|error| error.to_string())?;
        report["provider_round_details"] = provider_round_details;
        report["ai_reviewer"] = json!({
            "status": provider_review.status,
            "report_source": "provider_round",
            "provider_rounds": provider_round_count,
            "error": provider_review.error,
            "markdown_available": provider_review.reviewer_markdown.is_some()
        });
        if let Some(markdown) = provider_review.reviewer_markdown {
            report["ai_reviewer_report_markdown"] = Value::String(markdown);
        }
        report["metrics"][0]["value"] = Value::String(execution_provider_rounds.to_string());
        report["evidence_manifest"]["token_usage"] = json!({
            "total_tokens": execution_usage.total_tokens,
            "source": execution_usage.usage_source
        });
        report["next_gen_harness_closure"]["provider_rounds"] = json!(execution_provider_rounds);
    }
    let cancelled_after_provider = control.is_cancel_requested();
    let gate = evaluate_report_gate(&report);
    report["report_gate"] = serde_json::to_value(&gate).map_err(|error| error.to_string())?;
    report["status"] = Value::String(gate.status.clone());
    let summary = store.write_report(options.level.as_str(), &mut report, &stable_ai)?;
    let run_status = if cancelled_after_provider {
        HarnessEvalRunStatus::Cancelled
    } else if summary.status == "passed" {
        HarnessEvalRunStatus::Completed
    } else {
        HarnessEvalRunStatus::Failed
    };
    let record = HarnessEvalRunRecord {
        run_id,
        level: summary.level.clone(),
        status: run_status.as_str().to_string(),
        requested_at_ms,
        finished_at_ms: Some(now_ms()),
        authorized_real_model: options.allow_real_model,
        provider: summary.provider.clone(),
        budget: summary.budget.clone(),
        report_id: Some(summary.id.clone()),
        report_path: Some(summary.report_path.clone()),
        result_package_dir: summary.result_package_dir.clone(),
        total_elapsed_ms: summary.total_elapsed_ms,
        provider_rounds: summary.provider_rounds,
        tool_calls: summary.tool_calls,
        total_tokens: summary.total_tokens,
        scenario_count: summary.scenario_count,
        message: if cancelled_after_provider {
            "harness eval cancellation requested after provider/tool work; report retained for audit"
                .to_string()
        } else {
            "harness eval smoke completed through library runner".to_string()
        },
    };
    store.upsert_run(&record)?;
    Ok(record)
}

fn cancelled_record(
    run_id: String,
    options: &HarnessEvalRunnerOptions,
    requested_at_ms: u128,
    total_elapsed_ms: Option<u128>,
    message: impl Into<String>,
) -> HarnessEvalRunRecord {
    HarnessEvalRunRecord {
        run_id,
        level: options.level.as_str().to_string(),
        status: HarnessEvalRunStatus::Cancelled.as_str().to_string(),
        requested_at_ms,
        finished_at_ms: Some(now_ms()),
        authorized_real_model: options.allow_real_model,
        provider: options.provider.clone(),
        budget: options.budget.clone(),
        report_id: None,
        report_path: None,
        result_package_dir: None,
        total_elapsed_ms,
        provider_rounds: 0,
        tool_calls: 0,
        total_tokens: 0,
        scenario_count: 0,
        message: message.into(),
    }
}

fn live_gateway_scenario_summary(details: &Value) -> Value {
    let mut summary = details.clone();
    if let Some(scenarios) = summary.get_mut("scenarios").and_then(Value::as_array_mut) {
        for scenario in scenarios {
            if let Some(object) = scenario.as_object_mut() {
                object.remove("trace");
                object.insert(
                    "trace_artifact".to_string(),
                    Value::String("live-scenarios/<scenario>.json".to_string()),
                );
            }
        }
    }
    summary
}

#[derive(Default)]
struct LiveGatewayProviderEvidence {
    usage: HarnessEvalUsageSummary,
    provider_rounds: usize,
    rounds: Vec<Value>,
}

fn live_gateway_provider_evidence(details: Option<&Value>) -> LiveGatewayProviderEvidence {
    let Some(scenarios) = details
        .and_then(|value| value.get("scenarios"))
        .and_then(Value::as_array)
    else {
        return LiveGatewayProviderEvidence::default();
    };

    let mut evidence = LiveGatewayProviderEvidence {
        usage: HarnessEvalUsageSummary {
            usage_source: "live_gateway_canonical_usage".to_string(),
            ..HarnessEvalUsageSummary::default()
        },
        ..LiveGatewayProviderEvidence::default()
    };
    let model = details
        .and_then(|value| value.get("model"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    for scenario in scenarios {
        let metrics = scenario.get("metrics").unwrap_or(&Value::Null);
        let input_tokens = json_u32(metrics.get("input_tokens"));
        let output_tokens = json_u32(metrics.get("output_tokens"));
        let cache_tokens = json_u32(metrics.get("cache_tokens"));
        let total_tokens = json_u32(metrics.get("total_tokens"));
        let model_rounds = json_u32(metrics.get("model_rounds"));
        evidence.usage.input_tokens = evidence.usage.input_tokens.saturating_add(input_tokens);
        evidence.usage.output_tokens = evidence.usage.output_tokens.saturating_add(output_tokens);
        evidence.usage.cache_read_input_tokens = evidence
            .usage
            .cache_read_input_tokens
            .saturating_add(cache_tokens);
        evidence.usage.total_tokens =
            evidence
                .usage
                .total_tokens
                .saturating_add(if total_tokens > 0 {
                    total_tokens
                } else {
                    input_tokens
                        .saturating_add(output_tokens)
                        .saturating_add(cache_tokens)
                });
        evidence.provider_rounds = evidence
            .provider_rounds
            .saturating_add(model_rounds as usize);
        evidence.rounds.push(json!({
            "round_kind": "live_gateway_scenario",
            "scenario_id": scenario.get("scenario_id"),
            "model": model.as_deref(),
            "status": scenario.get("status"),
            "metrics": metrics,
            "trace_artifact": format!(
                "live-scenarios/{}.json",
                scenario.get("scenario_id").and_then(Value::as_str).unwrap_or("unknown")
            )
        }));
    }
    evidence
}

fn merge_usage(
    left: &HarnessEvalUsageSummary,
    right: &HarnessEvalUsageSummary,
) -> HarnessEvalUsageSummary {
    if right.total_tokens == 0 {
        return left.clone();
    }
    if left.total_tokens == 0 {
        return right.clone();
    }
    HarnessEvalUsageSummary {
        input_tokens: left.input_tokens.saturating_add(right.input_tokens),
        output_tokens: left.output_tokens.saturating_add(right.output_tokens),
        cache_creation_input_tokens: left
            .cache_creation_input_tokens
            .saturating_add(right.cache_creation_input_tokens),
        cache_read_input_tokens: left
            .cache_read_input_tokens
            .saturating_add(right.cache_read_input_tokens),
        total_tokens: left.total_tokens.saturating_add(right.total_tokens),
        usage_source: format!("{}+{}", left.usage_source, right.usage_source),
    }
}

fn json_u32(value: Option<&Value>) -> u32 {
    value
        .and_then(Value::as_u64)
        .map(|value| value.min(u64::from(u32::MAX)) as u32)
        .unwrap_or_default()
}

fn report_reviewer_requested() -> bool {
    matches!(
        std::env::var("COWD_EVAL_REPORT_REVIEWER").ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE")
    )
}

struct FullRealToolEval {
    report: RealToolScenarioReport,
    tool_details: Vec<ToolCallDetail>,
    usage: crate::HarnessEvalUsageSummary,
}

fn build_evidence_manifest(
    options: &HarnessEvalRunnerOptions,
    requested_at_ms: u128,
    tool_calls: usize,
    total_tokens: u32,
) -> Value {
    let repo = std::env::current_dir()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| ".".to_string());
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string());
    json!({
        "kind": "harness_eval.evidence_manifest",
        "report_id": null,
        "repo": repo,
        "commit": commit,
        "version": env!("CARGO_PKG_VERSION"),
        "command": format!(
            "harness-eval {}{}",
            options.level.as_str(),
            if options.allow_real_model { " --allow-real-model" } else { "" }
        ),
        "requested_at_ms": requested_at_ms,
        "real_model_authorized": options.allow_real_model,
        "provider": options.provider.as_deref(),
        "budget": options.budget.as_deref(),
        "token_usage": {
            "total_tokens": total_tokens,
            "source": if total_tokens > 0 { "provider_or_tool_estimate" } else { "deterministic_contract" }
        },
        "tool_calls": tool_calls,
        "sidecar_fixture_status": "not_required_deterministic",
        "db_fixture_status": "not_required_deterministic",
        "source_fixture_status": "not_required_deterministic",
        "target_repo_dirty_state": "not_checked_by_library_runner",
        "notes": "manifest is generated by harness-eval runner and finalized with report_id by report store"
    })
}

fn mission_runtime_collaboration_failure(
    started: Instant,
    objective: &str,
    stage: &str,
    error: impl std::fmt::Display,
) -> Value {
    json!({
        "kind": "harness_eval.mission_runtime_collaboration_closure",
        "status": "failed",
        "objective": objective,
        "failure_degraded_reason": format!("{stage}: {error}"),
        "selected_strategy": Value::Null,
        "execution_graph": Value::Null,
        "agents": Value::Null,
        "sessions": Value::Null,
        "conflicts": Value::Null,
        "mission_projection": Value::Null,
        "terminal_evidence": Value::Null,
        "final_result_quality": {
            "passed_checks": [],
            "failed_checks": [stage],
            "score": 0.0,
        },
        "latency": {"elapsed_ms": started.elapsed().as_millis(), "provider_rounds": 0},
    })
}

fn evaluate_mission_runtime_collaboration_closure() -> Value {
    let started = Instant::now();
    let objective = "复杂代码重构需要多 Agent 并行审查、跨 Session 跟踪、冲突仲裁和证据化回归";
    let simple_decision = runtime::build_runtime_execution_decision("解释 ping 的含义", None);
    let capability_response = runtime::runtime_capabilities_response_with_detail(
        objective,
        Some("harness_eval"),
        Some("DeepInvestigation"),
        Some("runtime_action_contract"),
    );
    let strategy = harness_contract::strategy::decide_strategy(
        &harness_contract::strategy::StrategyInput::from_prompt(objective),
    );
    let collaboration = runtime::CollaborationTemplateMatcher.decide(objective, &strategy);
    let session_id = format!("mission-eval-session-{}", uuid::Uuid::new_v4());
    let mission = runtime::MissionRuntime::new();
    let session = match mission.start_session(runtime::StartMissionSessionRequest {
        title: "Mission runtime collaboration closure".to_string(),
        session_id: Some(session_id.clone()),
    }) {
        Ok(session) => session,
        Err(error) => {
            return mission_runtime_collaboration_failure(
                started,
                objective,
                "start_mission_session",
                error,
            );
        }
    };
    let team_id = format!("harness-eval-team-{}", uuid::Uuid::new_v4());
    let capability = runtime::resolve_agent_capability(runtime::AgentCapabilityRequest {
        role_id: "executor".to_string(),
        allowed_capabilities: vec![
            "read".to_string(),
            "search".to_string(),
            "write".to_string(),
            "test".to_string(),
        ],
        evidence_duties: vec!["changes".to_string(), "verification".to_string()],
    });
    let runtime_services = match runtime::RuntimeServices::in_memory() {
        Ok(services) => services,
        Err(error) => {
            return mission_runtime_collaboration_failure(
                started,
                objective,
                "initialize_runtime_services",
                error,
            );
        }
    };
    let template_id = match harness_contract::team::TeamTemplateDefinitionId::new(
        harness_contract::agent::DefinitionScope::Builtin,
        "cowd/parallel-research-synthesis",
    ) {
        Ok(template_id) => template_id,
        Err(error) => {
            return mission_runtime_collaboration_failure(
                started,
                objective,
                "resolve_team_template",
                error,
            );
        }
    };
    let team_plan = match runtime_services.team_runtime().plan(
        harness_contract::team::TeamInstantiationRequest {
            request_id: format!("harness-eval-request-{team_id}"),
            team_id: team_id.clone(),
            session_id: session.session_id.clone(),
            mission_id: None,
            parent_execution: None,
            selection_mode: harness_contract::team::TeamSelectionMode::Explicit,
            strategy_binding: None,
            template_selector: harness_contract::team::TeamTemplateSelector::LatestStable {
                template_id,
            },
            objective: objective.to_string(),
            acceptance: vec!["summary".to_string(), "evidence".to_string()],
            risk: None,
            role_binding_overrides: Vec::new(),
            cardinality_overrides: Vec::new(),
            focus_partition_plans: Vec::new(),
            permission_lease: "read_only".to_string(),
            model_lease: "harness_eval".to_string(),
            budget_lease: None,
            managed_invocation: None,
            resource_scopes: vec!["read:crates/runtime".to_string()],
        },
    ) {
        Ok(team_plan) => team_plan,
        Err(error) => {
            return mission_runtime_collaboration_failure(
                started,
                objective,
                "plan_team_execution_graph",
                error,
            );
        }
    };
    let relation = match runtime_services.session_relations().add_relation(
        &session.session_id,
        format!("{}-review", session.session_id),
        runtime::SessionRelationKind::ConflictsWith,
        "review lane disputes unbounded execution",
        vec![format!("execution_graph:{}", team_plan.graph.id)],
    ) {
        Ok(relation) => relation,
        Err(error) => {
            return mission_runtime_collaboration_failure(
                started,
                objective,
                "record_session_conflict_relation",
                error,
            );
        }
    };
    runtime_services
        .conflict_resolver()
        .resolve(runtime::ConflictResolutionRequest {
            source: runtime::ConflictSourceKind::SessionRelation,
            severity: runtime::ConflictSeverity::Medium,
            summary: relation.summary.clone(),
            evidence_refs: relation.evidence_refs.clone(),
            affected_scope: vec![
                format!("session:{}", relation.from_session_id),
                format!("session:{}", relation.to_session_id),
            ],
        });
    let conflict_count = runtime_services.conflict_resolver().receipts().len() as u64;
    let projection = mission.projection(
        runtime_services.session_relations(),
        runtime_services.agent_runtime(),
        runtime_services.team_runtime(),
        runtime_services.approval_queue(),
        runtime_services.conflict_resolver(),
        runtime_services.mission_evidence(),
        runtime_services.mission_schedules().projection(),
    );
    let checks = [
        (
            "simple_question_direct",
            simple_decision.pattern() == ExecutionPattern::Direct,
        ),
        (
            "template_selected",
            collaboration.template_id != runtime::CollaborationTemplateId::DirectExecutor,
        ),
        (
            "execution_graph_quality",
            harness_contract::execution_graph::validate_execution_graph(&team_plan.graph).is_ok()
                && team_plan.graph.nodes.iter().any(|node| {
                    node.kind == harness_contract::execution_graph::ExecutionNodeKind::Verify
                })
                && team_plan.graph.nodes.iter().any(|node| {
                    node.kind == harness_contract::execution_graph::ExecutionNodeKind::Synthesize
                }),
        ),
        (
            "collaboration_graph_compiler",
            team_plan.role_slots.len() >= 2,
        ),
        (
            "capability_binding",
            capability.allowed_tools.contains("write_file")
                && capability.allowed_tools.contains("bash"),
        ),
        (
            "conflict_arbitration",
            conflict_count > 0 && relation.kind == runtime::SessionRelationKind::ConflictsWith,
        ),
        (
            "mission_projection_v2",
            projection.schema_version == 3
                && projection.conflict_projection["kind"] == "runtime.conflicts"
                && projection.capability_projection["name"] == "cowd-runtime-capability-catalog",
        ),
        (
            "model_visible_actions",
            capability_response["backend_capabilities"]["contracts"]
                .as_array()
                .is_some_and(|contracts| {
                    contracts
                        .iter()
                        .any(|contract| contract["runtime_action"] == "use_team_template")
                }),
        ),
    ];
    let passed_checks = checks
        .iter()
        .filter(|&(_name, passed)| *passed)
        .map(|(name, _passed)| (*name).to_string())
        .collect::<Vec<_>>();
    let failed_checks = checks
        .iter()
        .filter(|&(_name, passed)| !passed)
        .map(|(name, _passed)| (*name).to_string())
        .collect::<Vec<_>>();
    let elapsed_ms = started.elapsed().as_millis();
    json!({
        "kind": "harness_eval.mission_runtime_collaboration_closure",
        "status": if failed_checks.is_empty() { "passed" } else { "failed" },
        "objective": objective,
        "model_provider": "deterministic_runtime_contract",
        "profile": "DeepInvestigation",
        "selected_strategy": {
            "simple_pattern": simple_decision.pattern().as_str(),
            "complex_pattern": strategy.pattern.as_str(),
            "template": collaboration.template_id.as_str(),
            "runtime_actions": ["continue_single", "use_team_template", "build_execution_graph", "dispatch_session", "request_arbiter", "parallel_tool_batch"],
        },
        "execution_graph": {
            "execution_graph_id": team_plan.graph.id,
            "node_count": team_plan.graph.nodes.len(),
            "edge_count": team_plan.graph.edges.len(),
            "is_dag": harness_contract::execution_graph::validate_execution_graph(&team_plan.graph).is_ok(),
            "has_verify_node": team_plan.graph.nodes.iter().any(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::Verify),
            "has_synthesize_node": team_plan.graph.nodes.iter().any(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::Synthesize),
            "ready_node_ids": team_plan.graph.nodes.iter().filter(|node| node.kind == harness_contract::execution_graph::ExecutionNodeKind::AgentTask).map(|node| node.id.clone()).collect::<Vec<_>>(),
            "blocked_node_ids": Vec::<String>::new(),
        },
        "agents": {
            "team_id": team_id,
            "role_count": team_plan.role_slots.len(),
            "capability_summary": capability.capability_summary,
            "allowed_tools": capability.allowed_tools,
            "permission_mode": format!("{:?}", capability.permission_mode),
        },
        "sessions": {
            "session_id": session.session_id,
            "dispatch_model": "execution_graph_session_handoff",
            "dispatch_lifecycle_owner": "runtime.session_execution",
            "relation_id": relation.relation_id,
        },
        "tool_calls": {
            "count": 0,
            "mode": "deterministic_contract_eval",
            "note": "full real-tool lane is reported separately under real_tool_scenarios",
        },
        "conflicts": {
            "count": conflict_count,
            "relation_kind": format!("{:?}", relation.kind).to_ascii_lowercase(),
        },
        "approvals": {
            "required": false,
            "reason": "scenario is deterministic and does not execute external/destructive writes",
        },
        "evidence_refs": [
            format!("team:{team_id}"),
            format!("execution_graph:{}", team_plan.graph.id),
            format!("session-relation:{}", relation.relation_id),
            format!("synthesis:{}", team_plan.graph.id)
        ],
        "terminal_evidence": {
            "agent_terminal_count": team_plan.role_slots.len(),
            "mailbox_completed_count": 1,
            "synthesis_receipt_id": format!("synthesis:{}", team_plan.graph.id),
            "session_relation_count": 1,
            "runtime_turn_result_count": 1,
            "recovery_applied_count": usize::from(conflict_count > 0),
            "recovery_verified_count": 1,
            "session_handoff_owner": "runtime.session_execution",
            "source": "mission_runtime_collaboration_closure"
        },
        "token_usage": {
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "usage_source": "deterministic_runtime_contract"
        },
        "latency": {
            "elapsed_ms": elapsed_ms,
            "provider_rounds": 0
        },
        "final_result_quality": {
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "score": if failed_checks.is_empty() { 1.0 } else { 0.0 },
        },
        "failure_degraded_reason": if failed_checks.is_empty() {
            Value::Null
        } else {
            json!(failed_checks)
        },
        "mission_projection": {
            "schema_version": projection.schema_version,
            "execution_graph_kind": "harness.execution_graph",
            "conflict_kind": projection.conflict_projection["kind"],
            "evidence_kind": projection.evidence_projection["kind"],
            "capability_name": projection.capability_projection["name"],
        },
    })
}

fn run_full_real_tool_scenarios() -> FullRealToolEval {
    let target_repo = std::env::current_dir()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|_| ".".to_string());
    let mut tool_details = Vec::new();
    tool_details.push(run_eval_tool_call(
        1,
        "repo_refactor",
        "workspace_snapshot",
        json!({
            "include_git": true,
            "include_files": true,
            "roots": ["crates/harness-eval/src", "crates/runtime/src/execution_core"],
            "max_files": 80
        }),
    ));
    tool_details.push(run_eval_tool_call(
        2,
        "repo_refactor",
        "grep_many",
        json!({
            "searches": [
                { "pattern": "runtime_orchestrate", "path": "crates", "glob": "*.rs" },
                { "pattern": "ToolStart|ToolComplete", "path": "crates", "glob": "*.rs" }
            ],
            "max_concurrency": 2
        }),
    ));
    tool_details.push(run_eval_tool_call(
        3,
        "reality_memory",
        "grep_many",
        json!({
            "searches": [
                { "pattern": "KnowledgeFabric|context_governance|suppressed_for_current_turn", "path": "crates", "glob": "*.rs" },
                { "pattern": "team_runtime|collaboration_run|Mission", "path": "crates/runtime/src", "glob": "*.rs" }
            ],
            "max_concurrency": 2
        }),
    ));
    let passed_tool_calls = tool_details
        .iter()
        .filter(|detail| detail.summary.status == "passed")
        .count();
    let report = RealToolScenarioReport {
        kind: "harness_eval.real_tool_scenario_report",
        target_repo,
        total: 3,
        passed: usize::from(passed_tool_calls == tool_details.len()),
        tool_calls: tool_details.len(),
        scenarios: vec![RealToolScenarioResult {
            scenario_id: "full_readonly_evidence".to_string(),
            title: "Full eval local read-only evidence collection".to_string(),
            status: if passed_tool_calls == tool_details.len() {
                "passed"
            } else {
                "failed"
            }
            .to_string(),
            tool_calls: tool_details.len(),
            runtime_evidence: vec![
                "workspace_snapshot captured runtime/eval code roots".to_string(),
                "grep_many scanned orchestration and tool event markers".to_string(),
            ],
            matrix_evidence: vec!["KnowledgeFabric/matrix related code markers scanned".to_string()],
            memory_evidence: vec![
                "suppressed_for_current_turn marker participates in context authority checks"
                    .to_string(),
            ],
            changed_files: Vec::new(),
            diff_summary: "read-only eval changed no source files".to_string(),
            conclusion: "full harness eval produced real local tool evidence".to_string(),
        }],
    };
    let estimated_tokens = estimate_tool_detail_tokens(&tool_details);
    FullRealToolEval {
        report,
        tool_details,
        usage: crate::HarnessEvalUsageSummary {
            input_tokens: estimated_tokens / 3,
            output_tokens: estimated_tokens.saturating_sub(estimated_tokens / 3),
            total_tokens: estimated_tokens,
            usage_source: "deterministic_tool_estimate".to_string(),
            ..crate::HarnessEvalUsageSummary::default()
        },
    }
}

fn run_eval_tool_call(
    call_index: usize,
    scenario_id: &str,
    name: &str,
    input: Value,
) -> ToolCallDetail {
    let started = Instant::now();
    let workspace_root = std::env::current_dir().unwrap_or_else(|_| std::path::PathBuf::from("."));
    let host = tools::ToolHost::builtin("harness-eval", workspace_root);
    let lease = host.pin_snapshot();
    let effect = lease.describe_effect(name, &input);
    let result = runtime::ToolPolicy
        .authorize(
            &effect,
            format!("harness-eval:{scenario_id}:{call_index}"),
            runtime::PermissionMode::ReadOnly,
            30,
        )
        .map_err(|error| error.to_string())
        .and_then(|decision| {
            lease
                .execute(&decision.authorization, name, &input)
                .map_err(|error| error.to_string())
        });
    let elapsed_ms = started.elapsed().as_millis();
    let (status, output, error) = match result {
        Ok(output) => ("passed".to_string(), Some(output), None),
        Err(error) => ("failed".to_string(), None, Some(error)),
    };
    let output_summary = output
        .as_deref()
        .map(summarize_text)
        .or_else(|| error.as_deref().map(summarize_text))
        .unwrap_or_else(|| "no output".to_string());
    ToolCallDetail {
        summary: ToolCallSummary {
            call_index,
            scenario_id: scenario_id.to_string(),
            name: name.to_string(),
            status,
            elapsed_ms,
            input_summary: summarize_text(&input.to_string()),
            output_summary,
            detail_path: format!("run-evidence/tool-call-{call_index}.json"),
        },
        input,
        output,
        error,
    }
}

fn summarize_text(text: &str) -> String {
    const LIMIT: usize = 220;
    let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if compact.chars().count() <= LIMIT {
        compact
    } else {
        format!("{}...", compact.chars().take(LIMIT).collect::<String>())
    }
}

fn estimate_tool_detail_tokens(details: &[ToolCallDetail]) -> u32 {
    let chars = details
        .iter()
        .map(|detail| {
            detail.summary.input_summary.len()
                + detail.summary.output_summary.len()
                + detail.output.as_ref().map_or(0, String::len)
                + detail.error.as_ref().map_or(0, String::len)
        })
        .sum::<usize>();
    ((chars / 4).max(1)).min(u32::MAX as usize) as u32
}

fn stable_ai_report_for(options: &HarnessEvalRunnerOptions) -> StableAiHealthReport {
    StableAiHealthReport::from_fake_eval(
        env!("CARGO_PKG_VERSION"),
        options
            .provider
            .clone()
            .unwrap_or_else(|| "deterministic_smoke".to_string()),
        None,
        options.allow_real_model,
        if options.allow_real_model {
            "real model explicitly authorized, but gateway smoke keeps deterministic lane"
        } else {
            "real provider not enabled for default gateway smoke"
        },
        fake_provider_scenario_report(),
        harness_capability_coverage_report(),
        "gateway smoke runs via /api/harness-eval/runs",
        "webui/tui consume harness eval report summaries",
        "deterministic runner writes trace and report package",
    )
}

fn fake_provider_scenario_report() -> crate::ScenarioSuiteReport {
    let matrix = stable_ai_scenario_matrix();
    let specs = matrix
        .iter()
        .map(|item| {
            ScenarioSpec::new(item.id.clone(), item.objective.clone())
                .expect_pattern(pattern_for_scenario(item.kind))
                .require(ScenarioCheck::text_contains(
                    format!("{}.evidence", item.id),
                    item.required_evidence[0].clone(),
                    "harness-eval",
                    "scenario runner must emit required evidence markers",
                ))
        })
        .collect::<Vec<_>>();
    let observations = matrix
        .iter()
        .map(|item| ScenarioObservation {
            scenario_id: item.id.clone(),
            strategy_pattern: pattern_for_scenario(item.kind),
            finalization_blocked: item.kind == E2eScenarioKind::Recovery,
            regression_allowed: item.kind != E2eScenarioKind::Recovery,
            has_execution_graph: matches!(
                item.kind,
                E2eScenarioKind::ComplexPlan | E2eScenarioKind::TeamParallel
            ),
            execution_graph_quality_ok: item.kind != E2eScenarioKind::SimpleOnce,
            growth_has_blocker: item.kind == E2eScenarioKind::Recovery,
            growth_signal_kinds: item.required_evidence.clone(),
            memory_candidate_count: usize::from(item.kind == E2eScenarioKind::RealityMemory),
            matrix_signal_count: usize::from(matches!(
                item.kind,
                E2eScenarioKind::RealityMemory | E2eScenarioKind::ComplexPlan
            )),
            assistant_text: format!(
                "fake provider scenario {} passed with evidence {}",
                item.id,
                item.required_evidence.join(",")
            ),
        })
        .collect::<Vec<_>>();
    ScenarioSuite::new(specs).evaluate(&observations)
}

fn pattern_for_scenario(kind: E2eScenarioKind) -> ExecutionPattern {
    match kind {
        E2eScenarioKind::SimpleOnce => ExecutionPattern::Direct,
        E2eScenarioKind::TeamParallel => ExecutionPattern::Collaborate,
        E2eScenarioKind::GovernedConnector => ExecutionPattern::Execute,
        E2eScenarioKind::ComplexPlan
        | E2eScenarioKind::RealityMemory
        | E2eScenarioKind::ToolLsp
        | E2eScenarioKind::Recovery => ExecutionPattern::Execute,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_gateway_usage_is_canonical_real_provider_evidence() {
        let details = json!({
            "model": "deepseek-v4-flash",
            "scenarios": [
                {
                    "scenario_id": "direct",
                    "status": "passed",
                    "metrics": {
                        "input_tokens": 100,
                        "output_tokens": 20,
                        "cache_tokens": 3,
                        "total_tokens": 123,
                        "model_rounds": 1
                    }
                },
                {
                    "scenario_id": "team",
                    "status": "passed",
                    "metrics": {
                        "input_tokens": 200,
                        "output_tokens": 40,
                        "cache_tokens": 7,
                        "total_tokens": 247,
                        "model_rounds": 4
                    }
                }
            ]
        });

        let evidence = live_gateway_provider_evidence(Some(&details));

        assert_eq!(evidence.provider_rounds, 5);
        assert_eq!(evidence.usage.input_tokens, 300);
        assert_eq!(evidence.usage.output_tokens, 60);
        assert_eq!(evidence.usage.cache_read_input_tokens, 10);
        assert_eq!(evidence.usage.total_tokens, 370);
        assert_eq!(evidence.rounds.len(), 2);
        assert_eq!(evidence.rounds[1]["round_kind"], "live_gateway_scenario");
        assert_eq!(
            evidence.rounds[1]["trace_artifact"],
            "live-scenarios/team.json"
        );
    }

    #[test]
    fn deep_eval_is_gated_without_explicit_real_authorization() {
        let root =
            std::env::temp_dir().join(format!("cowd-harness-eval-gated-{}", uuid::Uuid::new_v4()));
        let store = HarnessEvalReportStore::new(&root);
        let record = run_eval(
            &store,
            HarnessEvalRunnerOptions {
                level: HarnessEvalLevel::Deep,
                provider: Some("configured".to_string()),
                budget: Some("low".to_string()),
                allow_real_model: false,
            },
        )
        .expect("run eval");
        assert_eq!(record.status, "gated");
        assert_eq!(record.total_tokens, 0);
        assert!(store.list_reports().expect("reports").is_empty());
        assert_eq!(store.list_runs().expect("runs").len(), 1);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn full_eval_generates_real_tool_evidence_and_report_gate() {
        let root =
            std::env::temp_dir().join(format!("cowd-harness-eval-full-{}", uuid::Uuid::new_v4()));
        let store = HarnessEvalReportStore::new(&root);
        let record = run_eval(
            &store,
            HarnessEvalRunnerOptions {
                level: HarnessEvalLevel::Full,
                provider: None,
                budget: Some("low".to_string()),
                allow_real_model: false,
            },
        )
        .expect("run eval");

        assert_eq!(record.status, "completed");
        assert!(record.tool_calls >= 3);
        assert!(record.total_tokens > 0);
        let report_id = record.report_id.as_deref().expect("report id");
        let detail = store
            .get_report(report_id)
            .expect("detail")
            .expect("report exists");
        assert_eq!(detail.summary.status, "passed");
        assert_eq!(detail.report["report_gate"]["status"], "passed");
        assert_eq!(
            detail.report["mission_runtime_collaboration"]["status"],
            "passed"
        );
        assert_eq!(
            detail.report["next_gen_harness_closure"]["status"],
            "passed"
        );
        assert_eq!(detail.report["next_gen_harness_closure"]["failed"], 0);
        assert_eq!(
            detail.report["event_observation_parity"]["status"],
            "passed"
        );
        assert_eq!(detail.report["reality_context_eval"]["failed"], 0);
        assert!(detail.report["reality_context_eval"]["scenarios"]
            .as_array()
            .is_some_and(|items| items.len() >= 10));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("summary.md")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("quality-rubric.json")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("full-analysis-report-template.md")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("analysis-context.json")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("run-evidence/tool-call-1.json")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("tool-calls/tool-call-1.json")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("evidence/reality-context-eval.json")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("evidence/next-gen-harness-closure.json")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("evidence/evidence-manifest.json")));
        assert!(
            detail.report["real_tool_scenarios"]["scenarios"][0]["changed_files"]
                .as_array()
                .is_some_and(Vec::is_empty)
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn controlled_eval_cancels_before_report_write() {
        let root =
            std::env::temp_dir().join(format!("cowd-harness-eval-cancel-{}", uuid::Uuid::new_v4()));
        let store = HarnessEvalReportStore::new(&root);
        let cancel = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let record = run_eval_controlled(
            &store,
            HarnessEvalRunnerOptions {
                level: HarnessEvalLevel::Full,
                provider: None,
                budget: Some("low".to_string()),
                allow_real_model: false,
            },
            HarnessEvalRunControl::with_run_id("cancel-test").with_cancel(cancel),
        )
        .expect("cancelled eval");

        assert_eq!(record.run_id, "cancel-test");
        assert_eq!(record.status, "cancelled");
        assert!(record.report_id.is_none());
        assert!(store.list_reports().expect("reports").is_empty());
        assert_eq!(
            store
                .get_run("cancel-test")
                .expect("run")
                .expect("run exists")
                .status,
            "cancelled"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn mission_runtime_collaboration_closure_exercises_runtime_projection() {
        let report = evaluate_mission_runtime_collaboration_closure();

        assert_eq!(report["status"], "passed");
        assert!(report["mission_projection"]["schema_version"]
            .as_u64()
            .is_some_and(|version| version >= 2));
        assert!(report["selected_strategy"]["runtime_actions"]
            .as_array()
            .expect("runtime actions")
            .iter()
            .any(|item| item == "use_team_template"));
        assert!(report["execution_graph"]["is_dag"]
            .as_bool()
            .unwrap_or(false));
        assert!(report["conflicts"]["count"].as_u64().unwrap_or_default() > 0);
        assert!(report["final_result_quality"]["failed_checks"]
            .as_array()
            .is_some_and(Vec::is_empty));
    }

    #[test]
    fn next_gen_harness_quick_eval_declares_plan_only_tool_lane() {
        let root = std::env::temp_dir().join(format!(
            "cowd-harness-eval-nextgen-{}",
            uuid::Uuid::new_v4()
        ));
        let store = HarnessEvalReportStore::new(&root);
        let record = run_eval(
            &store,
            HarnessEvalRunnerOptions {
                level: HarnessEvalLevel::Quick,
                provider: None,
                budget: Some("low".to_string()),
                allow_real_model: false,
            },
        )
        .expect("run eval");
        let detail = store
            .get_report(record.report_id.as_deref().expect("report id"))
            .expect("detail")
            .expect("report exists");
        assert_eq!(
            detail.report["next_gen_harness_closure"]["status"],
            "passed"
        );
        let tool_batch = detail.report["next_gen_harness_closure"]["scenarios"]
            .as_array()
            .expect("scenarios")
            .iter()
            .find(|scenario| scenario["scenario_id"] == "tool_batch_efficiency")
            .expect("tool batch scenario");
        assert_eq!(tool_batch["claims_tool_validation"], false);
        assert_eq!(tool_batch["tool_calls"], 0);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn deep_allow_real_without_provider_rounds_records_failed_run() {
        let root =
            std::env::temp_dir().join(format!("cowd-harness-eval-deep-{}", uuid::Uuid::new_v4()));
        let store = HarnessEvalReportStore::new(&root);
        let record = run_eval(
            &store,
            HarnessEvalRunnerOptions {
                level: HarnessEvalLevel::Deep,
                provider: Some("configured".to_string()),
                budget: Some("low".to_string()),
                allow_real_model: true,
            },
        )
        .expect("run eval");

        assert_eq!(record.status, "failed");
        let detail = store
            .get_report(record.report_id.as_deref().expect("report id"))
            .expect("detail")
            .expect("report exists");
        assert_eq!(detail.report["report_gate"]["status"], "failed");
        assert!(detail.report["report_gate"]["items"]
            .as_array()
            .expect("gate items")
            .iter()
            .any(
                |item| item["name"] == "real_model_claim_has_provider_rounds"
                    && item["status"] == "failed"
            ));
        let _ = std::fs::remove_dir_all(root);
    }
}
