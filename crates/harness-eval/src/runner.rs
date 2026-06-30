use std::time::Instant;

use harness_contract::core::ExecutionMode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    evaluate_complex_harness_scenarios, evaluate_knowledge_fabric_context_governance,
    evaluate_reality_context_scenarios, evaluate_report_gate, harness_capability_coverage_report,
    report::{
        HarnessEvalLevel, HarnessEvalRunRecord, HarnessEvalRunStatus, ToolCallDetail,
        ToolCallSummary,
    },
    report_store::{empty_usage, now_ms, HarnessEvalReportStore},
    stable_ai_scenario_matrix, E2eScenarioKind, RealToolScenarioReport, RealToolScenarioResult,
    ScenarioCheck, ScenarioObservation, ScenarioSpec, ScenarioSuite, StableAiHealthReport,
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

pub fn run_eval(
    store: &HarnessEvalReportStore,
    options: HarnessEvalRunnerOptions,
) -> Result<HarnessEvalRunRecord, String> {
    let requested_at_ms = now_ms();
    let run_id = format!(
        "harness-eval-{}-{}",
        options.level.as_str(),
        uuid::Uuid::new_v4()
    );
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
        store.append_run(&record)?;
        return Ok(record);
    }

    let started = Instant::now();
    let stable_ai = stable_ai_report_for(&options);
    let complex =
        (options.level == HarnessEvalLevel::Full).then(evaluate_complex_harness_scenarios);
    let knowledge = evaluate_knowledge_fabric_context_governance();
    let reality_context = evaluate_reality_context_scenarios();
    let real_tool = (options.level == HarnessEvalLevel::Full).then(run_full_real_tool_scenarios);
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
    ];
    if let Some(complex) = &complex {
        scenarios.push(json!({
            "capability": "complex_harness_scenario_suite",
            "status": if complex.failed == 0 && complex.average_score >= 0.9 { "passed" } else { "failed" },
            "evidence": format!("{}/{} complex scenarios passed; average={:.2}", complex.passed, complex.total, complex.average_score),
            "notes": "full smoke validates repo refactor, memory governance, multi-agent, cross-session, and recovery scenario models"
        }));
    }

    let total_elapsed_ms = started.elapsed().as_millis();
    let runtime_actions = 4 + usize::from(complex.is_some());
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
    let event_observation_parity = json!({
        "status": if tool_calls == tool_call_details.len() { "passed" } else { "failed" },
        "tool_events": tool_calls,
        "observations": tool_call_details.len(),
        "source": "harness_eval.execution_trace"
    });
    let mut report = json!({
        "kind": "mission_harness.eval_report",
        "level": options.level.as_str(),
        "status": if scenarios.iter().all(|item| item.get("status").and_then(Value::as_str) == Some("passed")) { "passed" } else { "failed" },
        "provider": options.provider,
        "budget": options.budget,
        "gateway_process": false,
        "scenario_matrix": stable_ai_scenario_matrix(),
        "stable_ai": stable_ai,
        "scenarios": scenarios,
        "metrics": [
            {"name": "provider_rounds", "value": "0", "notes": "default gateway run is deterministic and does not consume provider tokens"},
            {"name": "runtime_actions", "value": runtime_actions.to_string(), "notes": "library runner exercised scenario, coverage, and knowledge fabric checks"},
            {"name": "tool_calls", "value": tool_calls.to_string(), "notes": if options.level == HarnessEvalLevel::Full { "full eval executed local read-only tool evidence" } else { "quick smoke lane intentionally does not execute tools" }}
        ],
        "complex_scenarios": complex,
        "reality_context_eval": reality_context,
        "real_tool_scenarios": real_tool_scenarios,
        "event_observation_parity": event_observation_parity,
        "report_package": {
            "status": "prepared",
            "required_dirs": ["requests", "responses", "events", "run-evidence", "model-speed", "quality-rubric"]
        },
        "execution_trace": {
            "kind": "harness_eval.execution_trace",
            "started_at_ms": requested_at_ms,
            "finished_at_ms": now_ms(),
            "total_elapsed_ms": total_elapsed_ms,
            "provider_rounds": 0,
            "runtime_actions": runtime_actions,
            "tool_calls": tool_calls,
            "total_usage": usage,
            "rounds": [],
            "tool_call_log": tool_call_log,
            "runtime_action_log": [
                {"index": 1, "action": "stable_ai_scenario_matrix", "evidence": "deterministic fake provider suite"},
                {"index": 2, "action": "harness_capability_coverage", "evidence": "runtime module map coverage"},
                {"index": 3, "action": "knowledge_fabric.evaluate", "evidence": "context governance activated and blocked namespaces"},
                {"index": 4, "action": "reality_context_eval.evaluate", "evidence": "RecallReport and ContextEnvelope scenario matrix generated"}
            ]
        },
        "tool_call_details": tool_call_details,
        "result_package_dir": null
    });
    let gate = evaluate_report_gate(&report);
    report["report_gate"] = serde_json::to_value(&gate).map_err(|error| error.to_string())?;
    report["status"] = Value::String(gate.status.clone());
    let summary = store.write_report(options.level.as_str(), &mut report, &stable_ai)?;
    let record = HarnessEvalRunRecord {
        run_id,
        level: summary.level.clone(),
        status: HarnessEvalRunStatus::Completed.as_str().to_string(),
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
        message: "harness eval smoke completed through library runner".to_string(),
    };
    store.append_run(&record)?;
    Ok(record)
}

struct FullRealToolEval {
    report: RealToolScenarioReport,
    tool_details: Vec<ToolCallDetail>,
    usage: crate::HarnessEvalUsageSummary,
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
    let result = tools::execute_tool(name, &input);
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
                .expect_mode(mode_for_scenario(item.kind))
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
            strategy_mode: mode_for_scenario(item.kind),
            finalization_blocked: item.kind == E2eScenarioKind::Recovery,
            regression_allowed: item.kind != E2eScenarioKind::Recovery,
            has_workgraph: matches!(
                item.kind,
                E2eScenarioKind::ComplexPlan | E2eScenarioKind::TeamParallel
            ),
            workgraph_quality_ok: item.kind != E2eScenarioKind::SimpleOnce,
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

fn mode_for_scenario(kind: E2eScenarioKind) -> ExecutionMode {
    match kind {
        E2eScenarioKind::SimpleOnce => ExecutionMode::DirectAnswer,
        E2eScenarioKind::TeamParallel => ExecutionMode::SupervisorSubagents,
        E2eScenarioKind::GovernedConnector => ExecutionMode::RiskGate,
        E2eScenarioKind::ComplexPlan
        | E2eScenarioKind::RealityMemory
        | E2eScenarioKind::ToolLsp
        | E2eScenarioKind::Recovery => ExecutionMode::PlanExecute,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            .any(|path| path.ends_with("run-evidence/tool-call-1.json")));
        assert!(detail
            .artifacts
            .iter()
            .any(|path| path.ends_with("evidence/reality-context-eval.json")));
        assert!(
            detail.report["real_tool_scenarios"]["scenarios"][0]["changed_files"]
                .as_array()
                .is_some_and(Vec::is_empty)
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
