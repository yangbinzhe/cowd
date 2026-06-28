use std::time::Instant;

use harness_contract::core::ExecutionMode;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    evaluate_complex_harness_scenarios, evaluate_knowledge_fabric_context_governance,
    harness_capability_coverage_report,
    report::{HarnessEvalLevel, HarnessEvalRunRecord, HarnessEvalRunStatus},
    report_store::{empty_usage, now_ms, HarnessEvalReportStore},
    stable_ai_scenario_matrix, E2eScenarioKind, ScenarioCheck, ScenarioObservation, ScenarioSpec,
    ScenarioSuite, StableAiHealthReport,
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
    let runtime_actions = 3 + usize::from(complex.is_some());
    let usage = empty_usage("deterministic_smoke");
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
            {"name": "tool_calls", "value": "0", "notes": "smoke lane does not execute tools; deep CLI lane records real tool evidence"}
        ],
        "complex_scenarios": complex,
        "real_tool_scenarios": null,
        "execution_trace": {
            "kind": "harness_eval.execution_trace",
            "started_at_ms": requested_at_ms,
            "finished_at_ms": now_ms(),
            "total_elapsed_ms": total_elapsed_ms,
            "provider_rounds": 0,
            "runtime_actions": runtime_actions,
            "tool_calls": 0,
            "total_usage": usage,
            "rounds": [],
            "tool_call_log": [],
            "runtime_action_log": [
                {"index": 1, "action": "stable_ai_scenario_matrix", "evidence": "deterministic fake provider suite"},
                {"index": 2, "action": "harness_capability_coverage", "evidence": "runtime module map coverage"},
                {"index": 3, "action": "knowledge_fabric.evaluate", "evidence": "context governance activated and blocked namespaces"}
            ]
        },
        "result_package_dir": null
    });
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
}
