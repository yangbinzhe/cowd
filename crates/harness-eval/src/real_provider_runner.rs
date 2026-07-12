use std::time::Instant;

use serde_json::{json, Value};

use crate::{
    provider_rounds::{build_provider_round_detail, provider_response_text, summarize_text},
    report::{HarnessEvalUsageSummary, ProviderRoundDetail, ProviderRoundSummary},
    HarnessEvalRunnerOptions,
};

#[derive(Debug, Clone)]
pub(crate) struct RealProviderEvalReport {
    pub status: String,
    pub provider_rounds: Vec<ProviderRoundSummary>,
    pub provider_round_details: Vec<ProviderRoundDetail>,
    pub usage: HarnessEvalUsageSummary,
    pub reviewer_markdown: Option<String>,
    pub error: Option<String>,
}

impl RealProviderEvalReport {
    pub(crate) fn blocked(reason: impl Into<String>) -> Self {
        Self {
            status: "blocked".to_string(),
            provider_rounds: Vec::new(),
            provider_round_details: Vec::new(),
            usage: HarnessEvalUsageSummary {
                usage_source: "real_provider_blocked".to_string(),
                ..HarnessEvalUsageSummary::default()
            },
            reviewer_markdown: None,
            error: Some(reason.into()),
        }
    }
}

pub(crate) fn run_deep_real_provider_review(
    options: &HarnessEvalRunnerOptions,
    report_seed: &Value,
) -> RealProviderEvalReport {
    if std::env::var("COWD_EVAL_REAL_MODEL").ok().as_deref() != Some("1") {
        return RealProviderEvalReport::blocked(
            "COWD_EVAL_REAL_MODEL=1 is required before harness-eval consumes provider tokens",
        );
    }

    let Some(model) = options
        .provider
        .clone()
        .or_else(|| std::env::var("COWD_EVAL_MODEL").ok())
        .filter(|value| !value.trim().is_empty())
    else {
        return RealProviderEvalReport::blocked(
            "deep-real requires --provider or COWD_EVAL_MODEL to select the model",
        );
    };

    let started = Instant::now();
    let prompt = reviewer_prompt(report_seed, &model);
    let request_json = json!({
        "model": model,
        "system_prompt": [
            "You are the AI reviewer for Cowd AI Harness deep-real evaluation.",
            "Generate concise but evidence-based Markdown. Do not claim capabilities not supported by the report seed."
        ],
        "user_prompt_summary": summarize_text(&prompt, 1200)
    });
    let request = runtime::ApiRequest {
        system_prompt: vec![
            "You are the AI reviewer for Cowd AI Harness deep-real evaluation.".to_string(),
            "Generate concise but evidence-based Markdown. Do not claim capabilities not supported by the report seed.".to_string(),
        ],
        messages: vec![runtime::ConversationMessage {
            role: runtime::MessageRole::User,
            blocks: vec![runtime::ContentBlock::Text { text: prompt }],
            usage: None,
        }],
        model: model.clone(),
    };

    let provider_registry = std::env::current_dir()
        .map_err(|error| error.to_string())
        .and_then(|cwd| {
            runtime::ConfigLoader::default_for(cwd)
                .load()
                .map_err(|error| error.to_string())
        })
        .and_then(|config| {
            runtime::ProviderRegistry::new(config.providers().clone())
                .map(std::sync::Arc::new)
                .map_err(|rejected| rejected.diagnostics.errors.join("; "))
        });
    let provider_registry = match provider_registry {
        Ok(registry) => registry,
        Err(error) => {
            return RealProviderEvalReport::blocked(format!(
                "provider registry initialization failed: {error}"
            ));
        }
    };

    let events =
        match runtime::ProviderRuntimeClient::new(provider_registry, model.clone(), Vec::new())
            .and_then(|mut client| {
                use runtime::ApiClient;
                client
                    .stream_collect(request)
                    .map_err(|error| error.to_string())
            }) {
            Ok(events) => events,
            Err(error) => {
                return RealProviderEvalReport::blocked(format!("provider round failed: {error}"));
            }
        };
    let detail = build_provider_round_detail(
        1,
        "ai_reviewer_full_analysis",
        model,
        request_json,
        &events,
        started.elapsed().as_millis(),
        "provider-rounds/001-round.json",
    );
    let reviewer_markdown = provider_response_text(&events);
    let mut usage = HarnessEvalUsageSummary {
        input_tokens: detail.summary.usage.input_tokens,
        output_tokens: detail.summary.usage.output_tokens,
        cache_creation_input_tokens: detail.summary.usage.cache_creation_input_tokens,
        cache_read_input_tokens: detail.summary.usage.cache_read_input_tokens,
        total_tokens: detail.summary.usage.total_tokens,
        usage_source: detail.summary.usage.usage_source.clone(),
    };
    if usage.total_tokens == 0 {
        usage.usage_source = "provider_event_missing_usage".to_string();
    }
    RealProviderEvalReport {
        status: detail.summary.status.clone(),
        provider_rounds: vec![detail.summary.clone()],
        provider_round_details: vec![detail],
        usage,
        reviewer_markdown: (!reviewer_markdown.trim().is_empty()).then_some(reviewer_markdown),
        error: None,
    }
}

fn reviewer_prompt(report_seed: &Value, model: &str) -> String {
    let mut review_seed = reviewer_evidence_summary(report_seed);
    review_seed["execution_trace"]["provider_rounds"] = json!(1);
    review_seed["execution_trace"]["rounds"] = json!([{
        "round_index": 1,
        "name": "ai_reviewer_full_analysis",
        "model": model,
        "status": "running",
        "note": "this reviewer request is the real provider evidence round"
    }]);
    review_seed["ai_reviewer"] = json!({
        "status": "running",
        "report_source": "provider_round",
        "provider_rounds": 1,
        "model": model
    });
    if let Some(metrics) = review_seed["metrics"].as_array_mut() {
        for metric in metrics {
            if metric["name"].as_str() == Some("provider_rounds") {
                metric["value"] = json!("1");
                metric["notes"] = json!("this reviewer request is provider round 1");
            }
        }
    }
    let seed = serde_json::to_string_pretty(&review_seed).unwrap_or_else(|_| "{}".to_string());
    format!(
        r#"# Task
Generate `full-analysis-report.md` for this Cowd harness eval package.

## Required behavior
- Distinguish deterministic checks, real local tool evidence, and real provider rounds.
- Evaluate whether the evidence truly proves the harness capability.
- Summarize request/response evidence; do not paste full raw payloads.
- Include gaps and risks honestly.
- Output Markdown only.

## Provider round facts
- This request is real provider round 1 for model `{model}`.
- The seed report below has been normalized for review: `execution_trace.provider_rounds=1`.
- In the generated report, use `provider_rounds=1` and explain that the reviewer response itself is the real provider evidence round.

## Report seed
```json
{seed}
```
"#
    )
}

/// Builds the bounded evidence handoff for the optional reviewer model.
///
/// Raw Gateway request/response traces remain in the result package for
/// forensic inspection. Sending them back through a second model call is both
/// wasteful and misleading: a busy live scenario can make the reviewer input
/// larger than the execution it is assessing. The reviewer receives only the
/// verdicts, identities, usage, and trace artifact references required for an
/// evidence-based assessment.
fn reviewer_evidence_summary(report: &Value) -> Value {
    let scenarios = report
        .get("scenarios")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| {
                    json!({
                        "capability": item.get("capability"),
                        "status": item.get("status"),
                        "evidence": item.get("evidence"),
                        "notes": item.get("notes"),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let execution_trace = report
        .get("execution_trace")
        .map(|trace| {
            json!({
                "total_elapsed_ms": trace.get("total_elapsed_ms"),
                "provider_rounds": trace.get("provider_rounds"),
                "runtime_actions": trace.get("runtime_actions"),
                "tool_calls": trace.get("tool_calls"),
                "total_usage": trace.get("total_usage"),
                "rounds": trace.get("rounds"),
            })
        })
        .unwrap_or(Value::Null);
    json!({
        "kind": report.get("kind"),
        "level": report.get("level"),
        "status": report.get("status"),
        "provider": report.get("provider"),
        "budget": report.get("budget"),
        "authorized_real_model": report.get("authorized_real_model"),
        "metrics": report.get("metrics"),
        "scenarios": scenarios,
        "stable_ai": {
            "status": report.pointer("/stable_ai/status"),
            "coverage": report.pointer("/stable_ai/coverage"),
        },
        "reality_context_eval": {
            "passed": report.pointer("/reality_context_eval/passed"),
            "failed": report.pointer("/reality_context_eval/failed"),
            "total": report.pointer("/reality_context_eval/total"),
        },
        "mission_runtime_collaboration": {
            "status": report.pointer("/mission_runtime_collaboration/status"),
            "selected_strategy": report.pointer("/mission_runtime_collaboration/selected_strategy"),
            "terminal_evidence": report.pointer("/mission_runtime_collaboration/terminal_evidence"),
        },
        "next_gen_harness_closure": {
            "status": report.pointer("/next_gen_harness_closure/status"),
            "passed": report.pointer("/next_gen_harness_closure/passed"),
            "total": report.pointer("/next_gen_harness_closure/total"),
            "missing_capabilities": report.pointer("/next_gen_harness_closure/missing_capabilities"),
        },
        "complex_scenarios": {
            "passed": report.pointer("/complex_scenarios/passed"),
            "failed": report.pointer("/complex_scenarios/failed"),
            "total": report.pointer("/complex_scenarios/total"),
            "average_score": report.pointer("/complex_scenarios/average_score"),
        },
        "live_gateway_scenarios": report.get("live_gateway_scenarios"),
        "execution_trace": execution_trace,
        "report_gate": report.get("report_gate"),
        "evidence_package": {
            "result_package_dir": report.get("result_package_dir"),
            "raw_live_trace": "live-scenarios/<scenario>.json",
            "raw_provider_round": "provider-rounds/<round>.json",
            "rule": "Raw payloads are retained in the package and deliberately excluded from this reviewer request."
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn reviewer_prompt_overrides_pre_provider_round_seed() {
        let prompt = reviewer_prompt(
            &json!({
                "execution_trace": {"provider_rounds": 0},
                "metrics": [{"name": "provider_rounds", "value": "0"}],
                "provider": "deepseek-v4-flash"
            }),
            "deepseek-v4-flash",
        );

        assert!(prompt.contains("This request is real provider round 1"));
        assert!(prompt.contains("`execution_trace.provider_rounds=1`"));
        assert!(prompt.contains("use `provider_rounds=1`"));
        assert!(prompt.contains(r#""provider_rounds": 1"#));
        assert!(!prompt.contains(r#""provider_rounds": 0"#));
        assert!(prompt.contains(r#""value": "1"#));
        assert!(!prompt.contains(r#""value": "0"#));
    }

    #[test]
    fn reviewer_prompt_excludes_unbounded_live_http_trace_payloads() {
        let raw_payload = "x".repeat(200_000);
        let prompt = reviewer_prompt(
            &json!({
                "kind": "mission_harness.eval_report",
                "level": "deep",
                "scenarios": [{"capability": "live", "status": "passed", "evidence": "3/3"}],
                "live_gateway_scenarios": {"status": "passed", "scenarios": [{"trace_artifact": "live-scenarios/live.json"}]},
                "live_gateway_scenario_details": {"scenarios": [{"trace": raw_payload}]},
                "execution_trace": {"provider_rounds": 0, "total_usage": {"total_tokens": 0}}
            }),
            "deepseek-v4-flash",
        );

        assert!(prompt.contains("live-scenarios/live.json"));
        assert!(!prompt.contains(&"x".repeat(1_000)));
        assert!(
            prompt.len() < 30_000,
            "reviewer handoff must remain bounded"
        );
    }
}
