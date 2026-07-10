//! Per-turn progress supervisor.
//!
//! The supervisor detects repeated evidence-gathering loops and asks the model
//! to re-plan. It is a guidance mechanism, not a hard resource gate.

use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use harness_contract::core::ExecutionMode;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallFingerprint {
    pub tool_name: String,
    pub target: String,
    pub range: Option<(u64, u64)>,
    pub input_hash: u64,
    pub output_hash: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolProgressObservation {
    pub fingerprint: ToolCallFingerprint,
    pub is_error: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallRecord {
    pub tool_name: String,
    pub target: String,
    pub input_hash: u64,
    pub output_hash: u64,
    pub produced_new_evidence: bool,
    pub duplicate_call: bool,
    pub is_error: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolCallLedger {
    pub calls: Vec<ToolCallRecord>,
    pub duplicate_count: usize,
    pub no_progress_count: usize,
    pub evidence_count: usize,
}

impl ToolCallLedger {
    #[must_use]
    pub fn compact_summary(&self) -> String {
        format!(
            "tool_calls={}, evidence={}, duplicates={}, no_progress={}",
            self.calls.len(),
            self.evidence_count,
            self.duplicate_count,
            self.no_progress_count
        )
    }

    #[must_use]
    pub fn evidence_targets(&self) -> Vec<String> {
        let mut targets = Vec::new();
        let mut seen = HashSet::new();
        for call in &self.calls {
            if call.produced_new_evidence && seen.insert(call.target.clone()) {
                targets.push(format!("{} -> {}", call.tool_name, call.target));
            }
        }
        targets
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SupervisorDecision {
    Continue,
    Nudge {
        reason: String,
        prompt: String,
        reason_code: String,
        recommended_mode: ExecutionMode,
        recommended_action: String,
    },
    Replan {
        reason: String,
        prompt: String,
        reason_code: String,
        recommended_mode: ExecutionMode,
        recommended_action: String,
    },
    FallbackAnswer {
        reason: String,
        prompt: String,
        reason_code: String,
        recommended_mode: ExecutionMode,
        recommended_action: String,
    },
}

impl SupervisorDecision {
    #[must_use]
    pub fn should_inject(&self) -> bool {
        !matches!(self, Self::Continue)
    }

    #[must_use]
    pub fn prompt(&self) -> Option<&str> {
        match self {
            Self::Continue => None,
            Self::Nudge { prompt, .. }
            | Self::Replan { prompt, .. }
            | Self::FallbackAnswer { prompt, .. } => Some(prompt),
        }
    }

    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Continue => "continue",
            Self::Nudge { .. } => "nudge",
            Self::Replan { .. } => "replan",
            Self::FallbackAnswer { .. } => "fallback_answer",
        }
    }

    #[must_use]
    pub fn reason(&self) -> Option<&str> {
        match self {
            Self::Continue => None,
            Self::Nudge { reason, .. }
            | Self::Replan { reason, .. }
            | Self::FallbackAnswer { reason, .. } => Some(reason),
        }
    }

    #[must_use]
    pub fn recommended_action(&self) -> Option<&str> {
        match self {
            Self::Continue => None,
            Self::Nudge {
                recommended_action, ..
            }
            | Self::Replan {
                recommended_action, ..
            }
            | Self::FallbackAnswer {
                recommended_action, ..
            } => Some(recommended_action),
        }
    }

    #[must_use]
    pub fn recommended_mode(&self) -> Option<ExecutionMode> {
        match self {
            Self::Continue => None,
            Self::Nudge {
                recommended_mode, ..
            }
            | Self::Replan {
                recommended_mode, ..
            }
            | Self::FallbackAnswer {
                recommended_mode, ..
            } => Some(*recommended_mode),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TurnSupervisor {
    same_target_counts: HashMap<String, usize>,
    exact_counts: HashMap<String, usize>,
    observed_output_hashes: HashSet<u64>,
    injected_decisions: usize,
    total_observations: usize,
    ledger: ToolCallLedger,
}

impl TurnSupervisor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            same_target_counts: HashMap::new(),
            exact_counts: HashMap::new(),
            observed_output_hashes: HashSet::new(),
            injected_decisions: 0,
            total_observations: 0,
            ledger: ToolCallLedger::default(),
        }
    }

    pub fn observe_tool_result(
        &mut self,
        tool_name: &str,
        input: &str,
        output: &str,
        is_error: bool,
    ) -> (ToolProgressObservation, SupervisorDecision) {
        let fingerprint = fingerprint_tool_call(tool_name, input, output);
        self.total_observations = self.total_observations.saturating_add(1);
        let target_key = format!("{}:{}", fingerprint.tool_name, fingerprint.target);
        let exact_key = format!(
            "{}:{}:{}",
            target_key,
            fingerprint.input_hash,
            fingerprint
                .range
                .map_or_else(|| "none".to_string(), |(a, b)| format!("{a}-{b}"))
        );
        let target_count = increment(&mut self.same_target_counts, target_key);
        let exact_count = increment(&mut self.exact_counts, exact_key);
        let novel_output = self.observed_output_hashes.insert(fingerprint.output_hash);
        let duplicate_call = exact_count > 1;
        if duplicate_call {
            self.ledger.duplicate_count = self.ledger.duplicate_count.saturating_add(1);
        }
        if novel_output && !is_error {
            self.ledger.evidence_count = self.ledger.evidence_count.saturating_add(1);
        } else if !is_error {
            self.ledger.no_progress_count = self.ledger.no_progress_count.saturating_add(1);
        }
        self.ledger.calls.push(ToolCallRecord {
            tool_name: fingerprint.tool_name.clone(),
            target: fingerprint.target.clone(),
            input_hash: fingerprint.input_hash,
            output_hash: fingerprint.output_hash,
            produced_new_evidence: novel_output && !is_error,
            duplicate_call,
            is_error,
        });
        let observation = ToolProgressObservation {
            fingerprint,
            is_error,
        };

        let decision = self.decide(target_count, exact_count, novel_output, is_error);
        if decision.should_inject() {
            self.injected_decisions = self.injected_decisions.saturating_add(1);
        }
        (observation, decision)
    }

    #[must_use]
    pub fn ledger(&self) -> &ToolCallLedger {
        &self.ledger
    }

    #[must_use]
    pub fn partial_answer_text(&self, reason: &str) -> String {
        let mut lines = vec![
            "Runtime supervisor produced a partial answer instead of dropping the turn."
                .to_string(),
            format!("Reason: {reason}"),
            format!("Tool governance: {}", self.ledger.compact_summary()),
        ];
        let targets = self.ledger.evidence_targets();
        if targets.is_empty() {
            lines.push("Checked evidence targets: none recorded.".to_string());
        } else {
            lines.push("Checked evidence targets:".to_string());
            for target in targets.into_iter().take(12) {
                lines.push(format!("- {target}"));
            }
        }
        lines.push(
            "Current status: the runtime stopped repeated or over-budget tool exploration and preserved the evidence trail for review.".to_string(),
        );
        lines.push(
            "Next step: synthesize from the checked evidence or change strategy before requesting more tools.".to_string(),
        );
        lines.join("\n")
    }

    fn decide(
        &self,
        target_count: usize,
        exact_count: usize,
        novel_output: bool,
        is_error: bool,
    ) -> SupervisorDecision {
        if self.total_observations >= 16 && self.injected_decisions >= 1 {
            return SupervisorDecision::FallbackAnswer {
                reason: "many tool calls after repeated replanning guidance".to_string(),
                prompt: fallback_prompt(),
                reason_code: "replan_budget_exhausted".to_string(),
                recommended_mode: ExecutionMode::DirectAnswer,
                recommended_action: "answer_with_checked_evidence".to_string(),
            };
        }

        if target_count >= 8 || exact_count >= 5 {
            if self.injected_decisions >= 1 {
                return SupervisorDecision::FallbackAnswer {
                    reason: format!(
                        "repeated evidence target continued after guidance target_count={target_count} exact_count={exact_count}"
                    ),
                    prompt: fallback_prompt(),
                    reason_code: "repeated_evidence_target_after_guidance".to_string(),
                    recommended_mode: ExecutionMode::DirectAnswer,
                    recommended_action: "answer_with_checked_evidence".to_string(),
                };
            }
            return SupervisorDecision::Replan {
                reason: format!(
                    "repeated evidence target detected target_count={target_count} exact_count={exact_count}"
                ),
                prompt: replan_prompt(),
                reason_code: "repeated_evidence_target".to_string(),
                recommended_mode: ExecutionMode::ParallelReadFanout,
                recommended_action: "runtime_orchestrate(request_parallel_tools)".to_string(),
            };
        }

        if is_error && (target_count >= 4 || exact_count >= 3) {
            return SupervisorDecision::Nudge {
                reason: format!(
                    "repeated tool failures detected target_count={target_count} exact_count={exact_count}"
                ),
                prompt: nudge_prompt(),
                reason_code: "repeated_tool_failure".to_string(),
                recommended_mode: ExecutionMode::ReflexionRetry,
                recommended_action: "runtime_orchestrate(request_reflexion_retry)".to_string(),
            };
        }

        if is_error {
            return SupervisorDecision::Continue;
        }

        if (target_count >= 4 || exact_count >= 3) && !novel_output {
            if self.injected_decisions >= 1 {
                return SupervisorDecision::FallbackAnswer {
                    reason: format!(
                        "low-novelty repeated tool usage continued after guidance target_count={target_count} exact_count={exact_count}"
                    ),
                    prompt: fallback_prompt(),
                    reason_code: "low_novelty_tool_loop_after_guidance".to_string(),
                    recommended_mode: ExecutionMode::DirectAnswer,
                    recommended_action: "answer_with_checked_evidence".to_string(),
                };
            }
            return SupervisorDecision::Nudge {
                reason: format!(
                    "low-novelty repeated tool usage target_count={target_count} exact_count={exact_count}"
                ),
                prompt: nudge_prompt(),
                reason_code: "low_novelty_tool_loop".to_string(),
                recommended_mode: ExecutionMode::ReflexionRetry,
                recommended_action: "runtime_orchestrate(request_reflexion_retry)".to_string(),
            };
        }

        SupervisorDecision::Continue
    }
}

impl Default for TurnSupervisor {
    fn default() -> Self {
        Self::new()
    }
}

#[must_use]
pub fn explicit_tool_round_budget(input: &str) -> Option<usize> {
    let lower = input.to_lowercase();
    let has_tool_scope = lower.contains("tool") || input.contains("工具");
    let has_budget_marker = input.contains("最多")
        || input.contains("不超过")
        || input.contains("不要超过")
        || lower.contains("at most")
        || lower.contains("no more than")
        || lower.contains("maximum")
        || lower.contains("max ");
    if !has_tool_scope || !has_budget_marker {
        return None;
    }

    for (marker, value) in [
        ("一", 1),
        ("1", 1),
        ("one", 1),
        ("两", 2),
        ("二", 2),
        ("2", 2),
        ("two", 2),
        ("三", 3),
        ("3", 3),
        ("three", 3),
        ("四", 4),
        ("4", 4),
        ("four", 4),
        ("五", 5),
        ("5", 5),
        ("five", 5),
    ] {
        if lower.contains(marker) || input.contains(marker) {
            return Some(value);
        }
    }
    None
}

#[must_use]
pub fn fingerprint_tool_call(tool_name: &str, input: &str, output: &str) -> ToolCallFingerprint {
    let value = serde_json::from_str::<Value>(input).unwrap_or(Value::Null);
    let normalized = tool_name.trim().replace('-', "_").to_ascii_lowercase();
    let target = target_for(&normalized, &value);
    let range = range_for(&value);
    ToolCallFingerprint {
        tool_name: normalized,
        target,
        range,
        input_hash: stable_hash(input),
        output_hash: stable_hash(&collapse_output(output)),
    }
}

fn target_for(tool_name: &str, input: &Value) -> String {
    match tool_name {
        "read_file" => input
            .get("path")
            .and_then(Value::as_str)
            .map(|path| format!("file:{}", normalize_path(path)))
            .unwrap_or_else(|| "file:unknown".to_string()),
        "grep_search" => {
            let pattern = input.get("pattern").and_then(Value::as_str).unwrap_or("");
            let path = input.get("path").and_then(Value::as_str).unwrap_or(".");
            let glob = input.get("glob").and_then(Value::as_str).unwrap_or("*");
            format!("grep:{}:{}:{}", normalize_path(path), glob, pattern)
        }
        "glob_search" => {
            let pattern = input.get("pattern").and_then(Value::as_str).unwrap_or("*");
            let path = input.get("path").and_then(Value::as_str).unwrap_or(".");
            format!("glob:{}:{}", normalize_path(path), pattern)
        }
        "execute_code" => {
            let language = input.get("language").and_then(Value::as_str).unwrap_or("");
            let code = input.get("code").and_then(Value::as_str).unwrap_or("");
            format!(
                "execute_code:{language}:{}",
                code.lines().next().unwrap_or("")
            )
        }
        "read_many" | "grep_many" | "glob_many" | "tool_batch_readonly" => {
            format!("batch:{tool_name}")
        }
        other => format!("tool:{other}"),
    }
}

fn range_for(input: &Value) -> Option<(u64, u64)> {
    let offset = input.get("offset").and_then(Value::as_u64)?;
    let limit = input.get("limit").and_then(Value::as_u64).unwrap_or(0);
    Some((offset, offset.saturating_add(limit)))
}

fn normalize_path(path: &str) -> String {
    path.trim().replace('\\', "/")
}

fn increment(map: &mut HashMap<String, usize>, key: String) -> usize {
    let entry = map.entry(key).or_insert(0);
    *entry = entry.saturating_add(1);
    *entry
}

fn stable_hash(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

fn collapse_output(output: &str) -> String {
    let semantic_output = output
        .split_once("Summary:")
        .map_or(output, |(_, summary)| summary);
    semantic_output
        .split_whitespace()
        .filter(|token| {
            !token
                .trim_matches(|ch: char| ch.is_ascii_punctuation())
                .starts_with("tool://")
        })
        .take(160)
        .collect::<Vec<_>>()
        .join(" ")
}

fn nudge_prompt() -> String {
    "Runtime supervisor: the recent tool path is repeating with low new evidence. Prefer a more efficient strategy now: use `runtime_capabilities`, `runtime_orchestrate(request_reflexion_retry)`, `workspace_snapshot`, `read_many`, `grep_many`, or `tool_batch_readonly`; if the evidence is already enough, give a staged answer with checked facts and remaining risks.".to_string()
}

fn replan_prompt() -> String {
    "Runtime supervisor: repeated evidence gathering is no longer efficient. Stop range-by-range probing and re-plan. Choose one: call `runtime_capabilities(detail=orchestration_options)` for a read-only plan, call `runtime_orchestrate(request_parallel_tools)` for stateful batch/DAG execution, request runtime-owned subagent/team collaboration for independent analysis, or answer from current evidence with explicit residual risks.".to_string()
}

fn fallback_prompt() -> String {
    "Runtime supervisor: multiple replanning attempts did not produce an efficient path. Do not continue collecting similar evidence. Produce an honest staged answer now: what was checked, current conclusion, uncertainty, and the next concrete step.".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_same_file_low_novelty_triggers_nudge_then_replan() {
        let mut supervisor = TurnSupervisor::new();
        let mut decisions = Vec::new();
        for index in 0..8 {
            let (_, decision) = supervisor.observe_tool_result(
                "read_file",
                &format!(
                    r#"{{"path":"README.md","offset":{},"limit":50}}"#,
                    index * 10
                ),
                "same summary",
                false,
            );
            decisions.push(decision.kind());
        }

        assert!(decisions.contains(&"nudge"));
        assert!(decisions.contains(&"fallback_answer"));
        assert!(!decisions
            .iter()
            .skip(4)
            .any(|decision| decision == &"replan"));
    }

    #[test]
    fn varied_batch_tools_continue() {
        let mut supervisor = TurnSupervisor::new();
        for (tool, input, output) in [
            ("workspace_snapshot", "{}", "workspace"),
            ("grep_many", r#"{"searches":[]}"#, "grep"),
            ("read_many", r#"{"files":[]}"#, "reads"),
        ] {
            let (_, decision) = supervisor.observe_tool_result(tool, input, output, false);
            assert_eq!(decision, SupervisorDecision::Continue);
        }
    }

    #[test]
    fn raw_evidence_refs_do_not_create_false_novelty() {
        let mut supervisor = TurnSupervisor::new();
        let mut decisions = Vec::new();
        for index in 0..4 {
            let (_, decision) = supervisor.observe_tool_result(
                "read_file",
                r#"{"path":"README.md","offset":0,"limit":80}"#,
                &format!(
                    "Tool `read_file` completed. Raw evidence ref: tool://tool-raw-call-{index}. Summary: same README evidence"
                ),
                false,
            );
            decisions.push(decision.kind());
        }

        assert!(decisions.contains(&"nudge"));
    }

    #[test]
    fn ledger_tracks_duplicates_and_partial_answer_evidence() {
        let mut supervisor = TurnSupervisor::new();
        for _ in 0..3 {
            let _ = supervisor.observe_tool_result(
                "grep_search",
                r#"{"path":"crates/runtime","pattern":"RunModelTelemetry"}"#,
                "same telemetry evidence",
                false,
            );
        }

        assert_eq!(supervisor.ledger().calls.len(), 3);
        assert!(supervisor.ledger().duplicate_count >= 2);
        assert!(supervisor.ledger().no_progress_count >= 2);
        let partial = supervisor.partial_answer_text("test stop");
        assert!(partial.contains("partial answer"));
        assert!(partial.contains("duplicates="));
        assert!(partial.contains("grep_search"));
    }

    #[test]
    fn explicit_tool_round_budget_recognizes_user_constraints() {
        assert_eq!(
            explicit_tool_round_budget("最多两轮工具调用，然后直接回答。"),
            Some(2)
        );
        assert_eq!(
            explicit_tool_round_budget("Use at most two tool rounds before answering."),
            Some(2)
        );
        assert_eq!(
            explicit_tool_round_budget("请尽可能少用工具，但没有硬限制。"),
            None
        );
    }
}
