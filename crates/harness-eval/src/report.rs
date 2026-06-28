use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::{ComplexHarnessScenarioReport, E2eScenarioMatrixItem, StableAiHealthReport};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessEvalLevel {
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
            "deep" | "real" => Some(Self::Deep),
            _ => None,
        }
    }
}

impl Default for HarnessEvalLevel {
    fn default() -> Self {
        Self::Quick
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessEvalRunStatus {
    Completed,
    Gated,
    Cancelled,
    Failed,
}

impl HarnessEvalRunStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
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
