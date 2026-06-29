use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeExecutionReportSpec {
    pub required_sections: Vec<String>,
    pub required_artifacts: Vec<String>,
}

impl RuntimeExecutionReportSpec {
    #[must_use]
    pub fn scenario_eval() -> Self {
        Self {
            required_sections: vec![
                "scenario objective".to_string(),
                "model-visible capability context".to_string(),
                "runtime_capabilities usage".to_string(),
                "runtime_orchestrate usage".to_string(),
                "execution mode and template".to_string(),
                "token and latency".to_string(),
                "tool and agent counts".to_string(),
                "correctness and residual risks".to_string(),
            ],
            required_artifacts: vec![
                "report.md".to_string(),
                "summary.json".to_string(),
                "request-response/".to_string(),
                "tool-calls/".to_string(),
                "runtime-events/".to_string(),
                "evidence/".to_string(),
                "token-usage/".to_string(),
                "traces/".to_string(),
            ],
        }
    }
}
