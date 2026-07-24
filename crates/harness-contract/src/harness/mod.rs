//! Harness execution contracts for Cowd AI turns.
//!
//! A harness describes and receipts an executor. This crate intentionally does
//! not spawn external processes.

use crate::agent::{AgentExecutorKind, AgentSpec};
use crate::context::ContextEpoch;
use crate::strategy::StrategyDecision;
use crate::tool::GovernedToolPlanProjection;
use crate::verification::VerificationReport;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessManifest {
    pub id: String,
    pub name: String,
    pub executor_kind: AgentExecutorKind,
    pub capabilities: Vec<String>,
    pub supported_tools: Vec<String>,
    pub supports_streaming: bool,
    pub supports_cancel: bool,
    pub requires_sandbox: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HarnessTurnInput {
    pub agent_spec: AgentSpec,
    pub strategy: StrategyDecision,
    pub context_epoch: ContextEpoch,
    pub governed_tool_plans: Vec<GovernedToolPlanProjection>,
    pub policy_context: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessTurnReceipt {
    pub id: String,
    pub harness_id: String,
    pub agent_spec_id: String,
    pub strategy_pattern: String,
    pub context_epoch_id: String,
    pub governed_tool_plan_ids: Vec<String>,
    pub verification_can_finalize: bool,
    pub policy_receipts: Vec<String>,
    pub output_summary: String,
}

pub trait HarnessAdapter {
    fn manifest(&self) -> HarnessManifest;
    fn prepare_session(&self, agent_spec: &AgentSpec) -> Result<(), String>;
    fn execute_turn(
        &self,
        input: HarnessTurnInput,
        verification: &VerificationReport,
        output_summary: impl Into<String>,
    ) -> Result<HarnessTurnReceipt, String>;
    fn cancel(&self, _receipt_id: &str) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct CowdNativeHarness;

impl HarnessAdapter for CowdNativeHarness {
    fn manifest(&self) -> HarnessManifest {
        HarnessManifest {
            id: "cowd-native".to_string(),
            name: "Cowd Native Runtime".to_string(),
            executor_kind: AgentExecutorKind::CowdNative,
            capabilities: vec![
                "runtime_ai_kernel".to_string(),
                "governed_tool_execution".to_string(),
                "verification".to_string(),
                "growth_trace".to_string(),
            ],
            supported_tools: vec!["runtime_registered_tools".to_string()],
            supports_streaming: true,
            supports_cancel: true,
            requires_sandbox: false,
        }
    }

    fn prepare_session(&self, agent_spec: &AgentSpec) -> Result<(), String> {
        agent_spec.validate().map_err(|error| error.to_string())
    }

    fn execute_turn(
        &self,
        input: HarnessTurnInput,
        verification: &VerificationReport,
        output_summary: impl Into<String>,
    ) -> Result<HarnessTurnReceipt, String> {
        self.prepare_session(&input.agent_spec)?;
        let manifest = self.manifest();
        Ok(HarnessTurnReceipt {
            id: format!("harness-receipt-{}", uuid::Uuid::new_v4()),
            harness_id: manifest.id,
            agent_spec_id: input.agent_spec.id,
            strategy_pattern: input.strategy.pattern.as_str().to_string(),
            context_epoch_id: input.context_epoch.epoch_id,
            governed_tool_plan_ids: input
                .governed_tool_plans
                .into_iter()
                .map(|plan| plan.plan_id)
                .collect(),
            verification_can_finalize: verification.can_finalize,
            policy_receipts: input.policy_context,
            output_summary: output_summary.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cowd_native_receipts_turn() {
        let harness = CowdNativeHarness;
        let spec = AgentSpec::worker();
        harness.prepare_session(&spec).expect("valid spec");
    }
}
