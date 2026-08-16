//! Effect-derived acceptance and terminal-fact contracts.
//!
//! Acceptance verdicts and committed execution facts are separate carriers so
//! a failed verdict can never erase already-committed effects and evidence.

use serde::{Deserialize, Serialize};

/// Effect-derived acceptance evaluation with durable digest identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptanceEvaluation {
    pub evaluator_revision: u64,
    pub contract_digest: String,
    pub receipt_set_digest: String,
    pub derived_obligations: Vec<String>,
    pub verdict: String,
}

/// Terminal facts separated from the verdict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentTerminalFacts {
    pub agent_id: String,
    pub run_id: String,
    pub committed_effects: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub artifact_refs: Vec<String>,
    pub digest: String,
}
