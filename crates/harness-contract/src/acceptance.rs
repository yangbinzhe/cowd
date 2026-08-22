//! Effect-derived acceptance and terminal-fact contracts.
//!
//! Acceptance verdicts and committed execution facts are separate carriers so
//! a failed verdict can never erase already-committed effects and evidence.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Typed acceptance verdict. Runtime derives it from committed effects and
/// evidence; a verdict never mutates the facts that produced it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AcceptanceVerdict {
    Satisfied,
    Unsatisfied,
    FrameworkInvalid,
    Unresolved,
}

/// Typed terminal-fact kinds consumed by dependency predicates. They are
/// Runtime-attested facts, never presentation booleans.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TerminalFactKind {
    CommittedEffect,
    ObservedEvidence,
    Artifact,
    AcceptanceVerdict,
}

/// Effect-derived acceptance evaluation with durable digest identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct AcceptanceEvaluation {
    pub evaluator_revision: u64,
    pub contract_digest: String,
    pub receipt_set_digest: String,
    pub derived_obligations: Vec<String>,
    pub verdict: AcceptanceVerdict,
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
