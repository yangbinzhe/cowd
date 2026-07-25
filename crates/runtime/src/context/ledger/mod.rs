use std::collections::{BTreeMap, BTreeSet};

use harness_contract::context::{ContextComponentUsage, ContextLedgerProjection};
use serde::{Deserialize, Serialize};

/// Per-provider-attempt capacity contract. The hard cap is the only request
/// packing limit; subsystem budgets never reserve an arbitrary percentage of
/// a model window for the request itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestBudgetReport {
    pub model: String,
    pub context_window_tokens: u64,
    /// configured | user_registry | bundled | assumed | calibrated
    pub context_window_source: String,
    #[serde(default)]
    pub provider_max_output_tokens: u64,
    #[serde(default)]
    pub max_output_source: String,
    #[serde(default)]
    pub preferred_output_tokens: u64,
    #[serde(default)]
    pub output_floor_tokens: u64,
    pub requested_output_tokens: u64,
    pub protocol_overhead_tokens: u64,
    pub safety_margin_tokens: u64,
    pub hard_input_cap_tokens: u64,
    /// Mirrors `hard_input_cap_tokens` for consumers that need a target field.
    /// It is intentionally not a second, lower request limit.
    pub target_input_cap_tokens: u64,
    pub fixed_input_tokens: u64,
    #[serde(default)]
    pub required_input_tokens: u64,
    pub dynamic_input_tokens: u64,
    pub omitted_packet_ids: Vec<String>,
    #[serde(default)]
    pub omitted_packet_reasons: BTreeMap<String, String>,
    pub executable: bool,
}

impl RequestBudgetReport {
    #[must_use]
    pub fn for_attempt(
        model: impl Into<String>,
        context_window_tokens: u64,
        requested_output_tokens: u64,
        protocol_overhead_tokens: u64,
        safety_margin_tokens: u64,
        fixed_input_tokens: u64,
    ) -> Self {
        let hard_input_cap_tokens = context_window_tokens
            .saturating_sub(requested_output_tokens)
            .saturating_sub(protocol_overhead_tokens)
            .saturating_sub(safety_margin_tokens);
        Self {
            model: model.into(),
            context_window_tokens,
            context_window_source: "unknown".to_string(),
            provider_max_output_tokens: 0,
            max_output_source: "unknown".to_string(),
            preferred_output_tokens: requested_output_tokens,
            output_floor_tokens: 0,
            requested_output_tokens,
            protocol_overhead_tokens,
            safety_margin_tokens,
            hard_input_cap_tokens,
            target_input_cap_tokens: hard_input_cap_tokens,
            fixed_input_tokens,
            required_input_tokens: 0,
            dynamic_input_tokens: 0,
            omitted_packet_ids: Vec::new(),
            omitted_packet_reasons: BTreeMap::new(),
            executable: fixed_input_tokens <= hard_input_cap_tokens,
        }
    }

    pub fn set_context_window_source(&mut self, source: impl Into<String>) {
        self.context_window_source = source.into();
    }

    pub fn set_output_policy(
        &mut self,
        provider_max_output_tokens: u64,
        max_output_source: impl Into<String>,
        preferred_output_tokens: u64,
        output_floor_tokens: u64,
        required_input_tokens: u64,
    ) {
        self.provider_max_output_tokens = provider_max_output_tokens;
        self.max_output_source = max_output_source.into();
        self.preferred_output_tokens = preferred_output_tokens;
        self.output_floor_tokens = output_floor_tokens;
        self.required_input_tokens = required_input_tokens;
    }

    #[must_use]
    pub fn dynamic_hard_remaining(&self) -> u64 {
        self.hard_input_cap_tokens
            .saturating_sub(self.fixed_input_tokens)
    }

    pub fn record_dynamic_packets(
        &mut self,
        tokens: u64,
        omitted_packet_ids: Vec<String>,
        omitted_packet_reasons: BTreeMap<String, String>,
    ) {
        self.dynamic_input_tokens = tokens;
        self.omitted_packet_ids = omitted_packet_ids;
        self.omitted_packet_reasons = omitted_packet_reasons;
        self.executable = self.executable
            && self
                .fixed_input_tokens
                .saturating_add(self.dynamic_input_tokens)
                <= self.hard_input_cap_tokens;
    }

    #[must_use]
    pub fn input_total_tokens(&self) -> u64 {
        self.fixed_input_tokens
            .saturating_add(self.dynamic_input_tokens)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextComponentKind {
    System,
    History,
    Memory,
    ToolSchema,
    ToolInput,
    ToolResult,
    AgentHandoff,
    Capability,
}

impl ContextComponentKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::History => "history",
            Self::Memory => "memory",
            Self::ToolSchema => "tool_schema",
            Self::ToolInput => "tool_input",
            Self::ToolResult => "tool_result",
            Self::AgentHandoff => "agent_handoff",
            Self::Capability => "capability",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextLedgerEntry {
    pub component: ContextComponentKind,
    pub tokens: u64,
    pub reference: Option<String>,
    pub request_sequence: usize,
}

#[derive(Debug, Clone)]
pub struct ContextLedger {
    max_tokens: u64,
    tool_result_limit: u64,
    tool_result_consumed: u64,
    entries: Vec<ContextLedgerEntry>,
    evidence_hashes: BTreeSet<String>,
    request_sequence: usize,
    calibrated_input_tokens: Option<u64>,
}

impl ContextLedger {
    #[must_use]
    pub fn new(max_tokens: u64, tool_result_limit: u64) -> Self {
        Self {
            max_tokens: max_tokens.max(1),
            tool_result_limit: tool_result_limit.min(max_tokens).max(1),
            tool_result_consumed: 0,
            entries: Vec::new(),
            evidence_hashes: BTreeSet::new(),
            request_sequence: 0,
            calibrated_input_tokens: None,
        }
    }

    pub fn reset(&mut self, max_tokens: u64, tool_result_limit: u64) {
        *self = Self::new(max_tokens, tool_result_limit);
    }

    pub fn record(
        &mut self,
        component: ContextComponentKind,
        tokens: u64,
        reference: Option<String>,
        request_sequence: usize,
    ) {
        self.entries.push(ContextLedgerEntry {
            component,
            tokens,
            reference,
            request_sequence,
        });
    }

    pub fn begin_request(&mut self, request_sequence: usize) {
        self.entries.clear();
        self.request_sequence = request_sequence;
        self.calibrated_input_tokens = None;
    }

    pub fn begin_request_with_budget(&mut self, request_sequence: usize, max_tokens: u64) {
        self.begin_request(request_sequence);
        self.max_tokens = max_tokens.max(1);
        self.tool_result_limit = self.tool_result_limit.min(self.max_tokens).max(1);
    }

    pub fn reconcile_input_tokens(&mut self, actual_input_tokens: u64) {
        if actual_input_tokens == 0 {
            return;
        }
        let estimated = self.entries.iter().map(|entry| entry.tokens).sum::<u64>();
        if estimated == 0 {
            return;
        }
        let mut assigned = 0u64;
        let last_index = self.entries.len().saturating_sub(1);
        for (index, entry) in self.entries.iter_mut().enumerate() {
            let calibrated = if index == last_index {
                actual_input_tokens.saturating_sub(assigned)
            } else {
                entry
                    .tokens
                    .saturating_mul(actual_input_tokens)
                    .div_ceil(estimated)
                    .min(actual_input_tokens.saturating_sub(assigned))
            };
            entry.tokens = calibrated;
            assigned = assigned.saturating_add(calibrated);
        }
        self.calibrated_input_tokens = Some(actual_input_tokens);
    }

    #[must_use]
    pub fn reserve_tool_result(&mut self, requested: u64) -> u64 {
        let granted = requested.min(self.remaining_tool_result_tokens());
        self.tool_result_consumed = self.tool_result_consumed.saturating_add(granted);
        granted
    }

    #[must_use]
    pub fn remaining_tool_result_tokens(&self) -> u64 {
        self.tool_result_limit
            .saturating_sub(self.tool_result_consumed)
    }

    #[must_use]
    pub fn register_evidence_hash(&mut self, hash: impl Into<String>) -> bool {
        self.evidence_hashes.insert(hash.into())
    }

    #[must_use]
    pub fn projection(&self) -> ContextLedgerProjection {
        let mut by_component = BTreeMap::<ContextComponentKind, (u64, u64)>::new();
        for entry in &self.entries {
            let aggregate = by_component.entry(entry.component).or_default();
            aggregate.0 = aggregate.0.saturating_add(entry.tokens);
            aggregate.1 = aggregate.1.saturating_add(1);
        }
        let components = by_component
            .into_iter()
            .map(|(kind, (tokens, occurrences))| ContextComponentUsage {
                kind: kind.as_str().to_string(),
                tokens,
                occurrences,
            })
            .collect::<Vec<_>>();
        let consumed_tokens = components.iter().map(|item| item.tokens).sum::<u64>();
        ContextLedgerProjection {
            max_tokens: self.max_tokens,
            consumed_tokens,
            remaining_tokens: self.max_tokens.saturating_sub(consumed_tokens),
            tool_result_limit: self.tool_result_limit,
            tool_result_consumed: self.tool_result_consumed,
            components,
            request_sequence: self.request_sequence as u64,
            calibrated_input_tokens: self.calibrated_input_tokens,
        }
    }
}

#[must_use]
pub fn estimate_text_tokens(value: &str) -> u64 {
    let mut ascii = 0u64;
    let mut non_ascii = 0u64;
    let mut structural = 0u64;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch.is_ascii_whitespace() {
            ascii += 1;
        } else if ch.is_ascii() {
            structural += 1;
        } else {
            non_ascii += 1;
        }
    }
    ascii
        .div_ceil(4)
        .saturating_add(structural.div_ceil(2))
        .saturating_add(non_ascii)
        .max(u64::from(!value.is_empty()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tool_result_lease_is_really_consumed() {
        let mut ledger = ContextLedger::new(1_000, 100);
        assert_eq!(ledger.reserve_tool_result(80), 80);
        assert_eq!(ledger.reserve_tool_result(80), 20);
        assert_eq!(ledger.reserve_tool_result(1), 0);
    }

    #[test]
    fn cjk_and_structured_text_are_not_estimated_as_plain_ascii() {
        assert!(estimate_text_tokens("中文内容") >= 4);
        assert!(estimate_text_tokens(r#"{"a":1,"b":2}"#) > 3);
    }

    #[test]
    fn each_provider_request_replaces_the_previous_snapshot() {
        let mut ledger = ContextLedger::new(1_000, 100);
        ledger.begin_request(1);
        ledger.record(ContextComponentKind::History, 200, None, 1);
        ledger.begin_request(2);
        ledger.record(ContextComponentKind::History, 300, None, 2);
        let projection = ledger.projection();
        assert_eq!(projection.request_sequence, 2);
        assert_eq!(projection.consumed_tokens, 300);
    }

    #[test]
    fn provider_usage_recalibrates_the_component_estimates() {
        let mut ledger = ContextLedger::new(1_000, 100);
        ledger.begin_request(1);
        ledger.record(ContextComponentKind::System, 100, None, 1);
        ledger.record(ContextComponentKind::History, 300, None, 1);
        ledger.reconcile_input_tokens(200);
        let projection = ledger.projection();
        assert_eq!(projection.calibrated_input_tokens, Some(200));
        assert_eq!(projection.consumed_tokens, 200);
    }

    #[test]
    fn request_budget_uses_hard_capacity_as_its_only_request_limit() {
        let mut budget =
            RequestBudgetReport::for_attempt("small-model", 8_000, 2_000, 128, 256, 2_500);
        assert!(budget.executable);
        assert_eq!(budget.target_input_cap_tokens, budget.hard_input_cap_tokens);
        budget.record_dynamic_packets(
            budget.dynamic_hard_remaining() + 1,
            vec!["late".into()],
            BTreeMap::new(),
        );
        assert!(!budget.executable);
    }
}
