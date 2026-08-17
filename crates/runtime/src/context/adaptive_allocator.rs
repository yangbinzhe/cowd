use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use crate::context_runtime::{
    ContextAuthority, ContextItem, ContextOmission, ContextProfile, ContextRole, ContextSourceKind,
    ContextSourceLifecycle,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextResolution {
    Pinned,
    Hot,
    WarmCard,
    ExactEvidence,
}

/// Request-local facts used by the Runtime-owned final context selector.
///
/// Source budgets are deliberately absent. Sources contribute candidates and
/// may borrow all remaining capacity when their evidence is useful.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextDemand {
    pub model_window_tokens: u64,
    pub output_reserve_tokens: u64,
    pub protocol_overhead_tokens: u64,
    pub safety_margin_tokens: u64,
    pub explicit_ceiling_tokens: Option<u64>,
    pub risk_basis_points: u16,
    pub complexity_basis_points: u16,
    pub minimum_coverage_basis_points: u16,
}

impl ContextDemand {
    #[must_use]
    pub fn for_envelope(
        available_tokens: u64,
        profile: ContextProfile,
        intent: &str,
        candidates: &[ContextItem],
    ) -> Self {
        let intent_tokens = (intent.chars().count() as u64 / 4).max(1);
        let source_count = candidates
            .iter()
            .map(|candidate| candidate.source)
            .collect::<HashSet<_>>()
            .len() as u64;
        let conflict_count = candidates
            .iter()
            .filter(|candidate| !candidate.conflict_with.is_empty())
            .count() as u64;
        let profile_complexity = match profile {
            ContextProfile::DeepInvestigation
            | ContextProfile::YoloGoal
            | ContextProfile::Collaboration => 7_000,
            ContextProfile::Review | ContextProfile::AutonomousGoal => 6_000,
            ContextProfile::SubAgent | ContextProfile::Resume => 5_000,
            ContextProfile::MainTurn => 4_000,
            ContextProfile::Cron
            | ContextProfile::SurfaceQuickReply
            | ContextProfile::SurfaceTaskIntake => 2_500,
        };
        let complexity_basis_points = (profile_complexity
            + intent_tokens.min(1_500) as u16
            + source_count.saturating_mul(120).min(1_000) as u16)
            .min(10_000);
        let risk_basis_points = (2_000_u64
            + conflict_count.saturating_mul(900)
            + candidates
                .iter()
                .filter(|candidate| candidate.role == ContextRole::Warning)
                .count() as u64
                * 600)
            .min(10_000) as u16;
        let minimum_coverage_basis_points =
            (4_500_u32 + u32::from(complexity_basis_points) / 3 + u32::from(risk_basis_points) / 5)
                .min(9_000) as u16;
        Self {
            model_window_tokens: available_tokens,
            output_reserve_tokens: 0,
            protocol_overhead_tokens: 0,
            safety_margin_tokens: 0,
            explicit_ceiling_tokens: Some(available_tokens),
            risk_basis_points,
            complexity_basis_points,
            minimum_coverage_basis_points,
        }
    }

    #[must_use]
    pub fn dynamic_capacity(&self) -> u64 {
        let available = self
            .model_window_tokens
            .saturating_sub(self.output_reserve_tokens)
            .saturating_sub(self.protocol_overhead_tokens)
            .saturating_sub(self.safety_margin_tokens);
        self.explicit_ceiling_tokens
            .map_or(available, |ceiling| available.min(ceiling))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextAllocationReport {
    pub selected_count: usize,
    pub omitted_count: usize,
    pub expanded_count: usize,
    pub conflict_count: usize,
    pub unresolved_conflict_count: usize,
    pub coverage_basis_points: u16,
    pub borrowed_budget_tokens: u64,
    pub used_tokens: u64,
}

#[derive(Debug, Clone)]
pub struct ContextAllocation {
    pub selected: Vec<ContextItem>,
    pub omitted: Vec<ContextOmission>,
    pub report: ContextAllocationReport,
}

pub struct ContextAllocator;

impl ContextAllocator {
    /// Select the minimum sufficient, highest-utility set without hard source
    /// partitions. Exact evidence is admitted when coverage or conflict makes
    /// a warm card alone unsafe.
    #[must_use]
    pub fn allocate(items: Vec<ContextItem>, demand: &ContextDemand) -> ContextAllocation {
        let capacity = demand.dynamic_capacity();
        let source_count = items
            .iter()
            .map(|item| item.source)
            .collect::<HashSet<_>>()
            .len()
            .max(1) as u64;
        let soft_share = capacity / source_count;
        let mut ranked = deduplicate(items);
        ranked.sort_by(compare_candidates);

        let mut selected = Vec::new();
        let mut omitted = Vec::new();
        let mut selected_ids = HashSet::new();
        let mut used_by_source = HashMap::<ContextSourceKind, u64>::new();
        let mut used_tokens = 0_u64;
        let mut expanded_count = 0_usize;
        let mut conflict_count = 0_usize;
        let mut unresolved_conflict_count = 0_usize;

        for item in ranked {
            let resolution = resolution_for(&item);
            let pinned = resolution == ContextResolution::Pinned;
            let conflicts = item
                .conflict_with
                .iter()
                .filter(|id| selected_ids.contains(id.as_str()))
                .count();
            conflict_count += conflicts;

            if conflicts > 0 {
                let stronger_selected = selected.iter().any(|selected_item: &ContextItem| {
                    item.conflict_with.iter().any(|id| id == &selected_item.id)
                        && authority_rank(selected_item.authority) > authority_rank(item.authority)
                });
                if stronger_selected {
                    omitted.push(ContextOmission {
                        source: item.source,
                        reason: "superseded by higher-authority conflicting evidence".to_string(),
                        token_estimate: item.token_estimate,
                    });
                    continue;
                }
                unresolved_conflict_count += 1;
            }

            let next_tokens = used_tokens.saturating_add(item.token_estimate);
            if next_tokens > capacity {
                omitted.push(ContextOmission {
                    source: item.source,
                    reason: if pinned {
                        "pinned context exceeds provider-safe dynamic capacity".to_string()
                    } else {
                        "minimum-sufficient context consumed provider-safe capacity".to_string()
                    },
                    token_estimate: item.token_estimate,
                });
                continue;
            }

            let coverage = coverage_basis_points(&selected, demand);
            let requires_expansion = matches!(resolution, ContextResolution::ExactEvidence)
                && (coverage < demand.minimum_coverage_basis_points
                    || conflicts > 0
                    || demand.risk_basis_points >= 6_000);
            if coverage >= demand.minimum_coverage_basis_points && !pinned && !requires_expansion {
                omitted.push(ContextOmission {
                    source: item.source,
                    reason: "minimum sufficient evidence coverage reached".to_string(),
                    token_estimate: item.token_estimate,
                });
                continue;
            }

            if resolution == ContextResolution::ExactEvidence {
                expanded_count += 1;
            }
            used_tokens = next_tokens;
            *used_by_source.entry(item.source).or_default() += item.token_estimate;
            selected_ids.insert(item.id.clone());
            selected.push(item);
        }

        let borrowed_budget_tokens = used_by_source
            .values()
            .map(|used| used.saturating_sub(soft_share))
            .sum();
        let coverage_basis_points = coverage_basis_points(&selected, demand);
        ContextAllocation {
            report: ContextAllocationReport {
                selected_count: selected.len(),
                omitted_count: omitted.len(),
                expanded_count,
                conflict_count,
                unresolved_conflict_count,
                coverage_basis_points,
                borrowed_budget_tokens,
                used_tokens,
            },
            selected,
            omitted,
        }
    }
}

fn deduplicate(items: Vec<ContextItem>) -> Vec<ContextItem> {
    let mut by_identity = HashMap::<String, ContextItem>::new();
    for item in items {
        let key = item
            .source_id
            .clone()
            .unwrap_or_else(|| format!("{:?}:{}", item.source, item.id));
        match by_identity.get(&key) {
            Some(current) if compare_candidates(current, &item) != Ordering::Greater => {}
            _ => {
                by_identity.insert(key, item);
            }
        }
    }
    by_identity.into_values().collect()
}

fn compare_candidates(left: &ContextItem, right: &ContextItem) -> Ordering {
    resolution_rank(resolution_for(left))
        .cmp(&resolution_rank(resolution_for(right)))
        .then_with(|| authority_rank(left.authority).cmp(&authority_rank(right.authority)))
        .then_with(|| {
            left.score
                .partial_cmp(&right.score)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| utility_density(left).cmp(&utility_density(right)))
        .then_with(|| right.id.cmp(&left.id))
        .reverse()
}

fn resolution_for(item: &ContextItem) -> ContextResolution {
    if matches!(
        item.role,
        ContextRole::Instruction | ContextRole::TaskState | ContextRole::Warning
    ) {
        ContextResolution::Pinned
    } else if matches!(
        item.role,
        ContextRole::RecentTurn | ContextRole::ToolSummary
    ) || matches!(
        item.source,
        ContextSourceKind::Conversation | ContextSourceKind::Task | ContextSourceKind::AgentPeer
    ) {
        ContextResolution::Hot
    } else if item.source_lifecycle == ContextSourceLifecycle::Session
        || matches!(item.role, ContextRole::Orientation | ContextRole::Identity)
    {
        ContextResolution::WarmCard
    } else {
        ContextResolution::ExactEvidence
    }
}

const fn resolution_rank(resolution: ContextResolution) -> u8 {
    match resolution {
        ContextResolution::Pinned => 4,
        ContextResolution::Hot => 3,
        ContextResolution::WarmCard => 2,
        ContextResolution::ExactEvidence => 1,
    }
}

const fn authority_rank(authority: ContextAuthority) -> u8 {
    match authority {
        ContextAuthority::System => 7,
        ContextAuthority::User => 6,
        ContextAuthority::Project => 5,
        ContextAuthority::Session => 4,
        ContextAuthority::Agent => 3,
        ContextAuthority::Tool => 2,
        ContextAuthority::Derived => 1,
    }
}

fn utility_density(item: &ContextItem) -> u64 {
    let score = (item.score.clamp(0.0, 10.0) * 1_000.0) as u64;
    let evidence = item.evidence.len().min(16) as u64 * 80;
    let novelty = u64::from(item.source_reason.is_some()) * 120;
    (u64::from(authority_rank(item.authority)) * 1_000 + score + evidence + novelty)
        .saturating_mul(1_000)
        / item.token_estimate.max(1)
}

/// Marginal utility density (value per token) for the information-selection
/// budget. Exposed for audit/projection; selection order already ranks by it
/// among otherwise-equal candidates.
#[must_use]
pub fn marginal_utility_density(item: &ContextItem) -> u64 {
    utility_density(item)
}

fn coverage_basis_points(selected: &[ContextItem], demand: &ContextDemand) -> u16 {
    if selected.is_empty() {
        return 0;
    }
    let sources = selected
        .iter()
        .map(|item| item.source)
        .collect::<HashSet<_>>()
        .len() as u32;
    let roles = selected
        .iter()
        .map(|item| item.role)
        .collect::<HashSet<_>>()
        .len() as u32;
    let evidence = selected
        .iter()
        .filter(|item| !item.evidence.is_empty())
        .count() as u32;
    let warnings = selected
        .iter()
        .filter(|item| item.role == ContextRole::Warning)
        .count() as u32;
    let base = 2_500_u32
        + sources.saturating_mul(900)
        + roles.saturating_mul(500)
        + evidence.min(8).saturating_mul(300)
        + warnings.min(3).saturating_mul(350);
    let complexity_penalty = u32::from(demand.complexity_basis_points) / 8;
    base.saturating_sub(complexity_penalty).min(10_000) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: &str, source: ContextSourceKind, score: f32, tokens: u64) -> ContextItem {
        let mut item = ContextItem::new(id, source, ContextRole::Evidence, id);
        item.score = score;
        item.token_estimate = tokens;
        item
    }

    #[test]
    fn unused_source_share_is_borrowed_by_higher_utility_evidence() {
        let demand = ContextDemand {
            model_window_tokens: 1_000,
            output_reserve_tokens: 0,
            protocol_overhead_tokens: 0,
            safety_margin_tokens: 0,
            explicit_ceiling_tokens: Some(1_000),
            risk_basis_points: 8_000,
            complexity_basis_points: 8_000,
            minimum_coverage_basis_points: 9_000,
        };
        let allocation = ContextAllocator::allocate(
            vec![
                item("s1", ContextSourceKind::Conversation, 1.0, 400),
                item("s2", ContextSourceKind::Conversation, 0.9, 400),
                item("m1", ContextSourceKind::Memory, 0.1, 400),
            ],
            &demand,
        );
        assert_eq!(
            allocation
                .selected
                .iter()
                .filter(|item| item.source == ContextSourceKind::Conversation)
                .count(),
            2
        );
        assert!(allocation.report.borrowed_budget_tokens > 0);
    }

    #[test]
    fn marginal_utility_density_rewards_value_per_token() {
        let cheap = item("cheap", ContextSourceKind::Memory, 8.0, 100);
        let expensive = item("expensive", ContextSourceKind::Memory, 9.0, 2_000);
        assert!(marginal_utility_density(&cheap) > marginal_utility_density(&expensive));
    }

    #[test]
    fn higher_authority_conflict_supersedes_lower_authority_candidate() {
        let mut lower = item("derived", ContextSourceKind::Memory, 1.0, 100);
        lower.authority = ContextAuthority::Derived;
        let mut higher = item("user", ContextSourceKind::Conversation, 1.0, 100);
        higher.authority = ContextAuthority::User;
        lower.conflict_with.push(higher.id.clone());
        higher.conflict_with.push(lower.id.clone());
        let demand = ContextDemand::for_envelope(
            1_000,
            ContextProfile::MainTurn,
            "resolve conflict",
            &[lower.clone(), higher.clone()],
        );
        let allocation = ContextAllocator::allocate(vec![lower, higher], &demand);
        assert!(allocation.selected.iter().any(|item| item.id == "user"));
        assert!(!allocation.selected.iter().any(|item| item.id == "derived"));
    }
}
