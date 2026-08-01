//! Bounded autonomous governance for durable memory and derived knowledge.
//!
//! Foreground writes remain responsible for immediate idempotency, authority,
//! and conflict fencing. This pass handles residual, cross-turn maintenance
//! without deleting evidence: safe cases are archived or superseded and every
//! ambiguous case remains in the durable review queue.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::memory_authority::authority_level;
use crate::types::{MemoryEntry, MemoryId, MemoryLayer, MemorySource};
use crate::{
    CognitiveContextManager, GovernanceConfig, KnowledgeFabric, MaintenanceCandidate,
    MaintenanceCandidateFilter, MaintenanceCandidateKind, MaintenanceCandidateStatus,
    MaintenanceScanConfig, MemoryError, MemoryKernel, MemoryScanCursor, MemoryState,
    MemoryTurnContext,
};

const LAST_REPORT_KEY: &str = "memory_governance:last_report";
type Result<T> = std::result::Result<T, MemoryError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutomaticGovernanceMode {
    Startup,
    Nightly,
    Manual,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct AutomaticGovernanceReport {
    pub run_id: String,
    pub mode: String,
    pub outcome: String,
    pub fatal_error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    pub duration_ms: u64,
    pub scanned_entries: usize,
    pub active_entries: usize,
    pub scanned_candidates: usize,
    pub auto_applied_duplicates: usize,
    pub auto_resolved_conflicts: usize,
    pub auto_archived_stale: usize,
    pub auto_validated_authority: usize,
    pub auto_refreshed_relationships: usize,
    pub auto_dismissed_obsolete: usize,
    pub consolidated_knowledge_packs: usize,
    pub auto_retired_knowledge_conflicts: usize,
    pub pending_human_review: usize,
    pub affected_memory_ids: Vec<MemoryId>,
    pub affected_knowledge_pack_ids: Vec<String>,
    pub affected_knowledge_conflict_ids: Vec<String>,
    pub pending_knowledge_conflict_ids: Vec<String>,
    pub discovered_by_kind: BTreeMap<String, usize>,
    pub handled_by_action: BTreeMap<String, usize>,
    pub details: Vec<AutomaticGovernanceDetail>,
    pub details_truncated: bool,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutomaticGovernanceDetail {
    pub candidate_id: String,
    pub kind: String,
    pub status: String,
    pub summary: String,
    pub reason: String,
    pub action: String,
    pub affected_memory_ids: Vec<MemoryId>,
    pub error: Option<String>,
}

impl AutomaticGovernanceReport {
    fn new(status: &crate::AutomaticGovernanceRunStatus) -> Self {
        let now = status.started_at;
        Self {
            run_id: status.run_id.clone(),
            mode: status.mode.clone(),
            outcome: "running".to_string(),
            started_at: now,
            completed_at: now,
            ..Self::default()
        }
    }

    fn increment_action(&mut self, action: &str) {
        *self
            .handled_by_action
            .entry(action.to_string())
            .or_default() += 1;
    }

    fn push_detail(&mut self, detail: AutomaticGovernanceDetail, limit: usize) {
        if self.details.len() < limit {
            self.details.push(detail);
        } else {
            self.details_truncated = true;
        }
    }
}

struct AutomaticGovernanceLease<'a> {
    manager: &'a CognitiveContextManager,
    run_id: String,
}

impl Drop for AutomaticGovernanceLease<'_> {
    fn drop(&mut self) {
        self.manager.finish_automatic_governance(&self.run_id);
    }
}

/// Apply only deterministic, evidence-preserving maintenance actions.
///
/// Safe automation:
/// - exact duplicates within one scope/layer/category;
/// - lower-authority conflicts with one unambiguous winner;
/// - unused, weak, nearly fully stale inferred L2/L3 entries;
/// - validation of fresh, repeatedly used high-confidence entries;
/// - relationship candidates already satisfied by the derived link graph;
/// - exact knowledge-pack duplicates inside one namespace.
///
/// Equal-authority conflicts, shared/global promotion, quarantined evidence,
/// and weakly supported decisions remain pending for explicit review.
pub async fn run_automatic_governance(
    manager: Arc<CognitiveContextManager>,
    knowledge: Option<&KnowledgeFabric>,
    policy: &GovernanceConfig,
    mode: AutomaticGovernanceMode,
) -> Result<AutomaticGovernanceReport> {
    let mode = format!("{mode:?}").to_ascii_lowercase();
    let status = manager
        .try_begin_automatic_governance(&mode)
        .ok_or(MemoryError::GovernanceAlreadyRunning)?;
    let _lease = AutomaticGovernanceLease {
        manager: manager.as_ref(),
        run_id: status.run_id.clone(),
    };
    let mut report = AutomaticGovernanceReport::new(&status);
    let execution_result: Result<()> = async {
        let kernel = MemoryKernel::new(Arc::clone(&manager));
        let mut active_entries = Vec::new();
        let mut cursor = MemoryScanCursor::default();
        loop {
            let page = manager.scan_entries_page(cursor, 750).await?;
            if page.entries.is_empty() {
                break;
            }
            report.scanned_entries = report.scanned_entries.saturating_add(page.entries.len());
            active_entries.extend(kernel.filter_active_entries(page.entries).await);
            manager.update_automatic_governance_progress(
                &report.run_id,
                "scanning",
                report.scanned_entries,
                0,
                0,
            );
            let Some(next) = page.next else {
                break;
            };
            cursor = next;
            tokio::task::yield_now().await;
        }
        report.active_entries = active_entries.len();

        let scan = MaintenanceScanConfig {
            stale_threshold: f32::from(policy.stale_threshold_bp) / 10_000.0,
            low_confidence_threshold: f32::from(policy.low_confidence_threshold_bp) / 10_000.0,
            authority_confidence_threshold: 0.92,
            max_candidates: policy.max_candidates,
        };
        manager.update_automatic_governance_progress(
            &report.run_id,
            "analyzing",
            report.scanned_entries,
            0,
            0,
        );
        let (active_entries, discovered) = manager
            .scan_memory_maintenance_entries_off_thread(active_entries, scan)
            .await?;
        let entries = active_entries
            .into_iter()
            .map(|entry| (entry.id, entry))
            .collect::<HashMap<_, _>>();
        for candidate in &discovered {
            *report
                .discovered_by_kind
                .entry(candidate.kind.as_str().to_string())
                .or_default() += 1;
        }
        let candidates = manager.list_memory_maintenance(MaintenanceCandidateFilter {
            status: Some(MaintenanceCandidateStatus::Open),
            limit: Some(policy.max_candidates),
            ..MaintenanceCandidateFilter::default()
        })?;
        report.scanned_candidates = candidates.len();
        manager.update_automatic_governance_progress(
            &report.run_id,
            "applying",
            report.scanned_entries,
            0,
            report.scanned_candidates,
        );
        let links = match kernel.links().await {
            Ok(links) => links,
            Err(error) => {
                report
                    .errors
                    .push(format!("memory relationship projection: {error}"));
                Vec::new()
            }
        };
        let governance_ctx = MemoryTurnContext::new(
            format!("memory-governance:{}", report.started_at.timestamp()),
            "system",
        );

        for (candidate_index, candidate) in candidates.into_iter().enumerate() {
            let affected_before = report.affected_memory_ids.len();
            let active_entry_count = candidate_entries(&candidate, &entries).len();
            let minimum_active_entries = match candidate.kind {
                MaintenanceCandidateKind::Duplicate | MaintenanceCandidateKind::Conflict => 2,
                MaintenanceCandidateKind::Stale
                | MaintenanceCandidateKind::AuthorityPromotion
                | MaintenanceCandidateKind::RelationshipRefresh => 1,
            };
            let obsolete_l4_authority = candidate.kind
                == MaintenanceCandidateKind::AuthorityPromotion
                && candidate_entries(&candidate, &entries)
                    .iter()
                    .all(|entry| entry.layer == MemoryLayer::L4);
            if active_entry_count < minimum_active_entries || obsolete_l4_authority {
                manager.transition_memory_maintenance(
                    &candidate.id,
                    MaintenanceCandidateStatus::Dismissed,
                )?;
                report.auto_dismissed_obsolete = report.auto_dismissed_obsolete.saturating_add(1);
                report.increment_action("dismissed_obsolete");
                report.push_detail(
                    AutomaticGovernanceDetail {
                        candidate_id: candidate.id,
                        kind: candidate.kind.as_str().to_string(),
                        status: "dismissed".to_string(),
                        summary: candidate.summary,
                        reason: candidate.reason,
                        action: "dismissed_obsolete".to_string(),
                        affected_memory_ids: Vec::new(),
                        error: None,
                    },
                    policy.max_candidates,
                );
                manager.update_automatic_governance_progress(
                    &report.run_id,
                    "applying",
                    report.scanned_entries,
                    candidate_index + 1,
                    report.scanned_candidates,
                );
                continue;
            }
            let result = match candidate.kind {
                MaintenanceCandidateKind::Duplicate => {
                    apply_duplicate(
                        &manager,
                        &kernel,
                        knowledge,
                        &governance_ctx,
                        &candidate,
                        &entries,
                        &mut report,
                    )
                    .await
                }
                MaintenanceCandidateKind::Conflict => {
                    apply_conflict(
                        &manager,
                        &kernel,
                        knowledge,
                        &governance_ctx,
                        &candidate,
                        &entries,
                        &mut report,
                    )
                    .await
                }
                MaintenanceCandidateKind::Stale => {
                    apply_stale(
                        &manager,
                        &kernel,
                        knowledge,
                        &governance_ctx,
                        &candidate,
                        &entries,
                        f32::from(policy.low_confidence_threshold_bp) / 10_000.0,
                        &mut report,
                    )
                    .await
                }
                MaintenanceCandidateKind::AuthorityPromotion => {
                    apply_authority_validation(
                        &kernel,
                        &governance_ctx,
                        &candidate,
                        &entries,
                        &mut report,
                    )
                    .await
                }
                MaintenanceCandidateKind::RelationshipRefresh => {
                    let satisfied = candidate
                        .entry_ids
                        .iter()
                        .all(|id| links.iter().any(|link| link.from == *id || link.to == *id));
                    if satisfied {
                        report.auto_refreshed_relationships =
                            report.auto_refreshed_relationships.saturating_add(1);
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                }
            };
            match result {
                Ok(true) => {
                    manager.transition_memory_maintenance(
                        &candidate.id,
                        MaintenanceCandidateStatus::Applied,
                    )?;
                    let action = action_for_kind(candidate.kind);
                    report.increment_action(action);
                    report.push_detail(
                        AutomaticGovernanceDetail {
                            candidate_id: candidate.id,
                            kind: candidate.kind.as_str().to_string(),
                            status: "applied".to_string(),
                            summary: candidate.summary,
                            reason: candidate.reason,
                            action: action.to_string(),
                            affected_memory_ids: report.affected_memory_ids[affected_before..]
                                .to_vec(),
                            error: None,
                        },
                        policy.max_candidates,
                    );
                }
                Ok(false) => {
                    report.pending_human_review = report.pending_human_review.saturating_add(1);
                    report.increment_action("pending_review");
                    report.push_detail(
                        AutomaticGovernanceDetail {
                            candidate_id: candidate.id,
                            kind: candidate.kind.as_str().to_string(),
                            status: "pending_review".to_string(),
                            summary: candidate.summary,
                            reason: candidate.reason,
                            action: "pending_review".to_string(),
                            affected_memory_ids: Vec::new(),
                            error: None,
                        },
                        policy.max_candidates,
                    );
                }
                Err(error) => {
                    report.errors.push(format!("{}: {error}", candidate.id));
                    report.pending_human_review = report.pending_human_review.saturating_add(1);
                    report.increment_action("failed");
                    report.push_detail(
                        AutomaticGovernanceDetail {
                            candidate_id: candidate.id,
                            kind: candidate.kind.as_str().to_string(),
                            status: "failed".to_string(),
                            summary: candidate.summary,
                            reason: candidate.reason,
                            action: "failed".to_string(),
                            affected_memory_ids: Vec::new(),
                            error: Some(error.to_string()),
                        },
                        policy.max_candidates,
                    );
                }
            }
            manager.update_automatic_governance_progress(
                &report.run_id,
                "applying",
                report.scanned_entries,
                candidate_index + 1,
                report.scanned_candidates,
            );
            tokio::task::yield_now().await;
        }

        manager.update_automatic_governance_progress(
            &report.run_id,
            "knowledge",
            report.scanned_entries,
            report.scanned_candidates,
            report.scanned_candidates,
        );
        if let Some(fabric) = knowledge {
            match fabric.retire_inactive_conflicts() {
                Ok(conflict_ids) => {
                    report.auto_retired_knowledge_conflicts = conflict_ids.len();
                    for conflict_id in &conflict_ids {
                        report.increment_action("retired_knowledge_conflict");
                        report.push_detail(
                            AutomaticGovernanceDetail {
                                candidate_id: conflict_id.clone(),
                                kind: "knowledge_conflict".to_string(),
                                status: "retired".to_string(),
                                summary: "Retired an inactive knowledge conflict".to_string(),
                                reason: "all referenced knowledge sources are inactive".to_string(),
                                action: "retired_knowledge_conflict".to_string(),
                                affected_memory_ids: Vec::new(),
                                error: None,
                            },
                            policy.max_candidates,
                        );
                    }
                    report.affected_knowledge_conflict_ids.extend(conflict_ids);
                }
                Err(error) => report
                    .errors
                    .push(format!("knowledge conflict retirement: {error}")),
            }
            match fabric.consolidate_exact_duplicates() {
                Ok(consolidation) => {
                    report.consolidated_knowledge_packs = consolidation.superseded_pack_ids.len();
                    for pack_id in &consolidation.superseded_pack_ids {
                        report.increment_action("consolidated_knowledge_pack");
                        report.push_detail(
                            AutomaticGovernanceDetail {
                                candidate_id: pack_id.clone(),
                                kind: "knowledge_pack".to_string(),
                                status: "superseded".to_string(),
                                summary: "Consolidated an exact duplicate knowledge pack"
                                    .to_string(),
                                reason: "an equivalent canonical pack exists in the namespace"
                                    .to_string(),
                                action: "consolidated_knowledge_pack".to_string(),
                                affected_memory_ids: Vec::new(),
                                error: None,
                            },
                            policy.max_candidates,
                        );
                    }
                    report.pending_human_review = report
                        .pending_human_review
                        .saturating_add(consolidation.unresolved_conflict_ids.len());
                    report
                        .pending_knowledge_conflict_ids
                        .extend(consolidation.unresolved_conflict_ids);
                    report
                        .affected_knowledge_pack_ids
                        .extend(consolidation.superseded_pack_ids);
                    report
                        .affected_knowledge_pack_ids
                        .extend(consolidation.canonical_pack_ids);
                }
                Err(error) => report
                    .errors
                    .push(format!("knowledge consolidation: {error}")),
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = &execution_result {
        let error = error.to_string();
        report.outcome = "failed".to_string();
        report.fatal_error = Some(error.clone());
        report.errors.push(format!("fatal: {error}"));
    } else if report.errors.is_empty() {
        report.outcome = "succeeded".to_string();
    } else {
        report.outcome = "completed_with_errors".to_string();
    }
    report.affected_memory_ids.sort();
    report.affected_memory_ids.dedup();
    report.affected_knowledge_pack_ids.sort();
    report.affected_knowledge_pack_ids.dedup();
    report.affected_knowledge_conflict_ids.sort();
    report.affected_knowledge_conflict_ids.dedup();
    report.pending_knowledge_conflict_ids.sort();
    report.pending_knowledge_conflict_ids.dedup();
    report.completed_at = Utc::now();
    report.duration_ms = report
        .completed_at
        .signed_duration_since(report.started_at)
        .num_milliseconds()
        .max(0) as u64;
    let encoded = serde_json::to_string(&report)?;
    let persistence_result = manager.kernel_kv_put(LAST_REPORT_KEY, &encoded).await;
    match (execution_result, persistence_result) {
        (Ok(()), Ok(())) => Ok(report),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(report_error)) => {
            tracing::error!(
                run_id = %report.run_id,
                %report_error,
                "failed to persist fatal automatic-governance report"
            );
            Err(error)
        }
    }
}

fn action_for_kind(kind: MaintenanceCandidateKind) -> &'static str {
    match kind {
        MaintenanceCandidateKind::Duplicate => "merged_duplicate",
        MaintenanceCandidateKind::Conflict => "resolved_conflict",
        MaintenanceCandidateKind::Stale => "archived_stale",
        MaintenanceCandidateKind::AuthorityPromotion => "validated_authority",
        MaintenanceCandidateKind::RelationshipRefresh => "refreshed_relationship",
    }
}

pub async fn last_automatic_governance_report(
    manager: &CognitiveContextManager,
) -> Result<Option<AutomaticGovernanceReport>> {
    manager
        .kernel_kv_get(LAST_REPORT_KEY)
        .await?
        .map(|raw| serde_json::from_str(&raw).map_err(crate::MemoryError::Serialisation))
        .transpose()
}

async fn apply_duplicate(
    manager: &CognitiveContextManager,
    kernel: &MemoryKernel,
    knowledge: Option<&KnowledgeFabric>,
    ctx: &MemoryTurnContext,
    candidate: &MaintenanceCandidate,
    entries: &HashMap<MemoryId, MemoryEntry>,
    report: &mut AutomaticGovernanceReport,
) -> Result<bool> {
    let group = candidate_entries(candidate, entries);
    let Some(canonical) = group.iter().max_by_key(|entry| canonical_rank(entry)) else {
        return Ok(false);
    };
    let duplicate_ids = group
        .iter()
        .filter(|entry| entry.id != canonical.id)
        .map(|entry| entry.id)
        .collect::<Vec<_>>();
    if duplicate_ids.is_empty() {
        return Ok(false);
    }
    for id in duplicate_ids {
        archive_derived_memory(
            manager,
            kernel,
            knowledge,
            ctx,
            id,
            MemoryState::Archived,
            format!(
                "automatic governance merged exact duplicate into {}",
                canonical.id
            ),
        )
        .await?;
        report.affected_memory_ids.push(id);
    }
    report.auto_applied_duplicates = report.auto_applied_duplicates.saturating_add(1);
    Ok(true)
}

async fn apply_conflict(
    manager: &CognitiveContextManager,
    kernel: &MemoryKernel,
    knowledge: Option<&KnowledgeFabric>,
    ctx: &MemoryTurnContext,
    candidate: &MaintenanceCandidate,
    entries: &HashMap<MemoryId, MemoryEntry>,
    report: &mut AutomaticGovernanceReport,
) -> Result<bool> {
    let group = candidate_entries(candidate, entries);
    let mut by_authority = BTreeMap::<_, Vec<&MemoryEntry>>::new();
    for entry in group {
        by_authority
            .entry(authority_level(entry))
            .or_default()
            .push(entry);
    }
    let Some((_, winners)) = by_authority.last_key_value() else {
        return Ok(false);
    };
    if winners.len() != 1 || by_authority.len() < 2 {
        return Ok(false);
    }
    let winner_id = winners[0].id;
    let loser_ids = by_authority
        .values()
        .flatten()
        .filter(|entry| entry.id != winner_id)
        .map(|entry| entry.id)
        .collect::<Vec<_>>();
    for id in loser_ids {
        archive_derived_memory(
            manager,
            kernel,
            knowledge,
            ctx,
            id,
            MemoryState::Superseded,
            format!("automatic governance selected higher-authority memory {winner_id}"),
        )
        .await?;
        report.affected_memory_ids.push(id);
    }
    report.auto_resolved_conflicts = report.auto_resolved_conflicts.saturating_add(1);
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn apply_stale(
    manager: &CognitiveContextManager,
    kernel: &MemoryKernel,
    knowledge: Option<&KnowledgeFabric>,
    ctx: &MemoryTurnContext,
    candidate: &MaintenanceCandidate,
    entries: &HashMap<MemoryId, MemoryEntry>,
    low_confidence_threshold: f32,
    report: &mut AutomaticGovernanceReport,
) -> Result<bool> {
    let Some(entry) = candidate_entries(candidate, entries).into_iter().next() else {
        return Ok(false);
    };
    let safe_to_archive = matches!(entry.layer, MemoryLayer::L2 | MemoryLayer::L3)
        && matches!(
            entry.source,
            MemorySource::AutoExtracted | MemorySource::Compression | MemorySource::Prefetch
        )
        && entry.access_count == 0
        && entry.confidence <= low_confidence_threshold
        && entry.staleness >= 0.98;
    if !safe_to_archive {
        return Ok(false);
    }
    archive_derived_memory(
        manager,
        kernel,
        knowledge,
        ctx,
        entry.id,
        MemoryState::Archived,
        "automatic governance archived unused low-confidence stale inferred memory".to_string(),
    )
    .await?;
    report.affected_memory_ids.push(entry.id);
    report.auto_archived_stale = report.auto_archived_stale.saturating_add(1);
    Ok(true)
}

async fn apply_authority_validation(
    kernel: &MemoryKernel,
    ctx: &MemoryTurnContext,
    candidate: &MaintenanceCandidate,
    entries: &HashMap<MemoryId, MemoryEntry>,
    report: &mut AutomaticGovernanceReport,
) -> Result<bool> {
    let Some(entry) = candidate_entries(candidate, entries).into_iter().next() else {
        return Ok(false);
    };
    if entry.layer == MemoryLayer::L4 {
        return Ok(false);
    }
    kernel
        .transition_state(
            ctx,
            entry.id,
            MemoryState::Validated,
            "automatic governance validated fresh, repeatedly used, high-confidence memory",
        )
        .await
        .map_err(|error| MemoryError::Store(error.to_string()))?;
    report.affected_memory_ids.push(entry.id);
    report.auto_validated_authority = report.auto_validated_authority.saturating_add(1);
    Ok(true)
}

async fn archive_derived_memory(
    manager: &CognitiveContextManager,
    kernel: &MemoryKernel,
    knowledge: Option<&KnowledgeFabric>,
    ctx: &MemoryTurnContext,
    memory_id: MemoryId,
    state: MemoryState,
    reason: String,
) -> Result<()> {
    kernel
        .transition_state(ctx, memory_id, state, reason)
        .await
        .map_err(|error| MemoryError::Store(error.to_string()))?;
    manager.evict_vector_entry(&memory_id)?;
    if let Some(fabric) = knowledge {
        fabric
            .quarantine_source(&format!("memory:{memory_id}"))
            .map_err(|error| crate::MemoryError::Store(error.to_string()))?;
    }
    Ok(())
}

fn candidate_entries<'a>(
    candidate: &MaintenanceCandidate,
    entries: &'a HashMap<MemoryId, MemoryEntry>,
) -> Vec<&'a MemoryEntry> {
    candidate
        .entry_ids
        .iter()
        .filter_map(|id| entries.get(id))
        .collect()
}

fn canonical_rank(entry: &MemoryEntry) -> (u8, u64, u32, u32, DateTime<Utc>, MemoryId) {
    (
        authority_level(entry) as u8,
        entry.access_count,
        (entry.confidence.clamp(0.0, 1.0) * 10_000.0) as u32,
        ((1.0 - entry.staleness.clamp(0.0, 1.0)) * 10_000.0) as u32,
        entry.updated_at,
        entry.id,
    )
}
