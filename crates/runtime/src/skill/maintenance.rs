//! Bounded, replayable projection from canonical Skill usage Receipts and
//! terminal Outcomes into inert maintenance Drafts.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use harness_contract::{
    outcome::ExecutionOutcome,
    skill::{
        SkillMaintenanceDraft, SkillMaintenanceRecommendation, SkillMaintenanceValidation,
        SkillUsageCounts, SkillUsageReceipt, SKILL_MAINTENANCE_DRAFT_SCHEMA_VERSION,
        SKILL_USAGE_RECEIPT_SCHEMA_VERSION,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    execution_core::outcome_service::OUTCOME_EVENT_KIND, RuntimeEventScope, RuntimeEventStore,
    RuntimeProjectionDescriptor, RuntimeProjectionEventInterest, RuntimeProjectionInterest,
    RuntimeProjectionLane, RuntimeProjectionLatencyClass, RuntimeProjectionPass,
};

use super::usage::SKILL_USAGE_RECEIPT_EVENT_KIND;

pub(crate) const PROJECTOR_ID: &str = "projector:skill-maintenance:v1";
const PROJECTOR_WORKER_BATCH: usize = 8;
const MAX_RECEIPTS_PER_SCOPE: usize = 512;
const MAX_MAINTENANCE_SCOPES: usize = 256;
const MAX_OUTCOMES: usize = 4_096;
const LEGACY_USAGE_EVENT_KIND: &str = "skill.usage.observed";
const IDLE_POLL: Duration = Duration::from_secs(1);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct MaintenanceScope {
    skill_id: String,
    skill_revision: String,
    workspace_identity: String,
    workload_fingerprint: String,
    config_revision: String,
    evaluation_environment: String,
}

impl From<&SkillUsageReceipt> for MaintenanceScope {
    fn from(receipt: &SkillUsageReceipt) -> Self {
        Self {
            skill_id: receipt.skill_id.clone(),
            skill_revision: receipt.skill_revision.clone(),
            workspace_identity: receipt.workspace_identity.clone(),
            workload_fingerprint: receipt.workload_fingerprint.clone(),
            config_revision: receipt.config_revision.clone(),
            evaluation_environment: receipt.evaluation_environment.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct OutcomeEvidence {
    execution_id: String,
    succeeded: bool,
    verification_blocked: bool,
    terminal_class: String,
    observed_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ScopedReceipts {
    scope: MaintenanceScope,
    receipts: Vec<SkillUsageReceipt>,
    outcomes: BTreeMap<String, OutcomeEvidence>,
}

impl From<&ExecutionOutcome> for OutcomeEvidence {
    fn from(outcome: &ExecutionOutcome) -> Self {
        Self {
            execution_id: outcome.identity.execution_id.clone(),
            succeeded: outcome.terminal.is_success(),
            verification_blocked: outcome.strategy_feedback.verification_blocked,
            terminal_class: outcome.terminal.class_name().to_string(),
            observed_at_ms: outcome.observation.observed_at_ms,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillMaintenanceSnapshot {
    pub revision: u64,
    pub source_cursor: u64,
    pub projected_at_ms: u64,
    pub drafts: BTreeMap<String, SkillMaintenanceDraft>,
    pub rejected_receipts: u64,
    #[serde(default)]
    receipts: BTreeMap<String, ScopedReceipts>,
    #[serde(default)]
    outcomes: BTreeMap<String, OutcomeEvidence>,
    #[serde(default)]
    legacy_counts: BTreeMap<String, SkillUsageCounts>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillMaintenanceProjectionHealth {
    pub checkpoint_cursor: u64,
    pub latest_commit_cursor: u64,
    pub lag_commits: u64,
    pub projected_at_ms: u64,
    pub draft_count: usize,
    pub rejected_receipts: u64,
    pub worker_running: bool,
}

pub struct SkillMaintenanceProjector {
    event_store: Arc<RuntimeEventStore>,
    snapshot: RwLock<Arc<SkillMaintenanceSnapshot>>,
    projection_lock: Mutex<()>,
}

impl SkillMaintenanceProjector {
    #[must_use]
    pub fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        let snapshot = event_store
            .projection_checkpoint(PROJECTOR_ID)
            .ok()
            .flatten()
            .and_then(|checkpoint| serde_json::from_value(checkpoint.payload).ok())
            .unwrap_or_default();
        Self {
            event_store,
            snapshot: RwLock::new(Arc::new(snapshot)),
            projection_lock: Mutex::new(()),
        }
    }

    pub(crate) fn projection_lane(self: &Arc<Self>) -> RuntimeProjectionLane {
        let projector = Arc::clone(self);
        RuntimeProjectionLane::blocking(
            RuntimeProjectionDescriptor::new(
                PROJECTOR_ID,
                projection_interest(),
                PROJECTOR_WORKER_BATCH,
                IDLE_POLL,
            )
            .expect("Skill maintenance projection descriptor is static and valid")
            .with_latency_class(RuntimeProjectionLatencyClass::Maintenance),
            move |batch_size| {
                let processed = projector.project_available(batch_size)?;
                Ok(RuntimeProjectionPass::scanned(processed, batch_size))
            },
        )
    }

    #[must_use]
    pub fn snapshot(&self) -> Arc<SkillMaintenanceSnapshot> {
        Arc::clone(
            &self
                .snapshot
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    pub fn project_available(&self, max_commits: usize) -> Result<usize, String> {
        let _guard = self
            .projection_lock
            .lock()
            .map_err(|_| "Skill maintenance projection lock poisoned".to_string())?;
        let current = self.snapshot();
        let mut next = (*current).clone();
        let page = self
            .event_store
            .projection_scan_page(
                current.source_cursor,
                &projection_interest(),
                max_commits.max(1),
                10_000,
                32 * 1024 * 1024,
            )
            .map_err(|error| error.to_string())?;
        if page.scanned_commits == 0 {
            return Ok(0);
        }
        let mut changed = false;
        for batch in &page.batches {
            for event in &batch.events {
                match event.kind.as_str() {
                    SKILL_USAGE_RECEIPT_EVENT_KIND => {
                        let receipt = event.payload.get("receipt").cloned().and_then(|value| {
                            serde_json::from_value::<SkillUsageReceipt>(value).ok()
                        });
                        match receipt {
                            Some(receipt)
                                if receipt.schema_version == SKILL_USAGE_RECEIPT_SCHEMA_VERSION
                                    && receipt_fields_complete(&receipt) =>
                            {
                                reduce_receipt(&mut next, receipt);
                                changed = true;
                            }
                            _ => {
                                next.rejected_receipts = next.rejected_receipts.saturating_add(1);
                                changed = true;
                            }
                        }
                    }
                    OUTCOME_EVENT_KIND => {
                        if let Ok(outcome) =
                            serde_json::from_value::<ExecutionOutcome>(event.payload.clone())
                        {
                            reduce_outcome(&mut next, outcome);
                            changed = true;
                        }
                    }
                    LEGACY_USAGE_EVENT_KIND => {
                        if reduce_legacy(&mut next, &event.payload) {
                            changed = true;
                        }
                    }
                    _ => {}
                }
            }
        }
        next.source_cursor = page.scanned_through_cursor;
        if changed {
            next.revision = next.revision.saturating_add(1);
            recompute_drafts(&mut next);
        }
        next.projected_at_ms = now_ms();
        self.event_store
            .put_projection_checkpoint(
                PROJECTOR_ID,
                next.source_cursor,
                &serde_json::to_value(&next).map_err(|error| error.to_string())?,
                next.projected_at_ms,
            )
            .map_err(|error| error.to_string())?;
        *self
            .snapshot
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(next);
        Ok(page.scanned_commits)
    }

    pub fn drafts(&self, limit: usize) -> Vec<SkillMaintenanceDraft> {
        let mut drafts = self.snapshot().drafts.values().cloned().collect::<Vec<_>>();
        drafts.sort_by(|left, right| {
            right
                .created_at_ms
                .cmp(&left.created_at_ms)
                .then_with(|| left.draft_id.cmp(&right.draft_id))
        });
        drafts.truncate(limit.min(100));
        drafts
    }

    pub fn draft(&self, draft_id: &str) -> Option<SkillMaintenanceDraft> {
        self.snapshot().drafts.get(draft_id).cloned()
    }

    pub fn health(&self) -> SkillMaintenanceProjectionHealth {
        self.health_with_worker(false)
    }

    pub(crate) fn health_with_worker(
        &self,
        worker_running: bool,
    ) -> SkillMaintenanceProjectionHealth {
        let snapshot = self.snapshot();
        let latest = self.event_store.current_commit_cursor();
        SkillMaintenanceProjectionHealth {
            checkpoint_cursor: snapshot.source_cursor,
            latest_commit_cursor: latest,
            lag_commits: latest.saturating_sub(snapshot.source_cursor),
            projected_at_ms: snapshot.projected_at_ms,
            draft_count: snapshot.drafts.len(),
            rejected_receipts: snapshot.rejected_receipts,
            worker_running,
        }
    }
}

fn projection_interest() -> RuntimeProjectionInterest {
    let mut interests = vec![
        RuntimeProjectionEventInterest::new(
            RuntimeEventScope::Skill,
            SKILL_USAGE_RECEIPT_EVENT_KIND,
        ),
        RuntimeProjectionEventInterest::new(RuntimeEventScope::Skill, LEGACY_USAGE_EVENT_KIND),
    ];
    interests.extend(
        [
            RuntimeEventScope::Agent,
            RuntimeEventScope::Team,
            RuntimeEventScope::Tool,
            RuntimeEventScope::Task,
        ]
        .map(|scope| RuntimeProjectionEventInterest::new(scope, OUTCOME_EVENT_KIND)),
    );
    RuntimeProjectionInterest::new(interests)
}

fn receipt_fields_complete(receipt: &SkillUsageReceipt) -> bool {
    [
        receipt.receipt_id.as_str(),
        receipt.skill_id.as_str(),
        receipt.skill_revision.as_str(),
        receipt.workspace_identity.as_str(),
        receipt.workload_fingerprint.as_str(),
        receipt.config_revision.as_str(),
        receipt.evaluation_environment.as_str(),
        receipt.execution_id.as_str(),
        receipt.session_id.as_str(),
        receipt.turn_id.as_str(),
    ]
    .iter()
    .all(|value| !value.trim().is_empty())
        && receipt.receipt_id
            == SkillUsageReceipt::stable_id(
                &receipt.skill_id,
                &receipt.skill_revision,
                receipt.usage,
                &receipt.workspace_identity,
                &receipt.workload_fingerprint,
                &receipt.config_revision,
                &receipt.evaluation_environment,
                &receipt.execution_id,
                &receipt.session_id,
                &receipt.turn_id,
            )
}

fn reduce_receipt(snapshot: &mut SkillMaintenanceSnapshot, receipt: SkillUsageReceipt) {
    let scope = MaintenanceScope::from(&receipt);
    let scope_key = digest_json(&scope);
    let scoped = snapshot
        .receipts
        .entry(scope_key)
        .or_insert_with(|| ScopedReceipts {
            scope,
            receipts: Vec::new(),
            outcomes: BTreeMap::new(),
        });
    let receipts = &mut scoped.receipts;
    if receipts
        .iter()
        .any(|existing| existing.receipt_id == receipt.receipt_id)
    {
        return;
    }
    receipts.push(receipt);
    receipts.sort_by(|left, right| {
        left.observed_at_ms
            .cmp(&right.observed_at_ms)
            .then_with(|| left.receipt_id.cmp(&right.receipt_id))
    });
    if receipts.len() > MAX_RECEIPTS_PER_SCOPE {
        receipts.drain(..receipts.len() - MAX_RECEIPTS_PER_SCOPE);
    }
    if let Some(outcome) = snapshot
        .outcomes
        .get(&receipts.last().expect("receipt").execution_id)
    {
        scoped
            .outcomes
            .insert(outcome.execution_id.clone(), outcome.clone());
    }
    let retained = receipts
        .iter()
        .map(|receipt| receipt.execution_id.as_str())
        .collect::<BTreeSet<_>>();
    scoped
        .outcomes
        .retain(|execution_id, _| retained.contains(execution_id.as_str()));
    while snapshot.receipts.len() > MAX_MAINTENANCE_SCOPES {
        let Some(oldest) = snapshot
            .receipts
            .iter()
            .min_by_key(|(_, scoped)| {
                scoped
                    .receipts
                    .last()
                    .map(|receipt| receipt.observed_at_ms)
                    .unwrap_or_default()
            })
            .map(|(scope_key, _)| scope_key.clone())
        else {
            break;
        };
        snapshot.receipts.remove(&oldest);
    }
}

fn reduce_outcome(snapshot: &mut SkillMaintenanceSnapshot, outcome: ExecutionOutcome) {
    let evidence = OutcomeEvidence::from(&outcome);
    snapshot
        .outcomes
        .insert(evidence.execution_id.clone(), evidence);
    let evidence = snapshot
        .outcomes
        .get(&outcome.identity.execution_id)
        .cloned()
        .expect("inserted Outcome evidence");
    for scoped in snapshot.receipts.values_mut() {
        if scoped
            .receipts
            .iter()
            .any(|receipt| receipt.execution_id == evidence.execution_id)
        {
            scoped
                .outcomes
                .insert(evidence.execution_id.clone(), evidence.clone());
        }
    }
    while snapshot.outcomes.len() > MAX_OUTCOMES {
        let Some(oldest) = snapshot
            .outcomes
            .iter()
            .min_by_key(|(_, outcome)| outcome.observed_at_ms)
            .map(|(execution_id, _)| execution_id.clone())
        else {
            break;
        };
        snapshot.outcomes.remove(&oldest);
    }
}

fn reduce_legacy(snapshot: &mut SkillMaintenanceSnapshot, payload: &serde_json::Value) -> bool {
    let Some(skill_id) = payload.get("skill_id").and_then(serde_json::Value::as_str) else {
        return false;
    };
    let Some(delta) = payload.get("delta") else {
        return false;
    };
    let counts = snapshot
        .legacy_counts
        .entry(skill_id.to_string())
        .or_default();
    counts.hits = counts.hits.saturating_add(
        delta
            .get("hits")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    );
    counts.misses = counts.misses.saturating_add(
        delta
            .get("misses")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    );
    counts.loads = counts.loads.saturating_add(
        delta
            .get("loads")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    );
    counts.failures = counts.failures.saturating_add(
        delta
            .get("failures")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
    );
    true
}

fn recompute_drafts(snapshot: &mut SkillMaintenanceSnapshot) {
    snapshot.drafts = snapshot
        .receipts
        .values()
        .map(|scoped| {
            let draft = build_draft(
                &scoped.scope,
                &scoped.receipts,
                &scoped.outcomes,
                snapshot
                    .legacy_counts
                    .get(&scoped.scope.skill_id)
                    .cloned()
                    .unwrap_or_default(),
            );
            (draft.draft_id.clone(), draft)
        })
        .collect();
}

fn build_draft(
    scope: &MaintenanceScope,
    receipts: &[SkillUsageReceipt],
    outcomes: &BTreeMap<String, OutcomeEvidence>,
    legacy_counts: SkillUsageCounts,
) -> SkillMaintenanceDraft {
    let mut canonical_counts = SkillUsageCounts::default();
    let mut receipt_ids = Vec::new();
    let mut execution_ids = BTreeSet::new();
    let mut created_at_ms = 0_u64;
    for receipt in receipts {
        canonical_counts.observe(receipt.usage);
        receipt_ids.push(receipt.receipt_id.clone());
        execution_ids.insert(receipt.execution_id.clone());
        created_at_ms = created_at_ms.max(receipt.observed_at_ms);
    }
    receipt_ids.sort();
    receipt_ids.dedup();
    let associated = execution_ids
        .iter()
        .filter_map(|execution_id| outcomes.get(execution_id))
        .collect::<Vec<_>>();
    let outcome_refs = associated
        .iter()
        .map(|outcome| format!("outcome:{}", outcome.execution_id))
        .collect::<Vec<_>>();
    let verified_success_count = associated
        .iter()
        .filter(|outcome| outcome.succeeded && !outcome.verification_blocked)
        .count() as u64;
    let terminal_failure_count = associated
        .iter()
        .filter(|outcome| !outcome.succeeded)
        .count() as u64;
    let missing_outcome_count =
        (execution_ids.len() as u64).saturating_sub(associated.len() as u64);
    let evidence_closed = receipts.len() < MAX_RECEIPTS_PER_SCOPE;
    let recommendation = if !evidence_closed {
        SkillMaintenanceRecommendation::Keep
    } else if canonical_counts.failures >= 3 && verified_success_count == 0 {
        SkillMaintenanceRecommendation::Deprecate
    } else if canonical_counts.failures >= 2
        || terminal_failure_count >= 2
        || (canonical_counts.misses >= 3 && canonical_counts.loads == 0)
    {
        SkillMaintenanceRecommendation::Revise
    } else {
        SkillMaintenanceRecommendation::Keep
    };
    let evidence_digest = digest_json(&serde_json::json!({
        "scope": scope,
        "receipts": receipts.iter().map(SkillUsageReceipt::digest).collect::<Vec<_>>(),
        "outcomes": associated,
    }));
    let scope_digest = digest_json(scope);
    let draft_id = format!("skill-maintenance-{}", &scope_digest[7..31]);
    let short_evidence = evidence_digest
        .strip_prefix("sha256:")
        .unwrap_or(&evidence_digest)
        .chars()
        .take(12)
        .collect::<String>();
    let proposed_revision = if recommendation == SkillMaintenanceRecommendation::Keep {
        scope.skill_revision.clone()
    } else {
        format!("{}+maintenance.{short_evidence}", scope.skill_revision)
    };
    let target = match recommendation {
        SkillMaintenanceRecommendation::Keep => {
            "Retain the current revision; continue collecting canonical evidence."
        }
        SkillMaintenanceRecommendation::Revise => {
            "Create and independently validate a new Skill package revision; do not mutate the active package."
        }
        SkillMaintenanceRecommendation::Deprecate => {
            "Prepare a replacement or disablement review after independent validation."
        }
        SkillMaintenanceRecommendation::Archive => {
            "Archive only after proving that no governed workload still depends on the Skill."
        }
    }
    .to_string();
    SkillMaintenanceDraft {
        draft_id,
        skill_id: scope.skill_id.clone(),
        base_revision: scope.skill_revision.clone(),
        proposed_revision,
        workspace_identity: scope.workspace_identity.clone(),
        workload_fingerprint: scope.workload_fingerprint.clone(),
        config_revision: scope.config_revision.clone(),
        evaluation_environment: scope.evaluation_environment.clone(),
        canonical_counts,
        legacy_counts,
        evidence_receipt_ids: receipt_ids,
        outcome_refs,
        evidence_digest,
        target,
        recommendation,
        validation: SkillMaintenanceValidation {
            receipt_schema_valid: true,
            evidence_closed,
            outcome_association_count: associated.len() as u64,
            verified_success_count,
            terminal_failure_count,
            missing_outcome_count,
            notes: vec![
                "Legacy Gateway counters are provenance-only and never affect recommendation."
                    .to_string(),
                "This Draft is inert until an inspected package revision passes a human review."
                    .to_string(),
            ],
        },
        created_at_ms,
        schema_version: SKILL_MAINTENANCE_DRAFT_SCHEMA_VERSION,
    }
}

fn digest_json(value: &impl Serialize) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        RuntimeEventInput, RuntimeEventRef, RuntimeEventScope, RuntimeTransactionEventInput,
        SkillInvocation,
    };
    use harness_contract::{
        outcome::{
            OutcomeIdentity, OutcomeObservation, OutcomeQuality, OutcomeStrategyFeedback,
            OutcomeTerminalClass, OutcomeTiming, OutcomeUsage, RuntimeIdentity, StrategyIdentity,
            OUTCOME_SCHEMA_REVISION,
        },
        reality::EvidenceCompleteness,
        skill::{SkillAdapterKind, SkillUsageKind},
        strategy::ExecutionCandidateKind,
    };

    fn receipt(index: u64, usage: SkillUsageKind) -> SkillUsageReceipt {
        let execution_id = format!("execution-{index}");
        let receipt_id = SkillUsageReceipt::stable_id(
            "review",
            "1.0.0",
            usage,
            "workspace",
            "workload",
            "config",
            "production",
            &execution_id,
            "session",
            &format!("turn-{index}"),
        );
        SkillUsageReceipt {
            receipt_id,
            skill_id: "review".to_string(),
            skill_revision: "1.0.0".to_string(),
            adapter: SkillAdapterKind::PromptOnly,
            usage,
            workspace_identity: "workspace".to_string(),
            workload_fingerprint: "workload".to_string(),
            config_revision: "config".to_string(),
            evaluation_environment: "production".to_string(),
            execution_id,
            session_id: "session".to_string(),
            turn_id: format!("turn-{index}"),
            observed_at_ms: index,
            schema_version: SKILL_USAGE_RECEIPT_SCHEMA_VERSION,
        }
    }

    fn outcome(index: u64, success: bool) -> ExecutionOutcome {
        ExecutionOutcome {
            identity: OutcomeIdentity {
                execution_id: format!("execution-{index}"),
                session_id: "session".to_string(),
                turn_id: format!("turn-{index}"),
                terminal_generation: 1,
                paired_sample_id: None,
                task_id: None,
                mission_id: None,
                agent_id: None,
                team_id: None,
                execution_graph_ref: None,
            },
            runtime: RuntimeIdentity {
                workspace_key: "workspace".to_string(),
                runtime_revision: "runtime".to_string(),
                config_revision: "config".to_string(),
            },
            provider: None,
            strategy: StrategyIdentity {
                decision_id: format!("decision-{index}"),
                policy_revision: "policy".to_string(),
                decision_source: "test".to_string(),
                selected_candidate: ExecutionCandidateKind::Direct,
                selected_pattern: "react".to_string(),
            },
            timing: OutcomeTiming {
                started_at_ms: 0,
                completed_at_ms: index,
                duration_ms: index,
            },
            usage: OutcomeUsage::default(),
            terminal: if success {
                OutcomeTerminalClass::Succeeded("ok".to_string())
            } else {
                OutcomeTerminalClass::Failed("failed".to_string())
            },
            quality: OutcomeQuality::Unknown,
            observation: OutcomeObservation {
                source: "test".to_string(),
                observed_at_ms: index,
                freshness_ms: 0,
            },
            strategy_feedback: OutcomeStrategyFeedback {
                workload: None,
                verification_blocked: !success,
                context_pressure: false,
                coordination_cost_ms: 0,
                evaluation_environment: "production".to_string(),
            },
            evidence_refs: Vec::new(),
            evidence_completeness: EvidenceCompleteness::Sufficient,
            schema_revision: OUTCOME_SCHEMA_REVISION,
        }
    }

    fn append_receipt(store: &RuntimeEventStore, receipt: &SkillUsageReceipt) {
        store
            .append(RuntimeEventInput {
                stream_id: format!("test:{}", receipt.receipt_id),
                scope: RuntimeEventScope::Skill,
                kind: SKILL_USAGE_RECEIPT_EVENT_KIND.to_string(),
                status: Some("observed".to_string()),
                actor: Some("test".to_string()),
                refs: vec![RuntimeEventRef {
                    kind: "skill".to_string(),
                    id: receipt.skill_id.clone(),
                }],
                payload: serde_json::json!({"receipt": receipt}),
            })
            .expect("receipt");
    }

    #[test]
    fn receipt_outcome_projection_is_replay_stable_and_legacy_is_non_authoritative() {
        let store = Arc::new(RuntimeEventStore::open_in_memory().expect("store"));
        for index in 1..=3 {
            append_receipt(&store, &receipt(index, SkillUsageKind::Failure));
            store
                .append(RuntimeEventInput {
                    stream_id: format!("outcome:execution-{index}"),
                    scope: RuntimeEventScope::Task,
                    kind: OUTCOME_EVENT_KIND.to_string(),
                    status: Some("failed".to_string()),
                    actor: Some("test".to_string()),
                    refs: Vec::new(),
                    payload: serde_json::to_value(outcome(index, false)).unwrap(),
                })
                .expect("outcome");
        }
        store
            .append(RuntimeEventInput {
                stream_id: "skill-usage:review".to_string(),
                scope: RuntimeEventScope::Skill,
                kind: LEGACY_USAGE_EVENT_KIND.to_string(),
                status: Some("observed".to_string()),
                actor: Some("legacy".to_string()),
                refs: Vec::new(),
                payload: serde_json::json!({
                    "skill_id": "review",
                    "delta": {"hits": 999, "misses": 0, "loads": 0, "failures": 0}
                }),
            })
            .expect("legacy");
        let first = SkillMaintenanceProjector::new(Arc::clone(&store));
        first.project_available(128).expect("projection");
        let draft = first.drafts(1).remove(0);
        assert_eq!(
            draft.recommendation,
            SkillMaintenanceRecommendation::Deprecate
        );
        assert_eq!(draft.legacy_counts.hits, 999);
        assert_eq!(draft.validation.outcome_association_count, 3);

        store
            .delete_projection_checkpoint(PROJECTOR_ID)
            .expect("delete checkpoint");
        let replay = SkillMaintenanceProjector::new(Arc::clone(&store));
        replay.project_available(128).expect("replay");
        assert_eq!(draft.digest(), replay.drafts(1).remove(0).digest());
    }

    #[test]
    fn draft_contract_has_no_executable_skill_payload() {
        let source = include_str!("maintenance.rs");
        let production = source.split("#[cfg(test)]").next().unwrap_or(source);
        for forbidden in [
            "RuntimeSkillCatalog",
            "load_instruction(",
            "install_skill",
            "std::fs::write",
            "Command::new",
        ] {
            assert!(
                !production.contains(forbidden),
                "maintenance Draft production path must not contain {forbidden}"
            );
        }
        let _unused_type_anchor: Option<SkillInvocation> = None;
        let _unused_event_anchor: Option<RuntimeTransactionEventInput> = None;
    }
}
