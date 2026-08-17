//! Runtime-owned Reality recall and Matrix scenario ports.
//!
//! Gateway may project the resulting receipts, but it does not assemble model
//! context. Every source is checked against the immutable Binding data lease
//! immediately before becoming a `ContextItem`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use fact_kernel::{
    Confidence, FactCandidate, FactLedger, FactRecallQuery, FactScope, FactSource, SourceKind,
    UnavailableFactLedger,
};
use harness_contract::agent::{AgentBindingSnapshot, CognitiveReadScope};
use harness_contract::reality::RealityBoundary;
use matrix_core::{MatrixScenarioResult, MatrixScenarioRun, MatrixScenarioSpec, MatrixSnapshotRef};
#[cfg(test)]
use matrix_repository::open_matrix_sqlite_repository_handle;
use matrix_repository::{MatrixRecallQuery, MatrixStore, MatrixStoreHandle};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::StorageRegistry;

use crate::{ContextItem, ContextRole, ContextSourceKind, ContextVisibility};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RealityRecallSourceStatus {
    pub source: ContextSourceKind,
    pub status: String,
    pub selected_count: usize,
    pub omitted_count: usize,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RealityRecallReport {
    pub items: Vec<ContextItem>,
    pub sources: Vec<RealityRecallSourceStatus>,
}

impl RealityRecallReport {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            sources: Vec::new(),
        }
    }
}

/// Read-only Runtime adapter over the durable Fact and Matrix stores. It is
/// intentionally stateless so every request must carry a Binding snapshot.
#[derive(Clone)]
pub struct RealityRecallPort {
    fact_ledger: Arc<dyn FactLedger>,
    matrix_store: Option<Arc<dyn MatrixStore>>,
    matrix_store_error: Option<String>,
    project_scope_key: Option<String>,
}

impl RealityRecallPort {
    #[must_use]
    pub fn for_config_home(config_home: impl Into<PathBuf>) -> Self {
        let config_home = config_home.into();
        let ledger = StorageRegistry::default_for_config_home(&config_home)
            .endpoint(&storage::StorageDomainId::Fact)
            .map_err(|error| error.to_string())
            .and_then(|endpoint| {
                fact_sqlite::SqliteFactLedger::open(endpoint).map_err(|error| error.to_string())
            })
            .map(|ledger| Arc::new(ledger) as Arc<dyn FactLedger>)
            .unwrap_or_else(|error| Arc::new(UnavailableFactLedger::new(error)));
        let (matrix_store, matrix_store_error) = matrix_store_for_config_home(&config_home)
            .map_or_else(|error| (None, Some(error)), |store| (Some(store), None));
        Self {
            fact_ledger: ledger,
            matrix_store,
            matrix_store_error,
            project_scope_key: None,
        }
    }

    /// Compose the local adapters with the canonical project scope derived
    /// from the Runtime workspace rather than treating config-home as a data
    /// authorization boundary.
    #[must_use]
    pub fn for_config_home_and_workspace(
        config_home: impl Into<PathBuf>,
        workspace_root: impl AsRef<Path>,
    ) -> Self {
        Self::for_config_home(config_home).with_workspace_scope(workspace_root)
    }

    /// Compose Runtime against a prevalidated Fact ledger. PostgreSQL/global
    /// composition injects its adapter here; Runtime never chooses a driver
    /// or opens a fact database on the recall path.
    #[must_use]
    pub fn with_fact_ledger(
        config_home: impl Into<PathBuf>,
        fact_ledger: Arc<dyn FactLedger>,
    ) -> Self {
        let config_home = config_home.into();
        let (matrix_store, matrix_store_error) = matrix_store_for_config_home(&config_home)
            .map_or_else(|error| (None, Some(error)), |store| (Some(store), None));
        Self {
            fact_ledger,
            matrix_store,
            matrix_store_error,
            project_scope_key: None,
        }
    }

    /// Compose Runtime against prevalidated durable Fact and Matrix ports.
    /// Global selected-backend composition injects both adapters here; Runtime
    /// never opens a Matrix concrete repository on a recall/scenario path.
    #[must_use]
    pub fn with_fact_and_matrix_store(
        _config_home: impl Into<PathBuf>,
        fact_ledger: Arc<dyn FactLedger>,
        matrix_store: Arc<dyn MatrixStore>,
    ) -> Self {
        Self {
            fact_ledger,
            matrix_store: Some(matrix_store),
            matrix_store_error: None,
            project_scope_key: None,
        }
    }

    #[must_use]
    pub fn with_workspace_scope(mut self, workspace_root: impl AsRef<Path>) -> Self {
        self.project_scope_key = Some(
            FactScope::Project(crate::memory_project_id_for_workspace(
                workspace_root.as_ref(),
            ))
            .key(),
        );
        self
    }

    /// Recall durable Fact and Matrix evidence under an immutable Binding.
    /// Empty grants do not fall back to global recall: source status remains
    /// visible while no data crosses the lease boundary.
    #[must_use]
    pub fn recall_for_binding(
        &self,
        binding: &AgentBindingSnapshot,
        query: &str,
        limit: usize,
    ) -> RealityRecallReport {
        if let Err(error) = binding.validate() {
            return invalid_binding_report(error.to_string());
        }
        let limit = limit.clamp(1, 64);
        let mut report = RealityRecallReport::empty();
        let fact_status = self.recall_facts(binding, query, limit);
        report.items.extend(fact_status.0);
        report.sources.push(fact_status.1);
        let matrix_status = self.recall_matrix(binding, query, limit);
        report.items.extend(matrix_status.0);
        report.sources.push(matrix_status.1);
        report.items.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
        report.items.truncate(limit);
        report
    }

    /// Execute synchronous repository adapters outside Tokio's worker pool.
    ///
    /// Fact and Matrix repositories intentionally expose synchronous domain
    /// ports today. Runtime callers remain async-first and share Tokio's
    /// bounded blocking pool rather than blocking the conversation executor.
    pub async fn recall_for_binding_async(
        &self,
        binding: &AgentBindingSnapshot,
        query: &str,
        limit: usize,
    ) -> RealityRecallReport {
        if let Err(error) = binding.validate() {
            return invalid_binding_report(error.to_string());
        }
        let limit = limit.clamp(1, 64);
        let fact_port = self.clone();
        let fact_binding = binding.clone();
        let fact_query = query.to_string();
        let matrix_port = self.clone();
        let matrix_binding = binding.clone();
        let matrix_query = query.to_string();
        let (facts, matrix) = tokio::join!(
            tokio::task::spawn_blocking(move || {
                fact_port.recall_facts(&fact_binding, &fact_query, limit)
            }),
            tokio::task::spawn_blocking(move || {
                matrix_port.recall_matrix(&matrix_binding, &matrix_query, limit)
            })
        );
        let facts = facts.unwrap_or_else(|error| {
            (
                Vec::new(),
                degraded_status(
                    ContextSourceKind::Fact,
                    format!("Fact recall worker failed: {error}"),
                ),
            )
        });
        let matrix = matrix.unwrap_or_else(|error| {
            (
                Vec::new(),
                degraded_status(
                    ContextSourceKind::Matrix,
                    format!("Matrix recall worker failed: {error}"),
                ),
            )
        });
        let mut report = RealityRecallReport {
            items: facts.0,
            sources: vec![facts.1],
        };
        report.items.extend(matrix.0);
        report.sources.push(matrix.1);
        report.items.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.id.cmp(&right.id))
        });
        report.items.truncate(limit);
        report
    }

    #[must_use]
    pub fn matrix_scenarios(&self) -> MatrixScenarioPort {
        MatrixScenarioPort {
            matrix_store: self.matrix_store.clone(),
            matrix_store_error: self.matrix_store_error.clone(),
        }
    }

    fn recall_facts(
        &self,
        binding: &AgentBindingSnapshot,
        query: &str,
        limit: usize,
    ) -> (Vec<ContextItem>, RealityRecallSourceStatus) {
        let lease = &binding.data_lease;
        if lease.fact_refs.is_empty() && lease.fact_boundaries.is_empty() {
            return (
                Vec::new(),
                disabled_status(
                    ContextSourceKind::Fact,
                    "Binding grants no Fact references or boundaries",
                ),
            );
        }
        // The current task is the irreducible execution boundary. Wider Fact
        // scopes require the matching cognitive read grant as well as a
        // Reality boundary grant.
        let mut authorized_scope_keys = vec![FactScope::Task(lease.task_id.clone()).key()];
        if lease.read_scopes.contains(&CognitiveReadScope::Session) {
            authorized_scope_keys.push(FactScope::Session(lease.session_id.clone()).key());
        }
        if lease.read_scopes.contains(&CognitiveReadScope::Team) {
            if let Some(team_id) = &lease.team_id {
                authorized_scope_keys.push(FactScope::Team(team_id.clone()).key());
            }
        }
        if lease.read_scopes.contains(&CognitiveReadScope::Project)
            || lease
                .read_scopes
                .contains(&CognitiveReadScope::WorkspaceKnowledge)
        {
            if let Some(project_scope_key) = &self.project_scope_key {
                authorized_scope_keys.push(project_scope_key.clone());
            }
        }
        let recall_query = FactRecallQuery::new(
            lease
                .fact_refs
                .iter()
                .filter_map(|reference| reference.strip_prefix("fact:"))
                .map(str::to_string)
                .collect(),
            authorized_scope_keys,
            lease.fact_boundaries.clone(),
            query,
            limit.saturating_add(1),
        );
        let facts = match self.fact_ledger.recall_facts(&recall_query) {
            Ok(facts) => facts,
            Err(error) => {
                return (
                    Vec::new(),
                    degraded_status(ContextSourceKind::Fact, error.to_string()),
                )
            }
        };
        let mut items = facts.into_iter().map(fact_context_item).collect::<Vec<_>>();
        let omitted_count = usize::from(items.len() > limit);
        items.truncate(limit);
        let selected_count = items.len();
        (
            items,
            RealityRecallSourceStatus {
                source: ContextSourceKind::Fact,
                status: "enabled_and_wired".to_string(),
                selected_count,
                omitted_count,
                detail: None,
            },
        )
    }

    fn recall_matrix(
        &self,
        binding: &AgentBindingSnapshot,
        query: &str,
        limit: usize,
    ) -> (Vec<ContextItem>, RealityRecallSourceStatus) {
        let granted_snapshots = binding
            .data_lease
            .matrix_snapshot_refs
            .iter()
            .filter_map(|reference| reference.strip_prefix("matrix:source_snapshot:"))
            .map(str::to_string)
            .collect::<Vec<_>>();
        if granted_snapshots.is_empty() {
            return (
                Vec::new(),
                disabled_status(
                    ContextSourceKind::Matrix,
                    "Binding grants no Matrix source snapshots",
                ),
            );
        }
        let repository = match self.matrix_store() {
            Ok(repository) => repository,
            Err(error) => {
                return (
                    Vec::new(),
                    degraded_status(ContextSourceKind::Matrix, error.to_string()),
                )
            }
        };
        let recall_query =
            MatrixRecallQuery::new(granted_snapshots, query, limit.saturating_add(1));
        let facts = match repository.recall_facts(&recall_query) {
            Ok(facts) => facts,
            Err(error) => {
                return (
                    Vec::new(),
                    degraded_status(ContextSourceKind::Matrix, error.to_string()),
                )
            }
        };
        let mut items = facts
            .into_iter()
            .map(matrix_context_item)
            .collect::<Vec<_>>();
        let omitted_count = usize::from(items.len() > limit);
        items.truncate(limit);
        let selected_count = items.len();
        (
            items,
            RealityRecallSourceStatus {
                source: ContextSourceKind::Matrix,
                status: "enabled_and_wired".to_string(),
                selected_count,
                omitted_count,
                detail: None,
            },
        )
    }

    fn matrix_store(&self) -> Result<&Arc<dyn MatrixStore>, String> {
        self.matrix_store.as_ref().ok_or_else(|| {
            self.matrix_store_error
                .clone()
                .unwrap_or_else(|| "Matrix store is unavailable".to_string())
        })
    }
}

/// Matrix scenario command owned by Runtime. Gateway and surfaces may invoke
/// it through Runtime APIs but never receive a repository write handle.
#[derive(Clone)]
pub struct MatrixScenarioPort {
    matrix_store: Option<Arc<dyn MatrixStore>>,
    matrix_store_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixScenarioStartRequest {
    pub spec: MatrixScenarioSpec,
    pub parameters: Value,
}

impl MatrixScenarioPort {
    pub fn start(
        &self,
        binding: &AgentBindingSnapshot,
        request: MatrixScenarioStartRequest,
    ) -> Result<MatrixScenarioRun, String> {
        ensure_matrix_snapshot_granted(binding, &request.spec.base_snapshot)?;
        let repository = self.matrix_store()?;
        let spec = repository
            .create_scenario_spec(request.spec)
            .map_err(|error| error.to_string())?;
        repository
            .start_scenario_run(&spec.scenario_id, request.parameters)
            .map_err(|error| error.to_string())
    }

    pub fn complete(
        &self,
        binding: &AgentBindingSnapshot,
        result: MatrixScenarioResult,
    ) -> Result<MatrixScenarioResult, String> {
        let repository = self.matrix_store()?;
        let run = repository
            .get_scenario_run(&result.run_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("Matrix scenario run not found: {}", result.run_id))?;
        ensure_matrix_snapshot_granted(binding, &run.base_snapshot)?;
        repository
            .complete_scenario_run(result)
            .map_err(|error| error.to_string())
    }

    /// Translate a completed simulation into a *candidate* only. The caller
    /// must submit it to Fact/Memory governance; this port cannot persist an
    /// observed Fact or directly mutate Memory.
    pub fn fact_candidate(
        &self,
        binding: &AgentBindingSnapshot,
        result: &MatrixScenarioResult,
        statement: impl Into<String>,
    ) -> Result<FactCandidate, String> {
        let repository = self.matrix_store()?;
        let run = repository
            .get_scenario_run(&result.run_id)
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("Matrix scenario run not found: {}", result.run_id))?;
        ensure_matrix_snapshot_granted(binding, &run.base_snapshot)?;
        result
            .validate_for_run(&run)
            .map_err(|error| format!("invalid Matrix scenario result: {error}"))?;
        let source = FactSource {
            kind: SourceKind::Simulation,
            id: format!("matrix:scenario_result:{}", result.result_id),
            label: Some("runtime matrix scenario".to_string()),
        };
        Ok(FactCandidate::observed(
            "matrix.scenario",
            statement,
            FactScope::Task(binding.data_lease.task_id.clone()),
            source,
        )
        .with_reality(RealityBoundary::Simulated)
        .with_confidence(Confidence::from_basis_points(5_000))
        .with_payload(result.outputs.clone())
        .with_tags(vec![
            "matrix_scenario".to_string(),
            format!("scenario:{}", result.scenario_id),
            format!("result:{}", result.result_id),
        ]))
    }

    fn matrix_store(&self) -> Result<&Arc<dyn MatrixStore>, String> {
        self.matrix_store.as_ref().ok_or_else(|| {
            self.matrix_store_error
                .clone()
                .unwrap_or_else(|| "Matrix store is unavailable".to_string())
        })
    }
}

fn ensure_matrix_snapshot_granted(
    binding: &AgentBindingSnapshot,
    snapshot: &MatrixSnapshotRef,
) -> Result<(), String> {
    snapshot.validate()?;
    if !binding
        .data_lease
        .matrix_snapshot_refs
        .iter()
        .any(|reference| reference == &snapshot.snapshot_ref)
    {
        return Err(format!(
            "Matrix snapshot `{}` is outside binding `{}` data lease",
            snapshot.snapshot_ref, binding.binding_id
        ));
    }
    Ok(())
}

fn matrix_store_for_config_home(config_home: &Path) -> Result<Arc<dyn MatrixStore>, String> {
    let registry = StorageRegistry::default_for_config_home(config_home);
    let endpoint = registry
        .endpoint(&storage::StorageDomainId::Matrix)
        .cloned()
        .map_err(|error| error.to_string())?;
    MatrixStoreHandle::new(endpoint)
        .open()
        .map_err(|error| error.to_string())
}

fn fact_context_item(fact: fact_kernel::FactRecord) -> ContextItem {
    let mut item = ContextItem::new(
        format!("fact:{}", fact.id.as_str()),
        ContextSourceKind::Fact,
        ContextRole::Evidence,
        format!(
            "Fact type: {}\nStatement: {}",
            fact.fact_type, fact.statement
        ),
    );
    item.authority = crate::context_authority_for_reality_boundary(fact.boundary);
    item.visibility = ContextVisibility::Private;
    item.score = fact
        .confidence
        .basis_points()
        .map_or(0.0, |value| f32::from(value) / 10_000.0);
    item.source_id = Some(format!("fact:{}", fact.id.as_str()));
    item.source_version = Some(fact.updated_at.to_rfc3339());
    item.source_reason = Some(format!(
        "{} fact under Binding data lease",
        fact.boundary.as_str()
    ));
    item.evidence = fact
        .evidence
        .iter()
        .map(|evidence| format!("fact:evidence:{}", evidence.as_str()))
        .collect();
    item
}

fn matrix_context_item(fact: matrix_core::MatrixFact) -> ContextItem {
    let snapshot_ref = format!("matrix:source_snapshot:{}", fact.snapshot_id);
    let mut item = ContextItem::new(
        format!("matrix:fact:{}", fact.fact_id),
        ContextSourceKind::Matrix,
        ContextRole::Evidence,
        format!(
            "Matrix fact type: {}\nSnapshot: {}\nDimensions: {}\nMeasures: {}",
            fact.fact_type,
            snapshot_ref,
            compact_json(&fact.dimensions, 480),
            compact_json(&fact.measures, 480),
        ),
    );
    item.authority = crate::context_authority_for_reality_boundary(RealityBoundary::Observed);
    item.visibility = ContextVisibility::Private;
    item.score = fact.confidence.clamp(0.0, 1.0);
    item.source_id = Some(format!("matrix:fact:{}", fact.fact_id));
    item.source_version = Some(snapshot_ref.clone());
    item.source_reason = Some("observed Matrix fact under Binding snapshot lease".to_string());
    item.evidence = std::iter::once(snapshot_ref)
        .chain(fact.source_ref)
        .collect();
    item
}

fn compact_json(value: &Value, limit: usize) -> String {
    let rendered = value.to_string();
    if rendered.chars().count() <= limit {
        rendered
    } else {
        format!("{}...", rendered.chars().take(limit).collect::<String>())
    }
}

fn disabled_status(
    source: ContextSourceKind,
    detail: impl Into<String>,
) -> RealityRecallSourceStatus {
    RealityRecallSourceStatus {
        source,
        status: "disabled_by_binding".to_string(),
        selected_count: 0,
        omitted_count: 0,
        detail: Some(detail.into()),
    }
}

fn degraded_status(
    source: ContextSourceKind,
    detail: impl Into<String>,
) -> RealityRecallSourceStatus {
    RealityRecallSourceStatus {
        source,
        status: "degraded".to_string(),
        selected_count: 0,
        omitted_count: 0,
        detail: Some(detail.into()),
    }
}

fn invalid_binding_report(detail: String) -> RealityRecallReport {
    RealityRecallReport {
        items: Vec::new(),
        sources: vec![
            degraded_status(
                ContextSourceKind::Fact,
                format!("invalid Agent Binding: {detail}"),
            ),
            degraded_status(
                ContextSourceKind::Matrix,
                format!("invalid Agent Binding: {detail}"),
            ),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use fact_kernel::FactLedger;
    use harness_contract::agent::{
        AgentCapability, AgentDefinitionId, AgentDefinitionRevisionRef, AgentExecutorPolicy,
        AgentInstanceRef, AgentModelPolicy, CognitiveReadScope, CognitiveWriteMode,
    };
    use matrix_core::{
        MatrixFact, MatrixFactInput, MatrixScenarioOutputContract, MatrixSourceKind,
        MatrixSourceSnapshotInput,
    };
    use storage::StorageRegistry;

    fn binding(config_home: &Path, snapshot_ref: Option<String>) -> AgentBindingSnapshot {
        let _ = config_home;
        AgentBindingSnapshot {
            binding_id: "binding:test".to_string(),
            definition_ref: AgentDefinitionRevisionRef::new(
                AgentDefinitionId::try_from("builtin/cowd/researcher").unwrap(),
                1,
            )
            .unwrap(),
            definition_digest: "a".repeat(64),
            instructions: "test instructions".to_string(),
            instance: AgentInstanceRef {
                instance_id: "instance:test".to_string(),
                role_slot_id: None,
            },
            executor: AgentExecutorPolicy::CowdNative,
            model_policy: AgentModelPolicy {
                profile: "test".to_string(),
                allowed_models: vec!["test".to_string()],
                fallback_allowed: false,
            },
            effective_capabilities: vec![AgentCapability::Read],
            skill_refs: Vec::new(),
            tool_contract_refs: Vec::new(),
            data_lease: harness_contract::agent::AgentDataLease {
                session_id: "session:test".to_string(),
                task_id: "task:test".to_string(),
                team_id: None,
                read_scopes: vec![CognitiveReadScope::Session],
                write_mode: CognitiveWriteMode::CandidateOnly,
                team_working_state_visible: false,
                fact_boundaries: Vec::new(),
                fact_refs: Vec::new(),
                matrix_snapshot_refs: snapshot_ref.into_iter().collect(),
            },
            release: None,
            evaluation: None,
            display: None,
            binding_digest: "b".repeat(64),
        }
    }

    #[test]
    fn matrix_scenario_port_refuses_unleased_snapshot_and_returns_candidate_only() {
        let home = tempfile::tempdir().unwrap();
        let registry = StorageRegistry::default_for_config_home(home.path());
        let handle = registry
            .endpoint(&storage::StorageDomainId::Matrix)
            .unwrap()
            .as_handle();
        std::fs::create_dir_all(handle.path.parent().unwrap()).unwrap();
        let repository = open_matrix_sqlite_repository_handle(&handle).unwrap();
        let snapshot = repository
            .create_source_snapshot(MatrixSourceSnapshotInput {
                snapshot_id: Some("scenario-port-snapshot".to_string()),
                source_pack_id: None,
                source_system: "fixture".to_string(),
                source_kind: MatrixSourceKind::Manual,
                resource_ref: Some("fixture://matrix".to_string()),
                business_period: None,
                captured_at: Some(Utc::now()),
                schema_version: Some("v1".to_string()),
                row_count: Some(1),
                checksum: None,
                confidence: Some(1.0),
                metadata: Value::Null,
            })
            .unwrap();
        let snapshot_ref = MatrixSnapshotRef::from_source_snapshot(&snapshot);
        let port = RealityRecallPort::for_config_home(home.path()).matrix_scenarios();
        let denied = binding(home.path(), None);
        let request = MatrixScenarioStartRequest {
            spec: MatrixScenarioSpec::new(
                snapshot_ref.clone(),
                serde_json::json!({"shift": 0.2}),
                "runtime/scenario/test@1",
                MatrixScenarioOutputContract {
                    required_outputs: vec!["risk".to_string()],
                    evidence_required: true,
                },
            ),
            parameters: Value::Null,
        };
        assert!(port.start(&denied, request.clone()).is_err());

        let granted = binding(home.path(), Some(snapshot_ref.snapshot_ref.clone()));
        let run = port.start(&granted, request).unwrap();
        let result = port
            .complete(
                &granted,
                MatrixScenarioResult::simulated(
                    &run,
                    serde_json::json!({"risk": "high"}),
                    vec![snapshot_ref.snapshot_ref],
                ),
            )
            .unwrap();
        let candidate = port
            .fact_candidate(&granted, &result, "simulated risk is high")
            .unwrap();
        assert_eq!(candidate.reality, RealityBoundary::Simulated);
        assert_eq!(candidate.status, fact_kernel::FactStatus::Candidate);
    }

    #[test]
    fn recall_port_injects_only_binding_leased_fact_and_matrix_evidence() {
        let home = tempfile::tempdir().unwrap();
        let registry = StorageRegistry::default_for_config_home(home.path());
        let matrix_handle = registry
            .endpoint(&storage::StorageDomainId::Matrix)
            .unwrap()
            .as_handle();
        std::fs::create_dir_all(matrix_handle.path.parent().unwrap()).unwrap();
        let repository = open_matrix_sqlite_repository_handle(&matrix_handle).unwrap();
        let snapshot = repository
            .create_source_snapshot(MatrixSourceSnapshotInput {
                snapshot_id: Some("recall-port-snapshot".to_string()),
                source_pack_id: None,
                source_system: "fixture".to_string(),
                source_kind: MatrixSourceKind::Manual,
                resource_ref: Some("fixture://matrix".to_string()),
                business_period: None,
                captured_at: Some(Utc::now()),
                schema_version: Some("v1".to_string()),
                row_count: Some(1),
                checksum: None,
                confidence: Some(0.96),
                metadata: Value::Null,
            })
            .unwrap();
        repository
            .ingest_fact(&MatrixFact::from_input(MatrixFactInput {
                fact_id: Some("matrix-recall-fact".to_string()),
                snapshot_id: Some(snapshot.snapshot_id.clone()),
                fact_type: "supply.shortage".to_string(),
                entity_refs: vec!["matrix:entity:supplier-east".to_string()],
                metric_key: Some("shortage_risk".to_string()),
                dimensions: serde_json::json!({"region": "east"}),
                measures: serde_json::json!({"shortage_days": 12}),
                event_time: Some(Utc::now()),
                valid_from: None,
                valid_to: None,
                source_ref: Some(snapshot.reference()),
                confidence: Some(0.96),
                raw_hash: None,
            }))
            .unwrap();

        let fact_endpoint = registry.endpoint(&storage::StorageDomainId::Fact).unwrap();
        let fact_ledger = fact_sqlite::SqliteFactLedger::open(fact_endpoint).unwrap();
        let mut fact = fact_kernel::FactRecord::new(
            "supply.policy",
            "east region requires an expedited allocation",
        );
        fact.id = fact_kernel::FactId::from_string("fact-recall-policy");
        fact.confidence = Confidence::from_basis_points(9_400);
        fact_ledger.upsert_fact(fact).unwrap();

        let mut leased = binding(
            home.path(),
            Some(MatrixSnapshotRef::from_source_snapshot(&snapshot).snapshot_ref),
        );
        leased.data_lease.fact_refs = vec!["fact:fact-recall-policy".to_string()];
        let report = RealityRecallPort::for_config_home(home.path()).recall_for_binding(
            &leased,
            "east shortage allocation",
            12,
        );
        assert!(report
            .items
            .iter()
            .any(|item| item.source == ContextSourceKind::Fact));
        assert!(report
            .items
            .iter()
            .any(|item| item.source == ContextSourceKind::Matrix));

        leased.data_lease.fact_refs.clear();
        leased.data_lease.matrix_snapshot_refs.clear();
        let denied = RealityRecallPort::for_config_home(home.path()).recall_for_binding(
            &leased,
            "east shortage allocation",
            12,
        );
        assert!(denied.items.is_empty());
        assert!(denied
            .sources
            .iter()
            .all(|source| source.status == "disabled_by_binding"));

        leased.data_lease.fact_refs = vec!["not-a-fact-reference".to_string()];
        let invalid = RealityRecallPort::for_config_home(home.path()).recall_for_binding(
            &leased,
            "east shortage allocation",
            12,
        );
        assert!(invalid.items.is_empty());
        assert!(invalid
            .sources
            .iter()
            .all(|source| source.status == "degraded"));
    }
}
