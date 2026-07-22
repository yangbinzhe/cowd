//! Runtime-owned Reality recall and Matrix scenario ports.
//!
//! Gateway may project the resulting receipts, but it does not assemble model
//! context. Every source is checked against the immutable Binding data lease
//! immediately before becoming a `ContextItem`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use fact_kernel::hypothesis::FactReality;
use fact_kernel::{Confidence, FactCandidate, FactScope, FactSource, SourceKind};
use harness_contract::agent::AgentBindingSnapshot;
use matrix_core::{MatrixScenarioResult, MatrixScenarioRun, MatrixScenarioSpec, MatrixSnapshotRef};
use matrix_repository::{open_matrix_sqlite_repository_handle, MatrixSqliteRepository};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use storage::{SqliteExecutor, StorageRegistry};

use crate::{ContextAuthority, ContextItem, ContextRole, ContextSourceKind, ContextVisibility};

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
#[derive(Debug, Clone)]
pub struct RealityRecallPort {
    config_home: PathBuf,
}

impl RealityRecallPort {
    #[must_use]
    pub fn for_config_home(config_home: impl Into<PathBuf>) -> Self {
        Self {
            config_home: config_home.into(),
        }
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

    #[must_use]
    pub fn matrix_scenarios(&self) -> MatrixScenarioPort {
        MatrixScenarioPort {
            config_home: self.config_home.clone(),
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
        let facts = match load_fact_records(&self.config_home) {
            Ok(facts) => facts,
            Err(error) => {
                return (
                    Vec::new(),
                    degraded_status(ContextSourceKind::Fact, error.to_string()),
                )
            }
        };
        let query_terms = query_terms(query);
        let granted_refs = lease.fact_refs.iter().collect::<BTreeSet<_>>();
        let granted_boundaries = lease.fact_boundaries.iter().collect::<BTreeSet<_>>();
        let mut items = facts
            .into_iter()
            .filter(|fact| fact_granted(fact, &granted_refs, &granted_boundaries))
            .filter(|fact| query_matches(&fact.statement, &query_terms))
            .map(fact_context_item)
            .collect::<Vec<_>>();
        items.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let omitted_count = items.len().saturating_sub(limit);
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
            .collect::<BTreeSet<_>>();
        if granted_snapshots.is_empty() {
            return (
                Vec::new(),
                disabled_status(
                    ContextSourceKind::Matrix,
                    "Binding grants no Matrix source snapshots",
                ),
            );
        }
        let repository = match open_matrix_repository(&self.config_home) {
            Ok(repository) => repository,
            Err(error) => {
                return (
                    Vec::new(),
                    degraded_status(ContextSourceKind::Matrix, error.to_string()),
                )
            }
        };
        let facts = match repository.list_facts(512) {
            Ok(facts) => facts,
            Err(error) => {
                return (
                    Vec::new(),
                    degraded_status(ContextSourceKind::Matrix, error.to_string()),
                )
            }
        };
        let query_terms = query_terms(query);
        let mut items = facts
            .into_iter()
            .filter(|fact| {
                granted_snapshots.contains(&format!("matrix:source_snapshot:{}", fact.snapshot_id))
            })
            .filter(|fact| matrix_query_matches(fact, &query_terms))
            .map(matrix_context_item)
            .collect::<Vec<_>>();
        items.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let omitted_count = items.len().saturating_sub(limit);
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
}

/// Matrix scenario command owned by Runtime. Gateway and surfaces may invoke
/// it through Runtime APIs but never receive a repository write handle.
#[derive(Debug, Clone)]
pub struct MatrixScenarioPort {
    config_home: PathBuf,
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
        let repository = open_matrix_repository(&self.config_home)?;
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
        let repository = open_matrix_repository(&self.config_home)?;
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
        let repository = open_matrix_repository(&self.config_home)?;
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
        .with_reality(FactReality::Simulated)
        .with_confidence(Confidence::from_basis_points(5_000))
        .with_payload(result.outputs.clone())
        .with_tags(vec![
            "matrix_scenario".to_string(),
            format!("scenario:{}", result.scenario_id),
            format!("result:{}", result.result_id),
        ]))
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

fn open_matrix_repository(config_home: &Path) -> Result<MatrixSqliteRepository, String> {
    let registry = StorageRegistry::default_for_config_home(config_home);
    let handle = registry
        .endpoint(&storage::StorageDomainId::Matrix)
        .map(storage::StorageEndpoint::as_handle)
        .map_err(|error| error.to_string())?;
    if let Some(parent) = handle.path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    open_matrix_sqlite_repository_handle(&handle).map_err(|error| error.to_string())
}

fn load_fact_records(config_home: &Path) -> Result<Vec<fact_kernel::FactRecord>, String> {
    let registry = StorageRegistry::default_for_config_home(config_home);
    let endpoint = registry
        .endpoint(&storage::StorageDomainId::Fact)
        .map_err(|error| error.to_string())?;
    if !endpoint.path.exists() {
        return Ok(Vec::new());
    }
    let connection = SqliteExecutor::for_endpoint(endpoint)
        .and_then(|executor| executor.checkout())
        .map_err(|error| error.to_string())?;
    let table_exists = connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'fact_records'",
            [],
            |_| Ok(()),
        )
        .optional()
        .map_err(|error| error.to_string())?
        .is_some();
    if !table_exists {
        return Ok(Vec::new());
    }
    let mut statement = connection
        .prepare("SELECT payload_json FROM fact_records ORDER BY updated_at DESC LIMIT 512")
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map([], |row| row.get::<_, String>(0))
        .map_err(|error| error.to_string())?;
    rows.map(|row| {
        let payload = row.map_err(|error| error.to_string())?;
        serde_json::from_str(&payload).map_err(|error| error.to_string())
    })
    .collect()
}

fn fact_granted(
    fact: &fact_kernel::FactRecord,
    granted_refs: &BTreeSet<&String>,
    granted_boundaries: &BTreeSet<&String>,
) -> bool {
    let reference = format!("fact:{}", fact.id.as_str());
    let direct = granted_refs.contains(&reference);
    let boundary = fact_boundary(fact);
    let boundary_granted = granted_boundaries.contains(&boundary.to_string());
    direct || boundary_granted
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
    item.authority = ContextAuthority::Project;
    item.visibility = ContextVisibility::Private;
    item.score = fact.confidence.basis_points() as f32 / 10_000.0;
    item.source_id = Some(format!("fact:{}", fact.id.as_str()));
    item.source_version = Some(fact.updated_at.to_rfc3339());
    item.source_reason = Some(format!(
        "{} fact under Binding data lease",
        fact_boundary(&fact)
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
    item.authority = ContextAuthority::Project;
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

fn matrix_query_matches(fact: &matrix_core::MatrixFact, terms: &[String]) -> bool {
    let searchable = format!(
        "{} {} {} {} {}",
        fact.fact_type,
        fact.metric_key.as_deref().unwrap_or_default(),
        fact.source_ref.as_deref().unwrap_or_default(),
        fact.dimensions,
        fact.measures,
    );
    query_matches(&searchable, terms)
}

fn query_terms(query: &str) -> Vec<String> {
    query
        .split(|character: char| !character.is_alphanumeric() && !character.is_alphabetic())
        .map(str::trim)
        .filter(|term| term.chars().count() > 1)
        .map(str::to_lowercase)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn query_matches(value: &str, terms: &[String]) -> bool {
    if terms.is_empty() {
        return true;
    }
    let value = value.to_lowercase();
    terms.iter().any(|term| value.contains(term))
}

fn fact_boundary(fact: &fact_kernel::FactRecord) -> &'static str {
    match fact.status.as_str() {
        "active" | "validated" => "observed",
        "candidate" | "held" => "inferred",
        "conflict" | "conflicted" => "conflict",
        _ => "hypothetical",
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use harness_contract::agent::{
        AgentCapability, AgentDefinitionId, AgentDefinitionRevisionRef, AgentExecutorPolicy,
        AgentInstanceRef, AgentModelPolicy, CognitiveReadScope, CognitiveWriteMode,
    };
    use matrix_core::{
        MatrixFact, MatrixFactInput, MatrixScenarioOutputContract, MatrixSourceKind,
        MatrixSourceSnapshotInput,
    };
    use storage::{SqliteExecutor, StorageRegistry};

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
        assert_eq!(candidate.reality, FactReality::Simulated);
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

        let fact_handle = registry
            .endpoint(&storage::StorageDomainId::Fact)
            .unwrap()
            .as_handle();
        std::fs::create_dir_all(fact_handle.path.parent().unwrap()).unwrap();
        let connection = SqliteExecutor::for_handle(&fact_handle)
            .unwrap()
            .checkout()
            .unwrap();
        connection
            .execute_batch(
                "CREATE TABLE fact_records (
                    fact_id TEXT PRIMARY KEY,
                    fact_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );",
            )
            .unwrap();
        let mut fact = fact_kernel::FactRecord::new(
            "supply.policy",
            "east region requires an expedited allocation",
        );
        fact.id = fact_kernel::FactId::from_string("fact-recall-policy");
        fact.confidence = Confidence::from_basis_points(9_400);
        connection
            .execute(
                "INSERT INTO fact_records (fact_id, fact_type, status, payload_json, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![
                    fact.id.as_str(),
                    &fact.fact_type,
                    &fact.status,
                    serde_json::to_string(&fact).unwrap(),
                    fact.updated_at.to_rfc3339(),
                ],
            )
            .unwrap();

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
    }
}
