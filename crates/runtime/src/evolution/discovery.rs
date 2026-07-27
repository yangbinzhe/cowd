//! Event-sourced discovery side of Runtime evolution.
//!
//! Signals, diagnoses, missions and proposals share the Runtime Event Store
//! with candidate evaluation and release governance. Gateway is only a typed
//! command/query adapter and never opens a second file-backed registry.

use std::collections::BTreeMap;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::{
    candidate_kinds_from_root_cause, EvolutionCapabilityGoal, EvolutionDiagnosis,
    EvolutionDiagnosisEngine, EvolutionLifecycleDraft, EvolutionMission, EvolutionProposal,
    EvolutionSignal,
};
use crate::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore, RuntimeTransactionEventInput, VerifiedDecisionLease,
    VerifiedPrincipal,
};

const SIGNAL_PREFIX: &str = "evolution:signal:";
const DIAGNOSIS_PREFIX: &str = "evolution:diagnosis:";
const MISSION_PREFIX: &str = "evolution:mission:";
const PROPOSAL_PREFIX: &str = "evolution:proposal:";

#[derive(Debug)]
pub(crate) struct EvolutionDiscoveryService {
    event_store: Arc<RuntimeEventStore>,
}

impl EvolutionDiscoveryService {
    #[must_use]
    pub(crate) fn new(event_store: Arc<RuntimeEventStore>) -> Self {
        Self { event_store }
    }

    pub(crate) fn record_signal(&self, signal: EvolutionSignal) -> Result<EvolutionSignal, String> {
        validate_signal(&signal)?;
        if let Some(existing) = self.signal(&signal.signal_id)? {
            if existing == signal {
                return Ok(existing);
            }
            return Err(format!(
                "evolution signal idempotency conflict: {}",
                signal.signal_id
            ));
        }
        let stream = signal_stream(&signal.signal_id);
        self.event_store
            .append_batch_if_revision(
                stream.clone(),
                self.stream_revision(&stream)?,
                format!("evolution-signal:{}", signal.signal_id),
                vec![evolution_event(
                    stream,
                    "evolution.signal.recorded.v1",
                    Some(signal.severity_label()),
                    vec![RuntimeEventRef {
                        kind: "evolution_signal".to_string(),
                        id: signal.signal_id.clone(),
                    }],
                    serde_json::json!({"signal": signal}),
                    Some(format!("signal:{}", signal.signal_id)),
                )],
            )
            .map_err(|error| error.to_string())?;
        self.signal(&signal.signal_id)?
            .ok_or_else(|| "recorded evolution signal was not materialized".to_string())
    }

    pub(crate) fn signal(&self, signal_id: &str) -> Result<Option<EvolutionSignal>, String> {
        self.event_store
            .list_stream(&signal_stream(signal_id))?
            .into_iter()
            .find(|event| event.kind == "evolution.signal.recorded.v1")
            .and_then(|event| event.payload.get("signal").cloned())
            .map(|value| serde_json::from_value(value).map_err(|error| error.to_string()))
            .transpose()
    }

    pub(crate) fn list_signals(&self) -> Result<Vec<EvolutionSignal>, String> {
        let mut signals = self
            .event_store
            .list_scope(RuntimeEventScope::Evolution, 100_000)?
            .into_iter()
            .filter(|event| {
                event.stream_id.starts_with(SIGNAL_PREFIX)
                    && event.kind == "evolution.signal.recorded.v1"
            })
            .filter_map(|event| event.payload.get("signal").cloned())
            .map(|value| serde_json::from_value(value).map_err(|error| error.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        signals.sort_by(|left: &EvolutionSignal, right: &EvolutionSignal| {
            right.created_at_ms.cmp(&left.created_at_ms)
        });
        Ok(signals)
    }

    pub(crate) fn create_diagnosis(
        &self,
        signal_ids: Vec<String>,
    ) -> Result<EvolutionDiagnosis, String> {
        let selected = self.select_signals(signal_ids)?;
        let key = source_set_key(&selected);
        let diagnosis_id = format!("evo-diagnosis-{key}");
        if let Some(existing) = self.diagnosis(&diagnosis_id)? {
            return Ok(existing);
        }
        let mut diagnosis = EvolutionDiagnosisEngine::diagnose(&selected);
        diagnosis.diagnosis_id = diagnosis_id;
        let stream = diagnosis_stream(&diagnosis.diagnosis_id);
        self.event_store
            .append_batch_if_revision(
                stream.clone(),
                self.stream_revision(&stream)?,
                format!("evolution-diagnosis:{}", diagnosis.diagnosis_id),
                vec![evolution_event(
                    stream,
                    "evolution.diagnosis.recorded.v1",
                    Some("diagnosed"),
                    diagnosis
                        .source_signal_ids
                        .iter()
                        .map(|id| RuntimeEventRef {
                            kind: "evolution_signal".to_string(),
                            id: id.clone(),
                        })
                        .collect(),
                    serde_json::json!({"diagnosis": diagnosis}),
                    Some(format!("diagnosis:{}", diagnosis.diagnosis_id)),
                )],
            )
            .map_err(|error| error.to_string())?;
        self.diagnosis(&diagnosis.diagnosis_id)?
            .ok_or_else(|| "recorded evolution diagnosis was not materialized".to_string())
    }

    pub(crate) fn create_lifecycle(
        &self,
        signal_ids: Vec<String>,
    ) -> Result<EvolutionLifecycleDraft, String> {
        let selected = self.select_signals(signal_ids)?;
        let key = source_set_key(&selected);
        let diagnosis_id = format!("evo-diagnosis-{key}");
        let mission_id = format!("evo-mission-{key}");
        let proposal_id = format!("evo-proposal-{key}");
        if let (Some(mission), Some(proposal)) =
            (self.mission(&mission_id)?, self.proposal(&proposal_id)?)
        {
            return Ok(EvolutionLifecycleDraft { mission, proposal });
        }

        let mut diagnosis = EvolutionDiagnosisEngine::diagnose(&selected);
        diagnosis.diagnosis_id = diagnosis_id.clone();
        let goals = candidate_kinds_from_root_cause(&diagnosis.root_cause_kind)
            .into_iter()
            .map(EvolutionCapabilityGoal::for_kind)
            .collect::<Vec<_>>();
        let mut mission = EvolutionMission::new(
            diagnosis.affected_owner.clone(),
            diagnosis.affected_files_or_modules.clone(),
            diagnosis.source_signal_ids.clone(),
            diagnosis_id.clone(),
            goals,
        );
        mission.mission_id = mission_id.clone();
        let mut proposal = EvolutionProposal::from_diagnosis(&diagnosis, &selected);
        proposal.proposal_id = proposal_id.clone();
        proposal.mission_id = Some(mission_id.clone());
        proposal.goal_ids = mission.goal_ids.clone();
        mission.attach_proposal(proposal_id.clone());

        let mut expected_streams = Vec::new();
        let mut events = Vec::new();
        if self.diagnosis(&diagnosis_id)?.is_none() {
            let stream = diagnosis_stream(&diagnosis_id);
            expected_streams.push(ExpectedStreamRevision {
                expected_revision: self.stream_revision(&stream)?,
                stream_id: stream.clone(),
            });
            events.push(evolution_event(
                stream,
                "evolution.diagnosis.recorded.v1",
                Some("diagnosed"),
                diagnosis
                    .source_signal_ids
                    .iter()
                    .map(|id| RuntimeEventRef {
                        kind: "evolution_signal".to_string(),
                        id: id.clone(),
                    })
                    .collect(),
                serde_json::json!({"diagnosis": diagnosis}),
                Some(format!("diagnosis:{diagnosis_id}")),
            ));
        }
        if self.mission(&mission_id)?.is_none() {
            let stream = mission_stream(&mission_id);
            expected_streams.push(ExpectedStreamRevision {
                expected_revision: self.stream_revision(&stream)?,
                stream_id: stream.clone(),
            });
            events.push(evolution_event(
                stream,
                "evolution.mission.opened.v1",
                Some("open"),
                vec![RuntimeEventRef {
                    kind: "evolution_diagnosis".to_string(),
                    id: diagnosis_id.clone(),
                }],
                serde_json::json!({"mission": mission}),
                Some(format!("mission:{mission_id}")),
            ));
        }
        if self.proposal(&proposal_id)?.is_none() {
            let stream = proposal_stream(&proposal_id);
            expected_streams.push(ExpectedStreamRevision {
                expected_revision: self.stream_revision(&stream)?,
                stream_id: stream.clone(),
            });
            events.push(evolution_event(
                stream,
                "evolution.proposal.created.v1",
                Some("proposed"),
                vec![
                    RuntimeEventRef {
                        kind: "evolution_diagnosis".to_string(),
                        id: diagnosis_id,
                    },
                    RuntimeEventRef {
                        kind: "evolution_mission".to_string(),
                        id: mission_id,
                    },
                ],
                serde_json::json!({"proposal": proposal}),
                Some(format!("proposal:{proposal_id}")),
            ));
        }
        if !events.is_empty() {
            self.event_store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!("evolution-lifecycle:{key}"),
                    expected_streams,
                    events,
                })
                .map_err(|error| error.to_string())?;
        }
        Ok(EvolutionLifecycleDraft {
            mission: self
                .mission(&mission.mission_id)?
                .ok_or_else(|| "evolution mission was not materialized".to_string())?,
            proposal: self
                .proposal(&proposal.proposal_id)?
                .ok_or_else(|| "evolution proposal was not materialized".to_string())?,
        })
    }

    pub(crate) fn list_diagnoses(&self) -> Result<Vec<EvolutionDiagnosis>, String> {
        list_payloads(
            &self.event_store,
            DIAGNOSIS_PREFIX,
            "evolution.diagnosis.recorded.v1",
            "diagnosis",
        )
        .map(|mut values: Vec<EvolutionDiagnosis>| {
            values.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
            values
        })
    }

    pub(crate) fn diagnosis(
        &self,
        diagnosis_id: &str,
    ) -> Result<Option<EvolutionDiagnosis>, String> {
        first_payload(
            &self.event_store,
            &diagnosis_stream(diagnosis_id),
            "evolution.diagnosis.recorded.v1",
            "diagnosis",
        )
    }

    pub(crate) fn list_missions(&self) -> Result<Vec<EvolutionMission>, String> {
        let events = self
            .event_store
            .list_scope(RuntimeEventScope::Evolution, 100_000)?;
        let mut by_stream = BTreeMap::<String, Vec<crate::DurableRuntimeEvent>>::new();
        for event in events {
            if event.stream_id.starts_with(MISSION_PREFIX) {
                by_stream
                    .entry(event.stream_id.clone())
                    .or_default()
                    .push(event);
            }
        }
        let mut missions = by_stream
            .into_values()
            .filter_map(materialize_mission)
            .collect::<Vec<_>>();
        missions.sort_by(|left: &EvolutionMission, right: &EvolutionMission| {
            right.updated_at_ms.cmp(&left.updated_at_ms)
        });
        Ok(missions)
    }

    pub(crate) fn mission(&self, mission_id: &str) -> Result<Option<EvolutionMission>, String> {
        Ok(materialize_mission(
            self.event_store.list_stream(&mission_stream(mission_id))?,
        ))
    }

    pub(crate) fn list_proposals(&self) -> Result<Vec<EvolutionProposal>, String> {
        let events = self
            .event_store
            .list_scope(RuntimeEventScope::Evolution, 100_000)?;
        let mut by_stream = BTreeMap::<String, Vec<crate::DurableRuntimeEvent>>::new();
        for event in events {
            if event.stream_id.starts_with(PROPOSAL_PREFIX) {
                by_stream
                    .entry(event.stream_id.clone())
                    .or_default()
                    .push(event);
            }
        }
        let mut proposals = by_stream
            .into_values()
            .filter_map(materialize_proposal)
            .collect::<Vec<_>>();
        proposals.sort_by(|left, right| right.created_at_ms.cmp(&left.created_at_ms));
        Ok(proposals)
    }

    pub(crate) fn proposal(&self, proposal_id: &str) -> Result<Option<EvolutionProposal>, String> {
        Ok(materialize_proposal(
            self.event_store
                .list_stream(&proposal_stream(proposal_id))?,
        ))
    }

    pub(crate) fn decide_proposal(
        &self,
        principal: &VerifiedPrincipal,
        lease: &VerifiedDecisionLease,
        proposal_id: &str,
        decision: &str,
    ) -> Result<EvolutionProposal, String> {
        if !principal.is_human_interactive()
            || !principal.has_capability("evolution.release.manage")
        {
            return Err(
                "proposal decision requires an interactive human with evolution.release.manage"
                    .to_string(),
            );
        }
        if !matches!(decision, "approved" | "rejected" | "archived") {
            return Err("decision must be approved, rejected, or archived".to_string());
        }
        let proposal = self
            .proposal(proposal_id)?
            .ok_or_else(|| "evolution proposal not found".to_string())?;
        if proposal.status == decision {
            return Ok(proposal);
        }
        if proposal.status != "draft" {
            return Err("only a draft evolution proposal can be decided".to_string());
        }
        let action = format!("proposal.decision.{decision}");
        let scope = format!("evolution.proposal:{proposal_id}");
        let review_id = format!("evolution-proposal:{proposal_id}");
        let expected_digest = proposal_decision_digest(&proposal, decision)?;
        if lease.review_id() != review_id
            || lease.action() != action
            || lease.scope() != scope
            || lease.evidence_digest() != expected_digest
        {
            return Err("proposal decision lease does not match the requested action".to_string());
        }
        let stream = proposal_stream(proposal_id);
        self.event_store
            .append_transaction_with_verified_decision_lease(
                AppendTransactionRequest {
                    transaction_id: format!("evolution-proposal-decision:{proposal_id}:{decision}"),
                    expected_streams: vec![ExpectedStreamRevision {
                        expected_revision: self.stream_revision(&stream)?,
                        stream_id: stream.clone(),
                    }],
                    events: vec![evolution_event(
                        stream,
                        "evolution.proposal.decided.v1",
                        Some(decision),
                        vec![RuntimeEventRef {
                            kind: "evolution_proposal".to_string(),
                            id: proposal_id.to_string(),
                        }],
                        serde_json::json!({
                            "decision": decision,
                            "decided_by": principal.claims().principal_id,
                        }),
                        Some(format!("proposal-decision:{decision}")),
                    )],
                },
                lease,
            )
            .map_err(|error| error.to_string())?;
        self.proposal(proposal_id)?
            .ok_or_else(|| "decided evolution proposal was not materialized".to_string())
    }

    pub(crate) fn proposal_decision_digest(
        &self,
        proposal_id: &str,
        decision: &str,
    ) -> Result<String, String> {
        let proposal = self
            .proposal(proposal_id)?
            .ok_or_else(|| "evolution proposal not found".to_string())?;
        proposal_decision_digest(&proposal, decision)
    }

    pub(crate) fn link_candidate(
        &self,
        proposal_id: &str,
        candidate_id: &str,
    ) -> Result<(), String> {
        let proposal = self
            .proposal(proposal_id)?
            .ok_or_else(|| "evolution proposal not found".to_string())?;
        if proposal.status != "approved" {
            return Err("evolution proposal must be approved before candidate registration".into());
        }
        if proposal.candidate_ids.iter().any(|id| id == candidate_id) {
            return Ok(());
        }
        let mission_id = proposal
            .mission_id
            .as_deref()
            .ok_or_else(|| "evolution proposal is not attached to a mission".to_string())?;
        let proposal_stream = proposal_stream(proposal_id);
        let mission_stream = mission_stream(mission_id);
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!("evolution-candidate-link:{proposal_id}:{candidate_id}"),
                expected_streams: vec![
                    ExpectedStreamRevision {
                        expected_revision: self.stream_revision(&proposal_stream)?,
                        stream_id: proposal_stream.clone(),
                    },
                    ExpectedStreamRevision {
                        expected_revision: self.stream_revision(&mission_stream)?,
                        stream_id: mission_stream.clone(),
                    },
                ],
                events: vec![
                    evolution_event(
                        proposal_stream,
                        "evolution.proposal.candidate_linked.v1",
                        Some("candidate_ready"),
                        vec![RuntimeEventRef {
                            kind: "evolution_candidate".to_string(),
                            id: candidate_id.to_string(),
                        }],
                        serde_json::json!({"candidate_id": candidate_id}),
                        Some(format!("candidate-link:{candidate_id}")),
                    ),
                    evolution_event(
                        mission_stream,
                        "evolution.mission.candidate_linked.v1",
                        Some("candidate_ready"),
                        vec![
                            RuntimeEventRef {
                                kind: "evolution_proposal".to_string(),
                                id: proposal_id.to_string(),
                            },
                            RuntimeEventRef {
                                kind: "evolution_candidate".to_string(),
                                id: candidate_id.to_string(),
                            },
                        ],
                        serde_json::json!({"candidate_id": candidate_id}),
                        Some(format!("candidate-link:{candidate_id}")),
                    ),
                ],
            })
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn select_signals(&self, signal_ids: Vec<String>) -> Result<Vec<EvolutionSignal>, String> {
        let signals = self.list_signals()?;
        let selected = if signal_ids.is_empty() {
            signals.into_iter().take(3).collect::<Vec<_>>()
        } else {
            let requested = signal_ids
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            signals
                .into_iter()
                .filter(|signal| requested.contains(&signal.signal_id))
                .collect::<Vec<_>>()
        };
        if selected.is_empty() {
            return Err("at least one existing evolution signal is required".to_string());
        }
        Ok(selected)
    }

    fn stream_revision(&self, stream: &str) -> Result<u64, String> {
        self.event_store
            .stream_revision(stream)
            .map_err(|error| error.to_string())
    }
}

fn validate_signal(signal: &EvolutionSignal) -> Result<(), String> {
    if signal.signal_id.trim().is_empty()
        || signal.source.owner.trim().is_empty()
        || signal.summary.trim().is_empty()
        || signal.evidence_refs.is_empty()
        || signal
            .evidence_refs
            .iter()
            .any(|evidence| !evidence.boundary.can_be_authoritative())
    {
        return Err(
            "evolution signal requires an id, owner, summary, and authoritative evidence"
                .to_string(),
        );
    }
    Ok(())
}

fn source_set_key(signals: &[EvolutionSignal]) -> String {
    let mut ids = signals
        .iter()
        .map(|signal| signal.signal_id.as_str())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    let digest = format!("{:x}", Sha256::digest(ids.join(":").as_bytes()));
    digest[..24].to_string()
}

fn signal_stream(id: &str) -> String {
    format!("{SIGNAL_PREFIX}{id}")
}

fn diagnosis_stream(id: &str) -> String {
    format!("{DIAGNOSIS_PREFIX}{id}")
}

fn mission_stream(id: &str) -> String {
    format!("{MISSION_PREFIX}{id}")
}

fn proposal_stream(id: &str) -> String {
    format!("{PROPOSAL_PREFIX}{id}")
}

fn evolution_event(
    stream_id: String,
    kind: &str,
    status: Option<&str>,
    refs: Vec<RuntimeEventRef>,
    payload: serde_json::Value,
    idempotency_key: Option<String>,
) -> RuntimeTransactionEventInput {
    RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id,
            scope: RuntimeEventScope::Evolution,
            kind: kind.to_string(),
            status: status.map(str::to_string),
            actor: Some("runtime.evolution_discovery".to_string()),
            refs,
            payload,
        },
        idempotency_key,
        schema_version: 1,
    }
}

fn first_payload<T: serde::de::DeserializeOwned>(
    store: &RuntimeEventStore,
    stream: &str,
    kind: &str,
    field: &str,
) -> Result<Option<T>, String> {
    store
        .list_stream(stream)?
        .into_iter()
        .find(|event| event.kind == kind)
        .and_then(|event| event.payload.get(field).cloned())
        .map(|value| serde_json::from_value(value).map_err(|error| error.to_string()))
        .transpose()
}

fn list_payloads<T: serde::de::DeserializeOwned>(
    store: &RuntimeEventStore,
    stream_prefix: &str,
    kind: &str,
    field: &str,
) -> Result<Vec<T>, String> {
    store
        .list_scope(RuntimeEventScope::Evolution, 100_000)?
        .into_iter()
        .filter(|event| event.stream_id.starts_with(stream_prefix) && event.kind == kind)
        .filter_map(|event| event.payload.get(field).cloned())
        .map(|value| serde_json::from_value(value).map_err(|error| error.to_string()))
        .collect()
}

fn materialize_proposal(mut events: Vec<crate::DurableRuntimeEvent>) -> Option<EvolutionProposal> {
    events.sort_by_key(|event| event.sequence);
    let mut proposal: Option<EvolutionProposal> = None;
    for event in events {
        match event.kind.as_str() {
            "evolution.proposal.created.v1" => {
                proposal = event
                    .payload
                    .get("proposal")
                    .and_then(|value| serde_json::from_value(value.clone()).ok());
            }
            "evolution.proposal.decided.v1" => {
                if let Some(current) = proposal.as_mut() {
                    current.status = event
                        .payload
                        .get("decision")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or("rejected")
                        .to_string();
                }
            }
            "evolution.proposal.candidate_linked.v1" => {
                if let (Some(current), Some(candidate_id)) = (
                    proposal.as_mut(),
                    event
                        .payload
                        .get("candidate_id")
                        .and_then(serde_json::Value::as_str),
                ) {
                    if !current.candidate_ids.iter().any(|id| id == candidate_id) {
                        current.candidate_ids.push(candidate_id.to_string());
                    }
                }
            }
            _ => {}
        }
    }
    proposal
}

fn materialize_mission(mut events: Vec<crate::DurableRuntimeEvent>) -> Option<EvolutionMission> {
    events.sort_by_key(|event| event.sequence);
    let mut mission: Option<EvolutionMission> = None;
    for event in events {
        match event.kind.as_str() {
            "evolution.mission.opened.v1" => {
                mission = event
                    .payload
                    .get("mission")
                    .and_then(|value| serde_json::from_value(value.clone()).ok());
            }
            "evolution.mission.candidate_linked.v1" => {
                if let (Some(current), Some(candidate_id)) = (
                    mission.as_mut(),
                    event
                        .payload
                        .get("candidate_id")
                        .and_then(serde_json::Value::as_str),
                ) {
                    current.attach_candidate(candidate_id.to_string());
                    current.updated_at_ms = u128::from(event.created_at_ms);
                }
            }
            _ => {}
        }
    }
    mission
}

fn proposal_decision_digest(
    proposal: &EvolutionProposal,
    decision: &str,
) -> Result<String, String> {
    let evidence = serde_json::to_vec(&serde_json::json!({
        "proposal": proposal,
        "decision": decision,
    }))
    .map_err(|error| error.to_string())?;
    Ok(format!("sha256:{:x}", Sha256::digest(evidence)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::reality::EvidenceRef;

    fn signal() -> EvolutionSignal {
        EvolutionSignal::low_novelty_tool_loop(
            "runtime",
            "session-1",
            vec![EvidenceRef::new("runtime_event", "event-1")],
        )
    }

    #[test]
    fn discovery_ledger_is_idempotent_and_replayable() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = EvolutionDiscoveryService::new(Arc::clone(&events));
        let recorded = discovery.record_signal(signal()).expect("record signal");
        let repeated = discovery
            .record_signal(recorded.clone())
            .expect("idempotent signal");
        assert_eq!(recorded, repeated);
        assert_eq!(discovery.list_signals().expect("signals").len(), 1);

        let first = discovery
            .create_lifecycle(vec![recorded.signal_id.clone()])
            .expect("lifecycle");
        let second = discovery
            .create_lifecycle(vec![recorded.signal_id])
            .expect("replayed lifecycle");
        assert_eq!(first, second);
        assert_eq!(discovery.list_diagnoses().expect("diagnoses").len(), 1);
        assert_eq!(discovery.list_missions().expect("missions").len(), 1);
        assert_eq!(discovery.list_proposals().expect("proposals").len(), 1);
    }

    #[test]
    fn discovery_ledger_rejects_conflicting_signal_reuse() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = EvolutionDiscoveryService::new(Arc::clone(&events));
        let recorded = discovery.record_signal(signal()).expect("record signal");
        let mut conflicting = recorded.clone();
        conflicting.summary = "a different observation".to_string();

        let error = discovery
            .record_signal(conflicting)
            .expect_err("conflicting signal id must fail closed");
        assert!(error.contains("evolution signal idempotency conflict"));
        assert_eq!(
            discovery
                .signal(&recorded.signal_id)
                .expect("signal lookup"),
            Some(recorded)
        );
    }
}
