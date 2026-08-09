//! Event-sourced discovery side of Runtime evolution.
//!
//! Signals, diagnoses, missions and proposals share the Runtime Event Store
//! with candidate evaluation and release governance. Gateway is only a typed
//! command/query adapter and never opens a second file-backed registry.

use std::collections::BTreeMap;
use std::sync::Arc;

use sha2::{Digest, Sha256};

use super::{
    candidate_kinds_from_root_cause, EvolutionCapabilityGoal, EvolutionCase,
    EvolutionCaseCatalogPage, EvolutionCaseIndex, EvolutionCasePage, EvolutionCaseState,
    EvolutionDiagnosis, EvolutionDiagnosisEngine, EvolutionLifecycleDraft, EvolutionMission,
    EvolutionProposal, EvolutionSignal, EVOLUTION_CASE_CATALOG_PAGE_SIZE,
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
const CASE_PREFIX: &str = "evolution:case:v2:";
const CASE_EVENT_KIND: &str = "evolution.case.revised.v2";
const CASE_INDEX_STREAM: &str = "evolution:case-index:v2";
const CASE_INDEX_EVENT_KIND: &str = "evolution.case_index.revised.v2";
const CASE_CATALOG_EVENT_KIND: &str = "evolution.case_catalog_page.frozen.v2";
const CASE_CAS_RETRIES: usize = 32;

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
                let case = self.observe_case(&existing)?;
                if matches!(
                    case.state,
                    EvolutionCaseState::Ready | EvolutionCaseState::Diagnosed
                ) && case_can_auto_diagnose(&case)
                {
                    self.promote_case(&case.case_id)?;
                }
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
        let recorded = self
            .signal(&signal.signal_id)?
            .ok_or_else(|| "recorded evolution signal was not materialized".to_string())?;
        let case = self.observe_case(&recorded)?;
        if matches!(
            case.state,
            EvolutionCaseState::Ready | EvolutionCaseState::Diagnosed
        ) && case_can_auto_diagnose(&case)
        {
            self.promote_case(&case.case_id)?;
        }
        Ok(recorded)
    }

    /// Materialize a deterministic projector signal without letting a replayed
    /// payload revision poison the projector. An existing canonical signal
    /// wins by identity and is never overwritten, while its Case projection is
    /// still repaired when historical data predates the Case index.
    pub(crate) fn materialize_projected_signal(
        &self,
        signal: EvolutionSignal,
    ) -> Result<EvolutionSignal, String> {
        validate_signal(&signal)?;
        if let Some(existing) = self.signal(&signal.signal_id)? {
            // The legacy event may already occupy the canonical signal
            // stream while predating the Case index. Re-recording the exact
            // canonical value is idempotent and materializes the missing Case
            // without allowing an obsolete payload to replace current truth.
            return self.record_signal(existing);
        }
        self.record_signal(signal)
    }

    pub(crate) fn signal(&self, signal_id: &str) -> Result<Option<EvolutionSignal>, String> {
        self.event_store
            .latest_for_stream(&signal_stream(signal_id))?
            .filter(|event| event.kind == "evolution.signal.recorded.v1")
            .and_then(|event| event.payload.get("signal").cloned())
            .map(|value| serde_json::from_value(value).map_err(|error| error.to_string()))
            .transpose()
    }

    pub(crate) fn list_signals(&self) -> Result<Vec<EvolutionSignal>, String> {
        let mut signals = self
            .event_store
            .replay_scope_stream_prefix(RuntimeEventScope::Evolution, SIGNAL_PREFIX)?
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
        let case_ids = selected
            .iter()
            .map(|signal| EvolutionCase::from_signal(signal).case_id)
            .collect::<std::collections::BTreeSet<_>>();
        let lifecycle = self.create_lifecycle_with_key(selected, &key)?;
        for case_id in case_ids {
            if let Some(case) = self.case(&case_id)? {
                self.transition_case(
                    case,
                    EvolutionCaseState::Proposed,
                    lifecycle.proposal.diagnosis_id.clone(),
                    Some(lifecycle.proposal.proposal_id.clone()),
                    "explicit_proposal_materialized",
                )?;
            }
        }
        Ok(lifecycle)
    }

    fn create_lifecycle_with_key(
        &self,
        selected: Vec<EvolutionSignal>,
        key: &str,
    ) -> Result<EvolutionLifecycleDraft, String> {
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
            if let Err(error) = self
                .event_store
                .append_transaction(AppendTransactionRequest {
                    transaction_id: format!("evolution-lifecycle:{key}"),
                    expected_streams,
                    events,
                })
            {
                let concurrently_materialized = self.mission(&mission.mission_id)?.is_some()
                    && self.proposal(&proposal.proposal_id)?.is_some();
                if !concurrently_materialized {
                    return Err(error.to_string());
                }
            }
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

    pub(crate) fn case(&self, case_id: &str) -> Result<Option<EvolutionCase>, String> {
        self.event_store
            .latest_for_stream(&case_stream(case_id))?
            .filter(|event| event.kind == CASE_EVENT_KIND)
            .and_then(|event| event.payload.get("case").cloned())
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| error.to_string())
    }

    pub(crate) fn list_cases(&self, limit: usize) -> Result<Vec<EvolutionCase>, String> {
        self.case_index()?
            .recent_cases
            .into_iter()
            .take(limit.min(125))
            .filter_map(|summary| self.case(&summary.case_id).transpose())
            .collect()
    }

    pub(crate) fn case_page(
        &self,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<EvolutionCasePage, String> {
        let index = self.case_index()?;
        let limit = limit.clamp(1, EVOLUTION_CASE_CATALOG_PAGE_SIZE);
        let (mut page, mut offset) = parse_case_cursor(cursor)?;
        if page > index.catalog_tail_page {
            return Err("evolution_case_cursor_out_of_range".to_string());
        }
        let mut items = Vec::with_capacity(limit);
        while items.len() < limit && page <= index.catalog_tail_page {
            let catalog = self.case_catalog_page(&index, page)?;
            if offset > catalog.case_ids.len() {
                return Err("evolution_case_cursor_out_of_range".to_string());
            }
            for case_id in catalog.case_ids.iter().skip(offset) {
                let case = self.case(case_id)?.ok_or_else(|| {
                    format!("evolution case catalog references missing case: {case_id}")
                })?;
                items.push(case);
                offset = offset.saturating_add(1);
                if items.len() == limit {
                    break;
                }
            }
            if offset >= catalog.case_ids.len() {
                if page >= index.catalog_tail_page {
                    break;
                }
                page = page.saturating_add(1);
                offset = 0;
            }
        }
        let current_catalog = self.case_catalog_page(&index, page)?;
        let next_cursor =
            if offset < current_catalog.case_ids.len() || page < index.catalog_tail_page {
                Some(format!("v2:{page}:{offset}"))
            } else {
                None
            };
        Ok(EvolutionCasePage {
            items,
            next_cursor,
            total: index.total_cases,
        })
    }

    fn case_catalog_page(
        &self,
        index: &EvolutionCaseIndex,
        page: u64,
    ) -> Result<EvolutionCaseCatalogPage, String> {
        if page == index.catalog_tail_page {
            return Ok(EvolutionCaseCatalogPage {
                page,
                case_ids: index.catalog_tail_case_ids.clone(),
            });
        }
        if page > index.catalog_tail_page {
            return Err("evolution_case_cursor_out_of_range".to_string());
        }
        self.event_store
            .latest_for_stream(&case_catalog_stream(page))?
            .filter(|event| event.kind == CASE_CATALOG_EVENT_KIND)
            .and_then(|event| event.payload.get("page").cloned())
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| format!("evolution case catalog page {page} is missing"))
    }

    pub(crate) fn case_index(&self) -> Result<EvolutionCaseIndex, String> {
        self.event_store
            .latest_for_stream(CASE_INDEX_STREAM)?
            .filter(|event| event.kind == CASE_INDEX_EVENT_KIND)
            .and_then(|event| event.payload.get("index").cloned())
            .map(serde_json::from_value)
            .transpose()
            .map(|index| index.unwrap_or_default())
            .map_err(|error| error.to_string())
    }

    pub(crate) fn resume_auto_cases(&self, limit: usize) -> Result<usize, String> {
        let mut resumed = 0;
        for case in self.list_cases(limit)? {
            if matches!(
                case.state,
                EvolutionCaseState::Ready | EvolutionCaseState::Diagnosed
            ) && case_can_auto_diagnose(&case)
            {
                self.promote_case(&case.case_id)?;
                resumed += 1;
            }
        }
        Ok(resumed)
    }

    fn observe_case(&self, signal: &EvolutionSignal) -> Result<EvolutionCase, String> {
        let seed = EvolutionCase::from_signal(signal);
        let stream = case_stream(&seed.case_id);
        for _ in 0..CASE_CAS_RETRIES {
            let current = self.case(&seed.case_id)?;
            if current.as_ref().is_some_and(|case| {
                case.signal_ids
                    .iter()
                    .any(|signal_id| signal_id == &signal.signal_id)
            }) {
                return Ok(current.expect("checked above"));
            }
            let mut next = current.clone().unwrap_or_else(|| seed.clone());
            if current.is_some() {
                next.observe(signal);
            }
            let expected_revision = self.stream_revision(&stream)?;
            if current.as_ref().map_or(expected_revision != 0, |case| {
                case.revision != expected_revision
            }) {
                continue;
            }
            next.revision = expected_revision.saturating_add(1);
            let event = evolution_event(
                stream.clone(),
                CASE_EVENT_KIND,
                Some(case_state_label(next.state)),
                vec![RuntimeEventRef {
                    kind: "evolution_signal".to_string(),
                    id: signal.signal_id.clone(),
                }],
                serde_json::json!({"case": next}),
                Some(format!("case-signal:{}", signal.signal_id)),
            );
            match self.append_case_revision(
                current.as_ref(),
                &next,
                expected_revision,
                event,
                format!("signal:{}", signal.signal_id),
            ) {
                Ok(_) => {
                    return self
                        .case(&seed.case_id)?
                        .ok_or_else(|| "evolution case was not materialized".to_string());
                }
                Err(crate::RuntimeEventStoreError::StaleRevision { .. }) => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
        Err(format!(
            "evolution case {} remained contended after bounded retries",
            seed.case_id
        ))
    }

    pub(crate) fn promote_case(&self, case_id: &str) -> Result<EvolutionCase, String> {
        let mut case = self
            .case(case_id)?
            .ok_or_else(|| "evolution case not found".to_string())?;
        if case.is_terminal() || case.state == EvolutionCaseState::Open {
            return Ok(case);
        }
        if case.state == EvolutionCaseState::Ready {
            case = self.transition_case(
                case,
                EvolutionCaseState::Diagnosed,
                None,
                None,
                "diagnosis_claimed",
            )?;
        }
        let selected = self.select_signals(case.signal_ids.clone())?;
        let lifecycle = self.create_lifecycle_with_key(selected, &case.key_sha256[..24])?;
        self.transition_case(
            case,
            EvolutionCaseState::Proposed,
            lifecycle.proposal.diagnosis_id.clone(),
            Some(lifecycle.proposal.proposal_id),
            "proposal_materialized",
        )
    }

    fn transition_case(
        &self,
        case: EvolutionCase,
        state: EvolutionCaseState,
        diagnosis_id: Option<String>,
        proposal_id: Option<String>,
        reason: &str,
    ) -> Result<EvolutionCase, String> {
        let case_id = case.case_id;
        for _ in 0..CASE_CAS_RETRIES {
            let stream = case_stream(&case_id);
            let current = self
                .case(&case_id)?
                .ok_or_else(|| "evolution case not found".to_string())?;
            if current.state == state || current.is_terminal() {
                return Ok(current);
            }
            let expected_revision = self.stream_revision(&stream)?;
            if current.revision != expected_revision {
                continue;
            }
            let mut next = current.clone();
            next.revision = expected_revision.saturating_add(1);
            next.state = state;
            next.diagnosis_id = diagnosis_id.clone().or(next.diagnosis_id);
            next.proposal_id = proposal_id.clone().or(next.proposal_id);
            next.updated_at_ms = now_ms();
            let refs = [
                next.diagnosis_id.as_ref().map(|id| RuntimeEventRef {
                    kind: "evolution_diagnosis".to_string(),
                    id: id.clone(),
                }),
                next.proposal_id.as_ref().map(|id| RuntimeEventRef {
                    kind: "evolution_proposal".to_string(),
                    id: id.clone(),
                }),
            ]
            .into_iter()
            .flatten()
            .collect();
            let event = evolution_event(
                stream,
                CASE_EVENT_KIND,
                Some(case_state_label(state)),
                refs,
                serde_json::json!({"case": next}),
                Some(format!("case-transition:{reason}")),
            );
            match self.append_case_revision(
                Some(&current),
                &next,
                expected_revision,
                event,
                format!("transition:{reason}"),
            ) {
                Ok(_) => {
                    return self
                        .case(&case_id)?
                        .ok_or_else(|| "transitioned evolution case disappeared".to_string());
                }
                Err(crate::RuntimeEventStoreError::StaleRevision { .. }) => continue,
                Err(error) => {
                    let latest = self.case(&case_id)?;
                    if latest.as_ref().is_some_and(|case| case.state == state) {
                        return Ok(latest.expect("checked above"));
                    }
                    return Err(error.to_string());
                }
            }
        }
        Err(format!(
            "evolution case {case_id} transition remained contended after bounded retries"
        ))
    }

    fn append_case_revision(
        &self,
        previous: Option<&EvolutionCase>,
        next: &EvolutionCase,
        expected_case_revision: u64,
        case_event: RuntimeTransactionEventInput,
        reason: String,
    ) -> Result<(), crate::RuntimeEventStoreError> {
        let current_index = self.case_index().map_err(|error| {
            crate::RuntimeEventStoreError::Corrupt(format!(
                "evolution case index is unreadable: {error}"
            ))
        })?;
        let expected_index_revision = self
            .event_store
            .stream_revision(CASE_INDEX_STREAM)
            .map_err(|error| {
                crate::RuntimeEventStoreError::Corrupt(format!(
                    "evolution case index revision is unreadable: {error}"
                ))
            })?;
        if current_index.revision != expected_index_revision {
            return Err(crate::RuntimeEventStoreError::StaleRevision {
                stream_id: CASE_INDEX_STREAM.to_string(),
                expected: current_index.revision,
                actual: expected_index_revision,
            });
        }
        let next_index = current_index.apply(previous, next);
        let index_event = evolution_event(
            CASE_INDEX_STREAM.to_string(),
            CASE_INDEX_EVENT_KIND,
            Some("indexed"),
            vec![RuntimeEventRef {
                kind: "evolution_case".to_string(),
                id: next.case_id.clone(),
            }],
            serde_json::json!({"index": next_index}),
            Some(format!("case-index:{}:{}", next.case_id, next.revision)),
        );
        let mut expected_streams = vec![
            ExpectedStreamRevision {
                stream_id: case_stream(&next.case_id),
                expected_revision: expected_case_revision,
            },
            ExpectedStreamRevision {
                stream_id: CASE_INDEX_STREAM.to_string(),
                expected_revision: expected_index_revision,
            },
        ];
        let mut events = vec![case_event, index_event];
        if previous.is_none()
            && current_index.catalog_tail_case_ids.len() >= EVOLUTION_CASE_CATALOG_PAGE_SIZE
        {
            let page = EvolutionCaseCatalogPage {
                page: current_index.catalog_tail_page,
                case_ids: current_index.catalog_tail_case_ids.clone(),
            };
            let stream = case_catalog_stream(page.page);
            expected_streams.push(ExpectedStreamRevision {
                stream_id: stream.clone(),
                expected_revision: 0,
            });
            events.push(evolution_event(
                stream,
                CASE_CATALOG_EVENT_KIND,
                Some("frozen"),
                page.case_ids
                    .iter()
                    .map(|case_id| RuntimeEventRef {
                        kind: "evolution_case".to_string(),
                        id: case_id.clone(),
                    })
                    .collect(),
                serde_json::json!({"page": page}),
                Some(format!(
                    "case-catalog-page:{}",
                    current_index.catalog_tail_page
                )),
            ));
        }
        self.event_store
            .append_transaction(AppendTransactionRequest {
                transaction_id: format!(
                    "evolution-case-v2:{}:{}:{}",
                    next.case_id, next.revision, reason
                ),
                expected_streams,
                events,
            })
            .map(|_| ())
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
            .replay_scope_stream_prefix(RuntimeEventScope::Evolution, MISSION_PREFIX)?;
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
            .replay_scope_stream_prefix(RuntimeEventScope::Evolution, PROPOSAL_PREFIX)?;
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
        let selected = if signal_ids.is_empty() {
            let mut selected = Vec::new();
            for case in self.list_cases(3)? {
                for signal_id in case.signal_ids.iter().rev() {
                    if let Some(signal) = self.signal(signal_id)? {
                        selected.push(signal);
                    }
                    if selected.len() == 3 {
                        break;
                    }
                }
                if selected.len() == 3 {
                    break;
                }
            }
            selected
        } else {
            signal_ids
                .into_iter()
                .map(|signal_id| {
                    self.signal(&signal_id)?
                        .ok_or_else(|| format!("evolution signal not found: {signal_id}"))
                })
                .collect::<Result<Vec<_>, _>>()?
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

fn case_stream(id: &str) -> String {
    format!("{CASE_PREFIX}{id}")
}

fn case_catalog_stream(page: u64) -> String {
    format!("evolution:case-catalog:v2:{page:020}")
}

fn case_state_label(state: EvolutionCaseState) -> &'static str {
    match state {
        EvolutionCaseState::Open => "open",
        EvolutionCaseState::Ready => "ready",
        EvolutionCaseState::Diagnosed => "diagnosed",
        EvolutionCaseState::Proposed => "proposed",
        EvolutionCaseState::Closed => "closed",
        EvolutionCaseState::Expired => "expired",
    }
}

fn case_can_auto_diagnose(case: &EvolutionCase) -> bool {
    matches!(
        case.key.signal_type.as_str(),
        "low_novelty_tool_loop"
            | "missing_tool_capability"
            | "memory_noise"
            | "eval_failure"
            | "context_pressure"
    )
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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

fn parse_case_cursor(cursor: Option<&str>) -> Result<(u64, usize), String> {
    let Some(cursor) = cursor.filter(|cursor| !cursor.is_empty()) else {
        return Ok((0, 0));
    };
    let mut parts = cursor.split(':');
    if parts.next() != Some("v2") {
        return Err("invalid_evolution_case_cursor".to_string());
    }
    let page = parts
        .next()
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| "invalid_evolution_case_cursor".to_string())?;
    let offset = parts
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .ok_or_else(|| "invalid_evolution_case_cursor".to_string())?;
    if parts.next().is_some() || offset > EVOLUTION_CASE_CATALOG_PAGE_SIZE {
        return Err("invalid_evolution_case_cursor".to_string());
    }
    Ok((page, offset))
}

fn list_payloads<T: serde::de::DeserializeOwned>(
    store: &RuntimeEventStore,
    stream_prefix: &str,
    kind: &str,
    field: &str,
) -> Result<Vec<T>, String> {
    store
        .replay_scope_stream_prefix(RuntimeEventScope::Evolution, stream_prefix)?
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
            vec![EvidenceRef::observed("runtime_event", "event-1")],
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

    #[test]
    fn legacy_import_keeps_existing_canonical_signal_on_payload_conflict() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = EvolutionDiscoveryService::new(Arc::clone(&events));
        let recorded = discovery.record_signal(signal()).expect("record signal");
        let mut legacy = recorded.clone();
        legacy.summary = "obsolete legacy payload".to_string();

        let retained = discovery
            .materialize_projected_signal(legacy)
            .expect("legacy conflict is isolated");
        assert_eq!(retained, recorded);
        assert_eq!(discovery.list_signals().expect("signals").len(), 1);
    }

    #[test]
    fn one_hundred_same_scope_signals_collapse_to_one_case_and_one_proposal() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = EvolutionDiscoveryService::new(events);
        for index in 0..100 {
            let mut next = signal();
            next.signal_id = format!("signal-{index}");
            next.source.session_id = Some(format!("session-{index}"));
            next.scope.workspace_identity = "workspace".to_string();
            next.scope.affected_subject = "memory-governance".to_string();
            next.scope.config_definition_revision = "cfg-1".to_string();
            next.created_at_ms = 10_000;
            discovery.record_signal(next).expect("record signal");
        }
        let index = discovery.case_index().expect("case index");
        assert_eq!(index.total_cases, 1, "{index:#?}");
        assert_eq!(index.total_signal_observations, 100);
        assert_eq!(index.state_counts.get("proposed"), Some(&1));
        let cases = discovery.list_cases(25).expect("cases");
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].recurrence_count, 100);
        assert_eq!(discovery.list_diagnoses().expect("diagnoses").len(), 1);
        assert_eq!(discovery.list_missions().expect("missions").len(), 1);
        assert_eq!(discovery.list_proposals().expect("proposals").len(), 1);
    }

    #[test]
    fn concurrent_case_observation_uses_stream_and_index_cas() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = Arc::new(EvolutionDiscoveryService::new(events));
        let mut workers = Vec::new();
        for index in 0..8 {
            let discovery = Arc::clone(&discovery);
            workers.push(std::thread::spawn(move || {
                let mut next = signal();
                next.signal_id = format!("concurrent-{index}");
                next.scope.workspace_identity = "workspace".to_string();
                next.scope.affected_subject = "shared-memory".to_string();
                next.scope.config_definition_revision = "cfg-1".to_string();
                next.created_at_ms = 10_000;
                discovery.record_signal(next)
            }));
        }
        for worker in workers {
            worker.join().expect("worker").expect("record signal");
        }
        let index = discovery.case_index().expect("case index");
        assert_eq!(index.total_cases, 1, "{index:#?}");
        assert_eq!(index.total_signal_observations, 8);
        assert_eq!(discovery.list_cases(25).expect("cases").len(), 1);
        assert_eq!(discovery.list_proposals().expect("proposals").len(), 1);
    }

    #[test]
    fn case_catalog_paginates_across_frozen_pages_without_scope_replay() {
        let events = Arc::new(RuntimeEventStore::open_in_memory().expect("event store"));
        let discovery = EvolutionDiscoveryService::new(events);
        for index in 0..=EVOLUTION_CASE_CATALOG_PAGE_SIZE {
            let mut next = signal();
            next.signal_id = format!("catalog-signal-{index}");
            next.scope.workspace_identity = "workspace".to_string();
            next.scope.affected_subject = format!("subject-{index}");
            next.scope.config_definition_revision = "cfg-1".to_string();
            next.created_at_ms = 10_000;
            next.severity = crate::EvolutionSignalSeverity::Warning;
            discovery.record_signal(next).expect("record signal");
        }

        let first = discovery.case_page(None, 125).expect("first page");
        assert_eq!(first.total, 126);
        assert_eq!(first.items.len(), 125);
        assert_eq!(first.next_cursor.as_deref(), Some("v2:1:0"));
        let second = discovery
            .case_page(first.next_cursor.as_deref(), 125)
            .expect("second page");
        assert_eq!(second.items.len(), 1);
        assert!(second.next_cursor.is_none());
    }
}
