//! Recoverable projection of Objective-verified collaboration into advisory evidence.
//! Never schedules work, changes an Agent definition, or grants a capability.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use harness_contract::evolution::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{
    AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput, RuntimeEventRef,
    RuntimeEventScope, RuntimeEventStore, RuntimeProjectionDescriptor,
    RuntimeProjectionEventInterest, RuntimeProjectionInterest, RuntimeProjectionLane,
    RuntimeProjectionPass, RuntimeTransactionEventInput,
};

mod episode;
#[cfg(test)]
mod tests;

pub(crate) const EPISODE_KIND: &str = "evolution.collaboration_experience.recorded.v1";
pub(crate) const PATTERN_KIND: &str = "evolution.collaboration_pattern.projected.v1";
const SUPPORT_KIND: &str = "evolution.collaboration_pattern.support_updated.v1";
const VERDICT_KIND: &str = "agentic.objective_verdict_bound";
const PROJECTOR_ID: &str = "projector:agentic-collaboration-experience:v1";
// Bounds projection working memory, not task length or model output. Complete
// episodes remain durable; this is a recent independent evidence window only.
const SUPPORT_WINDOW: usize = 256;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Support {
    turn_hash: String,
    completed_at_ms: u64,
    evidence_count: u32,
}

pub(crate) struct CollaborationExperienceProjector {
    store: Arc<RuntimeEventStore>,
    graphs: crate::ExecutionGraphStateStore,
    workspace_key: String,
}

impl CollaborationExperienceProjector {
    pub(crate) fn new(
        store: Arc<RuntimeEventStore>,
        graphs: crate::ExecutionGraphStateStore,
        workspace_key: String,
    ) -> Self {
        Self {
            store,
            graphs,
            workspace_key,
        }
    }

    fn interest() -> RuntimeProjectionInterest {
        RuntimeProjectionInterest::new([RuntimeProjectionEventInterest::new(
            RuntimeEventScope::Program,
            VERDICT_KIND,
        )])
    }

    pub(crate) fn projection_lane(self) -> Result<RuntimeProjectionLane, String> {
        Ok(RuntimeProjectionLane::blocking(
            RuntimeProjectionDescriptor::new(
                PROJECTOR_ID,
                Self::interest(),
                64,
                Duration::from_secs(30),
            )?,
            move |batch_size| self.project_available(batch_size),
        ))
    }

    pub(crate) fn project_available(
        &self,
        batch_size: usize,
    ) -> Result<RuntimeProjectionPass, String> {
        let checkpoint = self
            .store
            .projection_checkpoint(PROJECTOR_ID)
            .map_err(|error| error.to_string())?;
        let cursor = checkpoint.as_ref().map_or(0, |value| value.source_cursor);
        let page = self
            .store
            .projection_scan_page(
                cursor,
                &Self::interest(),
                batch_size.max(1),
                10_000,
                16 * 1024 * 1024,
            )
            .map_err(|error| error.to_string())?;
        if page.scanned_commits == 0 {
            return Ok(RuntimeProjectionPass::default());
        }
        for batch in &page.batches {
            for event in &batch.events {
                let episode =
                    episode::from_verdict(&self.store, &self.graphs, &self.workspace_key, event)?;
                self.record_episode(&episode, &event.event_id)?;
            }
        }
        // Output transactions precede the source cursor. A crash here causes
        // a safe replay, never a lost episode or a second pattern supporter.
        self.store
            .put_projection_checkpoint_retrying(
                PROJECTOR_ID,
                page.scanned_through_cursor,
                &json!({"matched_events": page.matched_events}),
                episode::now_ms(),
            )
            .map_err(|error| error.to_string())?;
        Ok(
            RuntimeProjectionPass::scanned(page.scanned_commits, batch_size)
                .with_matches(page.matched_events),
        )
    }

    fn record_episode(
        &self,
        episode: &CollaborationExperienceEpisode,
        source: &str,
    ) -> Result<(), String> {
        let episode_stream = format!("evolution:episode:{}", episode.episode_id);
        let signature_digest = episode.semantic_signature.digest();
        let pattern_id = CollaborationSemanticPattern::deterministic_id(&signature_digest);
        let pattern_stream = format!("evolution:pattern:{pattern_id}");
        for _ in 0..4 {
            if let Some(existing) = self
                .store
                .event_by_idempotency_key(&episode_stream, &episode.episode_id)
                .map_err(|error| error.to_string())?
            {
                let recorded: CollaborationExperienceEpisode = serde_json::from_value(
                    existing
                        .payload
                        .get("episode")
                        .cloned()
                        .ok_or("episode payload missing")?,
                )
                .map_err(|error| error.to_string())?;
                return if recorded == *episode {
                    Ok(())
                } else {
                    Err("episode identity conflict".into())
                };
            }
            let pattern_revision = self
                .store
                .stream_revision(&pattern_stream)
                .map_err(|error| error.to_string())?;
            let mut expected = vec![ExpectedStreamRevision {
                stream_id: episode_stream.clone(),
                expected_revision: 0,
            }];
            let mut events = vec![event(
                &episode_stream,
                EPISODE_KIND,
                &episode.episode_id,
                json!({"episode": episode}),
                vec![RuntimeEventRef {
                    kind: "source_event".into(),
                    id: source.into(),
                }],
            )];
            if episode.is_pattern_eligible() {
                let previous = self
                    .store
                    .latest_for_stream_kind(&pattern_stream, SUPPORT_KIND)?;
                let mut support: BTreeMap<String, Support> = previous
                    .map(|event| serde_json::from_value(event.payload["support"].clone()))
                    .transpose()
                    .map_err(|error| error.to_string())?
                    .unwrap_or_default();
                // Several Programs in one Turn are correlated, not independent
                // replications. Keep one representative per Turn in the window.
                support.retain(|_, value| value.turn_hash != episode.turn_ref_hash);
                support.insert(
                    episode.episode_id.clone(),
                    Support {
                        turn_hash: episode.turn_ref_hash.clone(),
                        completed_at_ms: episode.completed_at_ms,
                        evidence_count: episode.evidence_refs.len() as u32,
                    },
                );
                while support.len() > SUPPORT_WINDOW {
                    let oldest = support
                        .iter()
                        .min_by_key(|(id, value)| (value.completed_at_ms, *id))
                        .map(|(id, _)| id.clone())
                        .ok_or("empty support window")?;
                    support.remove(&oldest);
                }
                expected.push(ExpectedStreamRevision {
                    stream_id: pattern_stream.clone(),
                    expected_revision: pattern_revision,
                });
                events.push(event(
                    &pattern_stream,
                    SUPPORT_KIND,
                    &format!("support:{}", episode.episode_id),
                    json!({"support": support}),
                    Vec::new(),
                ));
                if support.len() >= MINIMUM_PATTERN_DISTINCT_TURNS {
                    let signature = episode.semantic_signature.clone().normalized();
                    let pattern = CollaborationSemanticPattern {
                        schema_version: COLLABORATION_PATTERN_SCHEMA_VERSION,
                        pattern_id: pattern_id.clone(),
                        pattern_revision: pattern_revision + 2,
                        signature_digest: signature_digest.clone(),
                        semantic_suggestion: suggestion(&signature),
                        semantic_signature: signature,
                        evidence_summary: PatternEvidenceSummary {
                            eligible_episode_count: support.len() as u32,
                            distinct_turn_count: support.len() as u32,
                            evidence_ref_count: support.values().map(|s| s.evidence_count).sum(),
                            coverage_basis_points: 10_000,
                        },
                        lifecycle: SemanticPatternLifecycle::Advisory,
                        qualifying_episode_ids: support.keys().cloned().collect(),
                        distinct_turn_ref_hashes: support
                            .values()
                            .map(|s| s.turn_hash.clone())
                            .collect(),
                        support_count: support.len() as u32,
                        latest_completed_at_ms: support
                            .values()
                            .map(|s| s.completed_at_ms)
                            .max()
                            .unwrap_or(0),
                    };
                    if !pattern.is_actionable() {
                        return Err("derived pattern is not actionable".into());
                    }
                    events.push(event(
                        &pattern_stream,
                        PATTERN_KIND,
                        &format!("pattern:{}", episode.episode_id),
                        json!({"pattern": pattern}),
                        Vec::new(),
                    ));
                }
            }
            match self.store.append_transaction(AppendTransactionRequest {
                transaction_id: format!("collaboration-experience:{}", episode.episode_id),
                expected_streams: expected,
                events,
            }) {
                Ok(_) => return Ok(()),
                Err(crate::RuntimeEventStoreError::StaleRevision { .. }) => continue,
                Err(crate::RuntimeEventStoreError::TransactionConflict { .. }) => continue,
                Err(error) => return Err(error.to_string()),
            }
        }
        Err("collaboration experience transaction contention; retry from durable cursor".into())
    }
}

/// Latest durable read model per signature, without replaying every historic
/// pattern revision. Invalid data is surfaced instead of silently omitted.
pub(crate) fn read_patterns(
    store: &RuntimeEventStore,
    limit: usize,
) -> Result<Vec<CollaborationSemanticPattern>, String> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut patterns = Vec::new();
    for stream in store
        .stream_ids_for_scope(RuntimeEventScope::Evolution)
        .map_err(|error| error.to_string())?
    {
        if !stream.starts_with("evolution:pattern:") {
            continue;
        }
        let Some(event) = store.latest_for_stream_kind(&stream, PATTERN_KIND)? else {
            continue;
        };
        let pattern: CollaborationSemanticPattern = serde_json::from_value(
            event
                .payload
                .get("pattern")
                .cloned()
                .ok_or("pattern payload missing")?,
        )
        .map_err(|error| error.to_string())?;
        if pattern.pattern_id
            != CollaborationSemanticPattern::deterministic_id(&pattern.signature_digest)
            || pattern.signature_digest != pattern.semantic_signature.digest()
            || stream != format!("evolution:pattern:{}", pattern.pattern_id)
            || (matches!(
                pattern.lifecycle,
                SemanticPatternLifecycle::Advisory | SemanticPatternLifecycle::CandidateCreated
            ) && !pattern.is_actionable())
        {
            return Err(format!("pattern identity/signature mismatch in {stream}"));
        }
        patterns.push(pattern);
        patterns.sort_by(|a, b| {
            b.latest_completed_at_ms
                .cmp(&a.latest_completed_at_ms)
                .then(a.pattern_id.cmp(&b.pattern_id))
        });
        patterns.truncate(limit);
    }
    Ok(patterns)
}

fn event(
    stream: &str,
    kind: &str,
    key: &str,
    payload: serde_json::Value,
    refs: Vec<RuntimeEventRef>,
) -> RuntimeTransactionEventInput {
    RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id: stream.into(),
            scope: RuntimeEventScope::Evolution,
            kind: kind.into(),
            status: Some("projected".into()),
            actor: Some(PROJECTOR_ID.into()),
            refs,
            payload,
        },
        idempotency_key: Some(key.into()),
        schema_version: 1,
    }
}

fn suggestion(signature: &CollaborationSemanticSignature) -> SemanticCollaborationSuggestion {
    SemanticCollaborationSuggestion {
        required_capability_ids: signature.required_capability_ids.clone(),
        required_skill_ids: signature.required_skill_ids.clone(),
        required_tool_capabilities: signature.required_tool_capabilities.clone(),
        dependency_shapes: signature.dependency_shapes.clone(),
        acceptance_kinds: signature.acceptance_kinds.clone(),
        result_field_shapes: signature.result_field_shapes.clone(),
    }
}
