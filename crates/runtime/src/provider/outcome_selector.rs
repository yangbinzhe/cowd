//! Pure Provider ordering from immutable Outcome evidence.

use std::collections::BTreeMap;

use harness_contract::strategy::ExecutionCandidateKind;
use serde::{Deserialize, Serialize};

use crate::{OutcomeReadSnapshot, RoutingMode};

const MIN_AUTO_SAMPLES: u64 = 3;
const MAX_SAMPLE_AGE_MS: u64 = 30 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderSelectionCandidateReceipt {
    pub model: String,
    pub provider_segments: Vec<String>,
    pub eligible: bool,
    pub sample_count: u64,
    pub paired_sample_count: u64,
    pub quality_mean_bp: Option<u16>,
    pub duration_p50_ms: u64,
    pub total_tokens_p50: u64,
    pub exclusion_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderSelectionReceipt {
    pub requested_mode: RoutingMode,
    pub effective_mode: RoutingMode,
    pub snapshot_revision: u64,
    pub selected_model: String,
    pub fallback_reason: Option<String>,
    pub candidates: Vec<ProviderSelectionCandidateReceipt>,
}

pub fn select_provider_from_outcome_snapshot(
    mode: RoutingMode,
    configured_models: &[String],
    config_revision: &str,
    policy_revision: Option<&str>,
    selected_candidate: Option<ExecutionCandidateKind>,
    snapshot: &OutcomeReadSnapshot,
    now_ms: u64,
) -> (Vec<String>, ProviderSelectionReceipt) {
    let pinned = configured_models.to_vec();
    if mode == RoutingMode::Pinned || configured_models.len() < 2 {
        return (
            pinned.clone(),
            ProviderSelectionReceipt {
                requested_mode: mode,
                effective_mode: RoutingMode::Pinned,
                snapshot_revision: snapshot.revision,
                selected_model: pinned.first().cloned().unwrap_or_default(),
                fallback_reason: (mode == RoutingMode::Auto).then(|| {
                    "auto routing requires at least two configured candidates".to_string()
                }),
                candidates: Vec::new(),
            },
        );
    }

    let mut receipts = configured_models
        .iter()
        .map(|model| {
            candidate_receipt(
                model,
                config_revision,
                policy_revision,
                selected_candidate,
                snapshot,
                now_ms,
            )
        })
        .collect::<Vec<_>>();
    let Some(primary) = receipts.first() else {
        return (
            pinned,
            ProviderSelectionReceipt {
                requested_mode: mode,
                effective_mode: RoutingMode::Pinned,
                snapshot_revision: snapshot.revision,
                selected_model: String::new(),
                fallback_reason: Some("configured provider candidate set is empty".to_string()),
                candidates: receipts,
            },
        );
    };
    if !primary.eligible {
        return (
            pinned.clone(),
            ProviderSelectionReceipt {
                requested_mode: mode,
                effective_mode: RoutingMode::Pinned,
                snapshot_revision: snapshot.revision,
                selected_model: pinned.first().cloned().unwrap_or_default(),
                fallback_reason: Some(
                    "primary has insufficient comparable evidence; fail closed to pinned"
                        .to_string(),
                ),
                candidates: receipts,
            },
        );
    }
    let primary_quality = primary.quality_mean_bp.unwrap_or_default();
    for candidate in receipts.iter_mut().skip(1) {
        if candidate.eligible && candidate.quality_mean_bp.unwrap_or_default() < primary_quality {
            candidate.eligible = false;
            candidate
                .exclusion_reasons
                .push("quality is below the protected primary".to_string());
        }
    }
    let mut eligible = receipts
        .iter()
        .filter(|candidate| candidate.eligible)
        .cloned()
        .collect::<Vec<_>>();
    eligible.sort_by(|left, right| {
        right
            .quality_mean_bp
            .cmp(&left.quality_mean_bp)
            .then_with(|| left.duration_p50_ms.cmp(&right.duration_p50_ms))
            .then_with(|| left.total_tokens_p50.cmp(&right.total_tokens_p50))
            .then_with(|| left.model.cmp(&right.model))
    });
    let selected = eligible
        .first()
        .map(|candidate| candidate.model.clone())
        .unwrap_or_else(|| configured_models[0].clone());
    let mut ordered = vec![selected.clone()];
    ordered.extend(
        configured_models
            .iter()
            .filter(|model| *model != &selected)
            .cloned(),
    );
    (
        ordered,
        ProviderSelectionReceipt {
            requested_mode: mode,
            effective_mode: RoutingMode::Auto,
            snapshot_revision: snapshot.revision,
            selected_model: selected,
            fallback_reason: None,
            candidates: receipts,
        },
    )
}

fn candidate_receipt(
    model: &str,
    config_revision: &str,
    policy_revision: Option<&str>,
    selected_candidate: Option<ExecutionCandidateKind>,
    snapshot: &OutcomeReadSnapshot,
    now_ms: u64,
) -> ProviderSelectionCandidateReceipt {
    let matching = snapshot
        .segments
        .values()
        .filter(|segment| {
            segment.key.as_ref().is_some_and(|key| {
                key.model == model
                    && key.config_revision == config_revision
                    && policy_revision.is_some_and(|policy| key.policy_revision == policy)
                    && selected_candidate.is_some_and(|candidate| key.candidate == candidate)
            })
        })
        .collect::<Vec<_>>();
    let provider_segments = matching
        .iter()
        .filter_map(|segment| segment.key.as_ref())
        .map(|key| format!("{}|{}|{}", key.provider, key.profile, key.protocol))
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let sample_count = matching.iter().fold(0_u64, |total, segment| {
        total.saturating_add(segment.sample_count)
    });
    let quality_count = matching.iter().fold(0_u64, |total, segment| {
        total.saturating_add(segment.quality_observed_count)
    });
    let paired_sample_count = matching.iter().fold(0_u64, |total, segment| {
        total.saturating_add(segment.paired_sample_count)
    });
    let complete_count = matching.iter().fold(0_u64, |total, segment| {
        total.saturating_add(segment.evidence_complete_count)
    });
    let latest = matching
        .iter()
        .map(|segment| segment.last_observed_at_ms)
        .max()
        .unwrap_or_default();
    let mut reasons = Vec::new();
    if policy_revision.is_none() || selected_candidate.is_none() {
        reasons.push("active strategy segment is unavailable".to_string());
    }
    if provider_segments.len() > 1 {
        reasons.push("model evidence spans ambiguous provider identities".to_string());
    }
    if sample_count < MIN_AUTO_SAMPLES {
        reasons.push(format!(
            "sample count {sample_count} is below {MIN_AUTO_SAMPLES}"
        ));
    }
    if paired_sample_count < MIN_AUTO_SAMPLES || paired_sample_count != sample_count {
        reasons.push("paired comparison coverage is incomplete".to_string());
    }
    if quality_count != sample_count {
        reasons.push("quality evidence coverage is incomplete".to_string());
    }
    if complete_count != sample_count {
        reasons.push("execution evidence completeness is insufficient".to_string());
    }
    if latest == 0 || now_ms.saturating_sub(latest) > MAX_SAMPLE_AGE_MS {
        reasons.push("outcome evidence is stale".to_string());
    }
    let weighted = |value: fn(&crate::OutcomeSegmentSnapshot) -> u64| {
        matching
            .iter()
            .fold(0_u64, |total, segment| {
                total.saturating_add(value(segment).saturating_mul(segment.sample_count))
            })
            .saturating_div(sample_count.max(1))
    };
    let quality_mean_bp = if sample_count == 0 || quality_count != sample_count {
        None
    } else {
        let quality_by_value = matching
            .iter()
            .fold(BTreeMap::new(), |mut values, segment| {
                if let Some(quality) = segment.quality_mean_bp {
                    *values.entry(quality).or_insert(0_u64) += segment.sample_count;
                }
                values
            });
        let total = quality_by_value
            .iter()
            .fold(0_u64, |sum, (quality, count)| {
                sum.saturating_add(u64::from(*quality).saturating_mul(*count))
            });
        Some(u16::try_from(total / sample_count.max(1)).unwrap_or(10_000))
    };
    ProviderSelectionCandidateReceipt {
        model: model.to_string(),
        provider_segments,
        eligible: reasons.is_empty(),
        sample_count,
        paired_sample_count,
        quality_mean_bp,
        duration_p50_ms: weighted(|segment| segment.duration_p50_ms),
        total_tokens_p50: weighted(|segment| segment.total_tokens_p50),
        exclusion_reasons: reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::{outcome::OutcomeSegmentKey, strategy::ExecutionCandidateKind};

    fn segment(
        model: &str,
        quality: u16,
        duration: u64,
    ) -> (String, crate::OutcomeSegmentSnapshot) {
        let key = OutcomeSegmentKey {
            provider: "test".to_string(),
            model: model.to_string(),
            profile: "default".to_string(),
            protocol: "responses".to_string(),
            config_revision: "cfg".to_string(),
            policy_revision: "policy".to_string(),
            candidate: ExecutionCandidateKind::Direct,
        };
        (
            serde_json::to_string(&key).unwrap(),
            crate::OutcomeSegmentSnapshot {
                key: Some(key),
                sample_count: 3,
                paired_sample_count: 3,
                success_count: 3,
                failure_count: 0,
                evidence_complete_count: 3,
                quality_observed_count: 3,
                quality_mean_bp: Some(quality),
                last_observed_at_ms: 1_000,
                duration_p50_ms: duration,
                duration_p95_ms: duration,
                total_tokens_p50: 100,
                total_tokens_p95: 100,
                ..Default::default()
            },
        )
    }

    #[test]
    fn auto_is_evidence_gated_and_never_degrades_quality() {
        let mut snapshot = OutcomeReadSnapshot {
            revision: 7,
            ..Default::default()
        };
        snapshot.segments.extend([
            segment("primary", 8_000, 1_000),
            segment("fast", 8_000, 500),
            segment("bad", 7_000, 100),
        ]);
        let (ordered, receipt) = select_provider_from_outcome_snapshot(
            RoutingMode::Auto,
            &["primary".to_string(), "fast".to_string(), "bad".to_string()],
            "cfg",
            Some("policy"),
            Some(ExecutionCandidateKind::Direct),
            &snapshot,
            1_000,
        );
        assert_eq!(ordered[0], "fast");
        assert_eq!(receipt.effective_mode, RoutingMode::Auto);
        assert!(!receipt.candidates[2].eligible);
    }

    #[test]
    fn auto_falls_back_to_pinned_when_primary_evidence_is_missing() {
        let snapshot = OutcomeReadSnapshot::default();
        let (ordered, receipt) = select_provider_from_outcome_snapshot(
            RoutingMode::Auto,
            &["primary".to_string(), "fallback".to_string()],
            "cfg",
            Some("policy"),
            Some(ExecutionCandidateKind::Direct),
            &snapshot,
            1_000,
        );
        assert_eq!(ordered, vec!["primary", "fallback"]);
        assert_eq!(receipt.effective_mode, RoutingMode::Pinned);
    }

    #[test]
    fn auto_rejects_each_incomplete_or_ambiguous_evidence_dimension() {
        let (_, complete) = segment("primary", 8_000, 1_000);
        let cases = [
            {
                let mut value = complete.clone();
                value.sample_count = 2;
                value.paired_sample_count = 2;
                value.quality_observed_count = 2;
                value.evidence_complete_count = 2;
                ("sample count", value, 1_000)
            },
            {
                let mut value = complete.clone();
                value.paired_sample_count = 2;
                ("paired comparison", value, 1_000)
            },
            {
                let mut value = complete.clone();
                value.quality_observed_count = 2;
                ("quality evidence", value, 1_000)
            },
            {
                let mut value = complete.clone();
                value.evidence_complete_count = 2;
                ("evidence completeness", value, 1_000)
            },
            ("stale", complete.clone(), 1_000 + MAX_SAMPLE_AGE_MS + 1),
        ];
        for (expected_reason, segment, now_ms) in cases {
            let snapshot = OutcomeReadSnapshot {
                segments: BTreeMap::from([("case".to_string(), segment)]),
                ..Default::default()
            };
            let receipt = candidate_receipt(
                "primary",
                "cfg",
                Some("policy"),
                Some(ExecutionCandidateKind::Direct),
                &snapshot,
                now_ms,
            );
            assert!(!receipt.eligible);
            assert!(
                receipt
                    .exclusion_reasons
                    .iter()
                    .any(|reason| reason.contains(expected_reason)),
                "missing `{expected_reason}` exclusion in {:?}",
                receipt.exclusion_reasons
            );
        }

        let (_, mut alternate_provider) = segment("primary", 8_000, 900);
        alternate_provider
            .key
            .as_mut()
            .expect("segment key")
            .provider = "other".to_string();
        let snapshot = OutcomeReadSnapshot {
            segments: BTreeMap::from([
                ("primary-a".to_string(), complete),
                ("primary-b".to_string(), alternate_provider),
            ]),
            ..Default::default()
        };
        let receipt = candidate_receipt(
            "primary",
            "cfg",
            Some("policy"),
            Some(ExecutionCandidateKind::Direct),
            &snapshot,
            1_000,
        );
        assert!(!receipt.eligible);
        assert!(receipt
            .exclusion_reasons
            .iter()
            .any(|reason| reason.contains("ambiguous provider identities")));
    }
}
