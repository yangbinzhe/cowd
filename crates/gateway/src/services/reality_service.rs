use std::path::Path;

use chrono::Utc;
use harness_contract::reality::{RealityBoundary, RealityCapabilityStatus, RecallSourceKind};
use memory::types::MemoryLayer;
use memory::{rank_candidates, RecallCandidate, RecallOmission, RecallReport, RecallSourceResult};
use runtime::{ContextAuthority, ContextItem, ContextRole, ContextSourceKind, ContextVisibility};
use serde::{Deserialize, Serialize};

use super::{AuditService, ContextService, GrowthService, MatrixService, MemoryService};
use crate::services::{service_envelope, ServiceEnvelope};

#[derive(Clone)]
pub(crate) struct RealityService {
    pub(crate) label: &'static str,
    pub(crate) owner: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RealityFlowQuery {
    pub(crate) session_id: Option<String>,
    pub(crate) limit: usize,
}

pub(crate) type RealityStatusProjection = serde_json::Value;
pub(crate) type RealityStaticProjection = serde_json::Value;
pub(crate) type FactFlowProjection = serde_json::Value;
pub(crate) type RealityBoundaryProjection = serde_json::Value;
pub(crate) type PromotionTraceProjection = serde_json::Value;

#[derive(Debug, Clone, Default)]
pub(crate) struct RealityRecallAugmentation {
    pub(crate) candidates: Vec<RecallCandidate>,
    pub(crate) sources: Vec<RecallSourceResult>,
    pub(crate) context_items: Vec<ContextItem>,
}

impl RealityService {
    pub(crate) fn new() -> Self {
        Self {
            label: "reality",
            owner: "0.9.380 Reality Core service boundary",
        }
    }

    pub(crate) fn envelope(&self, operation: &'static str) -> ServiceEnvelope {
        service_envelope(self.label, self.owner, operation)
    }

    pub(crate) fn status_contract(&self) -> ServiceEnvelope {
        self.envelope("status")
    }

    pub(crate) fn static_contract(&self) -> ServiceEnvelope {
        self.envelope("static")
    }

    pub(crate) fn flow_contract(&self) -> ServiceEnvelope {
        self.envelope("flow")
    }

    pub(crate) fn promotions_contract(&self) -> ServiceEnvelope {
        self.envelope("promotions")
    }

    pub(crate) fn boundaries_contract(&self) -> ServiceEnvelope {
        self.envelope("boundaries")
    }

    pub(super) fn contracts(&self) -> Vec<ServiceEnvelope> {
        vec![
            self.status_contract(),
            self.static_contract(),
            self.flow_contract(),
            self.promotions_contract(),
            self.boundaries_contract(),
        ]
    }

    pub(crate) async fn status_projection(
        &self,
        config_home: &Path,
        memory: &MemoryService,
        matrix: &MatrixService,
        growth: &GrowthService,
        context: &ContextService,
        audit: &AuditService,
    ) -> RealityStatusProjection {
        let memory_status = memory.status_projection().await;
        let knowledge_status = memory.knowledge_projection(config_home).await;
        let matrix_health = matrix_health_value(matrix, config_home);
        let growth_events = growth
            .durable_event_log(config_home)
            .unwrap_or_else(|_| growth.event_log());
        let growth_promotions = growth
            .durable_promotion_log(config_home)
            .unwrap_or_default();
        let degraded_reasons = degraded_reasons(&memory_status, &matrix_health);
        let capabilities = reality_capabilities(&memory_status, &knowledge_status, &matrix_health);

        serde_json::json!({
            "kind": "reality.status",
            "ok": degraded_reasons.is_empty(),
            "generated_at": Utc::now(),
            "envelope": self.status_contract(),
            "reality_core": {
                "status": if degraded_reasons.is_empty() { "ready" } else { "degraded" },
                "degraded": !degraded_reasons.is_empty(),
                "degraded_reasons": degraded_reasons,
            },
            "capabilities": capabilities,
            "engines": {
                "fact_kernel": {
                    "status": "ready",
                    "role": "internal semantic rules",
                    "writes": false,
                },
                "memory": engine_summary("memory", &memory.status(), memory_status),
                "knowledge_fabric": {
                    "status": status_string(&knowledge_status),
                    "role": "Universal knowledge corpus governance, canon packs, activation policy, compliance warnings, and evidence routing over Memory.",
                    "writes": false,
                    "api": "/api/memory/knowledge",
                    "projection": knowledge_status,
                },
                "matrix": engine_summary("matrix", &matrix.health(), matrix_health),
                "growth": {
                    "status": "ready",
                    "event_count": growth_events.len(),
                    "promotion_count": growth_promotions.len(),
                    "envelope": growth.event_log_contract(),
                },
                "context": {
                    "status": "ready",
                    "envelope": context.snapshot(),
                },
                "audit": {
                    "status": "ready",
                    "envelope": audit.audit_projection(),
                },
            },
            "latest": {
                "growth_event": growth_events.first(),
                "promotion": growth_promotions.first(),
            },
        })
    }

    pub(crate) async fn static_projection(
        &self,
        config_home: &Path,
        memory: &MemoryService,
        matrix: &MatrixService,
        growth: &GrowthService,
        context: &ContextService,
        audit: &AuditService,
    ) -> RealityStaticProjection {
        let memory_status = memory.status_projection().await;
        let knowledge_status = memory.knowledge_projection(config_home).await;
        let matrix_health = matrix_health_value(matrix, config_home);
        serde_json::json!({
            "kind": "reality.static",
            "ok": true,
            "generated_at": Utc::now(),
            "envelope": self.static_contract(),
            "core_map": [
                {
                    "id": "fact-kernel",
                    "label": "fact-kernel",
                    "role": "Fact semantics, evidence, confidence, promotion policy, and health checks.",
                    "status": "ready",
                    "writes": false,
                    "api": null,
                },
                {
                    "id": "memory",
                    "label": "Memory Engine",
                    "role": "Unstructured long-term memory, preferences, decisions, summaries, and semantic recall.",
                    "status": status_string(&memory_status),
                    "writes": true,
                    "api": "/api/memory/*",
                },
                {
                    "id": "knowledge-fabric",
                    "label": "Knowledge Fabric",
                    "role": "Default/shared/project corpus governance, canon rules, activation, conflicts, and compliance evidence derived from Memory.",
                    "status": status_string(&knowledge_status),
                    "writes": false,
                    "api": "/api/memory/knowledge",
                },
                {
                    "id": "matrix",
                    "label": "Matrix Engine",
                    "role": "Structured facts, entities, relations, metrics, evidence, lineage, and computation.",
                    "status": status_string(&matrix_health),
                    "writes": true,
                    "api": "/api/matrix/*",
                },
                {
                    "id": "growth",
                    "label": "Growth Channel",
                    "role": "Candidate promotion channel from runtime events into fact-kernel, Memory, and Matrix.",
                    "status": "ready",
                    "writes": true,
                    "api": "/api/growth/*",
                },
                {
                    "id": "context",
                    "label": "Context Bridge",
                    "role": "Current task context assembly and evidence routing.",
                    "status": "ready",
                    "writes": false,
                    "api": "/api/context/*",
                },
                {
                    "id": "audit",
                    "label": "Audit Trace",
                    "role": "Approval, risk, promotion, and execution trace governance.",
                    "status": "ready",
                    "writes": false,
                    "api": "/api/audit/*",
                }
            ],
            "boundaries": ["observed", "inferred", "simulated", "hypothetical", "conflict"],
            "management": [
                {
                    "id": "overview",
                    "label": "Reality overview",
                    "scope": "System health, engine readiness, contracts, latest growth event, and latest promotion receipt.",
                    "route": "/reality?section=overview",
                    "api": ["/api/reality/status", "/api/reality/static"],
                    "owner": "gateway.reality",
                    "mode": "read"
                },
                {
                    "id": "memory",
                    "label": "Memory Engine",
                    "scope": "Memory layers, recall, fact checks, symbol links, maintenance candidates, and memory packets.",
                    "route": "/memory",
                    "api": ["/api/memory/status", "/api/memory/layers", "/api/memory/search", "/api/memory/maintenance", "/api/memory/packet"],
                    "owner": "gateway.memory",
                    "mode": "read-write"
                },
                {
                    "id": "knowledge-fabric",
                    "label": "Knowledge Fabric",
                    "scope": "Shared knowledge packs, project corpora, canon rules, compliance warnings, and activation evidence.",
                    "route": "/reality?section=knowledge-fabric",
                    "api": ["/api/memory/knowledge", "/api/memory/knowledge/health"],
                    "owner": "gateway.memory",
                    "mode": "read"
                },
                {
                    "id": "matrix",
                    "label": "Matrix Engine",
                    "scope": "Structured source packs, facts, entities, relations, metrics, evidence, quality gates, and lineage.",
                    "route": "/reality?section=matrix",
                    "api": ["/api/matrix/*"],
                    "owner": "gateway.matrix",
                    "mode": "read-write"
                },
                {
                    "id": "growth",
                    "label": "Growth Channel",
                    "scope": "Runtime growth events, promotion decisions, Memory/Matrix targets, and held conflict boundaries.",
                    "route": "/reality?section=fact-flow",
                    "api": ["/api/growth/status", "/api/growth/events", "/api/reality/flow", "/api/reality/promotions"],
                    "owner": "gateway.growth",
                    "mode": "read"
                },
                {
                    "id": "context",
                    "label": "Context Bridge",
                    "scope": "Current context packets, recall reports, evidence routing, budget pressure, and session recommendations.",
                    "route": "/context",
                    "api": ["/api/context/current", "/api/reality/recall/report", "/api/reality/context/envelope", "/api/reality/evidence/:id", "/api/evidence/resolve", "/api/sessions/:id/context/recommendations"],
                    "owner": "gateway.context",
                    "mode": "read-write"
                },
                {
                    "id": "audit",
                    "label": "Audit Trace",
                    "scope": "Approval history, risk receipts, cross-plane audit, runtime executions, and release gates.",
                    "route": "/audit",
                    "api": ["/api/audit/*", "/api/approval/*", "/api/cross-plane/audit", "/api/cowd/release-gate"],
                    "owner": "gateway.audit",
                    "mode": "read"
                },
                {
                    "id": "gateway",
                    "label": "Gateway Control",
                    "scope": "Surfaces, connector health, platform channels, runtime service contracts, and backend readiness.",
                    "route": "/gateway",
                    "api": ["/api/surfaces/*", "/api/connectors/*", "/api/platforms", "/api/runtime/control-plane"],
                    "owner": "gateway.system",
                    "mode": "read-write"
                }
            ],
            "contracts": {
                "reality": self.contracts(),
                "memory": memory.contracts(),
                "matrix": matrix.contracts(),
                "growth": growth.contracts(),
                "context": context.contracts(),
                "audit": audit.contracts(),
            }
        })
    }

    pub(crate) async fn flow_projection(
        &self,
        config_home: &Path,
        growth: &GrowthService,
        query: RealityFlowQuery,
    ) -> FactFlowProjection {
        let events = filter_events(
            growth
                .durable_event_log(config_home)
                .unwrap_or_else(|_| growth.event_log()),
            query.session_id.as_deref(),
            query.limit,
        );
        let promotions = filter_promotions(
            growth
                .durable_promotion_log(config_home)
                .unwrap_or_default(),
            query.session_id.as_deref(),
            query.limit.saturating_mul(6).max(12),
        );
        let stages = fact_flow_stages(&events, &promotions);

        serde_json::json!({
            "kind": "reality.fact_flow",
            "ok": true,
            "generated_at": Utc::now(),
            "envelope": self.flow_contract(),
            "session_id": query.session_id,
            "source": "growth.promotions",
            "degraded": false,
            "degraded_reasons": [],
            "stage_count": stages.len(),
            "event_count": events.len(),
            "promotion_count": promotions.len(),
            "stages": stages,
            "events": events,
            "promotions": promotions,
        })
    }

    pub(crate) fn promotions_projection(
        &self,
        config_home: &Path,
        growth: &GrowthService,
        session_id: Option<&str>,
        target: Option<&str>,
        status: Option<&str>,
        limit: usize,
    ) -> PromotionTraceProjection {
        let mut promotions = filter_promotions(
            growth
                .durable_promotion_log(config_home)
                .unwrap_or_default(),
            session_id,
            limit,
        );
        if let Some(target) = target {
            promotions.retain(|promotion| promotion.target == target);
        }
        if let Some(status) = status {
            promotions.retain(|promotion| promotion.status == status);
        }
        serde_json::json!({
            "kind": "reality.promotions",
            "ok": true,
            "generated_at": Utc::now(),
            "envelope": self.promotions_contract(),
            "total": promotions.len(),
            "promotions": promotions,
        })
    }

    pub(crate) fn boundaries_projection(
        &self,
        config_home: &Path,
        growth: &GrowthService,
    ) -> RealityBoundaryProjection {
        let promotions = growth
            .durable_promotion_log(config_home)
            .unwrap_or_default();
        let promoted = promotions
            .iter()
            .filter(|promotion| matches!(promotion.status.as_str(), "promote" | "promoted"))
            .count();
        let held = promotions
            .iter()
            .filter(|promotion| {
                matches!(
                    promotion.status.as_str(),
                    "hold" | "held" | "duplicate" | "conflict_held"
                )
            })
            .count();
        let rejected = promotions
            .iter()
            .filter(|promotion| promotion.status == "reject")
            .count();
        serde_json::json!({
            "kind": "reality.boundaries",
            "ok": true,
            "generated_at": Utc::now(),
            "envelope": self.boundaries_contract(),
            "boundaries": [
                {
                    "id": "observed",
                    "label": "Observed",
                    "count": promoted,
                    "meaning": "Evidence-backed candidates accepted into fact, Memory, or Matrix targets."
                },
                {
                    "id": "inferred",
                    "label": "Inferred",
                    "count": 0,
                    "meaning": "Reasoned candidates not yet represented as authoritative observed facts."
                },
                {
                    "id": "simulated",
                    "label": "Simulated",
                    "count": 0,
                    "meaning": "Scenario outputs that must remain isolated from authoritative stores."
                },
                {
                    "id": "hypothetical",
                    "label": "Hypothetical",
                    "count": rejected,
                    "meaning": "Candidates blocked by fact-kernel promotion policy or explicit non-promotion boundary."
                },
                {
                    "id": "conflict",
                    "label": "Conflict",
                    "count": held,
                    "meaning": "Held candidates requiring evidence, confidence, or conflict review."
                }
            ],
            "source": "growth.promotion_receipts",
        })
    }

    pub(crate) fn recall_augmentation(
        &self,
        config_home: &Path,
        matrix: &MatrixService,
        growth: &GrowthService,
        query: &str,
        limit: usize,
    ) -> RealityRecallAugmentation {
        let limit = limit.clamp(1, 100);
        let mut augmentation = RealityRecallAugmentation::default();
        let mut fact_omitted = 0usize;
        let fact_hits = growth.recall_facts(query, limit);
        for hit in fact_hits {
            let fact = hit.fact;
            let boundary = fact_boundary(&fact);
            if !boundary.can_be_authoritative() {
                fact_omitted += 1;
                continue;
            }
            let refs = fact_references(&fact);
            let candidate = RecallCandidate::from_external(
                format!("Fact {} · {}", fact.id.as_str(), fact.fact_type),
                fact.statement.clone(),
                MemoryLayer::L3,
                RecallSourceKind::Fact,
                (hit.score as f32 / 40.0).clamp(0.1, 1.0),
                fact.confidence.basis_points() as f32 / 10_000.0,
                refs.clone(),
                boundary,
            );
            augmentation
                .context_items
                .push(fact_context_item(&fact, &refs, boundary));
            augmentation.candidates.push(candidate);
        }
        augmentation.sources.push(RecallSourceResult {
            source: RecallSourceKind::Fact,
            status: "enabled_and_wired".to_string(),
            selected_count: augmentation
                .candidates
                .iter()
                .filter(|candidate| candidate.source == RecallSourceKind::Fact)
                .count(),
            omitted_count: fact_omitted,
            degraded_reason: None,
        });

        match matrix.list_facts(config_home, limit.saturating_mul(3)) {
            Ok(facts) => {
                let mut selected = 0usize;
                let mut omitted = 0usize;
                for fact in facts {
                    if selected >= limit {
                        omitted += 1;
                        continue;
                    }
                    let score = matrix_fact_score(&fact, query);
                    if score <= 0.0 {
                        omitted += 1;
                        continue;
                    }
                    let boundary = matrix_boundary(fact.confidence);
                    if !boundary.can_be_authoritative() {
                        omitted += 1;
                        continue;
                    }
                    let refs = matrix_fact_references(&fact);
                    let content = matrix_fact_summary(&fact);
                    augmentation.candidates.push(RecallCandidate::from_external(
                        format!("Matrix fact {} · {}", fact.fact_id, fact.fact_type),
                        content.clone(),
                        MemoryLayer::L4,
                        RecallSourceKind::Matrix,
                        score,
                        fact.confidence,
                        refs.clone(),
                        boundary,
                    ));
                    augmentation.context_items.push(matrix_context_item(
                        &fact.fact_id,
                        content,
                        &refs,
                        fact.confidence,
                    ));
                    selected += 1;
                }
                match matrix.list_evidence_packets(config_home, limit) {
                    Ok(packets) => {
                        for packet in packets {
                            if selected >= limit {
                                omitted += 1;
                                continue;
                            }
                            let score = text_overlap_score(query, &packet.problem_statement);
                            if score <= 0.0 && !query.trim().is_empty() {
                                omitted += 1;
                                continue;
                            }
                            let boundary = matrix_boundary(packet.confidence);
                            if !boundary.can_be_authoritative() {
                                omitted += 1;
                                continue;
                            }
                            let refs = vec![format!("matrix:evidence:{}", packet.packet_id)];
                            let content = packet.context_summary();
                            augmentation.candidates.push(RecallCandidate::from_external(
                                format!("Matrix evidence {}", packet.packet_id),
                                content.clone(),
                                MemoryLayer::L4,
                                RecallSourceKind::Matrix,
                                score.max(0.35),
                                packet.confidence,
                                refs.clone(),
                                boundary,
                            ));
                            augmentation.context_items.push(matrix_context_item(
                                &packet.packet_id,
                                content,
                                &refs,
                                packet.confidence,
                            ));
                            selected += 1;
                        }
                    }
                    Err(error) => augmentation.sources.push(RecallSourceResult {
                        source: RecallSourceKind::Matrix,
                        status: "degraded".to_string(),
                        selected_count: selected,
                        omitted_count: omitted,
                        degraded_reason: Some(format!(
                            "matrix evidence recall unavailable: {error}"
                        )),
                    }),
                }
                if !augmentation
                    .sources
                    .iter()
                    .any(|source| source.source == RecallSourceKind::Matrix)
                {
                    augmentation.sources.push(RecallSourceResult {
                        source: RecallSourceKind::Matrix,
                        status: "enabled_and_wired".to_string(),
                        selected_count: selected,
                        omitted_count: omitted,
                        degraded_reason: None,
                    });
                }
            }
            Err(error) => augmentation.sources.push(RecallSourceResult {
                source: RecallSourceKind::Matrix,
                status: "degraded".to_string(),
                selected_count: 0,
                omitted_count: 0,
                degraded_reason: Some(format!("matrix fact recall unavailable: {error}")),
            }),
        }

        augmentation
    }

    pub(crate) fn augment_recall_report(
        &self,
        config_home: &Path,
        matrix: &MatrixService,
        growth: &GrowthService,
        query: &str,
        max_items: usize,
        report: &mut RecallReport,
    ) -> RealityRecallAugmentation {
        let augmentation = self.recall_augmentation(config_home, matrix, growth, query, max_items);
        report.selected.extend(augmentation.candidates.clone());
        rank_candidates(&mut report.selected);
        let max_items = max_items.max(1);
        if report.selected.len() > max_items {
            let overflow = report.selected.split_off(max_items);
            report
                .omitted
                .extend(overflow.into_iter().map(|candidate| RecallOmission {
                    id: candidate.id,
                    title: candidate.title,
                    source: candidate.source,
                    reason: "reality recall report budget exhausted".to_string(),
                }));
            report.truncated = true;
        }
        for source in &augmentation.sources {
            merge_recall_source(&mut report.sources, source.clone());
        }
        augmentation
    }
}

fn merge_recall_source(sources: &mut Vec<RecallSourceResult>, incoming: RecallSourceResult) {
    if let Some(existing) = sources
        .iter_mut()
        .find(|source| source.source == incoming.source)
    {
        existing.selected_count = existing
            .selected_count
            .saturating_add(incoming.selected_count);
        existing.omitted_count = existing
            .omitted_count
            .saturating_add(incoming.omitted_count);
        if incoming.status == "degraded" {
            existing.status = incoming.status;
            existing.degraded_reason = incoming.degraded_reason;
        }
    } else {
        sources.push(incoming);
    }
}

fn fact_boundary(fact: &fact_kernel::FactRecord) -> RealityBoundary {
    let status = fact.status.to_ascii_lowercase();
    if matches!(
        status.as_str(),
        "conflict"
            | "conflicted"
            | "superseded"
            | "stale"
            | "archived"
            | "rejected"
            | "held"
            | "hold"
    ) {
        return RealityBoundary::Conflict;
    }
    if matches!(status.as_str(), "simulated" | "simulation") {
        return RealityBoundary::Simulated;
    }
    if matches!(status.as_str(), "hypothetical" | "candidate") {
        return RealityBoundary::Hypothetical;
    }
    if fact.confidence.basis_points() < 5_000 {
        return RealityBoundary::Inferred;
    }
    RealityBoundary::Observed
}

fn fact_references(fact: &fact_kernel::FactRecord) -> Vec<String> {
    let mut refs = vec![format!("fact:{}", fact.id.as_str())];
    refs.extend(
        fact.evidence
            .iter()
            .map(|evidence| format!("fact:evidence:{}", evidence.as_str())),
    );
    refs
}

fn fact_context_item(
    fact: &fact_kernel::FactRecord,
    refs: &[String],
    boundary: RealityBoundary,
) -> ContextItem {
    let mut item = ContextItem::new(
        format!("fact:{}", fact.id.as_str()),
        ContextSourceKind::Fact,
        ContextRole::Evidence,
        format!(
            "Fact {} ({})\nstatus: {}\nboundary: {}\nconfidence_bp: {}\nstatement: {}",
            fact.id.as_str(),
            fact.fact_type,
            fact.status,
            boundary.as_str(),
            fact.confidence.basis_points(),
            fact.statement
        ),
    );
    item.authority = if boundary == RealityBoundary::Observed {
        ContextAuthority::Derived
    } else {
        ContextAuthority::Tool
    };
    item.visibility = ContextVisibility::Shared;
    item.score = fact.confidence.basis_points() as f32 / 10_000.0;
    item.evidence = refs.to_vec();
    item
}

fn matrix_boundary(confidence: f32) -> RealityBoundary {
    if confidence >= 0.5 {
        RealityBoundary::Observed
    } else if confidence >= 0.35 {
        RealityBoundary::Inferred
    } else {
        RealityBoundary::Hypothetical
    }
}

fn matrix_fact_references(fact: &matrix_core::MatrixFact) -> Vec<String> {
    let mut refs = vec![format!("matrix:fact:{}", fact.fact_id)];
    if let Some(source_ref) = &fact.source_ref {
        refs.push(source_ref.clone());
    }
    refs
}

fn matrix_fact_summary(fact: &matrix_core::MatrixFact) -> String {
    format!(
        "Matrix fact {}\ntype: {}\nmetric: {}\nentities: {}\nconfidence: {:.2}\ndimensions: {}\nmeasures: {}",
        fact.fact_id,
        fact.fact_type,
        fact.metric_key.as_deref().unwrap_or("none"),
        fact.entity_refs.join(", "),
        fact.confidence,
        fact.dimensions,
        fact.measures
    )
}

fn matrix_context_item(id: &str, content: String, refs: &[String], confidence: f32) -> ContextItem {
    let mut item = ContextItem::new(
        format!("matrix:{id}"),
        ContextSourceKind::Matrix,
        ContextRole::Evidence,
        content,
    );
    item.authority = ContextAuthority::Derived;
    item.visibility = ContextVisibility::Shared;
    item.score = confidence.clamp(0.0, 1.0);
    item.evidence = refs.to_vec();
    item
}

fn matrix_fact_score(fact: &matrix_core::MatrixFact, query: &str) -> f32 {
    let haystack = format!(
        "{} {} {} {} {}",
        fact.fact_id,
        fact.fact_type,
        fact.metric_key.as_deref().unwrap_or_default(),
        fact.entity_refs.join(" "),
        fact.measures
    );
    text_overlap_score(query, &haystack).max((fact.confidence * 0.3).min(0.3))
}

fn text_overlap_score(query: &str, text: &str) -> f32 {
    let query = query.trim().to_ascii_lowercase();
    if query.is_empty() {
        return 0.35;
    }
    let text = text.to_ascii_lowercase();
    if text.contains(&query) {
        return 0.95;
    }
    let tokens = query
        .split(|ch: char| !ch.is_alphanumeric() && ch != '_' && ch != '-')
        .filter(|token| token.len() >= 3)
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        return 0.0;
    }
    let matched = tokens.iter().filter(|token| text.contains(**token)).count();
    (matched as f32 / tokens.len() as f32).clamp(0.0, 1.0)
}

fn matrix_health_value(matrix: &MatrixService, config_home: &Path) -> serde_json::Value {
    match matrix.repository_health(config_home) {
        Ok(health) => serde_json::json!({
            "status": "ready",
            "ok": true,
            "health": health,
        }),
        Err(error) => serde_json::json!({
            "status": "degraded",
            "ok": false,
            "degraded_reason": error.to_string(),
        }),
    }
}

fn engine_summary(
    id: &'static str,
    envelope: &ServiceEnvelope,
    projection: serde_json::Value,
) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "status": status_string(&projection),
        "envelope": envelope,
        "projection": projection,
    })
}

fn status_string(value: &serde_json::Value) -> String {
    value
        .get("status")
        .and_then(serde_json::Value::as_str)
        .or_else(|| {
            value
                .pointer("/health/status")
                .and_then(serde_json::Value::as_str)
        })
        .unwrap_or_else(|| {
            if value.get("ok").and_then(serde_json::Value::as_bool) == Some(false) {
                "degraded"
            } else {
                "ready"
            }
        })
        .to_string()
}

fn reality_capabilities(
    memory_status: &serde_json::Value,
    knowledge_status: &serde_json::Value,
    matrix_health: &serde_json::Value,
) -> serde_json::Value {
    let matrix_status =
        if matrix_health.get("ok").and_then(serde_json::Value::as_bool) == Some(false) {
            RealityCapabilityStatus::Degraded
        } else {
            RealityCapabilityStatus::EnabledAndWired
        };

    serde_json::json!({
        "memory": memory_status
            .get("capabilities")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({})),
        "knowledge_fabric": {
            "status": knowledge_status
                .get("capability_status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or(RealityCapabilityStatus::ConfiguredButUnwired.as_str()),
            "reason": knowledge_status
                .get("degraded_reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("KnowledgeFabric durable projection is provided by MemoryService when storage is available"),
            "projection_mode": knowledge_status
                .get("projection_mode")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("durable_knowledge_store"),
        },
        "matrix_context_source": {
            "status": matrix_status.as_str(),
            "reason": if matrix_status == RealityCapabilityStatus::Degraded {
                matrix_health
                    .get("degraded_reason")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("matrix repository degraded")
            } else {
                "Matrix facts and evidence packets are read through MatrixService and merged into Reality recall reports/context projections as lightweight evidence refs; details expand through /api/reality/evidence/:id"
            },
        },
        "fact_runtime": {
            "status": RealityCapabilityStatus::EnabledAndWired.as_str(),
            "reason": "GrowthService injects a gateway-owned durable FactStore into FactKernelService, persists promoted facts/evidence in storage/fact.sqlite, and exposes fact recall to Reality reports/context projections",
        },
        "context_envelope": memory_status
            .pointer("/capabilities/context_envelope")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({
                "status": RealityCapabilityStatus::ConfiguredButUnwired.as_str(),
                "reason": "ContextEnvelope status is not exposed by memory projection",
            })),
    })
}

fn degraded_reasons(
    memory_status: &serde_json::Value,
    matrix_health: &serde_json::Value,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if memory_status
        .get("enabled")
        .and_then(serde_json::Value::as_bool)
        == Some(false)
    {
        reasons.push("memory not configured".to_string());
    }
    if matrix_health.get("ok").and_then(serde_json::Value::as_bool) == Some(false) {
        reasons.push(
            matrix_health
                .get("degraded_reason")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("matrix degraded")
                .to_string(),
        );
    }
    reasons.sort();
    reasons.dedup();
    reasons
}

fn filter_events(
    mut events: Vec<harness_contract::growth::GrowthEvent>,
    session_id: Option<&str>,
    limit: usize,
) -> Vec<harness_contract::growth::GrowthEvent> {
    if let Some(session_id) = session_id {
        events.retain(|event| event.session_id == session_id);
    }
    events.truncate(limit.max(1));
    events
}

fn filter_promotions(
    mut promotions: Vec<super::growth_service::GrowthPromotionReceipt>,
    session_id: Option<&str>,
    limit: usize,
) -> Vec<super::growth_service::GrowthPromotionReceipt> {
    if let Some(session_id) = session_id {
        let needle = format!("session:{session_id}");
        promotions.retain(|promotion| {
            promotion.summary.contains(session_id)
                || promotion
                    .target_id
                    .as_deref()
                    .is_some_and(|id| id.contains(session_id))
                || promotion.summary.contains(&needle)
        });
    }
    promotions.truncate(limit.max(1));
    promotions
}

fn fact_flow_stages(
    events: &[harness_contract::growth::GrowthEvent],
    promotions: &[super::growth_service::GrowthPromotionReceipt],
) -> Vec<serde_json::Value> {
    let mut stages = Vec::new();
    for event in events {
        stages.push(serde_json::json!({
            "stage_id": format!("event:{}", event.id),
            "kind": "event",
            "status": "observed",
            "summary": event.source_event_kind,
            "source_ref": event.id,
            "target_ref": null,
            "evidence_refs": event.evidence_refs,
            "decision": null,
            "reason": null,
            "confidence_bp": event.confidence_bp,
        }));
        for evidence in &event.evidence_refs {
            stages.push(serde_json::json!({
                "stage_id": format!("evidence:{}:{}", event.id, evidence.reference),
                "kind": "evidence",
                "status": "attached",
                "summary": evidence.summary,
                "source_ref": evidence.reference,
                "target_ref": event.id,
                "evidence_refs": [evidence],
                "decision": null,
                "reason": null,
                "confidence_bp": event.confidence_bp,
            }));
        }
        for candidate in &event.memory_candidates {
            stages.push(serde_json::json!({
                "stage_id": format!("memory-candidate:{}:{}", event.id, candidate.id),
                "kind": "memory_candidate",
                "status": "candidate",
                "summary": candidate.summary,
                "source_ref": event.id,
                "target_ref": candidate.id,
                "evidence_refs": event.evidence_refs,
                "decision": null,
                "reason": candidate.reason,
                "confidence_bp": candidate.confidence_bp,
            }));
        }
        for signal in &event.matrix_signals {
            stages.push(serde_json::json!({
                "stage_id": format!("matrix-signal:{}:{}", event.id, signal.fact_type),
                "kind": "matrix_signal",
                "status": "candidate",
                "summary": signal.fact_type,
                "source_ref": event.id,
                "target_ref": format!("growth-matrix:{}:{}", event.id, signal.fact_type),
                "evidence_refs": event.evidence_refs,
                "decision": null,
                "reason": null,
                "confidence_bp": signal.confidence_bp,
            }));
        }
    }

    for promotion in promotions {
        stages.push(serde_json::json!({
            "stage_id": format!(
                "promotion:{}:{}",
                promotion.target,
                promotion.target_id.as_deref().unwrap_or(&promotion.status)
            ),
            "kind": promotion_kind(&promotion.target),
            "status": promotion.status,
            "summary": promotion.summary,
            "source_ref": null,
            "target_ref": promotion.target_id,
            "evidence_refs": [],
            "decision": promotion.status,
            "reason": promotion.summary,
            "confidence_bp": null,
            "error": promotion.error,
        }));
    }
    stages
}

fn promotion_kind(target: &str) -> &'static str {
    match target {
        "fact.memory" | "fact.matrix" => "fact_decision",
        "memory.entry" => "memory_target",
        "matrix.fact" => "matrix_target",
        _ => "promotion",
    }
}
