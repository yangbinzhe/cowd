use std::path::Path;

use chrono::Utc;
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

impl RealityService {
    pub(crate) fn new() -> Self {
        Self {
            label: "reality",
            owner: "0.9.376 Reality Core service boundary",
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
        let matrix_health = matrix_health_value(matrix, config_home);
        let growth_events = growth
            .durable_event_log(config_home)
            .unwrap_or_else(|_| growth.event_log());
        let growth_promotions = growth
            .durable_promotion_log(config_home)
            .unwrap_or_default();
        let degraded_reasons = degraded_reasons(&memory_status, &matrix_health);

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
            "engines": {
                "fact_kernel": {
                    "status": "ready",
                    "role": "internal semantic rules",
                    "writes": false,
                },
                "memory": engine_summary("memory", &memory.status(), memory_status),
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
                    "scope": "Current context packets, evidence routing, budget pressure, and session recommendations.",
                    "route": "/context",
                    "api": ["/api/context/current", "/api/evidence/resolve", "/api/sessions/:id/context/recommendations"],
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
    mut events: Vec<ai_kernel::growth::GrowthEvent>,
    session_id: Option<&str>,
    limit: usize,
) -> Vec<ai_kernel::growth::GrowthEvent> {
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
    events: &[ai_kernel::growth::GrowthEvent],
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
