use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::IaccEntity;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccOntologyConcept {
    pub concept_id: String,
    pub name: String,
    pub domain: String,
    pub entity_type: String,
    #[serde(default)]
    pub required_attributes: Vec<String>,
    #[serde(default)]
    pub source_systems: Vec<String>,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccOntologyRelation {
    pub relation_type: String,
    pub from_concept_id: String,
    pub to_concept_id: String,
    pub cardinality: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccOntologyMetricBinding {
    pub metric_id: String,
    pub concept_id: String,
    pub grain: String,
    pub semantic_role: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccOntologyPack {
    pub ontology_id: String,
    pub domain: String,
    pub version: String,
    #[serde(default)]
    pub concepts: Vec<IaccOntologyConcept>,
    #[serde(default)]
    pub relations: Vec<IaccOntologyRelation>,
    #[serde(default)]
    pub metric_bindings: Vec<IaccOntologyMetricBinding>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccEntityMatchCandidate {
    pub candidate_id: String,
    pub left_entity_id: String,
    pub right_entity_id: String,
    pub match_type: String,
    pub confidence: f32,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccEntityConflictDecision {
    pub decision_id: String,
    pub candidate_id: String,
    pub decision: String,
    pub survivor_entity_id: String,
    pub retired_entity_id: String,
    pub survivorship_rule: String,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub decision_metadata: Value,
    pub decided_at: DateTime<Utc>,
}

#[must_use]
pub fn server_manufacturing_ontology_pack() -> IaccOntologyPack {
    IaccOntologyPack {
        ontology_id: "server_manufacturing_ontology".to_string(),
        domain: "server_manufacturing".to_string(),
        version: "v0.9.107".to_string(),
        concepts: vec![
            concept(
                "product",
                "Product",
                &["product_family", "lifecycle_state"],
                &["plm", "erp"],
            ),
            concept(
                "configuration",
                "Configuration",
                &["config_code", "option_set"],
                &["plm", "cpq"],
            ),
            concept(
                "bom",
                "BOM",
                &["bom_version", "effective_from"],
                &["plm", "erp"],
            ),
            concept(
                "component",
                "Component",
                &["part_number", "commodity"],
                &["plm", "erp", "mes"],
            ),
            concept(
                "supplier",
                "Supplier",
                &["supplier_code", "risk_tier"],
                &["erp", "srm"],
            ),
            concept(
                "purchase_order",
                "Purchase Order",
                &["po_no", "promise_date"],
                &["erp", "srm"],
            ),
            concept(
                "inventory_lot",
                "Inventory Lot",
                &["lot_no", "qty_available"],
                &["wms", "erp"],
            ),
            concept(
                "work_order",
                "Work Order",
                &["wo_no", "build_week"],
                &["mes", "erp"],
            ),
            concept(
                "work_center",
                "Work Center",
                &["site", "capacity_hours"],
                &["mes", "aps"],
            ),
            concept(
                "quality_issue",
                "Quality Issue",
                &["issue_code", "containment_state"],
                &["qms", "mes"],
            ),
            concept(
                "customer_order",
                "Customer Order",
                &["order_no", "commit_date"],
                &["erp", "crm"],
            ),
            concept(
                "shipment",
                "Shipment",
                &["shipment_no", "carrier"],
                &["tms", "erp"],
            ),
        ],
        relations: vec![
            ontology_relation("requires", "product", "component", "many_to_many"),
            ontology_relation("has_bom", "product", "bom", "one_to_many"),
            ontology_relation("supplied_by", "component", "supplier", "many_to_many"),
            ontology_relation("covered_by", "component", "inventory_lot", "one_to_many"),
            ontology_relation("planned_by", "product", "work_order", "one_to_many"),
            ontology_relation("processed_at", "work_order", "work_center", "many_to_one"),
            ontology_relation(
                "reserved_for",
                "component",
                "customer_order",
                "many_to_many",
            ),
            ontology_relation("blocked_by", "shipment", "quality_issue", "many_to_many"),
        ],
        metric_bindings: vec![
            metric_binding(
                "material_shortage_risk",
                "component",
                "component_week",
                "risk_subject",
            ),
            metric_binding(
                "supplier_commit_variance",
                "supplier",
                "supplier_component_week",
                "risk_driver",
            ),
            metric_binding(
                "inventory_coverage_weeks",
                "component",
                "component_site_week",
                "buffer",
            ),
            metric_binding(
                "work_center_load",
                "work_center",
                "work_center_week",
                "capacity_constraint",
            ),
            metric_binding(
                "order_delivery_risk",
                "customer_order",
                "order_week",
                "business_impact",
            ),
            metric_binding(
                "first_pass_yield",
                "work_center",
                "work_center_product_week",
                "quality_signal",
            ),
            metric_binding(
                "quality_escape_risk",
                "quality_issue",
                "issue_product_week",
                "risk_driver",
            ),
            metric_binding(
                "revenue_at_risk",
                "customer_order",
                "order_week",
                "financial_impact",
            ),
        ],
    }
}

#[must_use]
pub fn match_candidate(left: &IaccEntity, right: &IaccEntity) -> Option<IaccEntityMatchCandidate> {
    if left.entity_id == right.entity_id || left.entity_type != right.entity_type {
        return None;
    }
    let mut confidence: f32 = 0.0;
    let mut reason_codes = Vec::new();
    if left.canonical_key == right.canonical_key {
        confidence += 0.55;
        reason_codes.push("same_canonical_key".to_string());
    }
    if !left
        .source_keys
        .iter()
        .filter(|left_key| {
            right.source_keys.iter().any(|right_key| {
                left_key.normalized_system() == right_key.normalized_system()
                    && left_key.normalized_key() == right_key.normalized_key()
            })
        })
        .collect::<Vec<_>>()
        .is_empty()
    {
        confidence += 0.35;
        reason_codes.push("same_source_key".to_string());
    }
    if comparable_text(&left.display_name) == comparable_text(&right.display_name) {
        confidence += 0.5;
        reason_codes.push("same_display_name".to_string());
    }
    if confidence < 0.5 {
        return None;
    }
    Some(IaccEntityMatchCandidate {
        candidate_id: format!("entity-match-{}", uuid::Uuid::new_v4()),
        left_entity_id: left.entity_id.clone(),
        right_entity_id: right.entity_id.clone(),
        match_type: "possible_duplicate".to_string(),
        confidence: confidence.min(1.0),
        reason_codes,
        status: "open".to_string(),
        created_at: Utc::now(),
    })
}

fn comparable_text(value: &str) -> String {
    value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect()
}

fn concept(
    entity_type: &str,
    name: &str,
    required_attributes: &[&str],
    source_systems: &[&str],
) -> IaccOntologyConcept {
    IaccOntologyConcept {
        concept_id: entity_type.to_string(),
        name: name.to_string(),
        domain: "server_manufacturing".to_string(),
        entity_type: entity_type.to_string(),
        required_attributes: required_attributes
            .iter()
            .map(|value| value.to_string())
            .collect(),
        source_systems: source_systems
            .iter()
            .map(|value| value.to_string())
            .collect(),
        version: "v0.9.107".to_string(),
    }
}

fn ontology_relation(
    relation_type: &str,
    from_concept_id: &str,
    to_concept_id: &str,
    cardinality: &str,
) -> IaccOntologyRelation {
    IaccOntologyRelation {
        relation_type: relation_type.to_string(),
        from_concept_id: from_concept_id.to_string(),
        to_concept_id: to_concept_id.to_string(),
        cardinality: cardinality.to_string(),
        version: "v0.9.107".to_string(),
    }
}

fn metric_binding(
    metric_id: &str,
    concept_id: &str,
    grain: &str,
    semantic_role: &str,
) -> IaccOntologyMetricBinding {
    IaccOntologyMetricBinding {
        metric_id: metric_id.to_string(),
        concept_id: concept_id.to_string(),
        grain: grain.to_string(),
        semantic_role: semantic_role.to_string(),
        version: "v0.9.107".to_string(),
    }
}
