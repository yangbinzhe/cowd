use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use matrix::{
    MatrixEntity, MatrixEntityInput, MatrixFact, MatrixFactInput, MatrixMetricDefinition,
    MatrixMetricDependency, MatrixMetricDependencyInput, MatrixRelation, MatrixRelationInput,
    MatrixSourceKey,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgDomainScenario {
    pub scenario_id: String,
    pub title: String,
    pub problem_statement: String,
    #[serde(default)]
    pub primary_entities: Vec<String>,
    #[serde(default)]
    pub expected_metrics: Vec<String>,
    #[serde(default)]
    pub expected_relations: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgDomainPack {
    pub domain_id: String,
    pub name: String,
    pub industry: String,
    pub version: String,
    #[serde(default)]
    pub entity_types: Vec<String>,
    #[serde(default)]
    pub relation_types: Vec<String>,
    #[serde(default)]
    pub metric_ids: Vec<String>,
    #[serde(default)]
    pub scenarios: Vec<MfgDomainScenario>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgDomainSeedPlan {
    pub pack: MfgDomainPack,
    #[serde(default)]
    pub entities: Vec<MatrixEntity>,
    #[serde(default)]
    pub relations: Vec<MatrixRelation>,
    #[serde(default)]
    pub metric_definitions: Vec<MatrixMetricDefinition>,
    #[serde(default)]
    pub metric_dependencies: Vec<MatrixMetricDependency>,
    #[serde(default)]
    pub facts: Vec<MatrixFact>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MfgDomainSeedResult {
    pub domain_id: String,
    pub version: String,
    pub entity_count: usize,
    pub relation_count: usize,
    pub metric_definition_count: usize,
    pub metric_dependency_count: usize,
    pub fact_count: usize,
    pub scenario_count: usize,
    pub seeded_at: DateTime<Utc>,
}

#[must_use]
pub fn server_manufacturing_domain_pack() -> MfgDomainPack {
    let scenarios = server_manufacturing_scenarios();
    MfgDomainPack {
        domain_id: "server_manufacturing".to_string(),
        name: "Server Manufacturing Operations".to_string(),
        industry: "discrete_manufacturing.server".to_string(),
        version: "v0.9.85".to_string(),
        entity_types: vec![
            "enterprise",
            "site",
            "plant",
            "product",
            "sku",
            "configuration",
            "bom",
            "bom_line",
            "component",
            "component_family",
            "supplier",
            "purchase_order",
            "po_line",
            "inventory_lot",
            "work_order",
            "work_center",
            "operation",
            "quality_issue",
            "customer_order",
            "shipment",
            "person",
            "organization",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        relation_types: vec![
            "requires",
            "supplied_by",
            "substitutes",
            "stored_at",
            "planned_for",
            "produced_by",
            "processed_at",
            "reserved_for",
            "blocked_by",
            "affected_by",
            "depends_on",
            "owned_by",
            "shipped_to",
            "quality_checked_by",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        metric_ids: vec![
            "material_shortage_risk",
            "supplier_commit_variance",
            "work_center_load",
            "order_delivery_risk",
            "inventory_coverage_weeks",
            "first_pass_yield",
            "quality_escape_risk",
            "revenue_at_risk",
        ]
        .into_iter()
        .map(str::to_string)
        .collect(),
        scenarios,
    }
}

#[must_use]
pub fn server_manufacturing_seed_plan() -> MfgDomainSeedPlan {
    let pack = server_manufacturing_domain_pack();
    let entities = server_manufacturing_entities();
    let relations = server_manufacturing_relations();
    let metric_definitions = server_manufacturing_metric_definitions();
    let metric_dependencies = server_manufacturing_metric_dependencies();
    let facts = server_manufacturing_facts();
    MfgDomainSeedPlan {
        pack,
        entities,
        relations,
        metric_definitions,
        metric_dependencies,
        facts,
    }
}

fn server_manufacturing_scenarios() -> Vec<MfgDomainScenario> {
    vec![
        MfgDomainScenario {
            scenario_id: "server_mfg_gpu_shortage".to_string(),
            title: "GPU shortage threatens strategic AI server orders".to_string(),
            problem_statement:
                "H100 supply commitment is below the 2026-W30 AI server build requirement"
                    .to_string(),
            primary_entities: vec![
                "entity-component-gpu-h100".to_string(),
                "entity-product-ai-server-8gpu".to_string(),
                "entity-order-co-2026-0001".to_string(),
            ],
            expected_metrics: vec![
                "material_shortage_risk".to_string(),
                "supplier_commit_variance".to_string(),
                "order_delivery_risk".to_string(),
            ],
            expected_relations: vec![
                "requires".to_string(),
                "supplied_by".to_string(),
                "reserved_for".to_string(),
            ],
        },
        MfgDomainScenario {
            scenario_id: "server_mfg_bottleneck_load".to_string(),
            title: "Final assembly work center overload blocks weekly output".to_string(),
            problem_statement:
                "Assembly line capacity is overloaded for 2026-W30 AI server work orders"
                    .to_string(),
            primary_entities: vec![
                "entity-work-center-final-assembly".to_string(),
                "entity-work-order-wo-2026-w30-001".to_string(),
            ],
            expected_metrics: vec!["work_center_load".to_string()],
            expected_relations: vec!["processed_at".to_string(), "planned_for".to_string()],
        },
        MfgDomainScenario {
            scenario_id: "server_mfg_quality_escape".to_string(),
            title: "Memory DIMM quality issue affects storage server shipment".to_string(),
            problem_statement:
                "DIMM failure trend affects storage server final test and shipment readiness"
                    .to_string(),
            primary_entities: vec![
                "entity-component-dimm-64g".to_string(),
                "entity-quality-issue-dimm-fail".to_string(),
                "entity-product-storage-server".to_string(),
            ],
            expected_metrics: vec![
                "first_pass_yield".to_string(),
                "quality_escape_risk".to_string(),
            ],
            expected_relations: vec!["affected_by".to_string(), "quality_checked_by".to_string()],
        },
    ]
}

fn server_manufacturing_entities() -> Vec<MatrixEntity> {
    vec![
        entity(
            "entity-supplier-gpu-alpha",
            "supplier",
            "supplier-gpu-alpha",
            "GPU Supplier Alpha",
            json!({"tier": "strategic", "region": "global"}),
            vec![source_key("SRM", "SUP-GPU-ALPHA", "connector:srm:supplier")],
        ),
        entity(
            "entity-supplier-memory-beta",
            "supplier",
            "supplier-memory-beta",
            "Memory Supplier Beta",
            json!({"tier": "preferred", "region": "apac"}),
            vec![source_key("SRM", "SUP-MEM-BETA", "connector:srm:supplier")],
        ),
        entity(
            "entity-component-gpu-h100",
            "component",
            "gpu-h100",
            "GPU H100",
            json!({"family": "gpu", "lead_time_days": 84}),
            vec![
                source_key("ERP", "MAT-GPU-H100", "connector:erp:material"),
                source_key("PLM", "GPU_H100_80GB", "connector:plm:item"),
            ],
        ),
        entity(
            "entity-component-dimm-64g",
            "component",
            "dimm-ddr5-64g",
            "DDR5 64GB DIMM",
            json!({"family": "memory", "lead_time_days": 28}),
            vec![source_key("ERP", "MAT-DIMM-64G", "connector:erp:material")],
        ),
        entity(
            "entity-component-ssd-7t",
            "component",
            "ssd-nvme-7t",
            "NVMe SSD 7.68TB",
            json!({"family": "storage", "lead_time_days": 35}),
            vec![source_key("ERP", "MAT-SSD-7T", "connector:erp:material")],
        ),
        entity(
            "entity-product-ai-server-8gpu",
            "product",
            "ai-server-8gpu",
            "AI Server 8GPU",
            json!({"product_family": "ai_server", "priority": "strategic"}),
            vec![source_key("PLM", "PROD-AI-8GPU", "connector:plm:product")],
        ),
        entity(
            "entity-product-storage-server",
            "product",
            "storage-server-24bay",
            "Storage Server 24 Bay",
            json!({"product_family": "storage_server"}),
            vec![source_key(
                "PLM",
                "PROD-STORAGE-24BAY",
                "connector:plm:product",
            )],
        ),
        entity(
            "entity-sku-ai-server-8gpu-h100",
            "sku",
            "sku-ai-server-8gpu-h100",
            "AI Server 8GPU H100 SKU",
            json!({"configuration": "8xH100"}),
            vec![source_key("ERP", "SKU-AI-8GPU-H100", "connector:erp:sku")],
        ),
        entity(
            "entity-order-co-2026-0001",
            "customer_order",
            "co-2026-0001",
            "Customer Order CO-2026-0001",
            json!({"customer": "strategic_cloud_a", "priority": "critical", "qty": 16}),
            vec![source_key("ERP", "CO-2026-0001", "connector:erp:order")],
        ),
        entity(
            "entity-order-co-2026-0002",
            "customer_order",
            "co-2026-0002",
            "Customer Order CO-2026-0002",
            json!({"customer": "enterprise_b", "priority": "normal", "qty": 12}),
            vec![source_key("ERP", "CO-2026-0002", "connector:erp:order")],
        ),
        entity(
            "entity-work-center-final-assembly",
            "work_center",
            "final-assembly-line-1",
            "Final Assembly Line 1",
            json!({"site": "plant-a", "capacity_hours_per_week": 160}),
            vec![source_key(
                "MES",
                "WC-FINAL-ASM-1",
                "connector:mes:work-center",
            )],
        ),
        entity(
            "entity-work-center-burn-in",
            "work_center",
            "burn-in-room-1",
            "Burn-in Room 1",
            json!({"site": "plant-a", "capacity_hours_per_week": 120}),
            vec![source_key(
                "MES",
                "WC-BURN-IN-1",
                "connector:mes:work-center",
            )],
        ),
        entity(
            "entity-work-order-wo-2026-w30-001",
            "work_order",
            "wo-2026-w30-001",
            "Work Order WO-2026-W30-001",
            json!({"week": "2026-W30", "qty": 16, "status": "planned"}),
            vec![source_key(
                "MES",
                "WO-2026-W30-001",
                "connector:mes:work-order",
            )],
        ),
        entity(
            "entity-quality-issue-dimm-fail",
            "quality_issue",
            "qi-dimm-fail-2026-w30",
            "DIMM failure trend 2026-W30",
            json!({"defect": "memory_error", "severity": "warning"}),
            vec![source_key("QMS", "QI-DIMM-2026-W30", "connector:qms:issue")],
        ),
    ]
}

fn server_manufacturing_relations() -> Vec<MatrixRelation> {
    vec![
        relation(
            "requires",
            "entity-product-ai-server-8gpu",
            "entity-component-gpu-h100",
            json!({"qty_per": 8, "bom": "bom-ai-server-8gpu-v1"}),
        ),
        relation(
            "requires",
            "entity-product-ai-server-8gpu",
            "entity-component-dimm-64g",
            json!({"qty_per": 32, "bom": "bom-ai-server-8gpu-v1"}),
        ),
        relation(
            "requires",
            "entity-product-storage-server",
            "entity-component-dimm-64g",
            json!({"qty_per": 16, "bom": "bom-storage-server-v1"}),
        ),
        relation(
            "requires",
            "entity-product-storage-server",
            "entity-component-ssd-7t",
            json!({"qty_per": 24, "bom": "bom-storage-server-v1"}),
        ),
        relation(
            "supplied_by",
            "entity-component-gpu-h100",
            "entity-supplier-gpu-alpha",
            json!({"allocation": "primary"}),
        ),
        relation(
            "supplied_by",
            "entity-component-dimm-64g",
            "entity-supplier-memory-beta",
            json!({"allocation": "primary"}),
        ),
        relation(
            "reserved_for",
            "entity-order-co-2026-0001",
            "entity-product-ai-server-8gpu",
            json!({"week": "2026-W30", "qty": 16}),
        ),
        relation(
            "reserved_for",
            "entity-order-co-2026-0002",
            "entity-product-storage-server",
            json!({"week": "2026-W30", "qty": 12}),
        ),
        relation(
            "produced_by",
            "entity-work-order-wo-2026-w30-001",
            "entity-product-ai-server-8gpu",
            json!({"week": "2026-W30"}),
        ),
        relation(
            "processed_at",
            "entity-work-order-wo-2026-w30-001",
            "entity-work-center-final-assembly",
            json!({"operation": "assembly", "hours": 188}),
        ),
        relation(
            "processed_at",
            "entity-work-order-wo-2026-w30-001",
            "entity-work-center-burn-in",
            json!({"operation": "burn_in", "hours": 96}),
        ),
        relation(
            "affected_by",
            "entity-product-storage-server",
            "entity-quality-issue-dimm-fail",
            json!({"scope": "final_test"}),
        ),
        relation(
            "quality_checked_by",
            "entity-component-dimm-64g",
            "entity-quality-issue-dimm-fail",
            json!({"method": "failure_trend"}),
        ),
    ]
}

fn server_manufacturing_metric_definitions() -> Vec<MatrixMetricDefinition> {
    vec![
        metric(
            "material_shortage_risk",
            "Material shortage risk",
            "supply",
            "supply_risk_analyst",
            vec!["supply.material_shortage"],
            0.95,
        ),
        metric(
            "supplier_commit_variance",
            "Supplier commit variance",
            "supply",
            "supplier_manager",
            vec!["supply.commit_variance"],
            0.85,
        ),
        metric(
            "work_center_load",
            "Work center load",
            "manufacturing",
            "manufacturing_planner",
            vec!["manufacturing.work_center_load"],
            0.8,
        ),
        metric(
            "order_delivery_risk",
            "Order delivery risk",
            "fulfillment",
            "order_manager",
            vec!["fulfillment.order_delivery_risk"],
            0.92,
        ),
        metric(
            "first_pass_yield",
            "First pass yield",
            "quality",
            "quality_engineer",
            vec!["quality.first_pass_yield"],
            0.75,
        ),
        metric(
            "quality_escape_risk",
            "Quality escape risk",
            "quality",
            "quality_engineer",
            vec!["quality.escape_risk"],
            0.88,
        ),
    ]
}

fn server_manufacturing_metric_dependencies() -> Vec<MatrixMetricDependency> {
    vec![
        metric_dependency(
            "supplier_commit_variance",
            "material_shortage_risk",
            "supplier_commit_to_material_availability",
            Some("supplied_by"),
            vec!["supply.commit_variance", "supply.material_shortage"],
        ),
        metric_dependency(
            "material_shortage_risk",
            "order_delivery_risk",
            "material_availability_to_delivery",
            Some("requires,reserved_for"),
            vec![
                "supply.material_shortage",
                "fulfillment.order_delivery_risk",
            ],
        ),
        metric_dependency(
            "work_center_load",
            "order_delivery_risk",
            "capacity_to_delivery",
            Some("processed_at,produced_by,reserved_for"),
            vec![
                "manufacturing.work_center_load",
                "fulfillment.order_delivery_risk",
            ],
        ),
        metric_dependency(
            "first_pass_yield",
            "quality_escape_risk",
            "yield_to_quality_escape",
            Some("quality_checked_by"),
            vec!["quality.first_pass_yield", "quality.escape_risk"],
        ),
        metric_dependency(
            "quality_escape_risk",
            "order_delivery_risk",
            "quality_to_delivery",
            Some("affected_by,reserved_for"),
            vec!["quality.escape_risk", "fulfillment.order_delivery_risk"],
        ),
    ]
}

fn server_manufacturing_facts() -> Vec<MatrixFact> {
    vec![
        fact(
            "fact-smfg-shortage-gpu-w30",
            "snapshot-smfg-w30",
            "supply.material_shortage",
            vec!["component:gpu-h100", "mfg:entity:entity-component-gpu-h100"],
            "material_shortage_risk",
            json!({"week": "2026-W30", "entity_id": "entity-component-gpu-h100"}),
            json!({"short_qty": 128}),
            "connector:erp:material-shortage",
            0.94,
        ),
        fact(
            "fact-smfg-commit-gpu-alpha-w30",
            "snapshot-smfg-w30",
            "supply.commit_variance",
            vec![
                "supplier:supplier-gpu-alpha",
                "mfg:entity:entity-supplier-gpu-alpha",
            ],
            "supplier_commit_variance",
            json!({"week": "2026-W30", "entity_id": "entity-supplier-gpu-alpha"}),
            json!({"commit_gap_qty": 96}),
            "connector:srm:supplier-commit",
            0.9,
        ),
        fact(
            "fact-smfg-load-final-assembly-w30",
            "snapshot-smfg-w30",
            "manufacturing.work_center_load",
            vec![
                "work_center:final-assembly-line-1",
                "mfg:entity:entity-work-center-final-assembly",
            ],
            "work_center_load",
            json!({"week": "2026-W30", "entity_id": "entity-work-center-final-assembly"}),
            json!({"load_hours": 188, "capacity_hours": 160}),
            "connector:mes:capacity",
            0.88,
        ),
        fact(
            "fact-smfg-order-risk-co-0001-w30",
            "snapshot-smfg-w30",
            "fulfillment.order_delivery_risk",
            vec![
                "customer_order:co-2026-0001",
                "mfg:entity:entity-order-co-2026-0001",
            ],
            "order_delivery_risk",
            json!({"week": "2026-W30", "entity_id": "entity-order-co-2026-0001"}),
            json!({"orders_at_risk": 1, "revenue_at_risk": 4800000}),
            "connector:erp:customer-order",
            0.91,
        ),
        fact(
            "fact-smfg-quality-dimm-w30",
            "snapshot-smfg-w30",
            "quality.escape_risk",
            vec![
                "component:dimm-ddr5-64g",
                "mfg:entity:entity-component-dimm-64g",
            ],
            "quality_escape_risk",
            json!({"week": "2026-W30", "entity_id": "entity-component-dimm-64g"}),
            json!({"defect_ppm": 420}),
            "connector:qms:quality-trend",
            0.86,
        ),
    ]
}

fn entity(
    entity_id: &str,
    entity_type: &str,
    canonical_key: &str,
    display_name: &str,
    attributes: Value,
    source_keys: Vec<MatrixSourceKey>,
) -> MatrixEntity {
    MatrixEntity::from_input(MatrixEntityInput {
        entity_id: Some(entity_id.to_string()),
        entity_type: entity_type.to_string(),
        canonical_key: canonical_key.to_string(),
        display_name: Some(display_name.to_string()),
        source_keys,
        attributes,
        confidence: Some(0.95),
    })
}

fn relation(
    relation_type: &str,
    from_entity_id: &str,
    to_entity_id: &str,
    attributes: Value,
) -> MatrixRelation {
    MatrixRelation::from_input(MatrixRelationInput {
        relation_id: Some(format!(
            "relation-{relation_type}-{from_entity_id}-{to_entity_id}"
        )),
        relation_type: relation_type.to_string(),
        from_entity_id: from_entity_id.to_string(),
        to_entity_id: to_entity_id.to_string(),
        attributes,
        confidence: Some(0.94),
    })
}

fn metric(
    metric_id: &str,
    name: &str,
    domain: &str,
    owner_role: &str,
    inputs: Vec<&str>,
    business_priority: f32,
) -> MatrixMetricDefinition {
    let now = Utc::now();
    MatrixMetricDefinition {
        metric_id: metric_id.to_string(),
        name: name.to_string(),
        domain: domain.to_string(),
        grain: "entity_week".to_string(),
        owner_role: owner_role.to_string(),
        formula_ref: format!("mfg://domain/server_manufacturing/metrics/{metric_id}/v0.9.85"),
        inputs: inputs.into_iter().map(str::to_string).collect(),
        dimensions: vec!["entity_ref".to_string(), "week".to_string()],
        refresh_policy: "seeded_demo_recompute".to_string(),
        threshold_policy: json!({
            "warning": "domain_pack_default",
            "critical": "domain_pack_default"
        }),
        dependency_metric_ids: Vec::new(),
        business_priority,
        created_at: now,
        updated_at: now,
    }
}

fn fact(
    fact_id: &str,
    snapshot_id: &str,
    fact_type: &str,
    entity_refs: Vec<&str>,
    metric_key: &str,
    dimensions: Value,
    measures: Value,
    source_ref: &str,
    confidence: f32,
) -> MatrixFact {
    MatrixFact::from_input(MatrixFactInput {
        fact_id: Some(fact_id.to_string()),
        snapshot_id: Some(snapshot_id.to_string()),
        fact_type: fact_type.to_string(),
        entity_refs: entity_refs.into_iter().map(str::to_string).collect(),
        metric_key: Some(metric_key.to_string()),
        dimensions,
        measures,
        event_time: None,
        valid_from: None,
        valid_to: None,
        source_ref: Some(source_ref.to_string()),
        confidence: Some(confidence),
        raw_hash: None,
    })
}

fn metric_dependency(
    upstream_metric_id: &str,
    downstream_metric_id: &str,
    dependency_type: &str,
    entity_relation_type: Option<&str>,
    required_fact_types: Vec<&str>,
) -> MatrixMetricDependency {
    MatrixMetricDependency::from_input(MatrixMetricDependencyInput {
        dependency_id: Some(format!(
            "metric-dependency-{upstream_metric_id}-{downstream_metric_id}-{dependency_type}"
        )),
        upstream_metric_id: upstream_metric_id.to_string(),
        downstream_metric_id: downstream_metric_id.to_string(),
        dependency_type: dependency_type.to_string(),
        entity_relation_type: entity_relation_type.map(str::to_string),
        required_fact_types: required_fact_types
            .into_iter()
            .map(str::to_string)
            .collect(),
        transformation_ref: Some(format!(
            "mfg://domain/server_manufacturing/dependencies/{dependency_type}/v0.9.86"
        )),
        confidence: Some(0.82),
        notes: None,
    })
}

fn source_key(source_system: &str, source_key: &str, source_ref: &str) -> MatrixSourceKey {
    MatrixSourceKey {
        source_system: source_system.to_string(),
        source_key: source_key.to_string(),
        source_ref: Some(source_ref.to_string()),
    }
}
