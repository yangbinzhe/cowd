use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatrixSeverity {
    Normal,
    Warning,
    Critical,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MatrixAttentionItem {
    pub attention_id: String,
    pub title: String,
    pub business_domain: String,
    #[serde(default)]
    pub entity_ref: Option<String>,
    /// Canonical Matrix metric identifiers that caused this attention item.
    #[serde(default)]
    pub metric_refs: Vec<String>,
    #[serde(default)]
    pub period: Option<String>,
    pub priority_score: f32,
    pub severity: MatrixSeverity,
    pub urgency: f32,
    pub strategic_weight: f32,
    pub confidence: f32,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    #[serde(default)]
    pub linked_changes: Vec<String>,
    #[serde(default)]
    pub linked_anomalies: Vec<String>,
    #[serde(default)]
    pub linked_impacts: Vec<String>,
    #[serde(default)]
    pub owner_roles: Vec<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MatrixAttentionItem {
    #[must_use]
    pub fn from_fact(
        fact_id: &str,
        fact_type: &str,
        entity_ref: Option<String>,
        confidence: f32,
    ) -> Self {
        let now = Utc::now();
        Self {
            attention_id: format!("attention-{}", uuid::Uuid::new_v4()),
            title: format!("New operational fact requires metric evaluation: {fact_type}"),
            business_domain: domain_from_fact_type(fact_type).to_string(),
            entity_ref,
            metric_refs: Vec::new(),
            period: None,
            priority_score: 0.35,
            severity: MatrixSeverity::Unknown,
            urgency: 0.2,
            strategic_weight: 0.2,
            confidence,
            reason_codes: vec!["fact_ingested".to_string(), "metric_pending".to_string()],
            linked_changes: vec![format!("matrix:fact:{fact_id}")],
            linked_anomalies: Vec::new(),
            linked_impacts: Vec::new(),
            owner_roles: vec!["operations_analyst".to_string()],
            status: "open".to_string(),
            created_at: now,
            updated_at: now,
        }
    }
}

fn domain_from_fact_type(fact_type: &str) -> &str {
    fact_type.split('.').next().unwrap_or("operations")
}
