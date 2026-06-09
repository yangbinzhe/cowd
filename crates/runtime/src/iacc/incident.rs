use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IaccIncident {
    pub incident_id: String,
    pub title: String,
    pub attention_id: Option<String>,
    pub evidence_packet_id: Option<String>,
    pub task_id: Option<String>,
    pub agent_graph_id: Option<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl IaccIncident {
    #[must_use]
    pub fn new(title: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            incident_id: format!("incident-{}", uuid::Uuid::new_v4()),
            title: title.into(),
            attention_id: None,
            evidence_packet_id: None,
            task_id: None,
            agent_graph_id: None,
            status: "open".to_string(),
            created_at: now,
            updated_at: now,
        }
    }
}
