use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccCockpitProfileInput {
    #[serde(default)]
    pub profile_id: Option<String>,
    pub owner_ref: String,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub focus_refs: Vec<String>,
    #[serde(default)]
    pub focus_metric_ids: Vec<String>,
    #[serde(default)]
    pub thresholds: Value,
    #[serde(default)]
    pub template_id: Option<String>,
    #[serde(default)]
    pub cadence: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccCockpitProfile {
    pub profile_id: String,
    pub owner_ref: String,
    pub display_name: String,
    #[serde(default)]
    pub focus_refs: Vec<String>,
    #[serde(default)]
    pub focus_metric_ids: Vec<String>,
    #[serde(default)]
    pub thresholds: Value,
    pub template_id: String,
    pub cadence: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccCockpitWidget {
    pub widget_id: String,
    pub widget_type: String,
    pub title: String,
    pub status: String,
    pub priority_score: f32,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub source_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IaccCockpitProjection {
    pub projection_id: String,
    pub profile: IaccCockpitProfile,
    #[serde(default)]
    pub widgets: Vec<IaccCockpitWidget>,
    pub summary: String,
    pub generated_at: DateTime<Utc>,
}

impl IaccCockpitProfile {
    #[must_use]
    pub fn from_input(input: IaccCockpitProfileInput) -> Self {
        let now = Utc::now();
        let profile_id = input
            .profile_id
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("cockpit-profile-{}", uuid::Uuid::new_v4()));
        let display_name = input
            .display_name
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| input.owner_ref.clone());
        Self {
            profile_id,
            owner_ref: input.owner_ref,
            display_name,
            focus_refs: input.focus_refs,
            focus_metric_ids: input.focus_metric_ids,
            thresholds: input.thresholds,
            template_id: input
                .template_id
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "iacc.default_ops".to_string()),
            cadence: input
                .cadence
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "daily".to_string()),
            created_at: now,
            updated_at: now,
        }
    }
}

impl IaccCockpitWidget {
    #[must_use]
    pub fn new(
        widget_type: impl Into<String>,
        title: impl Into<String>,
        status: impl Into<String>,
        priority_score: f32,
        data: Value,
        source_refs: Vec<String>,
    ) -> Self {
        Self {
            widget_id: format!("cockpit-widget-{}", uuid::Uuid::new_v4()),
            widget_type: widget_type.into(),
            title: title.into(),
            status: status.into(),
            priority_score,
            data,
            source_refs,
        }
    }
}
