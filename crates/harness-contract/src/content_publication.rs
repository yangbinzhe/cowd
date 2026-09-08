//! Explicit sources for immutable authored content. Bodies never travel in tool JSON.
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactPublishInput {
    File {
        path: String,
        sha256: String,
        media_type: String,
    },
    MessageBlock {
        message_id: String,
        block_index: usize,
        sha256: String,
        media_type: String,
    },
    Artifact {
        content_ref: String,
        sha256: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArtifactMaterializeInput {
    pub content_ref: String,
    pub sha256: String,
    /// New destination, or an existing file with the exact same content.
    pub path: String,
}
