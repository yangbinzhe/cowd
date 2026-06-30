use serde::{Deserialize, Serialize};

use crate::types::MemoryId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CompactNavigationPointer {
    pub pointer_id: String,
    pub entry_id: MemoryId,
    pub summary: String,
    pub confidence: f32,
}
