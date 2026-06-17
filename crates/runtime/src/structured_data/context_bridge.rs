use crate::{ContextAuthority, ContextItem, ContextRole, ContextSourceKind, ContextVisibility};

use super::CowdStructuredEvidence;

impl CowdStructuredEvidence {
    #[must_use]
    pub fn to_context_item(&self) -> ContextItem {
        let summary = self.memory_summary();
        let mut item = ContextItem::new(
            summary.reference,
            ContextSourceKind::Task,
            ContextRole::Evidence,
            summary.summary,
        );
        item.authority = ContextAuthority::Derived;
        item.visibility = ContextVisibility::Shared;
        item.score = self.confidence;
        item.evidence = self
            .source_refs
            .iter()
            .map(|source| source.reference.clone())
            .collect();
        item
    }
}
