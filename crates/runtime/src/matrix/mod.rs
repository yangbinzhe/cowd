//! Runtime compatibility exports for the Matrix core crate.

pub use ::matrix::*;

use crate::{ContextAuthority, ContextItem, ContextRole, ContextSourceKind, ContextVisibility};

#[must_use]
pub fn evidence_to_context_item(packet: &MatrixEvidencePacket) -> ContextItem {
    let mut item = ContextItem::new(
        format!("matrix:evidence:{}", packet.packet_id),
        ContextSourceKind::Task,
        ContextRole::Evidence,
        packet.context_summary(),
    );
    item.authority = ContextAuthority::Derived;
    item.visibility = ContextVisibility::Shared;
    item.score = packet.confidence;
    item.evidence = packet
        .source_refs
        .iter()
        .map(|source| source.reference.clone())
        .collect();
    item
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix_reference_uses_matrix_namespace() {
        assert_eq!(matrix_reference("fact", "f1"), "matrix:fact:f1");
    }

    #[test]
    fn evidence_context_projection_stays_runtime_owned() {
        let mut packet = MatrixEvidencePacket::new("runtime projection");
        packet.source_refs.push(MatrixEvidenceSourceRef {
            kind: "fact".to_string(),
            reference: "matrix:fact:f1".to_string(),
            summary: "fact f1".to_string(),
        });

        let item = evidence_to_context_item(&packet);

        assert_eq!(item.id, format!("matrix:evidence:{}", packet.packet_id));
        assert_eq!(item.evidence, vec!["matrix:fact:f1"]);
    }
}
