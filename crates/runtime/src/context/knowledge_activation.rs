use harness_contract::knowledge::{
    KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace, KnowledgeTurnReport,
};
use memory::{DocumentContent, KnowledgeFabric, MemoryContextPacket, MemoryPacketRole};

use crate::context_runtime::{
    ContextAuthority, ContextItem, ContextRole, ContextSourceKind, ContextVisibility,
};
use crate::knowledge_compliance::{KnowledgeComplianceDecision, KnowledgeComplianceRuntime};

#[derive(Debug, Clone)]
pub struct RuntimeKnowledgeActivation {
    pub items: Vec<ContextItem>,
    pub prompt_fragment: String,
    pub report: KnowledgeTurnReport,
    pub compliance_decision: KnowledgeComplianceDecision,
}

#[derive(Debug, Clone, Default)]
pub struct KnowledgeActivationRuntime {
    fabric: KnowledgeFabric,
}

impl KnowledgeActivationRuntime {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn activate_from_packet(
        &self,
        session_id: &str,
        intent: &str,
        profile: &str,
        packet: &MemoryContextPacket,
    ) -> Option<RuntimeKnowledgeActivation> {
        for item in &packet.selected {
            let Some(document) = document_from_memory_item(item) else {
                continue;
            };
            let governance = governance_from_memory_item(item);
            let policy = activation_policy_from_memory_item(item);
            let namespace = namespace_from_memory_item(item);
            self.fabric
                .ingest_document(namespace, policy, governance, document);
        }

        let (plan, canon_packs, warnings) = self.fabric.activate(
            session_id,
            intent,
            profile,
            project_id_from_cwd().as_deref(),
        );
        if plan.active_pack_ids.is_empty() {
            return None;
        }

        let mut items = Vec::new();
        let mut fragment = String::from("<knowledge_context>\n");
        for canon in canon_packs {
            let content = format!(
                "{}\nrules:\n{}\nprocedures:\n{}\nevidence_refs: {}",
                canon.summary,
                canon
                    .rules
                    .iter()
                    .map(|rule| format!("- [{:?}] {}", rule.governance_level, rule.summary))
                    .collect::<Vec<_>>()
                    .join("\n"),
                canon.procedures.join("\n"),
                canon
                    .evidence_refs
                    .iter()
                    .map(|reference| format!("{}/{}", reference.ref_type, reference.id))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let mut item = ContextItem::new(
                canon.canon_id.clone(),
                ContextSourceKind::Knowledge,
                ContextRole::Evidence,
                content.clone(),
            );
            item.authority = ContextAuthority::Project;
            item.visibility = ContextVisibility::Shared;
            item.score = 0.88;
            item.evidence = canon
                .evidence_refs
                .iter()
                .map(|reference| format!("knowledge://{}/{}", reference.ref_type, reference.id))
                .collect();
            fragment.push_str(&format!(
                "  <knowledge canon=\"{}\" tokens=\"{}\">\n{}\n  </knowledge>\n",
                canon.canon_id, canon.token_estimate, content
            ));
            items.push(item);
        }
        let compliance_decision = KnowledgeComplianceRuntime::new().decide(warnings);
        if !compliance_decision.warnings.is_empty() {
            fragment.push_str("  <knowledge_compliance>\n");
            for warning in &compliance_decision.warnings {
                fragment.push_str(&format!(
                    "    <warning level=\"{:?}\" pack=\"{}\">{}</warning>\n",
                    warning.level, warning.pack_id, warning.summary
                ));
            }
            if !compliance_decision.allows_execution() {
                fragment.push_str("    <hard_gate action=\"block\">\n");
                for reason in &compliance_decision.hard_gate_reasons {
                    fragment.push_str(&format!("      <reason>{reason}</reason>\n"));
                }
                fragment.push_str("    </hard_gate>\n");
            }
            fragment.push_str("  </knowledge_compliance>\n");
        }
        fragment.push_str("</knowledge_context>");

        let report = self
            .fabric
            .turn_report(&plan, compliance_decision.warnings.clone());
        Some(RuntimeKnowledgeActivation {
            items,
            prompt_fragment: fragment,
            report,
            compliance_decision,
        })
    }
}

fn document_from_memory_item(item: &memory::MemoryPacketItem) -> Option<DocumentContent> {
    let atom = &item.atom;
    let category_is_knowledge = matches!(
        atom.layer,
        memory::MemoryLayer::L2 | memory::MemoryLayer::L3 | memory::MemoryLayer::L4
    );
    let role_is_knowledge = matches!(
        item.role,
        MemoryPacketRole::Supporting | MemoryPacketRole::Warning | MemoryPacketRole::Conflict
    );
    if !category_is_knowledge && !role_is_knowledge {
        return None;
    }
    Some(DocumentContent {
        title: atom.title.clone(),
        body: format!(
            "{}\nreason: {}\nevidence: {}",
            atom.title,
            item.reason,
            atom.evidence_pointer.as_deref().unwrap_or("memory packet")
        ),
        source: atom.evidence_pointer.clone(),
        author: Some("memory.kernel".to_string()),
        created_at: None,
        modified_at: None,
        language: None,
    })
}

fn governance_from_memory_item(item: &memory::MemoryPacketItem) -> KnowledgeGovernanceLevel {
    let text = format!("{} {}", item.atom.title, item.reason).to_ascii_lowercase();
    if matches!(item.role, MemoryPacketRole::Conflict) {
        KnowledgeGovernanceLevel::Blocking
    } else if text.contains("must")
        || text.contains("required")
        || text.contains("禁止")
        || text.contains("必须")
        || text.contains("不得")
    {
        KnowledgeGovernanceLevel::Required
    } else {
        KnowledgeGovernanceLevel::Advisory
    }
}

fn activation_policy_from_memory_item(
    item: &memory::MemoryPacketItem,
) -> KnowledgeActivationPolicy {
    if matches!(
        item.atom.layer,
        memory::MemoryLayer::L0 | memory::MemoryLayer::L2
    ) {
        KnowledgeActivationPolicy::DefaultForDomain
    } else {
        KnowledgeActivationPolicy::OnDemand
    }
}

fn namespace_from_memory_item(item: &memory::MemoryPacketItem) -> KnowledgeNamespace {
    if matches!(item.atom.layer, memory::MemoryLayer::L0) {
        KnowledgeNamespace::SharedLibrary("global".to_string())
    } else {
        KnowledgeNamespace::Project(
            std::env::current_dir()
                .ok()
                .and_then(|path| {
                    path.file_name()
                        .map(|name| name.to_string_lossy().to_string())
                })
                .unwrap_or_else(|| "workspace".to_string()),
        )
    }
}

fn project_id_from_cwd() -> Option<String> {
    std::env::current_dir().ok().and_then(|path| {
        path.file_name()
            .map(|name| name.to_string_lossy().to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use memory::{
        MemoryAtomView, MemoryContextPacket, MemoryInformationState, MemoryLayer, MemoryPacketItem,
        MemoryPacketRole, MemoryState,
    };

    fn packet_item(title: &str, layer: MemoryLayer, role: MemoryPacketRole) -> MemoryPacketItem {
        MemoryPacketItem {
            atom: MemoryAtomView {
                id: uuid::Uuid::new_v4(),
                layer,
                information_state: MemoryInformationState::Orientation,
                state: MemoryState::Active,
                evidence_pointer: Some(format!("memory:{title}")),
                explicit_authority: layer == MemoryLayer::L0,
                confidence: 0.91,
                salience: 4.0,
                title: title.to_string(),
            },
            role,
            reason: "must be applied to this task".to_string(),
        }
    }

    #[test]
    fn runtime_knowledge_activation_emits_context_items_and_report() {
        let packet = MemoryContextPacket {
            selected: vec![packet_item(
                "Architecture policy must retain evidence",
                MemoryLayer::L3,
                MemoryPacketRole::Supporting,
            )],
            omitted: Vec::new(),
            token_estimate: 128,
            truncated: false,
        };

        let activation = KnowledgeActivationRuntime::new()
            .activate_from_packet(
                "s1",
                "architecture evidence review",
                "DeepInvestigation",
                &packet,
            )
            .expect("knowledge should activate");

        assert!(activation
            .items
            .iter()
            .all(|item| item.source == ContextSourceKind::Knowledge));
        assert!(activation.prompt_fragment.contains("<knowledge_context>"));
        assert!(!activation.report.active_pack_ids.is_empty());
        assert!(!activation.report.evidence_refs.is_empty());
    }

    #[test]
    fn runtime_knowledge_activation_ignores_low_level_working_memory_noise() {
        let packet = MemoryContextPacket {
            selected: vec![packet_item(
                "Temporary terminal preference",
                MemoryLayer::L1,
                MemoryPacketRole::Orientation,
            )],
            omitted: Vec::new(),
            token_estimate: 64,
            truncated: false,
        };

        assert!(KnowledgeActivationRuntime::new()
            .activate_from_packet("s1", "architecture evidence review", "MainTurn", &packet)
            .is_none());
    }

    #[test]
    fn runtime_knowledge_activation_marks_blocking_rules_as_hard_gate() {
        let packet = MemoryContextPacket {
            selected: vec![packet_item(
                "Safety policy must stop without approval",
                MemoryLayer::L3,
                MemoryPacketRole::Conflict,
            )],
            omitted: Vec::new(),
            token_estimate: 128,
            truncated: false,
        };

        let activation = KnowledgeActivationRuntime::new()
            .activate_from_packet("s1", "safety policy approval", "DeepInvestigation", &packet)
            .expect("blocking knowledge should activate");

        assert!(activation
            .prompt_fragment
            .contains("<hard_gate action=\"block\">"));
        assert!(!activation.compliance_decision.allows_execution());
    }
}
