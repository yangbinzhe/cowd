use harness_contract::knowledge::{
    KnowledgeActivationPolicy, KnowledgeGovernanceLevel, KnowledgeNamespace, KnowledgeTurnReport,
};
#[cfg(test)]
use memory::RecallReport;
use memory::{
    DocumentContent, KnowledgeFabric, MemoryContextPacket, MemoryPacketRole, OmittedMemory,
};

use crate::context_runtime::{
    ContextAuthority, ContextItem, ContextRole, ContextSourceKind, ContextVisibility,
};
use crate::knowledge_compliance::{KnowledgeComplianceDecision, KnowledgeComplianceRuntime};

#[derive(Debug, Clone)]
pub struct RuntimeKnowledgeActivation {
    pub items: Vec<ContextItem>,
    pub debug_fragment: String,
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
    pub fn with_fabric(fabric: KnowledgeFabric) -> Self {
        Self { fabric }
    }

    pub fn for_config_home(config_home: impl AsRef<std::path::Path>) -> Result<Self, String> {
        memory::durable_knowledge_fabric_for_config_home(config_home)
            .map(Self::with_fabric)
            .map_err(|error| error.to_string())
    }

    #[must_use]
    pub fn activate_from_packet_for_project(
        &self,
        session_id: &str,
        intent: &str,
        profile: &str,
        project_id: Option<&str>,
        packet: &MemoryContextPacket,
    ) -> Option<RuntimeKnowledgeActivation> {
        let packet = filter_packet_for_turn_intent(packet, intent);
        for item in &packet.selected {
            if is_runtime_memory_noise_item(item) {
                continue;
            }
            let Some(document) = document_from_memory_item(item) else {
                continue;
            };
            let governance = governance_from_memory_item(item);
            let policy = activation_policy_from_memory_item(item);
            let namespace = namespace_from_memory_item(item, project_id);
            self.fabric
                .ingest_document(namespace, policy, governance, document);
        }

        let (plan, canon_packs, warnings) = self
            .fabric
            .activate(session_id, intent, profile, project_id);
        if plan.active_pack_ids.is_empty() {
            return None;
        }

        let mut items = Vec::new();
        let mut fragment = String::from("### Context: Knowledge\n");
        for canon in canon_packs {
            let required_or_blocking = canon.rules.iter().any(|rule| {
                matches!(
                    rule.governance_level,
                    KnowledgeGovernanceLevel::Required | KnowledgeGovernanceLevel::Blocking
                )
            });
            let relevant = canon_relevant_to_intent(&canon.summary, intent)
                || canon
                    .rules
                    .iter()
                    .any(|rule| canon_relevant_to_intent(&rule.summary, intent));
            let pointer_only = !required_or_blocking && !relevant;
            let content = if pointer_only {
                format!(
                    "Knowledge pointer only; not injected as full body because relevance is low for this turn.\ncanon: {}\nsummary: withheld_until_explicit_recall\nevidence_refs: {}",
                    canon.canon_id,
                    canon
                        .evidence_refs
                        .iter()
                        .map(|reference| format!("{}/{}", reference.ref_type, reference.id))
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            } else {
                format!(
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
                )
            };
            let mut item = ContextItem::new(
                canon.canon_id.clone(),
                ContextSourceKind::Knowledge,
                if pointer_only {
                    ContextRole::Orientation
                } else {
                    ContextRole::Evidence
                },
                content.clone(),
            );
            item.authority = ContextAuthority::Project;
            item.visibility = ContextVisibility::Shared;
            item.score = if pointer_only { 0.42 } else { 0.88 };
            item.evidence = canon
                .evidence_refs
                .iter()
                .map(|reference| format!("knowledge://{}/{}", reference.ref_type, reference.id))
                .collect();
            fragment.push_str(&format!(
                "- canon: {}\n  mode: {}\n  tokens: {}\n  summary: {}\n",
                canon.canon_id,
                if pointer_only { "pointer" } else { "body" },
                canon.token_estimate,
                content.replace('\n', "\n  ")
            ));
            items.push(item);
        }
        let compliance_decision = KnowledgeComplianceRuntime::new().decide(warnings);
        if !compliance_decision.warnings.is_empty() {
            fragment.push_str("## Compliance\n");
            for warning in &compliance_decision.warnings {
                fragment.push_str(&format!(
                    "- warning: {:?} pack={} {}\n",
                    warning.level, warning.pack_id, warning.summary
                ));
            }
            if !compliance_decision.allows_execution() {
                fragment.push_str("- hard_gate: block\n");
                for reason in &compliance_decision.hard_gate_reasons {
                    fragment.push_str(&format!("  reason: {reason}\n"));
                }
            }
        }

        let report = self
            .fabric
            .turn_report(&plan, compliance_decision.warnings.clone());
        Some(RuntimeKnowledgeActivation {
            items,
            debug_fragment: fragment,
            report,
            compliance_decision,
        })
    }

    #[cfg(test)]
    fn activate_from_packet(
        &self,
        session_id: &str,
        intent: &str,
        profile: &str,
        packet: &MemoryContextPacket,
    ) -> Option<RuntimeKnowledgeActivation> {
        self.activate_from_packet_for_project(
            session_id,
            intent,
            profile,
            Some("test-workspace"),
            packet,
        )
    }
}

fn canon_relevant_to_intent(text: &str, intent: &str) -> bool {
    let haystack = normalize_turn_text(text);
    normalize_turn_text(intent)
        .split_whitespace()
        .filter(|term| term.len() >= 3 && !is_generic_knowledge_relevance_term(term))
        .any(|term| haystack.contains(term))
}

fn is_generic_knowledge_relevance_term(term: &str) -> bool {
    matches!(
        term,
        "runtime"
            | "analysis"
            | "analyze"
            | "review"
            | "background"
            | "knowledge"
            | "evidence"
            | "context"
            | "system"
            | "task"
    )
}

#[must_use]
pub fn filter_packet_for_turn_intent(
    packet: &MemoryContextPacket,
    intent: &str,
) -> MemoryContextPacket {
    let mut selected = Vec::with_capacity(packet.selected.len());
    let mut omitted = packet.omitted.clone();

    for item in &packet.selected {
        if let Some(reason) = suppression_reason_for_turn_intent(item, intent) {
            omitted.push(OmittedMemory {
                id: item.atom.id,
                title: item.atom.title.clone(),
                reason,
            });
        } else {
            selected.push(item.clone());
        }
    }

    let mut recall_report = packet.recall_report.clone();
    recall_report
        .selected
        .retain(|candidate| selected.iter().any(|item| item.atom.id == candidate.id));
    recall_report
        .omitted
        .extend(omitted.iter().map(|item| memory::RecallOmission {
            id: item.id,
            title: item.title.clone(),
            source: harness_contract::reality::RecallSourceKind::Memory,
            reason: item.reason.clone(),
        }));

    MemoryContextPacket {
        selected,
        omitted,
        token_estimate: packet.token_estimate,
        truncated: packet.truncated,
        recall_report,
    }
}

fn suppression_reason_for_turn_intent(
    item: &memory::MemoryPacketItem,
    intent: &str,
) -> Option<String> {
    let item_text = normalize_turn_text(&format!("{} {}", item.atom.title, item.reason));
    let intent_text = normalize_turn_text(intent);

    if turn_requests_tools_or_orchestration(&intent_text)
        && memory_discourages_tools_or_orchestration(&item_text)
    {
        return Some(
            "suppressed_for_current_turn: explicit user request requires tools or runtime orchestration"
                .to_string(),
        );
    }

    if turn_forbids_tools_or_orchestration(&intent_text)
        && memory_requires_tools_or_orchestration(&item_text)
    {
        return Some(
            "suppressed_for_current_turn: explicit user request forbids tools or runtime orchestration"
                .to_string(),
        );
    }

    if let Some(reason) = code_evidence_quantity_conflict(&intent_text, &item_text) {
        return Some(reason);
    }

    if turn_caps_code_evidence_to_two(&intent_text)
        && memory_requires_many_code_evidence_paths(&item_text)
    {
        return Some(
            "suppressed_for_current_turn: explicit user request caps code evidence to two key points"
                .to_string(),
        );
    }

    if turn_demands_immediate_completion(&intent_text) && memory_pushes_work_to_later(&item_text) {
        return Some(
            "suppressed_for_current_turn: explicit user request forbids deferring the work"
                .to_string(),
        );
    }

    None
}

fn code_evidence_quantity_conflict(intent: &str, memory: &str) -> Option<String> {
    if !mentions_code_evidence(intent) || !mentions_code_evidence(memory) {
        return None;
    }
    let current_max = extract_quantity_bound(
        intent,
        &["最多", "不超过", "以内", "at most", "no more than"],
    )?;
    let memory_min = extract_quantity_bound(memory, &["至少", "不少于", "minimum", "at least"])?;
    if memory_min > current_max {
        return Some(format!(
            "suppressed_for_current_turn: current instruction caps code evidence to {current_max}, recalled memory requires at least {memory_min}"
        ));
    }
    None
}

fn mentions_code_evidence(text: &str) -> bool {
    contains_any(
        text,
        &[
            "代码点",
            "代码路径",
            "关键代码",
            "代码",
            "路径",
            "引用",
            "证据",
            "code point",
            "code path",
            "code references",
            "code",
            "path",
            "reference",
            "evidence",
        ],
    )
}

fn extract_quantity_bound(text: &str, markers: &[&str]) -> Option<usize> {
    markers
        .iter()
        .filter_map(|marker| text.find(marker).map(|index| &text[index + marker.len()..]))
        .filter_map(extract_leading_quantity)
        .next()
}

fn extract_leading_quantity(text: &str) -> Option<usize> {
    let trimmed = text
        .trim_start_matches(|ch: char| ch.is_whitespace() || matches!(ch, ':' | '：' | ',' | '，'));
    let digits = trimmed
        .chars()
        .take_while(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if !digits.is_empty() {
        return digits.parse::<usize>().ok();
    }
    for (word, value) in [
        ("一", 1),
        ("二", 2),
        ("两", 2),
        ("三", 3),
        ("四", 4),
        ("五", 5),
        ("六", 6),
        ("七", 7),
        ("八", 8),
        ("九", 9),
        ("十", 10),
        ("one", 1),
        ("two", 2),
        ("three", 3),
        ("four", 4),
        ("five", 5),
        ("six", 6),
        ("seven", 7),
        ("eight", 8),
        ("nine", 9),
        ("ten", 10),
    ] {
        if trimmed.starts_with(word) {
            return Some(value);
        }
    }
    None
}

fn normalize_turn_text(text: &str) -> String {
    text.to_ascii_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn turn_requests_tools_or_orchestration(intent: &str) -> bool {
    contains_any(
        intent,
        &[
            "runtime_capabilities",
            "runtime_orchestrate",
            "调用工具",
            "使用工具",
            "工具调用",
            "真实调用",
            "批量工具",
            "工具批量",
            "编排",
            "多agent",
            "多 agent",
            "subagent",
            "team",
            "orchestration",
            "use tools",
            "call tools",
        ],
    )
}

fn memory_discourages_tools_or_orchestration(text: &str) -> bool {
    contains_any(
        text,
        &[
            "不要使用工具",
            "不要调用工具",
            "不使用工具",
            "不用工具",
            "避免工具",
            "禁止工具",
            "不要使用工具或编排",
            "不要编排",
            "不编排",
            "避免编排",
            "no tools",
            "without tools",
            "do not use tools",
            "don't use tools",
            "avoid tools",
            "no orchestration",
            "without orchestration",
        ],
    )
}

fn turn_forbids_tools_or_orchestration(intent: &str) -> bool {
    contains_any(
        intent,
        &[
            "不要使用工具",
            "不要调用工具",
            "不使用工具",
            "不用工具",
            "不要编排",
            "不编排",
            "纯文字回答",
            "只回答正文",
            "no tools",
            "without tools",
            "do not use tools",
            "don't use tools",
            "avoid tools",
            "no orchestration",
            "without orchestration",
        ],
    )
}

fn memory_requires_tools_or_orchestration(text: &str) -> bool {
    contains_any(
        text,
        &[
            "必须使用工具",
            "必须调用工具",
            "必须工具",
            "必须编排",
            "必须使用团队",
            "必须多agent",
            "必须多 agent",
            "必须 subagent",
            "must use tools",
            "required tools",
            "must orchestrate",
            "must use team",
            "must use subagent",
        ],
    )
}

fn turn_caps_code_evidence_to_two(intent: &str) -> bool {
    let caps_to_two = contains_any(
        intent,
        &[
            "最多两个",
            "最多 2",
            "不超过两个",
            "不超过 2",
            "两个以内",
            "two key",
            "at most two",
            "no more than two",
        ],
    );
    caps_to_two
        && contains_any(
            intent,
            &[
                "代码点",
                "代码路径",
                "关键代码",
                "code point",
                "code path",
                "code references",
            ],
        )
}

fn memory_requires_many_code_evidence_paths(text: &str) -> bool {
    let requires_many = contains_any(
        text,
        &[
            "至少4",
            "至少 4",
            "至少四",
            "四个",
            "4个",
            "4 个",
            "at least 4",
            "at least four",
            "minimum 4",
        ],
    );
    requires_many
        && contains_any(
            text,
            &[
                "代码",
                "路径",
                "引用",
                "code",
                "path",
                "reference",
                "evidence",
            ],
        )
}

fn turn_demands_immediate_completion(intent: &str) -> bool {
    contains_any(
        intent,
        &[
            "不要往后推",
            "不要再往后推",
            "不要推迟",
            "不要延后",
            "一次性",
            "全部解决",
            "全部做完",
            "彻底完成",
            "do not defer",
            "finish now",
        ],
    )
}

fn memory_pushes_work_to_later(text: &str) -> bool {
    contains_any(
        text,
        &[
            "后续阶段",
            "以后再",
            "后续再",
            "暂不处理",
            "先不处理",
            "下阶段",
            "下一阶段",
            "defer",
            "later phase",
            "future phase",
        ],
    )
}

fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| haystack.contains(needle))
}

fn document_from_memory_item(item: &memory::MemoryPacketItem) -> Option<DocumentContent> {
    if is_runtime_memory_noise_item(item) {
        return None;
    }
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
            "{}\ncontent: {}\nreason: {}\nevidence: {}",
            atom.title,
            item.content_preview,
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

fn is_runtime_memory_noise_item(item: &memory::MemoryPacketItem) -> bool {
    let text = normalize_turn_text(&format!(
        "{} {} {}",
        item.atom.title, item.content_preview, item.reason
    ));
    contains_any(
        &text,
        &[
            "user preference:",
            "session critical context checkpoint",
            "session pending work checkpoint",
            "session preferences checkpoint",
            "session tool evidence checkpoint",
            "frequent tool usage:",
            "usage_feedback:selected_count",
            "active memory lacks explicit orientation evidence",
        ],
    ) || contains_any(
        &text,
        &[
            "用户偏好:",
            "用户偏好：",
            "会话 checkpoint",
            "会话检查点",
            "工具使用频率",
        ],
    )
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

fn namespace_from_memory_item(
    item: &memory::MemoryPacketItem,
    project_id: Option<&str>,
) -> KnowledgeNamespace {
    if matches!(item.atom.layer, memory::MemoryLayer::L0) {
        KnowledgeNamespace::SharedLibrary("global".to_string())
    } else {
        KnowledgeNamespace::Project(project_id.unwrap_or("unscoped-workspace").to_string())
    }
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
            content_preview: format!("{title} full policy body"),
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
            recall_report: RecallReport::default(),
        };

        let activation = KnowledgeActivationRuntime::new()
            .activate_from_packet(
                "s1",
                "runtime architecture analysis",
                "DeepInvestigation",
                &packet,
            )
            .expect("knowledge should activate");

        assert!(activation
            .items
            .iter()
            .all(|item| item.source == ContextSourceKind::Knowledge));
        assert!(activation.debug_fragment.contains("### Context: Knowledge"));
        assert!(activation
            .items
            .iter()
            .any(|item| item.content.contains("full policy body")));
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
            recall_report: RecallReport::default(),
        };

        assert!(KnowledgeActivationRuntime::new()
            .activate_from_packet("s1", "architecture evidence review", "MainTurn", &packet)
            .is_none());
    }

    #[test]
    fn runtime_knowledge_activation_ignores_user_preference_noise() {
        let packet = MemoryContextPacket {
            selected: vec![packet_item(
                "User preference: 不要无限展开读取上下文",
                MemoryLayer::L3,
                MemoryPacketRole::Warning,
            )],
            omitted: Vec::new(),
            token_estimate: 96,
            truncated: false,
            recall_report: RecallReport::default(),
        };

        assert!(KnowledgeActivationRuntime::new()
            .activate_from_packet("s1", "分析记忆架构", "DeepInvestigation", &packet)
            .is_none());
    }

    #[test]
    fn runtime_knowledge_activation_keeps_low_relevance_global_knowledge_as_pointer() {
        let mut item = packet_item(
            "Global payroll procedure background",
            MemoryLayer::L0,
            MemoryPacketRole::Supporting,
        );
        item.reason = "background reference".to_string();
        item.content_preview =
            "payroll reimbursement calendar and office expense process".to_string();
        let packet = MemoryContextPacket {
            selected: vec![item],
            omitted: Vec::new(),
            token_estimate: 128,
            truncated: false,
            recall_report: RecallReport::default(),
        };

        let activation = KnowledgeActivationRuntime::new()
            .activate_from_packet(
                "s1",
                "architecture evidence review",
                "DeepInvestigation",
                &packet,
            )
            .expect("default global knowledge should remain visible as pointer");

        assert!(activation.debug_fragment.contains("mode: pointer"));
        assert!(activation.items.iter().any(|item| {
            item.role == ContextRole::Orientation
                && item.content.contains("Knowledge pointer only")
                && !item
                    .content
                    .contains("payroll reimbursement calendar and office expense process")
        }));
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
            recall_report: RecallReport::default(),
        };

        let activation = KnowledgeActivationRuntime::new()
            .activate_from_packet("s1", "safety policy approval", "DeepInvestigation", &packet)
            .expect("blocking knowledge should activate");

        assert!(activation.debug_fragment.contains("hard_gate: block"));
        assert!(!activation.compliance_decision.allows_execution());
    }

    #[test]
    fn runtime_memory_filter_suppresses_tool_conflict_for_current_turn() {
        let packet = MemoryContextPacket {
            selected: vec![
                packet_item(
                    "User preference: 不要使用工具或编排",
                    MemoryLayer::L3,
                    MemoryPacketRole::Warning,
                ),
                packet_item(
                    "Architecture policy must retain evidence",
                    MemoryLayer::L3,
                    MemoryPacketRole::Supporting,
                ),
            ],
            omitted: Vec::new(),
            token_estimate: 256,
            truncated: false,
            recall_report: RecallReport::default(),
        };

        let filtered =
            filter_packet_for_turn_intent(&packet, "请先使用 runtime_capabilities 调用工具分析");

        assert_eq!(filtered.selected.len(), 1);
        assert_eq!(
            filtered.selected[0].atom.title,
            "Architecture policy must retain evidence"
        );
        assert_eq!(filtered.omitted.len(), 1);
        assert!(filtered.omitted[0]
            .reason
            .contains("explicit user request requires tools"));
    }

    #[test]
    fn runtime_memory_filter_suppresses_required_tool_rule_when_current_turn_forbids_tools() {
        let packet = MemoryContextPacket {
            selected: vec![packet_item(
                "Workflow policy must use tools and must orchestrate",
                MemoryLayer::L3,
                MemoryPacketRole::Warning,
            )],
            omitted: Vec::new(),
            token_estimate: 128,
            truncated: false,
            recall_report: RecallReport::default(),
        };

        let filtered = filter_packet_for_turn_intent(&packet, "不要使用工具，纯文字回答");

        assert!(filtered.selected.is_empty());
        assert!(filtered.omitted[0]
            .reason
            .contains("explicit user request forbids tools"));
    }

    #[test]
    fn runtime_knowledge_activation_suppresses_conflicting_path_count_rule() {
        let packet = MemoryContextPacket {
            selected: vec![packet_item(
                "Review rule must cite 至少 4 个代码路径",
                MemoryLayer::L3,
                MemoryPacketRole::Warning,
            )],
            omitted: Vec::new(),
            token_estimate: 128,
            truncated: false,
            recall_report: RecallReport::default(),
        };

        assert!(KnowledgeActivationRuntime::new()
            .activate_from_packet(
                "s1",
                "请最多两个关键代码点说明问题",
                "DeepInvestigation",
                &packet,
            )
            .is_none());
    }

    #[test]
    fn runtime_memory_filter_suppresses_generalized_code_evidence_quantity_conflict() {
        let packet = MemoryContextPacket {
            selected: vec![packet_item(
                "Review rule: at least five code references are required",
                MemoryLayer::L3,
                MemoryPacketRole::Warning,
            )],
            omitted: Vec::new(),
            token_estimate: 128,
            truncated: false,
            recall_report: RecallReport::default(),
        };

        let filtered = filter_packet_for_turn_intent(&packet, "Use at most three code references.");

        assert!(filtered.selected.is_empty());
        assert!(filtered.omitted[0]
            .reason
            .contains("current instruction caps code evidence to 3"));
        assert!(filtered.omitted[0].reason.contains("requires at least 5"));
    }

    #[test]
    fn runtime_memory_filter_suppresses_defer_rule_when_user_demands_completion() {
        let packet = MemoryContextPacket {
            selected: vec![packet_item(
                "Planning preference: 后续阶段再处理",
                MemoryLayer::L3,
                MemoryPacketRole::Supporting,
            )],
            omitted: Vec::new(),
            token_estimate: 128,
            truncated: false,
            recall_report: RecallReport::default(),
        };

        let filtered = filter_packet_for_turn_intent(&packet, "不要往后推，一次性全部解决");

        assert!(filtered.selected.is_empty());
        assert!(filtered.omitted[0]
            .reason
            .contains("forbids deferring the work"));
    }
}
