//! Immutable Agent display identity compilation.
//!
//! Display labels are compiled from the frozen Binding and Team role snapshot
//! and never participate in behavior decisions. The digest changes with the
//! label; the role's typed behavior facets do not.

use harness_contract::agent::AgentBindingSnapshot;
use harness_contract::team::{AgentDisplayIdentity, TeamRoleBindingSnapshot};
use sha2::{Digest, Sha256};

#[must_use]
pub fn compile_agent_display_identity(
    binding: &AgentBindingSnapshot,
    role: &TeamRoleBindingSnapshot,
    agent_id: &str,
    role_id: &str,
    role_display_name: Option<&str>,
    agent_name: &str,
    agent_description: &str,
) -> AgentDisplayIdentity {
    let label = agent_name.to_string();
    let role_label = role.role_name.clone();
    let focus_label = role.focus.clone();
    let locale = "auto".to_string();
    let provenance = format!("runtime.agent-binding:{}", binding.binding_digest);
    let digest = format!(
        "{:x}",
        Sha256::digest(
            format!(
                "{label}|{role_label}|{}|{locale}|{agent_description}",
                focus_label.as_deref().unwrap_or_default()
            )
            .as_bytes()
        )
    );
    AgentDisplayIdentity {
        agent_id: agent_id.to_string(),
        role_id: role_id.to_string(),
        role_display_name: role_display_name.map(str::to_owned),
        label,
        role_label,
        focus_label,
        locale,
        provenance,
        digest,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use harness_contract::team::{
        RoleBehaviorFacet, RoleCardinalityPolicy, RolePartitionPolicy, TeamRoleBindingSnapshot,
    };

    fn role(role_name: &str) -> TeamRoleBindingSnapshot {
        TeamRoleBindingSnapshot {
            role_id: "implementer".to_string(),
            slot: 1,
            focus: Some("focus-1".to_string()),
            role_name: role_name.to_string(),
            role_description: role_name.to_string(),
            behavior: vec![RoleBehaviorFacet::TerminalCandidate { required: true }],
            agent_definition_ref: "builtin/cowd/execute".to_string(),
            agent_name: "Execute".to_string(),
            agent_description: "Executes".to_string(),
            agent_definition_digest: "digest".to_string(),
            responsibility: role_name.to_string(),
            cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
            partition: RolePartitionPolicy::Single,
            task_contract_ref: "task/implementer@1".to_string(),
            acceptance: vec!["evidence".to_string()],
            team_markdown_fragment: None,
        }
    }

    fn binding() -> AgentBindingSnapshot {
        AgentBindingSnapshot {
            binding_id: "binding:execute".to_string(),
            definition_ref: harness_contract::agent::AgentDefinitionRevisionRef::new(
                harness_contract::agent::AgentDefinitionId::new(
                    harness_contract::agent::DefinitionScope::Builtin,
                    "cowd/execute",
                )
                .unwrap(),
                1,
            )
            .unwrap(),
            definition_digest: "digest".to_string(),
            instructions: "Execute.".to_string(),
            instance: harness_contract::agent::AgentInstanceRef {
                instance_id: "instance:1".to_string(),
                role_slot_id: Some("implementer:1".to_string()),
            },
            executor: harness_contract::agent::AgentExecutorPolicy::CowdNative,
            model_policy: harness_contract::agent::AgentModelPolicy {
                profile: "coding".to_string(),
                allowed_models: vec!["test".to_string()],
                fallback_allowed: false,
            },
            effective_capabilities: vec![harness_contract::agent::AgentCapability::Read],
            skill_refs: Vec::new(),
            tool_contract_refs: Vec::new(),
            data_lease: harness_contract::agent::AgentDataLease {
                session_id: "session-1".to_string(),
                task_id: "task-1".to_string(),
                team_id: Some("team-1".to_string()),
                read_scopes: Vec::new(),
                write_mode: harness_contract::agent::CognitiveWriteMode::CandidateOnly,
                team_working_state_visible: false,
                fact_boundaries: Vec::new(),
                fact_refs: Vec::new(),
                matrix_snapshot_refs: Vec::new(),
            },
            release: None,
            evaluation: None,
            display: None,
            binding_digest: "binding-digest".to_string(),
        }
    }

    #[test]
    fn display_label_changes_digest_but_never_behavior() {
        let behavior = role("Implementer").behavior;
        let english = compile_agent_display_identity(
            &binding(),
            &role("Implementer"),
            "agent:implementer:1",
            "implementer",
            Some("执行智能体"),
            "Execute",
            "Executes",
        );
        let chinese = compile_agent_display_identity(
            &binding(),
            &role("实现者"),
            "agent:implementer:1",
            "implementer",
            Some("实现智能体"),
            "执行者",
            "执行有界变更",
        );
        assert_eq!(english.agent_id, "agent:implementer:1");
        assert_eq!(english.role_id, "implementer");
        assert_eq!(english.role_display_name.as_deref(), Some("执行智能体"));
        assert_eq!(english.role_label, "Implementer");
        assert_eq!(chinese.role_label, "实现者");
        assert_ne!(english.digest, chinese.digest);
        assert_eq!(
            role("实现者").behavior,
            behavior,
            "display text must not change typed behavior"
        );
        assert_eq!(english.provenance, chinese.provenance);
    }
}
