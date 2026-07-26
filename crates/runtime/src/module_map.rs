use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeDomain {
    Conversation,
    Provider,
    Tooling,
    Mission,
    Session,
    Agent,
    Team,
    Steward,
    Approval,
    Context,
    Recovery,
    Policy,
    ExecutionCore,
    RealityBridge,
    Evolution,
    Configuration,
    Infrastructure,
    Skill,
}

impl RuntimeDomain {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Conversation => "conversation",
            Self::Provider => "provider",
            Self::Tooling => "tooling",
            Self::Mission => "mission",
            Self::Session => "session",
            Self::Agent => "agent",
            Self::Team => "team",
            Self::Steward => "steward",
            Self::Approval => "approval",
            Self::Context => "context",
            Self::Recovery => "recovery",
            Self::Policy => "policy",
            Self::ExecutionCore => "execution_core",
            Self::RealityBridge => "reality_bridge",
            Self::Evolution => "evolution",
            Self::Configuration => "configuration",
            Self::Infrastructure => "infrastructure",
            Self::Skill => "skill",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeModuleDescriptor {
    pub module: &'static str,
    pub domain: RuntimeDomain,
    pub owner: &'static str,
    pub public_surface: bool,
    pub lifecycle_owner: bool,
}

impl RuntimeModuleDescriptor {
    const fn public(
        module: &'static str,
        domain: RuntimeDomain,
        owner: &'static str,
        lifecycle_owner: bool,
    ) -> Self {
        Self {
            module,
            domain,
            owner,
            public_surface: true,
            lifecycle_owner,
        }
    }
}

#[must_use]
pub fn runtime_module_map() -> Vec<RuntimeModuleDescriptor> {
    use RuntimeDomain::{
        Agent, Approval, Configuration, Context, Conversation, Evolution, ExecutionCore,
        Infrastructure, Mission, Policy, Provider, RealityBridge, Recovery, Session, Skill,
        Steward, Team, Tooling,
    };
    vec![
        RuntimeModuleDescriptor::public("conversation", Conversation, "runtime", true),
        RuntimeModuleDescriptor::public("turn_inbox", Conversation, "runtime", false),
        RuntimeModuleDescriptor::public("host", Conversation, "runtime", true),
        RuntimeModuleDescriptor::public("cowd_event", Infrastructure, "runtime", false),
        RuntimeModuleDescriptor::public("runtime_control", Infrastructure, "runtime", true),
        RuntimeModuleDescriptor::public("runtime_harness", Infrastructure, "runtime", true),
        RuntimeModuleDescriptor::public("execution_core", ExecutionCore, "runtime", true),
        RuntimeModuleDescriptor::public("execution_live", ExecutionCore, "runtime", true),
        RuntimeModuleDescriptor::public("execution_projection", ExecutionCore, "runtime", false),
        RuntimeModuleDescriptor::public("orchestration", ExecutionCore, "runtime", true),
        RuntimeModuleDescriptor::public("provider_runtime_client", Provider, "runtime", true),
        RuntimeModuleDescriptor::public("provider_transport_pool", Provider, "runtime", true),
        RuntimeModuleDescriptor::public("provider_transport_policy", Provider, "runtime", false),
        RuntimeModuleDescriptor::public("provider_registry", Provider, "runtime", false),
        RuntimeModuleDescriptor::public("tool_dispatch", Tooling, "runtime", true),
        RuntimeModuleDescriptor::public("governed_tool_plan", Tooling, "runtime", false),
        RuntimeModuleDescriptor::public("tool_host", Tooling, "runtime", true),
        RuntimeModuleDescriptor::public("tool_invocation", Tooling, "runtime", false),
        RuntimeModuleDescriptor::public("tool_memory", Tooling, "runtime", false),
        RuntimeModuleDescriptor::public("tool_orchestrator", Tooling, "runtime", true),
        RuntimeModuleDescriptor::public("tool_policy", Tooling, "runtime", false),
        RuntimeModuleDescriptor::public("tool_execution_plane", Tooling, "runtime", true),
        RuntimeModuleDescriptor::public("mission_control", Mission, "runtime", true),
        RuntimeModuleDescriptor::public("mission_command_router", Mission, "runtime", true),
        RuntimeModuleDescriptor::public("mission_evidence", Mission, "runtime", false),
        RuntimeModuleDescriptor::public("mission_runtime", Mission, "runtime", true),
        RuntimeModuleDescriptor::public("mission_runtime_port", Mission, "runtime", true),
        RuntimeModuleDescriptor::public("mission_schedule", Mission, "runtime", true),
        RuntimeModuleDescriptor::public("task", Mission, "runtime", false),
        RuntimeModuleDescriptor::public("task_packet", Mission, "runtime", false),
        RuntimeModuleDescriptor::public("session_execution", Session, "runtime", true),
        RuntimeModuleDescriptor::public("session_input", Session, "runtime", true),
        RuntimeModuleDescriptor::public("input_classifier", Session, "runtime", false),
        RuntimeModuleDescriptor::public("session_lifecycle", Session, "runtime", true),
        RuntimeModuleDescriptor::public("mission_command_interpreter", Session, "runtime", true),
        RuntimeModuleDescriptor::public("session_relation_graph", Session, "runtime", false),
        RuntimeModuleDescriptor::public("agent", Agent, "runtime", true),
        RuntimeModuleDescriptor::public("agent_capability", Agent, "runtime", true),
        RuntimeModuleDescriptor::public("agent_catalog", Agent, "runtime", true),
        RuntimeModuleDescriptor::public("agent_evaluation", Agent, "runtime", false),
        RuntimeModuleDescriptor::public("collaboration_template", Agent, "runtime", false),
        RuntimeModuleDescriptor::public("definition_registry", Agent, "runtime", true),
        RuntimeModuleDescriptor::public("agent_in_process_worker", Agent, "runtime", false),
        RuntimeModuleDescriptor::public("managed_agent", Agent, "runtime", true),
        RuntimeModuleDescriptor::public("agent_model_selector", Agent, "runtime", false),
        RuntimeModuleDescriptor::public("agent_process_jsonl_adapter", Agent, "runtime", true),
        RuntimeModuleDescriptor::public("agent_result_validator", Agent, "runtime", false),
        RuntimeModuleDescriptor::public("agent_runtime", Agent, "runtime", true),
        RuntimeModuleDescriptor::public("agent_run_handle", Agent, "runtime", false),
        RuntimeModuleDescriptor::public("pairing", Agent, "runtime", false),
        RuntimeModuleDescriptor::public("team_definition", Team, "runtime", true),
        RuntimeModuleDescriptor::public("team_instantiation", Team, "runtime", true),
        RuntimeModuleDescriptor::public("team_projection", Team, "runtime", true),
        RuntimeModuleDescriptor::public("team_agent_selector", Team, "runtime", false),
        RuntimeModuleDescriptor::public("team_agent_task", Team, "runtime", false),
        RuntimeModuleDescriptor::public("team_l4_promotion", Team, "runtime", true),
        RuntimeModuleDescriptor::public("team_legacy_import", Team, "runtime", false),
        RuntimeModuleDescriptor::public("team_profile_migration", Team, "runtime", false),
        RuntimeModuleDescriptor::public("team_result_reducer", Team, "runtime", false),
        RuntimeModuleDescriptor::public("team_runtime", Team, "runtime", true),
        RuntimeModuleDescriptor::public("team_working_state", Team, "runtime", true),
        RuntimeModuleDescriptor::public("conflict_arbiter", Mission, "runtime", true),
        RuntimeModuleDescriptor::public("steward_agent", Steward, "runtime", false),
        RuntimeModuleDescriptor::public("approval_gate", Approval, "runtime", true),
        RuntimeModuleDescriptor::public("approval_queue", Approval, "runtime", true),
        RuntimeModuleDescriptor::public("context_fanout", Context, "runtime", false),
        RuntimeModuleDescriptor::public("artifact", Context, "runtime", false),
        RuntimeModuleDescriptor::public("context_evidence", Context, "runtime", false),
        RuntimeModuleDescriptor::public("context_ledger", Context, "runtime", false),
        RuntimeModuleDescriptor::public("budget_policy", Context, "runtime", true),
        RuntimeModuleDescriptor::public("context_profiler", Context, "runtime", false),
        RuntimeModuleDescriptor::public("context_runtime", Context, "runtime", true),
        RuntimeModuleDescriptor::public("context_tool_exposure", Context, "runtime", false),
        RuntimeModuleDescriptor::public("evidence_planner", Context, "runtime", false),
        RuntimeModuleDescriptor::public("intent_planner", Context, "harness-contract", false),
        RuntimeModuleDescriptor::public("knowledge_activation", Context, "runtime", true),
        RuntimeModuleDescriptor::public("knowledge_compliance", Context, "runtime", false),
        RuntimeModuleDescriptor::public("resources", Context, "runtime", true),
        RuntimeModuleDescriptor::public("skill", Skill, "runtime", true),
        RuntimeModuleDescriptor::public("structured_data", RealityBridge, "runtime", false),
        RuntimeModuleDescriptor::public("fact_extraction", RealityBridge, "runtime", true),
        RuntimeModuleDescriptor::public("reality_decision", RealityBridge, "runtime", true),
        RuntimeModuleDescriptor::public("reality_recall_port", RealityBridge, "runtime", true),
        RuntimeModuleDescriptor::public("evolution", Evolution, "runtime", true),
        RuntimeModuleDescriptor::public("recovery", Recovery, "runtime", true),
        RuntimeModuleDescriptor::public("recovery_recipes", Recovery, "runtime", false),
        RuntimeModuleDescriptor::public("runtime_event_replay", Recovery, "runtime", false),
        RuntimeModuleDescriptor::public("runtime_event_store", Recovery, "runtime", true),
        RuntimeModuleDescriptor::public("cross_plane_policy", Policy, "harness-contract", false),
        RuntimeModuleDescriptor::public("gates", Policy, "runtime", false),
        RuntimeModuleDescriptor::public("permission_enforcer", Policy, "runtime", false),
        RuntimeModuleDescriptor::public("permissions", Policy, "runtime", false),
        RuntimeModuleDescriptor::public("policy_engine", Policy, "runtime", true),
        RuntimeModuleDescriptor::public("security", Policy, "runtime", true),
        RuntimeModuleDescriptor::public("trust_resolver", Policy, "runtime", false),
        RuntimeModuleDescriptor::public("autonomy_profile", Policy, "runtime", false),
        RuntimeModuleDescriptor::public("config", Configuration, "runtime", false),
        RuntimeModuleDescriptor::public("config_validate", Configuration, "runtime", false),
        RuntimeModuleDescriptor::public("profile", Configuration, "runtime", false),
        RuntimeModuleDescriptor::public("capability", Infrastructure, "runtime", false),
        RuntimeModuleDescriptor::public("capability_manifest", Infrastructure, "runtime", false),
        RuntimeModuleDescriptor::public("checkpoint", Infrastructure, "runtime", false),
        RuntimeModuleDescriptor::public("lane_completion", Infrastructure, "runtime", false),
        RuntimeModuleDescriptor::public("eval_gate", Infrastructure, "harness-eval", false),
        RuntimeModuleDescriptor::public("lifecycle_hooks", Infrastructure, "runtime", false),
        RuntimeModuleDescriptor::public("mcp_lifecycle_hardened", Infrastructure, "mcp", false),
        RuntimeModuleDescriptor::public("mcp_server", Infrastructure, "mcp", false),
        RuntimeModuleDescriptor::public("mcp_tool_bridge", Infrastructure, "mcp", false),
        RuntimeModuleDescriptor::public("plugin_lifecycle", Infrastructure, "plugins", false),
        RuntimeModuleDescriptor::public("quality_gate", Infrastructure, "runtime", false),
        RuntimeModuleDescriptor::public("release_gate", Infrastructure, "runtime", false),
        RuntimeModuleDescriptor::public("sandbox", Infrastructure, "runtime", false),
        RuntimeModuleDescriptor::public("source_self_audit", Infrastructure, "runtime", false),
        RuntimeModuleDescriptor::public("surface_contract", Infrastructure, "surface", false),
        RuntimeModuleDescriptor::public("upgrade", Infrastructure, "runtime", true),
    ]
}

#[must_use]
pub fn runtime_module_names_by_domain(domain: RuntimeDomain) -> Vec<&'static str> {
    runtime_module_map()
        .into_iter()
        .filter(|descriptor| descriptor.domain == domain)
        .map(|descriptor| descriptor.module)
        .collect()
}
