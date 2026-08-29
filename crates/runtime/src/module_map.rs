use serde::{Deserialize, Serialize};

use crate::module_authority::{CapabilityRoleBinding, LifecycleRole, WriterKind};
use RuntimeDomain::{
    Agent, Approval, Configuration, Context, Conversation, Evolution, ExecutionCore,
    Infrastructure, Mission, Policy, Provider, RealityBridge, Recovery, Session, Skill, Steward,
    Team, Tooling,
};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RuntimeModuleDescriptor {
    pub module: &'static str,
    pub domain: RuntimeDomain,
    pub owner: &'static str,
    pub public_surface: bool,
    pub role_bindings: Vec<CapabilityRoleBinding>,
}

impl RuntimeModuleDescriptor {
    fn public(
        module: &'static str,
        domain: RuntimeDomain,
        owner: &'static str,
        role_bindings: &[CapabilityRoleBinding],
    ) -> Self {
        Self {
            module,
            domain,
            owner,
            public_surface: true,
            role_bindings: role_bindings.to_vec(),
        }
    }
}

const fn authority(capability: &'static str, state: &'static str) -> CapabilityRoleBinding {
    CapabilityRoleBinding::local(
        capability,
        state,
        LifecycleRole::Authority,
        WriterKind::Canonical,
    )
}

const fn coordinator(capability: &'static str, state: &'static str) -> CapabilityRoleBinding {
    CapabilityRoleBinding::local(
        capability,
        state,
        LifecycleRole::Coordinator,
        WriterKind::Coordinating,
    )
}

const fn worker(capability: &'static str, state: &'static str) -> CapabilityRoleBinding {
    CapabilityRoleBinding::local(capability, state, LifecycleRole::Worker, WriterKind::Effect)
}

const fn projector(capability: &'static str, state: &'static str) -> CapabilityRoleBinding {
    CapabilityRoleBinding::local(
        capability,
        state,
        LifecycleRole::Projector,
        WriterKind::Projection,
    )
}

const fn adapter(capability: &'static str, state: &'static str) -> CapabilityRoleBinding {
    CapabilityRoleBinding::local(
        capability,
        state,
        LifecycleRole::Adapter,
        WriterKind::ReadOnly,
    )
}

const fn external(capability: &'static str, state: &'static str) -> CapabilityRoleBinding {
    CapabilityRoleBinding::external(capability, state, LifecycleRole::Adapter)
}

#[must_use]
pub fn runtime_module_map() -> Vec<RuntimeModuleDescriptor> {
    let mut modules = conversation_execution_modules();
    modules.extend(provider_tool_modules());
    modules.extend(mission_session_modules());
    modules.extend(agent_team_modules());
    modules.extend(approval_context_modules());
    modules.extend(reality_recovery_policy_modules());
    modules.extend(configuration_infrastructure_modules());
    modules
}

fn conversation_execution_modules() -> Vec<RuntimeModuleDescriptor> {
    vec![
        RuntimeModuleDescriptor::public(
            "conversation",
            Conversation,
            "runtime",
            &[authority("conversation.turn", "runtime.conversation.turn")],
        ),
        RuntimeModuleDescriptor::public(
            "turn_inbox",
            Conversation,
            "runtime",
            &[worker("conversation.inbox", "runtime.conversation.turn")],
        ),
        RuntimeModuleDescriptor::public(
            "host",
            Conversation,
            "runtime",
            &[coordinator(
                "conversation.host",
                "runtime.conversation.turn",
            )],
        ),
        RuntimeModuleDescriptor::public(
            "cowd_event",
            Infrastructure,
            "runtime",
            &[projector("event.projection", "runtime.event.store")],
        ),
        RuntimeModuleDescriptor::public(
            "runtime_control",
            Infrastructure,
            "runtime",
            &[authority("runtime.control", "runtime.control")],
        ),
        RuntimeModuleDescriptor::public(
            "runtime_harness",
            Infrastructure,
            "runtime",
            &[adapter("runtime.harness", "runtime.control")],
        ),
        RuntimeModuleDescriptor::public(
            "execution_core",
            ExecutionCore,
            "runtime",
            &[authority("execution.graph", "runtime.execution.graph")],
        ),
        RuntimeModuleDescriptor::public(
            "execution_supervisor",
            ExecutionCore,
            "runtime",
            &[coordinator(
                "execution.supervision",
                "runtime.execution.graph",
            )],
        ),
        RuntimeModuleDescriptor::public(
            "execution_live",
            ExecutionCore,
            "runtime",
            &[projector("execution.live", "runtime.execution.graph")],
        ),
        RuntimeModuleDescriptor::public(
            "execution_projection",
            ExecutionCore,
            "runtime",
            &[projector("execution.projection", "runtime.execution.graph")],
        ),
        RuntimeModuleDescriptor::public(
            "orchestration",
            ExecutionCore,
            "runtime",
            &[authority(
                "collaboration.program",
                "runtime.collaboration.program",
            )],
        ),
    ]
}

fn provider_tool_modules() -> Vec<RuntimeModuleDescriptor> {
    vec![
        RuntimeModuleDescriptor::public(
            "provider_runtime_client",
            Provider,
            "runtime",
            &[worker("provider.request", "runtime.provider.transport")],
        ),
        RuntimeModuleDescriptor::public(
            "provider_transport_pool",
            Provider,
            "runtime",
            &[authority(
                "provider.transport",
                "runtime.provider.transport",
            )],
        ),
        RuntimeModuleDescriptor::public(
            "provider_transport_policy",
            Provider,
            "runtime",
            &[authority("provider.policy", "runtime.provider.policy")],
        ),
        RuntimeModuleDescriptor::public(
            "provider_registry",
            Provider,
            "runtime",
            &[authority("provider.catalog", "runtime.provider.catalog")],
        ),
        RuntimeModuleDescriptor::public(
            "tool_dispatch",
            Tooling,
            "runtime",
            &[worker("tool.dispatch", "runtime.tool.effect")],
        ),
        RuntimeModuleDescriptor::public(
            "governed_tool_plan",
            Tooling,
            "runtime",
            &[projector("tool.plan.projection", "runtime.tool.plan")],
        ),
        RuntimeModuleDescriptor::public(
            "tool_host",
            Tooling,
            "runtime",
            &[authority("tool.effect", "runtime.tool.effect")],
        ),
        RuntimeModuleDescriptor::public(
            "tool_invocation",
            Tooling,
            "runtime",
            &[projector("tool.invocation", "runtime.tool.effect")],
        ),
        RuntimeModuleDescriptor::public(
            "tool_memory",
            Tooling,
            "runtime",
            &[projector("tool.memory", "runtime.tool.effect")],
        ),
        RuntimeModuleDescriptor::public(
            "tool_orchestrator",
            Tooling,
            "runtime",
            &[authority("tool.plan", "runtime.tool.plan")],
        ),
        RuntimeModuleDescriptor::public(
            "tool_policy",
            Tooling,
            "runtime",
            &[authority("tool.policy", "runtime.tool.policy")],
        ),
        RuntimeModuleDescriptor::public(
            "tool_execution_plane",
            Tooling,
            "runtime",
            &[coordinator("tool.execution", "runtime.tool.effect")],
        ),
    ]
}

fn mission_session_modules() -> Vec<RuntimeModuleDescriptor> {
    vec![
        RuntimeModuleDescriptor::public(
            "mission_control",
            Mission,
            "runtime",
            &[authority("mission.lifecycle", "runtime.mission.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "mission_command_router",
            Mission,
            "runtime",
            &[adapter("mission.command", "runtime.mission.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "mission_evidence",
            Mission,
            "runtime",
            &[projector("mission.evidence", "runtime.mission.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "mission_runtime",
            Mission,
            "runtime",
            &[coordinator("mission.runtime", "runtime.mission.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "mission_runtime_port",
            Mission,
            "runtime",
            &[adapter("mission.port", "runtime.mission.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "mission_schedule",
            Mission,
            "runtime",
            &[authority("mission.schedule", "runtime.mission.schedule")],
        ),
        RuntimeModuleDescriptor::public(
            "task",
            Mission,
            "runtime",
            &[projector("mission.task", "runtime.mission.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "session_execution",
            Session,
            "runtime",
            &[authority("session.execution", "runtime.session.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "session_input",
            Session,
            "runtime",
            &[authority("session.input", "runtime.session.input")],
        ),
        RuntimeModuleDescriptor::public(
            "input_classifier",
            Session,
            "runtime",
            &[worker("session.input.classify", "runtime.session.input")],
        ),
        RuntimeModuleDescriptor::public(
            "session_lifecycle",
            Session,
            "runtime",
            &[authority("session.lifecycle", "runtime.session.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "mission_command_interpreter",
            Session,
            "runtime",
            &[adapter(
                "session.mission.command",
                "runtime.mission.lifecycle",
            )],
        ),
        RuntimeModuleDescriptor::public(
            "session_relation_graph",
            Session,
            "runtime",
            &[projector("session.relation", "runtime.session.lifecycle")],
        ),
    ]
}

fn agent_team_modules() -> Vec<RuntimeModuleDescriptor> {
    vec![
        RuntimeModuleDescriptor::public(
            "agent",
            Agent,
            "runtime",
            &[coordinator("agent.coordination", "runtime.agent.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "agent_capability",
            Agent,
            "runtime",
            &[authority("agent.capability", "runtime.agent.capability")],
        ),
        RuntimeModuleDescriptor::public(
            "agent_catalog",
            Agent,
            "runtime",
            &[projector("agent.catalog", "runtime.agent.definition")],
        ),
        RuntimeModuleDescriptor::public(
            "agent_evaluation",
            Agent,
            "runtime",
            &[projector("agent.evaluation", "runtime.agent.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "collaboration_template",
            Agent,
            "runtime",
            &[adapter(
                "agent.collaboration.template",
                "runtime.agent.definition",
            )],
        ),
        RuntimeModuleDescriptor::public(
            "definition_registry",
            Agent,
            "runtime",
            &[authority("agent.definition", "runtime.agent.definition")],
        ),
        RuntimeModuleDescriptor::public(
            "agent_in_process_worker",
            Agent,
            "runtime",
            &[worker("agent.in_process", "runtime.agent.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "managed_agent",
            Agent,
            "runtime",
            &[coordinator("agent.managed", "runtime.agent.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "agent_model_selector",
            Agent,
            "runtime",
            &[worker("agent.model.select", "runtime.agent.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "agent_process_jsonl_adapter",
            Agent,
            "runtime",
            &[external("agent.process.adapter", "managed_worker.process")],
        ),
        RuntimeModuleDescriptor::public(
            "agent_result_validator",
            Agent,
            "runtime",
            &[worker("agent.result.validate", "runtime.agent.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "agent_runtime",
            Agent,
            "runtime",
            &[authority("agent.execution", "runtime.agent.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "agent_run_handle",
            Agent,
            "runtime",
            &[projector("agent.run.handle", "runtime.agent.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "pairing",
            Agent,
            "runtime",
            &[adapter("agent.pairing", "runtime.agent.definition")],
        ),
        RuntimeModuleDescriptor::public(
            "team_definition",
            Team,
            "runtime",
            &[authority("team.definition", "runtime.team.definition")],
        ),
        RuntimeModuleDescriptor::public(
            "team_instantiation",
            Team,
            "runtime",
            &[worker("team.instantiate", "runtime.team.definition")],
        ),
        RuntimeModuleDescriptor::public(
            "team_projection",
            Team,
            "runtime",
            &[projector("team.projection", "runtime.team.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "team_agent_selector",
            Team,
            "runtime",
            &[worker("team.agent.select", "runtime.team.definition")],
        ),
        RuntimeModuleDescriptor::public(
            "team_agent_task",
            Team,
            "runtime",
            &[worker("team.agent.task", "runtime.team.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "team_l4_promotion",
            Team,
            "runtime",
            &[worker("team.l4.promote", "runtime.team.definition")],
        ),
        RuntimeModuleDescriptor::public(
            "team_legacy_import",
            Team,
            "runtime",
            &[adapter("team.legacy.import", "runtime.team.definition")],
        ),
        RuntimeModuleDescriptor::public(
            "team_profile_migration",
            Team,
            "runtime",
            &[adapter("team.profile.migrate", "runtime.team.definition")],
        ),
        RuntimeModuleDescriptor::public(
            "team_result_reducer",
            Team,
            "runtime",
            &[worker("team.result.reduce", "runtime.team.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "team_runtime",
            Team,
            "runtime",
            &[authority("team.execution", "runtime.team.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "team_working_state",
            Team,
            "runtime",
            &[projector("team.working", "runtime.team.execution")],
        ),
        RuntimeModuleDescriptor::public(
            "conflict_arbiter",
            Mission,
            "runtime",
            &[worker("mission.conflict", "runtime.mission.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "steward_agent",
            Steward,
            "runtime",
            &[worker("steward.agent", "runtime.agent.execution")],
        ),
    ]
}

fn approval_context_modules() -> Vec<RuntimeModuleDescriptor> {
    vec![
        RuntimeModuleDescriptor::public(
            "approval",
            Approval,
            "runtime",
            &[authority("approval.decision", "runtime.approval.decision")],
        ),
        RuntimeModuleDescriptor::public(
            "approval_queue",
            Approval,
            "runtime",
            &[authority("approval.queue", "runtime.approval.queue")],
        ),
        RuntimeModuleDescriptor::public(
            "context_fanout",
            Context,
            "runtime",
            &[worker("context.fanout", "runtime.context.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "artifact",
            Context,
            "runtime",
            &[projector("context.artifact", "runtime.context.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "context_evidence",
            Context,
            "runtime",
            &[projector("context.evidence", "runtime.context.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "context_ledger",
            Context,
            "runtime",
            &[projector("context.ledger", "runtime.context.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "budget_policy",
            Context,
            "runtime",
            &[authority("context.budget", "runtime.context.budget")],
        ),
        RuntimeModuleDescriptor::public(
            "context_profiler",
            Context,
            "runtime",
            &[projector("context.profile", "runtime.context.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "context_runtime",
            Context,
            "runtime",
            &[authority("context.lifecycle", "runtime.context.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "context_tool_exposure",
            Context,
            "runtime",
            &[projector(
                "context.tool.exposure",
                "runtime.context.lifecycle",
            )],
        ),
        RuntimeModuleDescriptor::public(
            "evidence_planner",
            Context,
            "runtime",
            &[worker("context.evidence.plan", "runtime.context.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "intent_planner",
            Context,
            "harness-contract",
            &[external("context.intent.plan", "harness.intent.plan")],
        ),
        RuntimeModuleDescriptor::public(
            "knowledge_activation",
            Context,
            "runtime",
            &[worker("knowledge.activate", "runtime.context.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "knowledge_compliance",
            Context,
            "runtime",
            &[worker("knowledge.compliance", "runtime.context.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "resources",
            Context,
            "runtime",
            &[authority(
                "execution.resource",
                "runtime.execution.resource",
            )],
        ),
        RuntimeModuleDescriptor::public(
            "skill",
            Skill,
            "runtime",
            &[authority("skill.lifecycle", "runtime.skill.lifecycle")],
        ),
    ]
}

fn reality_recovery_policy_modules() -> Vec<RuntimeModuleDescriptor> {
    vec![
        RuntimeModuleDescriptor::public(
            "structured_data",
            RealityBridge,
            "runtime",
            &[adapter("reality.structured", "runtime.reality.decision")],
        ),
        RuntimeModuleDescriptor::public(
            "fact_extraction",
            RealityBridge,
            "runtime",
            &[worker("reality.fact.extract", "runtime.reality.decision")],
        ),
        RuntimeModuleDescriptor::public(
            "reality_decision",
            RealityBridge,
            "runtime",
            &[authority("reality.decision", "runtime.reality.decision")],
        ),
        RuntimeModuleDescriptor::public(
            "reality_recall_port",
            RealityBridge,
            "runtime",
            &[external("reality.recall", "memory.recall")],
        ),
        RuntimeModuleDescriptor::public(
            "evolution",
            Evolution,
            "runtime",
            &[authority(
                "evolution.lifecycle",
                "runtime.evolution.lifecycle",
            )],
        ),
        RuntimeModuleDescriptor::public(
            "recovery",
            Recovery,
            "runtime",
            &[coordinator("recovery.coordinate", "runtime.event.store")],
        ),
        RuntimeModuleDescriptor::public(
            "recovery_recipes",
            Recovery,
            "runtime",
            &[adapter("recovery.recipe", "runtime.event.store")],
        ),
        RuntimeModuleDescriptor::public(
            "runtime_event_replay",
            Recovery,
            "runtime",
            &[worker("recovery.replay", "runtime.event.store")],
        ),
        RuntimeModuleDescriptor::public(
            "runtime_event_store",
            Recovery,
            "runtime",
            &[authority("event.store", "runtime.event.store")],
        ),
        RuntimeModuleDescriptor::public(
            "cross_plane_policy",
            Policy,
            "harness-contract",
            &[external("policy.cross_plane", "harness.cross_plane.policy")],
        ),
        RuntimeModuleDescriptor::public(
            "permissions",
            Policy,
            "runtime",
            &[projector("policy.permission", "runtime.policy")],
        ),
        RuntimeModuleDescriptor::public(
            "policy_engine",
            Policy,
            "runtime",
            &[authority("policy.engine", "runtime.policy")],
        ),
        RuntimeModuleDescriptor::public(
            "security",
            Policy,
            "runtime",
            &[authority("security.lifecycle", "runtime.security")],
        ),
        RuntimeModuleDescriptor::public(
            "trust_resolver",
            Policy,
            "runtime",
            &[worker("security.trust", "runtime.security")],
        ),
        RuntimeModuleDescriptor::public(
            "autonomy_profile",
            Policy,
            "runtime",
            &[projector("policy.autonomy", "runtime.policy")],
        ),
    ]
}

fn configuration_infrastructure_modules() -> Vec<RuntimeModuleDescriptor> {
    vec![
        RuntimeModuleDescriptor::public(
            "config",
            Configuration,
            "runtime",
            &[authority("configuration", "runtime.configuration")],
        ),
        RuntimeModuleDescriptor::public(
            "config_validate",
            Configuration,
            "runtime",
            &[worker("configuration.validate", "runtime.configuration")],
        ),
        RuntimeModuleDescriptor::public(
            "profile",
            Configuration,
            "runtime",
            &[projector("configuration.profile", "runtime.configuration")],
        ),
        RuntimeModuleDescriptor::public(
            "capability",
            Infrastructure,
            "runtime",
            &[projector(
                "capability.projection",
                "runtime.capability.catalog",
            )],
        ),
        RuntimeModuleDescriptor::public(
            "capability_manifest",
            Infrastructure,
            "runtime",
            &[authority(
                "capability.catalog",
                "runtime.capability.catalog",
            )],
        ),
        RuntimeModuleDescriptor::public(
            "checkpoint",
            Infrastructure,
            "runtime",
            &[authority("checkpoint", "runtime.checkpoint")],
        ),
        RuntimeModuleDescriptor::public(
            "lane_completion",
            Infrastructure,
            "runtime",
            &[projector(
                "execution.lane.completion",
                "runtime.execution.graph",
            )],
        ),
        RuntimeModuleDescriptor::public(
            "eval_gate",
            Infrastructure,
            "harness-eval",
            &[external("evaluation.gate", "harness.evaluation")],
        ),
        RuntimeModuleDescriptor::public(
            "lifecycle_hooks",
            Infrastructure,
            "runtime",
            &[adapter("lifecycle.hook", "runtime.control")],
        ),
        RuntimeModuleDescriptor::public(
            "mcp_lifecycle_hardened",
            Infrastructure,
            "mcp",
            &[external("mcp.lifecycle", "mcp.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "mcp_server",
            Infrastructure,
            "mcp",
            &[external("mcp.server", "mcp.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "mcp_tool_bridge",
            Infrastructure,
            "mcp",
            &[external("mcp.tool.bridge", "mcp.lifecycle")],
        ),
        RuntimeModuleDescriptor::public(
            "quality_gate",
            Infrastructure,
            "runtime",
            &[worker("quality.gate", "runtime.collaboration.program")],
        ),
        RuntimeModuleDescriptor::public(
            "release_gate",
            Infrastructure,
            "runtime",
            &[worker("release.gate", "runtime.capability.catalog")],
        ),
        RuntimeModuleDescriptor::public(
            "sandbox",
            Infrastructure,
            "runtime",
            &[external("sandbox", "sandbox.policy")],
        ),
        RuntimeModuleDescriptor::public(
            "source_self_audit",
            Infrastructure,
            "runtime",
            &[projector("source.audit", "runtime.capability.catalog")],
        ),
        RuntimeModuleDescriptor::public(
            "surface_contract",
            Infrastructure,
            "surface",
            &[external("surface.contract", "surface.contract")],
        ),
        RuntimeModuleDescriptor::public(
            "upgrade",
            Infrastructure,
            "runtime",
            &[authority("upgrade", "runtime.upgrade")],
        ),
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
