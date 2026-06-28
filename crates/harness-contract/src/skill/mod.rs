//! Pure contracts for Skill package inspection, profiling, runtime invocation,
//! and evidence. Implementations live in the `skill` and `runtime` crates.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillKind {
    Document,
    Workflow,
    RuntimePackage,
    BrowserStatic,
    McpServer,
    SidecarService,
    Composite,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillLifecycleStatus {
    Imported,
    Inspected,
    UsablePrompt,
    UsableRuntime,
    Blocked,
    Archived,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillAdapterKind {
    PromptOnly,
    ToolGuided,
    SandboxExec,
    BrowserStatic,
    McpServer,
    SidecarService,
    Composite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillDetectedRuntime {
    Markdown,
    Shell,
    Python,
    Node,
    Go,
    Rust,
    Browser,
    Notebook,
    Mcp,
    Docker,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkillRiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillEntrypoint {
    pub runtime: SkillDetectedRuntime,
    pub path: String,
    pub adapter: SkillAdapterKind,
    pub command_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillRiskSignal {
    pub level: SkillRiskLevel,
    pub kind: String,
    pub evidence: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillInspectionReport {
    pub source_root: String,
    pub detected_files: Vec<String>,
    pub detected_runtimes: Vec<SkillDetectedRuntime>,
    pub entrypoints: Vec<SkillEntrypoint>,
    pub risk_signals: Vec<SkillRiskSignal>,
    pub recommended_adapters: Vec<SkillAdapterKind>,
    pub blocked_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillCapabilityProfile {
    pub skill_id: String,
    pub name: String,
    pub version: Option<String>,
    pub source_root: String,
    pub package_fingerprint: String,
    pub kind: SkillKind,
    pub lifecycle_status: SkillLifecycleStatus,
    pub adapters: Vec<SkillAdapterKind>,
    pub risk_level: SkillRiskLevel,
    pub entrypoints: Vec<SkillEntrypoint>,
    pub inspection_summary: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentSkillProfile {
    pub baseline_skill_refs: Vec<String>,
    pub template_skill_refs: Vec<String>,
    pub team_skill_refs: Vec<String>,
    pub task_skill_refs: Vec<String>,
    pub explicit_grants: Vec<String>,
    pub hidden_skill_refs: Vec<String>,
    pub adapter_ceiling: Vec<SkillAdapterKind>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillInvocationEvidence {
    pub skill_id: String,
    pub skill_version: Option<String>,
    pub adapter: SkillAdapterKind,
    pub entrypoint: Option<String>,
    pub outcome: String,
    pub evidence_refs: Vec<String>,
}
