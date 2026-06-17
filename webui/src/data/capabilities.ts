import type { CapabilitySpec, NavId } from '../types';

type CapabilityId = Exclude<NavId, 'chat' | 'settings'>;

function spec(
  id: CapabilityId,
  title: string,
  subtitle: string,
  sections: Array<{ id: string; label: string; description: string }>,
  actions: Array<{ label: string; kind: 'primary' | 'secondary' | 'danger'; endpoint?: string }>,
  inspector: Array<{ label: string; value: string }>,
): CapabilitySpec {
  return {
    id,
    title,
    subtitle,
    primaryAction: actions[0]?.label || 'Refresh',
    metrics: [],
    chartKind: 'bar',
    chartTitle: '',
    chartData: [],
    tableTitle: '',
    rows: [],
    sections,
    actions,
    inspector,
  };
}

export const capabilitySpecs: Record<CapabilityId, CapabilitySpec> = {
  runtime: spec(
    'runtime',
    'Runtime Control',
    '会话租约、工具流水、价值闭环和控制面健康集中在一个运行视图。',
    [
      { id: 'overview', label: 'Overview', description: 'Health, readiness, blocked checks, and current control-plane state.' },
      { id: 'runs', label: 'Runs', description: 'Session runs, value-loop stages, leases, and active execution history.' },
      { id: 'policy', label: 'Policy', description: 'Runtime policy, observability switches, provider reload, and production gates.' },
      { id: 'timeline', label: 'Timeline', description: 'Chronological runtime events with tool/context/approval boundaries.' },
    ],
    [
      { label: 'Refresh runtime', kind: 'primary', endpoint: '/api/runtime/timeline' },
      { label: 'Reload providers', kind: 'secondary', endpoint: '/api/runtime/providers/reload' },
      { label: 'Acquire lease', kind: 'secondary', endpoint: '/api/runtime/session-leases/acquire' },
    ],
    [
      { label: 'API', value: '/api/runtime/control-plane' },
      { label: 'Parity', value: 'WebUI and TUI full control surface' },
      { label: 'CLI', value: 'core-only status and import commands' },
    ],
  ),
  context: spec(
    'context',
    'Context Studio',
    '把上下文预算、证据来源、压缩策略和注入效果用可审计视图展开。',
    [
      { id: 'packet', label: 'Packet builder', description: 'Build and preview current context packets before a turn.' },
      { id: 'budget', label: 'Budget', description: 'Token pressure, source mix, compression barriers, and reuse strategy.' },
      { id: 'evidence', label: 'Evidence', description: 'Resolve evidence refs, source confidence, and injection decisions.' },
      { id: 'history', label: 'History', description: 'Persisted envelopes and recommendations for the active session.' },
    ],
    [
      { label: 'Build packet', kind: 'primary', endpoint: '/api/context/current' },
      { label: 'Resolve evidence', kind: 'secondary', endpoint: '/api/evidence/resolve' },
      { label: 'Record recommendation', kind: 'secondary', endpoint: '/api/sessions/:id/context/recommendations' },
    ],
    [
      { label: 'API', value: '/api/context/current' },
      { label: 'Kernel role', value: 'Context orchestration and evidence routing' },
      { label: 'Memory link', value: 'Recall packets and facts feed context packets' },
    ],
  ),
  memory: spec(
    'memory',
    'Memory Graph',
    '长期记忆、事实判断、实体关系和维护任务在统一知识层协作。',
    [
      { id: 'recall', label: 'Recall', description: 'Search, explain, and assemble memory packets for current work.' },
      { id: 'entities', label: 'Entities', description: 'Detected entities, symbols, triples, and links.' },
      { id: 'facts', label: 'Facts', description: 'Fact registration, checking, conflict detection, and audit.' },
      { id: 'maintenance', label: 'Maintenance', description: 'Stale candidates, lifecycle review, and repair actions.' },
    ],
    [
      { label: 'Scan memory', kind: 'primary', endpoint: '/api/memory/maintenance' },
      { label: 'Check facts', kind: 'secondary', endpoint: '/api/memory/facts/check' },
      { label: 'Build memory packet', kind: 'secondary', endpoint: '/api/memory/packet' },
    ],
    [
      { label: 'API', value: '/api/memory/status' },
      { label: 'Long memory', value: 'Facts, entities, triples, clusters, links' },
      { label: 'Structured data', value: 'Facts can be grounded by structured evidence' },
    ],
  ),
  skills: spec(
    'skills',
    'Skills Console',
    '技能全集、投影、运行记录和 WebUI/TUI 能力对齐集中管理。',
    [
      { id: 'catalog', label: 'Catalog', description: 'Installed and discovered skills with manifest health.' },
      { id: 'projection', label: 'Projection', description: 'WebUI/TUI/CLI/MFG surface mapping and capability parity.' },
      { id: 'runs', label: 'Runs', description: 'Skill run history, validation, planning, and execution results.' },
      { id: 'governance', label: 'Governance', description: 'Permissions, evidence model, approval needs, and risk.' },
    ],
    [
      { label: 'Validate skill', kind: 'primary', endpoint: '/api/skills/:id/actions/validate' },
      { label: 'Plan run', kind: 'secondary', endpoint: '/api/skills/:id/actions/plan' },
      { label: 'Run skill', kind: 'secondary', endpoint: '/api/skills/:id/actions/run' },
    ],
    [
      { label: 'API', value: '/api/skills/projection' },
      { label: 'Parity', value: 'WebUI and TUI capability全集' },
      { label: 'CLI', value: 'Install, list, invoke core only' },
    ],
  ),
  agents: spec(
    'agents',
    'Agents Workbench',
    '多 agent 分工、执行状态、审阅结果和并行路线以工作流方式呈现。',
    [
      { id: 'lanes', label: 'Lanes', description: 'Parallel execution lanes, owners, blocker status, and outputs.' },
      { id: 'graph', label: 'Work graph', description: 'Dependencies, review handoff, and current execution topology.' },
      { id: 'review', label: 'Review', description: 'Agent result review, merge readiness, and evidence quality.' },
      { id: 'terminal', label: 'Terminal state', description: 'Spawn manifests, running status, and persisted outputs.' },
    ],
    [
      { label: 'Spawn lane', kind: 'primary', endpoint: '/api/tasks/:id/agent-graph' },
      { label: 'Review phase', kind: 'secondary', endpoint: '/api/tasks/:id/phases/:phase/review' },
      { label: 'Cancel task', kind: 'danger', endpoint: '/api/tasks/:id/cancel' },
    ],
    [
      { label: 'API', value: '/api/agents/runs' },
      { label: 'Execution', value: 'Parallel lanes with review gate' },
      { label: 'Output', value: 'Manifests and artifacts are persisted' },
    ],
  ),
  tools: spec(
    'tools',
    'Tools Registry',
    '工具目录、执行规划、批量只读、事务预览、checkpoint、缓存和工具流水统一运管监看。',
    [
      { id: 'registry', label: 'Registry', description: 'Tool catalog, safety profile, cache policy, tags, and availability.' },
      { id: 'operations', label: 'Operations', description: 'Intent planning, context fanout, safe execute, and readonly batch orchestration.' },
      { id: 'mutations', label: 'Mutations', description: 'Workspace-scoped edit preview, apply transaction, changed refs, and receipts.' },
      { id: 'checkpoints', label: 'Checkpoints', description: 'Create, diff, and restore checkpoints with explicit confirmation.' },
      { id: 'cache', label: 'Cache', description: 'Tool cache statistics and reuse visibility.' },
      { id: 'ledger', label: 'Ledger', description: 'Runtime tool events, outputs, errors, warnings, and next actions.' },
      { id: 'risk', label: 'Risk', description: 'Approval policy, destructive actions, preflight checks, and cross-plane governance.' },
    ],
    [
      { label: 'Run readonly batch', kind: 'primary', endpoint: '/api/tools/batch-readonly' },
      { label: 'Preview mutation', kind: 'secondary', endpoint: '/api/tools/mutations/preview' },
      { label: 'Create checkpoint', kind: 'secondary', endpoint: '/api/tools/checkpoints' },
    ],
    [
      { label: 'API', value: '/api/tools' },
      { label: 'Execution', value: '/api/tools/execute, /api/tools/batch-readonly' },
      { label: 'Safety', value: 'Workspace-scoped writes and approval-aware actions' },
    ],
  ),
  gateway: spec(
    'gateway',
    'Gateway and Cross-plane',
    '外部平台、连接器资源、MCP 服务、身份授权和跨平面执行统一管理。',
    [
      { id: 'connectors', label: 'Connectors', description: 'Platform accounts, connector capabilities, and MCP server bindings.' },
      { id: 'resources', label: 'Resources', description: 'Resource directory validation and promotion into memory.' },
      { id: 'identities', label: 'Identities', description: 'Identity bindings, grants, policy simulation, and revocation.' },
      { id: 'executions', label: 'Executions', description: 'Cross-plane action preflight, dry-run/live execution, and receipts.' },
    ],
    [
      { label: 'Run preflight', kind: 'primary', endpoint: '/api/cross-plane/action/preflight' },
      { label: 'Create identity', kind: 'secondary', endpoint: '/api/cross-plane/identities' },
      { label: 'Create grant', kind: 'secondary', endpoint: '/api/cross-plane/grants' },
    ],
    [
      { label: 'API', value: '/api/cross-plane/summary' },
      { label: 'Connectors', value: '/api/connectors/resources' },
      { label: 'Governance', value: 'Identities, grants, audit, executions' },
    ],
  ),
  mfg: spec(
    'mfg',
    'MFG Manufacturing Application',
    'MFG 是制造领域上层应用，运行在 cowd 内核的数据、记忆、上下文与跨平面能力之上。',
    [
      { id: 'data-plane', label: 'Data plane', description: 'Manufacturing data ingest planning, source packs, and connector runs.' },
      { id: 'entities', label: 'Entities', description: 'Manufacturing entities, source key resolution, relations, and impact paths.' },
      { id: 'metrics', label: 'Metrics', description: 'Metric lineage, materialization, attention plans, and compute jobs.' },
      { id: 'incidents', label: 'Incidents', description: 'Incident room, evidence quality, playbooks, skills, and actions.' },
    ],
    [
      { label: 'Ingest manufacturing facts', kind: 'primary', endpoint: '/api/matrix/facts/ingest' },
      { label: 'Plan compute job', kind: 'secondary', endpoint: '/api/matrix/compute/jobs/plan' },
      { label: 'Generate report', kind: 'secondary', endpoint: '/api/apps/mfg/cockpit/profiles/:id/reports/generate' },
    ],
    [
      { label: 'API', value: '/api/apps/mfg/app' },
      { label: 'Boundary', value: 'Application layer, not cowd kernel' },
      { label: 'Kernel dependency', value: 'Structured data, memory, context, cross-plane' },
    ],
  ),
  audit: spec(
    'audit',
    'Audit and Governance',
    '审计导出、使用统计、发布门禁、审批历史和跨平面回执集中核查。',
    [
      { id: 'export', label: 'Export', description: 'Approval and memory audit records with filters and pagination.' },
      { id: 'usage', label: 'Usage', description: 'Usage rollups by platform, session, tokens, and estimated cost.' },
      { id: 'release', label: 'Release gate', description: 'Surface capability projection and release gate checks.' },
      { id: 'evidence', label: 'Evidence', description: 'Approval history, cross-plane audit, and execution receipts.' },
    ],
    [
      { label: 'Refresh audit', kind: 'primary', endpoint: '/api/audit/export' },
      { label: 'Load release gate', kind: 'secondary', endpoint: '/api/cowd/release-gate' },
      { label: 'Load cross-plane audit', kind: 'secondary', endpoint: '/api/cross-plane/audit' },
    ],
    [
      { label: 'API', value: '/api/audit/export' },
      { label: 'Governance', value: 'Approval, memory, cross-plane, release gate' },
      { label: 'Evidence', value: 'Receipts and checks are shown as first-class records' },
    ],
  ),
};
