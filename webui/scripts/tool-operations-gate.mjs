#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const repoRoot = path.resolve(new URL('../../', import.meta.url).pathname);
const webuiRoot = path.join(repoRoot, 'webui');
const planRoot = process.env.COWD_PLAN_ROOT || path.resolve(repoRoot, '../plan/0617-最终目标收口');
const reportDir = path.join(planRoot, 'reports');
const version = process.env.COWD_VERSION || 'v0.9.246';
const gate = process.argv.includes('--gate');

function read(file) {
  return fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : '';
}

function hasAll(text, items) {
  return items.filter((item) => !text.includes(item));
}

const files = {
  backend: path.join(repoRoot, 'crates/cowd-cli/src/api_routes/system_routes.rs'),
  client: path.join(webuiRoot, 'src/api/client.ts'),
  page: path.join(webuiRoot, 'src/pages/ToolsPage.vue'),
  capabilities: path.join(webuiRoot, 'src/data/capabilities.ts'),
  styles: path.join(webuiRoot, 'src/styles/base.css'),
  tuiPanel: path.join(repoRoot, 'crates/cowd-cli/src/tui/components/tool_ops_panel.rs'),
  tuiProjection: path.join(repoRoot, 'crates/cowd-cli/src/tui/projection_client.rs'),
  tuiState: path.join(repoRoot, 'crates/cowd-cli/src/tui/state.rs'),
};

const requiredBackendRoutes = [
  '/api/tools/execute',
  '/api/tools/cache',
  '/api/tools/batch-readonly',
  '/api/tools/mutations/preview',
  '/api/tools/mutations/apply',
  '/api/tools/checkpoints',
  '/api/tools/checkpoints/:id/diff',
  '/api/tools/checkpoints/:id/restore',
  '/api/tools/intent-plan',
  '/api/tools/context-fanout/plan',
];

const requiredClientMethods = [
  'toolExecute:',
  'toolCacheStats:',
  'toolBatchReadonly:',
  'toolMutationPreview:',
  'toolMutationApply:',
  'toolCheckpoints:',
  'toolCheckpointCreate:',
  'toolCheckpointDiff:',
  'toolCheckpointRestore:',
  'toolIntentPlan:',
  'toolContextFanoutPlan:',
];

const requiredSections = [
  'registry',
  'operations',
  'mutations',
  'checkpoints',
  'cache',
  'ledger',
  'risk',
];

const requiredHeadings = [
  'Tool registry',
  'Execution planner',
  'Mutation transactions',
  'Checkpoints',
  'Tool cache',
  'Tool ledger',
  'Risk preflight',
];

const backendText = read(files.backend);
const clientText = read(files.client);
const pageText = read(files.page);
const capabilitiesText = read(files.capabilities);
const stylesText = read(files.styles);
const tuiPanelText = read(files.tuiPanel);
const tuiProjectionText = read(files.tuiProjection);
const tuiStateText = read(files.tuiState);
const failures = [];

for (const route of hasAll(backendText, requiredBackendRoutes)) failures.push(`backend missing route ${route}`);
for (const route of hasAll(clientText, requiredBackendRoutes.map((route) => route.replace('/:id', '/${encodeURIComponent(id)}')))) {
  const literal = route.includes('${') ? route.replace('/${encodeURIComponent(id)}', '') : route;
  if (!clientText.includes(literal)) failures.push(`client missing endpoint ${route}`);
}
for (const method of hasAll(clientText, requiredClientMethods)) failures.push(`client missing method ${method}`);
for (const section of requiredSections) {
  if (!capabilitiesText.includes(`id: '${section}'`)) failures.push(`capabilities missing section ${section}`);
  if (!pageText.includes(`data-section="${section}"`)) failures.push(`ToolsPage missing data-section ${section}`);
  if (!stylesText.includes(`data-active-section="${section}"`)) failures.push(`section filter missing ${section}`);
}
for (const heading of hasAll(pageText, requiredHeadings)) failures.push(`ToolsPage missing heading ${heading}`);

const pageMustUse = [
  'api.toolRegistry()',
  'api.toolCacheStats()',
  'api.toolCheckpoints()',
  'api.runtimeTimeline(',
  '<DataTable',
  '<RequestReceipt',
  '<RawPayload',
  'Preview mutation',
  'Apply transaction',
  'Run readonly batch',
];
for (const item of hasAll(pageText, pageMustUse)) failures.push(`ToolsPage missing implementation evidence ${item}`);

const rawOnlyPattern = /data-section="(?:operations|mutations|checkpoints|cache|ledger)"[\s\S]{0,1200}<RawPayload[\s\S]{0,300}<\/section>/g;
const rawOnlyMatches = Array.from(pageText.matchAll(rawOnlyPattern)).filter((match) => !/DataTable|RequestReceipt|EmptyState/.test(match[0]));
if (rawOnlyMatches.length) failures.push('ToolsPage has a tool-ops section that appears to expose only RawPayload');

if (!backendText.includes('is_webui_generic_tool_allowed')) failures.push('backend execute whitelist guard missing');
if (!backendText.includes('validate_workspace_relative_path')) failures.push('backend workspace path guard missing');
if (!backendText.includes('TOOL_CWD_LOCK')) failures.push('backend workspace cwd lock missing');

const requiredTuiPanelTerms = [
  'pub struct ToolOpsPanel',
  'ToolOpsMode::Registry',
  'ToolOpsMode::Operations',
  'ToolOpsMode::Mutations',
  'ToolOpsMode::Checkpoints',
  'ToolOpsMode::Cache',
  'ToolOpsMode::Ledger',
  'ToolOpsMode::Risk',
  'arm_apply_mutation',
  'arm_restore_checkpoint',
  'tool_ops_panel_requires_second_confirmation_for_dangerous_actions',
];
for (const term of hasAll(tuiPanelText, requiredTuiPanelTerms)) failures.push(`TUI ToolOpsPanel missing ${term}`);

const requiredTuiProjectionTerms = [
  'tool_execute',
  'tool_cache_stats',
  'tool_batch_readonly',
  'tool_mutation_preview',
  'tool_mutation_apply',
  'tool_checkpoints',
  'tool_checkpoint_create',
  'tool_checkpoint_diff',
  'tool_checkpoint_restore',
  'tool_intent_plan',
  'tool_context_fanout_plan',
];
for (const term of hasAll(tuiProjectionText, requiredTuiProjectionTerms)) failures.push(`TUI projection client missing ${term}`);

const requiredTuiStateTerms = [
  'ToolOpsPanel',
  'TAB_TOOLS',
  'handle_tool_ops_action',
  'refresh_tool_ops_panel_overview',
  'record_tool_ops_result',
];
for (const term of hasAll(tuiStateText, requiredTuiStateTerms)) failures.push(`TUI state missing ${term}`);

const report = {
  version,
  generated_at: new Date().toISOString(),
  status: failures.length ? 'fail' : 'pass',
  scope: 'tool operation management closure: backend routes, WebUI client, page sections, navigation filters, and structured rendering',
  tui_scope: 'TUI parity closure: ToolOpsPanel, projection client, sidebar action handling, and dangerous-action confirmation',
  required_sections: requiredSections,
  required_backend_routes: requiredBackendRoutes,
  required_client_methods: requiredClientMethods,
  required_tui_projection_methods: requiredTuiProjectionTerms,
  failures,
};

fs.mkdirSync(reportDir, { recursive: true });
const reportPath = path.join(reportDir, `${version}-tool-operations-gate.json`);
fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);

if (failures.length) {
  console.error(`Tool operations gate failed:\n${failures.map((item) => `- ${item}`).join('\n')}`);
  if (gate) process.exit(1);
}

console.log(`Tool operations gate written to ${reportPath}`);
