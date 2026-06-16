#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const repoRoot = path.resolve(new URL('../../', import.meta.url).pathname);
const webuiRoot = path.join(repoRoot, 'webui');
const planRoot = process.env.COWD_PLAN_ROOT || path.resolve(repoRoot, '../plan/0616-前端彻底重构/10-模块化管理重构方案');
const reportDir = path.join(planRoot, 'reports');
const version = process.env.COWD_VERSION || 'v0.9.245';
const gate = process.argv.includes('--gate');

const modules = [
  { id: 'runtime', page: 'RuntimePage.vue', routes: ['/api/runtime'], tui: ['runtime_activity_panel.rs', 'system_status_bar.rs'], cli: ['gateway', 'doctor'] },
  { id: 'context', page: 'ContextPage.vue', routes: ['/api/context', '/api/evidence'], tui: ['context_panel.rs', 'context_suggestions.rs'], cli: ['prompt', 'compact'] },
  { id: 'memory', page: 'MemoryPage.vue', routes: ['/api/memory', '/api/cowd/structured'], tui: ['memory_panel.rs', 'l4_knowledge_view.rs'], cli: ['import-session'] },
  { id: 'skills', page: 'SkillsPage.vue', routes: ['/api/skills'], tui: ['skills_panel.rs'], cli: ['skills'] },
  { id: 'agents', page: 'AgentsPage.vue', routes: ['/api/agents', '/api/tasks'], tui: ['agent_team_panel.rs', 'agents_overlay.rs'], cli: ['prompt'] },
  { id: 'tools', page: 'ToolsPage.vue', routes: ['/api/tools', '/api/commands'], tui: ['activity_panel.rs', 'runtime_activity_panel.rs'], cli: ['prompt'] },
  { id: 'gateway', page: 'GatewayPage.vue', routes: ['/api/connectors', '/api/cross-plane', '/api/platforms'], tui: ['gateway_panel.rs', 'approval_cockpit_panel.rs'], cli: ['gateway'] },
  { id: 'iacc', page: 'IaccPage.vue', routes: ['/api/iacc'], tui: ['goal_workbench_panel.rs', 'task_decomposition_view.rs'], cli: ['gateway'] },
  { id: 'audit', page: 'AuditPage.vue', routes: ['/api/audit', '/api/usage', '/api/cowd/release-gate'], tui: ['export_dialog.rs', 'approval_cockpit_panel.rs'], cli: ['doctor'] },
];

function read(file) {
  return fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : '';
}

function walk(target) {
  if (!fs.existsSync(target)) return [];
  const stat = fs.statSync(target);
  if (stat.isFile()) return [target];
  return fs.readdirSync(target).flatMap((entry) => walk(path.join(target, entry)));
}

function extractBackendRoutes() {
  const files = walk(path.join(repoRoot, 'crates/cowd-cli/src/api_routes')).filter((file) => file.endsWith('.rs'));
  files.push(path.join(repoRoot, 'crates/cowd-cli/src/api_routes.rs'));
  const routes = [];
  for (const file of files) {
    const text = read(file);
    const regex = /\.route\(\s*"([^"]+)"/g;
    let match;
    while ((match = regex.exec(text))) {
      routes.push({ path: match[1], file: path.relative(repoRoot, file) });
    }
  }
  return routes;
}

function extractClientEndpoints() {
  const text = read(path.join(webuiRoot, 'src/api/client.ts'));
  const endpoints = new Set();
  const regex = /(?:read|write|readText)\s*(?:<[^>]+>)?\(\s*([`'"])([\s\S]*?)\1/g;
  let match;
  while ((match = regex.exec(text))) endpoints.add(match[2]);
  return Array.from(endpoints).sort();
}

function extractCapabilityEndpoints() {
  const text = read(path.join(webuiRoot, 'src/data/capabilities.ts'));
  const endpoints = new Set();
  const regex = /endpoint:\s*['"`]([^'"`]+)['"`]/g;
  let match;
  while ((match = regex.exec(text))) endpoints.add(match[1]);
  return Array.from(endpoints).sort();
}

function referencesAny(text, needles) {
  return needles.some((needle) => text.includes(needle));
}

const backendRoutes = extractBackendRoutes();
const clientEndpoints = extractClientEndpoints();
const capabilityEndpoints = extractCapabilityEndpoints();
const tuiFiles = walk(path.join(repoRoot, 'crates/cowd-cli/src/tui')).map((file) => path.basename(file));
const cliMain = read(path.join(repoRoot, 'crates/cowd-cli/src/main.rs'));
const cliMod = read(path.join(repoRoot, 'crates/cowd-cli/src/cli/mod.rs'));
const cliText = `${cliMain}\n${cliMod}`;

const moduleReports = modules.map((module) => {
  const pagePath = path.join(webuiRoot, 'src/pages', module.page);
  const pageText = read(pagePath);
  const backend = module.routes.flatMap((prefix) => backendRoutes.filter((route) => route.path.startsWith(prefix)));
  const client = module.routes.flatMap((prefix) => clientEndpoints.filter((endpoint) => endpoint.startsWith(prefix)));
  const capability = module.routes.flatMap((prefix) => capabilityEndpoints.filter((endpoint) => endpoint.startsWith(prefix)));
  const tui = module.tui.filter((file) => tuiFiles.includes(file));
  const cli = module.cli.filter((term) => cliText.includes(term));
  const findings = [];
  if (!pageText) findings.push('missing WebUI page');
  if (!backend.length) findings.push('missing backend route family');
  if (!client.length) findings.push('missing WebUI API client family');
  if (!capability.length) findings.push('missing capability projection endpoint');
  if (!tui.length) findings.push('missing TUI projection evidence');
  if (!cli.length) findings.push('missing CLI core access evidence');
  if (module.id === 'iacc' && !(pageText.includes('manufacturing application layer') && pageText.includes('cowd kernel'))) {
    findings.push('IACC boundary text is missing from WebUI');
  }
  if (module.id === 'memory' && !referencesAny(pageText, ['Structured Data Core', 'structured'])) {
    findings.push('structured data core is not visible in Memory page');
  }
  return {
    id: module.id,
    page: path.relative(repoRoot, pagePath),
    status: findings.length ? 'review' : 'pass',
    findings,
    webui: {
      page_exists: Boolean(pageText),
      client_endpoint_count: new Set(client).size,
      capability_endpoint_count: new Set(capability).size,
    },
    backend: {
      route_count: backend.length,
      route_files: Array.from(new Set(backend.map((route) => route.file))).sort(),
    },
    tui: {
      expected_panels: module.tui,
      matched_panels: tui,
      parity_role: 'same core capability set, console-first interaction',
    },
    cli: {
      matched_terms: cli,
      parity_role: 'minimal kernel control, import/view/start/status oriented',
    },
  };
});

const blocking = moduleReports.flatMap((module) => module.findings.map((finding) => `${module.id}: ${finding}`));

const report = {
  version,
  generated_at: new Date().toISOString(),
  status: blocking.length ? 'fail' : 'pass',
  principles: [
    'WebUI is the strongest management surface and must expose complete management workflows.',
    'TUI keeps the same core capability set with console-appropriate interaction density.',
    'CLI stays minimal and controls only kernel entry, import/view/status/start workflows.',
    'IACC remains an application layer over cowd; structured data remains cowd kernel capability.',
  ],
  totals: {
    modules: moduleReports.length,
    backend_routes: backendRoutes.length,
    client_endpoints: clientEndpoints.length,
    capability_endpoints: capabilityEndpoints.length,
    blocking: blocking.length,
  },
  modules: moduleReports,
  blocking,
};

fs.mkdirSync(reportDir, { recursive: true });
const reportPath = path.join(reportDir, `${version}-capability-parity.json`);
fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);

if (blocking.length) {
  console.error(`Capability parity gate failed:\n${blocking.map((item) => `- ${item}`).join('\n')}`);
  if (gate) process.exit(1);
}

console.log(`Capability parity report written to ${reportPath}`);
