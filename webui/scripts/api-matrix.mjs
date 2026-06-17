#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const repoRoot = path.resolve(new URL('../../', import.meta.url).pathname);
const webuiRoot = path.join(repoRoot, 'webui');
const planRoot = process.env.COWD_PLAN_ROOT || path.resolve(repoRoot, '../plan/0616-前端彻底重构/10-模块化管理重构方案');
const reportDir = path.join(planRoot, 'reports');
const version = process.env.COWD_VERSION || 'v0.9.241';
const gate = process.argv.includes('--gate');

const clientPath = path.join(webuiRoot, 'src/api/client.ts');
const sourceDirs = [
  path.join(webuiRoot, 'src/pages'),
  path.join(webuiRoot, 'src/components'),
  path.join(webuiRoot, 'src/stores'),
];
const testDirs = [
  path.join(webuiRoot, 'src'),
  webuiRoot,
];
const routeDirs = [
  path.join(repoRoot, 'crates/cowd-cli/src/api_routes'),
  path.join(repoRoot, 'crates/cowd-cli/src/api_routes.rs'),
];

const criticalMethods = {
  uploadFile: { criticality: 'p0', page: 'workspace' },
  saveFile: { criticality: 'p1', page: 'workspace' },
  renameWorkspacePath: { criticality: 'p1', page: 'workspace' },
  deleteWorkspacePath: { criticality: 'p1', page: 'workspace' },
  skillAction: { criticality: 'p1', page: 'skills' },
  createMemoryEntry: { criticality: 'p1', page: 'memory' },
  updateMemoryEntry: { criticality: 'p1', page: 'memory' },
  deleteMemoryEntry: { criticality: 'p1', page: 'memory' },
  crossPlaneExecute: { criticality: 'p0', page: 'gateway' },
  mfgSourcePackUpsert: { criticality: 'p0', page: 'mfg', quarantineRequired: true },
  mfgIngestFact: { criticality: 'p0', page: 'mfg', quarantineRequired: true },
  mfgEntityUpsert: { criticality: 'p0', page: 'mfg', quarantineRequired: true },
  mfgRelationUpsert: { criticality: 'p0', page: 'mfg', quarantineRequired: true },
  mfgComputeJobRun: { criticality: 'p0', page: 'mfg', quarantineRequired: true },
  mfgSeedDomain: { criticality: 'p0', page: 'mfg', quarantineRequired: true },
  mfgSeedOntology: { criticality: 'p0', page: 'mfg', quarantineRequired: true },
  mfgExecuteAction: { criticality: 'p0', page: 'mfg', quarantineRequired: true },
  mfgExecutionBridge: { criticality: 'p0', page: 'mfg', quarantineRequired: true },
  mfgRetryReportDelivery: { criticality: 'p0', page: 'mfg', quarantineRequired: true },
};

function read(file) {
  return fs.readFileSync(file, 'utf8');
}

function walk(target) {
  if (!fs.existsSync(target)) return [];
  const stat = fs.statSync(target);
  if (stat.isFile()) return [target];
  return fs.readdirSync(target).flatMap((entry) => walk(path.join(target, entry)));
}

function normalizeRoute(route) {
  return route
    .replace(/\$\{[^`'"]+?\}/g, ':param')
    .replace(/\?.*$/, '')
    .replace(/:id/g, ':param')
    .replace(/:name/g, ':param')
    .replace(/:reference/g, ':param')
    .replace(/:run_id/g, ':param')
    .replace(/:phase_id/g, ':param')
    .replace(/\/+/g, '/');
}

function inferMethod(body) {
  const method = body.match(/method:\s*['"`]([A-Z]+)['"`]/)?.[1];
  if (method) return method;
  if (body.includes('write<') || body.includes('write(')) return 'POST';
  return 'GET';
}

function extractClientMethods() {
  const text = read(clientPath);
  const apiStart = text.indexOf('export const api = {');
  const apiText = text.slice(apiStart);
  const methodRegex = /^\s{2}([A-Za-z0-9_]+):\s*(?:async\s*)?\([^)]*\)\s*=>\s*([\s\S]*?)(?=\n\s{2}[A-Za-z0-9_]+:\s*(?:async\s*)?\([^)]*\)\s*=>|\n};)/gm;
  const entries = [];
  let match;
  while ((match = methodRegex.exec(apiText))) {
    const [, name, body] = match;
    const pathMatch = body.match(/(?:read|write|readText)\s*(?:<[^>]+>)?\(\s*([`'"])([\s\S]*?)\1/);
    if (!pathMatch) continue;
    const route = pathMatch[2].trim();
    entries.push({
      client_method: name,
      method: inferMethod(body),
      path: route,
      normalized_path: normalizeRoute(route),
      operation: inferMethod(body) === 'GET' ? 'read' : 'write',
      criticality: criticalMethods[name]?.criticality || 'p2',
      page: criticalMethods[name]?.page || inferPage(name, route),
      quarantine_required: Boolean(criticalMethods[name]?.quarantineRequired),
    });
  }
  return entries;
}

function inferPage(name, route) {
  const text = `${name} ${route}`.toLowerCase();
  for (const page of ['runtime', 'context', 'memory', 'skills', 'agents', 'tools', 'gateway', 'mfg', 'audit', 'settings', 'workspace']) {
    if (text.includes(page)) return page;
  }
  if (text.includes('session') || text.includes('message')) return 'chat';
  if (text.includes('connector') || text.includes('cross-plane') || text.includes('platform')) return 'gateway';
  return 'unknown';
}

function extractUiCalls() {
  const calls = new Map();
  for (const file of sourceDirs.flatMap(walk).filter((item) => /\.(vue|ts)$/.test(item))) {
    const text = read(file);
    const regex = /api\.([A-Za-z0-9_]+)\s*\(/g;
    let match;
    while ((match = regex.exec(text))) {
      const name = match[1];
      if (!calls.has(name)) calls.set(name, []);
      calls.get(name).push(path.relative(repoRoot, file));
    }
  }
  return calls;
}

function extractTestEvidence() {
  const evidence = new Map();
  for (const file of testDirs.flatMap(walk).filter((item) => /\.(test|spec)\.(ts|js)$/.test(item))) {
    const text = read(file);
    const regex = /api\.([A-Za-z0-9_]+)\s*\(/g;
    let match;
    while ((match = regex.exec(text))) {
      const name = match[1];
      if (!evidence.has(name)) evidence.set(name, []);
      evidence.get(name).push(path.relative(repoRoot, file));
    }
  }
  return evidence;
}

function extractRoutes() {
  const routes = [];
  for (const file of routeDirs.flatMap(walk).filter((item) => item.endsWith('.rs'))) {
    const text = read(file);
    const regex = /\.route\(\s*"([^"]+)"\s*,\s*([\s\S]*?)(?=\n\s*\.route\(|\n\s*\))/g;
    let match;
    while ((match = regex.exec(text))) {
      const route = match[1];
      const handlers = match[2];
      for (const method of ['get', 'post', 'put', 'patch', 'delete']) {
        const methodRegex = new RegExp(`\\b${method}\\s*\\(`, 'i');
        if (methodRegex.test(handlers)) {
          routes.push({
            method: method.toUpperCase(),
            path: route,
            normalized_path: normalizeRoute(route),
            file: path.relative(repoRoot, file),
          });
        }
      }
    }
  }
  return routes;
}

function routeMatches(entry, route) {
  if (entry.method !== route.method) return false;
  if (entry.normalized_path === route.normalized_path) return true;
  const a = entry.normalized_path.split('/').filter(Boolean);
  const b = route.normalized_path.split('/').filter(Boolean);
  if (a.length !== b.length) return false;
  return a.every((part, index) => part === b[index] || part === ':param' || b[index] === ':param');
}

function hasQuarantineEvidence(entry) {
  if (!entry.quarantine_required) return true;
  const mfgPath = path.join(webuiRoot, 'src/pages/MfgPage.vue');
  const text = read(mfgPath);
  return text.includes(`data-mfg-risk="${entry.client_method}"`) && text.includes('mfg-live-quarantined');
}

const clientEntries = extractClientMethods();
const uiCalls = extractUiCalls();
const testEvidence = extractTestEvidence();
const routes = extractRoutes();

const entries = clientEntries.map((entry) => {
  const matchedRoutes = routes.filter((route) => routeMatches(entry, route));
  const uiFiles = uiCalls.get(entry.client_method) || [];
  const testFiles = testEvidence.get(entry.client_method) || [];
  return {
    version,
    ...entry,
    ui_files: uiFiles,
    backend_routes: matchedRoutes.map((route) => route.file),
    has_ui_call: uiFiles.length > 0,
    has_backend_route: matchedRoutes.length > 0,
    has_frontend_test: testFiles.length > 0,
    test_files: testFiles,
    quarantine_ok: hasQuarantineEvidence(entry),
  };
});

const blocking = [];
for (const entry of entries.filter((item) => item.criticality === 'p0' || item.criticality === 'p1')) {
  if (!entry.has_ui_call) blocking.push(`${entry.client_method}: missing UI call`);
  if (!entry.has_backend_route) blocking.push(`${entry.client_method}: missing backend route for ${entry.method} ${entry.path}`);
  if (entry.operation === 'write' && !entry.has_frontend_test) blocking.push(`${entry.client_method}: missing frontend request test`);
  if (entry.quarantine_required && !entry.quarantine_ok) blocking.push(`${entry.client_method}: missing MFG temporary quarantine evidence`);
}

fs.mkdirSync(reportDir, { recursive: true });
const matrixPath = path.join(reportDir, `${version}-api-matrix.json`);
const gatePath = path.join(reportDir, `${version}-api-matrix-gate.md`);
fs.writeFileSync(matrixPath, JSON.stringify({
  version,
  generated_at: new Date().toISOString(),
  totals: {
    client_methods: entries.length,
    backend_routes: routes.length,
    blocking: blocking.length,
  },
  entries,
}, null, 2));
fs.writeFileSync(gatePath, [
  `# ${version} API Matrix Gate`,
  '',
  `Generated: ${new Date().toISOString()}`,
  '',
  `Client methods: ${entries.length}`,
  `Backend routes: ${routes.length}`,
  `Blocking findings: ${blocking.length}`,
  '',
  ...(blocking.length ? ['## Blocking', '', ...blocking.map((item) => `- ${item}`)] : ['## Blocking', '', 'None']),
  '',
].join('\n'));

if (blocking.length) {
  console.error(blocking.join('\n'));
  if (gate) process.exit(1);
}

console.log(`API matrix written to ${matrixPath}`);
