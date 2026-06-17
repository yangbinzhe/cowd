#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const repoRoot = path.resolve(new URL('../../', import.meta.url).pathname);
const webuiRoot = path.join(repoRoot, 'webui');
const planRoot = process.env.COWD_PLAN_ROOT || path.resolve(repoRoot, '../plan/0617-最终目标收口');
const reportDir = path.join(planRoot, 'reports');
const version = process.env.COWD_VERSION || 'v0.9.245';
const gate = process.argv.includes('--gate');

const allowTitleTerms = [
  'action',
  'audit',
  'collections',
  'config',
  'context',
  'detail',
  'evidence',
  'gateway',
  'ingest',
  'lease',
  'payload',
  'performance',
  'plan',
  'platform',
  'registry',
  'resolved',
  'result',
  'run',
  'summary',
];

function walk(target) {
  if (!fs.existsSync(target)) return [];
  const stat = fs.statSync(target);
  if (stat.isFile()) return [target];
  return fs.readdirSync(target).flatMap((entry) => walk(path.join(target, entry)));
}

function lineOf(text, index) {
  return text.slice(0, index).split('\n').length;
}

function titleOf(tag) {
  return tag.match(/title=(?:"([^"]+)"|'([^']+)'|{`([^`]+)`})/)?.slice(1).find(Boolean) || '';
}

const files = walk(path.join(webuiRoot, 'src')).filter((file) => /\.(vue|ts)$/.test(file));
const entries = [];
const failures = [];
const pageRequirements = [
  {
    file: 'webui/src/pages/MfgPage.vue',
    terms: ['Decision Trace', 'source -> fact -> action', 'Matrix turns structured manufacturing signals', 'api.mfgDecisionTrace', '<DataTable :rows="decisionTraceRows"'],
  },
  {
    file: 'webui/src/pages/MemoryPage.vue',
    terms: ['Structured data core', 'api.structuredSources()', 'api.structuredFacts()', 'api.structuredEvidence()', 'api.structuredWatermarks()'],
  },
  {
    file: 'webui/src/pages/ToolsPage.vue',
    terms: ['Tool registry', 'Execution planner', 'Mutation transactions', 'Checkpoints', 'Tool cache', 'Tool ledger', 'Risk preflight'],
  },
  {
    file: 'webui/src/pages/SkillsPage.vue',
    terms: ['api.skillCatalog()', 'api.skillProjection()', 'api.skillRuns()', 'api.skillDetail', 'api.skillFiles', 'Runs and governance'],
  },
  {
    file: 'webui/src/pages/AgentsPage.vue',
    terms: ['Team profiles', 'Evaluation JSON', 'api.agentTeamProfiles()', 'api.agentAssemble', 'api.agentRuns()', 'api.taskAgentGraph'],
  },
  {
    file: 'webui/src/pages/SettingsPage.vue',
    terms: ['Model count', 'Profiles', 'Approval policy', 'Auth token', 'Appearance'],
  },
];

for (const file of files) {
  const text = fs.readFileSync(file, 'utf8');
  const regex = /<RawPayload\b[^>]*\/?>/g;
  let match;
  while ((match = regex.exec(text))) {
    const tag = match[0];
    const line = lineOf(text, match.index);
    const title = titleOf(tag);
    const before = text.slice(Math.max(0, match.index - 800), match.index);
    const nearestSection = before.match(/<h[23][^>]*>([^<]+)<\/h[23]>/g)?.pop()?.replace(/<[^>]+>/g, '') || '';
    const normalizedTitle = title.toLowerCase();
    const allowedByTitle = title && allowTitleTerms.some((term) => normalizedTitle.includes(term));
    const hasManagementCompanion = /DataTable|DetailPanel|RequestReceipt|GovernedActionPanel|EndpointHealthList|TimelineList/.test(before);
    const entry = {
      file: path.relative(repoRoot, file),
      line,
      title: title || null,
      nearest_section: nearestSection || null,
      status: allowedByTitle || hasManagementCompanion ? 'pass' : 'review',
      evidence_role: title ? 'named detail/debug payload' : 'unnamed fallback payload',
    };
    entries.push(entry);
    if (!title) failures.push(`${entry.file}:${line} RawPayload missing title`);
    if (!allowedByTitle && !hasManagementCompanion) {
      failures.push(`${entry.file}:${line} RawPayload lacks evidence/debug title or nearby management component`);
    }
  }
}

for (const requirement of pageRequirements) {
  const file = path.join(repoRoot, requirement.file);
  const text = fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : '';
  if (!text) {
    failures.push(`${requirement.file}: missing page for structured primary view audit`);
    continue;
  }
  for (const term of requirement.terms) {
    if (!text.includes(term)) failures.push(`${requirement.file}: missing structured primary view term ${term}`);
  }
}

const report = {
  version,
  generated_at: new Date().toISOString(),
  status: failures.length ? 'fail' : 'pass',
  policy: 'Raw JSON is allowed only as named evidence, debug, detail, payload, result, or audit drill-down; primary management views must use structured UI.',
  page_requirements: pageRequirements,
  totals: {
    raw_payload_instances: entries.length,
    failures: failures.length,
  },
  entries,
  failures,
};

fs.mkdirSync(reportDir, { recursive: true });
const reportPath = path.join(reportDir, `${version}-raw-payload-audit.json`);
fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);

if (failures.length) {
  console.error(`RawPayload audit failed:\n${failures.map((item) => `- ${item}`).join('\n')}`);
  if (gate) process.exit(1);
}

console.log(`RawPayload audit written to ${reportPath}`);
