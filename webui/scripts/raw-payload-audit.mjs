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

const report = {
  version,
  generated_at: new Date().toISOString(),
  status: failures.length ? 'fail' : 'pass',
  policy: 'Raw JSON is allowed only as named evidence, debug, detail, payload, result, or audit drill-down; primary management views must use structured UI.',
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
