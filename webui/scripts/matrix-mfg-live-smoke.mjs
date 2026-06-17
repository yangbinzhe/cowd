#!/usr/bin/env node
import fs from 'node:fs';
import path from 'node:path';
import process from 'node:process';

const repoRoot = path.resolve(new URL('../../', import.meta.url).pathname);
const planRoot = process.env.COWD_PLAN_ROOT || path.resolve(repoRoot, '../plan/0617-最终目标收口');
const reportDir = path.join(planRoot, 'reports');
const version = process.env.COWD_VERSION || 'v0.9.276';
const baseUrl = process.env.COWD_GATEWAY_URL || 'http://127.0.0.1:8642';

function readTokenFromConfig() {
  const config = path.join(process.env.HOME || '', '.cowd/config.yaml');
  if (!fs.existsSync(config)) return '';
  const lines = fs.readFileSync(config, 'utf8').split(/\r?\n/);
  let inApiServer = false;
  for (const line of lines) {
    if (/platformType:\s*api_server/.test(line)) inApiServer = true;
    if (inApiServer && /^\s*token:\s*/.test(line)) {
      return line.replace(/^\s*token:\s*/, '').trim().replace(/^["']|["']$/g, '');
    }
    if (inApiServer && /^\s*-\s*platformType:/.test(line) && !/api_server/.test(line)) inApiServer = false;
  }
  return '';
}

const token = process.env.COWD_API_TOKEN || process.env.COWD_AUTH_TOKEN || readTokenFromConfig();
const headers = {
  'content-type': 'application/json',
  ...(token ? { authorization: `Bearer ${token}` } : {}),
};

async function request(step, method, route, body, { allowDegraded = false } = {}) {
  const started = Date.now();
  try {
    const response = await fetch(`${baseUrl}${route}`, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    const text = await response.text();
    let data = {};
    try {
      data = text ? JSON.parse(text) : {};
    } catch {
      data = { raw: text.slice(0, 500) };
    }
    const ok = response.ok;
    return {
      step,
      method,
      route,
      status: ok ? 'pass' : allowDegraded ? 'degraded' : 'fail',
      http_status: response.status,
      duration_ms: Date.now() - started,
      summary: summarize(data),
      data,
    };
  } catch (error) {
    return {
      step,
      method,
      route,
      status: allowDegraded ? 'degraded' : 'fail',
      http_status: 0,
      duration_ms: Date.now() - started,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}

function summarize(data) {
  if (!data || typeof data !== 'object') return String(data ?? '');
  const keys = ['kind', 'status', 'app_id', 'name', 'batch_id', 'job_id', 'incident_id', 'analysis_id', 'request_id', 'report_id'];
  const picked = {};
  for (const key of keys) {
    if (data[key] !== undefined) picked[key] = data[key];
    if (data.health?.[key] !== undefined) picked[`health.${key}`] = data.health[key];
    if (data.plan?.[key] !== undefined) picked[`plan.${key}`] = data.plan[key];
    if (data.job?.[key] !== undefined) picked[`job.${key}`] = data.job[key];
    if (data.packet?.[key] !== undefined) picked[`packet.${key}`] = data.packet[key];
    if (data.incident?.[key] !== undefined) picked[`incident.${key}`] = data.incident[key];
    if (data.analysis?.[key] !== undefined) picked[`analysis.${key}`] = data.analysis[key];
    if (data.report?.[key] !== undefined) picked[`report.${key}`] = data.report[key];
    if (data.delivery_state?.[key] !== undefined) picked[`delivery_state.${key}`] = data.delivery_state[key];
  }
  return Object.keys(picked).length ? picked : Object.fromEntries(Object.entries(data).slice(0, 5));
}

const stamp = new Date().toISOString().replace(/[:.]/g, '-');
const sourceRef = `source-pack://webui-live-smoke-${stamp}`;
const factType = 'manufacturing_quality_event';
const metricId = 'torque_deviation_rate';

const steps = [];
steps.push(await request('gateway health', 'GET', '/health'));
steps.push(await request('matrix health', 'GET', '/api/matrix/health'));
steps.push(await request('mfg app descriptor', 'GET', '/api/apps/mfg/app'));
steps.push(await request('structured ingest plan', 'POST', '/api/cowd/structured/ingest-plan', {
  source_ref: sourceRef,
  fact_type: factType,
  estimated_rows: 16,
  raw_checksum: `sha256:${stamp}`,
  metric_ids: [metricId],
}));
steps.push(await request('matrix data-plane ingest plan', 'POST', '/api/matrix/data-plane/ingest-plan', {
  ingest: {
    source_ref: sourceRef,
    fact_type: factType,
    partition_ref: 'line:A',
    estimated_rows: 16,
    raw_checksum: `sha256:${stamp}`,
    metric_ids: [metricId],
  },
}));
steps.push(await request('metric attention plan', 'POST', '/api/matrix/metrics/attention-plan', {
  trigger_fact_type: factType,
  entity_scope: 'line:A',
  period: 'latest',
  limit: 5,
}));
steps.push(await request('compute job plan', 'POST', '/api/matrix/compute/jobs/plan', {
  job: {
    trigger_fact_type: factType,
    trigger_fact_refs: [],
    entity_scope: 'line:A',
    period: 'latest',
    metric_ids: [metricId],
    priority: 0.7,
  },
}));
const evidence = await request('evidence build', 'POST', '/api/matrix/evidence/build', {
  problem_statement: 'Live smoke validates Matrix to MFG decision trace.',
}, { allowDegraded: true });
steps.push(evidence);
const evidencePacketId = evidence.data?.packet?.packet_id || evidence.data?.evidence_packet?.packet_id;
const incident = await request('incident create', 'POST', '/api/apps/mfg/incidents', {
  title: 'Live smoke Matrix/MFG decision trace',
  evidence_packet_id: evidencePacketId,
}, { allowDegraded: true });
steps.push(incident);
const incidentId = incident.data?.incident?.incident_id || incident.data?.incident_id;
if (incidentId) {
  steps.push(await request('incident analyze', 'POST', `/api/apps/mfg/incidents/${encodeURIComponent(incidentId)}/analyze`, {}, { allowDegraded: true }));
  steps.push(await request('incident room', 'GET', `/api/apps/mfg/incidents/${encodeURIComponent(incidentId)}/room`, undefined, { allowDegraded: true }));
} else {
  steps.push({
    step: 'incident analyze',
    method: 'POST',
    route: '/api/apps/mfg/incidents/:id/analyze',
    status: 'degraded',
    http_status: 0,
    summary: 'skipped because incident create did not return an incident id',
  });
}
const profileId = `live-smoke-profile-${stamp}`;
steps.push(await request('cockpit profile upsert', 'POST', '/api/apps/mfg/cockpit/profiles/upsert', {
  profile: {
    profile_id: profileId,
    owner_ref: 'user:live-smoke',
    display_name: 'Live Smoke Manufacturing Cockpit',
    focus_refs: ['line:A'],
    focus_metric_ids: [metricId],
    thresholds: { [metricId]: 0.08 },
    cadence: 'daily',
  },
}));
const reportStep = await request(
  'cockpit report generate',
  'POST',
  `/api/apps/mfg/cockpit/profiles/${encodeURIComponent(profileId)}/reports/generate`,
  {
    report: {
      report_id: `live-smoke-report-${stamp}`,
      cadence: 'daily',
      delivery_ref: 'channel://webui/live-smoke',
      note: 'generated by Matrix/MFG live smoke',
    },
  },
);
steps.push(reportStep);
const reportId = reportStep.data?.report?.report_id || reportStep.data?.report_id;
if (reportId) {
  steps.push(await request('cockpit delivery state probe', 'GET', `/api/apps/mfg/cockpit/reports/${encodeURIComponent(reportId)}/delivery-state`));
  steps.push(await request('decision trace aggregate', 'GET', `/api/apps/mfg/decision-trace?${new URLSearchParams({
    ...(incidentId ? { incident_id: incidentId } : {}),
    report_id: reportId,
  }).toString()}`));
} else {
  steps.push({
    step: 'cockpit delivery state probe',
    method: 'GET',
    route: '/api/apps/mfg/cockpit/reports/:id/delivery-state',
    status: 'fail',
    http_status: 0,
    summary: 'skipped because report generate did not return a report id',
  });
  steps.push({
    step: 'decision trace aggregate',
    method: 'GET',
    route: '/api/apps/mfg/decision-trace',
    status: 'fail',
    http_status: 0,
    summary: 'skipped because report generate did not return a report id',
  });
}

const failed = steps.filter((step) => step.status === 'fail');
const degraded = steps.filter((step) => step.status === 'degraded');
const report = {
  version,
  generated_at: new Date().toISOString(),
  base_url: baseUrl,
  status: failed.length ? 'fail' : 'pass',
  degraded_count: degraded.length,
  failed_count: failed.length,
  decision_chain: [
    'gateway health',
    'matrix health',
    'mfg app descriptor',
    'structured ingest plan',
    'matrix data-plane ingest plan',
    'metric attention plan',
    'compute job plan',
    'evidence build',
    'incident create',
    'incident analyze',
    'cockpit profile upsert',
    'cockpit report generate',
    'cockpit delivery state probe',
    'decision trace aggregate',
  ],
  steps,
};

fs.mkdirSync(reportDir, { recursive: true });
const reportPath = path.join(reportDir, `${version}-matrix-mfg-live-smoke.json`);
fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`);

if (failed.length) {
  console.error(`Matrix/MFG live smoke failed. Report: ${reportPath}`);
  for (const step of failed) console.error(`- ${step.step}: ${step.http_status} ${step.error || JSON.stringify(step.summary)}`);
  process.exit(1);
}

console.log(`Matrix/MFG live smoke passed with ${degraded.length} degraded step(s): ${reportPath}`);
