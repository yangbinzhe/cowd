import { mount } from '@vue/test-utils';
import { createPinia } from 'pinia';
import { nextTick } from 'vue';
import { createRouter, createWebHashHistory } from 'vue-router';
import { describe, expect, it, vi } from 'vitest';
import App from './App.vue';
import { api } from './api/client';
import ChatPage from './pages/ChatPage.vue';
import AgentsPage from './pages/AgentsPage.vue';
import AuditPage from './pages/AuditPage.vue';
import MemoryPage from './pages/MemoryPage.vue';
import RuntimePage from './pages/RuntimePage.vue';
import ContextPage from './pages/ContextPage.vue';
import GatewayPage from './pages/GatewayPage.vue';
import MfgPage from './pages/MfgPage.vue';
import SettingsPage from './pages/SettingsPage.vue';
import SkillsPage from './pages/SkillsPage.vue';
import ToolsPage from './pages/ToolsPage.vue';
import { pluginRoutes, webuiPagePlugins } from './plugins/registry';
import { useAppStore } from './stores/app';
import mfgWriteContracts from './data/mfgWriteContracts.json';

vi.stubGlobal('fetch', vi.fn(() => Promise.reject(new Error('offline'))));
vi.mock('vue-echarts', () => ({ default: { template: '<div class="chart"></div>' } }));

function mountApp(path = '/chat') {
  const router = createRouter({
    history: createWebHashHistory(),
    routes: [
      { path: '/', redirect: '/chat' },
      { path: '/chat', component: ChatPage },
      { path: '/runtime', component: RuntimePage },
      { path: '/context', component: ContextPage },
      { path: '/memory', component: MemoryPage },
      { path: '/skills', component: SkillsPage },
      { path: '/agents', component: AgentsPage },
      { path: '/tools', component: ToolsPage },
      { path: '/gateway', component: GatewayPage },
      ...pluginRoutes,
      { path: '/mfg', component: MfgPage },
      { path: '/audit', component: AuditPage },
      { path: '/settings', component: SettingsPage },
    ],
  });
  router.push(path);
  return router.isReady().then(() => mount(App, { global: { plugins: [createPinia(), router] } }));
}

async function settle() {
  await Promise.resolve();
  await Promise.resolve();
  await nextTick();
}

async function settleAsync() {
  await settle();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await settle();
}

describe('Cowd Vue WebUI shell', () => {
  it('registers MFG as a pluggable page without a legacy nav item', async () => {
    const plugin = webuiPagePlugins.find((item) => item.id === 'mfg');
    expect(plugin).toBeTruthy();
    expect(plugin?.route).toBe('/apps/mfg');
    expect(plugin?.apiNamespace).toBe('/api/apps/mfg');
    expect(plugin?.requiredCapabilities).toContain('cowd.matrix.runtime');

    const wrapper = await mountApp('/chat');
    const railButtons = wrapper.findAll('.rail-button');
    expect(railButtons.some((button) => button.attributes('aria-label') === 'MFG')).toBe(true);
    const legacyLabel = ['IA', 'CC'].join('');
    expect(railButtons.some((button) => button.attributes('aria-label') === legacyLabel)).toBe(false);
  });

  it('keeps Workspace out of the left rail and inside the right companion panel', async () => {
    const wrapper = await mountApp('/chat');
    const rail = wrapper.get('.rail').text();
    expect(rail).not.toContain('Workspace');
    expect(wrapper.get('.companion-tabs').text()).toContain('Activity');
    expect(wrapper.get('.companion-tabs').text()).toContain('Thinking');
    expect(wrapper.get('.companion-tabs').text()).toContain('Workspace');
    expect(wrapper.get('.companion-tabs').text()).toContain('Inspector');
  });

  it('renders chat, composer, markdown body, and context meter', async () => {
    const wrapper = await mountApp('/chat');
    await settle();
    expect(wrapper.get('.transcript').exists()).toBe(true);
    expect(wrapper.get('.composer textarea').exists()).toBe(true);
    expect(wrapper.get('.context-meter').exists()).toBe(true);
    expect(wrapper.text()).toContain('Context not reported');
    expect(wrapper.get('.chat-page').exists()).toBe(true);
  });

  it('renders Workspace rename controls and Inspector tab from real store state', async () => {
    const wrapper = await mountApp('/chat');
    await settleAsync();
    const store = useAppStore();
    store.workspaceFiles = [{ name: 'a.md', path: 'docs/a.md', kind: 'file' }];
    store.openCompanion('workspace');
    await settle();
    await wrapper.get('button[aria-label="Rename a.md"]').trigger('click');
    await settle();
    expect(wrapper.find('.rename-row').exists()).toBe(true);
    expect(wrapper.find('.rename-row input').element.value).toBe('docs/a.md');
    store.openCompanion('inspector');
    await settle();
    expect(wrapper.text()).toContain('Inspector');
    expect(wrapper.text()).toContain('Context');
  });

  it('renders tools management page with real registry controls', async () => {
    const wrapper = await mountApp('/tools');
    await settle();
    expect(wrapper.text()).toContain('Tools Registry');
    expect(wrapper.findAll('.metric-card').length).toBe(4);
    expect(wrapper.find('.capability-sidebar').exists()).toBe(true);
    expect(wrapper.find('.session-sidebar').exists()).toBe(false);
    expect(wrapper.findAll('.section-row').length).toBe(7);
    expect(wrapper.text()).toContain('Execution planner');
    expect(wrapper.text()).toContain('Mutation transactions');
    expect(wrapper.text()).toContain('Checkpoints');
    expect(wrapper.text()).toContain('Tool cache');
    expect(wrapper.text()).toContain('Tool ledger');
    expect(wrapper.text()).toContain('Risk preflight');
    expect(wrapper.find('.capability-sidebar').text()).not.toContain('Memory');
    expect(wrapper.find('.capability-sidebar').text()).not.toContain('Settings');
  });

  it('calls real tool operation endpoints through the backend', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ ok: true, checkpoints: [] }), { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    await api.toolExecute('tool_cache_stats');
    await api.toolBatchReadonly([{ name: 'tool_cache_stats', input: {} }], 2);
    await api.toolMutationPreview([{ path: 'README.md', old_string: 'A', new_string: 'B' }]);
    await api.toolMutationApply([{ path: 'README.md', old_string: 'A', new_string: 'B' }], { 'README.md': 'hash' });
    await api.toolCheckpoints();
    await api.toolCheckpointCreate('before edit');
    await api.toolCheckpointDiff('cp-1');
    await api.toolCheckpointRestore('cp-1');
    await api.toolIntentPlan('inspect workspace', ['tool_cache_stats']);
    await api.toolContextFanoutPlan('inspect workspace');
    expect(fetchMock).toHaveBeenCalledWith('/api/tools/execute', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/tools/batch-readonly', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/tools/mutations/preview', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/tools/mutations/apply', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/tools/checkpoints', expect.any(Object));
    expect(fetchMock).toHaveBeenCalledWith('/api/tools/checkpoints', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/tools/checkpoints/cp-1/diff', expect.any(Object));
    expect(fetchMock).toHaveBeenCalledWith('/api/tools/checkpoints/cp-1/restore', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/tools/intent-plan', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/tools/context-fanout/plan', expect.objectContaining({ method: 'POST' }));
  });

  it('marks HTML API fallback as offline instead of successful data', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response('<!doctype html><html></html>', {
      status: 200,
      headers: { 'content-type': 'text/html' },
    })));
    vi.stubGlobal('fetch', fetchMock);
    const manifest = await api.health();
    expect(manifest.__offline).toBe(true);
    expect(manifest.__error).toContain('Expected JSON');
  });

  it('uploads files as multipart form data without fake success', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ uploaded: true, path: 'uploads/sample.md' }), { status: 201 })));
    vi.stubGlobal('fetch', fetchMock);
    await api.uploadFile(new File(['# sample'], 'sample.md', { type: 'text/markdown' }), 'uploads');
    expect(fetchMock).toHaveBeenCalledWith('/api/upload', expect.objectContaining({ method: 'POST' }));
    const init = fetchMock.mock.calls[0][1] as RequestInit;
    expect(init.body).toBeInstanceOf(FormData);
    expect(new Headers(init.headers).has('Content-Type')).toBe(false);
  });

  it('adds session attachments through the backend endpoint', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ attachment: { ref_id: 'att-1', path: 'docs/a.md' } }), { status: 201 })));
    vi.stubGlobal('fetch', fetchMock);
    await api.addSessionAttachment('session-1', 'docs/a.md', 'A doc');
    expect(fetchMock).toHaveBeenCalledWith('/api/sessions/session-1/attachments', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ path: 'docs/a.md', label: 'A doc', kind: 'workspace_file' }),
    }));
  });

  it('wraps write failures with endpoint method payload and retry metadata', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response('write failed', { status: 503, statusText: 'Unavailable' })));
    vi.stubGlobal('fetch', fetchMock);
    await expect(api.saveFile('docs/a.md', 'content')).rejects.toMatchObject({
      endpoint: '/api/workspace/files',
      method: 'POST',
      status: 503,
      retryable: true,
    });
    const receipt = await api.writeReceipt('/api/test/write', {
      method: 'POST',
      body: JSON.stringify({ hello: 'world' }),
    });
    expect(receipt.ok).toBe(false);
    expect(receipt.endpoint).toBe('/api/test/write');
    expect(receipt.payload_summary).toContain('hello');
  });

  it('calls critical Workspace write endpoints through the backend', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ ok: true, to: 'docs/b.md' }), { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    await api.saveFile('docs/a.md', 'hello');
    await api.renameWorkspacePath('docs/a.md', 'docs/b.md');
    await api.deleteWorkspacePath('docs/b.md');
    expect(fetchMock).toHaveBeenCalledWith('/api/workspace/files', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ path: 'docs/a.md', content: 'hello' }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/workspace/rename', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ path: 'docs/a.md', to: 'docs/b.md' }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/workspace/files?path=docs%2Fb.md', expect.objectContaining({ method: 'DELETE' }));
  });

  it('calls critical Memory and Skills write endpoints through the backend', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ ok: true }), { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    await api.createMemoryEntry('L2', { title: 'fact' });
    await api.updateMemoryEntry('mem-1', { title: 'updated' });
    await api.deleteMemoryEntry('L2', 'mem-1');
    await api.skillAction('local:test', 'validate', { session_id: 's1' });
    expect(fetchMock).toHaveBeenCalledWith('/api/memory/L2', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ title: 'fact' }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/memory/entry/mem-1', expect.objectContaining({
      method: 'PATCH',
      body: JSON.stringify({ title: 'updated' }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/memory/L2/mem-1', expect.objectContaining({ method: 'DELETE' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/skills/local%3Atest/actions/validate', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ session_id: 's1' }),
    }));
  });

  it('calls critical MFG write endpoints with explicit request bodies', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ ok: true }), { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    await api.mfgSourcePackUpsert({ source_pack_id: 'sp-1' });
    await api.mfgEntityUpsert({ entity_id: 'entity-1' });
    await api.mfgRelationUpsert({ relation_type: 'feeds' });
    await api.mfgComputeJobRun('job-1');
    await api.mfgExecuteAction('analysis-1', 'action-1', { mode: 'dry_run' });
    await api.mfgExecutionBridge('exec-1', { mode: 'dry_run' });
    await api.mfgRetryReportDelivery('report-1', { mode: 'dry_run' });
    await api.mfgIngestFact([{ fact_type: 'quality', source_ref: 'source-pack://sp-1' }]);
    await api.mfgSeedDomain();
    await api.mfgSeedOntology();
    expect(fetchMock).toHaveBeenCalledWith('/api/matrix/source-packs/upsert', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/matrix/entities/upsert', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/matrix/relations/upsert', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/matrix/compute/jobs/job-1/run', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/apps/mfg/analyses/analysis-1/actions/action-1/execute', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/apps/mfg/executions/exec-1/cross-plane/execute', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/apps/mfg/cockpit/reports/report-1/delivery/retry', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/matrix/facts/ingest', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/apps/mfg/domain/server-manufacturing/seed', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/apps/mfg/ontology/server-manufacturing/seed', expect.objectContaining({ method: 'POST' }));
  });

  it('calls real MFG incident and cockpit report endpoints', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ kind: 'test.receipt' }), { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    await api.mfgCreateIncident({ title: 'Line A deviation' });
    await api.mfgAnalyzeIncident('incident-1');
    await api.mfgSkills();
    await api.mfgGenerateReport('profile-1', { cadence: 'daily' });
    expect(fetchMock).toHaveBeenCalledWith('/api/apps/mfg/incidents', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ title: 'Line A deviation' }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/apps/mfg/incidents/incident-1/analyze', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/apps/mfg/skills', expect.any(Object));
    expect(fetchMock).toHaveBeenCalledWith('/api/apps/mfg/cockpit/profiles/profile-1/reports/generate', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ report: { cadence: 'daily' } }),
    }));
  });

  it('renders MFG governed action chains from the write contract source', async () => {
    const fetchMock = vi.fn((path: RequestInfo | URL) => {
      const url = String(path);
      if (url === '/api/webui/manifest') return Promise.resolve(new Response(JSON.stringify({ status: 'test' })));
      if (url.startsWith('/api/sessions?')) return Promise.resolve(new Response(JSON.stringify({ sessions: [] })));
      if (url === '/api/config') return Promise.resolve(new Response(JSON.stringify({ version: 'test' })));
      if (url === '/api/runtime/control-plane') return Promise.resolve(new Response(JSON.stringify({})));
      if (url === '/api/commands') return Promise.resolve(new Response(JSON.stringify({ commands: [] })));
      if (url === '/api/config/providers') return Promise.resolve(new Response(JSON.stringify({ providers: [], models: [] })));
      if (url === '/api/profiles') return Promise.resolve(new Response(JSON.stringify({ profiles: [], active_profile: 'default' })));
      if (url === '/api/workspace') return Promise.resolve(new Response(JSON.stringify({ workspace_root: '', workspace_canonical: '' })));
      if (url === '/api/approval/config') return Promise.resolve(new Response(JSON.stringify({})));
      if (url === '/api/workspace/files') return Promise.resolve(new Response(JSON.stringify({ files: [] })));
      if (url === '/api/matrix/health') return Promise.resolve(new Response(JSON.stringify({ status: 'ready', fact_count: 2, schema_version: 'test' })));
      if (url === '/api/matrix/metrics') return Promise.resolve(new Response(JSON.stringify({ metrics: [{ metric_id: 'torque_deviation_rate', name: 'Torque deviation', unit: '%' }] })));
      if (url === '/api/matrix/entities') return Promise.resolve(new Response(JSON.stringify({ entities: [{ entity_id: 'line-a', entity_type: 'manufacturing_line', canonical_key: 'line:A', display_name: 'Line A' }] })));
      if (url === '/api/apps/mfg/decision-trace') return Promise.resolve(new Response(JSON.stringify({
        kind: 'mfg.decision_trace',
        chain: 'source -> fact -> metric -> evidence -> incident -> action -> report',
        rows: [
          { stage: 'source', ref: 'source-pack://line-a', domain: 'Matrix data plane', signal: 'mes', next: 'fact' },
          { stage: 'fact', ref: 'fact-1', domain: 'cowd structured core', signal: 'manufacturing_quality_event', next: 'metric' },
          { stage: 'action', ref: 'action-1', domain: 'MFG + cross-plane', signal: 'recommended', next: 'report' },
        ],
      })));
      if (url === '/api/apps/mfg/incidents') return Promise.resolve(new Response(JSON.stringify({ items: [{ incident_id: 'incident-1', title: 'Line A deviation' }] })));
      if (url === '/api/apps/mfg/skills') return Promise.resolve(new Response(JSON.stringify({ items: [{ skill_id: 'skill-1', name: 'Root cause analysis' }] })));
      if (url === '/api/apps/mfg/incidents/incident-1/room') return Promise.resolve(new Response(JSON.stringify({ analysis: { recommended_actions: [{ action_id: 'action-1', title: 'Notify QA' }] }, executions: [] })));
      return Promise.resolve(new Response(JSON.stringify({})));
    });
    vi.stubGlobal('fetch', fetchMock);
    const wrapper = await mountApp('/mfg');
    await settleAsync();
    await settleAsync();
    const domains = new Set((mfgWriteContracts as any[]).map((contract) => contract.domain));
    expect(domains).toEqual(new Set(['Cockpit', 'Data Plane', 'Entities', 'Evidence', 'Facts', 'Incidents', 'Metrics']));
    expect(wrapper.findAll('.governed-action-panel').length).toBeGreaterThanOrEqual(7);
    expect(wrapper.text()).toContain('Governed write contracts');
    expect(wrapper.text()).toContain('cowd owns structured data');
    expect(wrapper.text()).toContain('Source pack upsert');
    expect(wrapper.text()).toContain('Manufacturing fact ingest');
    expect(wrapper.text()).toContain('Metric compute run');
    expect(wrapper.text()).toContain('Decision Trace');
    expect(wrapper.text()).toContain('source -> fact -> metric -> evidence -> incident -> action -> report');
    expect(fetchMock).toHaveBeenCalledWith('/api/apps/mfg/decision-trace', expect.any(Object));
    expect(wrapper.text()).toContain('Matrix turns structured manufacturing signals');
    expect(wrapper.text()).toContain('cowd structured core');
    expect(wrapper.text()).toContain('MFG + cross-plane');
  });

  it('loads audit, usage, and release gate from real governance endpoints', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ kind: 'governance.test' }), { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    await api.auditExport('approval', 25, 5);
    await api.usageSummary();
    await api.cowdReleaseGate();
    expect(fetchMock).toHaveBeenCalledWith('/api/audit/export?source=approval&limit=25&offset=5', expect.any(Object));
    expect(fetchMock).toHaveBeenCalledWith('/api/usage', expect.any(Object));
    expect(fetchMock).toHaveBeenCalledWith('/api/cowd/release-gate', expect.any(Object));
  });

  it('verifies auth through the backend instead of showing a fake success', async () => {
    const fetchMock = vi.fn((path: RequestInfo | URL) => {
      const url = String(path);
      if (url === '/api/auth/verify') return Promise.resolve(new Response(JSON.stringify({ authenticated: true, status: 'verified' }), { status: 200 }));
      if (url.startsWith('/api/sessions?')) return Promise.resolve(new Response(JSON.stringify({ sessions: [] })));
      if (url === '/api/config/providers') return Promise.resolve(new Response(JSON.stringify({ providers: [], models: [] })));
      if (url === '/api/profiles') return Promise.resolve(new Response(JSON.stringify({ profiles: [], active_profile: 'default' })));
      if (url === '/api/commands') return Promise.resolve(new Response(JSON.stringify({ commands: [] })));
      if (url === '/api/workspace') return Promise.resolve(new Response(JSON.stringify({ workspace_root: '', workspace_canonical: '' })));
      if (url === '/api/workspace/files') return Promise.resolve(new Response(JSON.stringify({ dir: '', files: [] })));
      return Promise.resolve(new Response(JSON.stringify({})));
    });
    vi.stubGlobal('fetch', fetchMock);
    const wrapper = await mountApp('/settings');
    await settleAsync();
    await wrapper.findAll('button.ghost-action').find((button) => button.text().includes('Verify token'))?.trigger('click');
    await settleAsync();
    expect(fetchMock).toHaveBeenCalledWith('/api/auth/verify', expect.any(Object));
  });

  it('calls real cross-plane identity grant and action endpoints', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ kind: 'cross-plane.test' }), { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    const action = {
      actor_principal: 'webui-operator',
      requested_capability: 'service.read',
      risk: 'medium',
      data_classification: 'internal',
      identity_trust: 'unknown',
    };
    await api.crossPlaneCreateIdentity({ id: 'idb-1', principal_id: 'webui-operator', identity_ref: 'user:webui-operator' });
    await api.crossPlaneCreateGrant({ id: 'grant-1', principal_id: 'webui-operator', capability: 'service.read' });
    await api.crossPlanePolicySimulate(action);
    await api.crossPlaneExecute(action, 'dry_run', 'key-1');
    await api.crossPlaneRevokeIdentity('idb-1');
    await api.crossPlaneRevokeGrant('grant-1');
    expect(fetchMock).toHaveBeenCalledWith('/api/cross-plane/identities', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ id: 'idb-1', principal_id: 'webui-operator', identity_ref: 'user:webui-operator' }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/cross-plane/grants', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ id: 'grant-1', principal_id: 'webui-operator', capability: 'service.read' }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/cross-plane/policy/simulate', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/cross-plane/action/execute', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ action, mode: 'dry_run', idempotency_key: 'key-1' }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/cross-plane/identities/idb-1', expect.objectContaining({ method: 'DELETE' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/cross-plane/grants/grant-1', expect.objectContaining({ method: 'DELETE' }));
  });

  it('loads skill detail and files from real skill management endpoints', async () => {
    const fetchMock = vi.fn((path: RequestInfo | URL) => {
      const url = String(path);
      if (url === '/api/webui/manifest') return Promise.resolve(new Response(JSON.stringify({ status: 'test' })));
      if (url.startsWith('/api/sessions?')) return Promise.resolve(new Response(JSON.stringify({ sessions: [] })));
      if (url === '/api/config') return Promise.resolve(new Response(JSON.stringify({ version: 'test' })));
      if (url === '/api/runtime/control-plane') return Promise.resolve(new Response(JSON.stringify({})));
      if (url === '/api/commands') return Promise.resolve(new Response(JSON.stringify({ commands: [] })));
      if (url === '/api/config/providers') return Promise.resolve(new Response(JSON.stringify({ providers: [], models: [] })));
      if (url === '/api/profiles') return Promise.resolve(new Response(JSON.stringify({ profiles: [], active_profile: 'default' })));
      if (url === '/api/workspace') return Promise.resolve(new Response(JSON.stringify({ workspace_root: '', workspace_canonical: '' })));
      if (url === '/api/approval/config') return Promise.resolve(new Response(JSON.stringify({})));
      if (url === '/api/workspace/files') return Promise.resolve(new Response(JSON.stringify({ files: [] })));
      if (url === '/api/skills/catalog') return Promise.resolve(new Response(JSON.stringify({ items: [{ id: 'local:test', name: 'test', scope: 'local', status: 'ready', risk: 'review', tags: [] }] })));
      if (url === '/api/skills/projection?surface=webui') return Promise.resolve(new Response(JSON.stringify({ facets: { scopes: ['local'], domains: ['test-domain'], tags: ['test-tag'], statuses: ['ready'], risks: ['review'] } })));
      if (url === '/api/skills/runs') return Promise.resolve(new Response(JSON.stringify({ items: [{ run_id: 'run-1', skill_id: 'local:test', status: 'done' }] })));
      if (url === '/api/skills/runs/run-1') return Promise.resolve(new Response(JSON.stringify({ run: { run_id: 'run-1', status: 'done' } })));
      if (url === '/api/skills/local%3Atest') return Promise.resolve(new Response(JSON.stringify({ skill: { id: 'local:test', name: 'test', scope: 'local' } })));
      if (url === '/api/skills/local%3Atest/files') return Promise.resolve(new Response(JSON.stringify({ primary: 'SKILL.md', files: [{ path: 'SKILL.md', kind: 'file', primary: true }] })));
      if (url === '/api/skills/local%3Atest/files/raw?path=SKILL.md') return Promise.resolve(new Response(JSON.stringify({ path: 'SKILL.md', content: '# test' })));
      return Promise.resolve(new Response(JSON.stringify({})));
    });
    vi.stubGlobal('fetch', fetchMock);
    const wrapper = await mountApp('/skills');
    await settleAsync();
    await settleAsync();
    expect(wrapper.text()).toContain('Skills Console');
    expect(wrapper.text()).toContain('SKILL.md');
    expect(wrapper.find('.markdown-body h1').text()).toBe('test');
    await wrapper.find('.run-list article').trigger('click');
    await settleAsync();
    expect(fetchMock).toHaveBeenCalledWith('/api/skills/runs/run-1', expect.any(Object));
    expect(fetchMock).toHaveBeenCalledWith('/api/skills/local%3Atest/files/raw?path=SKILL.md', expect.any(Object));
  });

  it('loads memory graph workbench from real memory and structured-data endpoints', async () => {
    const fetchMock = vi.fn((path: RequestInfo | URL) => {
      const url = String(path);
      if (url === '/api/webui/manifest') return Promise.resolve(new Response(JSON.stringify({ status: 'test' })));
      if (url.startsWith('/api/sessions?')) return Promise.resolve(new Response(JSON.stringify({ sessions: [] })));
      if (url === '/api/config') return Promise.resolve(new Response(JSON.stringify({ version: 'test' })));
      if (url === '/api/runtime/control-plane') return Promise.resolve(new Response(JSON.stringify({})));
      if (url === '/api/commands') return Promise.resolve(new Response(JSON.stringify({ commands: [] })));
      if (url === '/api/config/providers') return Promise.resolve(new Response(JSON.stringify({ providers: [], models: [] })));
      if (url === '/api/profiles') return Promise.resolve(new Response(JSON.stringify({ profiles: [], active_profile: 'default' })));
      if (url === '/api/workspace') return Promise.resolve(new Response(JSON.stringify({ workspace_root: '', workspace_canonical: '' })));
      if (url === '/api/approval/config') return Promise.resolve(new Response(JSON.stringify({})));
      if (url === '/api/workspace/files') return Promise.resolve(new Response(JSON.stringify({ files: [] })));
      if (url === '/api/memory/status') return Promise.resolve(new Response(JSON.stringify({ enabled: true, status: 'ready', kernel_health: { degraded: false } })));
      if (url === '/api/memory/stats') return Promise.resolve(new Response(JSON.stringify({ total_entries: 1, entity_count: 1, triple_count: 1, vector_count: 1 })));
      if (url === '/api/memory/layers') return Promise.resolve(new Response(JSON.stringify({ layers: [{ layer: 'L2', entry_count: 1 }] })));
      if (url === '/api/memory/L2') return Promise.resolve(new Response(JSON.stringify({ enabled: true, entries: [{ id: 'mem-1', title: 'Line A fact', content: 'Torque deviation', tags: ['quality'], priority: 'High' }] })));
      if (url.startsWith('/api/memory/search')) return Promise.resolve(new Response(JSON.stringify({ results: [{ id: 'mem-1' }] })));
      if (url.startsWith('/api/memory/recall/explain')) return Promise.resolve(new Response(JSON.stringify({ total: 1, results: [{ id: 'mem-1', title: 'Line A fact', source_layer: 'L2', priority: 'High', score: 1, snippet: 'Torque deviation' }] })));
      if (url.startsWith('/api/memory/packet')) return Promise.resolve(new Response(JSON.stringify({ packet: { items: ['mem-1'] } })));
      if (url === '/api/memory/links') return Promise.resolve(new Response(JSON.stringify({ total: 1, links: [] })));
      if (url.startsWith('/api/memory/clusters')) return Promise.resolve(new Response(JSON.stringify({ clusters: [] })));
      if (url === '/api/memory/entities') return Promise.resolve(new Response(JSON.stringify({ entities: [{ id: 'line-a', name: 'Line A' }] })));
      if (url === '/api/memory/triples') return Promise.resolve(new Response(JSON.stringify({ triples: [{ subject: 'line-a', predicate: 'has_issue', object: 'torque' }] })));
      if (url.startsWith('/api/memory/symbol-links')) return Promise.resolve(new Response(JSON.stringify({ entries: [] })));
      if (url.startsWith('/api/memory/maintenance')) return Promise.resolve(new Response(JSON.stringify({ candidates: [] })));
      if (url === '/api/memory/performance') return Promise.resolve(new Response(JSON.stringify({ latency_ms: 2 })));
      if (url === '/api/memory/runtime') return Promise.resolve(new Response(JSON.stringify({ runtime: { active: true } })));
      if (url.startsWith('/api/cowd/structured/')) return Promise.resolve(new Response(JSON.stringify({ items: [] })));
      return Promise.resolve(new Response(JSON.stringify({})));
    });
    vi.stubGlobal('fetch', fetchMock);
    const wrapper = await mountApp('/memory');
    await settleAsync();
    await settleAsync();
    expect(wrapper.text()).toContain('Memory Graph');
    expect(wrapper.text()).toContain('Layer entries');
    expect(wrapper.text()).toContain('Line A fact');
    expect(wrapper.text()).toContain('Structured data core');
    expect(fetchMock).toHaveBeenCalledWith('/api/memory/recall/explain?q=manufacturing%20quality%20anomaly&limit=12', expect.any(Object));
    expect(fetchMock).toHaveBeenCalledWith('/api/cowd/structured/sources', expect.any(Object));
  });

  it('loads agents workbench from real agent and task endpoints', async () => {
    const fetchMock = vi.fn((path: RequestInfo | URL) => {
      const url = String(path);
      if (url === '/api/webui/manifest') return Promise.resolve(new Response(JSON.stringify({ status: 'test' })));
      if (url.startsWith('/api/sessions?')) return Promise.resolve(new Response(JSON.stringify({ sessions: [] })));
      if (url === '/api/config') return Promise.resolve(new Response(JSON.stringify({ version: 'test' })));
      if (url === '/api/runtime/control-plane') return Promise.resolve(new Response(JSON.stringify({})));
      if (url === '/api/commands') return Promise.resolve(new Response(JSON.stringify({ commands: [] })));
      if (url === '/api/config/providers') return Promise.resolve(new Response(JSON.stringify({ providers: [], models: [] })));
      if (url === '/api/profiles') return Promise.resolve(new Response(JSON.stringify({ profiles: [], active_profile: 'default' })));
      if (url === '/api/workspace') return Promise.resolve(new Response(JSON.stringify({ workspace_root: '', workspace_canonical: '' })));
      if (url === '/api/approval/config') return Promise.resolve(new Response(JSON.stringify({})));
      if (url === '/api/workspace/files') return Promise.resolve(new Response(JSON.stringify({ files: [] })));
      if (url === '/api/agents/catalog') return Promise.resolve(new Response(JSON.stringify({ summary: { total: 1, active: 1 }, agents: [{ name: 'planner', active: true, source: { id: 'project_cowd' }, description: 'Plans work' }] })));
      if (url === '/api/agents/directory') return Promise.resolve(new Response(JSON.stringify({ summary: { total: 1, active: 1 }, agents: [{ name: 'planner', active: true, source: { id: 'project_cowd' }, description: 'Plans work' }] })));
      if (url === '/api/agents/reputation') return Promise.resolve(new Response(JSON.stringify({ items: [{ agent_id: 'planner', reputation: 91, status: 'active' }] })));
      if (url === '/api/agents/runs') return Promise.resolve(new Response(JSON.stringify({ runs: [{ graph_id: 'agent-graph-task-1' }] })));
      if (url === '/api/tasks') return Promise.resolve(new Response(JSON.stringify({ current: { id: 'task-1', objective: 'Ship UI', status: 'open', phases: [] }, tasks: [{ id: 'task-1', objective: 'Ship UI', status: 'open', phases: [] }] })));
      if (url === '/api/tasks/task-1/agent-graph') return Promise.resolve(new Response(JSON.stringify({ status: 'running', nodes: [{ id: 'planner', title: 'Plan', role: 'planner', status: 'ready', objective: 'Ship UI', depends_on: [] }] })));
      return Promise.resolve(new Response(JSON.stringify({})));
    });
    vi.stubGlobal('fetch', fetchMock);
    const wrapper = await mountApp('/agents');
    await settleAsync();
    await settleAsync();
    expect(wrapper.text()).toContain('Agents Workbench');
    expect(wrapper.text()).toContain('Agent directory');
    expect(wrapper.text()).toContain('Discover team');
    expect(wrapper.text()).toContain('Task control');
    expect(wrapper.text()).toContain('Agent execution graph');
    expect(fetchMock).toHaveBeenCalledWith('/api/agents/catalog', expect.any(Object));
    expect(fetchMock).toHaveBeenCalledWith('/api/agents/directory', expect.any(Object));
    expect(fetchMock).toHaveBeenCalledWith('/api/agents/reputation', expect.any(Object));
    expect(fetchMock).toHaveBeenCalledWith('/api/tasks/task-1/agent-graph', expect.any(Object));
  });

  it('posts agent assemble requests through the backend contract', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ kind: 'agents.assemble', team: {} }), { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    await api.agentAssemble('build a review team');
    await api.createAgentTeamProfile({ id: 'qa-team', name: 'QA Team', members: ['planner'] });
    await api.updateAgentTeamProfile('qa-team', { name: 'QA Team', members: ['planner', 'reviewer'] });
    await api.deleteAgentTeamProfile('qa-team');
    expect(fetchMock).toHaveBeenCalledWith('/api/agents/assemble', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ task: 'build a review team' }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/agents/team-profiles', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ id: 'qa-team', name: 'QA Team', members: ['planner'] }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/agents/team-profiles/qa-team', expect.objectContaining({
      method: 'PUT',
      body: JSON.stringify({ name: 'QA Team', members: ['planner', 'reviewer'] }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/agents/team-profiles/qa-team', expect.objectContaining({ method: 'DELETE' }));
  });
});
