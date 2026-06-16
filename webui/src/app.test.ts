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
import IaccPage from './pages/IaccPage.vue';
import SettingsPage from './pages/SettingsPage.vue';
import SkillsPage from './pages/SkillsPage.vue';
import ToolsPage from './pages/ToolsPage.vue';

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
      { path: '/iacc', component: IaccPage },
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
  it('keeps Workspace out of the left rail and inside the right companion panel', async () => {
    const wrapper = await mountApp('/chat');
    const rail = wrapper.get('.rail').text();
    expect(rail).not.toContain('Workspace');
    expect(wrapper.get('.companion-tabs').text()).toContain('Activity');
    expect(wrapper.get('.companion-tabs').text()).toContain('Workspace');
  });

  it('renders chat, composer, markdown body, and context meter', async () => {
    const wrapper = await mountApp('/chat');
    await settle();
    expect(wrapper.get('.transcript').exists()).toBe(true);
    expect(wrapper.get('.composer textarea').exists()).toBe(true);
    expect(wrapper.get('.context-meter').exists()).toBe(true);
    expect(wrapper.get('.chat-page').exists()).toBe(true);
  });

  it('renders tools management page with real registry controls', async () => {
    const wrapper = await mountApp('/tools');
    await settle();
    expect(wrapper.text()).toContain('Tools Registry');
    expect(wrapper.findAll('.metric-card').length).toBe(3);
    expect(wrapper.find('.capability-sidebar').exists()).toBe(true);
    expect(wrapper.find('.session-sidebar').exists()).toBe(false);
    expect(wrapper.text()).toContain('Command execution');
    expect(wrapper.text()).toContain('Risk preflight');
    expect(wrapper.text()).toContain('Command and risk history');
    expect(wrapper.find('.capability-sidebar').text()).not.toContain('Memory');
    expect(wrapper.find('.capability-sidebar').text()).not.toContain('Settings');
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

  it('calls critical IACC write endpoints with explicit request bodies', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ ok: true }), { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    await api.iaccSourcePackUpsert({ source_pack_id: 'sp-1' });
    await api.iaccEntityUpsert({ entity_id: 'entity-1' });
    await api.iaccRelationUpsert({ relation_type: 'feeds' });
    await api.iaccComputeJobRun('job-1');
    await api.iaccExecuteAction('analysis-1', 'action-1', { mode: 'dry_run' });
    await api.iaccExecutionBridge('exec-1', { mode: 'dry_run' });
    await api.iaccRetryReportDelivery('report-1', { mode: 'dry_run' });
    await api.iaccIngestFact([{ fact_type: 'quality', source_ref: 'source-pack://sp-1' }]);
    await api.iaccSeedDomain();
    await api.iaccSeedOntology();
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/source-packs/upsert', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/entities/upsert', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/relations/upsert', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/compute/jobs/job-1/run', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/analyses/analysis-1/actions/action-1/execute', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/executions/exec-1/cross-plane/execute', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/cockpit/reports/report-1/delivery/retry', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/facts/ingest', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/domain/server-manufacturing/seed', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/ontology/server-manufacturing/seed', expect.objectContaining({ method: 'POST' }));
  });

  it('calls real IACC incident and cockpit report endpoints', async () => {
    const fetchMock = vi.fn(() => Promise.resolve(new Response(JSON.stringify({ kind: 'test.receipt' }), { status: 200 })));
    vi.stubGlobal('fetch', fetchMock);
    await api.iaccCreateIncident({ title: 'Line A deviation' });
    await api.iaccAnalyzeIncident('incident-1');
    await api.iaccSkills();
    await api.iaccGenerateReport('profile-1', { cadence: 'daily' });
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/incidents', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ title: 'Line A deviation' }),
    }));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/incidents/incident-1/analyze', expect.objectContaining({ method: 'POST' }));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/skills', expect.any(Object));
    expect(fetchMock).toHaveBeenCalledWith('/api/iacc/cockpit/profiles/profile-1/reports/generate', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ report: { cadence: 'daily' } }),
    }));
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
    expect(fetchMock).toHaveBeenCalledWith('/api/agents/assemble', expect.objectContaining({
      method: 'POST',
      body: JSON.stringify({ task: 'build a review team' }),
    }));
  });
});
