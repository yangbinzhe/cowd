import { mount } from '@vue/test-utils';
import { createPinia } from 'pinia';
import { nextTick } from 'vue';
import { createRouter, createWebHashHistory } from 'vue-router';
import { describe, expect, it, vi } from 'vitest';
import App from './App.vue';
import { api } from './api/client';
import ChatPage from './pages/ChatPage.vue';
import CapabilityPage from './pages/CapabilityPage.vue';
import SettingsPage from './pages/SettingsPage.vue';

vi.stubGlobal('fetch', vi.fn(() => Promise.reject(new Error('offline'))));
vi.mock('vue-echarts', () => ({ default: { template: '<div class="chart"></div>' } }));

function mountApp(path = '/chat') {
  const router = createRouter({
    history: createWebHashHistory(),
    routes: [
      { path: '/', redirect: '/chat' },
      { path: '/chat', component: ChatPage },
      { path: '/runtime', component: CapabilityPage, props: { page: 'runtime' } },
      { path: '/context', component: CapabilityPage, props: { page: 'context' } },
      { path: '/memory', component: CapabilityPage, props: { page: 'memory' } },
      { path: '/skills', component: CapabilityPage, props: { page: 'skills' } },
      { path: '/agents', component: CapabilityPage, props: { page: 'agents' } },
      { path: '/tools', component: CapabilityPage, props: { page: 'tools' } },
      { path: '/gateway', component: CapabilityPage, props: { page: 'gateway' } },
      { path: '/iacc', component: CapabilityPage, props: { page: 'iacc' } },
      { path: '/audit', component: CapabilityPage, props: { page: 'audit' } },
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

  it('renders capability pages with module sections instead of duplicated primary navigation', async () => {
    const wrapper = await mountApp('/runtime');
    await settle();
    expect(wrapper.text()).toContain('Runtime Control');
    expect(wrapper.findAll('.metric-card').length).toBe(3);
    expect(wrapper.find('.chart-panel').exists()).toBe(true);
    expect(wrapper.find('.work-table table').exists()).toBe(true);
    expect(wrapper.find('.capability-sidebar').exists()).toBe(true);
    expect(wrapper.find('.session-sidebar').exists()).toBe(false);
    expect(wrapper.findAll('.section-row').length).toBe(4);
    expect(wrapper.find('.capability-sidebar').text()).not.toContain('Memory');
    expect(wrapper.find('.capability-sidebar').text()).not.toContain('Settings');
    expect(wrapper.findAll('.action-button').length).toBe(0);
    expect(wrapper.text()).toContain('Live endpoint contract');
    expect(wrapper.text()).toContain('Offline/Error');
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
});
