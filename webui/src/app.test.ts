import { mount } from '@vue/test-utils';
import { createPinia } from 'pinia';
import { nextTick } from 'vue';
import { createRouter, createWebHashHistory } from 'vue-router';
import { describe, expect, it, vi } from 'vitest';
import App from './App.vue';
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

  it('renders capability pages with metrics, charts, and tables', async () => {
    const wrapper = await mountApp('/runtime');
    await settle();
    expect(wrapper.text()).toContain('Runtime Control');
    expect(wrapper.findAll('.metric-card').length).toBe(3);
    expect(wrapper.find('.chart-panel').exists()).toBe(true);
    expect(wrapper.find('.work-table table').exists()).toBe(true);
    expect(wrapper.find('.capability-sidebar').exists()).toBe(true);
    expect(wrapper.find('.session-sidebar').exists()).toBe(false);
    expect(wrapper.findAll('.section-row').length).toBeGreaterThan(1);
    expect(wrapper.findAll('.action-button').length).toBeGreaterThan(1);
  });
});
