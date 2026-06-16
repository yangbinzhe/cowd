import { createApp } from 'vue';
import { createPinia } from 'pinia';
import { createRouter, createWebHashHistory } from 'vue-router';
import App from './App.vue';
import ChatPage from './pages/ChatPage.vue';
import CapabilityPage from './pages/CapabilityPage.vue';
import SettingsPage from './pages/SettingsPage.vue';
import './styles/tokens.css';
import './styles/base.css';

const routes = [
  { path: '/', redirect: '/chat' },
  { path: '/chat', component: ChatPage, meta: { label: 'Chat' } },
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
];

const router = createRouter({
  history: createWebHashHistory(),
  routes,
});

createApp(App).use(createPinia()).use(router).mount('#app');
