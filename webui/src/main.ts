import { createApp } from 'vue';
import { createPinia } from 'pinia';
import { createRouter, createWebHashHistory } from 'vue-router';
import App from './App.vue';
import ChatPage from './pages/ChatPage.vue';
import { pluginRoutes } from './plugins/registry';
import './styles/tokens.css';
import './styles/base.css';

const routes = [
  { path: '/', redirect: '/chat' },
  { path: '/chat', component: ChatPage, meta: { label: 'Chat' } },
  { path: '/runtime', component: () => import('./pages/RuntimePage.vue') },
  { path: '/context', component: () => import('./pages/ContextPage.vue') },
  { path: '/memory', component: () => import('./pages/MemoryPage.vue') },
  { path: '/skills', component: () => import('./pages/SkillsPage.vue') },
  { path: '/agents', component: () => import('./pages/AgentsPage.vue') },
  { path: '/tools', component: () => import('./pages/ToolsPage.vue') },
  { path: '/gateway', component: () => import('./pages/GatewayPage.vue') },
  ...pluginRoutes,
  { path: '/mfg', redirect: '/apps/mfg' },
  { path: '/audit', component: () => import('./pages/AuditPage.vue') },
  { path: '/settings', component: () => import('./pages/SettingsPage.vue') },
];

const router = createRouter({
  history: createWebHashHistory(),
  routes,
});

createApp(App).use(createPinia()).use(router).mount('#app');
