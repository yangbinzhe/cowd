import { createApp } from 'vue';
import { createPinia } from 'pinia';
import { createRouter, createWebHashHistory } from 'vue-router';
import App from './App.vue';
import ChatPage from './pages/ChatPage.vue';
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
  { path: '/tools', component: () => import('./pages/CapabilityPage.vue'), props: { page: 'tools' } },
  { path: '/gateway', component: () => import('./pages/GatewayPage.vue') },
  { path: '/iacc', component: () => import('./pages/IaccPage.vue') },
  { path: '/audit', component: () => import('./pages/CapabilityPage.vue'), props: { page: 'audit' } },
  { path: '/settings', component: () => import('./pages/SettingsPage.vue') },
];

const router = createRouter({
  history: createWebHashHistory(),
  routes,
});

createApp(App).use(createPinia()).use(router).mount('#app');
