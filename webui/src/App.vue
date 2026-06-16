<script setup lang="ts">
import { onMounted } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import {
  Activity, Brain, Boxes, ClipboardCheck, Factory, Layers, MessageSquare,
  Network, RadioTower, Settings, Wrench,
} from 'lucide-vue-next';
import { useAppStore } from './stores/app';
import type { NavItem } from './types';
import CompanionPanel from './components/CompanionPanel.vue';
import SessionSidebar from './components/SessionSidebar.vue';

const store = useAppStore();
const route = useRoute();
const router = useRouter();

const nav: NavItem[] = [
  { id: 'chat', label: 'Chat', route: '/chat', icon: MessageSquare, group: 'Core' },
  { id: 'runtime', label: 'Runtime', route: '/runtime', icon: Activity, group: 'Core' },
  { id: 'context', label: 'Context', route: '/context', icon: Layers, group: 'Core' },
  { id: 'memory', label: 'Memory', route: '/memory', icon: Brain, group: 'Knowledge' },
  { id: 'skills', label: 'Skills', route: '/skills', icon: Boxes, group: 'Automation' },
  { id: 'agents', label: 'Agents', route: '/agents', icon: Network, group: 'Automation' },
  { id: 'tools', label: 'Tools', route: '/tools', icon: Wrench, group: 'Automation' },
  { id: 'gateway', label: 'Gateway', route: '/gateway', icon: RadioTower, group: 'Channels' },
  { id: 'iacc', label: 'IACC', route: '/iacc', icon: Factory, group: 'Apps' },
  { id: 'audit', label: 'Audit', route: '/audit', icon: ClipboardCheck, group: 'System' },
  { id: 'settings', label: 'Settings', route: '/settings', icon: Settings, group: 'System' },
];

function go(item: NavItem) {
  router.push(item.route);
}

onMounted(() => {
  store.boot();
});
</script>

<template>
  <div class="app-shell">
    <nav class="rail" aria-label="Cowd primary navigation">
      <button
        v-for="item in nav"
        :key="item.id"
        class="rail-button"
        :class="{ active: route.path === item.route || (item.id === 'chat' && route.path === '/') }"
        :title="item.label"
        :aria-label="item.label"
        type="button"
        @click="go(item)"
      >
        <component :is="item.icon" :size="19" stroke-width="1.8" />
      </button>
    </nav>

    <SessionSidebar />

    <main class="main-surface">
      <RouterView />
    </main>

    <CompanionPanel />
  </div>
</template>
