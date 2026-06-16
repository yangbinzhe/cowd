<script setup lang="ts">
import { computed } from 'vue';
import { RouterLink, useRoute } from 'vue-router';
import {
  Activity, Brain, Boxes, ClipboardCheck, Factory, Layers, Network,
  RadioTower, Settings, Wrench,
} from 'lucide-vue-next';
import { capabilitySpecs } from '../data/capabilities';
import { useAppStore } from '../stores/app';
import type { NavId } from '../types';

const route = useRoute();
const store = useAppStore();

const pageId = computed(() => route.path.replace('/', '') as Exclude<NavId, 'chat' | 'settings'>);
const spec = computed(() => capabilitySpecs[pageId.value]);
const snapshots = computed(() => store.capabilitySnapshots[pageId.value] || []);

const capabilityNav = [
  { id: 'runtime', label: 'Runtime', route: '/runtime', icon: Activity },
  { id: 'context', label: 'Context', route: '/context', icon: Layers },
  { id: 'memory', label: 'Memory', route: '/memory', icon: Brain },
  { id: 'skills', label: 'Skills', route: '/skills', icon: Boxes },
  { id: 'agents', label: 'Agents', route: '/agents', icon: Network },
  { id: 'tools', label: 'Tools', route: '/tools', icon: Wrench },
  { id: 'gateway', label: 'Gateway', route: '/gateway', icon: RadioTower },
  { id: 'iacc', label: 'IACC', route: '/iacc', icon: Factory },
  { id: 'audit', label: 'Audit', route: '/audit', icon: ClipboardCheck },
  { id: 'settings', label: 'Settings', route: '/settings', icon: Settings },
];
</script>

<template>
  <aside class="capability-sidebar">
    <header class="sidebar-head capability-head">
      <strong>{{ spec?.title || 'Control Center' }}</strong>
      <span>{{ spec?.subtitle || 'System settings and policy.' }}</span>
    </header>

    <nav class="capability-nav" aria-label="Capability pages">
      <RouterLink
        v-for="item in capabilityNav"
        :key="item.id"
        :to="item.route"
        class="capability-link"
        :class="{ active: route.path === item.route }"
      >
        <component :is="item.icon" :size="15" />
        <span>{{ item.label }}</span>
      </RouterLink>
    </nav>

    <section v-if="spec" class="sidebar-inspector">
      <h2>API coverage</h2>
      <dl>
        <dt>Checked</dt>
        <dd>{{ snapshots.length }}</dd>
        <dt>Ready</dt>
        <dd>{{ snapshots.filter((item) => item.status === 'ready').length }}</dd>
        <dt>Offline/Error</dt>
        <dd>{{ snapshots.filter((item) => item.status === 'offline' || item.status === 'error').length }}</dd>
      </dl>
    </section>

    <section v-if="spec" class="sidebar-inspector">
      <h2>Contract</h2>
      <dl>
        <template v-for="item in spec.inspector" :key="item.label">
          <dt>{{ item.label }}</dt>
          <dd>{{ item.value }}</dd>
        </template>
      </dl>
    </section>
  </aside>
</template>
