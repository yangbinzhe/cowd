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
const activeSection = computed(() => store.activeSectionByPage[pageId.value] || spec.value?.sections[0]?.id || '');

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

    <section v-if="spec" class="secondary-sections">
      <h2>Sections</h2>
      <button
        v-for="section in spec.sections"
        :key="section.id"
        type="button"
        class="section-row"
        :class="{ active: activeSection === section.id }"
        @click="store.selectSection(pageId, section.id)"
      >
        <strong>{{ section.label }}</strong>
        <span>{{ section.description }}</span>
      </button>
    </section>

    <section v-if="spec" class="sidebar-inspector">
      <h2>Inspector</h2>
      <dl>
        <template v-for="item in spec.inspector" :key="item.label">
          <dt>{{ item.label }}</dt>
          <dd>{{ item.value }}</dd>
        </template>
      </dl>
    </section>
  </aside>
</template>
