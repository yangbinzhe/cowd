<script setup lang="ts">
import { computed } from 'vue';
import { useRoute } from 'vue-router';
import { capabilitySpecs } from '../data/capabilities';
import { useAppStore } from '../stores/app';
import type { NavId } from '../types';

const route = useRoute();
const store = useAppStore();

const pageId = computed(() => route.path.replace('/', '') as Exclude<NavId, 'chat' | 'settings'>);
const spec = computed(() => capabilitySpecs[pageId.value]);
const snapshots = computed(() => store.capabilitySnapshots[pageId.value] || []);
const activeSection = computed(() => store.activeSectionByPage[pageId.value] || spec.value?.sections?.[0]?.id || 'overview');

function selectSection(sectionId: string) {
  store.selectSection(pageId.value, sectionId);
}
</script>

<template>
  <aside class="capability-sidebar">
    <header class="sidebar-head capability-head">
      <strong>{{ spec?.title || 'Control Center' }}</strong>
      <span>{{ spec?.subtitle || 'System settings and policy.' }}</span>
    </header>

    <nav v-if="spec?.sections?.length" class="secondary-sections" :aria-label="`${spec.title} sections`">
      <h2>Sections</h2>
      <button
        v-for="section in spec.sections"
        :key="section.id"
        class="section-row"
        :class="{ active: activeSection === section.id }"
        type="button"
        @click="selectSection(section.id)"
      >
        <strong>{{ section.label }}</strong>
        <span>{{ section.description }}</span>
      </button>
    </nav>

    <section v-if="spec" class="sidebar-inspector">
      <h2>Live endpoints</h2>
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
