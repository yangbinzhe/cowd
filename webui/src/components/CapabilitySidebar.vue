<script setup lang="ts">
import { computed, onMounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { capabilitySpecs } from '../data/capabilities';
import { useAppStore } from '../stores/app';
import type { NavId } from '../types';
import EndpointHealthList from './workbench/EndpointHealthList.vue';

const route = useRoute();
const router = useRouter();
const store = useAppStore();

const pageId = computed(() => route.path.replace('/', '') as Exclude<NavId, 'chat' | 'settings'>);
const spec = computed(() => capabilitySpecs[pageId.value]);
const snapshots = computed(() => store.capabilitySnapshots[pageId.value] || []);
type CapabilitySection = NonNullable<(typeof spec.value)>['sections'][number];

const activeSection = computed(() => store.activeSectionByPage[pageId.value] || String(route.query.section || ''));

function normalizeText(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function focusSection(section: CapabilitySection) {
  const direct = document.querySelector<HTMLElement>(`[data-section="${section.id}"], #section-${section.id}`);
  const sectionText = normalizeText(section.label);
  const sectionTokens = normalizeText(`${section.id} ${section.label}`).split(' ').filter((token) => token.length > 2);
  const byHeading = Array.from(document.querySelectorAll<HTMLElement>('main h1, main h2, main h3')).find((heading) => {
    const headingText = normalizeText(heading.textContent || '');
    return headingText.includes(sectionText) || sectionTokens.some((token) => headingText.includes(token));
  });
  const target = direct || byHeading;

  if (!target) return;
  if (!target.hasAttribute('tabindex')) target.setAttribute('tabindex', '-1');
  target.scrollIntoView({ behavior: 'smooth', block: 'start' });
  target.focus({ preventScroll: true });
}

async function selectSection(section: CapabilitySection) {
  store.selectSection(pageId.value, section.id);
  await router.replace({ query: { ...route.query, section: section.id } });
  focusSection(section);
}

watch(() => route.query.section, async (sectionId) => {
  if (!sectionId || typeof sectionId !== 'string') return;
  store.selectSection(pageId.value, sectionId);
});

onMounted(() => {
  const sectionId = route.query.section;
  if (sectionId && typeof sectionId === 'string') {
    store.selectSection(pageId.value, sectionId);
  }
});
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
        @click="selectSection(section)"
      >
        <strong>{{ section.label }}</strong>
        <span>{{ section.description }}</span>
      </button>
    </nav>

    <section v-if="spec" class="sidebar-inspector">
      <EndpointHealthList :endpoints="snapshots" />
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
