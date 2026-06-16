<script setup lang="ts">
const props = withDefaults(defineProps<{
  status?: 'ready' | 'empty' | 'offline' | 'error' | 'degraded' | 'unsupported' | 'loading';
  title?: string;
  detail?: string;
  endpoint?: string;
}>(), {
  status: 'ready',
  title: '',
  detail: '',
  endpoint: '',
});

const label: Record<string, string> = {
  ready: 'Ready',
  empty: 'Empty',
  offline: 'Offline',
  error: 'Error',
  degraded: 'Degraded',
  unsupported: 'Unsupported',
  loading: 'Loading',
};
</script>

<template>
  <section class="api-state-banner" :data-status="props.status">
    <strong>{{ title || label[props.status] }}</strong>
    <p>{{ detail || (endpoint ? `Endpoint: ${endpoint}` : label[props.status]) }}</p>
    <code v-if="endpoint">{{ endpoint }}</code>
  </section>
</template>
