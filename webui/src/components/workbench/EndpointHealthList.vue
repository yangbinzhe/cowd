<script setup lang="ts">
import type { EndpointSnapshot } from '../../api/client';
import StatusPill from './StatusPill.vue';

defineProps<{
  endpoints: EndpointSnapshot[];
}>();
</script>

<template>
  <section class="endpoint-health-list">
    <header>
      <h2>Endpoint health</h2>
      <span>{{ endpoints.length }}</span>
    </header>
    <article v-for="endpoint in endpoints" :key="endpoint.id">
      <div>
        <strong>{{ endpoint.label }}</strong>
        <code>{{ endpoint.method }} {{ endpoint.path }}</code>
        <p v-if="endpoint.__error">{{ endpoint.__error }}</p>
      </div>
      <StatusPill :status="endpoint.status" />
    </article>
  </section>
</template>
