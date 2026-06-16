<script setup lang="ts">
import { computed } from 'vue';
import type { ApiReceipt, ApiWriteError } from '../../api/client';
import StatusPill from './StatusPill.vue';

const props = withDefaults(defineProps<{
  receipt?: ApiReceipt | ApiWriteError | Record<string, unknown> | null;
  title?: string;
}>(), {
  receipt: null,
  title: 'Request receipt',
});

const normalized = computed(() => {
  const receipt = props.receipt as any;
  if (!receipt || typeof receipt !== 'object') return null;
  if (receipt.receipt && typeof receipt.receipt === 'object') return { ...receipt, ...receipt.receipt };
  if (receipt.data?.receipt && typeof receipt.data.receipt === 'object') return { ...receipt, ...receipt.data.receipt };
  return receipt;
});

function value(key: string) {
  return normalized.value && typeof normalized.value === 'object' ? (normalized.value as any)[key] : undefined;
}
</script>

<template>
  <section v-if="receipt" class="request-receipt" :data-ok="value('ok') !== false">
    <header>
      <h2>{{ title }}</h2>
      <StatusPill :status="value('ok') === false ? 'error' : (value('mode') || value('status') || 'ready')" />
    </header>
    <dl class="detail-list">
      <dt>Endpoint</dt>
      <dd>{{ value('endpoint') || value('path') || '-' }}</dd>
      <dt>Method</dt>
      <dd>{{ value('method') || '-' }}</dd>
      <dt>Status</dt>
      <dd>{{ value('status') || value('status_text') || '-' }}</dd>
      <dt>Retryable</dt>
      <dd>{{ value('retryable') === undefined ? '-' : String(value('retryable')) }}</dd>
      <dt>Error</dt>
      <dd>{{ value('error') || value('message') || '-' }}</dd>
      <dt>Payload</dt>
      <dd>{{ value('payload_summary') || '-' }}</dd>
    </dl>
  </section>
</template>
