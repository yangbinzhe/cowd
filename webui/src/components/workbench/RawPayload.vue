<script setup lang="ts">
import { computed, ref } from 'vue';

const props = withDefaults(defineProps<{
  data: unknown;
  title?: string;
  maxChars?: number;
}>(), {
  title: 'Raw payload',
  maxChars: 2400,
});

const expanded = ref(false);
const copied = ref(false);

const json = computed(() => {
  try {
    return JSON.stringify(props.data ?? {}, null, 2);
  } catch {
    return String(props.data ?? '');
  }
});

const clipped = computed(() => json.value.length > props.maxChars);
const visibleJson = computed(() => expanded.value ? json.value : json.value.slice(0, props.maxChars));

async function copyPayload() {
  await navigator.clipboard?.writeText(json.value);
  copied.value = true;
  window.setTimeout(() => { copied.value = false; }, 1200);
}
</script>

<template>
  <details class="raw-payload">
    <summary>
      <span>{{ title }}</span>
      <small>{{ clipped ? `${json.length} chars` : 'debug view' }}</small>
    </summary>
    <div class="raw-payload-toolbar">
      <button class="ghost-action" type="button" @click="expanded = !expanded">
        {{ expanded ? 'Collapse' : 'Show full' }}
      </button>
      <button class="ghost-action" type="button" @click="copyPayload">
        {{ copied ? 'Copied' : 'Copy JSON' }}
      </button>
    </div>
    <pre class="raw-payload-body">{{ visibleJson }}{{ clipped && !expanded ? '\n...' : '' }}</pre>
  </details>
</template>
