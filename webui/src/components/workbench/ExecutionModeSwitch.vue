<script setup lang="ts">
const model = defineModel<string>({ default: 'plan' });

defineProps<{
  livePolicy?: string;
}>();

const modes = [
  { id: 'plan', label: 'Plan', detail: 'Inspect expected work without changing data.' },
  { id: 'dry_run', label: 'Dry run', detail: 'Exercise the route in a non-destructive mode when supported.' },
  { id: 'live', label: 'Live', detail: 'Requires receipt, audit, and approval policy.' },
];
</script>

<template>
  <div class="execution-mode-switch" role="radiogroup" aria-label="Execution mode">
    <button
      v-for="mode in modes"
      :key="mode.id"
      type="button"
      :class="{ active: model === mode.id }"
      :aria-pressed="model === mode.id"
      @click="model = mode.id"
    >
      <strong>{{ mode.label }}</strong>
      <span>{{ mode.id === 'live' && livePolicy ? livePolicy : mode.detail }}</span>
    </button>
  </div>
</template>
