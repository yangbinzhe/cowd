<script setup lang="ts">
defineProps<{ items: Array<{ label: string; value: unknown }> }>();
</script>

<template>
  <dl class="detail-list">
    <template v-for="item in items" :key="item.label">
      <dt>{{ item.label }}</dt>
      <dd>{{ item.value }}</dd>
    </template>
  </dl>
</template>
