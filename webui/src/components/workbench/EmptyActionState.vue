<script setup lang="ts">
defineProps<{
  title: string;
  detail?: string;
  action?: string;
}>();
</script>

<template>
  <section class="empty-action-state">
    <strong>{{ title }}</strong>
    <p v-if="detail">{{ detail }}</p>
    <span v-if="action">{{ action }}</span>
  </section>
</template>
