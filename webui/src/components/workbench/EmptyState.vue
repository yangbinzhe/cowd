<script setup lang="ts">
defineProps<{ title: string; detail?: string }>();
</script>

<template>
  <div class="empty-state">
    <strong>{{ title }}</strong>
    <p v-if="detail">{{ detail }}</p>
  </div>
</template>
