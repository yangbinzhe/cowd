<script setup lang="ts">
defineProps<{ status: string }>();
</script>

<template>
  <span class="status-badge" :data-status="status">{{ status }}</span>
</template>
