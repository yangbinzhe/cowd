<script setup lang="ts">
defineProps<{
  refs?: string[];
  boundary?: string;
  approvalRequired?: boolean;
}>();
</script>

<template>
  <section class="impact-preview">
    <header>
      <h3>Impact preview</h3>
      <span>{{ approvalRequired ? 'Approval required' : 'No approval flag' }}</span>
    </header>
    <ul>
      <li v-for="ref in refs || []" :key="ref">{{ ref }}</li>
      <li v-if="!(refs || []).length">No affected references declared.</li>
    </ul>
    <p>{{ boundary || 'No kernel boundary declared.' }}</p>
  </section>
</template>
