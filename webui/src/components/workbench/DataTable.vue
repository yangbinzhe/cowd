<script setup lang="ts">
defineProps<{
  rows: Record<string, unknown>[];
  columns?: string[];
}>();
</script>

<template>
  <table class="data-table">
    <thead>
      <tr>
        <th v-for="column in columns || Object.keys(rows[0] || {})" :key="column">{{ column }}</th>
      </tr>
    </thead>
    <tbody>
      <tr v-for="(row, index) in rows" :key="index">
        <td v-for="column in columns || Object.keys(row)" :key="column">{{ row[column] }}</td>
      </tr>
    </tbody>
  </table>
</template>
