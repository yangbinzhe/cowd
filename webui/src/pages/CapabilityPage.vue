<script setup lang="ts">
import { computed } from 'vue';
import { RefreshCw } from 'lucide-vue-next';
import { capabilitySpecs } from '../data/capabilities';
import ChartPanel from '../components/ChartPanel.vue';

const props = defineProps<{ page: keyof typeof capabilitySpecs }>();
const spec = computed(() => capabilitySpecs[props.page]);
</script>

<template>
  <section class="capability-page">
    <header class="page-header">
      <div>
        <h1>{{ spec.title }}</h1>
        <p>{{ spec.subtitle }}</p>
      </div>
      <button class="primary-action" type="button"><RefreshCw :size="15" /> {{ spec.primaryAction }}</button>
    </header>

    <section class="metric-row" aria-label="Capability metrics">
      <article v-for="metric in spec.metrics" :key="metric.label" class="metric-card" :data-tone="metric.tone || 'neutral'">
        <span>{{ metric.label }}</span>
        <strong>{{ metric.value }}</strong>
        <small>{{ metric.delta }}</small>
      </article>
    </section>

    <div class="capability-grid">
      <ChartPanel :title="spec.chartTitle" :kind="spec.chartKind" :data="spec.chartData" />
      <section class="work-table">
        <header>
          <h2>{{ spec.tableTitle }}</h2>
          <span>{{ spec.rows.length }} rows</span>
        </header>
        <table>
          <thead>
            <tr>
              <th v-for="key in Object.keys(spec.rows[0] || {})" :key="key">{{ key }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, index) in spec.rows" :key="index">
              <td v-for="key in Object.keys(row)" :key="key">{{ row[key] }}</td>
            </tr>
          </tbody>
        </table>
      </section>
    </div>
  </section>
</template>
