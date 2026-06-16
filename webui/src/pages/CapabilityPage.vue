<script setup lang="ts">
import { computed, onMounted, watch } from 'vue';
import { AlertTriangle, CheckCircle2, Database, RefreshCw, WifiOff } from 'lucide-vue-next';
import { capabilitySpecs } from '../data/capabilities';
import { useAppStore } from '../stores/app';
import ChartPanel from '../components/ChartPanel.vue';

const props = defineProps<{ page: keyof typeof capabilitySpecs }>();
const store = useAppStore();
const spec = computed(() => capabilitySpecs[props.page]);
const snapshots = computed(() => store.capabilitySnapshots[props.page] || []);
const loading = computed(() => !!store.capabilityLoading[props.page]);
const error = computed(() => store.capabilityError[props.page]);
const readyCount = computed(() => snapshots.value.filter((item) => item.status === 'ready').length);
const offlineCount = computed(() => snapshots.value.filter((item) => item.status === 'offline' || item.status === 'error').length);
const totalRows = computed(() => snapshots.value.reduce((sum, item) => sum + item.count, 0));
const chartData = computed(() => snapshots.value.map((item) => ({ name: item.label, value: Math.max(1, item.count) })));

function refresh() {
  store.loadCapability(props.page);
}

function preview(data: any) {
  return JSON.stringify(data, null, 2).slice(0, 1800);
}

onMounted(refresh);
watch(() => props.page, refresh);
</script>

<template>
  <section class="capability-page">
    <header class="page-header">
      <div>
        <h1>{{ spec.title }}</h1>
        <p>{{ spec.subtitle }}</p>
      </div>
      <button class="primary-action" type="button" :disabled="loading" @click="refresh">
        <RefreshCw :size="15" />
        {{ loading ? 'Loading' : 'Refresh real APIs' }}
      </button>
    </header>

    <p v-if="error" class="settings-alert">{{ error }}</p>

    <section class="metric-row" aria-label="Capability metrics">
      <article class="metric-card" data-tone="success">
        <span>Ready APIs</span>
        <strong>{{ readyCount }}</strong>
        <small>{{ snapshots.length }} checked</small>
      </article>
      <article class="metric-card" :data-tone="offlineCount ? 'warn' : 'success'">
        <span>Offline/Error</span>
        <strong>{{ offlineCount }}</strong>
        <small>失败会明确展示，不再假成功</small>
      </article>
      <article class="metric-card" data-tone="info">
        <span>Records</span>
        <strong>{{ totalRows }}</strong>
        <small>来自真实 API payload</small>
      </article>
    </section>

    <div class="capability-grid">
      <ChartPanel :title="`${spec.title} API coverage`" kind="bar" :data="chartData.length ? chartData : spec.chartData" />
      <section class="work-table">
        <header>
          <h2>Live endpoint contract</h2>
          <span>{{ snapshots.length }} endpoints</span>
        </header>
        <table>
          <thead>
            <tr>
              <th>API</th>
              <th>Status</th>
              <th>Rows</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="item in snapshots" :key="item.id">
              <td>
                <strong>{{ item.label }}</strong>
                <small>{{ item.method }} {{ item.path }}</small>
              </td>
              <td>
                <span class="status-badge" :data-status="item.status">
                  <CheckCircle2 v-if="item.status === 'ready'" :size="13" />
                  <WifiOff v-else-if="item.status === 'offline'" :size="13" />
                  <AlertTriangle v-else-if="item.status === 'error'" :size="13" />
                  <Database v-else :size="13" />
                  {{ item.status }}
                </span>
              </td>
              <td>{{ item.count }}</td>
            </tr>
          </tbody>
        </table>
      </section>
    </div>

    <section class="management-grid live-management">
      <article v-for="item in snapshots" :key="`${item.id}:preview`" class="management-panel">
        <header>
          <h2>{{ item.label }}</h2>
          <span>{{ item.status }}</span>
        </header>
        <p v-if="item.__error">{{ item.__error }}</p>
        <pre class="action-result">{{ preview(item.data) }}</pre>
      </article>
    </section>
  </section>
</template>
