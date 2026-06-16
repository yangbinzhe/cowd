<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { Check, RefreshCw, Search } from 'lucide-vue-next';
import { api } from '../api/client';
import DataTable from '../components/workbench/DataTable.vue';
import EmptyState from '../components/workbench/EmptyState.vue';
import RawPayload from '../components/workbench/RawPayload.vue';
import RequestReceipt from '../components/workbench/RequestReceipt.vue';
import StatusPill from '../components/workbench/StatusPill.vue';
import { useAppStore } from '../stores/app';

const store = useAppStore();
const loading = ref(false);
const error = ref('');
const query = ref('Summarize current task evidence and runtime constraints');
const profile = ref('main_turn');
const evidenceRef = ref('workspace://changed-file/README.md');
const envelope = ref<any>({});
const history = ref<any>({});
const recommendations = ref<any>({});
const evidence = ref<any>({});
const actionResult = ref<any>(null);
const recommendationText = ref('Context recommendation acknowledged from WebUI.');
const sessionId = computed(() => store.activeSessionId || 'api-context');
const contextItems = computed(() => {
  const direct = envelope.value?.envelope?.items || envelope.value?.items || envelope.value?.context?.items;
  return Array.isArray(direct) ? direct : [];
});
const itemRows = computed(() => contextItems.value.slice(0, 16).map((item: any) => ({
  role: item.role || item.kind || '-',
  source: item.source_kind || item.source || '-',
  authority: item.authority || '-',
  score: item.score ?? '-',
  text: item.text || item.content || item.summary || '-',
})));
const historyRows = computed(() => {
  const rows = history.value?.summaries || history.value?.events || history.value?.contexts || [];
  return (Array.isArray(rows) ? rows : []).slice(0, 14).map((item: any) => ({
    envelope: item.envelope_id || item.id || '-',
    kind: item.kind || item.event_kind || '-',
    created: item.created_at || item.created_at_ms || '-',
    summary: item.summary || item.detail || item.source || '-',
  }));
});
const recommendationRows = computed(() => {
  const rows = recommendations.value?.recommendations || recommendations.value?.items || recommendations.value?.stats || [];
  return (Array.isArray(rows) ? rows : []).slice(0, 12).map((item: any) => ({
    id: item.envelope_id || item.id || '-',
    action: item.action || item.recommendation || '-',
    count: item.count || item.total || '-',
    status: item.status || '-',
  }));
});
const envelopeId = computed(() => envelope.value?.envelope_id || envelope.value?.envelope?.id || historyRows.value[0]?.envelope || '');

async function refresh() {
  loading.value = true;
  error.value = '';
  try {
    const [nextEnvelope, nextHistory, nextRecommendations] = await Promise.all([
      api.contextCurrent(sessionId.value, query.value, profile.value),
      api.contextHistory(sessionId.value),
      api.contextRecommendations(sessionId.value),
    ]);
    envelope.value = nextEnvelope;
    history.value = nextHistory;
    recommendations.value = nextRecommendations;
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  } finally {
    loading.value = false;
  }
}

async function resolveEvidence() {
  evidence.value = await api.resolveEvidence(evidenceRef.value);
}

async function acknowledgeRecommendation() {
  if (!envelopeId.value) return;
  actionResult.value = await api.recordContextRecommendation(sessionId.value, envelopeId.value, recommendationText.value, 'acknowledged');
  recommendations.value = await api.contextRecommendations(sessionId.value);
}

onMounted(refresh);
</script>

<template>
  <section class="capability-page context-page">
    <header class="page-header">
      <div>
        <h1>Context Builder</h1>
        <p>上下文包构建、历史包络、推荐动作和证据解析集中管理。</p>
      </div>
      <button class="primary-action" type="button" :disabled="loading" @click="refresh">
        <RefreshCw :size="15" />
        {{ loading ? 'Loading' : 'Build context' }}
      </button>
    </header>

    <p v-if="error" class="settings-alert">{{ error }}</p>

    <section class="metric-row">
      <article class="metric-card">
        <span>Context items</span>
        <strong>{{ itemRows.length }}</strong>
        <small>{{ envelope.source || envelope.mode || 'current envelope' }}</small>
      </article>
      <article class="metric-card" data-tone="info">
        <span>History</span>
        <strong>{{ historyRows.length }}</strong>
        <small>stored envelopes</small>
      </article>
      <article class="metric-card" data-tone="success">
        <span>Recommendations</span>
        <strong>{{ recommendationRows.length }}</strong>
        <small>{{ sessionId }}</small>
      </article>
    </section>

    <section class="context-grid">
      <section class="management-panel context-panel wide">
        <header>
          <h2>Context builder</h2>
          <StatusPill :status="envelope.__offline ? 'offline' : 'ready'" />
        </header>
        <div class="context-builder-row">
          <label class="field-line">
            Query
            <input v-model="query" type="text" @keyup.enter="refresh" />
          </label>
          <label class="field-line">
            Profile
            <select v-model="profile">
              <option value="main_turn">main_turn</option>
              <option value="yolo_goal">yolo_goal</option>
              <option value="collaboration">collaboration</option>
            </select>
          </label>
        </div>
        <button class="primary-action" type="button" @click="refresh">
          <Search :size="15" />
          Build packet
        </button>
        <DataTable v-if="itemRows.length" :rows="itemRows" :columns="['role', 'source', 'authority', 'score', 'text']" />
        <EmptyState v-else title="No context items" detail="当前查询没有可展示的上下文项，或后端离线。" />
      </section>

      <section class="management-panel context-panel">
        <header>
          <h2>Evidence resolve</h2>
          <span>reference lookup</span>
        </header>
        <label class="field-line">
          Evidence ref
          <input v-model="evidenceRef" type="text" @keyup.enter="resolveEvidence" />
        </label>
        <button class="ghost-action" type="button" @click="resolveEvidence">Resolve evidence</button>
        <RequestReceipt :receipt="evidence" title="Lookup receipt" />
        <RawPayload title="Resolved evidence" :data="evidence" />
      </section>

      <section class="management-panel context-panel">
        <header>
          <h2>Recommendation actions</h2>
          <span>{{ envelopeId || 'no envelope' }}</span>
        </header>
        <label class="field-line">
          Recommendation note
          <textarea v-model="recommendationText" rows="3" />
        </label>
        <button class="primary-action" type="button" :disabled="!envelopeId" @click="acknowledgeRecommendation">
          <Check :size="15" />
          Acknowledge
        </button>
        <RequestReceipt :receipt="actionResult" title="Recommendation receipt" />
        <DataTable v-if="recommendationRows.length" :rows="recommendationRows" :columns="['id', 'action', 'count', 'status']" />
        <EmptyState v-else title="No recommendation stats" detail="推荐动作统计会在后端有历史数据时展示。" />
      </section>

      <section class="management-panel context-panel wide">
        <header>
          <h2>History and raw envelope</h2>
          <span>{{ historyRows.length }} history rows</span>
        </header>
        <DataTable v-if="historyRows.length" :rows="historyRows" :columns="['envelope', 'kind', 'created', 'summary']" />
        <EmptyState v-else title="No context history" detail="持久化 session store 没有返回上下文历史。" />
        <RawPayload title="Current envelope" :data="envelope" />
        <RawPayload title="Action result" :data="actionResult || recommendations" />
      </section>
    </section>
  </section>
</template>
