<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue';
import { AlertTriangle, CheckCircle2, Database, RefreshCw, WifiOff } from 'lucide-vue-next';
import { api } from '../api/client';
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
const runtimeLeases = ref<any>(null);
const runtimeApprovals = ref<any[]>([]);
const leaseOwner = ref('webui');
const leaseMode = ref('shared');
const workbenchError = ref('');
const contextQuery = ref('');
const contextProfile = ref('main_turn');
const contextEnvelope = ref<any>(null);
const contextHistory = ref<any>(null);
const contextRecommendations = ref<any>(null);
const evidenceRef = ref('workspace://changed-file/README.md');
const evidenceResult = ref<any>(null);
const memoryQuery = ref('manufacturing quality anomaly');
const memoryLayer = ref('L2');
const memoryContent = ref('Manufacturing line A reported repeated torque deviation on station 3 with batch QA-2026-0616.');
const memoryResult = ref<any>(null);
const memoryPacket = ref<any>(null);
const maintenanceResult = ref<any>(null);
const structuredSourceRef = ref('service://iacc/manufacturing/demo-line-a');
const structuredFactType = ref('manufacturing_quality_event');
const structuredPlan = ref<any>(null);
const structuredCollections = ref<any>(null);

async function refresh() {
  await store.loadCapability(props.page);
  if (props.page === 'runtime') await loadRuntimeWorkbench();
  if (props.page === 'context') await loadContextWorkbench();
  if (props.page === 'memory') await loadMemoryWorkbench();
}

function preview(data: any) {
  return JSON.stringify(data, null, 2).slice(0, 1800);
}

async function loadRuntimeWorkbench() {
  workbenchError.value = '';
  try {
    const [leases, approvals] = await Promise.all([api.runtimeSessionLeases(), api.approvalPending()]);
    runtimeLeases.value = leases;
    runtimeApprovals.value = Array.isArray(approvals) ? approvals : (approvals as any).pending || [];
  } catch (error) {
    workbenchError.value = error instanceof Error ? error.message : String(error);
  }
}

async function reloadProviders() {
  await store.reloadProviders();
  await refresh();
}

async function acquireLease() {
  if (!store.activeSessionId) await store.createSession();
  await api.acquireRuntimeLease(store.activeSessionId, leaseOwner.value, leaseMode.value);
  await loadRuntimeWorkbench();
}

async function releaseLease() {
  if (!store.activeSessionId) return;
  await api.releaseRuntimeLease(store.activeSessionId, leaseOwner.value);
  await loadRuntimeWorkbench();
}

async function respondApproval(id: string, approved: boolean) {
  await api.approvalRespond(id, approved, approved ? 'approved from WebUI runtime workbench' : 'rejected from WebUI runtime workbench');
  await loadRuntimeWorkbench();
}

async function loadContextWorkbench() {
  workbenchError.value = '';
  try {
    const sessionId = store.activeSessionId || 'api-context';
    const [current, history, recommendations] = await Promise.all([
      api.contextCurrent(sessionId, contextQuery.value, contextProfile.value),
      api.contextHistory(sessionId),
      api.contextRecommendations(sessionId),
    ]);
    contextEnvelope.value = current;
    contextHistory.value = history;
    contextRecommendations.value = recommendations;
  } catch (error) {
    workbenchError.value = error instanceof Error ? error.message : String(error);
  }
}

async function resolveEvidence() {
  evidenceResult.value = await api.resolveEvidence(evidenceRef.value);
}

async function loadMemoryWorkbench() {
  workbenchError.value = '';
  try {
    const [search, packet, sources, facts, evidence, watermarks] = await Promise.all([
      api.memorySearch(memoryQuery.value),
      api.memoryPacket(memoryQuery.value),
      api.structuredSources(),
      api.structuredFacts(),
      api.structuredEvidence(),
      api.structuredWatermarks(),
    ]);
    memoryResult.value = search;
    memoryPacket.value = packet;
    structuredCollections.value = { sources, facts, evidence, watermarks };
  } catch (error) {
    workbenchError.value = error instanceof Error ? error.message : String(error);
  }
}

async function createMemoryFact() {
  memoryResult.value = await api.createMemoryEntry(memoryLayer.value, {
    title: 'Manufacturing quality signal',
    content: memoryContent.value,
    category: 'reference',
    priority: 'high',
    tags: ['manufacturing', 'quality', 'v0.9.220'],
  });
  await loadMemoryWorkbench();
}

async function scanMaintenance() {
  maintenanceResult.value = await api.scanMemoryMaintenance({ max_candidates: 20 });
}

async function planStructuredIngest() {
  structuredPlan.value = await api.structuredIngestPlan({
    source_ref: structuredSourceRef.value,
    fact_type: structuredFactType.value,
    estimated_rows: 128,
    raw_checksum: 'sha256:manufacturing-demo-v0.9.220',
    metric_ids: ['torque_deviation_rate', 'station_quality_escape'],
  });
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

    <p v-if="workbenchError" class="settings-alert">{{ workbenchError }}</p>

    <section v-if="props.page === 'runtime'" class="management-grid runtime-workbench">
      <article class="management-panel">
        <header>
          <h2>Control plane</h2>
          <button class="ghost-action" type="button" @click="reloadProviders">Reload providers</button>
        </header>
        <dl class="detail-list">
          <dt>Configured model</dt>
          <dd>{{ store.controlPlane?.configured_model || 'unknown' }}</dd>
          <dt>Provider count</dt>
          <dd>{{ store.providers?.provider_count ?? store.controlPlane?.provider_count ?? 0 }}</dd>
          <dt>Session</dt>
          <dd>{{ store.activeSessionId || 'none' }}</dd>
          <dt>Degraded</dt>
          <dd>{{ store.controlPlane?.degraded ? 'yes' : 'no' }}</dd>
        </dl>
      </article>

      <article class="management-panel">
        <header>
          <h2>Session lease</h2>
          <span>{{ runtimeLeases?.leases?.length || runtimeLeases?.count || 0 }} leases</span>
        </header>
        <label class="field-line">
          Owner
          <input v-model="leaseOwner" type="text" />
        </label>
        <label class="field-line">
          Mode
          <select v-model="leaseMode">
            <option value="shared">shared</option>
            <option value="exclusive">exclusive</option>
          </select>
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" @click="acquireLease">Acquire</button>
          <button class="ghost-action" type="button" @click="releaseLease">Release</button>
        </div>
        <pre class="action-result">{{ preview(runtimeLeases || {}) }}</pre>
      </article>

      <article class="management-panel">
        <header>
          <h2>Approvals</h2>
          <span>{{ runtimeApprovals.length }} pending</span>
        </header>
        <div v-if="!runtimeApprovals.length" class="empty-note">No pending approval requests.</div>
        <article v-for="approval in runtimeApprovals" :key="approval.id" class="approval-row">
          <span>{{ approval.summary || approval.id }}</span>
          <button class="ghost-action" type="button" @click="respondApproval(approval.id, false)">Reject</button>
          <button class="primary-action" type="button" @click="respondApproval(approval.id, true)">Approve</button>
        </article>
      </article>
    </section>

    <section v-if="props.page === 'context'" class="management-grid context-workbench">
      <article class="management-panel">
        <header>
          <h2>Context builder</h2>
          <span>{{ store.activeSessionId || 'api-context' }}</span>
        </header>
        <label class="field-line">
          Query
          <input v-model="contextQuery" type="text" placeholder="Summarize current task evidence" @keydown.enter.prevent="loadContextWorkbench" />
        </label>
        <label class="field-line">
          Profile
          <select v-model="contextProfile">
            <option value="main_turn">main_turn</option>
            <option value="yolo_goal">yolo_goal</option>
            <option value="collaboration">collaboration</option>
          </select>
        </label>
        <button class="primary-action" type="button" @click="loadContextWorkbench">Build packet</button>
        <pre class="action-result">{{ preview(contextEnvelope || {}) }}</pre>
      </article>

      <article class="management-panel">
        <header>
          <h2>Evidence resolve</h2>
          <span>ref</span>
        </header>
        <label class="field-line">
          Evidence ref
          <input v-model="evidenceRef" type="text" @keydown.enter.prevent="resolveEvidence" />
        </label>
        <button class="ghost-action" type="button" @click="resolveEvidence">Resolve evidence</button>
        <pre class="action-result">{{ preview(evidenceResult || {}) }}</pre>
      </article>

      <article class="management-panel">
        <header>
          <h2>History and recommendations</h2>
          <span>active session</span>
        </header>
        <pre class="action-result">{{ preview({ history: contextHistory, recommendations: contextRecommendations }) }}</pre>
      </article>
    </section>

    <section v-if="props.page === 'memory'" class="management-grid memory-workbench">
      <article class="management-panel">
        <header>
          <h2>Search, recall, packet</h2>
          <span>memory kernel</span>
        </header>
        <label class="field-line">
          Query
          <input v-model="memoryQuery" type="text" @keydown.enter.prevent="loadMemoryWorkbench" />
        </label>
        <button class="primary-action" type="button" @click="loadMemoryWorkbench">Search and build packet</button>
        <pre class="action-result">{{ preview({ search: memoryResult, packet: memoryPacket }) }}</pre>
      </article>

      <article class="management-panel">
        <header>
          <h2>Memory entry</h2>
          <span>create/update/delete APIs</span>
        </header>
        <label class="field-line">
          Layer
          <select v-model="memoryLayer">
            <option>L0</option>
            <option>L1</option>
            <option>L2</option>
            <option>L3</option>
            <option>L4</option>
          </select>
        </label>
        <label class="field-line">
          Manufacturing fact
          <textarea v-model="memoryContent" rows="4" />
        </label>
        <button class="primary-action" type="button" @click="createMemoryFact">Register memory fact</button>
      </article>

      <article class="management-panel">
        <header>
          <h2>Structured data core</h2>
          <span>cowd kernel</span>
        </header>
        <label class="field-line">
          Source ref
          <input v-model="structuredSourceRef" type="text" />
        </label>
        <label class="field-line">
          Fact type
          <input v-model="structuredFactType" type="text" />
        </label>
        <button class="ghost-action" type="button" @click="planStructuredIngest">Plan manufacturing ingest</button>
        <pre class="action-result">{{ preview({ plan: structuredPlan, collections: structuredCollections }) }}</pre>
      </article>

      <article class="management-panel">
        <header>
          <h2>Maintenance</h2>
          <span>scan/update</span>
        </header>
        <button class="ghost-action" type="button" @click="scanMaintenance">Scan candidates</button>
        <pre class="action-result">{{ preview(maintenanceResult || {}) }}</pre>
      </article>
    </section>

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
