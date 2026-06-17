<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { Database, GitBranch, Network, RefreshCw, Search, ShieldCheck } from 'lucide-vue-next';
import { api } from '../api/client';
import DataTable from '../components/workbench/DataTable.vue';
import EmptyState from '../components/workbench/EmptyState.vue';
import RawPayload from '../components/workbench/RawPayload.vue';
import RequestReceipt from '../components/workbench/RequestReceipt.vue';
import StatusPill from '../components/workbench/StatusPill.vue';

const loading = ref(false);
const error = ref('');
const status = ref<any>({});
const stats = ref<any>({});
const layers = ref<any>({});
const selectedLayer = ref('L2');
const layerEntries = ref<any>({});
const selectedEntryId = ref('');
const query = ref('manufacturing quality anomaly');
const symbolQuery = ref('mfg.manufacturing.line_a');
const searchResult = ref<any>({});
const recallExplain = ref<any>({});
const packet = ref<any>({});
const links = ref<any>({});
const clusters = ref<any>({});
const entities = ref<any>({});
const triples = ref<any>({});
const symbolLinks = ref<any>({});
const maintenance = ref<any>({});
const maintenanceScan = ref<any>(null);
const performance = ref<any>({});
const runtime = ref<any>({});
const structured = ref<any>({});
const structuredPlan = ref<any>(null);
const actionResult = ref<any>(null);

const entryTitle = ref('Manufacturing quality signal');
const entryContent = ref('Manufacturing line A reported repeated torque deviation on station 3 with batch QA-2026-0616.');
const entryTags = ref('manufacturing,quality,webui');
const entryPriority = ref('high');
const sourceRef = ref('service://mfg/manufacturing/webui-line-a');
const factType = ref('manufacturing_quality_event');

const layerItems = computed(() => Array.isArray(layers.value?.layers) ? layers.value.layers : []);
const entries = computed(() => Array.isArray(layerEntries.value?.entries) ? layerEntries.value.entries : []);
const selectedEntry = computed(() => entries.value.find((entry: any) => entry.id === selectedEntryId.value) || entries.value[0] || null);
const recallRows = computed(() => (Array.isArray(recallExplain.value?.results) ? recallExplain.value.results : []).map((item: any) => ({
  title: item.title || item.id,
  layer: item.source_layer || item.layer,
  priority: item.priority,
  score: item.score,
  snippet: item.snippet || item.content,
})));
const entityRows = computed(() => Array.isArray(entities.value?.entities) ? entities.value.entities.slice(0, 12) : []);
const tripleRows = computed(() => Array.isArray(triples.value?.triples) ? triples.value.triples.slice(0, 12) : []);
const candidateRows = computed(() => Array.isArray(maintenance.value?.candidates) ? maintenance.value.candidates : []);
const linkCount = computed(() => Number(links.value?.total || links.value?.links?.length || 0));
const healthLevel = computed(() => status.value?.kernel_health?.degraded ? 'degraded' : (status.value?.status || 'unknown'));

async function refresh() {
  loading.value = true;
  error.value = '';
  try {
    const [
      nextStatus,
      nextStats,
      nextLayers,
      nextLinks,
      nextClusters,
      nextEntities,
      nextTriples,
      nextMaintenance,
      nextPerformance,
      nextRuntime,
      sources,
      facts,
      evidence,
      watermarks,
    ] = await Promise.all([
      api.memoryStatus(),
      api.memoryStats(),
      api.memoryLayers(),
      api.memoryLinks(),
      api.memoryClusters(),
      api.memoryEntities(),
      api.memoryTriples(),
      api.memoryMaintenance(),
      api.memoryPerformance(),
      api.memoryRuntime(),
      api.structuredSources(),
      api.structuredFacts(),
      api.structuredEvidence(),
      api.structuredWatermarks(),
    ]);
    status.value = nextStatus;
    stats.value = nextStats;
    layers.value = nextLayers;
    links.value = nextLinks;
    clusters.value = nextClusters;
    entities.value = nextEntities;
    triples.value = nextTriples;
    maintenance.value = nextMaintenance;
    performance.value = nextPerformance;
    runtime.value = nextRuntime;
    structured.value = { sources, facts, evidence, watermarks };
    await loadLayer();
    await runRecall();
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  } finally {
    loading.value = false;
  }
}

async function loadLayer() {
  layerEntries.value = await api.memoryLayer(selectedLayer.value);
  selectedEntryId.value = entries.value[0]?.id || '';
}

async function runRecall() {
  const [search, explain, nextPacket, nextSymbolLinks] = await Promise.all([
    api.memorySearch(query.value),
    api.memoryRecallExplain(query.value, 12),
    api.memoryPacket(query.value, 12, 2000),
    api.memorySymbolLinks(symbolQuery.value),
  ]);
  searchResult.value = search;
  recallExplain.value = explain;
  packet.value = nextPacket;
  symbolLinks.value = nextSymbolLinks;
}

async function createEntry() {
  actionResult.value = await api.createMemoryEntry(selectedLayer.value, {
    title: entryTitle.value,
    content: entryContent.value,
    category: 'reference',
    priority: entryPriority.value,
    tags: entryTags.value.split(',').map((tag) => tag.trim()).filter(Boolean),
  });
  await Promise.all([loadLayer(), refresh()]);
}

async function updateEntry() {
  if (!selectedEntry.value?.id) return;
  actionResult.value = await api.updateMemoryEntry(selectedEntry.value.id, {
    content: entryContent.value,
    priority: entryPriority.value,
    tags: entryTags.value.split(',').map((tag) => tag.trim()).filter(Boolean),
  });
  await loadLayer();
}

async function deleteEntry() {
  if (!selectedEntry.value?.id) return;
  actionResult.value = await api.deleteMemoryEntry(selectedLayer.value, selectedEntry.value.id);
  await loadLayer();
}

async function scanMaintenance() {
  maintenanceScan.value = await api.scanMemoryMaintenance({ max_candidates: 40 });
  maintenance.value = maintenanceScan.value;
}

async function markCandidate(id: string, nextStatus: string) {
  actionResult.value = await api.updateMemoryMaintenance(id, nextStatus);
  maintenance.value = await api.memoryMaintenance();
}

async function planStructuredIngest() {
  structuredPlan.value = await api.structuredIngestPlan({
    source_ref: sourceRef.value,
    fact_type: factType.value,
    estimated_rows: 128,
    raw_checksum: 'sha256:memory-webui-v0.9.229',
    metric_ids: ['torque_deviation_rate', 'station_quality_escape'],
  });
}

function summarize(value: any, fallback = '-') {
  if (value === null || value === undefined || value === '') return fallback;
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value);
  return JSON.stringify(value).slice(0, 160);
}

onMounted(refresh);
</script>

<template>
  <section class="capability-page memory-page">
    <header class="page-header">
      <div>
        <h1>Memory Graph</h1>
        <p>长期记忆、事实图谱、召回解释、维护候选和结构化数据内核统一管理。</p>
      </div>
      <button class="primary-action" type="button" :disabled="loading" @click="refresh">
        <RefreshCw :size="15" />
        {{ loading ? 'Loading' : 'Refresh memory' }}
      </button>
    </header>

    <p v-if="error" class="settings-alert">{{ error }}</p>

    <section class="metric-row memory-overview">
      <article class="metric-card">
        <span>Kernel health</span>
        <strong>{{ healthLevel }}</strong>
        <small>{{ status.enabled === false ? 'memory disabled' : `${stats.total_entries || 0} entries` }}</small>
      </article>
      <article class="metric-card" data-tone="info">
        <span>Graph objects</span>
        <strong>{{ stats.entity_count || 0 }}/{{ stats.triple_count || 0 }}</strong>
        <small>entities / triples</small>
      </article>
      <article class="metric-card" data-tone="success">
        <span>Links</span>
        <strong>{{ linkCount }}</strong>
        <small>{{ stats.vector_count || 0 }} vectors indexed</small>
      </article>
    </section>

    <section class="memory-workbench">
      <nav class="memory-sections" aria-label="Memory workbench sections">
        <a href="#memory-layers"><Database :size="15" /> Layers</a>
        <a href="#memory-recall"><Search :size="15" /> Recall</a>
        <a href="#memory-graph"><GitBranch :size="15" /> Graph</a>
        <a href="#memory-maintenance"><ShieldCheck :size="15" /> Maintenance</a>
        <a href="#structured-core"><Network :size="15" /> Structured data</a>
      </nav>

      <main class="memory-main">
        <section id="memory-layers" class="management-panel memory-panel wide">
          <header>
            <h2>Layer entries</h2>
            <span>{{ entries.length }} entries</span>
          </header>
          <div class="memory-split">
            <aside class="memory-list">
              <div class="filter-row">
                <select v-model="selectedLayer" @change="loadLayer">
                  <option v-for="layer in ['L0', 'L1', 'L2', 'L3', 'L4']" :key="layer" :value="layer">{{ layer }}</option>
                </select>
                <StatusPill :status="layerEntries.enabled === false ? 'offline' : 'ready'" />
              </div>
              <button
                v-for="entry in entries"
                :key="entry.id"
                class="memory-entry-row"
                :class="{ active: selectedEntryId === entry.id }"
                type="button"
                @click="selectedEntryId = entry.id"
              >
                <strong>{{ entry.title || entry.id }}</strong>
                <span>{{ entry.content || entry.summary || entry.id }}</span>
                <small>{{ entry.category || '-' }} · {{ entry.priority || '-' }}</small>
              </button>
              <EmptyState v-if="!entries.length" title="No entries in this layer" detail="新增条目后会出现在对应 L0-L4 记忆层。" />
            </aside>
            <article class="memory-detail">
              <dl class="detail-list">
                <dt>ID</dt>
                <dd>{{ selectedEntry?.id || '-' }}</dd>
                <dt>Layer</dt>
                <dd>{{ selectedEntry?.layer || selectedLayer }}</dd>
                <dt>Scope</dt>
                <dd>{{ selectedEntry?.scope || '-' }}</dd>
                <dt>Tags</dt>
                <dd>{{ (selectedEntry?.tags || []).join(', ') || '-' }}</dd>
              </dl>
              <label class="field-line">
                Title
                <input v-model="entryTitle" type="text" />
              </label>
              <label class="field-line">
                Content
                <textarea v-model="entryContent" rows="5" />
              </label>
              <div class="memory-form-row">
                <label class="field-line">
                  Priority
                  <select v-model="entryPriority">
                    <option>critical</option>
                    <option>high</option>
                    <option>normal</option>
                    <option>low</option>
                  </select>
                </label>
                <label class="field-line">
                  Tags
                  <input v-model="entryTags" type="text" />
                </label>
              </div>
              <div class="button-row">
                <button class="primary-action" type="button" @click="createEntry">Register memory fact</button>
                <button class="ghost-action" type="button" :disabled="!selectedEntry" @click="updateEntry">Update selected</button>
                <button class="ghost-action" type="button" :disabled="!selectedEntry" @click="deleteEntry">Archive selected</button>
              </div>
              <RequestReceipt :receipt="actionResult" title="Memory layer receipt" />
            </article>
          </div>
        </section>

        <section id="memory-recall" class="management-panel memory-panel">
          <header>
            <h2>Search, recall, packet</h2>
            <span>{{ recallExplain.total || 0 }} matches</span>
          </header>
          <label class="search-field">
            <Search :size="15" />
            <input v-model="query" type="search" placeholder="Query memory" @keyup.enter="runRecall" />
          </label>
          <div class="button-row">
            <button class="primary-action" type="button" @click="runRecall">Run recall</button>
          </div>
          <DataTable v-if="recallRows.length" :rows="recallRows" :columns="['title', 'layer', 'priority', 'score', 'snippet']" />
          <EmptyState v-else title="No recall results" detail="当前查询没有匹配，或后端处于离线状态。" />
          <RawPayload title="Context packet" :data="packet" />
        </section>

        <section id="memory-graph" class="management-panel memory-panel">
          <header>
            <h2>Structured memory graph</h2>
            <span>{{ entityRows.length }} entities shown</span>
          </header>
          <div class="memory-tabs">
            <article>
              <h3>Entities</h3>
              <DataTable v-if="entityRows.length" :rows="entityRows" />
              <EmptyState v-else title="No entities" detail="实体抽取结果会展示在这里。" />
            </article>
            <article>
              <h3>Triples</h3>
              <DataTable v-if="tripleRows.length" :rows="tripleRows" />
              <EmptyState v-else title="No triples" detail="事实三元组会展示在这里。" />
            </article>
          </div>
          <label class="field-line">
            Symbol lookup
            <input v-model="symbolQuery" type="text" @keyup.enter="runRecall" />
          </label>
          <RawPayload title="Symbol links" :data="symbolLinks" />
          <RawPayload title="Clusters and runtime" :data="{ clusters, runtime, links }" />
        </section>

        <section id="memory-maintenance" class="management-panel memory-panel">
          <header>
            <h2>Maintenance</h2>
            <span>{{ candidateRows.length }} candidates</span>
          </header>
          <button class="primary-action" type="button" @click="scanMaintenance">Scan candidates</button>
          <RequestReceipt :receipt="maintenanceScan || actionResult" title="Maintenance receipt" />
          <div class="maintenance-list">
            <article v-for="candidate in candidateRows.slice(0, 12)" :key="candidate.id || candidate.memory_id">
              <div>
                <strong>{{ candidate.kind || candidate.reason || candidate.id }}</strong>
                <p>{{ candidate.summary || candidate.description || summarize(candidate) }}</p>
              </div>
              <div class="button-row" v-if="candidate.id">
                <button class="ghost-action" type="button" @click="markCandidate(candidate.id, 'acknowledged')">Ack</button>
                <button class="ghost-action" type="button" @click="markCandidate(candidate.id, 'dismissed')">Dismiss</button>
              </div>
            </article>
          </div>
          <EmptyState v-if="!candidateRows.length" title="No maintenance candidates" detail="扫描后会列出陈旧、冲突、重复和权威提升候选。" />
          <RequestReceipt :receipt="actionResult" title="Maintenance action receipt" />
          <RawPayload title="Performance" :data="performance" />
        </section>

        <section id="structured-core" class="management-panel memory-panel wide">
          <header>
            <h2>Structured data core</h2>
            <span>cowd kernel data substrate</span>
          </header>
          <p>结构化数据作为 cowd 底层数据处理能力沉淀，MFG 只作为制造领域应用在其上使用。</p>
          <div class="memory-form-row">
            <label class="field-line">
              Source ref
              <input v-model="sourceRef" type="text" />
            </label>
            <label class="field-line">
              Fact type
              <input v-model="factType" type="text" />
            </label>
          </div>
          <button class="primary-action" type="button" @click="planStructuredIngest">Plan manufacturing ingest</button>
          <RequestReceipt :receipt="structuredPlan" title="Structured ingest plan receipt" />
          <RawPayload title="Structured collections" :data="structured" />
          <RawPayload title="Ingest plan" :data="structuredPlan || {}" />
        </section>

        <section class="management-panel memory-panel">
          <header>
          <h2>Action evidence</h2>
          <span>latest write response</span>
        </header>
          <RequestReceipt :receipt="actionResult || structuredPlan" title="Memory action receipt" />
          <RawPayload title="Action result" :data="actionResult || searchResult" />
        </section>
      </main>
    </section>
  </section>
</template>
