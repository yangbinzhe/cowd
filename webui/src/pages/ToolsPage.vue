<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { GitBranch, Play, RefreshCw, ShieldCheck } from 'lucide-vue-next';
import { api } from '../api/client';
import DataTable from '../components/workbench/DataTable.vue';
import EmptyState from '../components/workbench/EmptyState.vue';
import RawPayload from '../components/workbench/RawPayload.vue';
import RequestReceipt from '../components/workbench/RequestReceipt.vue';
import { useAppStore } from '../stores/app';

const store = useAppStore();
const loading = ref(false);
const error = ref('');
const state = ref<any>({});
const result = ref<any>(null);
const selectedTool = ref('tool_cache_stats');
const selectedCommand = ref('/status');
const selectedCapability = ref('service.read');
const actor = ref('webui-operator');
const plannerPrompt = ref('review current workspace and route safe read-only context collection');
const fanoutPrompt = ref('inspect workspace README, package files, and recent runtime evidence');
const batchConcurrency = ref(4);
const batchCallsText = ref(JSON.stringify([
  { name: 'workspace_snapshot', input: {} },
  { name: 'tool_cache_stats', input: {} },
], null, 2));
const mutationEditsText = ref(JSON.stringify([
  { path: 'README.md', old_string: 'Cowd', new_string: 'Cowd' },
], null, 2));
const checkpointLabel = ref('webui-tool-ops');
const selectedCheckpointId = ref('');
const restoreArmedId = ref('');
const expectedHashes = ref<Record<string, string>>({});

const tools = computed(() => Array.isArray(state.value.tools?.tools) ? state.value.tools.tools : []);
const commands = computed(() => Array.isArray(state.value.commands?.commands) ? state.value.commands.commands : []);
const history = computed(() => Array.isArray(state.value.history?.history) ? state.value.history.history : []);
const checkpoints = computed(() => {
  const payload = state.value.checkpoints?.data || state.value.checkpoints || {};
  return Array.isArray(payload.checkpoints) ? payload.checkpoints : [];
});
const cacheStats = computed(() => state.value.cache?.data || state.value.cache || {});
const timelineEvents = computed(() => Array.isArray(state.value.timeline?.events) ? state.value.timeline.events : []);
const toolLedger = computed(() => timelineEvents.value.filter((event: any) => String(event.kind || '').includes('tool')));

const toolRows = computed(() => tools.value.map((tool: any) => ({
  name: tool.name,
  enabled: tool.enabled === false ? 'no' : 'yes',
  safety: tool.safety_category || '-',
  cache: tool.cache_policy || '-',
  readonly: tool.prepared_readonly_supported ? 'yes' : 'no',
  concurrency: tool.max_concurrency || '-',
  tags: Array.isArray(tool.managed_tags) ? tool.managed_tags.join(', ') : '-',
})));
const commandRows = computed(() => commands.value.map((command: any) => ({
  name: command.name,
  action: command.action,
  target: command.target,
  description: command.description || '-',
})));
const historyRows = computed(() => history.value.slice(0, 12).map((item: any) => ({
  command: item.command || item.name || '-',
  action: item.action || '-',
  target: item.target || '-',
  at: item.executed_at_ms || item.timestamp || '-',
})));
const checkpointRows = computed(() => checkpoints.value.map((checkpoint: any) => ({
  id: checkpoint.id || checkpoint.checkpoint_id || '-',
  label: checkpoint.label || '-',
  files: checkpoint.file_count || checkpoint.files || 0,
  created: checkpoint.created_at || checkpoint.created_at_ms || '-',
})));
const cacheRows = computed(() => Object.entries(cacheStats.value)
  .filter(([key]) => !key.startsWith('__'))
  .map(([key, value]) => ({
    metric: key,
    value: typeof value === 'object' ? JSON.stringify(value) : String(value),
  })));
const fanoutRows = computed(() => {
  const plan = result.value?.data?.data || result.value?.data || result.value || {};
  const calls = Array.isArray(plan.tool_calls) ? plan.tool_calls : Array.isArray(plan.calls) ? plan.calls : [];
  return calls.map((call: any) => ({
    tool: call.name || call.tool_name || '-',
    purpose: call.purpose || call.reason || '-',
    cache: call.cache_policy || '-',
    risk: call.safety_category || call.risk || '-',
  }));
});
const mutationPreviewRows = computed(() => {
  const payload = result.value?.data?.data || result.value?.data || result.value || {};
  const files = Array.isArray(payload.files) ? payload.files : Array.isArray(payload.preview) ? payload.preview : [];
  return files.map((file: any) => ({
    path: file.path || '-',
    status: file.status || file.action || '-',
    expected_hash: file.expected_hash || file.expectedHash || '-',
    changed: file.changed === false ? 'no' : 'yes',
  }));
});
const ledgerRows = computed(() => toolLedger.value.slice(0, 16).map((event: any) => ({
  seq: event.sequence || event.seq || '-',
  kind: event.kind || '-',
  status: event.status || event.level || '-',
  tool: event.tool || event.tool_name || event.name || '-',
  at: event.timestamp || event.created_at || '-',
})));
const activeSessionId = computed(() => store.currentSessionId || 'api-context');

function parseJsonArray(text: string, label: string) {
  const parsed = JSON.parse(text);
  if (!Array.isArray(parsed)) throw new Error(`${label} must be a JSON array`);
  return parsed;
}

function actionPayload() {
  return {
    actor_principal: actor.value,
    actor_identity_ref: `user:${actor.value}`,
    source_channel: 'channel://webui/tools',
    session_id: 'webui-tools',
    requested_capability: selectedCapability.value,
    provider_account: null,
    target_ref: null,
    resource_ref: null,
    risk: 'medium',
    data_classification: 'internal',
    identity_trust: 'unknown',
  };
}

function extractExpectedHashes(receipt: any) {
  const files = receipt?.data?.data?.files || receipt?.data?.files || receipt?.files || [];
  const hashes: Record<string, string> = {};
  if (Array.isArray(files)) {
    for (const file of files) {
      const path = file.path;
      const hash = file.expected_hash || file.expectedHash;
      if (path && hash) hashes[path] = hash;
    }
  }
  expectedHashes.value = hashes;
}

async function refresh() {
  loading.value = true;
  error.value = '';
  try {
    const [toolsData, commandsData, historyData, capabilities, crossPlane, cache, checkpointsData, timeline] = await Promise.all([
      api.toolRegistry(),
      api.commands(),
      api.commandHistory(),
      api.cowdCapabilities(),
      api.crossPlaneSummary(),
      api.toolCacheStats(),
      api.toolCheckpoints(),
      api.runtimeTimeline(activeSessionId.value),
    ]);
    state.value = { tools: toolsData, commands: commandsData, history: historyData, capabilities, crossPlane, cache, checkpoints: checkpointsData, timeline };
    selectedTool.value = selectedTool.value || tools.value[0]?.name || 'tool_cache_stats';
    selectedCommand.value = selectedCommand.value || commands.value[0]?.name || '/status';
    selectedCheckpointId.value = selectedCheckpointId.value || checkpointRows.value[0]?.id || '';
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  } finally {
    loading.value = false;
  }
}

async function runSafeTool() {
  result.value = await api.writeReceipt('/api/tools/execute', {
    method: 'POST',
    body: JSON.stringify({ name: selectedTool.value, input: {}, mode: 'read_only' }),
  });
  await refresh();
}

async function executeCommand() {
  result.value = await api.executeCommand(selectedCommand.value, {});
  await refresh();
}

async function planIntent() {
  result.value = await api.writeReceipt('/api/tools/intent-plan', {
    method: 'POST',
    body: JSON.stringify({ prompt: plannerPrompt.value, selected_tools: tools.value.map((tool: any) => tool.name).slice(0, 8) }),
  });
}

async function planFanout() {
  result.value = await api.writeReceipt('/api/tools/context-fanout/plan', {
    method: 'POST',
    body: JSON.stringify({ prompt: fanoutPrompt.value }),
  });
}

async function runBatchReadonly() {
  try {
    const calls = parseJsonArray(batchCallsText.value, 'Batch calls');
    result.value = await api.writeReceipt('/api/tools/batch-readonly', {
      method: 'POST',
      body: JSON.stringify({ calls, max_concurrency: Number(batchConcurrency.value) || 4 }),
    });
    await refresh();
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  }
}

async function previewMutation() {
  try {
    const edits = parseJsonArray(mutationEditsText.value, 'Mutation edits');
    result.value = await api.writeReceipt('/api/tools/mutations/preview', {
      method: 'POST',
      body: JSON.stringify({ edits }),
    });
    extractExpectedHashes(result.value);
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  }
}

async function applyMutation() {
  try {
    const edits = parseJsonArray(mutationEditsText.value, 'Mutation edits');
    result.value = await api.writeReceipt('/api/tools/mutations/apply', {
      method: 'POST',
      body: JSON.stringify({ edits, expected_hashes: expectedHashes.value }),
    });
    await refresh();
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  }
}

async function createCheckpoint() {
  result.value = await api.writeReceipt('/api/tools/checkpoints', {
    method: 'POST',
    body: JSON.stringify({ label: checkpointLabel.value || undefined }),
  });
  await refresh();
}

async function diffCheckpoint(id = selectedCheckpointId.value) {
  if (!id) return;
  result.value = await api.toolCheckpointDiff(id);
}

async function restoreCheckpoint(id = selectedCheckpointId.value) {
  if (!id) return;
  if (restoreArmedId.value !== id) {
    restoreArmedId.value = id;
    return;
  }
  result.value = await api.writeReceipt(`/api/tools/checkpoints/${encodeURIComponent(id)}/restore`, { method: 'POST' });
  restoreArmedId.value = '';
  await refresh();
}

async function runPreflight() {
  result.value = await api.crossPlanePreflight(actionPayload());
}

async function simulatePolicy() {
  result.value = await api.crossPlanePolicySimulate(actionPayload());
}

onMounted(refresh);
</script>

<template>
  <section class="capability-page tools-page">
    <header class="page-header">
      <div>
        <h1>Tools Registry</h1>
        <p>工具目录、执行规划、只读批处理、事务变更、checkpoint、缓存与运行流水集中管理。</p>
      </div>
      <button class="primary-action" type="button" :disabled="loading" @click="refresh">
        <RefreshCw :size="15" />
        {{ loading ? 'Loading' : 'Refresh tools' }}
      </button>
    </header>

    <p v-if="error" class="settings-alert">{{ error }}</p>

    <section class="metric-row tools-metrics">
      <article class="metric-card" data-tone="success">
        <span>Tools</span>
        <strong>{{ tools.length }}</strong>
        <small>{{ toolRows.filter((row) => row.readonly === 'yes').length }} readonly prepared</small>
      </article>
      <article class="metric-card" data-tone="info">
        <span>Checkpoints</span>
        <strong>{{ checkpoints.length }}</strong>
        <small>workspace rollback points</small>
      </article>
      <article class="metric-card" data-tone="warn">
        <span>Cache</span>
        <strong>{{ cacheRows.length }}</strong>
        <small>stats exposed by runtime</small>
      </article>
      <article class="metric-card" data-tone="neutral">
        <span>Ledger</span>
        <strong>{{ toolLedger.length }}</strong>
        <small>tool events in session</small>
      </article>
    </section>

    <section class="gateway-grid">
      <section class="management-panel gateway-panel wide" data-section="registry">
        <header>
          <h2>Tool registry</h2>
          <span>{{ tools.length }} tools</span>
        </header>
        <DataTable v-if="toolRows.length" :rows="toolRows" :columns="['name', 'enabled', 'safety', 'cache', 'readonly', 'concurrency', 'tags']" />
        <EmptyState v-else title="No tools" detail="后端工具注册表为空或服务未启动。" />
        <div class="button-row">
          <label class="field-line compact-field">
            Safe tool
            <select v-model="selectedTool">
              <option v-for="tool in tools" :key="tool.name" :value="tool.name">{{ tool.name }}</option>
              <option value="tool_cache_stats">tool_cache_stats</option>
            </select>
          </label>
          <button class="primary-action" type="button" @click="runSafeTool">
            <Play :size="15" />
            Safe execute
          </button>
        </div>
      </section>

      <section class="management-panel gateway-panel wide" data-section="operations">
        <header>
          <h2>Execution planner</h2>
          <span>intent, fanout, readonly batch</span>
        </header>
        <label class="field-line">
          Intent prompt
          <textarea v-model="plannerPrompt" rows="3" />
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" @click="planIntent">Plan intent</button>
          <button class="ghost-action" type="button" @click="planFanout">
            <GitBranch :size="15" />
            Plan context fanout
          </button>
        </div>
        <label class="field-line">
          Fanout prompt
          <textarea v-model="fanoutPrompt" rows="3" />
        </label>
        <DataTable v-if="fanoutRows.length" :rows="fanoutRows" :columns="['tool', 'purpose', 'cache', 'risk']" />
        <label class="field-line">
          Readonly batch calls
          <textarea v-model="batchCallsText" rows="6" />
        </label>
        <div class="button-row">
          <label class="field-line compact-field">
            Concurrency
            <input v-model.number="batchConcurrency" min="1" max="12" type="number" />
          </label>
          <button class="primary-action" type="button" @click="runBatchReadonly">Run readonly batch</button>
        </div>
      </section>

      <section class="management-panel gateway-panel wide" data-section="mutations">
        <header>
          <h2>Mutation transactions</h2>
          <span>preview before apply</span>
        </header>
        <label class="field-line">
          Workspace edits
          <textarea v-model="mutationEditsText" rows="7" />
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" @click="previewMutation">Preview mutation</button>
          <button class="primary-action" type="button" @click="applyMutation">Apply transaction</button>
        </div>
        <DataTable v-if="mutationPreviewRows.length" :rows="mutationPreviewRows" :columns="['path', 'status', 'expected_hash', 'changed']" />
        <RawPayload title="Expected hashes" :data="expectedHashes" />
      </section>

      <section class="management-panel gateway-panel" data-section="checkpoints">
        <header>
          <h2>Checkpoints</h2>
          <span>{{ checkpointRows.length }} available</span>
        </header>
        <label class="field-line">
          Label
          <input v-model="checkpointLabel" type="text" />
        </label>
        <button class="primary-action" type="button" @click="createCheckpoint">Create checkpoint</button>
        <label class="field-line">
          Checkpoint
          <select v-model="selectedCheckpointId">
            <option value="">Select checkpoint</option>
            <option v-for="checkpoint in checkpointRows" :key="checkpoint.id" :value="checkpoint.id">{{ checkpoint.label }} · {{ checkpoint.id }}</option>
          </select>
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" @click="diffCheckpoint()">Diff checkpoint</button>
          <button class="danger-action" type="button" @click="restoreCheckpoint()">
            {{ restoreArmedId === selectedCheckpointId ? 'Confirm restore' : 'Restore checkpoint' }}
          </button>
        </div>
        <DataTable v-if="checkpointRows.length" :rows="checkpointRows" :columns="['id', 'label', 'files', 'created']" />
        <EmptyState v-else title="No checkpoints" detail="创建 checkpoint 后可进行 diff 与显式二次确认恢复。" />
      </section>

      <section class="management-panel gateway-panel" data-section="cache">
        <header>
          <h2>Tool cache</h2>
          <span>{{ cacheRows.length }} metrics</span>
        </header>
        <DataTable v-if="cacheRows.length" :rows="cacheRows" :columns="['metric', 'value']" />
        <EmptyState v-else title="No cache stats" detail="服务未启动或运行时尚未产生缓存统计。" />
      </section>

      <section class="management-panel gateway-panel wide" data-section="ledger">
        <header>
          <h2>Tool ledger</h2>
          <span>{{ ledgerRows.length }} recent events</span>
        </header>
        <DataTable v-if="ledgerRows.length" :rows="ledgerRows" :columns="['seq', 'kind', 'status', 'tool', 'at']" />
        <EmptyState v-else title="No tool ledger events" detail="执行工具后，runtime timeline 中的工具事件会在这里聚合展示。" />
        <RequestReceipt :receipt="result" title="Tool operation receipt" />
        <RawPayload title="Tool operation payload" :data="result || state.cache || {}" />
      </section>

      <section class="management-panel gateway-panel" data-section="risk">
        <header>
          <h2>Risk preflight</h2>
          <span>{{ selectedCapability }}</span>
        </header>
        <label class="field-line">
          Actor
          <input v-model="actor" type="text" />
        </label>
        <label class="field-line">
          Capability
          <input v-model="selectedCapability" type="text" />
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" @click="simulatePolicy">Simulate policy</button>
          <button class="primary-action" type="button" @click="runPreflight">
            <ShieldCheck :size="15" />
            Run preflight
          </button>
        </div>
        <DataTable v-if="commandRows.length" :rows="commandRows" :columns="['name', 'action', 'target', 'description']" />
      </section>

      <section class="management-panel gateway-panel" data-section="risk">
        <header>
          <h2>Command and risk history</h2>
          <span>{{ historyRows.length }} shown</span>
        </header>
        <label class="field-line">
          Command
          <select v-model="selectedCommand">
            <option v-for="command in commands" :key="command.name" :value="command.name">{{ command.name }}</option>
            <option value="/status">/status</option>
          </select>
        </label>
        <button class="ghost-action" type="button" @click="executeCommand">
          <Play :size="15" />
          Execute command
        </button>
        <DataTable v-if="historyRows.length" :rows="historyRows" :columns="['command', 'action', 'target', 'at']" />
        <EmptyState v-else title="No command history" detail="命令执行后会记录在后端历史中。" />
      </section>
    </section>
  </section>
</template>
