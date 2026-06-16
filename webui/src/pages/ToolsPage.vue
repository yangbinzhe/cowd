<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { Play, RefreshCw, ShieldCheck } from 'lucide-vue-next';
import { api } from '../api/client';
import DataTable from '../components/workbench/DataTable.vue';
import EmptyState from '../components/workbench/EmptyState.vue';
import RawPayload from '../components/workbench/RawPayload.vue';

const loading = ref(false);
const error = ref('');
const state = ref<any>({});
const result = ref<any>(null);
const selectedCommand = ref('/status');
const selectedCapability = ref('service.read');
const actor = ref('webui-operator');

const tools = computed(() => Array.isArray(state.value.tools?.tools) ? state.value.tools.tools : []);
const commands = computed(() => Array.isArray(state.value.commands?.commands) ? state.value.commands.commands : []);
const history = computed(() => Array.isArray(state.value.history?.history) ? state.value.history.history : []);
const toolRows = computed(() => tools.value.map((tool: any) => ({
  name: tool.name,
  enabled: tool.enabled === false ? 'no' : 'yes',
  description: tool.description || '-',
})));
const commandRows = computed(() => commands.value.map((command: any) => ({
  name: command.name,
  action: command.action,
  target: command.target,
  description: command.description || '-',
})));
const historyRows = computed(() => history.value.slice(0, 14).map((item: any) => ({
  command: item.command || item.name || '-',
  action: item.action || '-',
  target: item.target || '-',
  at: item.executed_at_ms || item.timestamp || '-',
})));

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

async function refresh() {
  loading.value = true;
  error.value = '';
  try {
    const [toolsData, commandsData, historyData, capabilities, crossPlane] = await Promise.all([
      api.toolRegistry(),
      api.commands(),
      api.commandHistory(),
      api.cowdCapabilities(),
      api.crossPlaneSummary(),
    ]);
    state.value = { tools: toolsData, commands: commandsData, history: historyData, capabilities, crossPlane };
    selectedCommand.value = selectedCommand.value || commands.value[0]?.name || '/status';
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  } finally {
    loading.value = false;
  }
}

async function executeCommand() {
  result.value = await api.executeCommand(selectedCommand.value, {});
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
        <p>工具目录、斜杠命令、执行历史和风险预检集中管理。</p>
      </div>
      <button class="primary-action" type="button" :disabled="loading" @click="refresh">
        <RefreshCw :size="15" />
        {{ loading ? 'Loading' : 'Refresh tools' }}
      </button>
    </header>

    <p v-if="error" class="settings-alert">{{ error }}</p>

    <section class="metric-row">
      <article class="metric-card" data-tone="success">
        <span>Tools</span>
        <strong>{{ tools.length }}</strong>
        <small>registered by backend</small>
      </article>
      <article class="metric-card" data-tone="info">
        <span>Commands</span>
        <strong>{{ commands.length }}</strong>
        <small>{{ history.length }} history items</small>
      </article>
      <article class="metric-card" data-tone="warn">
        <span>Cross-plane</span>
        <strong>{{ state.crossPlane?.interop?.actions_24h || 0 }}</strong>
        <small>policy governed actions</small>
      </article>
    </section>

    <section class="gateway-grid">
      <section class="management-panel gateway-panel wide">
        <header>
          <h2>Tool registry</h2>
          <span>{{ tools.length }} tools</span>
        </header>
        <DataTable v-if="toolRows.length" :rows="toolRows" :columns="['name', 'enabled', 'description']" />
        <EmptyState v-else title="No tools" detail="后端工具注册表为空或服务未启动。" />
      </section>

      <section class="management-panel gateway-panel">
        <header>
          <h2>Command execution</h2>
          <span>{{ selectedCommand }}</span>
        </header>
        <label class="field-line">
          Command
          <select v-model="selectedCommand">
            <option v-for="command in commands" :key="command.name" :value="command.name">{{ command.name }}</option>
          </select>
        </label>
        <button class="primary-action" type="button" @click="executeCommand">
          <Play :size="15" />
          Execute command
        </button>
        <DataTable v-if="commandRows.length" :rows="commandRows" :columns="['name', 'action', 'target', 'description']" />
      </section>

      <section class="management-panel gateway-panel">
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
      </section>

      <section class="management-panel gateway-panel">
        <header>
          <h2>Command and risk history</h2>
          <span>{{ historyRows.length }} shown</span>
        </header>
        <DataTable v-if="historyRows.length" :rows="historyRows" :columns="['command', 'action', 'target', 'at']" />
        <EmptyState v-else title="No command history" detail="命令执行后会记录在后端历史中。" />
        <RawPayload title="Tool action result" :data="result || state.capabilities || {}" />
      </section>
    </section>
  </section>
</template>
