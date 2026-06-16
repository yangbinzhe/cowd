<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { Network, RefreshCw, ShieldCheck } from 'lucide-vue-next';
import { api } from '../api/client';
import DataTable from '../components/workbench/DataTable.vue';
import EmptyState from '../components/workbench/EmptyState.vue';
import RawPayload from '../components/workbench/RawPayload.vue';
import StatusPill from '../components/workbench/StatusPill.vue';

const loading = ref(false);
const error = ref('');
const state = ref<any>({});
const actionResult = ref<any>(null);
const resourceRef = ref('');
const actor = ref('webui-operator');
const capability = ref('service.read');

const accounts = computed(() => Array.isArray(state.value.accounts?.accounts) ? state.value.accounts.accounts : []);
const capabilities = computed(() => Array.isArray(state.value.capabilities?.capabilities) ? state.value.capabilities.capabilities : []);
const resources = computed(() => Array.isArray(state.value.resources?.resources) ? state.value.resources.resources : Array.isArray(state.value.resources?.items) ? state.value.resources.items : []);
const mcpServers = computed(() => Array.isArray(state.value.mcp?.servers) ? state.value.mcp.servers : []);
const executions = computed(() => Array.isArray(state.value.executions?.executions) ? state.value.executions.executions : []);
const accountRows = computed(() => accounts.value.map((item: any) => ({
  provider: item.provider || item.provider_id || item.id,
  account: item.account_id || item.id || '-',
  status: item.status || (item.enabled === false ? 'disabled' : 'ready'),
  scopes: (item.scopes || item.enabled_bindings || []).join(', '),
})));
const capabilityRows = computed(() => capabilities.value.slice(0, 14).map((item: any) => ({
  id: item.id || item.capability_id || item.name,
  provider: item.provider || '-',
  risk: item.risk || item.risk_level || '-',
  mode: item.mode || item.access || '-',
})));
const resourceRows = computed(() => resources.value.slice(0, 14).map((item: any) => ({
  reference: item.reference || item.resource_ref || item.id,
  title: item.title || item.name || '-',
  kind: item.kind || item.mime || '-',
  status: item.status || '-',
})));
const executionRows = computed(() => executions.value.slice(0, 12).map((item: any) => ({
  id: item.execution_id || item.id,
  status: item.status || item.decision || '-',
  capability: item.requested_capability || item.capability || '-',
  provider: item.provider_account || item.provider || '-',
})));

async function refresh() {
  loading.value = true;
  error.value = '';
  try {
    const [platforms, summary, nextAccounts, nextCapabilities, nextResources, mcp, crossPlane, audit, adapters, nextExecutions] = await Promise.all([
      api.platforms(),
      api.connectorsSummary(),
      api.connectorAccounts(),
      api.connectorCapabilities(),
      api.connectorResources(),
      api.connectorMcpServers(),
      api.crossPlaneSummary(),
      api.crossPlaneAudit(),
      api.crossPlaneAdapters(),
      api.crossPlaneExecutions(),
    ]);
    state.value = { platforms, summary, accounts: nextAccounts, capabilities: nextCapabilities, resources: nextResources, mcp, crossPlane, audit, adapters, executions: nextExecutions };
    if (!resourceRef.value) {
      resourceRef.value = resources.value[0]?.reference || resources.value[0]?.resource_ref || '';
    }
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  } finally {
    loading.value = false;
  }
}

async function revalidateResource() {
  if (!resourceRef.value) return;
  actionResult.value = await api.connectorRevalidateResource(resourceRef.value);
  await refresh();
}

async function promoteResourceMemory() {
  if (!resourceRef.value) return;
  actionResult.value = await api.connectorPromoteMemory(resourceRef.value);
  await refresh();
}

async function runPreflight() {
  actionResult.value = await api.crossPlanePreflight({
    actor_principal: actor.value,
    source_channel: 'channel://webui/local',
    session_id: 'webui-gateway',
    requested_capability: capability.value,
    provider_account: accounts.value[0]?.account_id || accounts.value[0]?.id || 'webui-local',
    target_ref: null,
    resource_ref: resourceRef.value || null,
    risk: 'medium',
    data_classification: 'internal',
    identity_trust: 'unknown',
  });
  await refresh();
}

onMounted(refresh);
</script>

<template>
  <section class="capability-page gateway-page">
    <header class="page-header">
      <div>
        <h1>Gateway and Cross-plane</h1>
        <p>连接器账号、服务能力、资源治理、MCP 状态和跨平面执行门禁集中管理。</p>
      </div>
      <button class="primary-action" type="button" :disabled="loading" @click="refresh">
        <RefreshCw :size="15" />
        {{ loading ? 'Loading' : 'Refresh gateway' }}
      </button>
    </header>

    <p v-if="error" class="settings-alert">{{ error }}</p>

    <section class="metric-row">
      <article class="metric-card">
        <span>Accounts</span>
        <strong>{{ accounts.length }}</strong>
        <small>{{ state.summary?.status || 'connector registry' }}</small>
      </article>
      <article class="metric-card" data-tone="info">
        <span>Capabilities</span>
        <strong>{{ capabilities.length }}</strong>
        <small>{{ resources.length }} resources</small>
      </article>
      <article class="metric-card" data-tone="success">
        <span>Cross-plane</span>
        <strong>{{ executions.length }}</strong>
        <small>executions recorded</small>
      </article>
    </section>

    <section class="gateway-grid">
      <section class="management-panel gateway-panel wide">
        <header>
          <h2>Platforms and connectors</h2>
          <StatusPill :status="state.summary?.__offline ? 'offline' : 'ready'" />
        </header>
        <DataTable v-if="accountRows.length" :rows="accountRows" :columns="['provider', 'account', 'status', 'scopes']" />
        <EmptyState v-else title="No connector accounts" detail="配置平台账号后会在这里展示。" />
        <RawPayload title="Platforms" :data="state.platforms || {}" />
      </section>

      <section class="management-panel gateway-panel">
        <header>
          <h2>Connector capabilities</h2>
          <span>{{ capabilityRows.length }} shown</span>
        </header>
        <DataTable v-if="capabilityRows.length" :rows="capabilityRows" :columns="['id', 'provider', 'risk', 'mode']" />
        <EmptyState v-else title="No connector capabilities" detail="连接器能力清单为空或后端离线。" />
      </section>

      <section class="management-panel gateway-panel">
        <header>
          <h2>MCP servers</h2>
          <span>{{ mcpServers.length }} servers</span>
        </header>
        <RawPayload title="MCP server registry" :data="state.mcp || {}" />
      </section>

      <section class="management-panel gateway-panel wide">
        <header>
          <h2>Resources and memory promotion</h2>
          <span>{{ resources.length }} resources</span>
        </header>
        <label class="field-line">
          Resource ref
          <input v-model="resourceRef" type="text" />
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" :disabled="!resourceRef" @click="revalidateResource">Revalidate resource</button>
          <button class="primary-action" type="button" :disabled="!resourceRef" @click="promoteResourceMemory">Promote to memory</button>
        </div>
        <DataTable v-if="resourceRows.length" :rows="resourceRows" :columns="['reference', 'title', 'kind', 'status']" />
        <EmptyState v-else title="No connector resources" detail="资源桥接和记忆提升需要连接器返回资源。" />
      </section>

      <section class="management-panel gateway-panel">
        <header>
          <h2>Cross-plane governance</h2>
          <span>{{ state.crossPlane?.status || 'preflight' }}</span>
        </header>
        <label class="field-line">
          Actor
          <input v-model="actor" type="text" />
        </label>
        <label class="field-line">
          Capability
          <input v-model="capability" type="text" />
        </label>
        <button class="primary-action" type="button" @click="runPreflight">
          <ShieldCheck :size="15" />
          Run preflight
        </button>
        <RawPayload title="Cross-plane summary" :data="state.crossPlane || {}" />
      </section>

      <section class="management-panel gateway-panel">
        <header>
          <h2>Audit and executions</h2>
          <span>{{ executionRows.length }} executions</span>
        </header>
        <DataTable v-if="executionRows.length" :rows="executionRows" :columns="['id', 'status', 'capability', 'provider']" />
        <EmptyState v-else title="No executions" detail="跨平面动作执行后会在这里展示。" />
        <RawPayload title="Gateway action result" :data="actionResult || state.audit || {}" />
      </section>
    </section>
  </section>
</template>
