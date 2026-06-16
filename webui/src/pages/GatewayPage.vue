<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { Network, RefreshCw, ShieldCheck } from 'lucide-vue-next';
import { api } from '../api/client';
import DataTable from '../components/workbench/DataTable.vue';
import EmptyState from '../components/workbench/EmptyState.vue';
import RawPayload from '../components/workbench/RawPayload.vue';
import RequestReceipt from '../components/workbench/RequestReceipt.vue';
import StatusPill from '../components/workbench/StatusPill.vue';

const loading = ref(false);
const error = ref('');
const state = ref<any>({});
const actionResult = ref<any>(null);
const resourceRef = ref('');
const actor = ref('webui-operator');
const capability = ref('service.read');
const identityRef = ref('user:webui-operator');
const identityId = ref('');
const grantId = ref('');
const executeMode = ref('dry_run');
const idempotencyKey = ref('');

const accounts = computed(() => Array.isArray(state.value.accounts?.accounts) ? state.value.accounts.accounts : []);
const capabilities = computed(() => Array.isArray(state.value.capabilities?.capabilities) ? state.value.capabilities.capabilities : []);
const resources = computed(() => Array.isArray(state.value.resources?.resources) ? state.value.resources.resources : Array.isArray(state.value.resources?.items) ? state.value.resources.items : []);
const mcpServers = computed(() => Array.isArray(state.value.mcp?.servers) ? state.value.mcp.servers : []);
const executions = computed(() => Array.isArray(state.value.executions?.executions) ? state.value.executions.executions : []);
const identities = computed(() => Array.isArray(state.value.identities?.identities) ? state.value.identities.identities : []);
const grants = computed(() => Array.isArray(state.value.grants?.grants) ? state.value.grants.grants : []);
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
const identityRows = computed(() => identities.value.slice(0, 12).map((item: any) => ({
  id: item.id,
  principal: item.principal_id,
  identity: item.identity_ref,
  trust: item.trust,
})));
const grantRows = computed(() => grants.value.slice(0, 12).map((item: any) => ({
  id: item.id,
  principal: item.principal_id,
  capability: item.capability,
  type: item.grant_type,
})));

function crossPlaneAction() {
  return {
    actor_principal: actor.value,
    actor_identity_ref: identityRef.value || null,
    source_channel: 'channel://webui/local',
    session_id: 'webui-gateway',
    requested_capability: capability.value,
    provider_account: accounts.value[0]?.account_id || accounts.value[0]?.id || 'webui-local',
    target_ref: null,
    resource_ref: resourceRef.value || null,
    risk: 'medium',
    data_classification: 'internal',
    identity_trust: 'unknown',
  };
}

async function refresh() {
  loading.value = true;
  error.value = '';
  try {
    const [platforms, summary, nextAccounts, nextCapabilities, nextResources, mcp, crossPlane, identitiesData, grantsData, audit, adapters, nextExecutions] = await Promise.all([
      api.platforms(),
      api.connectorsSummary(),
      api.connectorAccounts(),
      api.connectorCapabilities(),
      api.connectorResources(),
      api.connectorMcpServers(),
      api.crossPlaneSummary(),
      api.crossPlaneIdentities(),
      api.crossPlaneGrants(),
      api.crossPlaneAudit(),
      api.crossPlaneAdapters(),
      api.crossPlaneExecutions(),
    ]);
    state.value = { platforms, summary, accounts: nextAccounts, capabilities: nextCapabilities, resources: nextResources, mcp, crossPlane, identities: identitiesData, grants: grantsData, audit, adapters, executions: nextExecutions };
    if (!resourceRef.value) {
      resourceRef.value = resources.value[0]?.reference || resources.value[0]?.resource_ref || '';
    }
    identityId.value = identityId.value || identities.value[0]?.id || '';
    grantId.value = grantId.value || grants.value[0]?.id || '';
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
  actionResult.value = await api.crossPlanePreflight(crossPlaneAction());
  await refresh();
}

async function simulatePolicy() {
  actionResult.value = await api.crossPlanePolicySimulate(crossPlaneAction());
  await refresh();
}

async function executeCrossPlaneAction() {
  actionResult.value = await api.crossPlaneExecute(crossPlaneAction(), executeMode.value, idempotencyKey.value || undefined);
  await refresh();
}

async function createIdentity() {
  actionResult.value = await api.crossPlaneCreateIdentity({
    id: identityId.value || `idb-webui-${Date.now()}`,
    principal_id: actor.value,
    identity_ref: identityRef.value,
    trust: 'verified',
    source: 'webui',
    created_at: new Date().toISOString(),
    expires_at: null,
  });
  identityId.value = actionResult.value?.identity?.id || identityId.value;
  await refresh();
}

async function revokeIdentity() {
  if (!identityId.value) return;
  actionResult.value = await api.crossPlaneRevokeIdentity(identityId.value);
  identityId.value = '';
  await refresh();
}

async function resolveIdentity() {
  actionResult.value = await api.crossPlaneResolveIdentity(identityRef.value);
  await refresh();
}

async function createGrant() {
  actionResult.value = await api.crossPlaneCreateGrant({
    id: grantId.value || `grant-webui-${Date.now()}`,
    principal_id: actor.value,
    capability: capability.value,
    account_id: accounts.value[0]?.account_id || accounts.value[0]?.id || null,
    target_ref: null,
    resource_ref: resourceRef.value || null,
    source_channel: 'channel://webui/local',
    grant_type: 'persistent',
    expires_at: null,
    remaining_uses: null,
    created_by: 'webui',
    approval_id: null,
  });
  grantId.value = actionResult.value?.grant?.id || grantId.value;
  await refresh();
}

async function revokeGrant() {
  if (!grantId.value) return;
  actionResult.value = await api.crossPlaneRevokeGrant(grantId.value);
  grantId.value = '';
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
      <article class="metric-card" data-tone="info">
        <span>Identity/Grants</span>
        <strong>{{ identities.length }}/{{ grants.length }}</strong>
        <small>control-plane bindings</small>
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
        <RequestReceipt :receipt="actionResult" title="Resource receipt" />
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
        <label class="field-line">
          Identity ref
          <input v-model="identityRef" type="text" />
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" @click="simulatePolicy">Simulate policy</button>
          <button class="primary-action" type="button" @click="runPreflight">
            <ShieldCheck :size="15" />
            Run preflight
          </button>
        </div>
        <RequestReceipt :receipt="actionResult" title="Cross-plane readiness receipt" />
        <RawPayload title="Cross-plane summary" :data="state.crossPlane || {}" />
      </section>

      <section class="management-panel gateway-panel">
        <header>
          <h2>Identities and grants</h2>
          <span>{{ identities.length }} identities</span>
        </header>
        <label class="field-line">
          Identity binding id
          <input v-model="identityId" type="text" />
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" @click="createIdentity">Create identity</button>
          <button class="ghost-action" type="button" @click="resolveIdentity">Resolve identity</button>
          <button class="ghost-action" type="button" :disabled="!identityId" @click="revokeIdentity">Revoke identity</button>
        </div>
        <RequestReceipt :receipt="actionResult" title="Identity receipt" />
        <DataTable v-if="identityRows.length" :rows="identityRows" :columns="['id', 'principal', 'identity', 'trust']" />
        <EmptyState v-else title="No identities" detail="创建身份绑定后，跨平面动作会使用可信主体判定。" />
        <label class="field-line">
          Grant id
          <input v-model="grantId" type="text" />
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" @click="createGrant">Create grant</button>
          <button class="ghost-action" type="button" :disabled="!grantId" @click="revokeGrant">Revoke grant</button>
        </div>
        <RequestReceipt :receipt="actionResult" title="Grant receipt" />
        <DataTable v-if="grantRows.length" :rows="grantRows" :columns="['id', 'principal', 'capability', 'type']" />
        <EmptyState v-else title="No grants" detail="授权为空时，高风险动作会被策略门禁拦截或要求审批。" />
      </section>

      <section class="management-panel gateway-panel">
        <header>
          <h2>Action execution</h2>
          <span>{{ executeMode }}</span>
        </header>
        <label class="field-line">
          Mode
          <select v-model="executeMode">
            <option value="dry_run">dry_run</option>
            <option value="commit">commit</option>
          </select>
        </label>
        <label class="field-line">
          Idempotency key
          <input v-model="idempotencyKey" type="text" placeholder="optional" />
        </label>
        <button class="primary-action" type="button" @click="executeCrossPlaneAction">Execute action</button>
        <RequestReceipt :receipt="actionResult" title="Execution receipt" />
        <RawPayload title="Action readiness or receipt" :data="actionResult || {}" />
      </section>

      <section class="management-panel gateway-panel">
        <header>
          <h2>Audit and executions</h2>
          <span>{{ executionRows.length }} executions</span>
        </header>
        <DataTable v-if="executionRows.length" :rows="executionRows" :columns="['id', 'status', 'capability', 'provider']" />
        <EmptyState v-else title="No executions" detail="跨平面动作执行后会在这里展示。" />
        <RequestReceipt :receipt="actionResult" title="Gateway action receipt" />
        <RawPayload title="Gateway action result" :data="actionResult || state.audit || {}" />
      </section>
    </section>
  </section>
</template>
