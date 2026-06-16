<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue';
import { AlertTriangle, CheckCircle2, Database, RefreshCw, WifiOff } from 'lucide-vue-next';
import { api } from '../api/client';
import { capabilitySpecs } from '../data/capabilities';
import { useAppStore } from '../stores/app';
import ChartPanel from '../components/ChartPanel.vue';
import RawPayload from '../components/workbench/RawPayload.vue';

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
const structuredSourceRef = ref('service://iacc/manufacturing/webui-line-a');
const structuredFactType = ref('manufacturing_quality_event');
const structuredPlan = ref<any>(null);
const structuredCollections = ref<any>(null);
const skillState = ref<any>(null);
const selectedSkillId = ref('');
const skillActionResult = ref<any>(null);
const taskObjective = ref('Restore full WebUI capability with tested runtime evidence');
const taskState = ref<any>(null);
const taskActionResult = ref<any>(null);
const toolState = ref<any>(null);
const gatewayState = ref<any>(null);
const crossPlaneResult = ref<any>(null);
const resourceRef = ref('');
const iaccState = ref<any>(null);
const iaccResult = ref<any>(null);
const iaccIncidentTitle = ref('Line A torque deviation threatens QA-2026-0616 shipment');
const selectedIncidentId = ref('');
const selectedIaccSkillId = ref('');
const selectedActionId = ref('');
const cockpitProfileId = ref('webui-manufacturing');
const cockpitOwnerRef = ref('user:webui-operator');
const cockpitReportId = ref('');
const auditState = ref<any>(null);
const auditSource = ref('all');
const auditLimit = ref(50);
const auditOffset = ref(0);
const releaseSurface = ref('webui');

async function refresh() {
  await store.loadCapability(props.page);
  if (props.page === 'runtime') await loadRuntimeWorkbench();
  if (props.page === 'context') await loadContextWorkbench();
  if (props.page === 'memory') await loadMemoryWorkbench();
  if (props.page === 'skills') await loadSkillsWorkbench();
  if (props.page === 'agents') await loadAgentsWorkbench();
  if (props.page === 'tools') await loadToolsWorkbench();
  if (props.page === 'gateway') await loadGatewayWorkbench();
  if (props.page === 'iacc') await loadIaccWorkbench();
  if (props.page === 'audit') await loadAuditWorkbench();
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
    raw_checksum: 'sha256:manufacturing-webui-v0.9.220',
    metric_ids: ['torque_deviation_rate', 'station_quality_escape'],
  });
}

async function loadSkillsWorkbench() {
  const [catalog, projection, runs] = await Promise.all([api.skillCatalog(), api.skillProjection(), api.skillRuns()]);
  skillState.value = { catalog, projection, runs };
  const first = (catalog as any).items?.[0]?.id;
  if (!selectedSkillId.value && first) selectedSkillId.value = first;
}

async function runSkillAction(action: 'validate' | 'plan' | 'run') {
  if (!selectedSkillId.value) return;
  skillActionResult.value = await api.skillAction(selectedSkillId.value, action, { session_id: store.activeSessionId || 'webui' });
  await loadSkillsWorkbench();
}

async function loadAgentsWorkbench() {
  taskState.value = await api.tasks();
}

async function startTask() {
  taskActionResult.value = await api.startTask(taskObjective.value, false);
  await loadAgentsWorkbench();
}

async function addTaskPhase() {
  const id = taskActionResult.value?.id || taskState.value?.current?.id;
  if (!id) return;
  taskActionResult.value = await api.startTaskPhase(id, {
    name: 'Implementation',
    objective: 'Implement and verify the next WebUI workbench capability',
    plan: ['wire backend API', 'add UI controls', 'run E2E'],
    acceptance: ['tests pass', 'screenshot saved'],
    test_commands: ['npm run test:e2e --prefix webui'],
  });
  await loadAgentsWorkbench();
}

async function loadToolsWorkbench() {
  const [tools, history, capabilities] = await Promise.all([api.toolRegistry(), api.commandHistory(), api.loadCapabilityPage('tools', store.activeSessionId)]);
  toolState.value = { tools, history, capabilities };
}

async function loadGatewayWorkbench() {
  const [platforms, summary, accounts, capabilities, resources, mcp, crossPlane, audit, adapters, executions] = await Promise.all([
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
  gatewayState.value = { platforms, summary, accounts, capabilities, resources, mcp, crossPlane, audit, adapters, executions };
  const firstRef = (resources as any).resources?.[0]?.reference || (resources as any).items?.[0]?.reference;
  if (!resourceRef.value && firstRef) resourceRef.value = firstRef;
}

async function revalidateResource() {
  if (!resourceRef.value) return;
  crossPlaneResult.value = await api.connectorRevalidateResource(resourceRef.value);
  await loadGatewayWorkbench();
}

async function promoteResourceMemory() {
  if (!resourceRef.value) return;
  crossPlaneResult.value = await api.connectorPromoteMemory(resourceRef.value);
  await loadGatewayWorkbench();
}

async function runCrossPlanePreflight() {
  crossPlaneResult.value = await api.crossPlanePreflight({
    actor_principal: 'webui-operator',
    source_channel: 'channel://webui/local',
    session_id: store.activeSessionId || 'webui',
    requested_capability: 'service.read',
    provider_account: 'webui-local',
    target_ref: null,
    resource_ref: resourceRef.value || null,
    risk: 'medium',
    data_classification: 'internal',
    identity_trust: 'unknown',
  });
}

function iaccItems(collection: any, key: string) {
  return Array.isArray(collection?.[key]) ? collection[key] : Array.isArray(collection?.items) ? collection.items : [];
}

const iaccMetricChart = computed(() => {
  const health = iaccState.value?.health || {};
  return [
    { name: 'facts', value: Number(health.fact_count || 0) },
    { name: 'metrics', value: Number(health.metric_definition_count || 0) },
    { name: 'attention', value: Number(health.attention_count || 0) },
    { name: 'incidents', value: Number(health.incident_count || 0) },
    { name: 'executions', value: Number(health.execution_count || 0) },
  ].map((item) => ({ ...item, value: Math.max(1, item.value) }));
});

const iaccIncidents = computed(() => iaccItems(iaccState.value?.incidents, 'items'));
const iaccMetrics = computed(() => iaccItems(iaccState.value?.metrics, 'metrics'));
const iaccEntities = computed(() => iaccItems(iaccState.value?.entities, 'entities'));
const iaccAttention = computed(() => iaccItems(iaccState.value?.attention, 'items'));
const iaccSkills = computed(() => iaccItems(iaccState.value?.skills, 'items'));
const iaccRoom = computed(() => iaccState.value?.room || {});
const iaccAnalysis = computed(() => iaccRoom.value?.analysis || iaccResult.value?.analysis || iaccResult.value?.operational_analysis);
const iaccRecommendedActions = computed(() => iaccAnalysis.value?.recommended_actions || []);
const auditRecords = computed(() => iaccItems(auditState.value?.audit, 'records'));
const usageSessions = computed(() => iaccItems(auditState.value?.usage, 'sessions'));
const releaseChecks = computed(() => iaccItems(auditState.value?.releaseGate, 'checks'));
const usageChart = computed(() => {
  const byPlatform = auditState.value?.usage?.by_platform || {};
  const points = Object.entries(byPlatform).map(([name, value]: [string, any]) => ({
    name,
    value: Math.max(1, Number(value.total_tokens || value.message_count || value.session_count || 0)),
  }));
  return points.length ? points : [{ name: 'usage', value: Math.max(1, Number(auditState.value?.usage?.tokens?.total || 0)) }];
});
const releaseChart = computed(() => {
  const checks = releaseChecks.value;
  if (checks.length) {
    return checks.map((check: any) => ({ name: check.name || check.id || check.kind || 'check', value: check.status === 'pass' || check.passed ? 100 : 20 }));
  }
  return [
    { name: 'capabilities', value: Math.max(1, Number(auditState.value?.capabilities?.capability_count || auditState.value?.capabilities?.capabilities?.length || 0)) },
    { name: 'projection', value: Math.max(1, Number(auditState.value?.projection?.capability_count || auditState.value?.projection?.capabilities?.length || 0)) },
    { name: 'surfaces', value: Math.max(1, Number(auditState.value?.surfaces?.surfaces?.length || Object.keys(auditState.value?.surfaces || {}).length || 0)) },
  ];
});

async function loadIaccWorkbench() {
  workbenchError.value = '';
  try {
    const [app, health, commandCenter, live, metrics, entities, changes, attention, incidents, skills] = await Promise.all([
      api.iaccApp(),
      api.iaccHealth(),
      api.iaccCommandCenter(),
      api.iaccCommandCenterLive(),
      api.iaccMetrics(),
      api.iaccEntities(),
      api.iaccChanges(),
      api.iaccAttentionHot(),
      api.iaccIncidents(),
      api.iaccSkills(),
    ]);
    iaccState.value = { app, health, commandCenter, live, metrics, entities, changes, attention, incidents, skills, room: iaccState.value?.room };
    const firstIncident = iaccItems(incidents, 'items')[0]?.incident_id;
    if (!selectedIncidentId.value && firstIncident) {
      selectedIncidentId.value = firstIncident;
      await loadIaccIncidentRoom();
    }
    const firstSkill = iaccItems(skills, 'items')[0]?.skill_id;
    if (!selectedIaccSkillId.value && firstSkill) selectedIaccSkillId.value = firstSkill;
  } catch (error) {
    workbenchError.value = error instanceof Error ? error.message : String(error);
  }
}

async function seedIaccManufacturing() {
  iaccResult.value = {
    domain: await api.iaccSeedDomain(),
    ontology: await api.iaccSeedOntology(),
    fact: await api.iaccIngestFact([{
      fact_type: 'manufacturing_quality_event',
      entity_refs: ['line:A', 'station:torque-03'],
      metric_key: 'torque_deviation_rate',
      dimensions: { line: 'A', station: 'torque-03', batch: 'QA-2026-0616' },
      measures: { deviation_rate: 0.18, affected_units: 42 },
      source_ref: 'webui://iacc/simulated-manufacturing-quality',
      confidence: 0.93,
    }]),
  };
  await loadIaccWorkbench();
}

async function createIaccIncident() {
  iaccResult.value = await api.iaccCreateIncident({
    title: iaccIncidentTitle.value,
    session_id: store.activeSessionId || 'webui-iacc',
  });
  selectedIncidentId.value = iaccResult.value?.incident?.incident_id || selectedIncidentId.value;
  await loadIaccIncidentRoom();
  await loadIaccWorkbench();
}

async function loadIaccIncidentRoom() {
  if (!selectedIncidentId.value) return;
  const room = await api.iaccIncidentRoom(selectedIncidentId.value);
  iaccState.value = { ...(iaccState.value || {}), room };
  selectedActionId.value = (room as any).analysis?.recommended_actions?.[0]?.action_id || selectedActionId.value;
}

async function analyzeIaccIncident() {
  if (!selectedIncidentId.value) return;
  iaccResult.value = await api.iaccAnalyzeIncident(selectedIncidentId.value);
  await loadIaccIncidentRoom();
}

async function recommendIaccPlaybooks() {
  if (!selectedIncidentId.value) return;
  iaccResult.value = await api.iaccRecommendPlaybooks(selectedIncidentId.value, 5);
  await loadIaccIncidentRoom();
}

async function promoteIaccCase() {
  if (!selectedIncidentId.value) return;
  iaccResult.value = await api.iaccPromoteIncidentCase(selectedIncidentId.value);
  await loadIaccIncidentRoom();
}

async function planIaccSkills() {
  if (!selectedIncidentId.value) return;
  iaccResult.value = await api.iaccPlanSkills(selectedIncidentId.value, 3);
  selectedIaccSkillId.value = iaccResult.value?.plan?.selected_skills?.[0]?.skill_id || selectedIaccSkillId.value;
  await loadIaccIncidentRoom();
}

async function runIaccSkill() {
  if (!selectedIncidentId.value || !selectedIaccSkillId.value) return;
  iaccResult.value = await api.iaccRunSkill(selectedIncidentId.value, selectedIaccSkillId.value);
  await loadIaccIncidentRoom();
}

async function executeIaccAction() {
  const analysisId = iaccAnalysis.value?.analysis_id;
  if (!analysisId || !selectedActionId.value) return;
  iaccResult.value = await api.iaccExecuteAction(analysisId, selectedActionId.value, {
    mode: 'dry_run',
    operator_id: 'webui-operator',
    note: 'executed from WebUI IACC workbench',
  });
  await loadIaccIncidentRoom();
}

async function bridgeIaccExecution() {
  const executionId = iaccResult.value?.execution?.execution_id || iaccRoom.value?.executions?.[0]?.execution_id;
  if (!executionId) return;
  iaccResult.value = await api.iaccExecutionBridge(executionId, {
    mode: 'dry_run',
    actor_principal: 'webui-operator',
    source_channel: 'channel://webui/iacc',
    requested_capability: 'channel.feishu.send_text',
  });
  await loadIaccIncidentRoom();
}

async function generateIaccReport() {
  const profile = await api.iaccUpsertProfile({
    profile_id: cockpitProfileId.value,
    owner_ref: cockpitOwnerRef.value,
    display_name: 'WebUI Manufacturing Cockpit',
    focus_refs: ['line:A', 'station:torque-03'],
    focus_metric_ids: ['torque_deviation_rate', 'station_quality_escape'],
    thresholds: { torque_deviation_rate: 0.08, station_quality_escape: 0 },
    cadence: 'daily',
  });
  const report = await api.iaccGenerateReport(cockpitProfileId.value, {
    report_id: cockpitReportId.value || undefined,
    cadence: 'daily',
    delivery_ref: 'channel://feishu/user/webui-operator',
    note: 'generated from WebUI IACC workbench',
  });
  cockpitReportId.value = report?.report?.report_id || cockpitReportId.value;
  iaccResult.value = { profile, report };
  await loadIaccWorkbench();
}

async function retryIaccReportDelivery() {
  if (!cockpitReportId.value) return;
  iaccResult.value = await api.iaccRetryReportDelivery(cockpitReportId.value, {
    mode: 'dry_run',
    force: true,
    actor_principal: 'webui-operator',
    source_channel: 'iacc.report.retry',
  });
}

async function loadAuditWorkbench() {
  workbenchError.value = '';
  try {
    const [audit, usage, capabilities, projection, surfaces, releaseGate, approvalHistory, crossPlaneAudit, executions] = await Promise.all([
      api.auditExport(auditSource.value, auditLimit.value, auditOffset.value),
      api.usageSummary(),
      api.cowdCapabilities(),
      api.cowdProjection(releaseSurface.value),
      api.cowdSurfaces(),
      api.cowdReleaseGate(),
      api.approvalHistory(),
      api.crossPlaneAudit(),
      api.crossPlaneExecutions(),
    ]);
    auditState.value = { audit, usage, capabilities, projection, surfaces, releaseGate, approvalHistory, crossPlaneAudit, executions };
  } catch (error) {
    workbenchError.value = error instanceof Error ? error.message : String(error);
  }
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
        <RawPayload :data="runtimeLeases || {}" />
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
        <RawPayload :data="contextEnvelope || {}" />
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
        <RawPayload :data="evidenceResult || {}" />
      </article>

      <article class="management-panel">
        <header>
          <h2>History and recommendations</h2>
          <span>active session</span>
        </header>
        <RawPayload :data="{ history: contextHistory, recommendations: contextRecommendations }" />
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
        <RawPayload :data="{ search: memoryResult, packet: memoryPacket }" />
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
        <RawPayload :data="{ plan: structuredPlan, collections: structuredCollections }" />
      </article>

      <article class="management-panel">
        <header>
          <h2>Maintenance</h2>
          <span>scan/update</span>
        </header>
        <button class="ghost-action" type="button" @click="scanMaintenance">Scan candidates</button>
        <RawPayload :data="maintenanceResult || {}" />
      </article>
    </section>

    <section v-if="props.page === 'skills'" class="management-grid skills-workbench">
      <article class="management-panel">
        <header>
          <h2>Skill lifecycle</h2>
          <span>{{ skillState?.catalog?.items?.length || 0 }} skills</span>
        </header>
        <label class="field-line">
          Skill
          <select v-model="selectedSkillId">
            <option v-for="skill in skillState?.catalog?.items || []" :key="skill.id" :value="skill.id">{{ skill.name || skill.id }}</option>
          </select>
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" @click="runSkillAction('validate')">Validate</button>
          <button class="ghost-action" type="button" @click="runSkillAction('plan')">Plan</button>
          <button class="primary-action" type="button" @click="runSkillAction('run')">Run</button>
        </div>
        <RawPayload :data="skillActionResult || skillState || {}" />
      </article>
      <article class="management-panel">
        <header>
          <h2>Projection and runs</h2>
          <span>webui</span>
        </header>
        <RawPayload :data="{ projection: skillState?.projection, runs: skillState?.runs }" />
      </article>
    </section>

    <section v-if="props.page === 'agents'" class="management-grid agents-workbench">
      <article class="management-panel">
        <header>
          <h2>Task control</h2>
          <span>{{ taskState?.tasks?.length || 0 }} tasks</span>
        </header>
        <label class="field-line">
          Objective
          <textarea v-model="taskObjective" rows="3" />
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" @click="startTask">Start task</button>
          <button class="ghost-action" type="button" @click="addTaskPhase">Add phase</button>
        </div>
        <RawPayload :data="taskActionResult || taskState || {}" />
      </article>
      <article class="management-panel">
        <header>
          <h2>Task registry</h2>
          <span>current</span>
        </header>
        <RawPayload :data="taskState || {}" />
      </article>
    </section>

    <section v-if="props.page === 'tools'" class="management-grid tools-workbench">
      <article class="management-panel">
        <header>
          <h2>Tool registry</h2>
          <span>{{ toolState?.tools?.count || toolState?.tools?.tools?.length || 0 }} tools</span>
        </header>
        <RawPayload :data="toolState?.tools || {}" />
      </article>
      <article class="management-panel">
        <header>
          <h2>Command and risk history</h2>
          <span>{{ toolState?.history?.total || toolState?.history?.history?.length || 0 }} events</span>
        </header>
        <RawPayload :data="{ history: toolState?.history, capabilities: toolState?.capabilities }" />
      </article>
    </section>

    <section v-if="props.page === 'gateway'" class="management-grid gateway-workbench">
      <article class="management-panel">
        <header>
          <h2>Platforms and connectors</h2>
          <span>{{ gatewayState?.summary?.connector_count || gatewayState?.accounts?.accounts?.length || 0 }} connectors</span>
        </header>
        <RawPayload :data="{ platforms: gatewayState?.platforms, summary: gatewayState?.summary, accounts: gatewayState?.accounts, capabilities: gatewayState?.capabilities, mcp: gatewayState?.mcp }" />
      </article>

      <article class="management-panel">
        <header>
          <h2>Resources and memory promotion</h2>
          <span>{{ gatewayState?.resources?.count || gatewayState?.resources?.resources?.length || 0 }} resources</span>
        </header>
        <label class="field-line">
          Resource ref
          <input v-model="resourceRef" type="text" placeholder="service://..." />
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" :disabled="!resourceRef" @click="revalidateResource">Revalidate</button>
          <button class="primary-action" type="button" :disabled="!resourceRef" @click="promoteResourceMemory">Promote memory</button>
        </div>
        <RawPayload :data="gatewayState?.resources || {}" />
      </article>

      <article class="management-panel">
        <header>
          <h2>Cross-plane governance</h2>
          <span>preflight/audit</span>
        </header>
        <button class="primary-action" type="button" @click="runCrossPlanePreflight">Run preflight</button>
        <RawPayload :data="crossPlaneResult || { summary: gatewayState?.crossPlane, audit: gatewayState?.audit, adapters: gatewayState?.adapters, executions: gatewayState?.executions }" />
      </article>
    </section>

    <section v-if="props.page === 'iacc'" class="management-grid iacc-workbench">
      <ChartPanel title="IACC operating load" kind="bar" :data="iaccMetricChart" />

      <article class="management-panel iacc-command-panel">
        <header>
          <h2>Manufacturing command center</h2>
          <span>{{ iaccState?.health?.status || 'unknown' }}</span>
        </header>
        <dl class="detail-list">
          <dt>Schema</dt>
          <dd>{{ iaccState?.health?.schema_version || 'unknown' }}</dd>
          <dt>Capabilities</dt>
          <dd>{{ iaccState?.health?.capabilities?.length || 0 }}</dd>
          <dt>Risk queue</dt>
          <dd>{{ iaccState?.commandCenter?.risk_queue?.length || 0 }}</dd>
          <dt>Live actions</dt>
          <dd>{{ iaccState?.live?.action_queue?.length || 0 }}</dd>
        </dl>
      </article>

      <article class="management-panel">
        <header>
          <h2>Manufacturing data seed</h2>
          <span>{{ iaccMetrics.length }} metrics</span>
        </header>
        <p>IACC is an upper application. This action seeds manufacturing ontology/domain data and writes a real manufacturing fact through IACC APIs.</p>
        <button class="primary-action" type="button" @click="seedIaccManufacturing">Seed manufacturing fact</button>
        <RawPayload :data="{ metrics: iaccState?.metrics, attention: iaccState?.attention, changes: iaccState?.changes }" />
      </article>

      <article class="management-panel">
        <header>
          <h2>Incident room</h2>
          <span>{{ iaccIncidents.length }} incidents</span>
        </header>
        <label class="field-line">
          New incident
          <textarea v-model="iaccIncidentTitle" rows="3" />
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" @click="createIaccIncident">Create incident</button>
          <button class="ghost-action" type="button" :disabled="!selectedIncidentId" @click="loadIaccIncidentRoom">Open room</button>
        </div>
        <label class="field-line">
          Current incident
          <select v-model="selectedIncidentId" @change="loadIaccIncidentRoom">
            <option value="">Select incident</option>
            <option v-for="incident in iaccIncidents" :key="incident.incident_id" :value="incident.incident_id">
              {{ incident.title || incident.incident_id }}
            </option>
          </select>
        </label>
        <RawPayload :data="{ room: iaccRoom, entities: iaccEntities.slice(0, 8), attention: iaccAttention.slice(0, 8) }" />
      </article>

      <article class="management-panel">
        <header>
          <h2>Analysis, playbook, actions</h2>
          <span>{{ iaccRecommendedActions.length }} actions</span>
        </header>
        <div class="button-row">
          <button class="ghost-action" type="button" :disabled="!selectedIncidentId" @click="analyzeIaccIncident">Analyze</button>
          <button class="ghost-action" type="button" :disabled="!selectedIncidentId" @click="recommendIaccPlaybooks">Recommend playbooks</button>
          <button class="ghost-action" type="button" :disabled="!selectedIncidentId" @click="promoteIaccCase">Promote case</button>
        </div>
        <label class="field-line">
          Recommended action
          <select v-model="selectedActionId">
            <option value="">Select action</option>
            <option v-for="action in iaccRecommendedActions" :key="action.action_id" :value="action.action_id">
              {{ action.title || action.action_id }}
            </option>
          </select>
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" :disabled="!selectedActionId" @click="executeIaccAction">Execute dry run</button>
          <button class="ghost-action" type="button" @click="bridgeIaccExecution">Bridge cross-plane</button>
        </div>
        <RawPayload :data="{ analysis: iaccAnalysis, executions: iaccRoom?.executions, playbooks: iaccRoom?.playbooks }" />
      </article>

      <article class="management-panel">
        <header>
          <h2>Manufacturing skills</h2>
          <span>{{ iaccSkills.length }} skills</span>
        </header>
        <label class="field-line">
          Skill
          <select v-model="selectedIaccSkillId">
            <option value="">Select skill</option>
            <option v-for="skill in iaccSkills" :key="skill.skill_id" :value="skill.skill_id">
              {{ skill.name || skill.skill_id }}
            </option>
          </select>
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" :disabled="!selectedIncidentId" @click="planIaccSkills">Plan skills</button>
          <button class="primary-action" type="button" :disabled="!selectedIncidentId || !selectedIaccSkillId" @click="runIaccSkill">Run skill</button>
        </div>
        <RawPayload :data="{ skills: iaccState?.skills, skill_runs: iaccRoom?.skill_runs || iaccResult?.skill_run }" />
      </article>

      <article class="management-panel">
        <header>
          <h2>Cockpit reports</h2>
          <span>delivery/retry</span>
        </header>
        <label class="field-line">
          Profile id
          <input v-model="cockpitProfileId" type="text" />
        </label>
        <label class="field-line">
          Owner ref
          <input v-model="cockpitOwnerRef" type="text" />
        </label>
        <label class="field-line">
          Report id
          <input v-model="cockpitReportId" type="text" placeholder="optional" />
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" @click="generateIaccReport">Generate report</button>
          <button class="ghost-action" type="button" :disabled="!cockpitReportId" @click="retryIaccReportDelivery">Retry delivery</button>
        </div>
        <RawPayload :data="iaccResult || {}" />
      </article>
    </section>

    <section v-if="props.page === 'audit'" class="management-grid audit-workbench">
      <ChartPanel title="Usage by platform" kind="bar" :data="usageChart" />
      <ChartPanel title="Release gate coverage" kind="radar" :data="releaseChart" />

      <article class="management-panel">
        <header>
          <h2>Audit export</h2>
          <span>{{ auditRecords.length }} records</span>
        </header>
        <div class="button-row">
          <label class="field-line">
            Source
            <select v-model="auditSource" @change="loadAuditWorkbench">
              <option value="all">all</option>
              <option value="approval">approval</option>
              <option value="memory">memory</option>
            </select>
          </label>
          <label class="field-line">
            Limit
            <input v-model.number="auditLimit" type="number" min="1" max="500" @change="loadAuditWorkbench" />
          </label>
          <label class="field-line">
            Offset
            <input v-model.number="auditOffset" type="number" min="0" @change="loadAuditWorkbench" />
          </label>
        </div>
        <button class="primary-action" type="button" @click="loadAuditWorkbench">Refresh audit</button>
        <RawPayload :data="auditState?.audit || {}" />
      </article>

      <article class="management-panel">
        <header>
          <h2>Usage summary</h2>
          <span>{{ usageSessions.length }} sessions</span>
        </header>
        <dl class="detail-list">
          <dt>Status</dt>
          <dd>{{ auditState?.usage?.status || 'unknown' }}</dd>
          <dt>Messages</dt>
          <dd>{{ auditState?.usage?.message_count || 0 }}</dd>
          <dt>Tokens</dt>
          <dd>{{ auditState?.usage?.tokens?.total || 0 }}</dd>
          <dt>Cost</dt>
          <dd>{{ Number(auditState?.usage?.estimated_cost_usd || 0).toFixed(6) }}</dd>
        </dl>
        <RawPayload :data="{ by_platform: auditState?.usage?.by_platform, by_model: auditState?.usage?.by_model, sessions: usageSessions.slice(0, 12) }" />
      </article>

      <article class="management-panel">
        <header>
          <h2>Release gate</h2>
          <span>{{ auditState?.releaseGate?.status || auditState?.releaseGate?.result || 'unknown' }}</span>
        </header>
        <label class="field-line">
          Surface
          <select v-model="releaseSurface" @change="loadAuditWorkbench">
            <option value="webui">webui</option>
            <option value="tui">tui</option>
            <option value="cli">cli</option>
          </select>
        </label>
        <RawPayload :data="{ capabilities: auditState?.capabilities, projection: auditState?.projection, surfaces: auditState?.surfaces, release_gate: auditState?.releaseGate }" />
      </article>

      <article class="management-panel">
        <header>
          <h2>Governance evidence</h2>
          <span>approval/cross-plane</span>
        </header>
        <RawPayload :data="{ approval: auditState?.approvalHistory, cross_plane_audit: auditState?.crossPlaneAudit, executions: auditState?.executions }" />
      </article>
    </section>

    <section class="management-grid live-management">
      <article v-for="item in snapshots" :key="`${item.id}:preview`" class="management-panel">
        <header>
          <h2>{{ item.label }}</h2>
          <span>{{ item.status }}</span>
        </header>
        <p v-if="item.__error">{{ item.__error }}</p>
        <RawPayload :data="item.data" />
      </article>
    </section>
  </section>
</template>
