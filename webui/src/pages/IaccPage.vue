<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { RefreshCw } from 'lucide-vue-next';
import { api } from '../api/client';
import { useAppStore } from '../stores/app';
import ChartPanel from '../components/ChartPanel.vue';
import DataTable from '../components/workbench/DataTable.vue';
import RawPayload from '../components/workbench/RawPayload.vue';

const store = useAppStore();
const loading = ref(false);
const error = ref('');
const state = ref<any>({});
const result = ref<any>(null);
const incidentTitle = ref('Line A torque deviation threatens QA-2026-0616 shipment');
const selectedIncidentId = ref('');
const selectedSkillId = ref('');
const selectedActionId = ref('');
const cockpitProfileId = ref('webui-manufacturing');
const cockpitOwnerRef = ref('user:webui-operator');
const cockpitReportId = ref('');
const sourcePackId = ref('webui-server-manufacturing');
const selectedMetricId = ref('torque_deviation_rate');
const selectedEntityId = ref('');
const relationTargetId = ref('');
const evidenceId = ref('');
const qualityGateId = ref('');
const computeJobId = ref('');
const connectorRunId = ref('');
const factPayload = ref('');
const dataPlaneResult = ref<any>(null);
const sourcePackResult = ref<any>(null);
const entityResult = ref<any>(null);
const metricResult = ref<any>(null);
const evidenceResult = ref<any>(null);

function items(collection: any, key: string) {
  return Array.isArray(collection?.[key]) ? collection[key] : Array.isArray(collection?.items) ? collection.items : [];
}

const metricChart = computed(() => {
  const health = state.value?.health || {};
  return [
    { name: 'facts', value: Number(health.fact_count || 0) },
    { name: 'metrics', value: Number(health.metric_definition_count || 0) },
    { name: 'attention', value: Number(health.attention_count || 0) },
    { name: 'incidents', value: Number(health.incident_count || 0) },
    { name: 'executions', value: Number(health.execution_count || 0) },
  ];
});

const incidents = computed(() => items(state.value?.incidents, 'items'));
const metrics = computed(() => items(state.value?.metrics, 'metrics'));
const entities = computed(() => items(state.value?.entities, 'entities'));
const attention = computed(() => items(state.value?.attention, 'items'));
const skills = computed(() => items(state.value?.skills, 'items'));
const room = computed(() => state.value?.room || {});
const analysis = computed(() => room.value?.analysis || result.value?.analysis || result.value?.operational_analysis);
const recommendedActions = computed(() => analysis.value?.recommended_actions || []);

async function refresh() {
  loading.value = true;
  error.value = '';
  try {
    const [app, health, governance, dataPlane, commandCenter, live, metricsData, entitiesData, changes, attentionData, incidentsData, skillsData] = await Promise.all([
      api.iaccApp(),
      api.iaccHealth(),
      api.iaccProductionGovernance(),
      api.iaccDataPlaneHealth(),
      api.iaccCommandCenter(),
      api.iaccCommandCenterLive(),
      api.iaccMetrics(),
      api.iaccEntities(),
      api.iaccChanges(),
      api.iaccAttentionHot(),
      api.iaccIncidents(),
      api.iaccSkills(),
    ]);
    state.value = {
      app,
      health,
      governance,
      dataPlane,
      commandCenter,
      live,
      metrics: metricsData,
      entities: entitiesData,
      changes,
      attention: attentionData,
      incidents: incidentsData,
      skills: skillsData,
      room: state.value?.room,
    };
    const firstIncident = items(incidentsData, 'items')[0]?.incident_id;
    if (!selectedIncidentId.value && firstIncident) {
      selectedIncidentId.value = firstIncident;
      await openIncidentRoom();
    }
    const firstSkill = items(skillsData, 'items')[0]?.skill_id;
    if (!selectedSkillId.value && firstSkill) selectedSkillId.value = firstSkill;
    const firstMetric = items(metricsData, 'metrics')[0]?.metric_id;
    if (!selectedMetricId.value && firstMetric) selectedMetricId.value = firstMetric;
    const firstEntity = items(entitiesData, 'entities')[0]?.entity_id;
    if (!selectedEntityId.value && firstEntity) selectedEntityId.value = firstEntity;
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  } finally {
    loading.value = false;
  }
}

async function planDataPlaneIngest() {
  dataPlaneResult.value = await api.iaccDataPlaneIngestPlan({
    source_ref: `source-pack://${sourcePackId.value}`,
    fact_type: 'manufacturing_quality_event',
    partition_ref: 'line:A',
    high_watermark: new Date().toISOString(),
    estimated_rows: 128,
    raw_checksum: 'sha256:webui-iacc-ingest-plan',
    metric_ids: [selectedMetricId.value || 'torque_deviation_rate'],
  });
}

function defaultSourcePack() {
  return {
    source_pack_id: sourcePackId.value,
    source_name: 'WebUI Server Manufacturing Pack',
    owner: 'webui-operator',
    access_mode: 'managed',
    refresh_mode: 'incremental',
    entity_mappings: [{
      source_entity: 'line',
      iacc_entity_type: 'manufacturing_line',
      source_key_field: 'line_id',
    }],
    fact_mappings: [{
      source_table: 'quality_events',
      fact_type: 'manufacturing_quality_event',
      metric_key: selectedMetricId.value || 'torque_deviation_rate',
      entity_ref_fields: ['line_id', 'station_id'],
      measure_fields: ['deviation_rate', 'affected_units'],
      dedup_key: 'batch_id',
      delta_signature: 'updated_at',
    }],
    reconciliation_rules: ['canonical_key=line_id'],
    quality_rules: ['deviation_rate must be >= 0'],
    freshness_sla: 'PT1H',
    security_policy: 'internal',
    metadata: { source: 'webui' },
  };
}

async function upsertSourcePack() {
  sourcePackResult.value = await api.iaccSourcePackUpsert(defaultSourcePack());
  await refresh();
}

async function validateSourcePack() {
  sourcePackResult.value = await api.iaccSourcePackValidate(sourcePackId.value);
}

async function sourcePackDeltaPlan() {
  sourcePackResult.value = await api.iaccSourcePackDeltaPlan(sourcePackId.value);
}

async function planConnectorRun() {
  sourcePackResult.value = await api.iaccSourcePackConnectorPlan(sourcePackId.value, {
    source_pack_id: sourcePackId.value,
    mode: 'dry_run',
    requested_capability: 'service.read',
  });
  connectorRunId.value = sourcePackResult.value?.run?.run_id || connectorRunId.value;
}

async function executeConnectorRun() {
  sourcePackResult.value = await api.iaccSourcePackConnectorRun(sourcePackId.value, {
    source_pack_id: sourcePackId.value,
    mode: 'dry_run',
    requested_capability: 'service.read',
  });
  connectorRunId.value = sourcePackResult.value?.run?.run_id || connectorRunId.value;
}

async function getConnectorRun() {
  if (!connectorRunId.value) return;
  sourcePackResult.value = await api.iaccConnectorRun(connectorRunId.value);
}

async function upsertEntity() {
  entityResult.value = await api.iaccEntityUpsert({
    entity_id: selectedEntityId.value || undefined,
    entity_type: 'manufacturing_line',
    canonical_key: 'line:A',
    display_name: 'Line A',
    source_keys: [{ source_system: sourcePackId.value, source_key: 'line:A', source_ref: `source-pack://${sourcePackId.value}` }],
    attributes: { plant: 'webui-demo' },
    confidence: 0.98,
  });
  selectedEntityId.value = entityResult.value?.entity?.entity_id || selectedEntityId.value;
  await refresh();
}

async function inspectEntity() {
  if (!selectedEntityId.value) return;
  const [entity, relations, impact] = await Promise.all([
    api.iaccEntity(selectedEntityId.value),
    api.iaccEntityRelations(selectedEntityId.value),
    api.iaccEntityImpactPath(selectedEntityId.value),
  ]);
  entityResult.value = { entity, relations, impact };
}

async function resolveEntitySourceKey() {
  entityResult.value = await api.iaccEntityResolveSourceKey(sourcePackId.value, 'line:A');
}

async function upsertRelation() {
  if (!selectedEntityId.value || !relationTargetId.value) return;
  entityResult.value = await api.iaccRelationUpsert({
    relation_type: 'feeds',
    from_entity_id: selectedEntityId.value,
    to_entity_id: relationTargetId.value,
    attributes: { source: 'webui' },
    confidence: 0.9,
  });
  await inspectEntity();
}

async function inspectMetric() {
  if (!selectedMetricId.value) return;
  const [detail, lineage] = await Promise.all([
    api.iaccMetricDetail(selectedMetricId.value),
    api.iaccMetricLineage(selectedMetricId.value),
  ]);
  metricResult.value = { detail, lineage };
}

async function materializeMetricSnapshot() {
  metricResult.value = await api.iaccMetricSnapshotMaterialize([selectedMetricId.value || 'torque_deviation_rate'], selectedEntityId.value || undefined);
}

async function planMetricAttention() {
  metricResult.value = await api.iaccAttentionPlan({
    trigger_fact_type: 'manufacturing_quality_event',
    entity_scope: selectedEntityId.value || undefined,
    period: 'latest',
    limit: 10,
  });
}

async function planComputeJob() {
  metricResult.value = await api.iaccComputeJobPlan({
    trigger_fact_type: 'manufacturing_quality_event',
    trigger_fact_refs: [],
    entity_scope: selectedEntityId.value || undefined,
    period: 'latest',
    metric_ids: [selectedMetricId.value || 'torque_deviation_rate'],
    priority: 0.8,
  });
  computeJobId.value = metricResult.value?.job?.job_id || metricResult.value?.plan?.job?.job_id || computeJobId.value;
}

async function runComputeJob() {
  if (!computeJobId.value) return;
  metricResult.value = await api.iaccComputeJobRun(computeJobId.value);
}

async function recomputeMetrics() {
  metricResult.value = await api.iaccMetricRecompute();
  await refresh();
}

async function buildEvidencePacket() {
  evidenceResult.value = await api.iaccEvidenceBuild({
    attention_id: attention.value[0]?.attention_id,
    problem_statement: incidentTitle.value,
  });
  evidenceId.value = evidenceResult.value?.packet?.packet_id || evidenceResult.value?.evidence_packet?.packet_id || evidenceId.value;
}

async function inspectEvidence() {
  if (!evidenceId.value) return;
  const [packet, context] = await Promise.all([
    api.iaccEvidence(evidenceId.value),
    api.iaccEvidenceContext(evidenceId.value),
  ]);
  evidenceResult.value = { packet, context };
}

async function evaluateEvidenceQuality() {
  if (!evidenceId.value) return;
  evidenceResult.value = await api.iaccEvidenceQualityGate(evidenceId.value);
  qualityGateId.value = evidenceResult.value?.quality_gate?.quality_gate_id || evidenceResult.value?.gate?.quality_gate_id || qualityGateId.value;
}

async function inspectQualityGate() {
  if (!qualityGateId.value) return;
  evidenceResult.value = await api.iaccQualityGate(qualityGateId.value);
}

async function initializeManufacturingKernel() {
  result.value = {
    domain: await api.iaccSeedDomain(),
    ontology: await api.iaccSeedOntology(),
  };
  await refresh();
}

async function ingestManufacturingFacts() {
  if (!factPayload.value.trim()) {
    error.value = 'Fact payload is required. Paste a JSON object or array from a real source pack.';
    return;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(factPayload.value);
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
    return;
  }
  const facts = Array.isArray(parsed) ? parsed : [parsed];
  const invalid = facts.some((fact: any) => !fact?.source_ref || !fact?.fact_type);
  if (invalid) {
    error.value = 'Each fact must include source_ref and fact_type.';
    return;
  }
  result.value = await api.iaccIngestFact(facts as Record<string, unknown>[]);
  await refresh();
}

async function createIncident() {
  result.value = await api.iaccCreateIncident({
    title: incidentTitle.value,
    session_id: store.activeSessionId || 'webui-iacc',
  });
  selectedIncidentId.value = result.value?.incident?.incident_id || selectedIncidentId.value;
  await openIncidentRoom();
  await refresh();
}

async function openIncidentRoom() {
  if (!selectedIncidentId.value) return;
  const nextRoom = await api.iaccIncidentRoom(selectedIncidentId.value);
  state.value = { ...(state.value || {}), room: nextRoom };
  selectedActionId.value = (nextRoom as any).analysis?.recommended_actions?.[0]?.action_id || selectedActionId.value;
}

async function analyzeIncident() {
  if (!selectedIncidentId.value) return;
  result.value = await api.iaccAnalyzeIncident(selectedIncidentId.value);
  await openIncidentRoom();
}

async function recommendPlaybooks() {
  if (!selectedIncidentId.value) return;
  result.value = await api.iaccRecommendPlaybooks(selectedIncidentId.value, 5);
  await openIncidentRoom();
}

async function promoteCase() {
  if (!selectedIncidentId.value) return;
  result.value = await api.iaccPromoteIncidentCase(selectedIncidentId.value);
  await openIncidentRoom();
}

async function planSkills() {
  if (!selectedIncidentId.value) return;
  result.value = await api.iaccPlanSkills(selectedIncidentId.value, 3);
  selectedSkillId.value = result.value?.plan?.selected_skills?.[0]?.skill_id || selectedSkillId.value;
  await openIncidentRoom();
}

async function runSkill() {
  if (!selectedIncidentId.value || !selectedSkillId.value) return;
  result.value = await api.iaccRunSkill(selectedIncidentId.value, selectedSkillId.value);
  await openIncidentRoom();
}

async function executeAction() {
  const analysisId = analysis.value?.analysis_id;
  if (!analysisId || !selectedActionId.value) return;
  result.value = await api.iaccExecuteAction(analysisId, selectedActionId.value, {
    mode: 'dry_run',
    operator_id: 'webui-operator',
    note: 'executed from WebUI IACC workbench',
  });
  await openIncidentRoom();
}

async function bridgeExecution() {
  const executionId = result.value?.execution?.execution_id || room.value?.executions?.[0]?.execution_id;
  if (!executionId) return;
  result.value = await api.iaccExecutionBridge(executionId, {
    mode: 'dry_run',
    actor_principal: 'webui-operator',
    source_channel: 'channel://webui/iacc',
    requested_capability: 'channel.feishu.send_text',
  });
  await openIncidentRoom();
}

async function generateReport() {
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
  result.value = { profile, report };
  await refresh();
}

async function retryReportDelivery() {
  if (!cockpitReportId.value) return;
  result.value = await api.iaccRetryReportDelivery(cockpitReportId.value, {
    mode: 'dry_run',
    force: true,
    actor_principal: 'webui-operator',
    source_channel: 'iacc.report.retry',
  });
}

onMounted(refresh);
</script>

<template>
  <section class="capability-page iacc-page">
    <header class="page-header">
      <div>
        <h1>IACC Manufacturing Application</h1>
        <p>IACC is the manufacturing application layer on top of the cowd kernel. This page manages its real domain data, incidents, skills, action bridge, and cockpit reports.</p>
      </div>
      <button class="primary-action" type="button" :disabled="loading" @click="refresh">
        <RefreshCw :size="15" />
        {{ loading ? 'Loading' : 'Refresh IACC' }}
      </button>
    </header>

    <p v-if="error" class="settings-alert">{{ error }}</p>

    <section class="metric-row" aria-label="IACC metrics">
      <article class="metric-card" data-tone="success">
        <span>Facts</span>
        <strong>{{ state?.health?.fact_count || 0 }}</strong>
        <small>{{ state?.health?.schema_version || 'schema unknown' }}</small>
      </article>
      <article class="metric-card" data-tone="info">
        <span>Metrics</span>
        <strong>{{ metrics.length }}</strong>
        <small>{{ entities.length }} entities</small>
      </article>
      <article class="metric-card" data-tone="warn">
        <span>Incidents</span>
        <strong>{{ incidents.length }}</strong>
        <small>{{ attention.length }} attention items</small>
      </article>
      <article class="metric-card" data-tone="info">
        <span>Skills</span>
        <strong>{{ skills.length }}</strong>
        <small>{{ room?.skill_runs?.length || 0 }} room runs</small>
      </article>
    </section>

    <section class="management-grid iacc-workbench">
      <ChartPanel data-section="overview" title="IACC operating load" kind="bar" :data="metricChart" />

      <article class="management-panel iacc-command-panel" data-section="overview">
        <header>
          <h2>Manufacturing command center</h2>
          <span>{{ state?.health?.status || 'unknown' }}</span>
        </header>
        <dl class="detail-list">
          <dt>Schema</dt>
          <dd>{{ state?.health?.schema_version || 'unknown' }}</dd>
          <dt>Capabilities</dt>
          <dd>{{ state?.health?.capabilities?.length || 0 }}</dd>
          <dt>Risk queue</dt>
          <dd>{{ state?.commandCenter?.risk_queue?.length || 0 }}</dd>
          <dt>Live actions</dt>
          <dd>{{ state?.live?.action_queue?.length || 0 }}</dd>
        </dl>
      </article>

      <article class="management-panel" data-section="data-plane">
        <header>
          <h2>Data plane and source packs</h2>
          <span>{{ state?.dataPlane?.status || 'unknown' }}</span>
        </header>
        <dl class="detail-list">
          <dt>Provider</dt>
          <dd>{{ state?.dataPlane?.provider || 'unknown' }}</dd>
          <dt>Mode</dt>
          <dd>{{ state?.dataPlane?.mode || 'unknown' }}</dd>
          <dt>Watermarks</dt>
          <dd>{{ state?.dataPlane?.watermark_count || 0 }}</dd>
          <dt>Governance</dt>
          <dd>{{ state?.governance?.status || state?.governance?.kind || 'unknown' }}</dd>
        </dl>
        <label class="field-line">
          Source pack id
          <input v-model="sourcePackId" type="text" />
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" @click="planDataPlaneIngest">Plan ingest</button>
          <button class="primary-action" type="button" @click="upsertSourcePack">Upsert source pack</button>
          <button class="ghost-action" type="button" @click="validateSourcePack">Validate</button>
        </div>
        <div class="button-row">
          <button class="ghost-action" type="button" @click="sourcePackDeltaPlan">Delta plan</button>
          <button class="ghost-action" type="button" @click="planConnectorRun">Plan connector run</button>
          <button class="ghost-action" type="button" @click="executeConnectorRun">Run connector</button>
        </div>
        <label class="field-line">
          Connector run id
          <input v-model="connectorRunId" type="text" @keydown.enter.prevent="getConnectorRun" />
        </label>
        <RawPayload :data="{ data_plane: dataPlaneResult, source_pack: sourcePackResult }" />
      </article>

      <article class="management-panel" data-section="source-pack">
        <header>
          <h2>Manufacturing data ingestion</h2>
          <span>{{ metrics.length }} metrics</span>
        </header>
        <p class="panel-note">Only ingest facts copied from a real source pack or connector output. Demo fixtures are not prefilled here.</p>
        <textarea v-model="factPayload" class="json-input" rows="8" placeholder='[{"fact_type":"...","source_ref":"source-pack://..."}]' />
        <div class="button-row">
          <button class="ghost-action" type="button" @click="initializeManufacturingKernel">Initialize domain model</button>
          <button class="primary-action" type="button" @click="ingestManufacturingFacts">Ingest facts</button>
        </div>
        <DataTable v-if="metrics.length" :rows="metrics.slice(0, 8)" :columns="['metric_id', 'name', 'unit', 'status']" />
        <RawPayload :data="{ metrics: state?.metrics, attention: state?.attention, changes: state?.changes }" />
      </article>

      <article class="management-panel" data-section="entities">
        <header>
          <h2>Entities and impact graph</h2>
          <span>{{ entities.length }} entities</span>
        </header>
        <label class="field-line">
          Entity id
          <input v-model="selectedEntityId" type="text" @keydown.enter.prevent="inspectEntity" />
        </label>
        <label class="field-line">
          Relation target id
          <input v-model="relationTargetId" type="text" />
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" @click="upsertEntity">Upsert line entity</button>
          <button class="ghost-action" type="button" :disabled="!selectedEntityId" @click="inspectEntity">Inspect</button>
          <button class="ghost-action" type="button" @click="resolveEntitySourceKey">Resolve source key</button>
        </div>
        <button class="ghost-action" type="button" :disabled="!selectedEntityId || !relationTargetId" @click="upsertRelation">Upsert relation</button>
        <DataTable v-if="entities.length" :rows="entities.slice(0, 8)" :columns="['entity_id', 'entity_type', 'canonical_key', 'display_name']" />
        <RawPayload :data="entityResult || {}" />
      </article>

      <article class="management-panel" data-section="metrics">
        <header>
          <h2>Metrics and compute</h2>
          <span>{{ metrics.length }} metrics</span>
        </header>
        <label class="field-line">
          Metric id
          <input v-model="selectedMetricId" type="text" @keydown.enter.prevent="inspectMetric" />
        </label>
        <label class="field-line">
          Compute job id
          <input v-model="computeJobId" type="text" />
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" @click="inspectMetric">Lineage</button>
          <button class="ghost-action" type="button" @click="materializeMetricSnapshot">Materialize</button>
          <button class="ghost-action" type="button" @click="planMetricAttention">Attention plan</button>
        </div>
        <div class="button-row">
          <button class="primary-action" type="button" @click="planComputeJob">Plan compute job</button>
          <button class="ghost-action" type="button" :disabled="!computeJobId" @click="runComputeJob">Run job</button>
          <button class="ghost-action" type="button" @click="recomputeMetrics">Recompute all</button>
        </div>
        <RawPayload :data="metricResult || {}" />
      </article>

      <article class="management-panel" data-section="evidence">
        <header>
          <h2>Evidence and quality</h2>
          <span>{{ state?.health?.evidence_count || 0 }} packets</span>
        </header>
        <label class="field-line">
          Evidence packet id
          <input v-model="evidenceId" type="text" @keydown.enter.prevent="inspectEvidence" />
        </label>
        <label class="field-line">
          Quality gate id
          <input v-model="qualityGateId" type="text" @keydown.enter.prevent="inspectQualityGate" />
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" @click="buildEvidencePacket">Build packet</button>
          <button class="ghost-action" type="button" :disabled="!evidenceId" @click="inspectEvidence">Inspect context</button>
          <button class="ghost-action" type="button" :disabled="!evidenceId" @click="evaluateEvidenceQuality">Quality gate</button>
        </div>
        <button class="ghost-action" type="button" :disabled="!qualityGateId" @click="inspectQualityGate">Open quality gate</button>
        <RawPayload :data="evidenceResult || {}" />
      </article>

      <article class="management-panel" data-section="incident-room">
        <header>
          <h2>Incident room</h2>
          <span>{{ incidents.length }} incidents</span>
        </header>
        <label class="field-line">
          New incident
          <textarea v-model="incidentTitle" rows="3" />
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" @click="createIncident">Create incident</button>
          <button class="ghost-action" type="button" :disabled="!selectedIncidentId" @click="openIncidentRoom">Open room</button>
        </div>
        <label class="field-line">
          Current incident
          <select v-model="selectedIncidentId" @change="openIncidentRoom">
            <option value="">Select incident</option>
            <option v-for="incident in incidents" :key="incident.incident_id" :value="incident.incident_id">
              {{ incident.title || incident.incident_id }}
            </option>
          </select>
        </label>
        <DataTable v-if="incidents.length" :rows="incidents.slice(0, 8)" :columns="['incident_id', 'title', 'severity', 'status']" />
        <RawPayload :data="{ room, entities: entities.slice(0, 8), attention: attention.slice(0, 8) }" />
      </article>

      <article class="management-panel" data-section="actions">
        <header>
          <h2>Analysis, playbook, actions</h2>
          <span>{{ recommendedActions.length }} actions</span>
        </header>
        <div class="button-row">
          <button class="ghost-action" type="button" :disabled="!selectedIncidentId" @click="analyzeIncident">Analyze</button>
          <button class="ghost-action" type="button" :disabled="!selectedIncidentId" @click="recommendPlaybooks">Recommend playbooks</button>
          <button class="ghost-action" type="button" :disabled="!selectedIncidentId" @click="promoteCase">Promote case</button>
        </div>
        <label class="field-line">
          Recommended action
          <select v-model="selectedActionId">
            <option value="">Select action</option>
            <option v-for="action in recommendedActions" :key="action.action_id" :value="action.action_id">
              {{ action.title || action.action_id }}
            </option>
          </select>
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" :disabled="!selectedActionId" @click="executeAction">Execute dry run</button>
          <button class="ghost-action" type="button" @click="bridgeExecution">Bridge cross-plane</button>
        </div>
        <RawPayload :data="{ analysis, executions: room?.executions, playbooks: room?.playbooks }" />
      </article>

      <article class="management-panel" data-section="skills">
        <header>
          <h2>Manufacturing skills</h2>
          <span>{{ skills.length }} skills</span>
        </header>
        <label class="field-line">
          Skill
          <select v-model="selectedSkillId">
            <option value="">Select skill</option>
            <option v-for="skill in skills" :key="skill.skill_id" :value="skill.skill_id">
              {{ skill.name || skill.skill_id }}
            </option>
          </select>
        </label>
        <div class="button-row">
          <button class="ghost-action" type="button" :disabled="!selectedIncidentId" @click="planSkills">Plan skills</button>
          <button class="primary-action" type="button" :disabled="!selectedIncidentId || !selectedSkillId" @click="runSkill">Run skill</button>
        </div>
        <DataTable v-if="skills.length" :rows="skills.slice(0, 8)" :columns="['skill_id', 'name', 'risk', 'status']" />
        <RawPayload :data="{ skills: state?.skills, skill_runs: room?.skill_runs || result?.skill_run }" />
      </article>

      <article class="management-panel" data-section="reports">
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
          <button class="primary-action" type="button" @click="generateReport">Generate report</button>
          <button class="ghost-action" type="button" :disabled="!cockpitReportId" @click="retryReportDelivery">Retry delivery</button>
        </div>
        <RawPayload :data="result || {}" />
      </article>
    </section>
  </section>
</template>
