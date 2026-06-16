<script setup lang="ts">
import { computed, onMounted, ref } from 'vue';
import { GitBranch, Play, RefreshCw, Search, Trash2, Users } from 'lucide-vue-next';
import { api } from '../api/client';
import DataTable from '../components/workbench/DataTable.vue';
import EmptyState from '../components/workbench/EmptyState.vue';
import RawPayload from '../components/workbench/RawPayload.vue';
import RequestReceipt from '../components/workbench/RequestReceipt.vue';
import StatusPill from '../components/workbench/StatusPill.vue';

const loading = ref(false);
const error = ref('');
const catalog = ref<any>({});
const directory = ref<any>({});
const discovery = ref<any>({});
const reputation = ref<any>({});
const runs = ref<any>({});
const tasks = ref<any>({});
const graph = ref<any>({});
const teamProfiles = ref<any>({});
const actionResult = ref<any>(null);
const profileResult = ref<any>(null);
const selectedTaskId = ref('');
const selectedProfileId = ref('');
const objective = ref('');
const discoverQuery = ref('');
const profileName = ref('WebUI Review Team');
const profileLeader = ref('');
const profileMembers = ref('planner,executor,reviewer');
const profilePolicy = ref('{"max_parallel_agents":3,"requires_review":true}');
const profileEvaluation = ref('{"success_metric":"accepted_phase_review","quality_gate":"all_tests_pass"}');
const phaseName = ref('');
const phaseObjective = ref('');
const artifactLabel = ref('');
const artifactValue = ref('');
const reviewResult = ref('');
const failureReason = ref('');

const agentRows = computed(() => (Array.isArray(directory.value?.agents) ? directory.value.agents : Array.isArray(catalog.value?.agents) ? catalog.value.agents : []).map((agent: any) => ({
  name: agent.name,
  active: agent.active,
  source: agent.source?.id || agent.source || '-',
  model: agent.model || '-',
  description: agent.description || '-',
})));
const discoveredRows = computed(() => (Array.isArray(discovery.value?.agents) ? discovery.value.agents : []).map((agent: any) => ({
  agent_id: agent.agent_id,
  role: agent.role,
  reputation: agent.reputation ?? '-',
  status: agent.status,
})));
const taskItems = computed(() => Array.isArray(tasks.value?.tasks) ? tasks.value.tasks : []);
const selectedTask = computed(() => taskItems.value.find((task: any) => task.id === selectedTaskId.value) || tasks.value?.current || taskItems.value[0] || null);
const phaseItems = computed(() => Array.isArray(selectedTask.value?.phases) ? selectedTask.value.phases : []);
const currentPhase = computed(() => phaseItems.value.find((phase: any) => phase.id === selectedTask.value?.current_phase) || phaseItems.value[0] || null);
const graphNodes = computed(() => Array.isArray(graph.value?.nodes) ? graph.value.nodes : []);
const runItems = computed(() => Array.isArray(runs.value?.runs) ? runs.value.runs : []);
const teamProfileItems = computed(() => Array.isArray(teamProfiles.value?.profiles) ? teamProfiles.value.profiles : []);
const reputationRows = computed(() => (Array.isArray(reputation.value?.items) ? reputation.value.items : []).map((item: any) => ({
  agent_id: item.agent_id || item.name || '-',
  reputation: item.reputation ?? '-',
  status: item.status ?? '-',
})));
const openTasks = computed(() => taskItems.value.filter((task: any) => !['completed', 'cancelled'].includes(String(task.status))).length);

async function refresh() {
  loading.value = true;
  error.value = '';
  try {
    const [nextCatalog, nextDirectory, nextReputation, nextRuns, nextTasks, nextProfiles] = await Promise.all([
      api.agentCatalog(),
      api.agentDirectory(),
      api.agentReputation(),
      api.agentRuns(),
      api.tasks(),
      api.agentTeamProfiles(),
    ]);
    catalog.value = nextCatalog;
    directory.value = nextDirectory;
    reputation.value = nextReputation;
    runs.value = nextRuns;
    tasks.value = nextTasks;
    teamProfiles.value = nextProfiles;
    if (!selectedTaskId.value) {
      selectedTaskId.value = nextTasks?.current?.id || nextTasks?.tasks?.[0]?.id || '';
    }
    if (!selectedProfileId.value) selectedProfileId.value = nextProfiles?.profiles?.[0]?.id || '';
    await loadGraph();
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  } finally {
    loading.value = false;
  }
}

async function loadGraph() {
  if (!selectedTaskId.value) {
    graph.value = {};
    return;
  }
  graph.value = await api.taskAgentGraph(selectedTaskId.value);
}

async function startTask() {
  if (!objective.value.trim()) {
    error.value = 'Objective is required before starting a task.';
    return;
  }
  actionResult.value = await api.startTask(objective.value, false);
  selectedTaskId.value = actionResult.value?.id || selectedTaskId.value;
  await refresh();
}

async function addPhase() {
  const taskId = selectedTaskId.value || selectedTask.value?.id;
  if (!taskId) return;
  if (!phaseName.value.trim() || !phaseObjective.value.trim()) {
    error.value = 'Phase name and objective are required.';
    return;
  }
  actionResult.value = await api.startTaskPhase(taskId, {
    name: phaseName.value,
    objective: phaseObjective.value,
    plan: [],
    acceptance: [],
    test_commands: [],
  });
  await refresh();
}

async function recordArtifact() {
  const taskId = selectedTaskId.value || selectedTask.value?.id;
  const phaseId = currentPhase.value?.id;
  if (!taskId || !phaseId) return;
  if (!artifactLabel.value.trim() || !artifactValue.value.trim()) {
    error.value = 'Artifact label and value are required.';
    return;
  }
  actionResult.value = await api.recordTaskArtifact(taskId, phaseId, {
    kind: 'validation',
    label: artifactLabel.value,
    value: artifactValue.value,
  });
  await refresh();
}

async function reviewPhase(completed = true) {
  const taskId = selectedTaskId.value || selectedTask.value?.id;
  const phaseId = currentPhase.value?.id;
  if (!taskId || !phaseId) return;
  if (!reviewResult.value.trim()) {
    error.value = 'Review result is required.';
    return;
  }
  actionResult.value = await api.reviewTaskPhase(taskId, phaseId, reviewResult.value, completed);
  await refresh();
}

async function transitionTask(action: 'complete' | 'cancel' | 'failure') {
  const taskId = selectedTaskId.value || selectedTask.value?.id;
  if (!taskId) return;
  if (action === 'complete') actionResult.value = await api.completeTask(taskId);
  if (action === 'cancel') actionResult.value = await api.cancelTask(taskId);
  if (action === 'failure') {
    if (!failureReason.value.trim()) {
      error.value = 'Failure reason is required.';
      return;
    }
    actionResult.value = await api.recordTaskFailure(taskId, failureReason.value);
  }
  await refresh();
}

async function discoverAgents() {
  if (!discoverQuery.value.trim()) {
    error.value = 'Task description is required before discovering agents.';
    return;
  }
  discovery.value = await api.agentAssemble(discoverQuery.value);
}

function teamProfilePayload() {
  let policy = {};
  let evaluation = {};
  try {
    policy = JSON.parse(profilePolicy.value || '{}');
  } catch {
    error.value = 'Team profile policy must be valid JSON.';
    throw new Error(error.value);
  }
  try {
    evaluation = JSON.parse(profileEvaluation.value || '{}');
  } catch {
    error.value = 'Team profile evaluation must be valid JSON.';
    throw new Error(error.value);
  }
  return {
    id: selectedProfileId.value || undefined,
    name: profileName.value,
    objective: objective.value || discoverQuery.value,
    leader: profileLeader.value || undefined,
    members: profileMembers.value.split(',').map((item) => item.trim()).filter(Boolean),
    policy,
    evaluation,
  };
}

function loadProfileIntoForm(profile: any) {
  selectedProfileId.value = profile.id || '';
  profileName.value = profile.name || '';
  objective.value = profile.objective || objective.value;
  discoverQuery.value = profile.objective || discoverQuery.value;
  profileLeader.value = profile.leader || '';
  profileMembers.value = Array.isArray(profile.members) ? profile.members.join(',') : '';
  profilePolicy.value = JSON.stringify(profile.policy || {}, null, 2);
  profileEvaluation.value = JSON.stringify(profile.evaluation || {}, null, 2);
}

async function selectTeamProfile(id: string) {
  selectedProfileId.value = id;
  const detail = await api.agentTeamProfile(id);
  profileResult.value = detail;
  if (detail?.profile) loadProfileIntoForm(detail.profile);
}

async function saveTeamProfile() {
  const payload = teamProfilePayload();
  profileResult.value = selectedProfileId.value
    ? await api.updateAgentTeamProfile(selectedProfileId.value, payload)
    : await api.createAgentTeamProfile(payload);
  selectedProfileId.value = profileResult.value?.profile?.id || selectedProfileId.value;
  await refresh();
}

async function createTeamProfile() {
  selectedProfileId.value = '';
  profileResult.value = await api.createAgentTeamProfile(teamProfilePayload());
  selectedProfileId.value = profileResult.value?.profile?.id || '';
  await refresh();
}

async function deleteTeamProfile() {
  if (!selectedProfileId.value) return;
  profileResult.value = await api.deleteAgentTeamProfile(selectedProfileId.value);
  selectedProfileId.value = '';
  await refresh();
}

function reuseTeamProfile() {
  const profile = teamProfileItems.value.find((item: any) => item.id === selectedProfileId.value);
  if (profile) loadProfileIntoForm(profile);
}

async function upsertGraphTemplate() {
  const taskId = selectedTaskId.value || selectedTask.value?.id;
  if (!taskId) return;
  if (!objective.value.trim() || !phaseObjective.value.trim()) {
    error.value = 'Objective and phase objective are required before generating an agent graph.';
    return;
  }
  const now = Date.now();
  const nodes = [
    {
      id: 'planner',
      role: 'planner',
      title: 'Plan',
      objective: objective.value,
      depends_on: [],
      status: 'ready',
      assigned_agent: discovery.value?.team?.leader?.agent_id || null,
      result: null,
      error: null,
      created_at_ms: now,
      updated_at_ms: now,
    },
    {
      id: 'executor',
      role: 'executor',
      title: 'Execute',
      objective: phaseObjective.value,
      depends_on: ['planner'],
      status: 'pending',
      assigned_agent: discovery.value?.team?.workers?.[0]?.agent_id || null,
      result: null,
      error: null,
      created_at_ms: now,
      updated_at_ms: now,
    },
    {
      id: 'reviewer',
      role: 'reviewer',
      title: 'Review',
      objective: reviewResult.value || 'Review the selected task phase and attached artifacts.',
      depends_on: ['executor'],
      status: 'pending',
      assigned_agent: discovery.value?.team?.workers?.[1]?.agent_id || null,
      result: null,
      error: null,
      created_at_ms: now,
      updated_at_ms: now,
    },
  ];
  actionResult.value = await api.upsertTaskAgentGraph(taskId, { objective: objective.value, nodes });
  await loadGraph();
}

function selectTask(id: string) {
  selectedTaskId.value = id;
  loadGraph();
}

onMounted(refresh);
</script>

<template>
  <section class="capability-page agents-page">
    <header class="page-header">
      <div>
        <h1>Agents Workbench</h1>
        <p>Agent 目录、任务生命周期、阶段验收、工件记录和并行执行图统一管理。</p>
      </div>
      <button class="primary-action" type="button" :disabled="loading" @click="refresh">
        <RefreshCw :size="15" />
        {{ loading ? 'Loading' : 'Refresh agents' }}
      </button>
    </header>

    <p v-if="error" class="settings-alert">{{ error }}</p>

    <section class="metric-row">
      <article class="metric-card">
        <span>Agents</span>
        <strong>{{ catalog.summary?.active || 0 }}/{{ catalog.summary?.total || 0 }}</strong>
        <small>active / total definitions</small>
      </article>
      <article class="metric-card" data-tone="info">
        <span>Open tasks</span>
        <strong>{{ openTasks }}</strong>
        <small>{{ taskItems.length }} total task records</small>
      </article>
      <article class="metric-card" data-tone="success">
        <span>Run graphs</span>
        <strong>{{ runItems.length }}</strong>
        <small>{{ graphNodes.length }} selected graph nodes</small>
      </article>
      <article class="metric-card" data-tone="info">
        <span>Team profiles</span>
        <strong>{{ teamProfileItems.length }}</strong>
        <small>persistent reusable teams</small>
      </article>
    </section>

    <section class="agents-workbench-grid">
      <section class="management-panel agents-panel" data-section="catalog">
        <header>
          <h2>Agent directory</h2>
          <span>{{ agentRows.length }} definitions</span>
        </header>
        <DataTable v-if="agentRows.length" :rows="agentRows" :columns="['name', 'active', 'source', 'model', 'description']" />
        <EmptyState v-else title="No agents registered" detail="后端没有发现 .cowd/agents、~/.cowd/agents 或 $CC_CONFIG_HOME/agents 定义。" />
      </section>

      <section class="management-panel agents-panel" data-section="discovery">
        <header>
          <h2>Discover team</h2>
          <span>{{ discovery.count || 0 }} matches</span>
        </header>
        <label class="search-field">
          <Search :size="15" />
          <input v-model="discoverQuery" type="search" placeholder="Task for team discovery" @keyup.enter="discoverAgents" />
        </label>
        <button class="primary-action" type="button" @click="discoverAgents">Assemble team</button>
        <DataTable v-if="discoveredRows.length" :rows="discoveredRows" :columns="['agent_id', 'role', 'reputation', 'status']" />
        <EmptyState v-else title="No matching team" detail="发现协议没有找到满足任务描述的 agent。" />
        <RawPayload title="Auto assembled team" :data="discovery.team || {}" />
        <DataTable v-if="reputationRows.length" :rows="reputationRows" :columns="['agent_id', 'reputation', 'status']" />
      </section>

      <section class="management-panel agents-panel wide" data-section="discovery">
        <header>
          <h2>Persistent team profiles</h2>
          <span>{{ teamProfileItems.length }} saved</span>
        </header>
        <div class="agents-task-layout">
          <aside class="task-list">
            <button
              v-for="profile in teamProfileItems"
              :key="profile.id"
              class="memory-entry-row"
              :class="{ active: selectedProfileId === profile.id }"
              type="button"
              @click="selectTeamProfile(profile.id)"
            >
              <strong>{{ profile.name }}</strong>
              <span>{{ profile.id }}</span>
              <small>{{ profile.members?.length || 0 }} members · {{ profile.leader || 'no leader' }}</small>
            </button>
            <EmptyState v-if="!teamProfileItems.length" title="No team profiles" detail="保存发现结果后可复用到后续任务和执行图。" />
          </aside>
          <main>
            <label class="field-line">
              Profile name
              <input v-model="profileName" type="text" />
            </label>
            <label class="field-line">
              Leader
              <input v-model="profileLeader" type="text" />
            </label>
            <label class="field-line">
              Members
              <input v-model="profileMembers" type="text" />
            </label>
            <label class="field-line">
              Policy JSON
              <textarea v-model="profilePolicy" rows="4" />
            </label>
            <label class="field-line">
              Evaluation JSON
              <textarea v-model="profileEvaluation" rows="4" />
            </label>
            <div class="button-row">
              <button class="primary-action" type="button" @click="saveTeamProfile">Save profile</button>
              <button class="ghost-action" type="button" @click="createTeamProfile">Create copy</button>
              <button class="ghost-action" type="button" :disabled="!selectedProfileId" @click="reuseTeamProfile">Reuse</button>
              <button class="icon-action danger" type="button" :disabled="!selectedProfileId" aria-label="Delete team profile" @click="deleteTeamProfile"><Trash2 :size="14" /></button>
            </div>
            <RequestReceipt :receipt="profileResult?.receipt || profileResult" title="Team profile receipt" />
          </main>
        </div>
      </section>

      <section class="management-panel agents-panel wide" data-section="tasks">
        <header>
          <h2>Task control</h2>
          <span>{{ selectedTask?.status || 'no task' }}</span>
        </header>
        <div class="agents-task-layout">
          <aside class="task-list">
            <button
              v-for="task in taskItems"
              :key="task.id"
              class="memory-entry-row"
              :class="{ active: selectedTaskId === task.id }"
              type="button"
              @click="selectTask(task.id)"
            >
              <strong>{{ task.objective || task.id }}</strong>
              <span>{{ task.id }}</span>
              <small>{{ task.status }} · failures {{ task.failure_count || 0 }}</small>
            </button>
            <EmptyState v-if="!taskItems.length" title="No tasks" detail="创建任务后会在这里管理阶段、工件和验收。" />
          </aside>
          <main>
            <label class="field-line">
              Objective
              <textarea v-model="objective" rows="3" />
            </label>
            <div class="button-row">
              <button class="primary-action" type="button" @click="startTask"><Play :size="15" /> Start task</button>
              <button class="ghost-action" type="button" :disabled="!selectedTask" @click="transitionTask('complete')">Complete</button>
              <button class="ghost-action" type="button" :disabled="!selectedTask" @click="transitionTask('cancel')">Cancel</button>
              <button class="ghost-action" type="button" :disabled="!selectedTask" @click="transitionTask('failure')">Record failure</button>
            </div>
            <label class="field-line">
              Failure reason
              <input v-model="failureReason" type="text" />
            </label>
            <RequestReceipt :receipt="actionResult" title="Task receipt" />
          </main>
        </div>
      </section>

      <section class="management-panel agents-panel" data-section="tasks">
        <header>
          <h2>Phase gate</h2>
          <span>{{ phaseItems.length }} phases</span>
        </header>
        <label class="field-line">
          Phase name
          <input v-model="phaseName" type="text" />
        </label>
        <label class="field-line">
          Phase objective
          <textarea v-model="phaseObjective" rows="3" />
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" :disabled="!selectedTask" @click="addPhase">Add phase</button>
          <button class="ghost-action" type="button" :disabled="!currentPhase" @click="reviewPhase(true)">Review complete</button>
        </div>
        <RequestReceipt :receipt="actionResult" title="Phase receipt" />
        <DataTable v-if="phaseItems.length" :rows="phaseItems" />
        <EmptyState v-else title="No phases" detail="阶段用于 TDD 目标、计划、验收和测试命令闭环。" />
      </section>

      <section class="management-panel agents-panel" data-section="reviews">
        <header>
          <h2>Artifacts and review</h2>
          <span>{{ currentPhase?.id || 'no phase' }}</span>
        </header>
        <label class="field-line">
          Artifact label
          <input v-model="artifactLabel" type="text" />
        </label>
        <label class="field-line">
          Artifact value
          <textarea v-model="artifactValue" rows="3" />
        </label>
        <label class="field-line">
          Review result
          <textarea v-model="reviewResult" rows="3" />
        </label>
        <div class="button-row">
          <button class="primary-action" type="button" :disabled="!currentPhase" @click="recordArtifact">Record artifact</button>
          <button class="ghost-action" type="button" :disabled="!currentPhase" @click="reviewPhase(false)">Save review</button>
        </div>
        <RequestReceipt :receipt="actionResult" title="Review receipt" />
      </section>

      <section class="management-panel agents-panel wide" data-section="graph">
        <header>
          <h2>Agent execution graph</h2>
          <span><StatusPill :status="graph.status || 'offline'" /></span>
        </header>
        <div class="button-row">
          <button class="primary-action" type="button" :disabled="!selectedTask" @click="upsertGraphTemplate">
            <GitBranch :size="15" />
            Upsert planner/executor/reviewer graph
          </button>
          <button class="ghost-action" type="button" :disabled="!selectedTask" @click="loadGraph">
            <Users :size="15" />
            Reload graph
          </button>
        </div>
        <div class="agent-graph-lanes">
          <article v-for="node in graphNodes" :key="node.id">
            <strong>{{ node.title }}</strong>
            <StatusPill :status="node.status" />
            <p>{{ node.objective }}</p>
            <small>{{ node.role }} · {{ node.assigned_agent || 'unassigned' }} · depends {{ (node.depends_on || []).join(', ') || '-' }}</small>
          </article>
        </div>
        <EmptyState v-if="!graphNodes.length" title="No agent graph" detail="选择任务后可以生成 planner/executor/reviewer 执行图。" />
        <RequestReceipt :receipt="actionResult || graph" title="Agent graph receipt" />
      </section>

      <section class="management-panel agents-panel wide" data-section="runs">
        <header>
          <h2>Runs and evidence</h2>
          <span>{{ runItems.length }} graphs</span>
        </header>
        <RawPayload title="Agent runs" :data="runs" />
        <RequestReceipt :receipt="actionResult || profileResult" title="Agent action receipt" />
        <RawPayload title="Action result" :data="actionResult || graph" />
      </section>
    </section>
  </section>
</template>
