<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue';
import { FileText, RefreshCw, Search } from 'lucide-vue-next';
import { api } from '../api/client';
import RawPayload from '../components/workbench/RawPayload.vue';

const loading = ref(false);
const error = ref('');
const query = ref('');
const scope = ref('all');
const status = ref('all');
const risk = ref('all');
const catalog = ref<any>({});
const projection = ref<any>({});
const runs = ref<any>({});
const detail = ref<any>({});
const files = ref<any>({});
const rawFile = ref<any>({});
const selectedSkillId = ref('');
const selectedFile = ref('SKILL.md');
const actionResult = ref<any>(null);

const items = computed(() => Array.isArray(catalog.value?.items) ? catalog.value.items : []);
const filteredItems = computed(() => items.value.filter((skill: any) => {
  const text = `${skill.id} ${skill.name} ${skill.description || ''} ${(skill.tags || []).join(' ')}`.toLowerCase();
  if (query.value && !text.includes(query.value.toLowerCase())) return false;
  if (scope.value !== 'all' && skill.scope !== scope.value) return false;
  if (status.value !== 'all' && skill.status !== status.value) return false;
  if (risk.value !== 'all' && skill.risk !== risk.value) return false;
  return true;
}));
const facets = computed(() => projection.value?.facets || {});
const skill = computed(() => detail.value?.skill || filteredItems.value.find((item: any) => item.id === selectedSkillId.value) || {});
const fileItems = computed(() => Array.isArray(files.value?.files) ? files.value.files : []);
const runItems = computed(() => Array.isArray(runs.value?.items) ? runs.value.items : []);

async function refresh() {
  loading.value = true;
  error.value = '';
  try {
    const [nextCatalog, nextProjection, nextRuns] = await Promise.all([
      api.skillCatalog(),
      api.skillProjection(),
      api.skillRuns(),
    ]);
    catalog.value = nextCatalog;
    projection.value = nextProjection;
    runs.value = nextRuns;
    if (!selectedSkillId.value) {
      selectedSkillId.value = nextCatalog?.items?.[0]?.id || '';
    }
    await loadSelectedSkill();
  } catch (err) {
    error.value = err instanceof Error ? err.message : String(err);
  } finally {
    loading.value = false;
  }
}

async function loadSelectedSkill() {
  if (!selectedSkillId.value) return;
  const [nextDetail, nextFiles] = await Promise.all([
    api.skillDetail(selectedSkillId.value),
    api.skillFiles(selectedSkillId.value),
  ]);
  detail.value = nextDetail;
  files.value = nextFiles;
  selectedFile.value = nextFiles?.primary || fileItems.value.find((file: any) => file.kind === 'file')?.path || 'SKILL.md';
  await loadRawFile();
}

async function loadRawFile(path = selectedFile.value) {
  if (!selectedSkillId.value || !path) return;
  selectedFile.value = path;
  rawFile.value = await api.skillFileRaw(selectedSkillId.value, path);
}

async function runAction(action: 'validate' | 'plan' | 'run') {
  if (!selectedSkillId.value) return;
  actionResult.value = await api.skillAction(selectedSkillId.value, action, { session_id: 'webui-skills' });
  await refresh();
}

watch(selectedSkillId, loadSelectedSkill);
onMounted(refresh);
</script>

<template>
  <section class="capability-page skills-page">
    <header class="page-header">
      <div>
        <h1>Skills Console</h1>
        <p>技能全集、分类、文件、运行记录和治理状态集中管理。</p>
      </div>
      <button class="primary-action" type="button" :disabled="loading" @click="refresh">
        <RefreshCw :size="15" />
        {{ loading ? 'Loading' : 'Refresh skills' }}
      </button>
    </header>

    <p v-if="error" class="settings-alert">{{ error }}</p>

    <section class="skills-console">
      <aside class="skills-catalog">
        <header class="skills-toolbar">
          <label class="search-field">
            <Search :size="15" />
            <input v-model="query" type="search" placeholder="Search skills" />
          </label>
          <div class="filter-row">
            <select v-model="scope">
              <option value="all">all scopes</option>
              <option v-for="item in facets.scopes || []" :key="item" :value="item">{{ item }}</option>
            </select>
            <select v-model="status">
              <option value="all">all statuses</option>
              <option v-for="item in facets.statuses || []" :key="item" :value="item">{{ item }}</option>
            </select>
            <select v-model="risk">
              <option value="all">all risks</option>
              <option v-for="item in facets.risks || []" :key="item" :value="item">{{ item }}</option>
            </select>
          </div>
        </header>

        <button
          v-for="item in filteredItems"
          :key="item.id"
          class="skill-row"
          :class="{ active: selectedSkillId === item.id }"
          type="button"
          @click="selectedSkillId = item.id"
        >
          <strong>{{ item.name }}</strong>
          <span>{{ item.description || item.source }}</span>
          <small>{{ item.scope }} · {{ item.status }} · {{ item.risk }}</small>
        </button>
      </aside>

      <main class="skills-detail">
        <section class="management-panel">
          <header>
            <h2>Detail</h2>
            <span>{{ skill.scope || 'unknown' }}</span>
          </header>
          <dl class="detail-list">
            <dt>Name</dt>
            <dd>{{ skill.name || '-' }}</dd>
            <dt>Source</dt>
            <dd>{{ skill.source || '-' }}</dd>
            <dt>Path</dt>
            <dd>{{ skill.path || 'virtual' }}</dd>
            <dt>Domain</dt>
            <dd>{{ skill.domain || '-' }}</dd>
            <dt>Tags</dt>
            <dd>{{ (skill.tags || []).join(', ') || '-' }}</dd>
            <dt>Tools</dt>
            <dd>{{ (skill.tools || []).join(', ') || '-' }}</dd>
          </dl>
          <div class="button-row">
            <button class="ghost-action" type="button" @click="runAction('validate')">Validate</button>
            <button class="ghost-action" type="button" @click="runAction('plan')">Plan</button>
            <button class="primary-action" type="button" @click="runAction('run')">Run</button>
          </div>
        </section>

        <section class="management-panel">
          <header>
            <h2>Files</h2>
            <span>{{ fileItems.length }} entries</span>
          </header>
          <div class="skill-files">
            <button
              v-for="file in fileItems"
              :key="file.path"
              class="file-row compact"
              type="button"
              :disabled="file.kind !== 'file'"
              @click="loadRawFile(file.path)"
            >
              <span><FileText :size="14" /> {{ file.path }}</span>
              <small>{{ file.kind }}{{ file.primary ? ' · primary' : '' }}</small>
            </button>
          </div>
          <article class="skill-markdown">
            <header>
              <strong>{{ rawFile.path || selectedFile }}</strong>
            </header>
            <pre>{{ rawFile.content || '' }}</pre>
          </article>
        </section>

        <section class="management-panel">
          <header>
            <h2>Runs and governance</h2>
            <span>{{ runItems.length }} runs</span>
          </header>
          <div class="run-list">
            <article v-for="run in runItems.slice(0, 8)" :key="run.run_id || run.skill_run_id || run.id">
              <strong>{{ run.skill_id || run.skill_name || run.id }}</strong>
              <span>{{ run.status || run.outcome || 'recorded' }}</span>
            </article>
          </div>
          <RawPayload title="Action result" :data="actionResult || { projection, runs }" />
        </section>
      </main>
    </section>
  </section>
</template>
