<script setup lang="ts">
import { computed, ref } from 'vue';
import { Moon, Plus, RefreshCw, Shield, Sun, Trash2 } from 'lucide-vue-next';
import { useAppStore } from '../stores/app';

const store = useAppStore();
const profileName = ref('');
const defaultModel = ref('');
const settingsError = ref('');
const busyAction = ref('');
const authResult = ref<any>(null);
const authState = computed(() => localStorage.getItem('cowd-auth-token') ? 'stored in browser' : 'not stored');
const origin = computed(() => location.origin);
const providerModels = computed(() => store.providers?.models || []);
const providerRows = computed(() => store.providers?.providers || []);
const configuredModel = computed(() => store.providers?.configured_model || store.controlPlane?.configured_model || store.settings?.model || '');

const theme = computed({
  get: () => document.documentElement.dataset.theme || 'dark',
  set: (value: string) => {
    document.documentElement.dataset.theme = value;
    localStorage.setItem('cowd-theme', value);
  },
});

const approvalJson = computed({
  get: () => JSON.stringify(store.approvalConfig || {}, null, 2),
  set: () => undefined,
});

async function run(label: string, action: () => Promise<unknown>) {
  settingsError.value = '';
  busyAction.value = label;
  try {
    await action();
  } catch (error) {
    settingsError.value = error instanceof Error ? error.message : String(error);
  } finally {
    busyAction.value = '';
  }
}

async function addProfile() {
  const name = profileName.value.trim();
  if (!name) return;
  await run('profile-create', async () => {
    await store.createProfile(name);
    profileName.value = '';
  });
}

async function saveDefaultModel() {
  const model = defaultModel.value || configuredModel.value;
  if (!model) return;
  await run('model-save', () => store.saveDefaultModel(model));
}

async function saveApprovalFromText(event: Event) {
  const value = (event.target as HTMLTextAreaElement).value;
  await run('approval-save', async () => {
    await store.saveApprovalConfig(JSON.parse(value));
  });
}

async function verifyAuth() {
  await run('auth-verify', async () => {
    authResult.value = await store.verifyAuth();
  });
}

function clearToken() {
  localStorage.removeItem('cowd-auth-token');
  authResult.value = { authenticated: false, status: 'cleared' };
}
</script>

<template>
  <section class="settings-page">
    <header class="page-header">
      <div>
        <h1>Settings</h1>
        <p>模型、provider、profile、approval 和安全状态均接后端真实接口，失败会显示后端错误。</p>
      </div>
      <button class="primary-action" type="button" :disabled="busyAction === 'providers'" @click="run('providers', store.reloadProviders)">
        <RefreshCw :size="15" />
        Reload providers
      </button>
    </header>

    <p v-if="settingsError" class="settings-alert">{{ settingsError }}</p>

    <div class="settings-grid">
      <section class="settings-section">
        <h2>Appearance</h2>
        <div class="segmented">
          <button :class="{ active: theme === 'light' }" type="button" @click="theme = 'light'"><Sun :size="15" /> Light</button>
          <button :class="{ active: theme === 'dark' }" type="button" @click="theme = 'dark'"><Moon :size="15" /> Dark</button>
        </div>
      </section>

      <section class="settings-section">
        <h2>Runtime model source</h2>
        <dl class="contract-list">
          <dt>Configured model</dt>
          <dd>{{ configuredModel || 'unknown' }}</dd>
          <dt>Provider status</dt>
          <dd>{{ store.providers?.configured_model_resolved === false ? 'degraded' : (store.controlPlane?.provider_status || 'unknown') }}</dd>
          <dt>Providers</dt>
          <dd>{{ providerRows.map((provider) => provider.name).join(', ') || 'none reported' }}</dd>
          <dt>Model count</dt>
          <dd>{{ store.providers?.provider_model_count ?? store.controlPlane?.provider_model_count ?? 0 }}</dd>
        </dl>
        <label>
          Default model
          <select v-model="defaultModel">
            <option value="">Keep {{ configuredModel || 'current' }}</option>
            <option v-for="model in providerModels" :key="model.id || model.name" :value="model.id || model.name">
              {{ model.id || model.name }} · {{ model.provider }}
            </option>
          </select>
        </label>
        <button class="ghost-action" type="button" :disabled="!defaultModel && !configuredModel" @click="saveDefaultModel">Save default model</button>
        <div class="profile-list">
          <article v-for="provider in providerRows" :key="provider.name" class="profile-row">
            <div>
              <strong>{{ provider.name }}</strong>
              <span>{{ provider.protocol || 'openai-compat' }} · {{ provider.model_count }} models · credential {{ provider.credential_present ? 'present' : 'missing' }}</span>
            </div>
          </article>
        </div>
      </section>

      <section class="settings-section">
        <h2>Profiles</h2>
        <div class="profile-create-row">
          <input v-model="profileName" placeholder="New profile name" @keydown.enter.prevent="addProfile" />
          <button class="ghost-action" type="button" @click="addProfile"><Plus :size="14" /> Create</button>
        </div>
        <div class="profile-list">
          <article v-for="profile in store.profiles" :key="profile.id || profile.name" class="profile-row">
            <div>
              <strong>{{ profile.name || profile.id }}</strong>
              <span>{{ profile.id }}</span>
            </div>
            <div>
              <button
                class="ghost-action"
                type="button"
                :disabled="(profile.id || profile.name) === store.selectedProfile"
                @click="run(`profile-${profile.id}`, () => store.chooseProfile(profile.id || profile.name))"
              >
                {{ (profile.id || profile.name) === store.selectedProfile ? 'Active' : 'Switch' }}
              </button>
              <button v-if="(profile.id || profile.name) !== 'default'" class="icon-action danger" type="button" @click="run(`delete-${profile.id}`, () => store.deleteProfile(profile.id || profile.name))">
                <Trash2 :size="14" />
              </button>
            </div>
          </article>
        </div>
      </section>

      <section class="settings-section">
        <h2>Approval policy</h2>
        <label><input type="checkbox" :checked="!!store.approvalConfig?.solo_mode" @change="run('solo', store.toggleSolo)" /> Solo mode</label>
        <textarea :value="approvalJson" spellcheck="false" @change="saveApprovalFromText" />
        <p v-if="store.settingsSavedAt" class="save-state">Approval saved at {{ store.settingsSavedAt }}</p>
      </section>

      <section class="settings-section">
        <h2>Security</h2>
        <p class="security-note"><Shield :size="16" /> Auth token: {{ authState }}</p>
        <p class="security-note">Origin: {{ origin }}</p>
        <div class="button-row">
          <button class="ghost-action" type="button" @click="verifyAuth">Verify token</button>
          <button class="ghost-action" type="button" @click="clearToken">Clear token</button>
        </div>
        <dl v-if="authResult" class="contract-list">
          <dt>Status</dt>
          <dd>{{ authResult.status || authResult.authenticated || 'unknown' }}</dd>
          <dt>Error</dt>
          <dd>{{ authResult.__error || authResult.error || '-' }}</dd>
        </dl>
      </section>
    </div>
  </section>
</template>
