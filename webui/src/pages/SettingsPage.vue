<script setup lang="ts">
import { computed, reactive } from 'vue';
import { Moon, Save, Shield, Sun } from 'lucide-vue-next';
import { useAppStore } from '../stores/app';

const store = useAppStore();
const form = reactive({
  model: store.settings?.model || 'claude-sonnet-4-6',
  profile: store.settings?.profile || 'default',
});
const theme = computed({
  get: () => document.documentElement.dataset.theme || 'dark',
  set: (value: string) => {
    document.documentElement.dataset.theme = value;
    localStorage.setItem('cowd-theme', value);
  },
});
</script>

<template>
  <section class="settings-page">
    <header class="page-header">
      <div>
        <h1>Settings</h1>
        <p>模型、外观、profile、运行策略、渠道和安全控制集中管理。</p>
      </div>
      <button class="primary-action" type="button" @click="store.saveSettings(form)"><Save :size="15" /> Save changes</button>
    </header>

    <div class="settings-grid">
      <section class="settings-section">
        <h2>Appearance</h2>
        <div class="segmented">
          <button :class="{ active: theme === 'light' }" type="button" @click="theme = 'light'"><Sun :size="15" /> Light</button>
          <button :class="{ active: theme === 'dark' }" type="button" @click="theme = 'dark'"><Moon :size="15" /> Dark</button>
        </div>
      </section>

      <section class="settings-section">
        <h2>Model and profile</h2>
        <label>Default model<input v-model="form.model" /></label>
        <label>Profile<input v-model="form.profile" /></label>
        <p v-if="store.settingsSavedAt" class="save-state">Saved at {{ store.settingsSavedAt }}</p>
      </section>

      <section class="settings-section">
        <h2>Runtime policy</h2>
        <label><input type="checkbox" checked /> Emit runtime events to WebUI and TUI</label>
        <label><input type="checkbox" checked /> Auto-open Activity on tool calls</label>
        <label><input type="checkbox" checked /> Keep CLI minimal</label>
      </section>

      <section class="settings-section">
        <h2>Security</h2>
        <p class="security-note"><Shield :size="16" /> Write operations require kernel approval policy when configured.</p>
      </section>
    </div>
  </section>
</template>
