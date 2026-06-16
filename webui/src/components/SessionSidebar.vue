<script setup lang="ts">
import { Archive, Plus, RefreshCw, Search, Trash2 } from 'lucide-vue-next';
import { useAppStore } from '../stores/app';

const store = useAppStore();

async function searchSessions() {
  await store.refreshSessions();
}
</script>

<template>
  <aside class="session-sidebar">
    <header class="sidebar-head">
      <button class="primary-action" type="button" @click="store.createSession">
        <Plus :size="16" />
        New session
      </button>
      <label class="search-field">
        <Search :size="15" />
        <input v-model="store.sessionQuery" type="search" placeholder="Search sessions" @keydown.enter.prevent="searchSessions" />
      </label>
      <button class="ghost-action" type="button" @click="searchSessions">
        <RefreshCw :size="15" />
        Refresh
      </button>
    </header>

    <div class="session-list" aria-label="Sessions">
      <article
        v-for="session in store.filteredSessions"
        :key="session.id"
        class="session-row"
        :class="{ active: session.id === store.activeSessionId }"
      >
        <button type="button" class="session-open" @click="store.loadMessages(session.id)">
          <span class="session-title">{{ session.title }}</span>
          <span class="session-meta">{{ session.model || 'default model' }} · {{ session.status || 'active' }}</span>
        </button>
        <span class="session-actions">
          <button class="icon-action" type="button" title="Compact session" @click="store.compactSession(session.id)"><Archive :size="13" /></button>
          <button class="icon-action danger" type="button" title="Delete session" @click="store.deleteSession(session.id)"><Trash2 :size="13" /></button>
        </span>
      </article>
    </div>

    <footer class="sidebar-foot">
      <span>Cowd</span>
      <strong>{{ store.settings?.version || '0.9.212' }}</strong>
    </footer>
  </aside>
</template>
