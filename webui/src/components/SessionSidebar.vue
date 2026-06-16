<script setup lang="ts">
import { Plus, Search } from 'lucide-vue-next';
import { useAppStore } from '../stores/app';

const store = useAppStore();
</script>

<template>
  <aside class="session-sidebar">
    <header class="sidebar-head">
      <button class="primary-action" type="button">
        <Plus :size="16" />
        New session
      </button>
      <label class="search-field">
        <Search :size="15" />
        <input type="search" placeholder="Search sessions" />
      </label>
    </header>

    <div class="session-list" aria-label="Sessions">
      <button
        v-for="session in store.sessions"
        :key="session.id"
        class="session-row"
        :class="{ active: session.id === store.activeSessionId }"
        type="button"
        @click="store.loadMessages(session.id)"
      >
        <span class="session-title">{{ session.title }}</span>
        <span class="session-meta">{{ session.model || 'default model' }} · {{ session.status || 'active' }}</span>
      </button>
    </div>

    <footer class="sidebar-foot">
      <span>Cowd</span>
      <strong>{{ store.settings?.version || '0.9.212' }}</strong>
    </footer>
  </aside>
</template>
