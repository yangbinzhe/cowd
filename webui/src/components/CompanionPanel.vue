<script setup lang="ts">
import { computed } from 'vue';
import { ChevronUp, FileText, Folder, RotateCcw, Save, Workflow } from 'lucide-vue-next';
import { useAppStore } from '../stores/app';

const store = useAppStore();

const breadcrumbs = computed(() => {
  const parts = store.workspaceDir.split('/').filter(Boolean);
  return [{ label: 'root', path: '' }, ...parts.map((part, index) => ({
    label: part,
    path: parts.slice(0, index + 1).join('/'),
  }))];
});

const parentDir = computed(() => {
  const parts = store.workspaceDir.split('/').filter(Boolean);
  parts.pop();
  return parts.join('/');
});

function openFile(path: string, kind: string) {
  if (kind === 'dir') store.loadWorkspace(path);
  else store.openFile(path);
}
</script>

<template>
  <aside class="companion-panel" aria-label="Cowd companion panel">
    <div class="companion-tabs" role="tablist">
      <button :class="{ active: store.companionTab === 'activity' }" type="button" @click="store.openCompanion('activity')">
        <Workflow :size="15" />
        Activity
      </button>
      <button :class="{ active: store.companionTab === 'workspace' }" type="button" @click="store.openCompanion('workspace')">
        <Folder :size="15" />
        Workspace
      </button>
    </div>

    <section v-if="store.companionTab === 'activity'" class="companion-body">
      <div class="panel-title">
        <h2>Execution stream</h2>
        <span>{{ store.activity.length }} events</span>
      </div>
      <div class="activity-list">
        <article v-for="event in store.activity" :key="event.id" class="activity-item" :data-kind="event.kind">
          <div>
            <strong>{{ event.title }}</strong>
            <p>{{ event.detail || 'No detail available.' }}</p>
          </div>
          <span>{{ event.status || 'seen' }}</span>
        </article>
      </div>
    </section>

    <section v-else class="companion-body workspace-tab">
      <div class="panel-title">
        <h2>Workspace</h2>
        <span>{{ store.workspaceFiles.length }} items</span>
      </div>
      <div class="workspace-root" :title="store.workspaceRoot">{{ store.workspaceRoot || 'gateway workspace' }}</div>
      <nav class="breadcrumbs" aria-label="Workspace breadcrumbs">
        <button v-for="crumb in breadcrumbs" :key="crumb.path || 'root'" type="button" @click="store.loadWorkspace(crumb.path)">
          {{ crumb.label }}
        </button>
      </nav>
      <button class="ghost-action" type="button" @click="store.loadWorkspace(parentDir)">
        <ChevronUp :size="15" />
        Parent folder
      </button>
      <div class="file-list">
        <button
          v-for="file in store.workspaceFiles"
          :key="file.path"
          class="file-row"
          type="button"
          @click="openFile(file.path, file.kind)"
        >
          <Folder v-if="file.kind === 'dir'" :size="16" />
          <FileText v-else :size="16" />
          <span>{{ file.name }}</span>
          <small>{{ file.kind }}</small>
        </button>
      </div>
      <div class="preview-pane" v-if="store.selectedFile">
        <div class="preview-head">
          <strong>{{ store.selectedFile }}</strong>
          <div>
            <button class="icon-action" type="button" :disabled="!store.editorDirty" @click="store.resetFile"><RotateCcw :size="14" /></button>
            <button class="icon-action" type="button" :disabled="!store.editorDirty" @click="store.saveFile"><Save :size="14" /></button>
          </div>
        </div>
        <textarea v-model="store.editorContent" spellcheck="false" />
        <span class="dirty-state" :class="{ dirty: store.editorDirty }">{{ store.editorDirty ? 'Unsaved changes' : 'Saved' }}</span>
      </div>
    </section>
  </aside>
</template>
