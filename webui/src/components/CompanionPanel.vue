<script setup lang="ts">
import { computed, ref } from 'vue';
import { Brain, ChevronUp, Eye, FilePenLine, FilePlus2, FileText, Folder, FolderPlus, Info, Link2, RotateCcw, Save, Search, Trash2, Upload, Workflow, X } from 'lucide-vue-next';
import { useAppStore } from '../stores/app';
import MarkdownBlock from './MarkdownBlock.vue';

const store = useAppStore();
const newFolderName = ref('');
const renamePath = ref('');
const renameTarget = ref('');
const fileInput = ref<HTMLInputElement | null>(null);

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

const selectedExt = computed(() => store.selectedFile.split('.').pop()?.toLowerCase() || '');
const isMarkdown = computed(() => ['md', 'markdown'].includes(selectedExt.value));
const isImage = computed(() => ['png', 'jpg', 'jpeg', 'gif', 'webp', 'svg'].includes(selectedExt.value));
const isStructured = computed(() => ['json', 'yaml', 'yml', 'toml'].includes(selectedExt.value));
const rawFileUrl = computed(() => `/api/file/raw?path=${encodeURIComponent(store.selectedFile)}`);
const canEdit = computed(() => !!store.selectedFile && !isImage.value);
const thinkingEvents = computed(() => store.activity.filter((event) => event.kind === 'think'));
const inspectorEvents = computed(() => store.activity.filter((event) => event.kind === 'error' || event.status === 'error'));

function openFile(path: string, kind: string) {
  if (kind === 'dir') store.loadWorkspace(path);
  else store.openFile(path);
}

async function createFolder() {
  const name = newFolderName.value.trim();
  if (!name) return;
  await store.createWorkspaceDir(name);
  newFolderName.value = '';
}

async function uploadFiles(files: FileList | null) {
  if (!files?.length) return;
  for (const file of Array.from(files)) {
    await store.uploadWorkspaceFile(file);
  }
  if (fileInput.value) fileInput.value.value = '';
}

async function dropUpload(event: DragEvent) {
  event.preventDefault();
  await uploadFiles(event.dataTransfer?.files || null);
}

function startRename(path: string) {
  renamePath.value = path;
  renameTarget.value = path;
}

async function commitRename() {
  if (!renamePath.value || !renameTarget.value.trim()) return;
  await store.renameWorkspacePath(renamePath.value, renameTarget.value.trim());
  renamePath.value = '';
  renameTarget.value = '';
}
</script>

<template>
  <aside class="companion-panel" aria-label="Cowd companion panel">
    <div class="companion-tabs" role="tablist">
      <button :class="{ active: store.companionTab === 'activity' }" type="button" @click="store.openCompanion('activity')">
        <Workflow :size="15" />
        <span>Activity</span>
      </button>
      <button :class="{ active: store.companionTab === 'thinking' }" type="button" @click="store.openCompanion('thinking')">
        <Brain :size="15" />
        <span>Thinking</span>
      </button>
      <button :class="{ active: store.companionTab === 'workspace' }" type="button" @click="store.openCompanion('workspace')">
        <Folder :size="15" />
        <span>Workspace</span>
      </button>
      <button :class="{ active: store.companionTab === 'inspector' }" type="button" @click="store.openCompanion('inspector')">
        <Info :size="15" />
        <span>Inspector</span>
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

    <section v-else-if="store.companionTab === 'thinking'" class="companion-body">
      <div class="panel-title">
        <h2>Thinking stream</h2>
        <span>{{ thinkingEvents.length }} events</span>
      </div>
      <div class="activity-list">
        <article v-for="event in thinkingEvents" :key="event.id" class="activity-item" data-kind="think">
          <div>
            <strong>{{ event.title }}</strong>
            <p>{{ event.detail || 'No thinking detail reported.' }}</p>
          </div>
          <span>{{ event.status || 'seen' }}</span>
        </article>
        <div v-if="!thinkingEvents.length" class="empty-state">
          <strong>No thinking events</strong>
          <p>Thinking deltas will appear here when the runtime reports them.</p>
        </div>
      </div>
    </section>

    <section v-else-if="store.companionTab === 'workspace'" class="companion-body workspace-tab">
      <div class="panel-title">
        <h2>Workspace</h2>
        <span>{{ store.workspaceFiles.length }} items</span>
      </div>
      <div class="workspace-root" :title="store.workspaceRoot">{{ store.workspaceRoot || 'gateway workspace' }}</div>
      <div class="upload-drop" @dragover.prevent @drop="dropUpload">
        <Upload :size="16" />
        <span>{{ store.uploadBusy ? 'Uploading...' : 'Drop files here' }}</span>
        <button type="button" @click="fileInput?.click()">Choose files</button>
        <input ref="fileInput" type="file" multiple @change="uploadFiles(($event.target as HTMLInputElement).files)" />
      </div>
      <div class="workspace-create">
        <label>
          <FolderPlus :size="14" />
          <input v-model="newFolderName" type="text" placeholder="New folder" @keydown.enter.prevent="createFolder" />
        </label>
        <button class="icon-action" type="button" :disabled="!newFolderName.trim()" @click="createFolder"><FilePlus2 :size="14" /></button>
      </div>
      <div v-if="store.attachments.length" class="attachment-list">
        <div class="panel-title compact">
          <h2>Context sources</h2>
          <span>{{ store.attachments.length }}</span>
        </div>
        <article v-for="attachment in store.attachments" :key="attachment.ref_id" class="attachment-row">
          <Link2 :size="14" />
          <span>{{ attachment.label || attachment.path }}</span>
          <button class="icon-action" type="button" @click="store.removeAttachment(attachment.ref_id)"><X :size="13" /></button>
        </article>
      </div>
      <nav class="breadcrumbs" aria-label="Workspace breadcrumbs">
        <button v-for="crumb in breadcrumbs" :key="crumb.path || 'root'" type="button" @click="store.loadWorkspace(crumb.path)">
          {{ crumb.label }}
        </button>
      </nav>
      <button class="ghost-action" type="button" @click="store.loadWorkspace(parentDir)">
        <ChevronUp :size="15" />
        Parent folder
      </button>
      <label class="workspace-search">
        <Search :size="14" />
        <input v-model="store.workspaceFilter" type="search" placeholder="Filter files" />
      </label>
      <div class="file-list">
        <article
          v-for="file in store.filteredWorkspaceFiles"
          :key="file.path"
          class="file-row"
        >
          <button type="button" @click="openFile(file.path, file.kind)">
            <Folder v-if="file.kind === 'dir'" :size="16" />
            <FileText v-else :size="16" />
            <span>{{ file.name }}</span>
          </button>
          <small>{{ file.kind }}</small>
          <button class="icon-action" type="button" :aria-label="`Rename ${file.name}`" @click="startRename(file.path)"><FilePenLine :size="13" /></button>
          <button class="icon-action" type="button" :aria-label="`Delete ${file.name}`" @click="store.deleteWorkspacePath(file.path)"><Trash2 :size="13" /></button>
          <div v-if="renamePath === file.path" class="rename-row">
            <input v-model="renameTarget" type="text" @keydown.enter.prevent="commitRename" />
            <button class="ghost-action" type="button" @click="commitRename">Rename</button>
            <button class="ghost-action" type="button" @click="renamePath = ''">Cancel</button>
          </div>
        </article>
      </div>
      <div class="preview-pane" v-if="store.selectedFile">
        <div class="preview-head">
          <strong>{{ store.selectedFile }}</strong>
          <div>
            <button class="icon-action" type="button" @click="store.attachWorkspaceFile(store.selectedFile)"><Link2 :size="14" /></button>
            <button class="icon-action" type="button" :disabled="!store.editorDirty || !canEdit" @click="store.resetFile"><RotateCcw :size="14" /></button>
            <button class="icon-action" type="button" :disabled="!store.editorDirty || !canEdit" @click="store.saveFile"><Save :size="14" /></button>
          </div>
        </div>
        <div v-if="isImage" class="image-preview">
          <img :src="rawFileUrl" alt="" />
        </div>
        <div v-else-if="isMarkdown" class="render-preview">
          <MarkdownBlock :content="store.editorContent" />
        </div>
        <textarea v-else-if="isStructured" v-model="store.editorContent" class="structured-preview" spellcheck="false" />
        <textarea v-else v-model="store.editorContent" spellcheck="false" />
        <p v-if="store.fileError" class="file-error">{{ store.fileError }}</p>
        <p v-if="!canEdit" class="readonly-note"><Eye :size="14" /> Preview only</p>
        <span class="dirty-state" :class="{ dirty: store.editorDirty }">{{ store.editorDirty ? 'Unsaved changes' : 'Saved' }}</span>
      </div>
    </section>

    <section v-else class="companion-body">
      <div class="panel-title">
        <h2>Inspector</h2>
        <span>{{ inspectorEvents.length }} errors</span>
      </div>
      <dl class="detail-list">
        <dt>Workspace</dt>
        <dd>{{ store.workspaceRoot || 'not reported' }}</dd>
        <dt>Selected file</dt>
        <dd>{{ store.selectedFile || '-' }}</dd>
        <dt>File error</dt>
        <dd>{{ store.fileError || '-' }}</dd>
        <dt>Context</dt>
        <dd>{{ store.contextUsagePercent === null ? store.contextUsageSource : `${store.contextUsagePercent}%` }}</dd>
      </dl>
      <div class="activity-list">
        <article v-for="event in inspectorEvents" :key="event.id" class="activity-item" data-kind="error">
          <div>
            <strong>{{ event.title }}</strong>
            <p>{{ event.detail || 'No detail available.' }}</p>
          </div>
          <span>{{ event.status || 'error' }}</span>
        </article>
      </div>
    </section>
  </aside>
</template>
