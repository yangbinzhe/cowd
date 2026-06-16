<script setup lang="ts">
import { computed, nextTick, ref } from 'vue';
import { Paperclip, Send, Square, Zap } from 'lucide-vue-next';
import { useAppStore } from '../stores/app';
import MarkdownBlock from '../components/MarkdownBlock.vue';

const store = useAppStore();
const draft = ref('');
const sending = ref(false);
const contextUsage = computed(() => Math.min(88, 42 + store.turns.length * 6));

async function submit() {
  const text = draft.value.trim();
  if (!text || sending.value) return;
  sending.value = true;
  draft.value = '';
  await store.send(text);
  sending.value = false;
  await nextTick();
}
</script>

<template>
  <section class="chat-page">
    <header class="page-header chat-topbar">
      <div>
        <h1>Cowd Chat</h1>
        <p>正文优先展示，工具调用、思考、上下文和文件路径由右侧 Activity/Workspace 承接。</p>
      </div>
      <div class="status-strip">
        <span>{{ store.health?.status || 'local' }}</span>
        <strong>{{ store.activeSession?.model || store.settings?.model || 'default model' }}</strong>
      </div>
    </header>

    <div class="transcript" aria-label="Chat transcript">
      <article v-for="turn in store.turns" :key="turn.id" class="turn" :data-role="turn.role">
        <div class="turn-role">{{ turn.role }}</div>
        <MarkdownBlock :content="turn.content" />
      </article>
    </div>

    <footer class="composer">
      <textarea v-model="draft" placeholder="Ask Cowd, reference files, or type / for commands" @keydown.enter.exact.prevent="submit" />
      <div class="composer-bar">
        <div class="composer-context">
          <span>Workspace: {{ store.workspaceDir || 'root' }}</span>
          <span>Profile: {{ store.settings?.profile || 'default' }}</span>
          <span>Context {{ contextUsage }}%</span>
          <div class="context-meter"><i :style="{ width: `${contextUsage}%` }" /></div>
        </div>
        <div class="composer-actions">
          <button class="icon-action" type="button" @click="store.openCompanion('workspace')"><Paperclip :size="16" /></button>
          <button class="ghost-action" type="button"><Zap :size="15" /> Commands</button>
          <button v-if="sending" class="primary-action" type="button"><Square :size="15" /> Stop</button>
          <button v-else class="primary-action" type="button" :disabled="!draft.trim()" @click="submit"><Send :size="15" /> Send</button>
        </div>
      </div>
    </footer>
  </section>
</template>
