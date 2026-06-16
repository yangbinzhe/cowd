import { defineStore } from 'pinia';
import { computed, ref } from 'vue';
import { api, normalizeActivity } from '../api/client';
import type { ActivityEvent, ChatTurn, CompanionTab, SessionSummary, WorkspaceFile } from '../types';

function blockText(block: any): string {
  if (!block) return '';
  if (typeof block === 'string') return block;
  return block.text || block.content || block.output || block.thinking || '';
}

export const useAppStore = defineStore('app', () => {
  const booted = ref(false);
  const health = ref<any>(null);
  const settings = ref<any>(null);
  const sessions = ref<SessionSummary[]>([]);
  const activeSessionId = ref('demo-main');
  const turns = ref<ChatTurn[]>([]);
  const activity = ref<ActivityEvent[]>([]);
  const companionTab = ref<CompanionTab>('activity');
  const workspaceRoot = ref('');
  const workspaceDir = ref('');
  const workspaceFiles = ref<WorkspaceFile[]>([]);
  const selectedFile = ref('');
  const selectedFileContent = ref('');
  const editorContent = ref('');
  const workspaceFilter = ref('');
  const fileError = ref('');
  const settingsSavedAt = ref('');
  const activeSectionByPage = ref<Record<string, string>>({});
  const activeModal = ref<'model' | 'workspace' | 'commands' | null>(null);
  const selectedModel = ref('claude-sonnet-4-6');
  const selectedProfile = ref('default');
  const sessionQuery = ref('');
  const editorDirty = computed(() => selectedFileContent.value !== editorContent.value);
  const filteredWorkspaceFiles = computed(() => {
    const query = workspaceFilter.value.trim().toLowerCase();
    if (!query) return workspaceFiles.value;
    return workspaceFiles.value.filter((file) => `${file.name} ${file.path}`.toLowerCase().includes(query));
  });
  const busy = ref(false);

  const activeSession = computed(() => sessions.value.find((item) => item.id === activeSessionId.value) || sessions.value[0]);
  const filteredSessions = computed(() => {
    const query = sessionQuery.value.trim().toLowerCase();
    if (!query) return sessions.value;
    return sessions.value.filter((session) => `${session.title} ${session.model} ${session.status}`.toLowerCase().includes(query));
  });

  async function boot() {
    if (booted.value) return;
    busy.value = true;
    const [manifest, sessionData, config, workspace] = await Promise.all([
      api.health(),
      api.sessions(),
      api.settings(),
      api.workspace(),
    ]);
    health.value = manifest;
    settings.value = config;
    selectedModel.value = config.model || selectedModel.value;
    selectedProfile.value = config.profile || selectedProfile.value;
    sessions.value = sessionData.sessions;
    if (!activeSessionId.value && sessions.value[0]) activeSessionId.value = sessions.value[0].id;
    workspaceRoot.value = workspace.workspace_canonical || workspace.workspace_root || '';
    await Promise.all([loadMessages(activeSessionId.value), loadWorkspace(''), loadActivity()]);
    busy.value = false;
    booted.value = true;
  }

  async function loadMessages(sessionId: string) {
    activeSessionId.value = sessionId;
    const data = await api.messages(sessionId);
    const rows = Array.isArray(data) ? data : (data.messages || []);
    turns.value = rows.map((row: any, index: number) => ({
      id: String(row.id || row.sequence || index),
      role: row.role || 'assistant',
      content: row.content || (row.blocks || []).map(blockText).join('') || '',
      status: 'complete',
      activity: [],
    }));
    if (!turns.value.length) {
      turns.value = [{ id: 'empty', role: 'system', content: '当前 session 暂无消息。', status: 'complete' }];
    }
  }

  async function createSession() {
    const id = `local-${Date.now()}`;
    sessions.value = [
      { id, title: 'New conversation', model: selectedModel.value, status: 'active' },
      ...sessions.value,
    ];
    activeSessionId.value = id;
    turns.value = [{ id: `system-${Date.now()}`, role: 'system', content: '新会话已创建。', status: 'complete' }];
  }

  async function send(content: string) {
    const sessionId = activeSessionId.value || 'demo-main';
    turns.value.push({ id: `local-${Date.now()}`, role: 'user', content, status: 'complete' });
    companionTab.value = 'activity';
    activity.value.unshift({ id: `send-${Date.now()}`, kind: 'runtime', title: 'Message queued', detail: content.slice(0, 140), status: 'pending' });
    await api.sendMessage(sessionId, content);
    const assistantTurn: ChatTurn = {
      id: `assistant-${Date.now()}`,
      role: 'assistant',
      content: '消息已提交。连接运行中的 gateway 后，这里会显示真实流式响应、工具调用和思考状态。',
      status: 'streaming',
    };
    turns.value.push(assistantTurn);
    window.setTimeout(() => {
      assistantTurn.status = 'complete';
      activity.value.unshift({ id: `done-${Date.now()}`, kind: 'think', title: 'Assistant turn settled', detail: '正文、工具状态和上下文摘要已归并到当前 turn。', status: 'complete' });
    }, 250);
  }

  async function loadActivity() {
    const data: any = await api.runtimeTimeline();
    activity.value = normalizeActivity(data.events || data.timeline || []);
  }

  async function loadWorkspace(dir = workspaceDir.value) {
    const data = await api.files(dir);
    workspaceDir.value = data.dir || dir || '';
    workspaceFiles.value = (data.files || []).map((file: any) => ({
      ...file,
      kind: file.kind || (file.is_dir ? 'dir' : 'file'),
    }));
  }

  async function openFile(path: string) {
    selectedFile.value = path;
    fileError.value = '';
    selectedFileContent.value = await api.rawFile(path);
    editorContent.value = selectedFileContent.value;
    companionTab.value = 'workspace';
  }

  async function saveFile() {
    if (!selectedFile.value) return;
    try {
      await api.saveFile(selectedFile.value, editorContent.value);
      selectedFileContent.value = editorContent.value;
      fileError.value = '';
    } catch (error) {
      fileError.value = error instanceof Error ? error.message : String(error);
    }
  }

  function resetFile() {
    editorContent.value = selectedFileContent.value;
  }

  function openCompanion(tab: CompanionTab) {
    companionTab.value = tab;
  }

  function selectSection(page: string, sectionId: string) {
    activeSectionByPage.value = { ...activeSectionByPage.value, [page]: sectionId };
  }

  function openModal(modal: 'model' | 'workspace' | 'commands') {
    activeModal.value = modal;
  }

  function closeModal() {
    activeModal.value = null;
  }

  function chooseModel(model: string) {
    selectedModel.value = model;
    settings.value = { ...settings.value, model };
    closeModal();
  }

  function chooseProfile(profile: string) {
    selectedProfile.value = profile;
    settings.value = { ...settings.value, profile };
    closeModal();
  }

  async function saveSettings(nextSettings: Record<string, unknown>) {
    settings.value = { ...settings.value, ...nextSettings };
    await api.saveSettings(settings.value);
    settingsSavedAt.value = new Date().toLocaleTimeString();
  }

  return {
    booted,
    health,
    settings,
    sessions,
    activeSessionId,
    turns,
    activity,
    companionTab,
    workspaceRoot,
    workspaceDir,
    workspaceFiles,
    workspaceFilter,
    filteredWorkspaceFiles,
    selectedFile,
    selectedFileContent,
    editorContent,
    fileError,
    settingsSavedAt,
    activeSectionByPage,
    activeModal,
    selectedModel,
    selectedProfile,
    sessionQuery,
    filteredSessions,
    editorDirty,
    busy,
    activeSession,
    boot,
    loadMessages,
    createSession,
    send,
    loadActivity,
    loadWorkspace,
    openFile,
    saveFile,
    resetFile,
    openCompanion,
    selectSection,
    openModal,
    closeModal,
    chooseModel,
    chooseProfile,
    saveSettings,
  };
});
