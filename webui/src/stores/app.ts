import { defineStore } from 'pinia';
import { computed, ref } from 'vue';
import { api, normalizeActivity, providerModels, type EndpointSnapshot } from '../api/client';
import type { ActivityEvent, ChatTurn, CompanionTab, NavId, SessionSummary, WorkspaceFile } from '../types';

function blockText(block: any): string {
  if (!block) return '';
  if (typeof block === 'string') return block;
  return block.text || block.content || block.output || block.thinking || '';
}

export const useAppStore = defineStore('app', () => {
  const booted = ref(false);
  const health = ref<any>(null);
  const settings = ref<any>(null);
  const controlPlane = ref<any>(null);
  const profiles = ref<any[]>([]);
  const approvalConfig = ref<any>(null);
  const sessions = ref<SessionSummary[]>([]);
  const activeSessionId = ref('');
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
  const selectedModel = ref('');
  const selectedProfile = ref('default');
  const commandError = ref('');
  const sessionQuery = ref('');
  const actionResults = ref<Record<string, any>>({});
  const capabilitySnapshots = ref<Record<string, EndpointSnapshot[]>>({});
  const capabilityLoading = ref<Record<string, boolean>>({});
  const capabilityError = ref<Record<string, string>>({});
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
  const availableModels = computed(() => {
    const models = providerModels(controlPlane.value, settings.value);
    return models.length ? models : (selectedModel.value ? [selectedModel.value] : []);
  });
  const availableProfiles = computed(() => profiles.value.map((profile: any) => profile.id || profile.name).filter(Boolean));

  async function boot() {
    if (booted.value) return;
    busy.value = true;
    const [manifest, sessionData, config, runtime, profileData, workspace, approvals] = await Promise.all([
      api.health(),
      api.sessions(),
      api.settings(),
      api.runtimeControlPlane(),
      api.profiles(),
      api.workspace(),
      api.approvalConfig(),
    ]);
    health.value = manifest;
    settings.value = config;
    controlPlane.value = runtime;
    profiles.value = profileData.profiles || [];
    approvalConfig.value = approvals;
    const reportedModel = runtime.configured_model || config.model || '';
    selectedModel.value = reportedModel && reportedModel !== 'unknown' ? reportedModel : selectedModel.value;
    selectedProfile.value = profileData.active_profile || profileData.runtime_profile || selectedProfile.value;
    sessions.value = sessionData.sessions;
    if (!activeSessionId.value && sessions.value[0]) activeSessionId.value = sessions.value[0].id;
    workspaceRoot.value = workspace.workspace_canonical || workspace.workspace_root || '';
    await Promise.all([
      activeSessionId.value ? loadMessages(activeSessionId.value) : Promise.resolve(),
      loadWorkspace(''),
      loadActivity(),
    ]);
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
    if (!turns.value.length) turns.value = [{ id: 'empty', role: 'system', content: '当前 session 暂无消息。', status: 'complete' }];
  }

  async function createSession() {
    const session = await api.createSession(selectedModel.value || undefined);
    sessions.value = [session, ...sessions.value.filter((item) => item.id !== session.id)];
    activeSessionId.value = session.id;
    selectedModel.value = session.model || selectedModel.value;
    turns.value = [{ id: `system-${Date.now()}`, role: 'system', content: '新会话已创建。', status: 'complete' }];
  }

  async function send(content: string) {
    const sessionId = activeSessionId.value;
    if (!sessionId) {
      await createSession();
    }
    turns.value.push({ id: `local-${Date.now()}`, role: 'user', content, status: 'complete' });
    companionTab.value = 'activity';
    activity.value.unshift({ id: `send-${Date.now()}`, kind: 'runtime', title: 'Message queued', detail: content.slice(0, 140), status: 'pending' });
    try {
      await api.sendMessage(activeSessionId.value, content);
      await loadMessages(activeSessionId.value);
      await loadActivity();
    } catch (error) {
      turns.value.push({
        id: `error-${Date.now()}`,
        role: 'system',
        content: `发送失败：${error instanceof Error ? error.message : String(error)}`,
        status: 'error',
      });
      activity.value.unshift({ id: `send-error-${Date.now()}`, kind: 'error', title: 'Message failed', detail: error instanceof Error ? error.message : String(error), status: 'error' });
    }
  }

  async function loadActivity() {
    if (!activeSessionId.value) {
      activity.value = [];
      return;
    }
    const data: any = await api.runtimeTimeline(activeSessionId.value);
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

  async function chooseModel(model: string) {
    commandError.value = '';
    if (!activeSessionId.value) await createSession();
    try {
      await api.updateSession(activeSessionId.value, { model });
      selectedModel.value = model;
      sessions.value = sessions.value.map((session) => session.id === activeSessionId.value ? { ...session, model } : session);
      closeModal();
    } catch (error) {
      commandError.value = `模型切换失败：${error instanceof Error ? error.message : String(error)}`;
    }
  }

  async function chooseProfile(profile: string) {
    commandError.value = '';
    try {
      const result: any = await api.switchProfile(profile);
      selectedProfile.value = result.active_profile || profile;
      const data: any = await api.profiles();
      profiles.value = data.profiles || [];
      closeModal();
    } catch (error) {
      commandError.value = `Profile 切换失败：${error instanceof Error ? error.message : String(error)}`;
    }
  }

  async function reloadProviders() {
    const result = await api.reloadProviders();
    controlPlane.value = await api.runtimeControlPlane();
    activity.value.unshift({ id: `providers-${Date.now()}`, kind: 'runtime', title: 'Providers reloaded', detail: JSON.stringify(result).slice(0, 240), status: 'complete' });
    return result;
  }

  async function refreshProfiles() {
    const data: any = await api.profiles();
    profiles.value = data.profiles || [];
    selectedProfile.value = data.active_profile || data.runtime_profile || selectedProfile.value;
    return data;
  }

  async function createProfile(name: string) {
    await api.createProfile(name);
    await refreshProfiles();
  }

  async function deleteProfile(id: string) {
    await api.deleteProfile(id);
    await refreshProfiles();
  }

  async function saveApprovalConfig(nextConfig: Record<string, unknown>) {
    approvalConfig.value = await api.updateApprovalConfig(nextConfig);
    settingsSavedAt.value = new Date().toLocaleTimeString();
  }

  async function toggleSolo() {
    approvalConfig.value = await api.toggleSolo();
  }

  async function loadCapability(page: Exclude<NavId, 'chat' | 'settings'>) {
    capabilityLoading.value = { ...capabilityLoading.value, [page]: true };
    capabilityError.value = { ...capabilityError.value, [page]: '' };
    try {
      const snapshots = await api.loadCapabilityPage(page, activeSessionId.value);
      capabilitySnapshots.value = { ...capabilitySnapshots.value, [page]: snapshots };
    } catch (error) {
      capabilityError.value = { ...capabilityError.value, [page]: error instanceof Error ? error.message : String(error) };
    } finally {
      capabilityLoading.value = { ...capabilityLoading.value, [page]: false };
    }
  }

  async function runCapabilityAction(page: string, label: string, endpoint?: string) {
    if (!endpoint) return;
    const id = `${page}:${label}`;
    companionTab.value = 'activity';
    activity.value.unshift({
      id: `${id}:${Date.now()}`,
      kind: 'tool',
      title: label,
      detail: endpoint,
      status: 'running',
    });
    try {
      const result = await api.executeCapabilityAction(endpoint, { label, session_id: activeSessionId.value });
      actionResults.value = { ...actionResults.value, [id]: result };
      activity.value.unshift({
        id: `${id}:done:${Date.now()}`,
        kind: 'tool',
        title: `${label} completed`,
        detail: JSON.stringify(result).slice(0, 220),
        status: 'complete',
      });
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      actionResults.value = { ...actionResults.value, [id]: { error: detail } };
      activity.value.unshift({ id: `${id}:error:${Date.now()}`, kind: 'error', title: `${label} failed`, detail, status: 'error' });
    }
  }

  return {
    booted,
    health,
    settings,
    controlPlane,
    profiles,
    approvalConfig,
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
    availableModels,
    availableProfiles,
    commandError,
    sessionQuery,
    filteredSessions,
    actionResults,
    capabilitySnapshots,
    capabilityLoading,
    capabilityError,
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
    reloadProviders,
    refreshProfiles,
    createProfile,
    deleteProfile,
    saveApprovalConfig,
    toggleSolo,
    loadCapability,
    runCapabilityAction,
  };
});
