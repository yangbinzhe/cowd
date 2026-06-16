import { defineStore } from 'pinia';
import { computed, ref } from 'vue';
import { api, normalizeActivity, providerModels, type EndpointSnapshot } from '../api/client';
import type { ActivityEvent, ChatTurn, CompanionTab, NavId, SessionAttachment, SessionSummary, WorkspaceFile } from '../types';

function blockText(block: any): string {
  if (!block) return '';
  if (typeof block === 'string') return block;
  return block.text || block.content || block.output || block.thinking || '';
}

export const useAppStore = defineStore('app', () => {
  let sessionStream: EventSource | null = null;
  let sessionStreamId = '';
  let streamingAssistantId = '';
  const booted = ref(false);
  const health = ref<any>(null);
  const settings = ref<any>(null);
  const controlPlane = ref<any>(null);
  const providers = ref<any>(null);
  const profiles = ref<any[]>([]);
  const commands = ref<any[]>([]);
  const commandHistory = ref<any[]>([]);
  const approvalConfig = ref<any>(null);
  const sessions = ref<SessionSummary[]>([]);
  const activeSessionId = ref('');
  const turns = ref<ChatTurn[]>([]);
  const activity = ref<ActivityEvent[]>([]);
  const companionTab = ref<CompanionTab>('activity');
  const workspaceRoot = ref('');
  const workspaceDir = ref('');
  const workspaceFiles = ref<WorkspaceFile[]>([]);
  const attachments = ref<SessionAttachment[]>([]);
  const selectedFile = ref('');
  const selectedFileContent = ref('');
  const editorContent = ref('');
  const workspaceFilter = ref('');
  const fileError = ref('');
  const uploadBusy = ref(false);
  const settingsSavedAt = ref('');
  const activeSectionByPage = ref<Record<string, string>>({});
  const activeModal = ref<'model' | 'workspace' | 'commands' | null>(null);
  const selectedModel = ref('');
  const selectedProfile = ref('default');
  const commandError = ref('');
  const contextUsagePercent = ref<number | null>(null);
  const contextUsageSource = ref('not reported');
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
    const models = providerModels(controlPlane.value, providers.value || settings.value);
    return models.length ? models : (selectedModel.value ? [selectedModel.value] : []);
  });
  const availableProfiles = computed(() => profiles.value.map((profile: any) => profile.id || profile.name).filter(Boolean));

  async function boot() {
    if (booted.value) return;
    busy.value = true;
    const [manifest, sessionData, config, runtime, providerData, profileData, commandData, workspace, approvals] = await Promise.all([
      api.health(),
      api.sessions(),
      api.settings(),
      api.runtimeControlPlane(),
      api.providers(),
      api.profiles(),
      api.commands(),
      api.workspace(),
      api.approvalConfig(),
    ]);
    health.value = manifest;
    settings.value = config;
    controlPlane.value = runtime;
    providers.value = providerData;
    profiles.value = profileData.profiles || [];
    commands.value = commandData.commands || [];
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
      activeSessionId.value ? loadAttachments(activeSessionId.value) : Promise.resolve(),
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
    connectSessionStream(sessionId);
    await refreshContextUsage(sessionId);
    await loadAttachments(sessionId);
  }

  async function refreshContextUsage(sessionId = activeSessionId.value) {
    if (!sessionId) {
      contextUsagePercent.value = null;
      contextUsageSource.value = 'no active session';
      return;
    }
    const stats: any = await api.sessionStats(sessionId);
    const used = Number(stats.input_tokens || stats.output_tokens || stats.total_tokens || stats.token_usage?.total || 0);
    const limit = Number(stats.context_window || stats.max_context_tokens || stats.token_budget || stats.token_usage?.limit || 0);
    if (used > 0 && limit > 0) {
      contextUsagePercent.value = Math.max(0, Math.min(100, Math.round((used / limit) * 100)));
      contextUsageSource.value = 'session stats';
    } else {
      contextUsagePercent.value = null;
      contextUsageSource.value = 'not reported';
    }
  }

  async function refreshSessions(query = sessionQuery.value) {
    const data = await api.searchSessions(query.trim());
    sessions.value = data.sessions || [];
    if (!activeSessionId.value && sessions.value[0]) activeSessionId.value = sessions.value[0].id;
    return data;
  }

  async function createSession() {
    const session = await api.createSession(selectedModel.value || undefined);
    sessions.value = [session, ...sessions.value.filter((item) => item.id !== session.id)];
    activeSessionId.value = session.id;
    selectedModel.value = session.model || selectedModel.value;
    turns.value = [{ id: `system-${Date.now()}`, role: 'system', content: '新会话已创建。', status: 'complete' }];
    attachments.value = [];
    connectSessionStream(session.id);
  }

  async function deleteSession(sessionId: string) {
    await api.deleteSession(sessionId);
    sessions.value = sessions.value.filter((session) => session.id !== sessionId);
    if (activeSessionId.value === sessionId) {
      activeSessionId.value = sessions.value[0]?.id || '';
      if (activeSessionId.value) await loadMessages(activeSessionId.value);
      else turns.value = [];
    }
  }

  async function compactSession(sessionId: string) {
    const result = await api.compactSession(sessionId);
    activity.value.unshift({ id: `compact-${Date.now()}`, kind: 'runtime', title: 'Session compacted', detail: JSON.stringify(result).slice(0, 220), status: 'complete' });
    await loadActivity();
  }

  async function send(content: string) {
    const sessionId = activeSessionId.value;
    if (!sessionId) {
      await createSession();
    }
    turns.value.push({ id: `local-${Date.now()}`, role: 'user', content, status: 'complete' });
    ensureStreamingAssistantTurn();
    companionTab.value = 'activity';
    activity.value.unshift({ id: `send-${Date.now()}`, kind: 'runtime', title: 'Message queued', detail: content.slice(0, 140), status: 'pending' });
    try {
      const contentWithAttachments = renderMessageWithAttachments(content);
      await api.sendMessage(activeSessionId.value, contentWithAttachments);
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

  function connectSessionStream(sessionId: string) {
    if (!sessionId || (sessionStream && sessionStreamId === sessionId)) return;
    if (sessionStream) sessionStream.close();
    sessionStreamId = sessionId;
    streamingAssistantId = '';
    try {
      sessionStream = new EventSource(`/api/sessions/${encodeURIComponent(sessionId)}/stream`);
      sessionStream.onmessage = (event) => handleSessionEvent(event.data);
      sessionStream.onerror = () => {
        if (sessionStreamId === sessionId) {
          sessionStream?.close();
          sessionStream = null;
          sessionStreamId = '';
          activity.value.unshift({
            id: `stream-error-${Date.now()}`,
            kind: 'error',
            title: 'Live stream disconnected',
            detail: 'SSE connection closed. The page will still refresh completed messages through the API.',
            status: 'error',
          });
        }
      };
    } catch (error) {
      activity.value.unshift({
        id: `stream-open-error-${Date.now()}`,
        kind: 'error',
        title: 'Live stream unavailable',
        detail: error instanceof Error ? error.message : String(error),
        status: 'error',
      });
    }
  }

  function handleSessionEvent(raw: string) {
    let event: any;
    try {
      event = JSON.parse(raw);
    } catch {
      event = { type: 'RuntimeEvent', content: raw };
    }
    const type = event.type || 'RuntimeEvent';
    if (type === 'Connected') return;

    if (type === 'TurnStarted') {
      ensureStreamingAssistantTurn();
      recordLiveActivity('runtime', 'Turn started', '', 'running');
      return;
    }

    if (type === 'TextDelta') {
      appendAssistantDelta(event.content || event.text || '');
      return;
    }

    if (type === 'ThinkingDelta') {
      companionTab.value = 'thinking';
      recordLiveActivity('think', 'Thinking', event.content || event.thinking || '', 'running');
      return;
    }

    if (type === 'ToolStart' || type === 'ToolProgress' || type === 'ToolComplete') {
      companionTab.value = 'activity';
      recordLiveActivity(
        'tool',
        event.name || type,
        event.summary || event.progress || event.preview || event.id || '',
        type === 'ToolComplete' ? 'complete' : 'running',
      );
      return;
    }

    if (type === 'ContextEnvelope') {
      recordLiveActivity('context', 'Context envelope', event.envelope_id || event.run_id || '', 'complete');
      return;
    }

    if (type === 'TurnComplete') {
      completeAssistantTurn(event.response || event.text || '');
      recordLiveActivity('runtime', 'Turn complete', event.iterations ? `${event.iterations} iterations` : '', 'complete');
      return;
    }

    if (type === 'TurnError') {
      companionTab.value = 'inspector';
      completeAssistantTurn(event.error || 'Turn failed', 'error');
      recordLiveActivity('error', 'Turn failed', event.error || '', 'error');
      return;
    }

    if (String(type).toLowerCase().includes('error')) companionTab.value = 'inspector';
    recordLiveActivity('runtime', type, event.summary || event.content || JSON.stringify(event).slice(0, 220), event.status || 'observed');
  }

  function ensureStreamingAssistantTurn() {
    const current = streamingAssistantId ? turns.value.find((turn) => turn.id === streamingAssistantId) : null;
    if (current && current.status === 'streaming') return current;
    streamingAssistantId = `assistant-stream-${Date.now()}`;
    const turn = { id: streamingAssistantId, role: 'assistant' as const, content: '', status: 'streaming' as const, activity: [] };
    turns.value.push(turn);
    return turn;
  }

  function appendAssistantDelta(delta: string) {
    if (!delta) return;
    const turn = ensureStreamingAssistantTurn();
    turn.content += delta;
  }

  function completeAssistantTurn(content: string, status: 'complete' | 'error' = 'complete') {
    const turn = streamingAssistantId ? turns.value.find((item) => item.id === streamingAssistantId) : null;
    if (turn) {
      if (content && turn.content !== content && !turn.content.endsWith(content)) turn.content = content;
      turn.status = status;
    } else if (content) {
      const alreadyShown = turns.value.some((item) => item.role === 'assistant' && item.content === content);
      if (!alreadyShown) turns.value.push({ id: `assistant-${Date.now()}`, role: status === 'error' ? 'system' : 'assistant', content, status });
    }
    streamingAssistantId = '';
  }

  function recordLiveActivity(kind: ActivityEvent['kind'], title: string, detail: string, status: string) {
    const id = `live-${title}-${status}`;
    const event = {
      id: `${id}-${Date.now()}`,
      kind,
      title,
      detail: String(detail || '').slice(0, 240),
      status,
    };
    const existingIndex = activity.value.findIndex((item) => item.title === title && item.kind === kind && item.status !== 'complete' && item.status !== 'error');
    if (existingIndex >= 0) {
      activity.value.splice(existingIndex, 1, event);
    } else {
      activity.value.unshift(event);
    }
    activity.value = activity.value.slice(0, 80);
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

  async function loadAttachments(sessionId = activeSessionId.value) {
    if (!sessionId) {
      attachments.value = [];
      return;
    }
    const data: any = await api.sessionAttachments(sessionId);
    attachments.value = data.attachments || [];
  }

  async function attachWorkspaceFile(path = selectedFile.value) {
    if (!path) return;
    if (!activeSessionId.value) await createSession();
    try {
      const result: any = await api.addSessionAttachment(activeSessionId.value, path, path);
      attachments.value = [result.attachment, ...attachments.value.filter((item) => item.path !== path)];
      activity.value.unshift({ id: `attachment-${Date.now()}`, kind: 'context', title: 'Attachment added', detail: path, status: 'complete' });
    } catch (error) {
      fileError.value = error instanceof Error ? error.message : String(error);
    }
  }

  async function removeAttachment(refId: string) {
    if (!activeSessionId.value) return;
    await api.deleteSessionAttachment(activeSessionId.value, refId);
    attachments.value = attachments.value.filter((item) => item.ref_id !== refId);
  }

  async function uploadWorkspaceFile(file: File, dir = workspaceDir.value) {
    uploadBusy.value = true;
    fileError.value = '';
    try {
      const result: any = await api.uploadFile(file, dir);
      await loadWorkspace(dir);
      activity.value.unshift({ id: `upload-${Date.now()}`, kind: 'context', title: 'File uploaded', detail: `${result.path} (${result.size} bytes)`, status: 'complete' });
      return result;
    } catch (error) {
      fileError.value = error instanceof Error ? error.message : String(error);
      companionTab.value = 'inspector';
      throw error;
    } finally {
      uploadBusy.value = false;
    }
  }

  async function createWorkspaceDir(name: string) {
    const path = [workspaceDir.value, name.trim()].filter(Boolean).join('/');
    try {
      await api.createDir(path);
      await loadWorkspace(workspaceDir.value);
      fileError.value = '';
    } catch (error) {
      fileError.value = error instanceof Error ? error.message : String(error);
      companionTab.value = 'inspector';
      throw error;
    }
  }

  async function deleteWorkspacePath(path: string) {
    try {
      await api.deleteWorkspacePath(path);
      if (selectedFile.value === path) {
        selectedFile.value = '';
        selectedFileContent.value = '';
        editorContent.value = '';
      }
      await loadWorkspace(workspaceDir.value);
      fileError.value = '';
    } catch (error) {
      fileError.value = error instanceof Error ? error.message : String(error);
      companionTab.value = 'inspector';
      throw error;
    }
  }

  async function renameWorkspacePath(path: string, to: string) {
    try {
      const result: any = await api.renameWorkspacePath(path, to);
      if (selectedFile.value === path) selectedFile.value = result.to || to;
      await loadWorkspace(workspaceDir.value);
      fileError.value = '';
      return result;
    } catch (error) {
      fileError.value = error instanceof Error ? error.message : String(error);
      companionTab.value = 'inspector';
      throw error;
    }
  }

  function renderMessageWithAttachments(content: string) {
    if (!attachments.value.length) return content;
    const refs = attachments.value
      .map((item) => `- workspace://file/${item.path} (${item.label || item.path}, ${item.sha256})`)
      .join('\n');
    return `${content}\n\nContext attachments:\n${refs}`;
  }

  async function saveFile() {
    if (!selectedFile.value) return;
    try {
      await api.saveFile(selectedFile.value, editorContent.value);
      selectedFileContent.value = editorContent.value;
      fileError.value = '';
    } catch (error) {
      fileError.value = error instanceof Error ? error.message : String(error);
      companionTab.value = 'inspector';
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
    const [runtime, providerData] = await Promise.all([api.runtimeControlPlane(), api.providers()]);
    controlPlane.value = runtime;
    providers.value = providerData;
    activity.value.unshift({ id: `providers-${Date.now()}`, kind: 'runtime', title: 'Providers reloaded', detail: JSON.stringify(result).slice(0, 240), status: 'complete' });
    return result;
  }

  async function saveDefaultModel(model: string) {
    settings.value = await api.saveConfig({ model });
    providers.value = await api.providers();
    selectedModel.value = model;
    settingsSavedAt.value = new Date().toLocaleTimeString();
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

  async function verifyAuth() {
    return api.authVerify();
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

  async function refreshCommands() {
    const [registry, history] = await Promise.all([api.commands(), api.commandHistory()]);
    commands.value = registry.commands || [];
    commandHistory.value = history.history || [];
  }

  async function executeCommand(command: string, args: Record<string, unknown> = {}) {
    const result: any = await api.executeCommand(command, args);
    commandHistory.value = [result, ...commandHistory.value];
    activity.value.unshift({
      id: `command-${Date.now()}`,
      kind: 'runtime',
      title: `Command ${result.command || command}`,
      detail: JSON.stringify(result).slice(0, 220),
      status: result.ok ? 'complete' : 'error',
    });
    return result;
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
    providers,
    profiles,
    commands,
    commandHistory,
    approvalConfig,
    sessions,
    activeSessionId,
    turns,
    activity,
    companionTab,
    workspaceRoot,
    workspaceDir,
    workspaceFiles,
    attachments,
    workspaceFilter,
    filteredWorkspaceFiles,
    selectedFile,
    selectedFileContent,
    editorContent,
    fileError,
    uploadBusy,
    settingsSavedAt,
    activeSectionByPage,
    activeModal,
    selectedModel,
    selectedProfile,
    contextUsagePercent,
    contextUsageSource,
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
    refreshSessions,
    loadMessages,
    refreshContextUsage,
    createSession,
    deleteSession,
    compactSession,
    send,
    loadActivity,
    loadWorkspace,
    openFile,
    loadAttachments,
    attachWorkspaceFile,
    removeAttachment,
    uploadWorkspaceFile,
    createWorkspaceDir,
    deleteWorkspacePath,
    renameWorkspacePath,
    saveFile,
    resetFile,
    openCompanion,
    selectSection,
    openModal,
    closeModal,
    chooseModel,
    chooseProfile,
    reloadProviders,
    saveDefaultModel,
    refreshProfiles,
    createProfile,
    deleteProfile,
    saveApprovalConfig,
    toggleSolo,
    verifyAuth,
    loadCapability,
    refreshCommands,
    executeCommand,
    runCapabilityAction,
  };
});
