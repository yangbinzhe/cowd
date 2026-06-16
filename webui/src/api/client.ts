import type { ActivityEvent, NavId, SessionSummary, WorkspaceFile } from '../types';

const authToken = () => localStorage.getItem('cowd-auth-token') || '';

export interface ApiOffline {
  __offline?: boolean;
  __error?: string;
}

export interface EndpointSnapshot extends ApiOffline {
  id: string;
  label: string;
  path: string;
  method: string;
  status: 'ready' | 'empty' | 'offline' | 'error';
  count: number;
  data: any;
}

function headers(init: RequestInit = {}) {
  const headers = new Headers(init.headers);
  if (!headers.has('Content-Type') && init.body) headers.set('Content-Type', 'application/json');
  const token = authToken();
  if (token) headers.set('Authorization', `Bearer ${token}`);
  return headers;
}

async function parseResponse(response: Response) {
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function read<T>(path: string, fallback: T, init: RequestInit = {}): Promise<T & ApiOffline> {
  try {
    const response = await fetch(path, { ...init, headers: headers(init) });
    if (!response.ok) throw new Error(await response.text());
    return await parseResponse(response) as T;
  } catch (error) {
    return {
      ...(fallback as any),
      __offline: true,
      __error: error instanceof Error ? error.message : String(error),
    };
  }
}

async function write<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, { ...init, headers: headers(init) });
  if (!response.ok) {
    const body = await response.text();
    throw new Error(body || `${response.status} ${response.statusText}`);
  }
  return await parseResponse(response) as T;
}

function countPayload(data: any): number {
  if (Array.isArray(data)) return data.length;
  if (!data || typeof data !== 'object') return data ? 1 : 0;
  for (const key of ['sessions', 'messages', 'events', 'timeline', 'tools', 'skills', 'runs', 'tasks', 'entries', 'files', 'profiles', 'accounts', 'resources', 'facts', 'incidents', 'playbooks', 'cases', 'executions']) {
    if (Array.isArray(data[key])) return data[key].length;
  }
  if (typeof data.count === 'number') return data.count;
  if (typeof data.total === 'number') return data.total;
  return Object.keys(data).filter((key) => !key.startsWith('__')).length;
}

function endpointStatus(data: any): EndpointSnapshot['status'] {
  if (data?.__offline) return 'offline';
  if (data?.error) return 'error';
  return countPayload(data) > 0 ? 'ready' : 'empty';
}

async function endpoint(label: string, path: string, init: RequestInit = {}): Promise<EndpointSnapshot> {
  const method = init.method || 'GET';
  const data = method === 'GET' ? await read(path, {}) : await write(path, init).catch((error) => ({
    __offline: true,
    __error: error instanceof Error ? error.message : String(error),
  }));
  return {
    id: `${method}:${path}`,
    label,
    path,
    method,
    status: endpointStatus(data),
    count: countPayload(data),
    data,
    __offline: data?.__offline,
    __error: data?.__error,
  };
}

const pageEndpoints = (page: Exclude<NavId, 'chat' | 'settings'>, sessionId: string) => {
  const sid = encodeURIComponent(sessionId || '');
  const routes: Record<Exclude<NavId, 'chat' | 'settings'>, Array<[string, string]>> = {
    runtime: [
      ['Control plane', '/api/runtime/control-plane'],
      ['Effective config', '/api/runtime/config/effective'],
      ['Session leases', '/api/runtime/session-leases'],
      ['Timeline', `/api/runtime/timeline?session_id=${sid}&limit=80`],
      ['Approvals pending', '/api/approval/pending'],
      ['Tasks', '/api/tasks'],
    ],
    context: [
      ['Current context', '/api/context/current'],
      ['Context history', `/api/sessions/${sid}/context/history`],
      ['Session runs', `/api/sessions/${sid}/runs`],
      ['Session stats', `/api/sessions/${sid}/stats`],
    ],
    memory: [
      ['Status', '/api/memory/status'],
      ['Stats', '/api/memory/stats'],
      ['Layers', '/api/memory/layers'],
      ['Runtime', '/api/memory/runtime'],
      ['Maintenance', '/api/memory/maintenance'],
      ['Clusters', '/api/memory/clusters'],
    ],
    skills: [
      ['Catalog', '/api/skills/catalog'],
      ['Projection', '/api/skills/projection'],
      ['Runs', '/api/skills/runs'],
    ],
    agents: [
      ['Agent runs', '/api/agents/runs'],
      ['Tasks', '/api/tasks'],
      ['Task graph', '/api/tasks/current/agent-graph'],
    ],
    tools: [
      ['Registry', '/api/tools'],
      ['Commands history', '/api/commands/history'],
      ['Cowd capabilities', '/api/cowd/capabilities'],
      ['Cross-plane summary', '/api/cross-plane/summary'],
    ],
    gateway: [
      ['Connectors summary', '/api/connectors/summary'],
      ['Connector accounts', '/api/connectors/accounts'],
      ['Connector capabilities', '/api/connectors/capabilities'],
      ['MCP servers', '/api/connectors/mcp/servers'],
      ['Platforms', '/api/platforms'],
      ['WeChat accounts', '/api/channels/wechat-ilink/accounts'],
    ],
    iacc: [
      ['App descriptor', '/api/iacc/app'],
      ['Health', '/api/iacc/health'],
      ['Metrics', '/api/iacc/metrics'],
      ['Entities', '/api/iacc/entities'],
      ['Changes', '/api/iacc/changes'],
      ['Incidents', '/api/iacc/incidents'],
      ['Skills', '/api/iacc/skills'],
      ['Command center', '/api/iacc/command-center'],
    ],
    audit: [
      ['Audit export', '/api/audit/export?limit=50'],
      ['Approval history', '/api/approval/history?limit=50'],
      ['Cross-plane audit', '/api/cross-plane/audit'],
      ['Action executions', '/api/cross-plane/action/executions'],
    ],
  };
  return routes[page];
};

export const api = {
  health: () => read('/api/webui/manifest', {
    kind: 'cowd.webui.manifest',
    status: 'offline',
    static_webui: 'local vite fallback',
  }),
  sessions: () => read<{ sessions: SessionSummary[] }>('/api/sessions?limit=24', { sessions: [] }),
  createSession: (model?: string) => write<SessionSummary>('/api/sessions', {
    method: 'POST',
    body: JSON.stringify({ model }),
  }),
  updateSession: (sessionId: string, patch: Record<string, unknown>) => write(`/api/sessions/${encodeURIComponent(sessionId)}`, {
    method: 'PATCH',
    body: JSON.stringify(patch),
  }),
  messages: (sessionId: string) => read<{ messages: any[] }>(`/api/sessions/${encodeURIComponent(sessionId)}/messages?limit=50`, { messages: [] }),
  sendMessage: (sessionId: string, content: string) => write(`/api/sessions/${encodeURIComponent(sessionId)}/messages`, {
    method: 'POST',
    body: JSON.stringify({ content }),
  }),
  workspace: () => read('/api/workspace', {
    workspace_root: '',
    workspace_canonical: '',
    profile_id: '',
  }),
  files: (dir = '') => read<{ dir: string; files: WorkspaceFile[] }>(`/api/workspace/files${dir ? `?dir=${encodeURIComponent(dir)}` : ''}`, {
    dir,
    files: [],
  }),
  rawFile: (path: string) => readText(`/api/file/raw?path=${encodeURIComponent(path)}`),
  saveFile: (path: string, content: string) => write('/api/workspace/files', {
    method: 'POST',
    body: JSON.stringify({ path, content }),
  }),
  runtimeTimeline: (sessionId: string) => read(`/api/runtime/timeline?session_id=${encodeURIComponent(sessionId)}&limit=50`, { events: [] }),
  runtimeControlPlane: () => read('/api/runtime/control-plane', {}),
  effectiveConfig: () => read('/api/runtime/config/effective', {}),
  reloadProviders: () => write('/api/runtime/providers/reload', { method: 'POST' }),
  approvalConfig: () => read('/api/approval/config', {}),
  updateApprovalConfig: (config: Record<string, unknown>) => write('/api/approval/config', {
    method: 'PUT',
    body: JSON.stringify(config),
  }),
  toggleSolo: () => write('/api/approval/solo', { method: 'POST' }),
  approvalPending: () => read('/api/approval/pending', []),
  approvalHistory: () => read('/api/approval/history?limit=20', []),
  settings: () => read('/api/config', { model: 'unknown', version: 'unknown' }),
  profiles: () => read('/api/profiles', { profiles: [], active_profile: '', runtime_profile: '' }),
  createProfile: (name: string) => write('/api/profiles', {
    method: 'POST',
    body: JSON.stringify({ name }),
  }),
  switchProfile: (profile: string) => write('/api/profiles/switch', {
    method: 'POST',
    body: JSON.stringify({ profile }),
  }),
  deleteProfile: (id: string) => write(`/api/profiles/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  loadCapabilityPage: async (page: Exclude<NavId, 'chat' | 'settings'>, sessionId: string) => Promise.all(
    pageEndpoints(page, sessionId).map(([label, path]) => endpoint(label, path)),
  ),
  executeCapabilityAction: (path: string, body: Record<string, unknown> = {}) => write(path, {
    method: 'POST',
    body: JSON.stringify({ source: 'webui', ...body }),
  }),
};

async function readText(path: string, fallback = ''): Promise<string> {
  try {
    const response = await fetch(path, { headers: headers() });
    if (!response.ok) throw new Error(await response.text());
    return await response.text();
  } catch (error) {
    throw new Error(error instanceof Error ? error.message : String(error));
  }
}

export function providerModels(controlPlane: any, config: any): string[] {
  const models = new Set<string>();
  const configured = controlPlane?.configured_model || config?.model;
  const normalized = typeof configured === 'string' ? configured.trim() : '';
  if (normalized && normalized !== 'unknown') models.add(normalized);
  const providerNames = controlPlane?.provider_names || [];
  const count = Number(controlPlane?.provider_model_count || 0);
  if (count > 0 && models.size === 0) {
    providerNames.forEach((name: string) => models.add(`${name}:default`));
  }
  return Array.from(models);
}

export function normalizeActivity(raw: any[]): ActivityEvent[] {
  if (!Array.isArray(raw) || raw.length === 0) return [];
  return raw.slice(0, 50).map((event, index) => ({
    id: String(event.id || event.sequence || index),
    kind: event.kind || event.type || 'runtime',
    title: event.title || event.type || event.kind || 'Runtime event',
    detail: event.detail || event.message || event.summary || JSON.stringify(event.payload || event).slice(0, 240),
    status: event.status || event.phase || 'observed',
  }));
}
