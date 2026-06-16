import type { ActivityEvent, SessionSummary, WorkspaceFile } from '../types';

const authToken = () => localStorage.getItem('cowd-auth-token') || '';

async function request<T>(path: string, init: RequestInit = {}, fallback: T): Promise<T> {
  try {
    const headers = new Headers(init.headers);
    if (!headers.has('Content-Type') && init.body) headers.set('Content-Type', 'application/json');
    const token = authToken();
    if (token) headers.set('Authorization', `Bearer ${token}`);
    const response = await fetch(path, { ...init, headers });
    if (!response.ok) throw new Error(await response.text());
    return await response.json() as T;
  } catch {
    return fallback;
  }
}

async function requestText(path: string, fallback = ''): Promise<string> {
  try {
    const headers = new Headers();
    const token = authToken();
    if (token) headers.set('Authorization', `Bearer ${token}`);
    const response = await fetch(path, { headers });
    if (!response.ok) throw new Error(await response.text());
    return await response.text();
  } catch {
    return fallback;
  }
}

export const api = {
  health: () => request('/api/webui/manifest', {}, {
    kind: 'cowd.webui.manifest',
    status: 'offline',
    static_webui: 'local vite fallback',
  }),
  sessions: () => request<{ sessions: SessionSummary[] }>('/api/sessions?limit=24', {}, {
    sessions: [
      { id: 'demo-main', title: 'WebUI refactor review', model: 'claude-sonnet-4-6', status: 'active' },
      { id: 'demo-runtime', title: 'Runtime alignment check', model: 'qwen3-coder-next', status: 'idle' },
    ],
  }),
  messages: (sessionId: string) => request<{ messages: any[] }>(`/api/sessions/${encodeURIComponent(sessionId)}/messages?limit=50`, {}, {
    messages: [
      { id: 'u1', role: 'user', content: '请检查当前结构化数据和 WebUI 能力是否完整对齐。' },
      { id: 'a1', role: 'assistant', content: '已完成初步检查。下面是当前风险、证据和下一步执行计划。\n\n- Runtime 已接入统一控制面。\n- Workspace 已进入右侧 companion tab。\n- 需要补齐图表化验收。', blocks: [] },
    ],
  }),
  sendMessage: (sessionId: string, content: string) => request(`/api/sessions/${encodeURIComponent(sessionId)}/messages`, {
    method: 'POST',
    body: JSON.stringify({ role: 'user', content }),
  }, { ok: true }),
  workspace: () => request('/api/workspace', {}, {
    workspace_root: '/media/yi/Datas/workspace/dev-iacc',
    workspace_canonical: '/media/yi/Datas/workspace/dev-iacc',
    profile_id: 'default',
  }),
  files: (dir = '') => request<{ dir: string; files: WorkspaceFile[] }>(`/api/workspace/files${dir ? `?dir=${encodeURIComponent(dir)}` : ''}`, {}, {
    dir,
    files: [
      { name: 'README.md', path: 'README.md', kind: 'file', size: 12400 },
      { name: 'webui', path: 'webui', kind: 'dir' },
      { name: 'crates', path: 'crates', kind: 'dir' },
      { name: 'Cargo.toml', path: 'Cargo.toml', kind: 'file', size: 4200 },
    ],
  }),
  rawFile: (path: string) => requestText(`/api/file/raw?path=${encodeURIComponent(path)}`, `# ${path}\n\n当前文件可预览。连接 gateway 后会展示真实内容。`),
  saveFile: (path: string, content: string) => request('/api/workspace/files', {
    method: 'POST',
    body: JSON.stringify({ path, content }),
  }, { path, saved: true }),
  runtimeTimeline: () => request('/api/runtime/timeline?limit=50', {}, { events: [], value_loop: { status: 'ready' } }),
  memoryStatus: () => request('/api/memory/status', {}, { status: 'ready', enabled: true }),
  skills: () => request('/api/skills/catalog', {}, { skills: [] }),
  agents: () => request('/api/agents/runs', {}, { runs: [] }),
  tools: () => request('/api/tools', {}, { tools: [] }),
  gateway: () => request('/api/connectors/summary', {}, { status: 'ready', accounts: [] }),
  iacc: () => request('/api/iacc/health', {}, { status: 'ready', readiness: { ready: true } }),
  audit: () => request('/api/audit/export?limit=20', {}, { entries: [] }),
  settings: () => request('/api/config', {}, { model: 'claude-sonnet-4-6', profile: 'default', version: '0.9.212' }),
};

export function normalizeActivity(raw: any[]): ActivityEvent[] {
  if (!Array.isArray(raw) || raw.length === 0) {
    return [
      { id: 'act-tool', kind: 'tool', title: 'Workspace scan', detail: '右侧 Workspace 可直接定位和预览文件', status: 'complete' },
      { id: 'act-context', kind: 'context', title: 'Context budget', detail: '当前上下文压力处于可控区间', status: 'ready' },
    ];
  }
  return raw.slice(0, 20).map((event, index) => ({
    id: String(event.id || event.sequence || index),
    kind: event.kind || event.type || 'runtime',
    title: event.title || event.type || event.kind || 'Runtime event',
    detail: event.detail || event.message || JSON.stringify(event.payload || event).slice(0, 240),
    status: event.status || 'observed',
  }));
}
