import { describe, it, expect, beforeEach, vi } from 'vitest';
import fs from 'node:fs';
import './api.js';
import './ui.js';
import './panels.js';
import './sessions.js';
import './messages.js';
import './workspace.js';
import './commands.js';
import './boot.js';

describe('WebUI single implementation boundary', () => {
  it('index loads only the canonical root modules', () => {
    const html = fs.readFileSync('index.html', 'utf8');
    const scripts = [...html.matchAll(/<script\s+src="([^"]+)"/g)].map(match => match[1]);
    const localScripts = scripts.filter(src => !src.startsWith('http://') && !src.startsWith('https://'));

    expect(localScripts).toEqual([
      'api.js',
      'state.js',
      'ui.js',
      'sessions.js',
      'messages.js',
      'workspace.js',
      'panels.js',
      'commands.js',
      'boot.js',
    ]);
    expect(html).not.toContain('assets/js/');
  });

  it('service worker caches only canonical root modules and static assets', () => {
    const sw = fs.readFileSync('sw.js', 'utf8');

    expect(sw).toContain("const CACHE = 'cowd-v4'");
    expect(sw).not.toContain('assets/js/');
    expect(sw).not.toContain('assets/style.css');
  });
});

describe('API module', () => {
  beforeEach(() => { vi.restoreAllMocks(); });

  it('has all session endpoints', () => {
    expect(typeof window.Api.listSessions).toBe('function');
    expect(typeof window.Api.createSession).toBe('function');
    expect(typeof window.Api.deleteSession).toBe('function');
    expect(typeof window.Api.sendMessage).toBe('function');
    expect(typeof window.Api.getStreamUrl).toBe('function');
    expect(typeof window.Api.compactSession).toBe('function');
    expect(typeof window.Api.getMessages).toBe('function');
    expect(typeof window.Api.getEvents).toBe('function');
  });

  it('has all memory endpoints', () => {
    expect(typeof window.Api.memoryStatus).toBe('function');
    expect(typeof window.Api.listMemoryLayers).toBe('function');
    expect(typeof window.Api.searchMemory).toBe('function');
    expect(typeof window.Api.recallExplain).toBe('function');
    expect(typeof window.Api.memoryPacket).toBe('function');
    expect(typeof window.Api.memoryLinks).toBe('function');
    expect(typeof window.Api.currentContext).toBe('function');
    expect(typeof window.Api.createMemoryEntry).toBe('function');
    expect(typeof window.Api.updateMemoryEntry).toBe('function');
    expect(typeof window.Api.deleteMemoryEntry).toBe('function');
    expect(typeof window.Api.linkSymbolToMemory).toBe('function');
    expect(typeof window.Api.findMemoriesBySymbol).toBe('function');
    expect(typeof window.Api.listEntities).toBe('function');
    expect(typeof window.Api.detectEntities).toBe('function');
    expect(typeof window.Api.listTriples).toBe('function');
    expect(typeof window.Api.addTriple).toBe('function');
    expect(typeof window.Api.checkFacts).toBe('function');
    expect(typeof window.Api.registerFacts).toBe('function');
  });

  it('memoryStatus uses the stable status endpoint', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ enabled: true, status: 'ready' })
      })
    );
    vi.stubGlobal('fetch', mockF);

    const status = await window.Api.memoryStatus();

    expect(String(mockF.mock.calls[0][0])).toBe('/api/memory/status');
    expect(status.status).toBe('ready');
  });

  it('recallExplain uses the stable explain endpoint', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ enabled: true, mode: 'keyword', results: [] })
      })
    );
    vi.stubGlobal('fetch', mockF);

    const explain = await window.Api.recallExplain('SessionKernel', 7);

    expect(String(mockF.mock.calls[0][0])).toBe('/api/memory/recall/explain?q=SessionKernel&limit=7');
    expect(explain.mode).toBe('keyword');
  });

  it('memory packet and link endpoints use kernel routes', async () => {
    const mockF = vi.fn((url) => {
      const path = String(url);
      if (path.includes('/api/memory/packet')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ packet: { selected: [] } }) });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ links: [{ kind: 'Supports' }] }) });
    });
    vi.stubGlobal('fetch', mockF);

    await window.Api.memoryPacket('SessionKernel', { max_items: 9, max_tokens: 1234 });
    const links = await window.Api.memoryLinks();

    expect(String(mockF.mock.calls[0][0])).toBe('/api/memory/packet?q=SessionKernel&max_items=9&max_tokens=1234');
    expect(String(mockF.mock.calls[1][0])).toBe('/api/memory/links');
    expect(links.links[0].kind).toBe('Supports');
  });

  it('currentContext uses the context envelope endpoint', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ enabled: true, envelope: { id: 'ctx-1' } })
      })
    );
    vi.stubGlobal('fetch', mockF);

    const context = await window.Api.currentContext({ q: 'ship', session_id: 's1', profile: 'Review' });

    expect(String(mockF.mock.calls[0][0])).toBe('/api/context/current?q=ship&session_id=s1&profile=Review');
    expect(context.envelope.id).toBe('ctx-1');
  });

  it('context history uses persisted envelope endpoints', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ envelopes: [{ envelope_id: 'ctx-1' }] })
      })
    );
    vi.stubGlobal('fetch', mockF);

    const history = await window.Api.contextHistory('s1', { from_seq: 10, limit: 8 });
    await window.Api.contextEnvelope('ctx-1');

    expect(String(mockF.mock.calls[0][0])).toBe('/api/sessions/s1/context?from_seq=10&limit=8');
    expect(String(mockF.mock.calls[1][0])).toBe('/api/context/ctx-1');
    expect(history.envelopes[0].envelope_id).toBe('ctx-1');
  });

  it('runtimeRuns reads session run timeline', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ runs: [{ run: { run_id: 'run-1' } }] })
      })
    );
    vi.stubGlobal('fetch', mockF);

    const runs = await window.Api.runtimeRuns('s1', { limit: 12 });

    expect(String(mockF.mock.calls[0][0])).toBe('/api/sessions/s1/runs?limit=12');
    expect(runs.runs[0].run.run_id).toBe('run-1');
  });

  it('records context recommendation actions', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ ok: true })
      })
    );
    vi.stubGlobal('fetch', mockF);

    await window.Api.recordContextRecommendation('s1', {
      envelope_id: 'ctx-1',
      recommendation: 'Start a handoff',
      action: 'acknowledged'
    });

    expect(String(mockF.mock.calls[0][0])).toBe('/api/sessions/s1/context/recommendations');
    expect(mockF.mock.calls[0][1].method).toBe('POST');
    expect(mockF.mock.calls[0][1].body).toContain('Start a handoff');
  });

  it('fetches context recommendation stats', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ recommendations: [] })
      })
    );
    vi.stubGlobal('fetch', mockF);

    await window.Api.contextRecommendationStats('s1', { limit: 20 });

    expect(String(mockF.mock.calls[0][0])).toBe('/api/sessions/s1/context/recommendations?limit=20');
    expect(mockF.mock.calls[0][1].method).toBe('GET');
  });

  it('has all skill endpoints', () => {
    expect(typeof window.Api.listSkills).toBe('function');
    expect(typeof window.Api.installSkill).toBe('function');
    expect(typeof window.Api.viewSkill).toBe('function');
    expect(typeof window.Api.uninstallSkill).toBe('function');
    expect(typeof window.Api.invokeSkill).toBe('function');
    expect(typeof window.Api.toggleSkill).toBe('function');
  });

  it('has durable task endpoints', () => {
    expect(typeof window.Api.taskStatus).toBe('function');
    expect(typeof window.Api.startTask).toBe('function');
    expect(typeof window.Api.startTaskPhase).toBe('function');
    expect(typeof window.Api.recordTaskPhaseArtifact).toBe('function');
    expect(typeof window.Api.reviewTaskPhase).toBe('function');
    expect(typeof window.Api.cancelTask).toBe('function');
    expect(typeof window.Api.completeTask).toBe('function');
    expect(typeof window.Api.recordTaskFailure).toBe('function');
  });

  it('task endpoints use stable route contracts', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ id: 'task-1', status: 'running' })
      })
    );
    vi.stubGlobal('fetch', mockF);

    await window.Api.taskStatus();
    await window.Api.startTask('ship', true);
    await window.Api.startTaskPhase('task-1', { name: 'phase', objective: 'do it' });
    await window.Api.recordTaskPhaseArtifact('task-1', 'phase-1', { kind: 'test', label: 'unit', value: 'passed' });
    await window.Api.reviewTaskPhase('task-1', 'phase-1', 'accepted', true);
    await window.Api.cancelTask('task-1');
    await window.Api.completeTask('task-1');
    await window.Api.recordTaskFailure('task-1', 'blocked');

    expect(String(mockF.mock.calls[0][0])).toBe('/api/tasks');
    expect(String(mockF.mock.calls[1][0])).toBe('/api/tasks/start');
    expect(JSON.parse(mockF.mock.calls[1][1].body)).toEqual({ objective: 'ship', yolo_mode: true });
    expect(String(mockF.mock.calls[2][0])).toBe('/api/tasks/task-1/phases');
    expect(String(mockF.mock.calls[3][0])).toBe('/api/tasks/task-1/phases/phase-1/artifacts');
    expect(String(mockF.mock.calls[4][0])).toBe('/api/tasks/task-1/phases/phase-1/review');
    expect(String(mockF.mock.calls[5][0])).toBe('/api/tasks/task-1/cancel');
    expect(String(mockF.mock.calls[6][0])).toBe('/api/tasks/task-1/complete');
    expect(String(mockF.mock.calls[7][0])).toBe('/api/tasks/task-1/failure');
  });

  it('has all cron endpoints', () => {
    expect(typeof window.Api.listCrons).toBe('function');
    expect(typeof window.Api.createCron).toBe('function');
    expect(typeof window.Api.deleteCron).toBe('function');
    expect(typeof window.Api.runCron).toBe('function');
    expect(typeof window.Api.pauseCron).toBe('function');
    expect(typeof window.Api.resumeCron).toBe('function');
  });

  it('has all workspace endpoints', () => {
    expect(typeof window.Api.getWorkspace).toBe('function');
    expect(typeof window.Api.listWorkspaces).toBe('function');
    expect(typeof window.Api.listFiles).toBe('function');
    expect(typeof window.Api.createFile).toBe('function');
    expect(typeof window.Api.getRawFile).toBe('function');
  });

  it('has all approval endpoints', () => {
    expect(typeof window.Api.pendingApprovals).toBe('function');
    expect(typeof window.Api.respondApproval).toBe('function');
    expect(typeof window.Api.getApprovalConfig).toBe('function');
    expect(typeof window.Api.updateApprovalConfig).toBe('function');
    expect(typeof window.Api.toggleSolo).toBe('function');
    expect(typeof window.Api.approvalHistory).toBe('function');
    expect(typeof window.Api.exportAudit).toBe('function');
  });

  it('approval endpoints use the gateway route contract', async () => {
    const mockF = vi.fn((url, opts) => {
      const path = String(url);
      if (path.includes('/api/approval/respond')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ resolved: true }) });
      }
      if (path.includes('/api/approval/history')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve([{ id: 'a-1', decision: 'approved' }]) });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve([]) });
    });
    vi.stubGlobal('fetch', mockF);

    await window.Api.pendingApprovals();
    await window.Api.respondApproval('a-1', true, 'session');
    const history = await window.Api.approvalHistory();

    expect(String(mockF.mock.calls[0][0])).toBe('/api/approval/pending');
    expect(String(mockF.mock.calls[1][0])).toBe('/api/approval/respond');
    expect(JSON.parse(mockF.mock.calls[1][1].body)).toEqual({ id: 'a-1', approved: true, persistence: 'session' });
    expect(history).toEqual([{ id: 'a-1', decision: 'approved' }]);
  });

  it('audit export endpoint sends enterprise audit query params', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ kind: 'audit_export', total: 1, records: [{ source: 'memory' }] })
      })
    );
    vi.stubGlobal('fetch', mockF);

    const data = await window.Api.exportAudit({ source: 'memory', limit: 25, offset: 50 });

    expect(String(mockF.mock.calls[0][0])).toBe('/api/audit/export?source=memory&limit=25&offset=50');
    expect(data.kind).toBe('audit_export');
    expect(data.records[0].source).toBe('memory');
  });

  it('profile endpoints use the enterprise profile route contract', async () => {
    const mockF = vi.fn((url, opts) => {
      const path = String(url);
      if (path === '/api/profiles' && opts.method === 'POST') {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ profile: { id: 'enterprise_ops' } }) });
      }
      if (path === '/api/profiles/switch') {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ active_profile: 'enterprise_ops', restart_required: true }) });
      }
      if (path === '/api/profiles/enterprise_ops') {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ deleted: 'enterprise_ops' }) });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ profiles: [{ id: 'default', is_active: true }] }) });
    });
    vi.stubGlobal('fetch', mockF);

    await window.Api.listProfiles();
    await window.Api.createProfile('Enterprise Ops');
    await window.Api.switchProfile('enterprise_ops');
    await window.Api.deleteProfile('enterprise_ops');

    expect(String(mockF.mock.calls[0][0])).toBe('/api/profiles');
    expect(String(mockF.mock.calls[1][0])).toBe('/api/profiles');
    expect(JSON.parse(mockF.mock.calls[1][1].body)).toEqual({ name: 'Enterprise Ops' });
    expect(String(mockF.mock.calls[2][0])).toBe('/api/profiles/switch');
    expect(JSON.parse(mockF.mock.calls[2][1].body)).toEqual({ profile: 'enterprise_ops' });
    expect(String(mockF.mock.calls[3][0])).toBe('/api/profiles/enterprise_ops');
  });

  it('has config and usage endpoints', () => {
    expect(typeof window.Api.getConfig).toBe('function');
    expect(typeof window.Api.updateConfig).toBe('function');
    expect(typeof window.Api.getProviders).toBe('function');
    expect(typeof window.Api.listProfiles).toBe('function');
    expect(typeof window.Api.createProfile).toBe('function');
    expect(typeof window.Api.switchProfile).toBe('function');
    expect(typeof window.Api.deleteProfile).toBe('function');
    expect(typeof window.Api.getUsage).toBe('function');
  });

  it('has auth endpoints', () => {
    expect(typeof window.Api.login).toBe('function');
    expect(typeof window.Api.verifyAuth).toBe('function');
    expect(typeof window.Api.logout).toBe('function');
  });

  it('requests throw on non-ok', async () => {
    vi.stubGlobal('fetch', vi.fn(() =>
      Promise.resolve({ ok: false, status: 500, text: () => Promise.resolve('Server error') })
    ));
    await expect(window.Api.listSessions()).rejects.toThrow('Server error');
  });

  it('listSessions transforms response', async () => {
    vi.stubGlobal('fetch', vi.fn(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve([{ id: 'abc', title: 'S1', created_at: 1 }]) })
    ));
    const { sessions } = await window.Api.listSessions();
    expect(sessions[0].id).toBe('abc');
    expect(sessions[0].started_at).toBe(1);
  });

  it('listSessions sends paging and filter query params', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve({ sessions: [], total: 0, offset: 20, limit: 20 }) })
    );
    vi.stubGlobal('fetch', mockF);

    const data = await window.Api.listSessions({
      q: 'Auth',
      model: 'claude-sonnet-4-6',
      status: 'active',
      sort: 'updated_at',
      order: 'desc',
      limit: 20,
      offset: 20
    });

    const url = String(mockF.mock.calls[0][0]);
    expect(url).toContain('/api/sessions?');
    expect(url).toContain('q=Auth');
    expect(url).toContain('model=claude-sonnet-4-6');
    expect(url).toContain('status=active');
    expect(url).toContain('offset=20');
    expect(data.total).toBe(0);
  });

  it('createSession uses default model', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve({ id: 'new-s' }) })
    );
    vi.stubGlobal('fetch', mockF);
    await window.Api.createSession();
    const body = JSON.parse(mockF.mock.calls[0][1].body);
    expect(body.model).toBe('claude-sonnet-4-6');
  });

  it('getEvents normalizes event responses and query params', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          events: [{
            event_type: 'message_appended',
            payload: { type: 'message_appended', sequence: 3, role: 'user' }
          }]
        })
      })
    );
    vi.stubGlobal('fetch', mockF);

    const events = await window.Api.getEvents('session-a', { from_seq: 3, limit: 20 });

    expect(String(mockF.mock.calls[0][0])).toContain('/api/sessions/session-a/events?from_seq=3&limit=20');
    expect(events[0].type).toBe('message_appended');
    expect(events[0].sequence).toBe(3);
    expect(events[0].payload.role).toBe('user');
  });

  it('getMessages sends sequence paging params and normalizes content', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          messages: [{
            sequence: 99950,
            role: 'assistant',
            blocks: [{ type: 'text', text: 'message 99950' }]
          }]
        })
      })
    );
    vi.stubGlobal('fetch', mockF);

    const messages = await window.Api.getMessages('message-100k', { from_seq: 99950, limit: 50 });

    expect(String(mockF.mock.calls[0][0])).toContain('/api/sessions/message-100k/messages?from_seq=99950&limit=50');
    expect(messages[0].sequence).toBe(99950);
    expect(messages[0].content).toBe('message 99950');
  });

  it('platform API endpoints are defined', () => {
    expect(typeof window.Api.listPlatforms).toBe('function');
    expect(typeof window.Api.getPlatform).toBe('function');
  });

  it('memory CRUD endpoints are defined', () => {
    expect(typeof window.Api.createMemoryEntry).toBe('function');
    expect(typeof window.Api.updateMemoryEntry).toBe('function');
    expect(typeof window.Api.deleteMemoryEntry).toBe('function');
    expect(typeof window.Api.listEntities).toBe('function');
    expect(typeof window.Api.listTriples).toBe('function');
  });

  it('updates memory entries through the backend PATCH route', async () => {
    const mockF = vi.fn(() =>
      Promise.resolve({ ok: true, json: () => Promise.resolve({ id: 'm1', updated: true }) })
    );
    vi.stubGlobal('fetch', mockF);

    const result = await window.Api.updateMemoryEntry('m1', {
      content: 'updated content',
      tags: ['after'],
      priority: 'High'
    });

    expect(String(mockF.mock.calls[0][0])).toBe('/api/memory/entry/m1');
    expect(mockF.mock.calls[0][1].method).toBe('PATCH');
    expect(JSON.parse(mockF.mock.calls[0][1].body)).toEqual({
      content: 'updated content',
      tags: ['after'],
      priority: 'High'
    });
    expect(result.updated).toBe(true);
  });

  it('normalizes memory entity and triple responses', async () => {
    vi.stubGlobal('fetch', vi.fn((url) => {
      if (String(url).includes('/api/memory/entities')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ entities: [{ name: 'SessionKernel' }] }) });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ triples: [{ subject: 'SessionKernel', predicate: 'owns', object: 'sessions' }] }) });
    }));

    const entities = await window.Api.listEntities();
    const triples = await window.Api.listTriples();

    expect(entities).toEqual([{ name: 'SessionKernel' }]);
    expect(triples).toEqual([{ subject: 'SessionKernel', predicate: 'owns', object: 'sessions' }]);
  });

  it('symbol-memory link endpoints are normalized', async () => {
    const mockF = vi.fn((url, opts) => {
      const path = String(url);
      if (path.includes('/api/memory/symbol-links?symbol=')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ entries: [{ title: 'Auth impact note' }] }) });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ symbol_id: 'authenticate_user', memory_id: 'm1' }) });
    });
    vi.stubGlobal('fetch', mockF);

    await window.Api.linkSymbolToMemory('authenticate_user', 'm1', { turn_index: 2, reference_type: 'impact' });
    const body = JSON.parse(mockF.mock.calls[0][1].body);
    expect(body.symbol_id).toBe('authenticate_user');
    expect(body.memory_id).toBe('m1');
    expect(body.turn_index).toBe(2);
    expect(body.reference_type).toBe('impact');

    const entries = await window.Api.findMemoriesBySymbol('authenticate_user');
    expect(String(mockF.mock.calls[1][0])).toContain('/api/memory/symbol-links?symbol=authenticate_user');
    expect(entries).toEqual([{ title: 'Auth impact note' }]);
  });

  it('gateway and skills endpoints are defined', () => {
    expect(typeof window.Api.getUsage).toBe('function');
    expect(typeof window.Api.toggleSolo).toBe('function');
    expect(typeof window.Api.compactSession).toBe('function');
  });

  it('Panels module exposes all panel renderers', () => {
    expect(typeof window.Panels.renderMemory).toBe('function');
    expect(typeof window.Panels.renderContext).toBe('function');
    expect(typeof window.Panels.renderMemoryNetwork).toBe('function');
    expect(typeof window.Panels.renderSkills).toBe('function');
    expect(typeof window.Panels.renderCrons).toBe('function');
    expect(typeof window.Panels.renderMemorySymbolResults).toBe('function');
    expect(typeof window.Panels.renderAgents).toBe('function');
    expect(typeof window.Panels.renderGateway).toBe('function');
    expect(typeof window.Panels.renderTools).toBe('function');
    expect(typeof window.Panels.renderAudit).toBe('function');
    expect(typeof window.Panels.renderSettings).toBe('function');
    expect(typeof window.Panels.renderCCConfig).toBe('function');
    expect(typeof window.Panels.renderCCProviders).toBe('function');
    expect(typeof window.Panels.renderCCApproval).toBe('function');
    expect(typeof window.Panels.renderCCHistory).toBe('function');
    expect(typeof window.Panels.renderCCUsage).toBe('function');
  });

  it('index exposes the context panel tab and slash command', () => {
    const html = fs.readFileSync('index.html', 'utf8');
    expect(html).toContain('data-panel="context"');
    expect(window.Commands.getMatches('/context')[0].cmd).toBe('/context');
  });

  it('Sessions search uses backend query and renders returned sessions', async () => {
    document.body.innerHTML = '<div id="toast"></div><ul id="session-list"></ul>';
    const mockF = vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          sessions: [{ id: 'auth-a', title: 'Auth Audit A', model: 'claude-sonnet-4-6', status: 'active' }],
          total: 1,
          offset: 0,
          limit: 20
        })
      })
    );
    vi.stubGlobal('fetch', mockF);

    await window.Sessions.searchSessions('Auth');

    expect(String(mockF.mock.calls[0][0])).toContain('/api/sessions?q=Auth');
    expect(document.getElementById('session-list').textContent).toContain('Auth Audit A');
    expect(window.Sessions.total()).toBe(1);
  });

  it('Sessions ignores stale list responses after a newer search', async () => {
    document.body.innerHTML = '<div id="toast"></div><ul id="session-list"></ul>';
    const pending = [];
    vi.stubGlobal('fetch', vi.fn((url) => new Promise(resolve => pending.push({ url, resolve }))));

    const firstLoad = window.Sessions.load();
    const searchLoad = window.Sessions.searchSessions('Auth');
    expect(pending.length).toBe(2);

    pending[1].resolve({
      ok: true,
      json: () => Promise.resolve({
        sessions: [{ id: 'auth-a', title: 'Auth Audit A', model: 'claude-sonnet-4-6', status: 'active' }],
        total: 1,
        offset: 0,
        limit: 20
      })
    });
    await searchLoad;
    expect(document.getElementById('session-list').textContent).toContain('Auth Audit A');

    pending[0].resolve({
      ok: true,
      json: () => Promise.resolve({
        sessions: [{ id: 'old-a', title: 'Old Full List', model: 'claude-sonnet-4-6', status: 'active' }],
        total: 1,
        offset: 0,
        limit: 20
      })
    });
    await firstLoad;
    expect(document.getElementById('session-list').textContent).toContain('Auth Audit A');
    expect(document.getElementById('session-list').textContent).not.toContain('Old Full List');
  });

  it('Messages renders SSE tool lifecycle with progress and error status', () => {
    document.body.innerHTML = '<div id="toast"></div><div id="connection-status"></div><div id="chat-messages"></div>';

    window.Messages._dispatch({ type: 'ToolStart', id: 'tool-1', name: 'bash', preview: 'starting bash' });
    expect(document.querySelector('#tool-tool-1 .tool-status').textContent).toBe('running');
    expect(document.querySelector('#tool-tool-1').textContent).toContain('starting bash');

    window.Messages._dispatch({ type: 'ToolProgress', id: 'tool-1', progress: 'running command' });
    expect(document.querySelector('#tool-tool-1').textContent).toContain('running command');

    window.Messages._dispatch({ type: 'ToolComplete', id: 'tool-1', summary: 'command failed', exit_code: 1 });
    expect(document.querySelector('#tool-tool-1 .tool-status').textContent).toBe('error');
    expect(document.querySelector('#tool-tool-1').classList.contains('error')).toBe(true);
  });

  it('Messages normalizes Anthropic tool_use blocks and connected events', () => {
    document.body.innerHTML = '<div id="toast"></div><div id="connection-status"></div><div id="chat-messages"></div>';

    window.Messages._dispatch({ type: 'Connected' });
    expect(document.getElementById('connection-status').textContent).toBe('Connected');

    window.Messages._dispatch({
      type: 'content_block_start',
      content_block: { type: 'tool_use', id: 'anth-tool', name: 'read', input: { path: 'README.md' } }
    });
    expect(document.querySelector('#tool-anth-tool .tool-name').textContent).toContain('read');
  });

  it('renders memory layer DTO labels', async () => {
    document.body.innerHTML = '<div id="toast"></div><div id="panel-content"></div>';
    vi.stubGlobal('fetch', vi.fn((url) => {
      const path = String(url);
      if (path.includes('/api/memory/stats')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ total_entries: 2, entity_count: 0, triple_count: 0 }) });
      }
      if (path.includes('/api/memory/entities')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ entities: [] }) });
      }
      if (path.includes('/api/memory/triples')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ triples: [] }) });
      }
      if (path.includes('/api/memory/layers')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ layers: [{ layer: 'L4', entry_count: 2 }] }) });
      }
      if (path.includes('/api/memory/links')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ links: [] }) });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
    }));

    await window.Panels.renderMemory();

    const text = document.getElementById('panel-content').textContent;
    expect(text).toContain('L4');
    expect(text).not.toContain('[object Object]');
  });

  it('renders context envelope diagnostics and segments', async () => {
    document.body.innerHTML = '<div id="toast"></div><div id="panel-content"></div>';
    window.Api.sid = 's1';
    vi.stubGlobal('fetch', vi.fn((url) => {
      const path = String(url);
      if (path.includes('/api/sessions/s1/context/recommendations')) {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({
            recommendations: [{
              recommendation: 'Start a handoff before adding more context',
              count: 2,
              actions: { acknowledged: 2 },
            }]
          })
        });
      }
      if (path.includes('/api/context/ctx-1')) {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({
            context: {
              envelope_id: 'ctx-1',
              envelope: {
                id: 'ctx-1',
                profile: 'MainTurn',
                intent: 'ship now',
                diagnostics: { pressure_bp: 150 },
                selected: [{
                  role: 'Evidence',
                  source: 'ToolTrace',
                  authority: 'Tool',
                  visibility: 'Private',
                  content: 'historical cargo test passed',
                  score: 0.88,
                  token_estimate: 8,
                  evidence: ['tool://test/evidence/event-1'],
                }],
                omitted: [],
              },
            },
          })
        });
      }
      if (path.includes('/api/sessions/s1/runs')) {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({
            runs: [{
              sequence: 7,
              created_at_ms: 1002,
              run: {
                run_id: 'run-1',
                profile: 'MainTurn',
                status: 'completed',
                intent_preview: 'ship now',
                context_envelope_id: 'ctx-1',
              },
            }]
          })
        });
      }
      if (path.includes('/api/sessions/s1/context')) {
        return Promise.resolve({
          ok: true,
          json: () => Promise.resolve({
            envelopes: [{
              envelope_id: 'ctx-1',
              sequence: 4,
              created_at_ms: 1000,
              envelope: { id: 'ctx-1', profile: 'MainTurn', intent: 'ship now', diagnostics: { pressure_bp: 150 } },
            }]
          })
        });
      }
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          enabled: true,
          source: 'runtime',
          envelope: {
            id: 'ctx-1',
            profile: 'MainTurn',
            selected: [{
              role: 'Evidence',
              source: 'Memory',
              authority: 'Session',
              visibility: 'Private',
              content: 'SessionKernel owns durable sessions',
              score: 0.93,
              token_estimate: 12,
              evidence: ['session://s1/memory/mem-1'],
            }],
            omitted: [{ source: 'Memory', reason: 'context lease exhausted', token_estimate: 30 }],
            budget: { total_tokens: 8000, used_tokens: 120 },
            diagnostics: {
              pressure_bp: 150,
              stable_head_hash: 'stablehashabcdef',
              runtime_header_hash: 'runtimehashabcdef',
              dynamic_tail_hash: 'dynamichashabcdef',
              degraded_sources: ['Memory'],
              recommendations: ['Start a handoff before adding more context'],
            },
            assembled: {
              stable_head: ['stable system'],
              runtime_header: ['session:s1 agent:primary'],
              dynamic_tail: ['memory packet body'],
            },
          },
        })
      });
    }));

    await window.Panels.renderContext();
    await new Promise(resolve => setTimeout(resolve, 0));
    await new Promise(resolve => setTimeout(resolve, 0));

    const text = document.getElementById('panel-content').textContent;
    expect(text).toContain('Context Runtime');
    expect(text).toContain('runtime');
    expect(text).toContain('SessionKernel owns durable sessions');
    expect(text).toContain('session://s1/memory/mem-1');
    expect(text).toContain('context lease exhausted');
    expect(text).toContain('stable system');
    expect(text).toContain('dynamichasha');
    expect(text).toContain('degraded: Memory');
    expect(text).toContain('Start a handoff');
    expect(text).toContain('ack 2');
    expect(text).toContain('Ack');
    expect(text).toContain('Context Timeline');
    expect(text).toContain('Runtime Runs');
    expect(text).toContain('run-1');
    expect(text).toContain('completed');
    expect(text).toContain('ship now');
    expect(text).toContain('historical cargo test passed');
    expect(text).toContain('tool://test/evidence/event-1');
  });

  it('renders recall explain metadata in the memory panel', async () => {
    document.body.innerHTML = '<div id="toast"></div><div id="panel-content"></div>';
    const mockF = vi.fn((url) => {
      const path = String(url);
      if (path.includes('/api/memory/stats')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ total_entries: 1, entity_count: 0, triple_count: 0 }) });
      }
      if (path.includes('/api/memory/entities')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ entities: [] }) });
      }
      if (path.includes('/api/memory/triples')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ triples: [] }) });
      }
      if (path.includes('/api/memory/recall/explain')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({
          mode: 'keyword',
          degraded: false,
          results: [{
            source_layer: 'L3',
            category: 'ProjectKnowledge',
            mode: 'keyword',
            score: 0.87,
            snippet: 'SessionKernel owns durable sessions',
          }]
        }) });
      }
      if (path.includes('/api/memory/packet')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({
          packet: {
            selected: [{
              role: 'Orientation',
              reason: 'query match',
              atom: { title: 'SessionKernel owns durable sessions', layer: 'L3', category: 'ProjectKnowledge', state: 'Active' },
            }],
            omitted: [{ title: 'Old session note', reason: 'budget' }],
            token_estimate: 42,
            truncated: false,
          }
        }) });
      }
      if (path.includes('/api/memory/links')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({
          links: [{ from: '11111111-1111-1111-1111-111111111111', to: '22222222-2222-2222-2222-222222222222', kind: 'Supports', weight: 0.8, evidence: 'same decision' }]
        }) });
      }
      if (path.includes('/api/memory/layers')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ layers: [] }) });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
    });
    vi.stubGlobal('fetch', mockF);

    await window.Panels.renderMemory();
    const input = Array.from(document.querySelectorAll('input')).find(el => el.placeholder === 'Search memory...');
    input.value = 'SessionKernel';
    input.dispatchEvent(new Event('input'));
    await new Promise(resolve => setTimeout(resolve, 0));

    const text = document.getElementById('panel-content').textContent;
    expect(String(mockF.mock.calls.at(-1)[0])).toContain('/api/memory/recall/explain?q=SessionKernel&limit=20');
    expect(text).toContain('Recall Explain');
    expect(text).toContain('Context Packet');
    expect(text).toContain('Orientation');
    expect(text).toContain('Memory Links');
    expect(text).toContain('Supports 1');
    expect(text).toContain('Mode: keyword');
    expect(text).toContain('L3');
    expect(text).toContain('score 0.87');
    expect(text).toContain('SessionKernel owns durable sessions');
  });

  it('renders symbol-memory search results in the memory panel', async () => {
    document.body.innerHTML = '<div id="toast"></div><div id="panel-content"></div>';
    const mockF = vi.fn((url) => {
      const path = String(url);
      if (path.includes('/api/memory/stats')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ total_entries: 1, entity_count: 0, triple_count: 0 }) });
      }
      if (path.includes('/api/memory/entities')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ entities: [] }) });
      }
      if (path.includes('/api/memory/triples')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ triples: [] }) });
      }
      if (path.includes('/api/memory/symbol-links?symbol=')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ entries: [{ title: 'Auth impact note', content: 'authenticate_user controls auth policy' }] }) });
      }
      if (path.includes('/api/memory/layers')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ layers: [] }) });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
    });
    vi.stubGlobal('fetch', mockF);

    await window.Panels.renderMemory();
    const input = document.getElementById('memory-symbol-search');
    expect(input).toBeTruthy();

    input.value = 'authenticate_user';
    input.dispatchEvent(new Event('input'));
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(String(mockF.mock.calls.at(-1)[0])).toContain('/api/memory/symbol-links?symbol=authenticate_user');
    expect(document.getElementById('memory-symbol-results').textContent).toContain('Auth impact note');
  });

  it('renders a knowledge network graph from triples and kernel links', async () => {
    document.body.innerHTML = '<div id="toast"></div><div id="panel-content"></div>';
    vi.stubGlobal('fetch', vi.fn((url) => {
      const path = String(url);
      if (path.includes('/api/memory/triples')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ triples: [{ subject: 'SessionKernel', predicate: 'owns', object: 'sessions' }] }) });
      }
      if (path.includes('/api/memory/links')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ links: [{ from: '11111111-1111-1111-1111-111111111111', to: '22222222-2222-2222-2222-222222222222', kind: 'Supports', weight: 0.9, evidence: 'shared decision' }] }) });
      }
      return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
    }));

    await window.Panels.renderMemoryNetwork();

    const text = document.getElementById('panel-content').textContent;
    expect(document.querySelector('.memory-network svg')).toBeTruthy();
    expect(text).toContain('Knowledge Network');
    expect(text).toContain('SessionKernel');
    expect(text).toContain('owns');
    expect(text).toContain('Supports');

    const graph = document.querySelector('.memory-network');
    const filter = graph.querySelector('input[placeholder="Filter network..."]');
    const type = graph.querySelector('select');
    expect(filter).toBeTruthy();
    expect(type).toBeTruthy();

    type.value = 'link';
    type.dispatchEvent(new Event('change'));
    expect(graph.textContent).toContain('Supports');
    expect(graph.textContent).not.toContain('owns');

    type.value = 'all';
    type.dispatchEvent(new Event('change'));
    filter.value = 'SessionKernel';
    filter.dispatchEvent(new Event('input'));
    expect(graph.querySelector('.memory-node.matched')).toBeTruthy();

    graph.querySelector('[data-node-id="SessionKernel"]').dispatchEvent(new Event('click'));
    expect(graph.querySelector('.memory-network-detail').textContent).toContain('SessionKernel');
  });

  it('renders durable task status in the agents panel', async () => {
    document.body.innerHTML = '<div id="toast"></div><div id="panel-content"></div>';
    vi.stubGlobal('fetch', vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          current: {
            id: 'task-1',
            objective: 'Finish v0.8.10',
            status: 'blocked',
            blocker_reason: 'external input required',
            phases: [{
              id: 'phase-1',
              name: 'browser-e2e',
              objective: 'Cover task workbench',
              status: 'completed',
              acceptance: ['E2E passes'],
              test_commands: ['npm run test:e2e'],
              artifacts: [{ kind: 'test', label: 'playwright', value: '2 passed' }],
              review_result: 'accepted'
            }]
          },
          tasks: []
        })
      })
    ));

    await window.Panels.renderAgents();

    const text = document.getElementById('panel-content').textContent;
    expect(text).toContain('blocked');
    expect(text).toContain('Finish v0.8.10');
    expect(text).toContain('browser-e2e');
    expect(text).toContain('E2E passes');
    expect(text).toContain('npm run test:e2e');
    expect(text).toContain('playwright');
    expect(text).toContain('accepted');
    expect(text).toContain('external input required');
  });

  it('renders approval queue and posts approval decisions', async () => {
    document.body.innerHTML = '<div id="toast"></div><div id="cc-content"></div>';
    const mockF = vi.fn((url, opts) => {
      const path = String(url);
      if (path.includes('/api/approval/respond')) {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ resolved: true }) });
      }
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve([{ id: 'approval-1', tool: 'bash', action: 'rm -rf /tmp/build' }])
      });
    });
    vi.stubGlobal('fetch', mockF);

    await window.Panels.renderCCApproval();

    const cc = document.getElementById('cc-content');
    expect(cc.textContent).toContain('Pending Approvals');
    expect(cc.textContent).toContain('bash');

    const approve = [...cc.querySelectorAll('button')].find(btn => btn.textContent === 'Approve');
    await approve.onclick();
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(String(mockF.mock.calls[1][0])).toBe('/api/approval/respond');
    expect(JSON.parse(mockF.mock.calls[1][1].body)).toEqual({ id: 'approval-1', approved: true });
    expect(mockF.mock.calls.filter(call => String(call[0]).includes('/api/approval/pending')).length).toBe(2);
  });

  it('renders unified audit export records in the enterprise audit panel', async () => {
    document.body.innerHTML = '<div id="toast"></div><div id="panel-content"></div>';
    const mockF = vi.fn((url) =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          kind: 'audit_export',
          source: 'all',
          total: 2,
          totals: { memory: 1, approval: 1 },
          records: [
            {
              source: 'memory',
              timestamp: '2026-06-05T01:00:00Z',
              summary: 'Create L3 enterprise memory',
              record: { operation: 'Create', layer: 'L3' }
            },
            {
              source: 'approval',
              timestamp: '2026-06-05T01:01:00Z',
              summary: 'Approved bash command',
              record: { decision: 'approved', tool: 'bash' }
            }
          ]
        })
      })
    );
    vi.stubGlobal('fetch', mockF);

    await window.Panels.renderAudit();

    const text = document.getElementById('panel-content').textContent;
    expect(String(mockF.mock.calls[0][0])).toBe('/api/audit/export?source=all&limit=50&offset=0');
    expect(text).toContain('Enterprise Audit');
    expect(text).toContain('Create L3 enterprise memory');
    expect(text).toContain('Approved bash command');
    expect(text).toContain('memory');
    expect(text).toContain('approval');
  });

  it('Workspace render opens the right panel without toggling it closed', async () => {
    document.body.innerHTML = `
      <div id="toast"></div>
      <aside id="right-panel" class="hidden"></aside>
      <div id="panel-tabs">
        <button data-panel="workspace"></button>
        <button data-panel="memory"></button>
      </div>
      <div id="panel-content"></div>
    `;
    vi.stubGlobal('fetch', vi.fn(() =>
      Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          files: [{ name: 'crates', path: 'crates', is_dir: true, type: 'dir' }]
        })
      })
    ));

    await window.Workspace.render();

    expect(document.getElementById('right-panel').classList.contains('hidden')).toBe(false);
    expect(document.querySelector('[data-panel="workspace"]').classList.contains('tab-active')).toBe(true);
    expect(document.getElementById('panel-content').textContent).toContain('crates');
  });

  it('Settings renders profiles and persists profile switches', async () => {
    document.body.innerHTML = '<div id="toast"></div><div id="panel-content"></div>';
    const mockF = vi.fn((url, opts) => {
      const path = String(url);
      if (path === '/api/config') {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ model: 'claude-sonnet-4-6' }) });
      }
      if (path === '/api/profiles/switch') {
        return Promise.resolve({ ok: true, json: () => Promise.resolve({ active_profile: 'enterprise_ops', runtime_profile: 'default', restart_required: true }) });
      }
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve({
          profiles: [
            { id: 'default', name: 'default', is_active: true },
            { id: 'enterprise_ops', name: 'Enterprise Ops', is_active: false }
          ],
          active_profile: 'default',
          runtime_profile: 'default'
        })
      });
    });
    vi.stubGlobal('fetch', mockF);

    await window.Panels.renderSettings();

    const panel = document.getElementById('panel-content');
    expect(panel.textContent).toContain('Profiles');
    expect(panel.textContent).toContain('Enterprise Ops');

    const switchBtn = [...panel.querySelectorAll('button')].find(btn => btn.textContent === 'Switch');
    await switchBtn.onclick();
    await new Promise(resolve => setTimeout(resolve, 0));

    const switchCall = mockF.mock.calls.find(call => String(call[0]) === '/api/profiles/switch');
    expect(switchCall).toBeTruthy();
    expect(JSON.parse(switchCall[1].body)).toEqual({ profile: 'enterprise_ops' });
  });

  it('command and fact endpoints are defined', () => {
    expect(typeof window.Api.listCommands).toBe('function');
    expect(typeof window.Api.commandHistory).toBe('function');
    expect(typeof window.Api.executeCommand).toBe('function');
    expect(typeof window.Api.auditFacts).toBe('function');
  });

  it('cron log endpoints are defined', () => {
    expect(typeof window.Api.listCronLogs).toBe('function');
    expect(typeof window.Api.listAllCronLogs).toBe('function');
  });

  it('binds main UI controls before auth login completes', async () => {
    document.body.innerHTML = `
      <div id="toast"></div>
      <button id="btn-new-session"></button>
      <button id="btn-send"></button>
      <button id="btn-slash"></button>
      <textarea id="chat-input"></textarea>
      <div id="slash-dropdown" class="hidden"></div>
      <input id="session-search">
      <select id="model-selector"></select>
      <aside id="right-panel" class="hidden"></aside>
      <button id="btn-toggle-panel"></button>
      <button id="btn-control-center"></button>
      <div id="panel-tabs">
        <button data-panel="workspace"></button>
        <button data-panel="close"></button>
      </div>
      <div id="control-center" class="modal-overlay hidden">
        <div class="modal-tabs"><button data-cc="config"></button></div>
      </div>
      <div id="login-modal" class="modal-overlay hidden">
        <input id="login-token" value="test-token">
        <div id="login-error"></div>
        <button id="btn-login"></button>
      </div>
    `;

    window.Api.verifyAuth = vi.fn(() => Promise.reject(new Error('unauthorized')));
    window.Api.login = vi.fn(() => Promise.resolve({ success: true, token: 'ok' }));
    window.Api.getConfig = vi.fn(() => Promise.resolve({ model: 'claude-sonnet-4-6' }));
    window.Sessions.load = vi.fn(() => Promise.resolve());
    window.Sessions.createSession = vi.fn();

    window.dispatchEvent(new Event('DOMContentLoaded'));
    await new Promise(resolve => setTimeout(resolve, 0));

    expect(document.getElementById('login-modal').classList.contains('hidden')).toBe(false);

    document.getElementById('btn-login').click();
    await new Promise(resolve => setTimeout(resolve, 0));

    document.getElementById('btn-new-session').click();

    expect(window.Api.login).toHaveBeenCalledWith('test-token');
    expect(window.Sessions.createSession).toHaveBeenCalled();
  });
});
