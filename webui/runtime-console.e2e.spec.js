import { test, expect } from '@playwright/test';

if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
  test.use({
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    },
  });
}

test('runtime console opens timeline context refs in browser', async ({ page }) => {
  const errors = [];
  let providerReloads = 0;
  let approved = 0;
  let completed = 0;

  page.on('pageerror', err => errors.push('pageerror: ' + err.message));
  page.on('console', msg => {
    if (msg.type() !== 'error') return;
    const text = msg.text();
    if (text.includes('Failed to load resource')) return;
    if (text.includes('Failed to find a valid digest')) return;
    errors.push('console: ' + text);
  });

  await page.route('https://cdn.jsdelivr.net/**', route => route.fulfill({ status: 204, body: '' }));
  await page.route('**/api/**', route => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    const json = body => route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(body),
    });

    if (path === '/api/auth/verify') return json({ ok: true });
    if (path === '/api/config') return json({ model: 'claude-sonnet-4-6', version: '0.8.53' });
    if (path === '/api/sessions') {
      return json({
        sessions: [{ id: 'session-runtime-1', title: 'Runtime Evidence Session', model: 'claude-sonnet-4-6' }],
        total: 1,
        offset: 0,
        limit: 20,
      });
    }
    if (path === '/api/sessions/session-runtime-1/messages') {
      return json({ session_id: 'session-runtime-1', messages: [], total: 0, has_more: false });
    }
    if (path === '/api/sessions/session-runtime-1/stream') {
      return route.fulfill({
        status: 200,
        contentType: 'text/event-stream',
        body: 'data: {"type":"Connected"}\n\n',
      });
    }
    if (path === '/api/context/current') {
      return json({
        enabled: true,
        source: 'runtime',
        envelope: {
          id: 'ctx-current-runtime',
          profile: 'YoloGoal',
          selected: [{
            role: 'Evidence',
            source: 'Memory',
            authority: 'Project',
            visibility: 'Shared',
            content: 'Runtime console browser context',
            score: 0.91,
            token_estimate: 12,
          }],
          omitted: [],
          budget: { total_tokens: 10000, used_tokens: 700 },
          diagnostics: {
            pressure_bp: 650,
            stable_head_hash: 'stable-browser-runtime',
            degraded_sources: [],
          },
        },
        lean_probe: {
          pressure_level: 'Nominal',
          degradation_path: 'None',
          selected_count: 1,
          omitted_count: 0,
          stable_head_hash: 'stable-browser-runtime',
          dynamic_tail_hash: 'tail-browser-runtime',
        },
        policy_decision: {
          action: 'KeepContext',
          reason: 'browser runtime context is healthy',
        },
      });
    }
    if (path === '/api/runtime/config/effective') {
      return json({ source: 'default', control_policy: { enabled: true, agent: { max_parallel_agents: 4 } } });
    }
    if (path === '/api/runtime/providers/reload') {
      providerReloads += 1;
      return json({
        kind: 'runtime_provider_reload',
        status: 'applied',
        applied: true,
        provider_count: 1,
        provider_model_count: 2,
        configured_model_resolved: true,
      });
    }
    if (path === '/api/runtime/control-plane') {
      return json({
        kind: 'runtime_control_plane',
        status: 'healthy',
        degraded: false,
        profile_id: 'default',
        config: { source: 'default', scenario: 'coding' },
        diagnostics: {
          component_count: 8,
          capability_count: 9,
          stored_sessions: 2,
          open_tasks: 0,
          elapsed_ms: 7,
          performance_status: 'healthy',
          production_ready: true,
          readiness_score: 100,
          required_check_count: 10,
          ready_required_count: 10,
          blocked_required_count: 0,
          provider_configured: true,
          provider_count: 1,
          provider_model_count: 2,
          configured_model_resolved: true,
        },
        readiness: {
          production_ready: true,
          score: 100,
          required_total: 10,
          required_ready: 10,
          required_blocked: 0,
          checks: [{ id: 'session.sqlite_source_of_truth', status: 'ready', required: true }],
          blocked: [],
        },
        components: {
          session: { source_of_truth: 'sqlite', active_count: 1 },
          memory: { status: 'available', search_mode: 'hybrid' },
          context: { durable_history: true },
          agent: { status: 'available', max_parallel_agents: 4 },
          task: { open: 0 },
          permissions: { auth_required: false, approval_gate: true },
          provider: {
            status: 'available',
            provider_count: 1,
            model_count: 2,
            configured_model: 'sonnet-enterprise',
            configured_model_provider: 'anthropic',
            configured_model_resolved: true,
          },
          channels: { adapters: [{ id: 'wechat-ilink' }, { id: 'cross-plane' }] },
        },
        capabilities: ['session.sqlite_source_of_truth', 'permission.cross_plane'],
        degraded_reasons: [],
      });
    }
    if (path === '/api/sessions/session-runtime-1/runs') {
      return json({
        runs: [{
          sequence: 7,
          created_at_ms: 1000,
          run: {
            run_id: 'run-browser-1',
            profile: 'YoloGoal',
            status: 'completed',
            intent_preview: 'verify runtime console context refs',
            context_envelope_id: 'ctx-browser-1',
          },
        }],
        tree: {
          roots: ['run-browser-1'],
          children: {},
          summary: { span_count: 1, root_count: 1, failed_count: 0, running_count: 0 },
        },
      });
    }
    if (path === '/api/runtime/timeline') {
      return json({
        session_id: 'session-runtime-1',
        total: 3,
        next_seq: null,
        limit: 12,
        has_more: false,
        degraded: false,
        health_summary: {
          status: 'healthy',
          score: 100,
          event_count: 3,
          failed_events: 0,
          degraded_events: 0,
          open_tasks: 0,
          positive_agent_lift: false,
          reasons: ['runtime event spine is coherent'],
          scope_counts: { turn: 1, context: 1, policy: 1 },
        },
        workgraph_summary: { count: 0, latest: null, agent_tasks: 0, memory_candidates: 0, conflicts: 0 },
        events: [
          {
            kind: 'ContextEnvelope',
            scope: 'context',
            sequence: 5,
            created_at_ms: 900,
            payload: { envelope_id: 'ctx-browser-1' },
          },
          {
            kind: 'RuntimeRun',
            scope: 'turn',
            status: 'completed',
            sequence: 7,
            created_at_ms: 1000,
            refs: [{ type: 'context_envelope', id: 'ctx-browser-1' }],
            payload: { summary: 'turn completed' },
          },
          {
            kind: 'runtime.policy.decided',
            scope: 'policy',
            sequence: 8,
            created_at_ms: 1010,
            payload: {
              agent_mode: 'Solo',
              requires_review: false,
              complexity: { level: 'Simple', score: 30, signals: [] },
            },
          },
        ],
      });
    }
    if (path === '/api/context/ctx-browser-1') {
      return json({
        enabled: true,
        source: 'history',
        context: {
          envelope_id: 'ctx-browser-1',
          run_id: 'run-browser-1',
          sequence: 5,
          envelope: {
            id: 'ctx-browser-1',
            profile: 'YoloGoal',
            diagnostics: { pressure_bp: 650 },
            selected: [{
              role: 'Evidence',
              source: 'RuntimeTimeline',
              content: 'Browser runtime timeline context detail',
              score: 0.95,
            }],
            omitted: [],
          },
        },
      });
    }
    if (path === '/api/memory/maintenance') {
      return json({ enabled: true, candidates: [] });
    }
    if (path === '/api/tasks') {
      return json({
        current: {
          id: 'task-runtime-1',
          objective: 'Finish daemon runtime parity',
          status: completed ? 'completed' : 'running',
          current_phase: 'webui-control',
          review_result: 'accepted',
          artifact_count: 2,
        },
        tasks: [{
          id: 'task-runtime-1',
          objective: 'Finish daemon runtime parity',
          status: completed ? 'completed' : 'running',
          current_phase: 'webui-control',
          review_result: 'accepted',
          artifact_count: 2,
        }],
      });
    }
    if (path === '/api/tasks/task-runtime-1/complete') {
      completed += 1;
      return json({ ok: true });
    }
    if (path === '/api/tasks/task-runtime-1/cancel') {
      return json({ ok: true });
    }
    if (path === '/api/approval/pending') {
      return json(approved ? [] : [{
        id: 'approval-runtime-1',
        tool_name: 'shell.exec',
        risk: 'medium',
        requester: 'session-runtime-1',
        input_preview: 'cargo test -p cowd-cli',
      }]);
    }
    if (path === '/api/approval/respond') {
      approved += 1;
      return json({ ok: true });
    }

    return json({});
  });

  await page.goto('http://127.0.0.1:9241/index.html', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('#login-modal')).toHaveClass(/hidden/);

  await page.getByText('Runtime Evidence Session').click();
  await page.click('#btn-toggle-panel');
  await expect(page.locator('#right-panel')).not.toHaveClass(/hidden/);
  await page.click('#panel-tabs button[data-panel="runtime"]');

  await expect(page.locator('#panel-content')).toContainText('Runtime Console');
  await expect(page.locator('#panel-content')).toContainText('Control Plane');
  await expect(page.locator('#panel-content')).toContainText('sqlite');
  await expect(page.locator('#panel-content')).toContainText('permission.cross_plane');
  await expect(page.locator('#panel-content')).toContainText('stored');
  await expect(page.locator('#panel-content')).toContainText('latency');
  await expect(page.locator('#panel-content')).toContainText('perf');
  await expect(page.locator('#panel-content')).toContainText('ready');
  await expect(page.locator('#panel-content')).toContainText('blocked');
  await expect(page.locator('#panel-content')).toContainText('provider');
  await expect(page.locator('#panel-content')).toContainText('models');
  await expect(page.locator('#panel-content')).toContainText('route');
  await expect(page.locator('#panel-content')).toContainText('Reload providers');
  await expect(page.locator('#panel-content')).toContainText('components');
  await expect(page.locator('#panel-content')).toContainText('caps');
  await expect(page.locator('#panel-content')).toContainText('Runtime Timeline');
  await expect(page.locator('#panel-content')).toContainText('Daemon Tasks');
  await expect(page.locator('#panel-content')).toContainText('Finish daemon runtime parity');
  await expect(page.locator('#panel-content')).toContainText('webui-control');
  await expect(page.locator('#panel-content')).toContainText('Pending Approvals');
  await expect(page.locator('#panel-content')).toContainText('shell.exec');
  await expect(page.locator('#panel-content')).toContainText('cargo test -p cowd-cli');
  await expect(page.locator('#panel-content')).toContainText('context_envelope:ctx-browser-1');
  await expect(page.locator('.runtime-timeline .runtime-context-link')).toContainText('ctx-browser-1');

  await page.getByRole('button', { name: 'Complete' }).click();
  await expect.poll(() => completed).toBe(1);
  await page.getByRole('button', { name: 'Approve' }).click();
  await expect.poll(() => approved).toBe(1);
  await page.getByRole('button', { name: 'Reload providers' }).click();
  await expect.poll(() => providerReloads).toBe(1);

  await page.locator('.runtime-timeline .runtime-context-link').click();
  await expect(page.locator('#panel-content')).toContainText('Browser runtime timeline context detail');

  expect(errors).toEqual([]);
});
