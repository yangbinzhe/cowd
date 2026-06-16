import { test, expect } from '@playwright/test';

if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
  test.use({
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    },
  });
}

async function routeShellApis(page) {
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
    if (path === '/api/config') return json({ model: 'claude-sonnet-4-6', version: '0.9.202' });
    if (path === '/api/sessions') return json({ sessions: [], total: 0, offset: 0, limit: 20 });
    if (path === '/api/workspace/files') return json({ files: [
      { name: 'README.md', path: 'README.md', is_dir: false },
      { name: 'src', path: 'src', is_dir: true },
    ] });
    if (path === '/api/context/current') return json({
      source: 'mock',
      envelope: {
        id: 'ctx-webui',
        profile: 'MainTurn',
        selected: [{ role: 'orientation', title: 'Runtime policy', content: 'Cowd kernel owns durable sessions', source: 'memory' }],
        omitted: [{ source: 'history', reason: 'budget', token_estimate: 128 }],
        diagnostics: { pressure_bp: 125, stable_head_hash: 'stablehash', runtime_header_hash: 'runtimehash', dynamic_tail_hash: 'dynamichash' },
        budget: { used_tokens: 512, total_tokens: 8000 },
        assembled: { stable_head: ['system'], runtime_header: ['lease'], dynamic_tail: ['turn'] },
      },
      lean_probe: { status: 'ok' },
      policy_decision: { decision: 'full' },
    });
    if (path === '/api/runtime/config/effective') return json({ control_policy: { agent: { max_parallel_agents: 4 } } });
    if (path === '/api/runtime/control-plane') return json({ components: { permissions: { approval_gate: true, auth_required: true } } });
    if (path === '/api/cowd/capabilities') return json({ capabilities: [{ id: 'runtime' }] });
    if (path === '/api/cowd/projection') return json({ surface: 'webui', capabilities: ['runtime', 'context'] });
    if (path === '/api/cowd/surfaces') return json({ surfaces: ['cli', 'tui', 'webui'] });
    if (path === '/api/cowd/release-gate') return json({ status: 'pass' });
    if (path === '/api/tasks') return json({ current: { id: 'task-1', objective: 'Inspect runtime', status: 'running' }, tasks: [] });
    if (path === '/api/approval/pending') return json({ approvals: [] });
    if (/^\/api\/sessions\/[^/]+\/runs$/.test(path)) return json({ runs: [{ run: { run_id: 'run-1', status: 'completed' } }], tree: { summary: { span_count: 1, root_count: 1, failed_count: 0, running_count: 0 } } });
    if (path === '/api/runtime/timeline') return json({ total: 1, next_seq: 2, degraded: false, events: [{ kind: 'ToolComplete', seq: 1 }], value_loop: { status: 'complete' } });
    if (/^\/api\/sessions\/[^/]+\/context$/.test(path)) return json({ summaries: [{ envelope_id: 'ctx-webui', profile: 'MainTurn', intent: 'ship', pressure_bp: 125 }], has_more: false });
    if (path === '/api/context/ctx-webui') return json({ context: { envelope_id: 'ctx-webui', selected: [] } });
    if (path === '/api/sessions/webui-runtime-console/context/recommendations') return json({ recommendations: [] });
    if (path === '/api/memory/maintenance') return json({ candidates: [] });
    if (path === '/api/memory/status') return json({ enabled: true, status: 'ready', total_entries: 12, degraded: false });
    if (path === '/api/memory/stats') return json({ entries: 12, entities: 2, triples: 2 });
    if (path === '/api/memory/links') return json({ links: [{ kind: 'Supports', from: 'SessionKernel', to: 'TaskKernel' }] });
    if (path === '/api/memory/entities') return json({ entities: [{ id: 'SessionKernel', name: 'SessionKernel' }, { id: 'TaskKernel', name: 'TaskKernel' }] });
    if (path === '/api/memory/triples') return json({ triples: [{ subject: 'SessionKernel', predicate: 'owns', object: 'sessions' }] });
    if (path === '/api/memory/layers') return json({ layers: [{ name: 'semantic' }, { name: 'episodic' }] });
    if (path === '/api/memory/runtime') return json({ clusters: [], status: 'ready' });
    if (path === '/api/memory/clusters') return json({ clusters: [{ id: 'cluster-1', label: 'Runtime' }] });
    return json({});
  });
}

test('v0.9.202 shell routes rail views into the central workbench', async ({ page }) => {
  const errors = [];
  page.on('pageerror', err => errors.push('pageerror: ' + err.message));
  page.on('console', msg => {
    if (msg.type() !== 'error') return;
    const text = msg.text();
    if (text.includes('Failed to load resource')) return;
    if (text.includes('Failed to find a valid digest')) return;
    errors.push('console: ' + text);
  });

  await routeShellApis(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('http://127.0.0.1:9241/index.html', { waitUntil: 'domcontentloaded' });

  await expect(page.locator('#app-shell')).toBeVisible();
  await expect(page.locator('#nav-rail')).toBeVisible();
  await expect(page.locator('#nav-rail [data-view="chat"]')).toHaveClass(/active/);
  await expect(page.locator('html')).toHaveAttribute('data-skin', 'graphite');
  await expect(page.locator('#version')).toHaveText('0.9.202');

  await page.click('#nav-rail [data-view="workspace"]');
  await expect(page.locator('#workbench-view')).not.toHaveClass(/hidden/);
  await expect(page.locator('#chat-view')).toHaveClass(/hidden/);
  await expect(page.locator('#right-panel')).toHaveClass(/hidden/);
  await expect(page.locator('#nav-rail [data-view="workspace"]')).toHaveClass(/active/);
  await expect(page.locator('#workbench-title')).toHaveText('Workspace');
  await expect(page.locator('#workbench-content')).toContainText('README.md');

  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.202-workbench-desktop.png' });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.202-workbench-mobile.png' });

  await page.click('#btn-workbench-chat');
  await expect(page.locator('#chat-view')).not.toHaveClass(/hidden/);
  await page.click('#btn-toggle-panel');
  await expect(page.locator('#right-panel')).not.toHaveClass(/hidden/);

  expect(errors).toEqual([]);
});

test('v0.9.203 chat chrome and activity metadata stay readable', async ({ page }) => {
  const errors = [];
  page.on('pageerror', err => errors.push('pageerror: ' + err.message));
  page.on('console', msg => {
    if (msg.type() !== 'error') return;
    const text = msg.text();
    if (text.includes('Failed to load resource')) return;
    if (text.includes('Failed to find a valid digest')) return;
    errors.push('console: ' + text);
  });

  await routeShellApis(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('http://127.0.0.1:9241/index.html', { waitUntil: 'domcontentloaded' });

  await expect(page.locator('.chat-title-block')).toContainText('Cowd');
  await expect(page.locator('#chat-input')).toBeVisible();
  await page.evaluate(() => {
    const mount = document.getElementById('chat-messages');
    const assistant = document.createElement('div');
    assistant.className = 'message assistant';
    const body = document.createElement('div');
    body.className = 'msg-body';
    body.textContent = 'Runtime check completed.';
    assistant.appendChild(body);
    assistant.appendChild(window.UI.addToolCard('browser-tool', 'workspace.read', 'complete'));
    assistant.appendChild(window.UI.addThinkCard('Checked shell state and route contract.'));
    mount.appendChild(assistant);
  });
  await expect(page.locator('.tool-card')).toContainText('Tool: workspace.read');
  await expect(page.locator('.think-card')).toContainText('Thinking');
  await expect(page.locator('body')).not.toContainText('&xutri;');
  await expect(page.locator('body')).not.toContainText('&xodot;');

  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.203-chat-desktop.png' });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.203-chat-mobile.png' });

  expect(errors).toEqual([]);
});

test('v0.9.204 workspace uses the full-page file workbench layout', async ({ page }) => {
  const errors = [];
  page.on('pageerror', err => errors.push('pageerror: ' + err.message));
  page.on('console', msg => {
    if (msg.type() !== 'error') return;
    const text = msg.text();
    if (text.includes('Failed to load resource')) return;
    if (text.includes('Failed to find a valid digest')) return;
    errors.push('console: ' + text);
  });

  await routeShellApis(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('http://127.0.0.1:9241/index.html', { waitUntil: 'domcontentloaded' });
  await page.click('#nav-rail [data-view="workspace"]');

  await expect(page.locator('#workbench-content.workspace-page')).toBeVisible();
  await expect(page.locator('.workspace-hero')).toContainText('Browse, preview, and create files');
  await expect(page.locator('.workspace-file-list')).toContainText('README.md');
  await expect(page.locator('.workspace-file-kind').first()).toHaveText('FILE');
  await expect(page.locator('.workspace-file-kind').nth(1)).toHaveText('DIR');

  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.204-workspace-desktop.png' });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.204-workspace-mobile.png' });

  expect(errors).toEqual([]);
});

test('v0.9.205 runtime and context render as full-page kernel workbenches', async ({ page }) => {
  const errors = [];
  page.on('pageerror', err => errors.push('pageerror: ' + err.message));
  page.on('console', msg => {
    if (msg.type() !== 'error') return;
    const text = msg.text();
    if (text.includes('Failed to load resource')) return;
    if (text.includes('Failed to find a valid digest')) return;
    errors.push('console: ' + text);
  });

  await routeShellApis(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('http://127.0.0.1:9241/index.html', { waitUntil: 'domcontentloaded' });

  await page.click('#nav-rail [data-view="runtime"]');
  await expect(page.locator('#workbench-content.workbench-page-runtime')).toBeVisible();
  await expect(page.locator('#workbench-title')).toHaveText('Runtime');
  await expect(page.locator('#workbench-content')).toContainText('Runtime Console');
  await expect(page.locator('#workbench-content')).toContainText('Runtime State');
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.205-runtime-desktop.png' });

  await page.click('#nav-rail [data-view="context"]');
  await expect(page.locator('#workbench-content.workbench-page-context')).toBeVisible();
  await expect(page.locator('#workbench-title')).toHaveText('Context');
  await expect(page.locator('#workbench-content')).toContainText('Context Runtime');
  await expect(page.locator('#workbench-content')).toContainText('Selected Context');
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.205-context-desktop.png' });

  await page.setViewportSize({ width: 390, height: 844 });
  await page.click('#nav-rail [data-view="runtime"]');
  await expect(page.locator('#workbench-content')).toContainText('MainTurn');
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.205-runtime-mobile.png' });

  expect(errors).toEqual([]);
});

test('v0.9.206 memory renders as a full-page knowledge workbench', async ({ page }) => {
  const errors = [];
  page.on('pageerror', err => errors.push('pageerror: ' + err.message));
  page.on('console', msg => {
    if (msg.type() !== 'error') return;
    const text = msg.text();
    if (text.includes('Failed to load resource')) return;
    if (text.includes('Failed to find a valid digest')) return;
    errors.push('console: ' + text);
  });

  await routeShellApis(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto('http://127.0.0.1:9241/index.html', { waitUntil: 'domcontentloaded' });
  await page.click('#nav-rail [data-view="memory"]');

  await expect(page.locator('#workbench-content.workbench-page-memory')).toBeVisible();
  await expect(page.locator('#workbench-title')).toHaveText('Memory');
  await expect(page.locator('#workbench-content')).toContainText(/ready|Memory/i);
  await expect(page.locator('#workbench-content')).toContainText('SessionKernel');
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.206-memory-desktop.png' });

  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.206-memory-mobile.png' });

  expect(errors).toEqual([]);
});
