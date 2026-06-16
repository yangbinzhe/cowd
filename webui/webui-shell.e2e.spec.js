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
    if (path === '/api/skills/projection') return json({
      catalog_count: 1,
      capabilities: ['plan', 'run', 'validate'],
      governance: { evidence_model: 'required', tool_fact_model: 'structured', approval_model: 'policy' },
      items: [{ id: 'runtime.inspect', name: 'runtime.inspect', scope: 'core', domain: 'runtime', risk: 'low', status: 'ready', description: 'Inspect runtime state', tags: ['cowd'] }],
      facets: { scopes: ['core'], domains: ['runtime'], risks: ['low'], statuses: ['ready'] },
      queue: { supports_watch: true },
    });
    if (path === '/api/skills/runs') return json({ items: [{ skill_id: 'runtime.inspect', status: 'completed', summary: 'validated runtime' }] });
    if (path === '/api/agents/runs') return json({ runs: [{ status: 'completed', objective: 'Parallel review', nodes: [{ id: 'n1', role: 'reviewer', status: 'done', title: 'Review UI' }], evidence: [], reviews: [], merge_decisions: [] }] });
    if (path === '/api/platforms') return json({ platforms: [{ name: 'wechat', configured: true, running: true, capabilities: ['send_text', 'receive'] }] });
    if (path === '/api/platforms/wechat') return json({ sessions: [{ id: 'wx-1', status: 'bound' }] });
    if (path === '/api/channels/wechat-ilink/accounts') return json({ accounts: [{ id: 'wx-account', nickname: 'Cowd WeChat', runtime_bound: true }] });
    if (path === '/api/audit/export') return json({ records: [{ id: 'audit-1', source: 'gateway', action: 'send_text', status: 'ok', target: 'wechat' }], total: 1 });
    if (path === '/api/iacc/app') return json({
      app_id: 'iacc.manufacturing',
      layer: 'application',
      domains: [{ domain_id: 'server_manufacturing', name: 'Server Manufacturing' }],
      cowd_capabilities: ['cowd.structured_data.core', 'cowd.memory.fact_check', 'cowd.cross_plane.gateway'],
    });
    if (path === '/api/iacc/health') return json({
      schema_version: '2026.06',
      expected_schema_version: '2026.06',
      cockpit_profile_count: 2,
      cockpit_report_count: 3,
      quality_gate_count: 4,
      execution_count: 7,
      attention_count: 1,
      capabilities: ['iacc.cockpit_report.generate', 'iacc.cockpit_report.deliver'],
    });
    if (path === '/api/cowd/structured/sources') return json({ list_status: 'ready', count: 1, items: [{ source_id: 'mes-inventory', source_name: 'MES inventory', high_watermark: '2026-06-16T08:00:00Z' }] });
    if (path === '/api/cowd/structured/facts') return json({ list_status: 'ready', count: 1, items: [{ fact_id: 'fact-inventory', fact_type: 'inventory_balance', metric_key: 'shortage_rate', confidence: 0.91 }] });
    if (path === '/api/cowd/structured/evidence') return json({ list_status: 'ready', count: 1, items: [{ evidence_id: 'evidence-risk', problem_statement: 'Inventory balance browser proof', confidence: 0.82, source_refs: [{ reference: 'iacc:fact:fact-inventory' }] }] });
    if (path === '/api/cowd/structured/watermarks') return json({ list_status: 'ready', count: 1, items: [{ source_ref: 'mes-inventory', fact_type: 'inventory_balance', high_watermark: '2026-06-16T08:00:00Z' }] });
    if (path === '/api/cowd/structured/ingest-plan') return json({ batch_id: 'ingest-browser', source_ref: 'mes-inventory' });
    if (path === '/api/iacc/cockpit/reports/cockpit-report-browser') return json({
      report: {
        report_id: 'cockpit-report-browser',
        status: 'ready',
        cadence: 'daily',
        owner_ref: 'quality:ops',
        profile_id: 'daily-risk',
        delivery_ref: 'wechat:ops',
        projection: { widgets: [{ id: 'risk' }, { id: 'inventory' }] },
        delivery_receipts: [{ cross_plane_receipt_id: 'receipt-iacc', cross_plane_status: 'sent', cross_plane_dispatch_status: 'dry_run', delivered_at: '2026-06-16T08:30:00Z' }],
      },
    });
    if (path === '/api/iacc/cockpit/reports/cockpit-report-browser/delivery-state') return json({
      delivery_state: {
        classification: 'dry_run_planned',
        attempt_count: 1,
        retryable: true,
        recommended_mode: 'dry_run',
        latest_receipt: { cross_plane_receipt_id: 'receipt-iacc', cross_plane_status: 'sent', cross_plane_dispatch_status: 'dry_run', audit_record_id: 'audit-iacc' },
      },
    });
    if (path === '/api/iacc/cockpit/reports/cockpit-report-browser/delivery/retry') return json({ after_state: { classification: 'dry_run_planned' } });
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

test('v0.9.207 skills agents and tools render as full-page capability workbenches', async ({ page }) => {
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

  await page.click('#nav-rail [data-view="skills"]');
  await expect(page.locator('#workbench-content.workbench-page-skills')).toBeVisible();
  await expect(page.locator('#workbench-content')).toContainText('runtime.inspect');
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.207-skills-desktop.png' });

  await page.click('#nav-rail [data-view="agents"]');
  await expect(page.locator('#workbench-content.workbench-page-agents')).toBeVisible();
  await expect(page.locator('#workbench-content')).toContainText('Task Registry');
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.207-agents-desktop.png' });

  await page.click('#nav-rail [data-view="tools"]');
  await expect(page.locator('#workbench-content.workbench-page-tools')).toBeVisible();
  await expect(page.locator('#workbench-content')).toContainText('Available Tools');
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.207-tools-desktop.png' });

  expect(errors).toEqual([]);
});

test('v0.9.208 gateway and audit render as full-page operations workbenches', async ({ page }) => {
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

  await page.click('#nav-rail [data-view="gateway"]');
  await expect(page.locator('#workbench-content.workbench-page-gateway')).toBeVisible();
  await expect(page.locator('#workbench-title')).toHaveText('Gateway');
  await expect(page.locator('#workbench-content')).toContainText(/Gateway|Connector|wechat/i);
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.208-gateway-desktop.png' });

  await page.click('#nav-rail [data-view="audit"]');
  await expect(page.locator('#workbench-content.workbench-page-audit')).toBeVisible();
  await expect(page.locator('#workbench-title')).toHaveText('Audit');
  await expect(page.locator('#workbench-content')).toContainText(/Audit|gateway|send_text/i);
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.208-audit-desktop.png' });

  expect(errors).toEqual([]);
});

test('v0.9.209 iacc renders as manufacturing application workbench on cowd data kernel', async ({ page }) => {
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
  await page.addInitScript(() => localStorage.setItem('cowd-iacc-report-id', 'cockpit-report-browser'));
  await page.goto('http://127.0.0.1:9241/index.html', { waitUntil: 'domcontentloaded' });

  await page.click('#nav-rail [data-view="iacc"]');
  await expect(page.locator('#workbench-content.workbench-page-iacc')).toBeVisible();
  await expect(page.locator('#workbench-title')).toHaveText('IACC');
  await expect(page.locator('#workbench-content')).toContainText('IACC Workbench');
  await expect(page.locator('#workbench-content')).toContainText('iacc.manufacturing');
  await expect(page.locator('#workbench-content')).toContainText('cowd.structured_data.core');
  await expect(page.locator('#workbench-content')).toContainText('inventory_balance');
  await expect(page.locator('#workbench-content')).toContainText('dry_run_planned');
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.209-iacc-desktop.png' });

  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: '../plan/0616-前端重构/screenshots/v0.9.209-iacc-mobile.png' });

  expect(errors).toEqual([]);
});
