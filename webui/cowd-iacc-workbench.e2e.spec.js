import { test, expect } from '@playwright/test';

if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
  test.use({
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    },
  });
}

test('cowd iacc workbench renders kernel overview structured data and ingest composer', async ({ page, baseURL }) => {
  const errors = [];
  let ingestBody = null;

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
    if (path === '/api/config') return json({ model: 'claude-sonnet-4-6', version: '0.9.117' });
    if (path === '/api/sessions') {
      return json({ sessions: [{ id: 'session-cowd-webui', title: 'Cowd IACC Browser', model: 'claude-sonnet-4-6' }], total: 1 });
    }
    if (path === '/api/sessions/session-cowd-webui/messages') return json({ messages: [], total: 0 });
    if (path === '/api/sessions/session-cowd-webui/stream') {
      return route.fulfill({ status: 200, contentType: 'text/event-stream', body: 'data: {"type":"Connected"}\n\n' });
    }
    if (path === '/api/context/current') {
      return json({
        envelope: { id: 'ctx-cowd-webui', selected: [], omitted: [], diagnostics: {}, budget: {} },
        lean_probe: { selected_count: 0, omitted_count: 0 },
        policy_decision: { action: 'KeepContext' },
      });
    }
    if (path === '/api/runtime/config/effective') return json({ source: 'default', control_policy: { enabled: true, agent: { max_parallel_agents: 4 } } });
    if (path === '/api/runtime/control-plane') {
      return json({
        readiness: { score: 100, checks: [] },
        diagnostics: { component_count: 8, readiness_score: 100 },
        components: { session: { source_of_truth: 'sqlite' }, memory: { status: 'available' }, provider: { status: 'available', provider_count: 1, model_count: 2 } },
        capabilities: ['cowd.structured_data.core', 'cowd.runtime.event'],
        degraded_reasons: [],
      });
    }
    if (path === '/api/runtime/session-leases') return json({ total: 0, items: [] });
    if (path === '/api/tasks') return json({ tasks: [] });
    if (path === '/api/approval/pending') return json([]);
    if (path === '/api/sessions/session-cowd-webui/runs') return json({ runs: [], tree: { roots: [], children: {}, summary: {} } });
    if (path === '/api/runtime/timeline') {
      return json({
        session_id: 'session-cowd-webui',
        total: 1,
        events: [{
          kind: 'execution.outcome',
          scope: 'turn',
          status: 'ok',
          sequence: 1,
          created_at_ms: 1718352000000,
          refs: [{ type: 'structured_fact', id: 'fact-webui-e2e' }],
          payload: { title: 'Structured fact captured', summary: 'fact-webui-e2e', metrics: ['stock_on_hand'] },
        }],
        health_summary: { status: 'healthy', score: 100, reasons: [] },
      });
    }
    if (path === '/api/cowd/capabilities') {
      return json({ capabilities: [{ id: 'cowd.structured_data.core' }, { id: 'cowd.runtime.event' }] });
    }
    if (path === '/api/cowd/projection') {
      return json({ surface: url.searchParams.get('surface') || 'webui', capability_count: 2, capabilities: [] });
    }
    if (path === '/api/cowd/surfaces') {
      return json({ webui_tui_full_parity: true, cli_is_minimal_control: true });
    }
    if (path === '/api/cowd/release-gate') {
      return json({ status: 'pass', checks: [{ check_id: 'structured_data.indexes.ready', status: 'pass' }] });
    }
    if (path === '/api/iacc/app') {
      return json({
        app_id: 'iacc.manufacturing',
        layer: 'application',
        cowd_capabilities: ['cowd.structured_data.core', 'cowd.runtime.event'],
        domains: [{ domain_id: 'server_manufacturing', name: 'Server Manufacturing' }],
      });
    }
    if (path === '/api/iacc/health') {
      return json({ status: 'ready', capabilities: ['cockpit_report_delivery_retry_state'] });
    }
    if (path === '/api/iacc/cockpit/reports/cockpit-report-browser') return json({ report_id: 'cockpit-report-browser', status: 'ready' });
    if (path === '/api/iacc/cockpit/reports/cockpit-report-browser/delivery-state') return json({ status: 'ready', attempts: [] });
    if (path === '/api/cowd/structured/sources') {
      return json({ list_status: 'ready', count: 1, items: [{ source_id: 'pack-webui-e2e', source_name: 'ERP', owner: 'ops', mappings: [{ mapping_id: 'm1' }] }] });
    }
    if (path === '/api/cowd/structured/facts') {
      return json({ list_status: 'ready', count: 1, items: [{ fact_id: 'fact-webui-e2e', fact_type: 'inventory_balance', metric_key: 'stock_on_hand', confidence: 0.93 }] });
    }
    if (path === '/api/cowd/structured/evidence') {
      return json({ list_status: 'ready', count: 1, items: [{ evidence_id: 'evidence-webui-e2e', problem_statement: 'Inventory balance browser proof', confidence: 0.82, source_refs: [{ reference: 'iacc:fact:fact-webui-e2e' }] }] });
    }
    if (path === '/api/cowd/structured/watermarks') {
      return json({ list_status: 'ready', count: 1, items: [{ source_ref: 'pack-webui-e2e', fact_type: 'inventory_balance', high_watermark: '2026-06-14T00:00:00Z' }] });
    }
    if (path === '/api/cowd/structured/ingest-plan') {
      ingestBody = request.postDataJSON();
      return json({ batch_id: 'batch-webui-e2e', source_ref: ingestBody.source_ref, fact_type: ingestBody.fact_type });
    }
    return json({});
  });

  await page.goto(`${baseURL}/index.html`, { waitUntil: 'domcontentloaded' });
  await page.getByText('Cowd IACC Browser').click();
  await page.click('#btn-toggle-panel');

  await page.click('#panel-tabs button[data-panel="runtime"]');
  await expect(page.locator('#panel-content')).toContainText('Cowd Kernel');
  await expect(page.locator('#panel-content')).toContainText('parity');
  await expect(page.locator('#panel-content')).toContainText('Structured fact captured');
  await expect(page.locator('#panel-content')).toContainText('outcome');
  const runtimeShot = await page.locator('#right-panel').screenshot();
  expect(runtimeShot.length).toBeGreaterThan(10_000);

  await page.click('#panel-tabs button[data-panel="iacc"]');
  await expect(page.locator('#panel-content')).toContainText('IACC Workbench');
  await expect(page.locator('#panel-content')).toContainText('iacc.manufacturing');
  await expect(page.locator('#panel-content')).toContainText('Structured Data');
  await expect(page.locator('#panel-content')).toContainText('pack-webui-e2e');
  await expect(page.locator('#panel-content')).toContainText('fact-webui-e2e');
  await expect(page.locator('#panel-content')).toContainText('evidence-webui-e2e');
  const iaccShot = await page.locator('#right-panel').screenshot();
  expect(iaccShot.length).toBeGreaterThan(10_000);

  const inputs = page.locator('.structured-ingest-form input');
  await inputs.nth(0).fill('pack-webui-e2e');
  await inputs.nth(1).fill('inventory_balance');
  await inputs.nth(2).fill('2026-W30');
  await inputs.nth(3).fill('2026-06-14T00:00:00Z');
  await page.locator('.structured-ingest-form button').click();
  await expect(page.locator('.structured-ingest-result')).toContainText('planned batch-webui-e2e');
  expect(ingestBody).toMatchObject({ source_ref: 'pack-webui-e2e', fact_type: 'inventory_balance' });

  expect(errors).toEqual([]);
});
