import { test, expect } from '@playwright/test';

if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
  test.use({
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    },
  });
}

test('connector console renders degraded services and executes mock service', async ({ page, baseURL }) => {
  const errors = [];
  let mockExecuted = false;

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
    if (path === '/api/config') return json({ model: 'claude-sonnet-4-6', version: '0.9.37' });
    if (path === '/api/sessions') {
      return json({ sessions: [{ id: 'session-connector-1', title: 'Connector Console', model: 'claude-sonnet-4-6' }], total: 1 });
    }
    if (path === '/api/sessions/session-connector-1/messages') return json({ messages: [] });
    if (path === '/api/sessions/session-connector-1/stream') {
      return route.fulfill({ status: 200, contentType: 'text/event-stream', body: 'data: {"type":"Connected"}\n\n' });
    }
    if (path === '/api/cross-plane/summary') return json({ identity_bindings: {}, grants: {}, interop: {} });
    if (path === '/api/cross-plane/identities') return json({ identities: [] });
    if (path === '/api/cross-plane/grants') return json({ grants: [] });
    if (path === '/api/cross-plane/audit') return json({ records: [] });
    if (path === '/api/cross-plane/action/adapters') return json({ capabilities: [] });
    if (path === '/api/cross-plane/action/executions') {
      return json({
        executions: [{
          status: 'dry_run',
          mode: 'dry_run',
          dispatch_status: 'not_dispatched',
          idempotency_key: 'idem-browser',
          action: { requested_capability: 'service.mock.docs.read' },
        }],
      });
    }
    if (path === '/api/connectors/summary') {
      return json({ summary: { accounts: 2, capabilities: 3, resources: 1, degraded: true } });
    }
    if (path === '/api/connectors/accounts') {
      return json({
        accounts: [
          { provider: 'feishu', account_id: 'feishu-main', auth_mode: 'app_secret', enabled_bindings: ['service.feishu.docx.read'], health: { status: 'degraded', reason: 'missing required fields: app_secret' } },
          { provider: 'mock', account_id: 'mock.docs', auth_mode: 'none', enabled_bindings: ['service.mock.docs.read'], health: { status: 'ready' } },
        ],
      });
    }
    if (path === '/api/connectors/capabilities') {
      return json({
        capabilities: [
          { capability_id: 'service.mock.docs.read', provider: 'mock', family: 'service.mock.docs', plane: 'service', risk: 'low', supports_commit: true, requires_approval: false },
          { capability_id: 'service.feishu.docx.read', provider: 'feishu', family: 'service.feishu', plane: 'service', risk: 'low', supports_commit: true, requires_approval: false },
        ],
      });
    }
    if (path === '/api/connectors/resources') {
      return json({
        kind: 'connector_resources',
        status: 'available',
        resources: [{
          reference: mockExecuted ? 'service://mock.docs/document/webui-doc' : 'service://feishu/docx/doccn-console',
          provider: mockExecuted ? 'mock.docs' : 'feishu',
          resource_type: mockExecuted ? 'document' : 'docx',
          title: mockExecuted ? 'WebUI Mock Doc' : 'Console Feishu Doc',
          source: 'connector_runtime',
          indexed_state: 'indexed',
        }],
      });
    }
    if (path === '/api/connectors/services/mock.docs/tools') {
      return json({
        service: { id: 'mock.docs', display_name: 'Mock Docs' },
        tools: [{ capability_id: 'service.mock.docs.read', provider: 'mock', plane: 'service', risk: 'low', supports_commit: true, requires_approval: false }],
      });
    }
    if (path === '/api/connectors/services/feishu.readonly/tools') {
      return json({
        service: { id: 'feishu.readonly', display_name: 'Feishu Read-only' },
        health: { status: 'degraded', reason: 'no ready provider account for feishu' },
        tools: [{ capability_id: 'service.feishu.docx.read', provider: 'feishu', plane: 'service', risk: 'low', supports_commit: true, requires_approval: false }],
      });
    }
    if (path === '/api/connectors/services/mock.docs/execute') {
      mockExecuted = true;
      const body = request.postDataJSON();
      return json({
        result: { status: 'ok', resource: { reference: 'service://mock.docs/document/' + body.resource_id } },
        receipt: { status: 'dry_run', dispatch_status: 'not_dispatched', blockers: [], action: { requested_capability: body.tool_id } },
        resource_persisted: true,
      });
    }
    if (path === '/api/channels/wechat-ilink/accounts') return json({ accounts: [] });
    if (path === '/api/platforms') return json([]);
    return json({});
  });

  await page.goto(`${baseURL}/index.html`, { waitUntil: 'domcontentloaded' });
  await page.click('#btn-toggle-panel');
  await page.click('#panel-tabs button[data-panel="gateway"]');

  await expect(page.locator('.connector-console')).toContainText('Connector Console');
  await expect(page.locator('.connector-console')).toContainText('service.feishu.docx.read');
  await expect(page.locator('.connector-console')).toContainText('no ready provider account for feishu');
  await expect(page.locator('.connector-console')).toContainText('Console Feishu Doc');

  await page.locator('.connector-service-executor').first().getByRole('button', { name: 'Run service' }).click();
  await expect(page.locator('.connector-console')).toContainText('Service Result');
  await expect(page.locator('.connector-console')).toContainText('service://mock.docs/document/webui-doc');
  await expect(page.locator('.connector-console')).toContainText('WebUI Mock Doc');

  expect(errors).toEqual([]);
});
