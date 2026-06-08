import { test, expect } from '@playwright/test';

if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
  test.use({
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    },
  });
}

test('live connector console preserves the active session across runtime and connector planes', async ({ page, baseURL }) => {
  const errors = [];

  page.on('pageerror', err => errors.push('pageerror: ' + err.message));
  page.on('console', msg => {
    if (msg.type() !== 'error') return;
    const text = msg.text();
    if (text.includes('Failed to load resource')) return;
    if (text.includes('Failed to find a valid digest')) return;
    errors.push('console: ' + text);
  });

  await page.route('https://cdn.jsdelivr.net/**', route => route.fulfill({ status: 204, body: '' }));
  await page.goto(`${baseURL}/index.html`, { waitUntil: 'domcontentloaded' });
  await expect(page.locator('#login-modal')).toHaveClass(/hidden/);

  const created = await page.evaluate(async () => {
    const json = async (path, body) => {
      const response = await fetch(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (!response.ok) throw new Error(`${path} failed: ${response.status} ${await response.text()}`);
      return response.json();
    };

    const getJson = async path => {
      const response = await fetch(path);
      if (!response.ok) throw new Error(`${path} failed: ${response.status} ${await response.text()}`);
      return response.json();
    };

    const session = await json('/api/sessions', { model: 'claude-sonnet-4-6' });
    const sessionId = session.id || session.session_id;
    window.Api.sid = sessionId;

    await json('/api/cross-plane/identities', {
      id: 'idb-v0945-webui',
      principal_id: 'user:v0945-webui',
      identity_ref: 'channel://webui/live/v0945?email=v0945@example.test',
      trust: 'verified',
      source: 'webui-live',
      created_at: '2026-06-08T00:00:00Z',
      expires_at: null,
    });
    await json('/api/cross-plane/grants', {
      id: 'grant-v0945-webui',
      principal_id: 'user:v0945-webui',
      capability: 'service.mock.docs.read',
      account_id: null,
      target_ref: null,
      resource_ref: null,
      source_channel: null,
      grant_type: 'persistent',
      expires_at: null,
      remaining_uses: null,
      created_by: 'webui-live',
      approval_id: null,
    });
    await json('/api/runtime/session-leases/acquire', {
      session_id: sessionId,
      owner: 'webui:v0945-live',
      mode: 'collaborative',
    });
    await json('/api/connectors/services/mock.docs/execute', {
      actor_principal: 'user:v0945-webui',
      actor_identity_ref: 'channel://webui/live/v0945?email=v0945@example.test',
      source_channel: 'local:webui-live',
      session_id: sessionId,
      tool_id: 'service.mock.docs.read',
      resource_id: 'v0945-webui-doc',
      title: 'v0.9.45 WebUI Same Session Doc',
      mode: 'commit',
      idempotency_key: 'v0945-webui-live',
    });

    const controlPlane = await getJson('/api/runtime/control-plane');
    const resources = await getJson('/api/connectors/resources?q=v0945-webui-doc&limit=20&offset=0');
    return { sessionId, controlPlane, resources };
  });

  expect(created.sessionId).toBeTruthy();
  expect(JSON.stringify(created.controlPlane)).toContain(created.sessionId);
  expect(JSON.stringify(created.resources)).toContain('v0945-webui-doc');

  await page.click('#btn-toggle-panel');
  await page.click('[data-panel="runtime"]');
  await expect(page.locator('#panel-content')).toContainText('Runtime Console');
  await expect(page.locator('#panel-content')).toContainText('Session Leases');
  await expect(page.locator('#panel-content')).toContainText(created.sessionId);
  await expect(page.locator('#panel-content')).toContainText('webui:v0945-live');

  await page.click('[data-panel="gateway"]');
  await expect(page.locator('#panel-content')).toContainText('Connector Console');
  await expect(page.locator('#panel-content')).toContainText('v0.9.45 WebUI Same Session Doc');
  await expect(page.locator('#panel-content')).toContainText('service.mock.docs.read');

  expect(errors).toEqual([]);
});
