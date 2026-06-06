import { test, expect } from '@playwright/test';

if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
  test.use({
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    },
  });
}

test('unauthenticated login keeps main controls interactive', async ({ page }) => {
  const sessionCalls = [];
  const errors = [];

  page.on('pageerror', err => errors.push('pageerror: ' + err.message));
  page.on('console', msg => {
    if (msg.type() !== 'error') return;
    const text = msg.text();
    if (text.includes('Failed to load resource')) return;
    if (text.includes('Failed to find a valid digest')) return;
    errors.push('console: ' + text);
  });

  await page.route('**/api/auth/verify', route => route.fulfill({ status: 401, body: 'unauthorized' }));
  await page.route('**/api/auth/login', route =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ success: true, token: 'browser-ok' }),
    })
  );
  await page.route('**/api/config', route =>
    route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ model: 'browser-model' }),
    })
  );
  await page.route('**/api/sessions**', route => {
    sessionCalls.push({
      method: route.request().method(),
      url: route.request().url(),
      body: route.request().postData(),
    });
    if (route.request().method() === 'POST') {
      return route.fulfill({
        status: 200,
        contentType: 'application/json',
        body: JSON.stringify({ id: 'browser-session-1', session_id: 'browser-session-1' }),
      });
    }
    return route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ sessions: [], total: 0, offset: 0, limit: 0 }),
    });
  });
  await page.route('https://cdn.jsdelivr.net/**', route => route.fulfill({ status: 204, body: '' }));

  await page.goto('http://127.0.0.1:9241/index.html', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('#login-modal')).not.toHaveClass(/hidden/);

  await page.fill('#login-token', 'test-token');
  await page.click('#btn-login');
  await expect(page.locator('#login-modal')).toHaveClass(/hidden/);

  await page.click('#btn-new-session');
  await page.waitForFunction(() => window.Api?.sid === 'browser-session-1');

  await page.click('#btn-toggle-panel');
  await expect(page.locator('#right-panel')).not.toHaveClass(/hidden/);

  expect(errors).toEqual([]);
  expect(sessionCalls.some(call => call.method === 'POST')).toBe(true);
});
