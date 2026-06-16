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
