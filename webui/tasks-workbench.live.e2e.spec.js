import { test, expect } from '@playwright/test';

if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
  test.use({
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    },
  });
}

test('live gateway workbench renders persisted task phase state', async ({ page, baseURL }) => {
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

    const task = await json('/api/tasks/start', {
      objective: 'Live WebUI workbench enterprise scenario',
      yolo_mode: true,
    });
    const withPhase = await json(`/api/tasks/${encodeURIComponent(task.id)}/phases`, {
      name: 'live-browser-gate',
      objective: 'Validate live gateway, UI, API, and persistence together',
      plan: ['Start real gateway in tmux', 'Drive browser against served WebUI'],
      acceptance: ['Task phase appears in agents panel', 'Review result is visible'],
      test_commands: ['scripts/webui_live_workbench_scenario.sh'],
    });
    const phase = withPhase.phases.at(-1);
    await json(`/api/tasks/${encodeURIComponent(task.id)}/phases/${encodeURIComponent(phase.id)}/artifacts`, {
      kind: 'test',
      label: 'live-playwright',
      value: 'real gateway rendered persisted task phase',
    });
    await json(`/api/tasks/${encodeURIComponent(task.id)}/phases/${encodeURIComponent(phase.id)}/review`, {
      result: 'accepted by live browser scenario',
      completed: true,
    });
    return { taskId: task.id, phaseId: phase.id };
  });

  await page.click('#btn-toggle-panel');
  await page.click('[data-panel="agents"]');
  await expect(page.locator('#panel-content')).toContainText('Live WebUI workbench enterprise scenario');
  await expect(page.locator('#panel-content')).toContainText('live-browser-gate');
  await expect(page.locator('#panel-content')).toContainText('Task phase appears in agents panel');
  await expect(page.locator('#panel-content')).toContainText('live-playwright');
  await expect(page.locator('#panel-content')).toContainText('accepted by live browser scenario');

  await page.click('[data-panel="memory"]');
  await expect(page.locator('#panel-content')).toContainText('disabled');

  expect(created.taskId).toBeTruthy();
  expect(created.phaseId).toBeTruthy();
  expect(errors).toEqual([]);
});
