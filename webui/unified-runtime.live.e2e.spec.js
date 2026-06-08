import { test, expect } from '@playwright/test';

if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
  test.use({
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    },
  });
}

test('live unified runtime keeps TUI WebUI and daemon state coherent', async ({ page, baseURL }) => {
  const sessionId = process.env.COWD_V0964_SESSION_ID;
  const taskObjective = process.env.COWD_V0964_TASK_OBJECTIVE;
  if (!sessionId) throw new Error('COWD_V0964_SESSION_ID is required');
  if (!taskObjective) throw new Error('COWD_V0964_TASK_OBJECTIVE is required');

  const errors = [];
  page.on('pageerror', err => errors.push('pageerror: ' + err.message));
  page.on('console', msg => {
    if (msg.type() !== 'error') return;
    const text = msg.text();
    if (text.includes('Failed to load resource')) return;
    if (text.includes('Failed to find a valid digest')) return;
    errors.push('console: ' + text);
  });

  await page.goto(`${baseURL}/index.html`, { waitUntil: 'domcontentloaded' });
  await expect(page.locator('#login-modal')).toHaveClass(/hidden/);

  const created = await page.evaluate(async ({ sessionId, taskObjective }) => {
    window.Api.sid = sessionId;

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

    await json('/api/runtime/session-leases/acquire', {
      session_id: sessionId,
      owner: 'webui:v0964-live',
      mode: 'collaborative',
    });

    const task = await json('/api/tasks/start', {
      objective: taskObjective,
      yolo_mode: true,
    });
    const withPhase = await json(`/api/tasks/${encodeURIComponent(task.id)}/phases`, {
      name: 'v0964-real-unified-gate',
      objective: 'Verify TUI, WebUI, daemon, task, runtime and persistence without mocks',
      plan: ['Start real daemon gateway', 'Attach TUI over socket', 'Drive WebUI against live API'],
      acceptance: ['same session lease is visible', 'task phase is visible', 'runtime control plane is visible'],
      test_commands: ['scripts/v0964_unified_runtime_surface_scenario.sh'],
    });
    const phase = withPhase.phases.at(-1);
    await json(`/api/tasks/${encodeURIComponent(task.id)}/phases/${encodeURIComponent(phase.id)}/artifacts`, {
      kind: 'e2e',
      label: 'live-unified-playwright',
      value: 'browser drove the real daemon API and WebUI',
    });
    await json(`/api/tasks/${encodeURIComponent(task.id)}/phases/${encodeURIComponent(phase.id)}/review`, {
      result: 'accepted by v0.9.64 real unified scenario',
      completed: true,
    });

    const controlPlane = await getJson('/api/runtime/control-plane');
    const leases = await getJson('/api/runtime/session-leases');
    const tasks = await getJson('/api/tasks');
    const memory = await getJson('/api/memory/status');
    const connectors = await getJson('/api/connectors/summary');
    return {
      taskId: task.id,
      phaseId: phase.id,
      controlPlane,
      leases,
      tasks,
      memory,
      connectors,
    };
  }, { sessionId, taskObjective });

  expect(JSON.stringify(created.leases)).toContain(sessionId);
  expect(JSON.stringify(created.leases)).toContain('webui:v0964-live');
  expect(JSON.stringify(created.tasks)).toContain(taskObjective);
  expect(JSON.stringify(created.controlPlane)).toContain('runtime_control_plane');
  expect(JSON.stringify(created.memory)).toContain('status');
  expect(JSON.stringify(created.connectors)).toContain('connector_summary');

  await page.click('#btn-toggle-panel');

  await page.click('[data-panel="runtime"]');
  await expect(page.locator('#panel-content')).toContainText('Runtime Console');
  await expect(page.locator('#panel-content')).toContainText('Session Leases');
  await expect(page.locator('#panel-content')).toContainText(sessionId);
  await expect(page.locator('#panel-content')).toContainText('webui:v0964-live');

  await page.click('[data-panel="agents"]');
  await expect(page.locator('#panel-content')).toContainText('Task Registry');
  await expect(page.locator('#panel-content')).toContainText(taskObjective);
  await expect(page.locator('#panel-content')).toContainText('v0964-real-unified-gate');
  await expect(page.locator('#panel-content')).toContainText('live-unified-playwright');
  await expect(page.locator('#panel-content')).toContainText('accepted by v0.9.64 real unified scenario');

  await page.click('[data-panel="memory"]');
  await expect(page.locator('#panel-content')).toContainText(/ready|available|disabled|degraded/i);

  await page.click('[data-panel="gateway"]');
  await expect(page.locator('#panel-content')).toContainText('Connector Console');

  expect(created.taskId).toBeTruthy();
  expect(created.phaseId).toBeTruthy();
  expect(errors).toEqual([]);
});
