import { test, expect } from '@playwright/test';

if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
  test.use({
    launchOptions: {
      executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH,
    },
  });
}

test('workbench panels render durable task and memory status', async ({ page }) => {
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
    if (path === '/api/config') return json({ model: 'claude-sonnet-4-6', version: '0.8.10' });
    if (path === '/api/sessions') {
      return json({
        sessions: [{ id: 'session-1', title: 'Enterprise Task Review', model: 'claude-sonnet-4-6' }],
        total: 1,
        offset: 0,
        limit: 20,
      });
    }
    if (path === '/api/tasks') {
      const current = {
        id: 'task-enterprise-1',
        objective: 'Complete v0.8.10 enterprise AI capability framework',
        status: 'running',
        blocker_reason: null,
        phases: [{
          id: 'phase-browser-1',
          name: 'browser-e2e',
          objective: 'Validate task workbench panel',
          status: 'completed',
          acceptance: ['Task phase is visible'],
          test_commands: ['npm run test:e2e'],
          artifacts: [{ kind: 'test', label: 'playwright', value: '2 passed' }],
          review_result: 'accepted',
        }],
      };
      return json({
        current,
        tasks: [
          current,
          { id: 'task-done-1', objective: 'Session kernel migration', status: 'completed' },
        ],
      });
    }
    if (path === '/api/memory/status') {
      return json({
        enabled: true,
        status: 'degraded',
        degraded: true,
        degraded_reason: 'vector store unavailable; lexical recall active',
        kernel_health: {
          evidence_coverage: 0.75,
          link_coverage: 0.5,
          background_lag_ms: 12,
        },
        total_entries: 42,
      });
    }
    if (path === '/api/memory/stats') {
      return json({ total_entries: 42, entity_count: 7, triple_count: 11 });
    }
    if (path === '/api/memory/entities') return json({ entities: [{ name: 'SessionKernel' }] });
    if (path === '/api/memory/triples') {
      return json({ triples: [{ subject: 'TaskKernel', predicate: 'persists', object: 'tasks' }] });
    }
    if (path === '/api/memory/links') {
      return json({
        links: [{
          from: '11111111-1111-1111-1111-111111111111',
          to: '22222222-2222-2222-2222-222222222222',
          kind: 'Supports',
          weight: 0.86,
          evidence: 'TaskKernel supports SessionKernel decisions',
        }],
      });
    }
    if (path === '/api/memory/packet') {
      return json({
        packet: {
          selected: [{
            role: 'Orientation',
            reason: 'query match',
            atom: {
              title: 'SessionKernel owns durable sessions',
              layer: 'L3',
              category: 'ProjectKnowledge',
              state: 'Active',
            },
          }],
          omitted: [],
          token_estimate: 44,
          truncated: false,
        },
      });
    }
    if (path === '/api/memory/recall/explain') {
      return json({
        mode: 'keyword',
        degraded: false,
        results: [{
          source_layer: 'L3',
          category: 'ProjectKnowledge',
          mode: 'keyword',
          score: 0.91,
          snippet: 'SessionKernel owns durable sessions',
        }],
      });
    }
    if (path === '/api/memory/layers') return json({ layers: [{ name: 'L3' }, { name: 'L4' }] });

    return json({});
  });

  await page.goto('http://127.0.0.1:9241/index.html', { waitUntil: 'domcontentloaded' });
  await expect(page.locator('#login-modal')).toHaveClass(/hidden/);

  await page.click('#btn-toggle-panel');
  await expect(page.locator('#right-panel')).not.toHaveClass(/hidden/);

  await page.click('[data-panel="agents"]');
  await expect(page.locator('#right-panel')).not.toHaveClass(/hidden/);
  await expect(page.locator('#panel-content')).toContainText('Task Registry');
  await expect(page.locator('#panel-content')).toContainText('running');
  await expect(page.locator('#panel-content')).toContainText('Complete v0.8.10 enterprise AI capability framework');
  await expect(page.locator('#panel-content')).toContainText('browser-e2e');
  await expect(page.locator('#panel-content')).toContainText('Task phase is visible');
  await expect(page.locator('#panel-content')).toContainText('playwright');
  await expect(page.locator('#panel-content')).toContainText('accepted');
  await expect(page.locator('#panel-content')).toContainText('Session kernel migration');

  await page.click('[data-panel="memory"]');
  await expect(page.locator('#panel-content')).toContainText('degraded');
  await expect(page.locator('#panel-content')).toContainText('vector store unavailable');
  await expect(page.locator('#panel-content')).toContainText('Memory Links');
  await expect(page.locator('#panel-content')).toContainText('Supports 1');
  await expect(page.locator('#panel-content')).toContainText('SessionKernel');
  await expect(page.locator('#panel-content')).toContainText('TaskKernel');
  await page.fill('input[placeholder="Search memory..."]', 'SessionKernel');
  await expect(page.locator('#panel-content')).toContainText('Context Packet');
  await expect(page.locator('#panel-content')).toContainText('Orientation');
  await expect(page.locator('#panel-content')).toContainText('Recall Explain');
  await expect(page.locator('#panel-content')).toContainText('score 0.91');
  await page.getByRole('button', { name: 'Network' }).click();
  await expect(page.locator('#panel-content')).toContainText('Knowledge Network');
  await expect(page.locator('.memory-network svg')).toBeVisible();
  await expect(page.locator('#panel-content')).toContainText('persists');
  await expect(page.locator('#panel-content')).toContainText('Supports');
  const graph = page.locator('.memory-network');
  await graph.locator('select').selectOption('link');
  await expect(graph).toContainText('Supports');
  await expect(graph).not.toContainText('persists');
  await graph.locator('select').selectOption('all');
  await graph.getByPlaceholder('Filter network...').fill('TaskKernel');
  await expect(graph.locator('.memory-node.matched')).toBeVisible();
  await graph.locator('[data-node-id="TaskKernel"]').click();
  await expect(graph.locator('.memory-network-detail')).toContainText('TaskKernel');

  expect(errors).toEqual([]);
});
