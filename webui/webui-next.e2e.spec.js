import { test, expect } from '@playwright/test';

test('new shell uses icon rail and right Activity/Workspace companion tabs', async ({ page }) => {
  await page.goto('/index.html#/chat');
  await expect(page.locator('.rail-button')).toHaveCount(11);
  await expect(page.locator('.session-sidebar')).toBeVisible();
  await expect(page.locator('.companion-tabs')).toContainText('Activity');
  await expect(page.locator('.companion-tabs')).toContainText('Workspace');
  await expect(page.locator('.rail')).not.toContainText('Workspace');
  await expect(page.locator('.transcript')).toBeVisible();
  await expect(page.locator('.composer textarea')).toBeVisible();
  await expect(page.locator('.turn-role')).toHaveCount(0);
  await expect(page.locator('.status-strip')).toContainText(/local|offline/);
  await expect(page.locator('.status-strip')).toContainText('Select model');
});

test('workspace tab supports folder browsing and editable preview surface', async ({ page }) => {
  await page.goto('/index.html#/chat');
  await page.getByRole('button', { name: 'Workspace' }).click();
  await expect(page.locator('.workspace-root')).toBeVisible();
  await expect(page.locator('.upload-drop')).toContainText('Drop files here');
  await expect(page.getByPlaceholder('New folder')).toBeVisible();
  await expect(page.locator('.breadcrumbs')).toBeVisible();
  await expect(page.getByRole('button', { name: /Parent folder/ })).toBeVisible();
  await expect(page.locator('.file-row')).toHaveCount(0);
  await expect(page.locator('.preview-pane')).toHaveCount(0);
});

test('tools page exposes current-page management without duplicated primary navigation', async ({ page }) => {
  await page.goto('/index.html#/tools');
  await expect(page.locator('.session-sidebar')).toHaveCount(0);
  await expect(page.locator('.capability-sidebar')).toBeVisible();
  await expect(page.locator('.section-row')).toHaveCount(7);
  await expect(page.locator('.capability-sidebar')).not.toContainText('Memory Graph');
  await expect(page.locator('.capability-sidebar')).not.toContainText('Settings');
  await expect(page.locator('.metric-card')).toHaveCount(4);
  await expect(page.getByRole('heading', { name: 'Tool registry' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Execution planner' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Mutation transactions' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Checkpoints' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Tool cache' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Tool ledger' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Risk preflight' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Run readonly batch' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Preview mutation' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Run preflight' })).toBeVisible();
  await expect(page.locator('.raw-payload').first()).toBeVisible();
  await page.locator('.section-row').filter({ hasText: 'Risk' }).click();
  await expect(page.locator('.section-row.active')).toContainText('Risk');
  await expect(page).toHaveURL(/section=risk/);
  await expect(page.getByRole('heading', { name: 'Tool registry' })).toHaveCount(0);
  await expect(page.getByRole('heading', { name: 'Risk preflight' })).toBeVisible();
});

test('runtime and context pages expose real workbench controls', async ({ page }) => {
  await page.goto('/index.html#/runtime');
  await expect(page.getByRole('heading', { name: 'Runtime Control', exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Control plane' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Session lease' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Acquire' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Reload providers' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Runtime timeline' })).toBeVisible();

  await page.goto('/index.html#/context');
  await expect(page.getByRole('heading', { name: 'Context Builder', exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Context builder', exact: true })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Build packet' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Evidence resolve' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Recommendation actions' })).toBeVisible();
});

test('memory page exposes memory and structured-data kernel controls', async ({ page }) => {
  await page.goto('/index.html#/memory');
  await expect(page.getByRole('heading', { name: 'Memory Graph', exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Layer entries' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Search, recall, packet' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Register memory fact' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Structured memory graph' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Maintenance' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Structured data core' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Plan manufacturing ingest' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Scan candidates' })).toBeVisible();
  await expect(page.locator('.memory-sections')).toBeVisible();
});

test('skills agents and tools pages expose lifecycle workbenches', async ({ page }) => {
  await page.goto('/index.html#/skills');
  await expect(page.getByRole('heading', { name: 'Skills Console' })).toBeVisible();
  await expect(page.locator('.skills-catalog')).toBeVisible();
  await expect(page.locator('.filter-row select')).toHaveCount(6);
  await expect(page.getByRole('heading', { name: 'Files' })).toBeVisible();
  await expect(page.locator('.markdown-body, .skill-markdown pre')).toBeVisible();
  await expect(page.getByRole('button', { name: 'Validate' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Runs and governance' })).toBeVisible();

  await page.goto('/index.html#/agents');
  await expect(page.getByRole('heading', { name: 'Agents Workbench' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Agent directory' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Discover team' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Task control' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Assemble team' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Start task' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Phase gate' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Agent execution graph' })).toBeVisible();

  await page.goto('/index.html#/tools');
  await expect(page.getByRole('heading', { name: 'Tool registry' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Tool ledger' })).toBeVisible();
});

test('gateway page exposes connector and cross-plane controls', async ({ page }) => {
  await page.goto('/index.html#/gateway');
  await expect(page.getByRole('heading', { name: 'Platforms and connectors' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Resources and memory promotion' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Cross-plane governance' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Identities and grants' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Action execution' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Simulate policy' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Run preflight' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Create identity' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Create grant' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Execute action' })).toBeVisible();
});

test('mfg page exposes manufacturing application workbench controls', async ({ page }) => {
  await page.goto('/index.html#/apps/mfg');
  await expect(page.getByRole('heading', { name: 'Manufacturing command center' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Data plane and source packs' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Manufacturing data ingestion' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Entities and impact graph' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Metrics and compute' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Evidence and quality' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Incident room' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Analysis, playbook, actions' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Manufacturing skills' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Cockpit reports' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Plan ingest' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Upsert source pack' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Build packet' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Ingest facts' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Create incident' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Generate report' })).toBeVisible();
});

test('audit page exposes usage and release gate governance controls', async ({ page }) => {
  await page.goto('/index.html#/audit');
  await expect(page.getByRole('heading', { name: 'Audit export' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Usage summary' })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Release gate', exact: true })).toBeVisible();
  await expect(page.getByRole('heading', { name: 'Governance evidence' })).toBeVisible();
  await expect(page.getByRole('button', { name: 'Refresh audit' })).toBeVisible();
  await expect(page.getByRole('combobox').filter({ hasText: /webui|tui|cli/ })).toBeVisible();
});

test('settings page is reachable and theme control is usable', async ({ page }) => {
  await page.goto('/index.html#/settings');
  await expect(page.getByRole('heading', { name: 'Settings' })).toBeVisible();
  await expect(page.locator('.capability-sidebar')).toHaveCount(0);
  await page.getByRole('button', { name: 'Light' }).click();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
});

test('composer model workspace and command controls are clickable', async ({ page }) => {
  await page.goto('/index.html#/chat');
  await page.locator('.status-strip button').click();
  await expect(page.getByRole('heading', { name: 'Model and profile' })).toBeVisible();
  await expect(page.locator('.command-modal')).toContainText('后端未报告可切换模型');
  await page.getByRole('button', { name: 'Close' }).click();

  await page.getByRole('button', { name: /root/ }).click();
  await expect(page.getByRole('heading', { name: 'Workspace picker' })).toBeVisible();
  await page.getByRole('button', { name: /Current workspace|dev-mfg/ }).click();
  await expect(page.locator('.companion-tabs button.active')).toContainText('Workspace');

  await page.getByRole('button', { name: /Commands/ }).click();
  await expect(page.getByRole('heading', { name: 'Commands' })).toBeVisible();
  await expect(page.locator('.command-modal')).toContainText('后端未报告 command registry');
});
