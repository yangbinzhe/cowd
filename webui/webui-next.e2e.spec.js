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
  await expect(page.locator('.status-strip')).toContainText('offline');
});

test('workspace tab supports folder browsing and editable preview surface', async ({ page }) => {
  await page.goto('/index.html#/chat');
  await page.getByRole('button', { name: 'Workspace' }).click();
  await expect(page.locator('.workspace-root')).toBeVisible();
  await expect(page.locator('.breadcrumbs')).toBeVisible();
  await expect(page.getByRole('button', { name: /Parent folder/ })).toBeVisible();
  await expect(page.locator('.file-row')).toHaveCount(0);
  await expect(page.locator('.preview-pane')).toHaveCount(0);
});

test('capability pages include visual charts instead of numeric-only cards', async ({ page }) => {
  await page.goto('/index.html#/memory');
  await expect(page.locator('.session-sidebar')).toHaveCount(0);
  await expect(page.locator('.capability-sidebar')).toBeVisible();
  await expect(page.locator('.section-row')).toHaveCount(0);
  await expect(page.locator('.metric-card')).toHaveCount(3);
  await expect(page.locator('.chart-panel canvas, .chart-panel svg, .chart-panel div').first()).toBeVisible();
  await expect(page.locator('.work-table table')).toBeVisible();
  await expect(page.locator('.action-button')).toHaveCount(0);
  await expect(page.locator('.status-badge')).toContainText(['offline']);
  await expect(page.locator('.action-result').first()).toBeVisible();
});

test('settings page is reachable and theme control is usable', async ({ page }) => {
  await page.goto('/index.html#/settings');
  await expect(page.getByRole('heading', { name: 'Settings' })).toBeVisible();
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
  await page.getByRole('button', { name: /Current workspace|dev-iacc/ }).click();
  await expect(page.locator('.companion-tabs button.active')).toContainText('Workspace');

  await page.getByRole('button', { name: /Commands/ }).click();
  await expect(page.getByRole('heading', { name: 'Commands' })).toBeVisible();
  await expect(page.locator('.command-modal')).toContainText('后端未报告 command registry');
});
