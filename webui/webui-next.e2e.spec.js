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
  await expect(page.locator('.turn[data-role="user"]')).toBeVisible();
});

test('workspace tab supports folder browsing and editable preview surface', async ({ page }) => {
  await page.goto('/index.html#/chat');
  await page.getByRole('button', { name: 'Workspace' }).click();
  await expect(page.locator('.workspace-root')).toBeVisible();
  await expect(page.locator('.breadcrumbs')).toBeVisible();
  await expect(page.getByRole('button', { name: /Parent folder/ })).toBeVisible();
  await page.locator('.file-row').filter({ hasText: 'README.md' }).first().click();
  await expect(page.locator('.render-preview')).toBeVisible();
  await page.getByRole('button', { name: 'Workspace' }).click();
  await page.locator('.file-row').filter({ hasText: 'Cargo.toml' }).first().click();
  await expect(page.locator('.preview-pane textarea')).toBeVisible();
});

test('capability pages include visual charts instead of numeric-only cards', async ({ page }) => {
  await page.goto('/index.html#/memory');
  await expect(page.locator('.session-sidebar')).toHaveCount(0);
  await expect(page.locator('.capability-sidebar')).toBeVisible();
  await expect(page.locator('.section-row')).toHaveCount(4);
  await expect(page.locator('.metric-card')).toHaveCount(3);
  await expect(page.locator('.chart-panel canvas, .chart-panel svg, .chart-panel div').first()).toBeVisible();
  await expect(page.locator('.work-table table')).toBeVisible();
  await expect(page.locator('.action-button')).toHaveCount(3);
});

test('settings page is reachable and theme control is usable', async ({ page }) => {
  await page.goto('/index.html#/settings');
  await expect(page.getByRole('heading', { name: 'Settings' })).toBeVisible();
  await page.getByRole('button', { name: 'Light' }).click();
  await expect(page.locator('html')).toHaveAttribute('data-theme', 'light');
});

test('composer model workspace and command controls are clickable', async ({ page }) => {
  await page.goto('/index.html#/chat');
  await page.getByRole('button', { name: 'claude-sonnet-4-6', exact: true }).click();
  await expect(page.getByRole('heading', { name: 'Model and profile' })).toBeVisible();
  await page.getByRole('button', { name: 'qwen3-coder-next', exact: true }).click();
  await expect(page.getByRole('button', { name: 'qwen3-coder-next', exact: true })).toBeVisible();

  await page.getByRole('button', { name: /root/ }).click();
  await expect(page.getByRole('heading', { name: 'Workspace picker' })).toBeVisible();
  await page.getByRole('button', { name: /Current workspace|dev-iacc/ }).click();
  await expect(page.locator('.companion-tabs button.active')).toContainText('Workspace');

  await page.getByRole('button', { name: /Commands/ }).click();
  await expect(page.getByRole('heading', { name: 'Commands' })).toBeVisible();
  await page.locator('.command-row').filter({ hasText: '/status' }).click();
  await expect(page.locator('.composer textarea')).toHaveValue('/status ');
});
