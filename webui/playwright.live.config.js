import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: '.',
  testMatch: ['*.live.e2e.spec.js'],
  use: {
    baseURL: process.env.COWD_WEBUI_BASE_URL || 'http://127.0.0.1:18669',
    serviceWorkers: 'block',
  },
});
