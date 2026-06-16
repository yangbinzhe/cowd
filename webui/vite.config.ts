import { defineConfig } from 'vite';
import vue from '@vitejs/plugin-vue';

export default defineConfig({
  plugins: [vue()],
  root: '.',
  base: './',
  build: {
    outDir: 'dist',
    assetsDir: 'assets/app',
    rollupOptions: {
      input: 'index.dev.html',
      output: {
        entryFileNames: 'assets/app/[name]-[hash].js',
        chunkFileNames: 'assets/app/[name]-[hash].js',
        assetFileNames: 'assets/app/[name]-[hash][extname]',
      },
    },
  },
  server: {
    host: '127.0.0.1',
    port: 5173,
    proxy: {
      '/api': 'http://127.0.0.1:8642',
      '/healthz': 'http://127.0.0.1:8642',
      '/readyz': 'http://127.0.0.1:8642',
    },
  },
});
