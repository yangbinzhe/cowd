const CACHE = 'cowd-v1';
const ASSETS = [
  '/',
  'index.html',
  'style.css',
  'api.js',
  'ui.js',
  'messages.js',
  'sessions.js',
  'workspace.js',
  'panels.js',
  'commands.js',
  'boot.js',
  'assets/logo.svg',
  'assets/favicon.svg'
];

self.addEventListener('install', e => {
  e.waitUntil(
    caches.open(CACHE).then(c => c.addAll(ASSETS))
  );
});

self.addEventListener('fetch', e => {
  if (e.request.method !== 'GET') return;
  e.respondWith(
    caches.match(e.request).then(r => r || fetch(e.request))
  );
});

self.addEventListener('activate', e => {
  e.waitUntil(
    caches.keys().then(keys =>
      Promise.all(keys.filter(k => k !== CACHE).map(k => caches.delete(k)))
    )
  );
});
