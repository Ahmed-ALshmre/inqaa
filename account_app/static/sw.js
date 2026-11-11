/* Minimal service worker — needed for installability + offline fallback */
const CACHE_NAME = 'soof-mobile-v44';
const APP_SHELL = [
  '/static/css/responsive-workspace.css?v=5',
  '/static/js/voice_composer.js?v=1',
  '/static/css/mobile-shell.css?v=3',
  '/static/js/image_composer.js?v=1',
  '/static/icons/soof-mark.svg',
  '/static/css/theme.css?v=2',
  '/static/css/ui.css?v=2',
  '/static/js/ui.js?v=2',
  '/static/css/advisor.css?v=3',
  '/static/js/advisor.js?v=8',
  '/static/css/dashboard.css?v=25',
  '/static/js/dashboard.js?v=49',
  '/static/css/conversation-workspace.css?v=3',
  '/static/css/mobile-install.css?v=1',
  '/static/js/pwa_install.js?v=2',
  '/static/css/product_questions.css?v=2',
  '/static/js/product_question_editor.js?v=2',
  '/static/js/product_questions_dialog.js?v=2',
  '/static/css/workspace.css?v=1',
  '/static/js/workspace.js?v=3',
  '/static/css/store-ai.css?v=2',
  '/static/js/conversation_tools.js?v=5',
  '/static/css/messenger.css?v=5',
  '/static/js/mobile_app.js?v=5',
  '/static/css/rewards.css?v=4',
  '/static/js/rewards.js?v=5',
  '/static/icons/icon-192.png?v=8',
  '/static/icons/icon-512.png?v=8',
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) =>
      Promise.all(APP_SHELL.map((url) =>
        cache.add(url).catch(() => {})
      ))
    ).then(() => self.skipWaiting())
  );
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE_NAME).map((k) => caches.delete(k)))
    ).then(() => self.clients.claim())
  );
});

self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;
  const url = new URL(req.url);
  // Only cache same-origin static assets; never cache APIs or HTML pages.
  if (url.origin !== self.location.origin) return;
  if (!url.pathname.startsWith('/static/')) return;

  event.respondWith(
    caches.match(req).then((cached) => {
      if (cached) return cached;
      return fetch(req).then((res) => {
        if (res && res.status === 200 && res.type === 'basic') {
          const clone = res.clone();
          caches.open(CACHE_NAME).then((cache) => cache.put(req, clone));
        }
        return res;
      }).catch(() => cached);
    })
  );
});
