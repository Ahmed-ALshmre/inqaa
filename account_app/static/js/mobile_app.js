/* Shared phone shell: installation, portrait mode and keyboard-safe sizing. */
(() => {
  'use strict';
  const root = document.documentElement;
  const handheld = root.classList.contains('app-handheld');
  const standalone = matchMedia('(display-mode: standalone)');
  const isInstalled = () => standalone.matches || navigator.standalone === true;
  let installedThisSession = false;
  let deferredPrompt = null;
  let installing = false;
  let viewportFrame = 0;
  let lockPending = false;
  const read = key => { try { return localStorage.getItem(key); } catch (_) { return null; } };
  const write = (key, value) => { try { localStorage.setItem(key, value); } catch (_) {} };

  function updateViewport() {
    cancelAnimationFrame(viewportFrame);
    viewportFrame = requestAnimationFrame(() => {
      const viewport = window.visualViewport;
      const height = viewport ? viewport.height : window.innerHeight;
      root.style.setProperty('--app-viewport-height', `${Math.round(height)}px`);
      const focused = document.activeElement;
      const editing = focused?.matches('input:not([type=checkbox]):not([type=radio]),textarea,[contenteditable=true]');
      const keyboardOpen = Boolean(editing && viewport && window.innerHeight - viewport.height > 120);
      document.body?.classList.toggle('app-keyboard-open', keyboardOpen);
      // The physical screen orientation is unaffected by the soft keyboard.
      const type = screen.orientation?.type;
      const landscape = type ? type.startsWith('landscape') :
        (typeof window.orientation === 'number' ? Math.abs(window.orientation) === 90 : screen.width > screen.height);
      root.classList.toggle('app-landscape', handheld && landscape);
      updateGateAccess();
    });
  }

  function updateGateAccess() {
    const gate = root.classList.contains('app-desktop') ? '.app-device-gate' :
      root.classList.contains('app-landscape') ? '.app-orientation-gate' : null;
    for (const element of document.body?.children || []) {
      if (element.matches('script,style')) continue;
      if (gate && !element.matches(gate)) {
        // Mark only the nodes we changed, preserving menu/dialog inert states.
        if (!element.inert) { element.inert = true; element.dataset.appGateInert = 'true'; }
      } else if (element.dataset.appGateInert) {
        element.inert = false; delete element.dataset.appGateInert;
      }
    }
  }

  async function lockPortrait() {
    if (!handheld || !isInstalled() || !screen.orientation?.lock || lockPending || document.hidden) return;
    lockPending = true;
    try { await screen.orientation.lock('portrait-primary'); } catch (_) {
      // Safari and some browser contexts reject the lock. The portrait gate is
      // the fallback; never force fullscreen or rotate the DOM and its inputs.
    } finally { lockPending = false; updateViewport(); }
  }

  function updateInstallUI() {
    const installed = isInstalled() || installedThisSession;
    root.classList.toggle('app-installed', installed);
    document.querySelectorAll('[data-app-install],#pwaInstallBtn').forEach(button => {
      button.hidden = !handheld || installed;
    });
    const banner = document.querySelector('.app-install-card');
    if (banner) banner.hidden = !handheld || installed || Number(read('soof.installDismissedUntil')) > Date.now();
    const native = document.querySelector('[data-app-native-install]');
    if (native) native.hidden = !deferredPrompt || installed;
  }

  function installSteps() {
    if (deferredPrompt) return ['اضغط «تثبيت صوف الآن».', 'وافق على التثبيت من نافذة المتصفح، ثم افتح صوف من الشاشة الرئيسية.'];
    const ios = /iPhone|iPad|iPod/.test(navigator.userAgent) || (navigator.platform === 'MacIntel' && navigator.maxTouchPoints > 0);
    if (ios) return ['افتح رابط صوف في Safari ثم اضغط زر المشاركة.', 'اختر «إضافة إلى الشاشة الرئيسية»، ثم فعّل «فتح كتطبيق ويب» إن ظهر، واضغط «إضافة».'];
    return ['افتح رابط صوف في Chrome أو متصفح هاتف يدعم التثبيت.', 'من قائمة المتصفح ⋮ اختر «تثبيت التطبيق» أو «إضافة إلى الشاشة الرئيسية»، ثم أكّد الإضافة.'];
  }

  function refreshInstallDialog() {
    const steps = document.querySelector('[data-app-install-steps]');
    if (steps) steps.replaceChildren(...installSteps().map(text => {
      const item = document.createElement('li'); item.textContent = text; return item;
    }));
    updateInstallUI();
  }

  window.installPWA = function () {
    if (!handheld || isInstalled() || installedThisSession) return;
    refreshInstallDialog();
    const dialog = document.getElementById('appInstallDialog');
    if (dialog && !dialog.open) dialog.showModal();
  };

  async function nativeInstall() {
    if (!deferredPrompt || installing) return;
    const prompt = deferredPrompt;
    deferredPrompt = null; installing = true;
    const button = document.querySelector('[data-app-native-install]');
    if (button) button.disabled = true;
    const status = document.querySelector('[data-app-install-status]');
    try {
      await prompt.prompt();
      const choice = await prompt.userChoice;
      if (choice.outcome === 'accepted') {
        installedThisSession = true;
        document.getElementById('appInstallDialog')?.close();
      } else if (status) status.textContent = 'يمكنك التثبيت لاحقًا من زر التثبيت أو من قائمة المتصفح.';
    } catch (_) {
      if (status) status.textContent = 'تعذّر فتح نافذة التثبيت. اتبع الخطوات من قائمة المتصفح.';
    } finally {
      installing = false;
      if (button) button.disabled = false;
      refreshInstallDialog();
    }
  }

  window.addEventListener('beforeinstallprompt', event => {
    event.preventDefault(); // Suppress desktop promotion, too.
    if (!handheld || isInstalled()) return;
    deferredPrompt = event;
    refreshInstallDialog();
  });
  window.addEventListener('appinstalled', () => {
    deferredPrompt = null; installedThisSession = true;
    document.getElementById('appInstallDialog')?.close();
    updateInstallUI();
  });
  standalone.addEventListener('change', () => { updateInstallUI(); lockPortrait(); });

  window.appGoBack = function () {
    let previousPath;
    try { previousPath = sessionStorage.getItem('soof.previousPath'); } catch (_) {}
    if (history.length > 1 && document.referrer.startsWith(location.origin)) history.back();
    else if (previousPath?.startsWith('/') && !previousPath.startsWith('//') && previousPath !== location.pathname + location.search) location.href = previousPath;
    else location.href = '/dashboard';
  };

  document.addEventListener('click', async event => {
    if (event.target.closest('[data-app-install]')) window.installPWA();
    if (event.target.closest('[data-app-native-install]')) nativeInstall();
    if (event.target.closest('[data-app-close-install]')) document.getElementById('appInstallDialog')?.close();
    if (event.target.closest('[data-app-dismiss-install]')) {
      write('soof.installDismissedUntil', String(Date.now() + 7 * 86400000)); updateInstallUI();
    }
    if (event.target.closest('[data-app-copy-link]')) {
      const status = document.querySelector('[data-app-copy-status]');
      try { await navigator.clipboard.writeText(location.origin + '/'); status.textContent = 'تم نسخ الرابط. أرسله إلى موبايلك وافتحه هناك.'; }
      catch (_) { status.textContent = 'رابط التطبيق: ' + location.origin + '/'; }
    }
    const link = event.target.closest('a[href^="/"]');
    if (!link || link.target || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey || event.defaultPrevented) return;
    try { sessionStorage.setItem('soof.previousPath', location.pathname + location.search); } catch (_) {}
  });

  function init() {
    document.querySelectorAll('[data-app-tab]').forEach(link => {
      const path = link.dataset.appTab;
      if (location.pathname === path || (path === '/settings' && location.pathname.startsWith('/settings/'))) link.setAttribute('aria-current', 'page');
    });
    const topbar = document.querySelector('.admin-topbar,.navbar-custom');
    if (topbar) {
      const button = document.createElement('button');
      button.type = 'button'; button.className = 'app-install-topbar';
      button.dataset.appInstall = ''; button.setAttribute('aria-label', 'تثبيت صوف على الموبايل');
      button.innerHTML = '<i class="bi bi-phone" aria-hidden="true"></i><span>تثبيت</span>';
      topbar.append(button);
    }
    const target = document.querySelector('.inbox-heading,.login-card,.orders-header,.products-layout,.admin-main');
    if (target && !document.body.classList.contains('advisor-workspace')) {
      const card = document.createElement('section'); card.className = 'app-install-card';
      card.setAttribute('aria-label', 'تثبيت تطبيق صوف');
      card.innerHTML = '<span class="app-install-symbol"><i class="bi bi-phone" aria-hidden="true"></i></span><div><strong>صوف أقرب إلك</strong><p>خلّيه على شاشة موبايلك</p></div><button type="button" class="app-install-cta" data-app-install>تثبيت <i class="bi bi-download" aria-hidden="true"></i></button><button type="button" class="app-install-dismiss" data-app-dismiss-install aria-label="إخفاء اقتراح التثبيت لأسبوع"><i class="bi bi-x" aria-hidden="true"></i></button>';
      if (target.matches('.inbox-heading,.orders-header')) target.after(card);
      else target.prepend(card);
    }
    // Keep every page's existing actions accessible in a tidy expandable tray.
    const actions = document.querySelector('.orders-page .nav-actions,.products-page .nav-actions');
    if (actions && topbar) {
      actions.id = 'appPageActions';
      const more = document.createElement('button'); more.type = 'button'; more.className = 'app-page-more';
      more.setAttribute('aria-label', 'إجراءات الصفحة'); more.setAttribute('aria-controls', actions.id); more.setAttribute('aria-expanded', 'false');
      more.innerHTML = '<i class="bi bi-three-dots" aria-hidden="true"></i>';
      const close = () => { actions.classList.remove('app-actions-open'); more.setAttribute('aria-expanded', 'false'); };
      more.addEventListener('click', () => {
        const open = actions.classList.toggle('app-actions-open'); more.setAttribute('aria-expanded', String(open));
      });
      topbar.append(more);
      document.addEventListener('click', event => { if (!actions.contains(event.target) && !more.contains(event.target)) close(); });
      document.addEventListener('keydown', event => { if (event.key === 'Escape') { close(); if (actions.contains(document.activeElement)) more.focus(); } });
    }
    document.getElementById('appInstallDialog')?.addEventListener('click', event => {
      if (event.target.id !== 'appInstallDialog') return;
      const rect = event.target.getBoundingClientRect();
      if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) event.target.close();
    });
    // Off-canvas admin navigation should never be keyboard-focusable when shut.
    const sidebar = document.getElementById('adminSidebar');
    if (sidebar && handheld) {
      const sync = () => { sidebar.inert = !sidebar.classList.contains('open'); };
      new MutationObserver(sync).observe(sidebar, {attributes:true,attributeFilter:['class']}); sync();
    }
    updateInstallUI(); updateViewport(); lockPortrait();
    if (handheld && 'serviceWorker' in navigator && window.isSecureContext) {
      navigator.serviceWorker.register('/sw.js', {updateViaCache:'none'}).catch(() => {});
    }
  }
  window.addEventListener('pageshow', () => { root.classList.remove('app-navigating'); updateViewport(); updateInstallUI(); lockPortrait(); });
  window.addEventListener('resize', updateViewport, {passive:true});
  window.visualViewport?.addEventListener('resize', updateViewport, {passive:true});
  window.visualViewport?.addEventListener('scroll', updateViewport, {passive:true});
  screen.orientation?.addEventListener('change', () => { updateViewport(); lockPortrait(); });
  window.addEventListener('orientationchange', updateViewport, {passive:true});
  document.addEventListener('visibilitychange', () => { if (!document.hidden) { updateViewport(); lockPortrait(); } });
  document.addEventListener('focusin', updateViewport);
  document.addEventListener('focusout', updateViewport);
  // A deferred script runs while readyState is interactive, before the page's
  // DOMContentLoaded workspace setup (including the advisor toolbar).
  if (document.readyState !== 'complete') document.addEventListener('DOMContentLoaded', init, {once:true});
  else init();
})();
