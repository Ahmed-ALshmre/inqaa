(function () {
  let viewportFrame = 0;

  function updateViewport() {
    cancelAnimationFrame(viewportFrame);
    viewportFrame = requestAnimationFrame(function () {
      const viewport = window.visualViewport;
      const height = viewport ? viewport.height : window.innerHeight;
      document.documentElement.style.setProperty('--app-viewport-height', `${Math.round(height)}px`);
      const keyboardOpen = viewport && window.innerHeight - viewport.height > 150;
      document.body?.classList.toggle('app-keyboard-open', Boolean(keyboardOpen));
    });
  }

  window.appGoBack = function () {
    let previousPath;
    try { previousPath = sessionStorage.getItem('soof.previousPath'); } catch {}
    if (history.length > 1 && document.referrer.startsWith(location.origin)) history.back();
    else if (previousPath && previousPath.startsWith('/') && !previousPath.startsWith('//') && previousPath !== location.pathname + location.search) location.href = previousPath;
    else location.href = '/dashboard';
  };

  document.addEventListener('click', function (event) {
    const link = event.target.closest('a[href^="/"]');
    if (!link || event.defaultPrevented || event.button !== 0 || link.target || link.hasAttribute('download') || event.ctrlKey || event.metaKey || event.shiftKey || event.altKey) return;
    const url = new URL(link.href, location.origin);
    if (url.origin !== location.origin || url.pathname.startsWith('/api/') || url.href === location.href || (url.pathname === location.pathname && url.search === location.search && url.hash)) return;
    try { sessionStorage.setItem('soof.previousPath', location.pathname + location.search); } catch {}
    document.documentElement.classList.add('app-navigating');
    setTimeout(() => document.documentElement.classList.remove('app-navigating'), 3000);
  });

  window.addEventListener('pageshow', function () {
    document.documentElement.classList.remove('app-navigating');
    updateViewport();
  });

  window.addEventListener('resize', updateViewport, {passive: true});
  window.visualViewport?.addEventListener('resize', updateViewport, {passive: true});
  window.visualViewport?.addEventListener('scroll', updateViewport, {passive: true});
  document.addEventListener('DOMContentLoaded', updateViewport, {once: true});
})();
