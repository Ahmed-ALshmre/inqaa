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
    const previousPath = sessionStorage.getItem('soof.previousPath');
    if (history.length > 1 && document.referrer.startsWith(location.origin)) history.back();
    else if (previousPath && previousPath !== location.pathname + location.search) location.href = previousPath;
    else location.href = '/dashboard';
  };

  document.addEventListener('click', function (event) {
    const link = event.target.closest('a[href^="/"]');
    if (!link || link.target || event.ctrlKey || event.metaKey || event.shiftKey) return;
    sessionStorage.setItem('soof.previousPath', location.pathname + location.search);
    document.documentElement.classList.add('app-navigating');
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
