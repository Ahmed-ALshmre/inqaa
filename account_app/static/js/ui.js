/* Local presentation helpers: no network requests or changes to message actions. */
(() => {
  document.addEventListener('DOMContentLoaded', () => {
    const search = document.querySelector('#searchInput,#settingsSearch,#productSearch,#orderSearch');
    if (search) {
      search.setAttribute('aria-keyshortcuts', 'Control+K Meta+K');
      const container = search.closest('.settings-search');
      if (container) {
        const hint = document.createElement('span');
        hint.className = 'ui-search-hint';
        hint.setAttribute('aria-hidden', 'true');
        hint.innerHTML = '<kbd>Ctrl</kbd> + <kbd>K</kbd>';
        container.append(hint);
        const empty = document.createElement('p');
        empty.className = 'ui-search-empty'; empty.hidden = true;
        empty.setAttribute('role', 'status');
        empty.textContent = 'ما لقينا إعداد بهذا الاسم. جرّب كلمة ثانية أو امسح البحث.';
        container.after(empty);
        search.addEventListener('input', () => requestAnimationFrame(() => {
          let count = 0;
          document.querySelectorAll('.admin-card-grid').forEach(section => {
            const visible = [...section.querySelectorAll('.admin-link-card')].filter(card => !card.hidden).length;
            count += visible; section.hidden = !visible;
            const heading = section.previousElementSibling;
            if (heading?.classList.contains('workspace-section-label')) heading.hidden = !visible;
          });
          empty.hidden = count > 0;
        }));
      }
    }

    let menuTrigger = null;
    const menus = [
      {panel:document.getElementById('dashboardDrawer'), selector:'.dashboard-drawer-toggle', close:() => window.closeDashboardDrawer?.(), offscreen:true},
      {panel:document.getElementById('adminSidebar'), selector:'.admin-menu-btn,.admin-floating-menu', close:() => window.closeAdminNav?.(), offscreen:false},
    ].filter(menu => menu.panel);
    for (const menu of menus) {
      let wasOpen = false;
      const controls = document.querySelectorAll(menu.selector);
      controls.forEach(button => button.setAttribute('aria-controls', menu.panel.id));
      const update = () => {
        const open = menu.panel.classList.contains('open');
        controls.forEach(button => button.setAttribute('aria-expanded', String(open)));
        // Dashboard drawer is always off canvas when closed, including desktop.
        if (menu.offscreen) { menu.panel.inert = !open; menu.panel.setAttribute('aria-hidden', String(!open)); }
        if (open && !wasOpen) requestAnimationFrame(() => {
          if (menu.panel.classList.contains('open')) menu.panel.querySelector('button,a,select,input')?.focus({preventScroll:true});
        });
        if (!open && wasOpen && menu.panel.contains(document.activeElement)) menuTrigger?.focus({preventScroll:true});
        wasOpen = open;
      };
      new MutationObserver(update).observe(menu.panel, {attributes:true,attributeFilter:['class']});
      update();
    }
    document.addEventListener('click', event => {
      const trigger = event.target.closest('.dashboard-drawer-toggle,.admin-menu-btn,.admin-floating-menu');
      if (trigger) menuTrigger = trigger;
      const dialog = event.target;
      if (dialog instanceof HTMLDialogElement && dialog.open) {
        const rect = dialog.getBoundingClientRect();
        if (event.clientX < rect.left || event.clientX > rect.right || event.clientY < rect.top || event.clientY > rect.bottom) dialog.close();
      }
    });
    document.addEventListener('keydown', event => {
      if ((event.ctrlKey || event.metaKey) && !event.altKey && event.key.toLowerCase() === 'k' && search
          && !document.querySelector('dialog[open],.modal.show') && !search.disabled) {
        event.preventDefault();
        search.focus(); search.select();
      }
      if (event.key === 'Escape' && !document.querySelector('dialog[open],.modal.show')) {
        const openMenus = menus.filter(menu => menu.panel.classList.contains('open'));
        if (openMenus.length) {
          openMenus.forEach(menu => menu.close());
          menuTrigger?.focus({preventScroll:true});
        }
      }
    });
    document.querySelectorAll('.dashboard-drawer a').forEach(link => {
      if (new URL(link.href, location.href).pathname === location.pathname) link.setAttribute('aria-current', 'page');
    });

    const composer = document.getElementById('messageInput');
    if (composer) {
      let frame = 0;
      const fit = () => {
        cancelAnimationFrame(frame);
        frame = requestAnimationFrame(() => {
          if (!composer.getClientRects().length) return;
          composer.style.setProperty('height', 'auto', 'important');
          const minimum = matchMedia('(max-width:768px)').matches ? 64 : 56;
          const height = Math.min(148, Math.max(minimum, composer.scrollHeight + 2));
          composer.style.setProperty('height', height + 'px', 'important');
          composer.style.overflowY = composer.scrollHeight > height ? 'auto' : 'hidden';
        });
      };
      composer.addEventListener('input', fit);
      composer.addEventListener('change', fit);
      composer.addEventListener('focus', fit);
      window.addEventListener('resize', fit, {passive:true});
      // Existing actions set drafts and clear successful sends programmatically.
      const send = document.getElementById('sendBtn');
      if (send) new MutationObserver(fit).observe(send, {attributes:true,attributeFilter:['disabled']});
      document.addEventListener('click', () => requestAnimationFrame(fit));
      fit();
    }
  }, {once:true});
})();
