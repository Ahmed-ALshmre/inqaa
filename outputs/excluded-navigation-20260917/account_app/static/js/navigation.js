(() => {
  'use strict';
  const init = () => {
    const dialog = document.getElementById('quickNavigation');
    if (!dialog) return;
    const search = document.getElementById('navigationSearch');
    const normalize = text => text.replace(/[أإآ]/g,'ا').replace(/ة/g,'ه').replace(/[\u064B-\u065F]/g,'').trim();
    const path = location.pathname === '/' ? '/dashboard' : location.pathname;
    const store = new URLSearchParams(location.search).get('store_id');
    document.querySelectorAll('.page-nav-link,.work-shortcut').forEach(link => {
      const url = new URL(link.href, location.origin);
      if (store) {url.searchParams.set('store_id',store);link.href=url.pathname+url.search;}
      if (url.pathname === path) link.setAttribute('aria-current','page');
    });
    const filter = () => {
      const query=normalize(search.value);
      const links=[...dialog.querySelectorAll('.page-nav-link')];
      links.forEach(link=>link.hidden=!normalize(link.textContent).includes(query));
      dialog.querySelectorAll('.page-nav-group').forEach(e=>e.hidden=Boolean(query));
      document.getElementById('navigationEmpty').hidden=links.some(link=>!link.hidden);
    };
    const open = () => {
      if(dialog.open) return;
      search.value='';filter();dialog.showModal();search.focus();
    };
    document.querySelectorAll('[data-open-navigation]').forEach(button=>button.addEventListener('click',open));
    dialog.querySelector('[data-close-navigation]').addEventListener('click',()=>dialog.close());
    dialog.addEventListener('click',event=>{if(event.target===dialog){const r=dialog.getBoundingClientRect();if(event.clientX<r.left||event.clientX>r.right||event.clientY<r.top||event.clientY>r.bottom)dialog.close();}});
    search.addEventListener('input',filter);
    search.addEventListener('keydown',event=>{
      const links=[...dialog.querySelectorAll('.page-nav-link')].filter(link=>!link.hidden);
      if(event.key==='ArrowDown'){event.preventDefault();links[0]?.focus();}
      if(event.key==='Enter' && links.length===1){event.preventDefault();links[0].click();}
    });
    document.addEventListener('keydown',event=>{
      if((event.ctrlKey||event.metaKey)&&event.key.toLowerCase()==='k') {event.preventDefault();dialog.open?dialog.close():open();}
      if(event.key==='Escape'){if(dialog.open){event.preventDefault();dialog.close();}window.closeAdminNav?.();window.closeDashboardDrawer?.();}
    });
    document.querySelectorAll('[data-inbox-shortcut]').forEach(button=>button.addEventListener('click',()=>{
      window.setFilter?.(button.dataset.inboxShortcut);
      document.getElementById('searchInput')?.focus({preventScroll:true});
    }));
  };
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();
