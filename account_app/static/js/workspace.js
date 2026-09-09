// Same-origin mutations carry a session token. Never attach it to external URLs.
const workspaceFetch = window.fetch.bind(window);
window.fetch = (input, options = {}) => {
  const url = new URL(input instanceof Request ? input.url : input, location.href);
  if (url.origin === location.origin) {
    const headers = new Headers(options.headers || (input instanceof Request ? input.headers : undefined));
    headers.set('X-CSRF-Token', document.querySelector('meta[name=csrf-token]')?.content || '');
    options = {...options, headers};
  }
  return workspaceFetch(input, options);
};
function openWorkspaceDialog(id) { document.getElementById(id)?.showModal(); }
document.addEventListener('DOMContentLoaded', () => {
  const person = window.workspacePerson;
  if (person && !person.owner) {
    const can = p => person.permissions.includes(p);
    document.querySelectorAll('a[href]').forEach(link => {
      const path = new URL(link.href,location.href).pathname;
      let permission = path.startsWith('/settings') ? 'settings' : path.startsWith('/orders') ? 'orders' : path.startsWith('/products') ? 'products' : path.includes('advisor') ? 'advisor' : '';
      if(path.includes('maintenance') || path.includes('/staff')) permission='owner';
      if(permission && !can(permission)) link.hidden=true;
    });
    document.querySelectorAll('[onclick]').forEach(button => {
      const action=button.getAttribute('onclick');
      let permission = /openOrderModal|createManualOrder|submitOrder/.test(action) ? 'orders' : /deleteConversation|testManyChat/.test(action) ? 'owner' : /toggleGlobalAI|saveInstructions/.test(action) ? 'settings' : /send|askAI|improveMessage|linkProduct|unlinkProduct|saveCustomer|setCustomerGender|toggleConversationAI|imageUpload|markHumanReview|resolveWithAI|hiSend|hiAsk|hiClose/.test(action) ? 'reply' : '';
      if(permission && !can(permission)) button.hidden=true;
    });
    if(!can('reply')) {const input=document.getElementById('messageInput'); if(input){input.disabled=true;input.placeholder='هذا الحساب للعرض فقط';}}
  }
  if(document.getElementById('advisorMessages')) {
    document.body.classList.add('advisor-workspace');
    const main=document.querySelector('.admin-main');
    const toolbar=document.createElement('div');toolbar.className='advisor-toolbar';toolbar.innerHTML='<strong>مساحة المستشار</strong>';
    main.prepend(toolbar);
    const makeDialog=(id,title,nodes)=>{
      const dialog=document.createElement('dialog');dialog.id=id;dialog.className='workspace-dialog';
      const heading=document.createElement('div');heading.className='dialog-heading';
      const text=document.createElement('h2');text.textContent=title;
      const close=document.createElement('button');close.textContent='×';close.setAttribute('aria-label','إغلاق');close.onclick=()=>dialog.close();heading.append(text,close);dialog.append(heading);
      for(const node of nodes.filter(Boolean)){dialog.append(node);if(node.tagName==='DETAILS')node.open=true;}
      document.body.append(dialog);
      const button=document.createElement('button');button.textContent=title;button.onclick=()=>dialog.showModal();toolbar.append(button);
      return dialog;
    };
    makeDialog('advisorMemoryDialog','الذاكرة',[document.querySelector('.advisor-memory-editor')]);
    makeDialog('advisorProposalsDialog','الاقتراحات',[document.querySelector('.advisor-overview'),document.querySelector('.advisor-proposals-card')]);
    const options=document.createElement('div');options.className='advisor-compose-actions';
    document.querySelectorAll('.advisor-compose-actions label').forEach(label=>options.append(label));
    makeDialog('advisorOptionsDialog','أدوات التحليل',[options,document.querySelector('.advisor-quick-prompts')]);
    document.querySelectorAll('.advisor-quick-prompts button').forEach(button=>button.addEventListener('click',()=>{document.getElementById('advisorOptionsDialog').close();requestAnimationFrame(()=>document.getElementById('advisorInput').focus());}));
    const resize=()=>{document.documentElement.style.setProperty('--advisor-viewport',`${window.visualViewport?.height || innerHeight}px`);};
    resize();window.visualViewport?.addEventListener('resize',resize);window.addEventListener('resize',resize);
  }
  document.querySelectorAll('dialog').forEach(dialog => dialog.addEventListener('click', event => {if(event.target === dialog && event.clientX < dialog.getBoundingClientRect().left) dialog.close();}));
  const search = document.getElementById('settingsSearch');
  search?.addEventListener('input', () => document.querySelectorAll('.admin-link-card').forEach(card => card.hidden = !card.textContent.includes(search.value.trim())));
  document.querySelectorAll('.admin-sidebar a').forEach(link => {if(new URL(link.href).pathname === location.pathname) link.setAttribute('aria-current','page');});
});
