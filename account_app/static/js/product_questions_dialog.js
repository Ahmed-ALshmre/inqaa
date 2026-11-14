(() => {
  const dialog=document.getElementById('productQuestionsDialog');
  if(!dialog||typeof dialog.showModal!=='function')return;
  const prompt=window.soofQuestionPrompt={loading:true,shown:false};
  const list=document.getElementById('pqDialogList'),more=document.getElementById('pqDialogMore');
  const status=document.getElementById('pqDialogStatus'),count=document.getElementById('pqDialogCount');
  const params=new URLSearchParams(location.search),store=params.get('store_id');
  const send=(url,options)=>typeof apiFetch==='function'?apiFetch(url,options):fetch(url,options);
  const url=(path,row)=>{const u=new URL(path,location.origin);if(params.has('key'))u.searchParams.set('key',params.get('key'));
    if(row)u.searchParams.set('store_id',row.store_id);else if(store&&store!=='all')u.searchParams.set('store_id',store);else u.searchParams.set('all_stores','1');
    return u.pathname+u.search;};
  document.getElementById('pqDialogManage').href=url('/product-questions');
  let cursor=null,busy=false,pending=0,ready=false,previousFocus=null,saved=false;
  const scopeText=store&&store!=='all'?'في هذا المتجر':'في المتاجر';
  function updateCount(){count.textContent=pending?`${pending} سؤال يحتاج توضيح ${scopeText}`:'تم توضيح الأسئلة 🌷';}
  function tryOpen(){
    if(prompt.shown||!ready||document.hidden||document.querySelector('dialog[open],.modal.show'))return;
    const active=document.activeElement;
    if(active?.matches('textarea,input,[contenteditable="true"]')&&(active.value||active.textContent))return;
    previousFocus=active;dialog.showModal();document.body.classList.add('pq-dialog-open');prompt.shown=true;
    document.getElementById('pqDialogTitle').focus({preventScroll:true});observer.disconnect();
  }
  const observer=new MutationObserver(tryOpen);
  observer.observe(document.body,{subtree:true,attributes:true,attributeFilter:['open','class']});
  document.addEventListener('close',tryOpen,true);
  document.addEventListener('hidden.bs.modal',tryOpen);
  document.addEventListener('visibilitychange',tryOpen);
  document.addEventListener('focusout',()=>setTimeout(tryOpen,0));
  dialog.addEventListener('close',()=>{
    document.body.classList.remove('pq-dialog-open');previousFocus?.focus?.({preventScroll:true});
    if(saved&&typeof loadStats==='function'){saved=false;loadStats();}
  });
  document.getElementById('pqDialogClose').onclick=()=>dialog.close();
  document.getElementById('pqDialogLater').onclick=()=>dialog.close();
  async function load(){
    if(busy)return;busy=true;more.disabled=true;list.setAttribute('aria-busy','true');
    status.textContent='جارٍ تحميل الأسئلة…';
    const controller=new AbortController(),timeout=setTimeout(()=>controller.abort(),10000);
    try{
      const request=new URL(url('/api/product-questions'),location.origin);
      request.searchParams.set('needs_attention','1');
      request.searchParams.set('limit','12');
      if(cursor)request.searchParams.set('before',cursor);
      const response=await send(request.pathname+request.search,{signal:controller.signal});const data=await response.json();
      if(!response.ok)throw new Error(data.error||'تعذر تحميل الأسئلة');
      pending=data.pending;prompt.pending=pending>0;updateCount();
      for(const row of data.questions){
        const card=window.soofProductQuestions.card(row,{topics:data.topics,send,fact:row.accepts_fact,
          url:row=>url(`/api/product-questions/${row.id}/answer`,row),onSaved:(_row,wasPending)=>{
            saved=true;
            if(!wasPending)return;
            card.remove();
            pending=Math.max(0,pending-1);
            prompt.pending=pending>0;
            updateCount();
            if(!pending)dialog.close();
            else if(!list.children.length&&!more.hidden)load();
          }});
        list.append(card);
      }
      cursor=data.questions.at(-1)?.id||cursor;more.hidden=!data.has_more;status.textContent='';ready=!!list.children.length;
      if(dialog.open&&!ready&&!data.has_more)dialog.close();
      else tryOpen();
    }catch(error){
      status.textContent=error.name==='AbortError'?'تأخر تحميل الأسئلة، افتح صفحة الأسئلة للمحاولة مجدداً.':error.message;
      const link=document.getElementById('productQuestionsLink');if(link)link.title='تعذر تحميل نافذة الأسئلة. افتح صفحة الأسئلة للمحاولة مجدداً.';
    }
    finally{
      clearTimeout(timeout);busy=false;more.disabled=false;list.setAttribute('aria-busy','false');prompt.loading=false;
      window.dispatchEvent(new Event('soof:questions-checked'));
      if(!ready)observer.disconnect();
    }
  }
  more.onclick=load;
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',load,{once:true});else load();
})();
