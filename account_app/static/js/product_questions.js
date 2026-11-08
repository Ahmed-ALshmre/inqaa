(() => {
  document.body.classList.add('product-questions-page');
  const list=document.getElementById('pqList'), status=document.getElementById('pqStatus'), more=document.getElementById('pqMore');
  let cursor=null, busy=false, generation=0;
  const message=document.getElementById('pqStatusMessage');
  const {make}=window.soofProductQuestions;
  const params=new URLSearchParams(location.search),allStores=params.get('all_stores')==='1';
  let pending=0;
  const updateCount=()=>{document.getElementById('pqCount').textContent=`${pending} بانتظار توجيه`;};
  function card(row,topics){
    return window.soofProductQuestions.card(row,{topics,url:row=>{
      const url=new URL(adminApi(`/api/product-questions/${row.id}/answer`),location.origin);url.searchParams.set('store_id',row.store_id);return url.pathname+url.search;
    },onSaved:(_row,wasPending)=>{if(wasPending)pending=Math.max(0,pending-1);updateCount();}});
  }
  async function load(reset=false){
    if(busy&&!reset)return;
    const run=++generation;busy=true;more.disabled=true;list.setAttribute('aria-busy','true');message.textContent='جارٍ تحميل الأسئلة…';
    if(reset)cursor=null;
    const requestedStatus=status.value;
    try{
      const url=new URL(adminApi('/api/product-questions'),location.origin);url.searchParams.set('status',requestedStatus);if(allStores)url.searchParams.set('all_stores','1');if(cursor)url.searchParams.set('before',cursor);
      const response=await fetch(url);const data=await response.json();if(!response.ok)throw new Error(data.error||'تعذر التحميل');if(run!==generation)return;
      if(reset)list.replaceChildren();pending=data.pending;updateCount();
      const fragment=document.createDocumentFragment();data.questions.forEach(row=>fragment.append(card(row,data.topics)));list.append(fragment);
      if(!list.children.length)list.append(make('div','pq-empty',requestedStatus==='pending'?'كل الأسئلة مجاب عنها حالياً 🌷':'لا توجد أجوبة محفوظة بعد.'));
      cursor=data.questions.at(-1)?.id||cursor;more.hidden=!data.has_more;message.textContent='';
    }catch(error){if(run===generation)message.textContent=error.message;}
    finally{if(run===generation){busy=false;more.disabled=false;list.setAttribute('aria-busy','false');}}
  }
  status.addEventListener('change',()=>load(true));document.getElementById('pqRefresh').addEventListener('click',()=>load(true));more.addEventListener('click',()=>load());load(true);
})();
