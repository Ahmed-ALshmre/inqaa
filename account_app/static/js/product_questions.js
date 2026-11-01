(() => {
  document.body.classList.add('product-questions-page');
  const list=document.getElementById('pqList'), status=document.getElementById('pqStatus'), more=document.getElementById('pqMore');
  let cursor=null, busy=false, generation=0;
  const message=document.getElementById('pqStatusMessage');
  const make=(tag,className,text)=>{const e=document.createElement(tag);e.className=className||'';if(text!==undefined)e.textContent=text;return e;};
  function card(row,topics){
    const el=make('article','pq-card'), meta=make('div','pq-meta');
    meta.append(make('strong','',row.product_name||row.product_id),make('span','',`تكرر ${row.occurrences} مرة`));
    const question=make('p','pq-question',row.question), label=make('label','','الجواب المعتمد عن هذا المنتج'), answer=make('textarea','form-control');
    answer.id=`pqAnswer${row.id}`;label.htmlFor=answer.id;answer.maxLength=1200;answer.value=row.answer||'';answer.placeholder='اكتب المعلومة المؤكدة، مثلاً: القماش قديفة ثقيل، والقصة كلوش.';
    const actions=make('div','pq-actions'), save=make('button','btn btn-primary',row.status==='answered'?'تحديث الجواب':'حفظ الجواب للبوت'), state=make('span','pq-card-status');
    save.type='button';actions.append(save,state);el.append(meta,make('h2','',topics[row.topic]||'تفصيل المنتج'),question,label,answer,actions);
    save.addEventListener('click',async()=>{
      save.disabled=true;answer.disabled=true;state.textContent='جارٍ الحفظ…';
      try{const response=await fetch(adminApi(`/api/product-questions/${row.id}/answer`),{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({answer:answer.value})});const data=await response.json();if(!response.ok)throw new Error(data.error||'تعذر الحفظ');state.textContent='تم حفظ المعلومة للردود القادمة';save.textContent='تحديث الجواب';}
      catch(error){state.textContent=error.message;}
      finally{save.disabled=false;answer.disabled=false;}
    });return el;
  }
  async function load(reset=false){
    if(busy&&!reset)return;
    const run=++generation;busy=true;more.disabled=true;list.setAttribute('aria-busy','true');message.textContent='جارٍ تحميل الأسئلة…';
    if(reset)cursor=null;
    const requestedStatus=status.value;
    try{
      const url=new URL(adminApi('/api/product-questions'),location.origin);url.searchParams.set('status',requestedStatus);if(cursor)url.searchParams.set('before',cursor);
      const response=await fetch(url);const data=await response.json();if(!response.ok)throw new Error(data.error||'تعذر التحميل');if(run!==generation)return;
      if(reset)list.replaceChildren();document.getElementById('pqCount').textContent=`${data.pending} بانتظار جواب`;
      const fragment=document.createDocumentFragment();data.questions.forEach(row=>fragment.append(card(row,data.topics)));list.append(fragment);
      if(!list.children.length)list.append(make('div','pq-empty',requestedStatus==='pending'?'كل الأسئلة مجاب عنها حالياً 🌷':'لا توجد أجوبة محفوظة بعد.'));
      cursor=data.questions.at(-1)?.id||cursor;more.hidden=!data.has_more;message.textContent='';
    }catch(error){if(run===generation)message.textContent=error.message;}
    finally{if(run===generation){busy=false;more.disabled=false;list.setAttribute('aria-busy','false');}}
  }
  status.addEventListener('change',()=>load(true));document.getElementById('pqRefresh').addEventListener('click',()=>load(true));more.addEventListener('click',()=>load());load(true);
})();
