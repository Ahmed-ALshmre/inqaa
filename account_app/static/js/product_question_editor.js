/* Shared, text-only editor: staff teaching is never a customer message. */
(() => {
  const make=(tag,className,text)=>{const e=document.createElement(tag);e.className=className||'';if(text!==undefined)e.textContent=text;return e;};
  function card(row,{topics={},send=fetch,url,onSaved=()=>{}}={}){
    const el=make('article','pq-card'),meta=make('div','pq-meta');
    meta.append(make('strong','',row.product_name||row.product_id),make('span','',`تكرر ${row.occurrences} مرة`));
    const scope=make('p','pq-scope',`${row.store_name||row.store_id} · ${row.product_id}`);
    const label=make('label','','اشرح للبوت شلون يفهم السؤال ويجاوب'),input=make('textarea','form-control');
    input.id=`pqGuidance${row.id}`;label.htmlFor=input.id;input.maxLength=4000;input.rows=4;input.value=row.guidance||'';
    input.placeholder='مثال: وضّحي لها أن قماش هذا المنتج ثقيل وشتوي، وبعدها اسألي عن القياس إذا بعده مو معروف. لا تعيدين سؤال جاوبت عليه.';
    const help=make('p','pq-help','اكتب المعلومة الصحيحة وطريقة التعامل معها. البوت يفهم الشرح ويصيغ جواباً يناسب المحادثة؛ ما ينسخه للزبون. الأسعار والحجز تبقى حسب بيانات النظام.');
    help.id=`pqHelp${row.id}`;input.setAttribute('aria-describedby',help.id);
    const actions=make('div','pq-actions'),save=make('button','btn btn-primary',row.guidance?'تحديث التوجيه':'حفظ التوجيه للبوت');
    const state=make('span','pq-card-status');state.setAttribute('role','status');state.setAttribute('aria-live','polite');
    save.type='button';actions.append(save,state);
    el.append(meta,scope,make('h2','',row.gap_label||topics[row.topic]||'تفصيل المنتج'),make('p','pq-question',row.question));
    if(row.answer){const old=make('details','pq-previous');old.append(make('summary','','معلومة مؤكدة محفوظة سابقاً'),make('p','',row.answer));el.append(old);}
    if(row.topic==='service')el.append(make('p','pq-note','هذا التوجيه خاص بنفس السؤال في هذه المحادثة؛ لا يُعمّم على بقية الزبائن.'));
    el.append(label,input,help,actions);
    const person=window.workspacePerson;
    if(row.topic==='price'&&person&&!person.owner&&!person.permissions?.includes('products')){
      save.disabled=true;input.disabled=true;state.textContent='توجيه السعر يحتاج صلاحية إدارة المنتجات';return el;
    }
    save.addEventListener('click',async()=>{
      if(save.disabled)return;
      if(input.value.trim().length<5){state.textContent='اكتب شرحاً من 5 أحرف على الأقل.';input.focus();return;}
      save.disabled=true;input.disabled=true;state.textContent='جارٍ حفظ التوجيه…';state.dataset.error='false';
      try{
        const response=await send(url(row),{method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({guidance:input.value,expected_revision:row.revision})});
        const data=await response.json();if(!response.ok)throw new Error(data.error||'تعذر حفظ التوجيه. جرّب مجدداً.');
        const wasPending=row.status==='pending';Object.assign(row,data);
        state.textContent='تم حفظ الشرح للردود القادمة';save.textContent='تحديث التوجيه';el.classList.add('pq-answered');
        onSaved(row,wasPending);
      }catch(error){state.textContent=error.message;state.dataset.error='true';}
      finally{save.disabled=false;input.disabled=false;}
    });
    return el;
  }
  window.soofProductQuestions={card,make};
})();
