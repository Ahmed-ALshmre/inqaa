(() => {
  if (!window.workspacePerson) return;
  const number = n => new Intl.NumberFormat('ar-IQ', {maximumFractionDigits: 0}).format(n);
  const money = n => `${number(n)} د.ع`;
  const escape = value => { const e = document.createElement('span'); e.textContent = String(value); return e.innerHTML; };
  const duration = seconds => seconds == null ? 'غير متوفر' : `${number(seconds / 60)} دقيقة`;
  let balance, busy = false, submitting = false;
  const statusNames = {pending:'قيد المراجعة',paid:'تم الصرف',rejected:'أُعيد إلى الرصيد'};
  let grantBusy=false, grantRequest=null;
  function grantHistory(data) {
    return `<section class="reward-history"><h3 class="h5">مكافآت الأدمن · آخر ٥٠ مكافأة</h3>${(data.grants || []).map(g=>`<div class="withdraw-row"><div><strong>${money(g.amount)}</strong><p class="reward-grant-reason">${escape(g.reason)}</p><small>${escape(g.granted_by)} · ${escape(new Date(g.created_at).toLocaleString('ar-IQ',{timeZone:'Asia/Baghdad'}))}</small></div></div>`).join('') || '<p class="reward-muted">لا توجد مكافآت إضافية بعد.</p>'}</section>`;
  }
  document.addEventListener('submit',async event=>{
    if(event.target.id!=='reward-grant-form') return;
    event.preventDefault(); if(grantBusy) return;
    const form=event.target, button=form.querySelector('button'), status=form.querySelector('[role=status]');
    const fields=new FormData(form);
    const payload={staff_id:Number(fields.get('staff_id')),amount:Number(fields.get('amount')),reason:String(fields.get('reason')).trim()};
    if(!Number.isSafeInteger(payload.amount) || payload.amount<1 || payload.amount>10000000 || !payload.reason){status.textContent='أدخل مبلغاً صحيحاً وسبب المكافأة.';return;}
    const fingerprint=JSON.stringify(payload);
    if(!grantRequest || grantRequest.fingerprint!==fingerprint) grantRequest={fingerprint,id:crypto.randomUUID()};
    if(!confirm(`إضافة ${money(payload.amount)} إلى رصيد ${form.querySelector('select').selectedOptions[0].textContent}؟\nالسبب: ${payload.reason}`)) return;
    grantBusy=true;button.disabled=true;status.textContent='جاري إضافة المكافأة…';
    try {
      const response=await fetch('/api/rewards/grants',{method:'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':document.querySelector('meta[name=csrf-token]')?.content || ''},body:JSON.stringify({...payload,request_id:grantRequest.id})});
      const result=await response.json();if(!response.ok) throw new Error(result.error || 'تعذر حفظ المكافأة');
      grantRequest=null;form.reset();status.textContent='تمت إضافة المكافأة إلى رصيد الموظف.';await refresh();
    } catch(error){status.textContent=error.message+' يمكنك إعادة المحاولة دون تكرار المكافأة.';}
    finally {grantBusy=false;button.disabled=false;}
  });
  function withdrawals(data, owner=false) {
    return `<section class="reward-withdraw"><h3>سحب الحوافز</h3>${owner ? '' : `<p>المتاح للسحب <strong>${money(data.available)}</strong> · قيد المراجعة ${money(data.reserved)} · المصروف ${money(data.paid)}</p><button class="btn btn-primary" data-withdraw ${data.available <= 0 || data.reserved > 0 ? 'disabled' : ''}>طلب سحب الحوافز</button><p class="reward-muted">يُرسل الطلب للمالك لصرفه؛ لا يحدث تحويل مالي تلقائي.</p>`}<div>${data.withdrawals.map(w=>`<div class="withdraw-row"><span>#${w.id} · ${money(w.amount)} · ${statusNames[w.status] || escape(w.status)}</span>${owner && w.status==='pending' ? `<div><button class="btn btn-success" data-settle="${w.id}" data-status="paid">تأكيد الصرف</button> <button class="btn btn-outline-secondary" data-settle="${w.id}" data-status="rejected">إعادة الرصيد</button></div>` : ''}</div>`).join('') || '<p class="reward-muted">لا توجد طلبات سحب بعد.</p>'}</div></section>`;
  }
  function celebrate(data) {
    const milestone = Math.floor(data.solved / 5) * 5;
    const key = `reward-milestone:${window.workspacePerson.id}`;
    let seen = 0;
    try { seen = Number(localStorage.getItem(key)) || 0; } catch (_) { seen = celebrate.seen || 0; }
    if (!milestone || milestone <= seen) return;
    celebrate.seen = milestone;
    try { localStorage.setItem(key, String(milestone)); } catch (_) {}
    document.querySelector('.reward-celebration')?.remove();
    const panel = document.createElement('div'); panel.className='reward-celebration'; panel.setAttribute('role','status');
    panel.innerHTML=`<div class="reward-celebration-card"><img class="reward-mascot" src="/static/icons/soof-mascot-celebrate.png?v=1" alt="" aria-hidden="true"><strong>أبدعت! ${number(milestone)} إنجاز</strong><p>جهدك يصنع فرقاً، ورصيدك يكبر مع كل حل صحيح.</p><span>باقي ${number(5-data.solved%5)} للإنجاز القادم — كمّل على راحتك 🌟</span></div>` + Array.from({length:20},(_,i)=>`<i aria-hidden="true" style="--x:${i*5}vw;--delay:${i%5*.08}s;--spin:${i%2 ? 240 : -240}deg;--hue:${i*29}"></i>`).join('');
    document.dispatchEvent(new Event('soof:celebrate'));
    document.body.append(panel); setTimeout(()=>panel.remove(),6000);
  }
  document.addEventListener('click', async event => {
    const button=event.target.closest('[data-withdraw],[data-settle]');
    if(!button || submitting) return;
    const paid=button.dataset.status==='paid';
    if(!confirm(button.hasAttribute('data-withdraw') ? 'إرسال طلب سحب كامل الرصيد المتاح إلى المالك؟' : paid ? 'هل صرفت هذا المبلغ فعلياً للموظف؟ سيتم تسجيله كمصروف.' : 'إعادة مبلغ الطلب إلى رصيد الموظف؟')) return;
    submitting=true; button.disabled=true;
    try {
      const response=await fetch(button.hasAttribute('data-withdraw') ? '/api/rewards/withdraw' : `/api/rewards/withdrawals/${button.dataset.settle}`, {method:'POST',headers:{'Content-Type':'application/json','X-CSRF-Token':document.querySelector('meta[name=csrf-token]')?.content || ''},body:JSON.stringify({status:button.dataset.status})});
      const data=await response.json(); if(!response.ok) throw new Error(data.error || 'تعذر إرسال الطلب');
      await refresh();
    } catch(error) { alert(error.message); }
    finally {submitting=false;button.disabled=false;}
  });
  function cards(data) {
    return `<div class="reward-cards"><div class="reward-card"><span>الرصيد المتراكم</span><strong>${money(data.balance)}</strong></div><div class="reward-card"><span>مشاكل تم حلها</span><strong>${number(data.solved)}</strong></div><div class="reward-card"><span>علاوات السرعة</span><strong>${money(data.bonus)}</strong></div><div class="reward-card"><span>مكافآت الأدمن</span><strong>${money(data.manual_bonus || 0)}</strong></div></div>`;
  }
  function render(data) {
    const content = document.getElementById('reward-content');
    if (!content) return;
    if (data.owner) {
      const panel=document.getElementById('reward-grant-panel');
      if(panel && !panel.children.length) panel.innerHTML=`<form id="reward-grant-form" class="reward-goal-panel"><h2 class="h5">إضافة مكافأة لموظف</h2><div class="reward-grant-fields"><label>الموظف<select name="staff_id" class="form-select" required><option value="">اختر الموظف</option>${data.employees.filter(p=>p.active).map(p=>`<option value="${p.id}">${escape(p.name)}</option>`).join('')}</select></label><label>المبلغ بالدينار العراقي<input name="amount" type="number" min="1" max="10000000" step="1" class="form-control" required></label><label>سبب المكافأة<textarea name="reason" maxlength="500" class="form-control" required placeholder="مثلاً: تميز في خدمة الزبائن"></textarea></label></div><button class="btn btn-primary" type="submit">إضافة المكافأة</button><p role="status" aria-live="polite"></p></form>`;
      content.innerHTML = '<h2 class="h5 mt-4">إنجازات فريق العمل</h2>' + (data.employees.length ? data.employees.map(p => `<section class="reward-employee"><h3 class="h5">${escape(p.name)}</h3>${cards(p)}${withdrawals(p,true)}<p>اليوم: ${number(p.today.solved)} / ${number(p.goal)} مشاكل · ${money(p.today.earned)}</p></section>`).join('') : '<p>أضف موظفين من الإعدادات لبدء احتساب المكافآت.</p>');
      content.querySelectorAll('.reward-employee').forEach((element,index)=>element.insertAdjacentHTML('beforeend',grantHistory(data.employees[index])));
      return;
    }
    const target = (Math.floor(data.today.solved / data.goal) + 1) * data.goal;
    content.innerHTML = cards(data) + withdrawals(data) + grantHistory(data) + `<section class="reward-goal-panel"><strong>${data.today.solved >= data.goal ? '✦ حققت هدف اليوم!' : 'خطوة أقرب إلى هدف اليوم'}</strong><progress max="${target}" value="${data.today.solved}" aria-label="التقدم نحو هدف اليوم"></progress><div>${number(data.today.solved)} / ${number(target)} مشاكل · مكافآت اليوم ${money(data.today.earned)}</div><p class="reward-muted mt-3">${data.baseline_seconds == null ? 'حلّ أول مشكلة لتبدأ المقارنة مع سرعتك السابقة.' : `معيار سرعتك الحالي: ${duration(data.baseline_seconds)}. الحل الأسرع يمنحك ×1.2.`}</p></section><section class="reward-history"><h2 class="h5">سجل المكافآت · آخر ٥٠ حلاً</h2>${data.history.length ? `<table><thead><tr><th>المراجعة</th><th>وقت الحل</th><th>المدة</th><th>المضاعف</th><th>المكافأة</th></tr></thead><tbody>${data.history.map(r => `<tr><td>#${r.review_id}</td><td>${escape(new Date(r.created_at).toLocaleString('ar-IQ',{timeZone:'Asia/Baghdad'}))}</td><td>${duration(r.duration_seconds)}</td><td>${r.multiplier > 1 ? '<span class="reward-fast">⚡ ×1.2</span>' : '×1'}</td><td>${money(r.amount)}</td></tr>`).join('')}</tbody></table>` : '<p class="reward-muted">رصيدك يبدأ مع أول مراجعة تحلّها. إنجازك القادم سيظهر هنا.</p>'}</section>`;
  }
  async function refresh() {
    if (busy || document.hidden) return;
    busy = true;
    try {
      const response = await fetch('/api/rewards');
      if (!response.ok) throw new Error('تعذّر تحميل المكافآت؛ ستتم إعادة المحاولة تلقائياً.');
      const data = await response.json();
      document.querySelectorAll('[data-credit-balance]').forEach(e => e.textContent = data.owner ? 'مكافآت الفريق' : money(data.balance));
      if (!data.owner) {
        const target=(Math.floor(data.today.solved/data.goal)+1)*data.goal;
        document.querySelectorAll('[data-credit-goal]').forEach(e => e.textContent = `اليوم ${number(data.today.solved)} / ${number(target)}`);
        document.querySelectorAll('[data-credit-progress]').forEach(e => {e.max=target;e.value=data.today.solved;});
        if (balance !== undefined && data.balance > balance) {
          const toast = document.createElement('div'); toast.className='reward-toast';toast.setAttribute('role','status');
          const mascot=document.createElement('img'); mascot.src='/static/icons/soof-mascot-celebrate.png?v=1'; mascot.alt=''; mascot.setAttribute('aria-hidden','true');
          const label=document.createElement('span'); label.textContent=`✦ +${money(data.balance-balance)}${data.history[0]?.multiplier > 1 ? ' · مكافأة سرعة ×1.2 ⚡' : ' · أحسنت، تم تسجيل إنجازك!'}`;
          toast.append(mascot,label);
          document.body.append(toast);setTimeout(()=>toast.remove(),4500);
          document.dispatchEvent(new Event('soof:celebrate'));
          document.querySelectorAll('.credit-bar').forEach(e=>{e.classList.remove('credit-pulse');void e.offsetWidth;e.classList.add('credit-pulse');});
        }
        balance = data.balance;
      }
      render(data);
      if (!data.owner) celebrate(data);
      const status = document.getElementById('reward-status'); if(status) status.textContent='';
    } catch(error) {
      const status=document.getElementById('reward-status');if(status) status.textContent=error.message;
    } finally {busy=false;}
  }
  const originalFetch=window.fetch;
  window.fetch=async (...args)=>{
    const response=await originalFetch(...args);
    const input=args[0];const url=new URL(input instanceof Request ? input.url : input,location.href);
    const method=args[1]?.method || (input instanceof Request ? input.method : 'GET');
    if(url.origin===location.origin && method.toUpperCase()!=='GET' && response.ok) setTimeout(refresh,100);
    return response;
  };
  refresh();setInterval(refresh,15000);
  document.addEventListener('visibilitychange',()=>{if(!document.hidden) refresh();});
})();
