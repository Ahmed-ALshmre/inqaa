async function loadFollowupSettings() {
  const res = await fetch(adminApi('/api/settings/followup'));
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || 'فشل تحميل إعدادات المتابعة');
  const settings = data.settings || {};
  document.getElementById('followupEnabled').checked = !!settings.enabled;
  document.getElementById('followupMinInterestScore').value = settings.min_interest_score ?? 50;
  document.getElementById('followupReviewInterval').value = settings.review_interval_minutes || 60;
  document.getElementById('followupMaxPerDay').value = settings.max_per_day || 1;
  document.getElementById('followupStopOnOrder').checked = settings.stop_on_order !== false;
  document.getElementById('followupStopOnRejection').checked = settings.stop_on_rejection !== false;
  document.getElementById('followupDelay').value = settings.default_delay_minutes || 120;
  document.getElementById('followupAdaptive').checked = settings.adaptive_timing !== false;
  for (const [id, key] of growthFields) document.getElementById(id).value = settings[key];
  renderGrowth(data);
  const state = settings.enabled ? 'المتابعة التلقائية مفعّلة' : 'المتابعة التلقائية متوقفة';
  setAdminStatus('followupSettingsStatus', `${state} — الرسائل المعلقة: ${data.pending_count || 0}`, !!settings.enabled);
}

async function saveFollowupSettings(event) {
  event.preventDefault();
  const payload = {
    enabled: document.getElementById('followupEnabled').checked,
    min_interest_score: Number(document.getElementById('followupMinInterestScore').value || 0),
    review_interval_minutes: Number(document.getElementById('followupReviewInterval').value || 60),
    max_per_day: Number(document.getElementById('followupMaxPerDay').value || 1),
    stop_on_order: document.getElementById('followupStopOnOrder').checked,
    stop_on_rejection: document.getElementById('followupStopOnRejection').checked,
    default_delay_minutes: Number(document.getElementById('followupDelay').value || 120),
    adaptive_timing: document.getElementById('followupAdaptive').checked,
  };
  for (const [id, key] of growthFields) payload[key] = Number(document.getElementById(id).value);
  const res = await fetch(adminApi('/api/settings/followup'), {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل حفظ الإعدادات');
  setAdminStatus('followupSettingsStatus', 'تم الحفظ، وسيعمل الذكاء الاصطناعي تلقائياً حسب الإعدادات الجديدة.', true);
  await loadFollowupSettings();
}

const growthFields = [
  ['followupCheckoutDelay', 'checkout_delay_minutes'], ['followupSelectionDelay', 'selection_delay_minutes'],
  ['followupGeneralDelay', 'general_delay_minutes'], ['followupStartHour', 'contact_start_hour'],
  ['followupEndHour', 'contact_end_hour'],
];

function renderGrowth(data) {
  const g = data.growth || {};
  document.getElementById('growthMetrics').textContent = `${g.people || 0} مراسل · ${g.buyers || 0} صاحب حجز · التحويل ${g.conversion || 0}%`;
  document.getElementById('growthProgress').value = Math.min(g.conversion || 0, 30);
  document.getElementById('growthGap').textContent = g.people ? `هدف 30% يحتاج ${g.target_buyers} صاحب حجز؛ المتبقي ${g.additional_buyers}.` : 'لا توجد رسائل واردة خلال الفترة بعد.';
  document.getElementById('growthFollowups').textContent = `متابعات مرسلة: ${g.followups?.sent || 0} · زبائن حجزوا بعد متابعة خلال الفترة: ${g.buyers_after_followup || 0} (تتابع زمني، لا يثبت أثر المتابعة وحدها).`;
  const urgent = document.getElementById('growthUrgent');
  urgent.replaceChildren();
  for (const item of g.urgent || []) {
    const p = document.createElement('p');
    p.textContent = `${item.name || item.sender_id} — انتظار ${item.waiting_minutes} دقيقة — ${item.contact_ready ? 'بيانات الاتصال مكتملة' : 'اهتمام ' + item.lead_score} — ${item.reviews} مراجعات`;
    const link = document.createElement('a');
    const destination = new URL(adminApi('/dashboard'), window.location.origin);
    destination.searchParams.set('sender_id', item.sender_id);
    link.href = destination.href;
    link.textContent = ' · فتح المحادثة';
    p.append(link);
    urgent.append(p);
  }
  if (!urgent.childElementCount) urgent.textContent = 'لا توجد مراجعات معلقة لزبائن بلا حجز.';
  const queue = document.getElementById('growthQueue');
  queue.replaceChildren();
  const statuses = {pending: 'مجدولة', sending: 'قيد الإرسال — افحصها إذا طال الانتظار', uncertain: 'الإرسال غير مؤكد — يحتاج فحصاً'};
  const reasons = {checkout: 'إكمال حجز', selection: 'لون أو قياس', general_interest: 'استفسار', requested_reminder: 'تذكير طلبه الزبون', custom_delay: 'موعد مخصص'};
  for (const item of data.queue || []) {
    let meta = {}; try { meta = JSON.parse(item.meta_json || '{}'); } catch (_) { /* legacy metadata */ }
    const p = document.createElement('p');
    const date = new Date(item.scheduled_at);
    const when = Number.isNaN(date.getTime()) ? 'موعد غير معروف' : date.toLocaleString('ar-IQ', {timeZone: 'Asia/Baghdad'});
    p.textContent = `${item.name || item.sender_id} — ${statuses[item.status] || item.status} — ${reasons[meta.reason] || 'متابعة'} — ${when}`;
    queue.append(p);
  }
  if (!queue.childElementCount) queue.textContent = 'لا توجد متابعات معلقة.';
}

async function cancelPendingFollowups() {
  const res = await fetch(adminApi('/api/followups/cancel_pending'), {method: 'POST'});
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'تعذر إلغاء الرسائل المعلقة');
  setAdminStatus('followupSettingsStatus', `تم إلغاء ${data.cancelled || 0} رسالة معلقة`, true);
  await loadFollowupSettings();
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('followupSettingsForm')?.addEventListener('submit', (event) => {
    saveFollowupSettings(event).catch((err) => setAdminStatus('followupSettingsStatus', err.message, false));
  });
  document.getElementById('cancelPendingFollowups')?.addEventListener('click', () => {
    cancelPendingFollowups().catch((err) => setAdminStatus('followupSettingsStatus', err.message, false));
  });
  loadFollowupSettings().catch((err) => setAdminStatus('followupSettingsStatus', err.message, false));
});
