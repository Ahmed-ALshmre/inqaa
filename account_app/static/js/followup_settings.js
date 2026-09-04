async function loadFollowupSettings() {
  const res = await fetch(adminApi('/api/settings/followup'));
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || 'فشل تحميل إعدادات المتابعة');
  const settings = data.settings || {};
  document.getElementById('followupEnabled').checked = !!settings.enabled;
  document.getElementById('followupMinInterestScore').value = settings.min_interest_score ?? 50;
  document.getElementById('followupReviewInterval').value = settings.review_interval_minutes || 60;
  document.getElementById('followupMaxPerDay').value = settings.max_per_day || 2;
  document.getElementById('followupStopOnOrder').checked = settings.stop_on_order !== false;
  document.getElementById('followupStopOnRejection').checked = settings.stop_on_rejection !== false;
  document.getElementById('followupDelay').value = settings.default_delay_minutes || 20;
  const state = settings.enabled ? 'المتابعة التلقائية مفعّلة' : 'المتابعة التلقائية متوقفة';
  setAdminStatus('followupSettingsStatus', `${state} — الرسائل المعلقة: ${data.pending_count || 0}`, !!settings.enabled);
}

async function saveFollowupSettings(event) {
  event.preventDefault();
  const payload = {
    enabled: document.getElementById('followupEnabled').checked,
    min_interest_score: Number(document.getElementById('followupMinInterestScore').value || 0),
    review_interval_minutes: Number(document.getElementById('followupReviewInterval').value || 60),
    max_per_day: Number(document.getElementById('followupMaxPerDay').value || 2),
    stop_on_order: document.getElementById('followupStopOnOrder').checked,
    stop_on_rejection: document.getElementById('followupStopOnRejection').checked,
    default_delay_minutes: Number(document.getElementById('followupDelay').value || 20),
  };
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
