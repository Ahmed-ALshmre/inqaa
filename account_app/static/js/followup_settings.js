let customerFollowups = [];

async function loadFollowupSettings() {
  const res = await fetch(adminApi('/api/settings/followup'));
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || 'فشل تحميل إعدادات المتابعة');
  const settings = data.settings || {};
  document.getElementById('followupEnabled').checked = !!settings.enabled;
  document.getElementById('followupMaxPerDay').value = settings.max_per_day || 2;
  document.getElementById('followupStopOnOrder').checked = settings.stop_on_order !== false;
  document.getElementById('followupStopOnRejection').checked = settings.stop_on_rejection !== false;
  document.getElementById('followupDelay').value = settings.default_delay_minutes || 20;
  document.getElementById('followupMessageTemplate').value = settings.message_template || '';
  setAdminStatus('followupSettingsStatus', `المتابعات المعلقة: ${data.pending_count || 0}`, true);
}

function followupSearchBlob(customer) {
  return [
    customer.sender_id,
    customer.name,
    customer.phone,
    customer.province,
    customer.message_template,
    customer.pending_message_text,
    customer.last_message,
  ].join(' ').toLowerCase();
}

function formatFollowupDate(value) {
  if (!value) return '-';
  return String(value).replace('T', ' ').slice(0, 16);
}

async function loadCustomerFollowups() {
  const el = document.getElementById('customerFollowupList');
  if (el) {
    el.innerHTML = '<div class="text-center py-4" style="color:var(--text-muted)">جاري تحميل الزبائن...</div>';
  }
  const res = await fetch(adminApi('/api/followups/customer_messages'));
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل تحميل رسائل الزبائن');
  customerFollowups = data.customers || [];
  renderCustomerFollowups();
}

function renderCustomerFollowups() {
  const el = document.getElementById('customerFollowupList');
  if (!el) return;
  const query = (document.getElementById('customerFollowupSearch')?.value || '').trim().toLowerCase();
  const list = customerFollowups.filter(customer => !query || followupSearchBlob(customer).includes(query));

  if (!list.length) {
    el.innerHTML = '<div class="text-center py-4" style="color:var(--text-muted)">لا توجد زبائن مطابقة</div>';
    return;
  }

  el.innerHTML = list.map(customer => {
    const senderId = adminEsc(customer.sender_id || '');
    const displayName = adminEsc(customer.name || customer.sender_id || '-');
    const phone = customer.phone ? `<span>${adminEsc(customer.phone)}</span>` : '';
    const pending = customer.pending_followup_id
      ? `<span class="badge bg-warning text-dark">متابعة معلقة: ${adminEsc(formatFollowupDate(customer.pending_scheduled_at))}</span>`
      : '<span class="badge bg-secondary">لا توجد متابعة معلقة</span>';
    const currentMessage = customer.pending_message_text
      ? `<div class="small mt-2" style="color:var(--text-muted)">المعلقة الآن: ${adminEsc(customer.pending_message_text)}</div>`
      : '';
    return `
      <div class="followup-customer-row" data-sender-id="${senderId}">
        <div class="followup-customer-head">
          <div>
            <div class="fw-semibold">${displayName}</div>
            <div class="small" style="color:var(--text-muted)">
              <span>${senderId}</span>
              ${phone}
              <span>آخر نشاط: ${adminEsc(formatFollowupDate(customer.last_seen_at || customer.last_message_at))}</span>
            </div>
          </div>
          <div>${pending}</div>
        </div>
        <textarea class="form-control form-control-sm followup-customer-message" rows="3"
          placeholder="رسالة خاصة لهذا الزبون... اتركها فارغة لاستخدام القالب العام">${adminEsc(customer.message_template || '')}</textarea>
        ${currentMessage}
        <div class="d-flex flex-wrap gap-2 mt-2">
          <button class="btn btn-primary btn-sm" type="button" onclick="saveCustomerFollowup('${senderId}')">
            <i class="bi bi-save"></i> حفظ لهذا الزبون
          </button>
          <button class="btn btn-outline-danger btn-sm" type="button" onclick="clearCustomerFollowup('${senderId}')">
            <i class="bi bi-x-circle"></i> استخدام القالب العام
          </button>
          <a class="btn btn-outline-secondary btn-sm" href="/dashboard?key=${encodeURIComponent(adminKey)}" target="_blank">
            <i class="bi bi-chat-dots"></i> فتح الداشبورد
          </a>
        </div>
      </div>`;
  }).join('');
}

async function saveCustomerFollowup(senderId) {
  const row = document.querySelector(`.followup-customer-row[data-sender-id="${CSS.escape(senderId)}"]`);
  const textarea = row?.querySelector('.followup-customer-message');
  if (!row || !textarea) return;
  const payload = {
    sender_id: senderId,
    message_template: textarea.value || '',
    update_pending: true,
  };
  const res = await fetch(adminApi('/api/followups/customer_messages'), {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload),
  });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل حفظ رسالة الزبون');
  setAdminStatus('customerFollowupStatus', `تم حفظ رسالة الزبون وتحديث ${data.updated_pending || 0} متابعة معلقة`, true);
  await loadCustomerFollowups();
}

function clearCustomerFollowup(senderId) {
  const row = document.querySelector(`.followup-customer-row[data-sender-id="${CSS.escape(senderId)}"]`);
  const textarea = row?.querySelector('.followup-customer-message');
  if (textarea) textarea.value = '';
  saveCustomerFollowup(senderId).catch((err) => setAdminStatus('customerFollowupStatus', err.message, false));
}

async function saveFollowupSettings(event) {
  event.preventDefault();
  const payload = {
    enabled: document.getElementById('followupEnabled').checked,
    max_per_day: Number(document.getElementById('followupMaxPerDay').value || 2),
    stop_on_order: document.getElementById('followupStopOnOrder').checked,
    stop_on_rejection: document.getElementById('followupStopOnRejection').checked,
    default_delay_minutes: Number(document.getElementById('followupDelay').value || 20),
    message_template: document.getElementById('followupMessageTemplate').value || ''
  };
  const res = await fetch(adminApi('/api/settings/followup'), {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل حفظ الإعدادات');
  setAdminStatus('followupSettingsStatus', 'تم حفظ إعدادات المتابعة', true);
}

async function postFollowupAction(path, successText) {
  const res = await fetch(adminApi(path), {method: 'POST'});
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل تنفيذ العملية');
  setAdminStatus('followupSettingsStatus', successText(data), true);
  await loadFollowupSettings();
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('followupSettingsForm')?.addEventListener('submit', (event) => {
    saveFollowupSettings(event).catch((err) => setAdminStatus('followupSettingsStatus', err.message, false));
  });
  document.getElementById('sendDueFollowups')?.addEventListener('click', () => {
    postFollowupAction('/api/followups/send_due', (data) => `تم الإرسال: ${data.sent || 0}، المتجاوزة: ${data.skipped || 0}`)
      .catch((err) => setAdminStatus('followupSettingsStatus', err.message, false));
  });
  document.getElementById('cancelPendingFollowups')?.addEventListener('click', () => {
    postFollowupAction('/api/followups/cancel_pending', (data) => `تم إلغاء ${data.cancelled || 0} متابعة معلقة`)
      .catch((err) => setAdminStatus('followupSettingsStatus', err.message, false));
  });
  document.getElementById('reloadCustomerFollowups')?.addEventListener('click', () => {
    loadCustomerFollowups().catch((err) => setAdminStatus('customerFollowupStatus', err.message, false));
  });
  loadFollowupSettings().catch((err) => setAdminStatus('followupSettingsStatus', err.message, false));
  loadCustomerFollowups().catch((err) => setAdminStatus('customerFollowupStatus', err.message, false));
});
