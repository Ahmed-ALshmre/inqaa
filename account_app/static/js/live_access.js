'use strict';
const accessStatus = document.getElementById('liveAccessStatus');
const accessCSRF = document.querySelector('meta[name="csrf-token"]').content;
async function accessRequest(path, body) {
  const response = await fetch(path, body ? {method: 'POST', headers: {'Content-Type': 'application/json', 'X-CSRF-Token': accessCSRF}, body: JSON.stringify(body)} : {});
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || 'تعذر إكمال الطلب');
  return data;
}
function accessDate(value) { return value ? new Date(value * 1000).toLocaleString('ar-IQ') : 'لم يُستخدم بعد'; }
async function loadAccess() {
  const [data, audit] = await Promise.all([accessRequest('/api/settings/live-access'), accessRequest('/api/settings/live-access/audit')]);
  const list = document.getElementById('liveAccessList'); list.replaceChildren();
  for (const key of data.keys) {
    const row = document.createElement('div'); row.className = 'border rounded p-3 mb-2';
    const name = document.createElement('strong'); name.textContent = key.label; row.append(name);
    const status = document.createElement('p');
    const active = !key.revoked_at && key.expires_at > Date.now() / 1000;
    status.textContent = `${key.revoked_at ? 'ملغى' : active ? 'فعال' : 'منتهي'} · ينتهي: ${accessDate(key.expires_at)} · آخر استخدام: ${accessDate(key.last_used_at)}`; row.append(status);
    if (active) {
      const button = document.createElement('button'); button.className = 'btn btn-outline-danger btn-sm'; button.textContent = 'إلغاء الوصول';
      button.onclick = async () => {
        button.disabled = true;
        try { await accessRequest(`/api/settings/live-access/${encodeURIComponent(key.id)}/revoke`, {}); await loadAccess(); accessStatus.textContent = 'تم إلغاء الوصول'; }
        catch (e) { accessStatus.textContent = e.message; button.disabled = false; }
      }; row.append(button);
    }
    list.append(row);
  }
  if (!data.keys.length) list.textContent = 'لا توجد اتصالات بعد.';
  const events = document.getElementById('liveAccessAudit'); events.replaceChildren();
  for (const event of audit.events) {
    const row = document.createElement('p'); row.className = 'small border-bottom pb-2';
    row.textContent = `${event.label} · ${accessDate(event.created_at)} · ${event.path} · ${event.status}`; events.append(row);
  }
  if (!audit.events.length) events.textContent = 'لا توجد قراءات مسجلة.';
}
document.getElementById('liveAccessForm').onsubmit = async event => {
  event.preventDefault(); const button = document.getElementById('liveAccessCreate'); button.disabled = true;
  document.getElementById('liveAccessToken').value = ''; document.getElementById('liveAccessSecret').hidden = true;
  try {
    const data = await accessRequest('/api/settings/live-access', {label: document.getElementById('liveAccessLabel').value, days: Number(document.getElementById('liveAccessDays').value)});
    document.getElementById('liveAccessToken').value = data.token; document.getElementById('liveAccessSecret').hidden = false;
    accessStatus.textContent = 'تم إنشاء الاتصال. يظهر المفتاح في هذه الصفحة مرة واحدة فقط.'; await loadAccess();
  } catch (e) { accessStatus.textContent = e.message; }
  finally { button.disabled = false; }
};
document.getElementById('liveAccessHide').onclick = () => { document.getElementById('liveAccessToken').value = ''; document.getElementById('liveAccessSecret').hidden = true; };
loadAccess().catch(e => { accessStatus.textContent = e.message; });
