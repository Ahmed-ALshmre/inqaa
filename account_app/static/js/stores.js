async function storesRequest(path, options = {}) {
  const response = await fetch(adminApi(path), {headers: {'Content-Type': 'application/json'}, ...options});
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || 'فشل الطلب');
  return data;
}

async function loadStores() {
  const data = await storesRequest('/api/stores');
  const list = document.getElementById('storesList');
  list.innerHTML = (data.stores || []).map(store => `
    <div class="admin-list-row" style="align-items:flex-start;gap:12px">
      <div><strong>${adminEsc(store.name)}</strong><div class="small text-muted">${adminEsc(store.store_id)}${store.page_id ? ` · Page ${adminEsc(store.page_id)}` : ''}</div></div>
      <div style="min-width:0;flex:1"><input class="form-control form-control-sm" dir="ltr" readonly value="${adminEsc(store.webhook_url)}" onclick="this.select()"></div>
      <button class="btn btn-outline-primary btn-sm" onclick="navigator.clipboard.writeText('${adminEsc(store.webhook_url)}');setAdminStatus('storesStatus','تم نسخ الرابط')"><i class="bi bi-copy"></i></button>
    </div>`).join('') || '<div class="admin-empty">لا توجد متاجر.</div>';
}

document.addEventListener('DOMContentLoaded', () => {
  loadStores().catch(error => setAdminStatus('storesStatus', error.message, false));
});
