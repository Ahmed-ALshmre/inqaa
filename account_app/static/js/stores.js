async function storesRequest(path, options = {}) {
  const response = await fetch(adminApi(path), {headers: {'Content-Type': 'application/json'}, ...options});
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || 'فشل الطلب');
  return data;
}

async function loadStores() {
  const data = await storesRequest('/api/stores');
  const list = document.getElementById('storesList');
  list.innerHTML = (data.stores || []).map((store, index) => {
    const config = store.menger || {};
    return `<form class="admin-panel mb-3 store-delivery-form" data-store="${adminEsc(store.store_id)}">
      <h3>${adminEsc(store.name)}</h3>
      <label class="form-label" for="webhook-${index}">رابط ManyChat</label>
      <input id="webhook-${index}" class="form-control mb-3" dir="ltr" readonly value="${adminEsc(store.webhook_url)}" onclick="this.select()">
      <div class="row g-3">
        <div class="col-md-6"><label class="form-label" for="remote-store-${index}">معرّف المتجر داخل Menger</label>
          <input id="remote-store-${index}" class="form-control" name="store_id" dir="ltr" value="${adminEsc(config.store_id || '')}" placeholder="معرّف المتجر المستلم"></div>
        <div class="col-md-6"><label class="form-label" for="telegram-${index}">معرّف محادثة تلغرام للطلبات</label>
          <input id="telegram-${index}" class="form-control" name="telegram_chat_id" dir="ltr" value="${adminEsc(config.telegram_chat_id || '')}" placeholder="-100...">
          <div class="small text-muted mt-1">يستخدم بوت تلغرام الحالي. اتركه فارغاً لاستخدام محادثة الطلبات العامة.</div></div>
      </div>
      <p class="small text-muted mt-3">تُرسل طلبات هذا المتجر كاملة إلى تلغرام وإلى Menger عبر الرابط الموحد. معرّف Menger يحدد المتجر المستلم، ويجب أن تطابق أسماء المنتجات أسماءها فيه.</p>
      <button class="btn btn-primary" type="submit">حفظ بيانات الإرسال</button>
      <span class="small ms-2" role="status"></span>
    </form>`;
  }).join('') || '<div class="admin-empty">لا توجد متاجر.</div>';
  list.querySelectorAll('.store-delivery-form').forEach(form => {
    form.addEventListener('submit', async event => {
      event.preventDefault();
      const button = form.querySelector('button[type="submit"]');
      const status = form.querySelector('[role="status"]');
      const values = Object.fromEntries(new FormData(form));
      button.disabled = true;
      status.textContent = 'جارٍ الحفظ…';
      try {
        await storesRequest(`/api/stores/${encodeURIComponent(form.dataset.store)}`, {
          method: 'PUT', body: JSON.stringify({menger: values})
        });
        status.textContent = 'تم حفظ بيانات المتجر';
      } catch (error) {
        status.textContent = error.message;
      } finally { button.disabled = false; }
    });
  });
}

async function loadConnection() {
  const data = await storesRequest('/api/settings/menger');
  const form = document.getElementById('mengerConnectionForm');
  form.elements.api_url.value = data.connection.api_url || '';
  form.elements.source.value = data.connection.source;
  form.elements.api_key.placeholder = data.connection.key_configured ? 'محفوظ — اتركه فارغاً للاحتفاظ به' : 'أدخل مفتاح الربط';
}

document.addEventListener('DOMContentLoaded', () => {
  loadStores().catch(error => setAdminStatus('storesStatus', error.message, false));
  loadConnection().catch(error => setAdminStatus('storesStatus', error.message, false));
  const form = document.getElementById('mengerConnectionForm');
  form.addEventListener('submit', async event => {
    event.preventDefault();
    const button = form.querySelector('button');
    const status = form.querySelector('[role="status"]');
    const values = Object.fromEntries(new FormData(form));
    values.clear_api_key = form.elements.clear_api_key.checked;
    button.disabled = true;
    try {
      await storesRequest('/api/settings/menger', {method:'PUT', body:JSON.stringify(values)});
      form.elements.api_key.value = '';
      form.elements.clear_api_key.checked = false;
      await loadConnection();
      status.textContent = 'تم حفظ الربط الموحد لجميع المتاجر';
    } catch (error) { status.textContent = error.message; }
    finally { button.disabled = false; }
  });
});
