async function storesRequest(path, options = {}) {
  const response = await fetch(adminApi(path), {headers: {'Content-Type': 'application/json'}, ...options});
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.error || 'فشل الطلب');
  return data;
}

let mengerStores = [];

function remoteStoreOptions(config) {
  const saved = config.store_id || '';
  const missing = saved && !mengerStores.some(store => store.store_id === saved);
  return `<option value="">مطابقة الاسم: ${adminEsc(config.store_name || 'غير مرتبط')}</option>` +
    (missing ? `<option value="${adminEsc(saved)}" selected>${adminEsc(config.store_name || saved)} — محفوظ، لم يُتحقق منه</option>` : '') +
    mengerStores.map(store => `<option value="${adminEsc(store.store_id)}" ${store.store_id === saved ? 'selected' : ''}>${adminEsc(store.name)}</option>`).join('');
}

async function loadStores() {
  const data = await storesRequest('/api/stores');
  const list = document.getElementById('storesList');
  list.innerHTML = (data.stores || []).map((store, index) => {
    const config = store.menger || {};
    return `<form class="admin-panel mb-3 store-delivery-form" data-store="${adminEsc(store.store_id)}">
      <h3>${adminEsc(store.name)}</h3>
      <label class="form-label" for="webhook-${index}">رابط استقبال رسائل المتجر</label>
      <input id="webhook-${index}" class="form-control mb-3" dir="ltr" readonly value="${adminEsc(store.webhook_url)}" onclick="this.select()">
      <div class="row g-3">
        <div class="col-md-6"><label class="form-label" for="remote-store-${index}">المتجر المستلم داخل Menger</label>
          <select id="remote-store-${index}" class="form-select" name="store_id" data-saved-name="${adminEsc(config.store_name || '')}">${remoteStoreOptions(config)}</select></div>
        <div class="col-md-6"><label class="form-label" for="telegram-${index}">معرّف محادثة تلغرام للطلبات</label>
          <input id="telegram-${index}" class="form-control" name="telegram_chat_id" dir="ltr" value="${adminEsc(config.telegram_chat_id || '')}" placeholder="-100...">
          <div class="small text-muted mt-1">يستخدم بوت تلغرام الحالي. اتركه فارغاً لاستخدام محادثة الطلبات العامة.</div></div>
      </div>
      <p class="small text-muted mt-3">تُرسل طلبات هذا المتجر فقط إلى وجهته المحددة. أدخل اسم كل منتج كما هو في مينجر بحقل «اسم الإرسال» في إعدادات المنتج؛ لا يُنقل الطلب إلى متجر آخر عند اختلاف اسم المنتج.</p>
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
      const remote = mengerStores.find(store => store.store_id === values.store_id);
      values.store_name = remote ? remote.name : form.elements.store_id.dataset.savedName;
      button.disabled = true;
      status.textContent = 'جارٍ الحفظ…';
      try {
        await storesRequest(`/api/stores/${encodeURIComponent(form.dataset.store)}`, {
          method: 'PUT', body: JSON.stringify({menger: values})
        });
        form.elements.store_id.dataset.savedName = values.store_name;
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
  document.getElementById('refreshMengerStores').addEventListener('click', async event => {
    const button = event.currentTarget;
    const status = document.getElementById('mengerStoresStatus');
    button.disabled = true;
    status.textContent = 'جارٍ فحص الاتصال…';
    try {
      const data = await storesRequest('/api/settings/menger/stores');
      mengerStores = data.stores;
      document.querySelectorAll('.store-delivery-form select[name="store_id"]').forEach(select => {
        select.innerHTML = remoteStoreOptions({store_id: select.value, store_name: select.dataset.savedName});
      });
      status.textContent = `تم الاتصال؛ ${mengerStores.length} متاجر متاحة. اختر وجهة كل متجر ثم احفظ بياناته.`;
    } catch (error) { status.textContent = error.message; }
    finally { button.disabled = false; }
  });
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
  const syncButton = document.getElementById('syncMengerStores');
  syncButton.addEventListener('click', async () => {
    const status = form.querySelector('[role="status"]');
    syncButton.disabled = true;
    status.textContent = 'جارٍ اختبار الاتصال ومطابقة المتاجر…';
    try {
      const result = await storesRequest('/api/settings/menger/sync-stores', {method:'POST', body:'{}'});
      const matched = (result.matched || []).length;
      const unmatched = result.unmatched || [];
      status.textContent = unmatched.length
        ? `تم ربط ${matched} متجر. غير المطابق: ${unmatched.join('، ')}`
        : `تم الاتصال وربط جميع المتاجر (${matched}) بنجاح`;
      await loadStores();
    } catch (error) {
      status.textContent = error.message;
    } finally {
      syncButton.disabled = false;
    }
  });
});
