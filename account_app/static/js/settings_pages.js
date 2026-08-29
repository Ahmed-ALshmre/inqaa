async function getJSON(path, options = {}) {
  const storeId = new URLSearchParams(location.search).get('store_id') || 'default';
  if (path.startsWith('/api/') && !path.includes('store_id=')) {
    path += `${path.includes('?') ? '&' : '?'}store_id=${encodeURIComponent(storeId)}`;
  }
  const res = await fetch(adminApi(path), {
    headers: { 'Content-Type': 'application/json' },
    ...options
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || 'فشل الطلب');
  return data;
}

async function initStoreSelector(id) {
  const select = document.getElementById(id);
  if (!select) return;
  const response = await fetch(adminApi('/api/stores'));
  const data = await response.json();
  const selected = new URLSearchParams(location.search).get('store_id') || data.current_store_id || 'default';
  select.innerHTML = (data.stores || []).map(store =>
    `<option value="${adminEsc(store.store_id)}">${adminEsc(store.name)}</option>`
  ).join('');
  select.value = selected;
  select.addEventListener('change', () => {
    const url = new URL(location.href);
    url.searchParams.set('store_id', select.value);
    location.href = url.toString();
  });
}

async function initAISettingsPage() {
  await initStoreSelector('aiStoreSelect');
  const data = await getJSON('/api/settings/ai');
  
  const aiEnabledEl = document.getElementById('aiEnabled');
  if (aiEnabledEl) aiEnabledEl.checked = !!data.ai_enabled;
  
  const checkerStateEl = document.getElementById('checkerState');
  if (checkerStateEl) checkerStateEl.textContent = data.checker_enabled ? data.checker_model : 'متوقف';
  
  const openrouterStateEl = document.getElementById('openrouterState');
  if (openrouterStateEl) openrouterStateEl.textContent = data.openrouter_key_present ? 'موجود' : 'غير مضبوط';
  
  const mainModelEl = document.getElementById('mainModelInput');
  if (mainModelEl) mainModelEl.value = data.main_model || '';
  document.getElementById('visionEnabled').checked = !!data.vision_enabled;
  document.getElementById('catalogMatchEnabled').checked = !!data.catalog_match_enabled;
  document.getElementById('checkerEnabled').checked = !!data.checker_enabled;
  document.getElementById('visionModelInput').value = data.vision_model || '';
  document.getElementById('catalogMatchModelInput').value = data.catalog_match_model || '';
  document.getElementById('checkerModelInput').value = data.checker_model || '';
  document.getElementById('improveModelInput').value = data.improve_model || '';
  document.getElementById('mainTemperatureInput').value = data.main_temperature ?? 0.7;
  document.getElementById('mainMaxTokensInput').value = data.main_max_tokens ?? 1500;
  
  const formEl = document.getElementById('aiSettingsForm');
  if (formEl) {
    formEl.addEventListener('submit', async (event) => {
      event.preventDefault();
      try {
        await getJSON('/api/settings/ai', {
          method: 'POST',
          body: JSON.stringify({
            enabled: document.getElementById('aiEnabled').checked,
            main_model: document.getElementById('mainModelInput').value.trim(),
            vision_enabled: document.getElementById('visionEnabled').checked,
            catalog_match_enabled: document.getElementById('catalogMatchEnabled').checked,
            checker_enabled: document.getElementById('checkerEnabled').checked,
            vision_model: document.getElementById('visionModelInput').value.trim(),
            catalog_match_model: document.getElementById('catalogMatchModelInput').value.trim(),
            checker_model: document.getElementById('checkerModelInput').value.trim(),
            improve_model: document.getElementById('improveModelInput').value.trim(),
            main_temperature: Number(document.getElementById('mainTemperatureInput').value),
            main_max_tokens: Number(document.getElementById('mainMaxTokensInput').value)
          })
        });
        setAdminStatus('aiSettingsStatus', 'تم الحفظ');
      } catch (err) {
        setAdminStatus('aiSettingsStatus', err.message, false);
      }
    });
  }
}

async function initAutoProductPage() {
  await initStoreSelector('autoProductStoreSelect');
  const [settings, productsData] = await Promise.all([
    getJSON('/api/settings/auto_product'),
    getJSON('/api/products')
  ]);
  const select = document.getElementById('autoProductSelect');
  const products = productsData.products || [];
  select.innerHTML = '<option value="">بدون منتج</option>' + products.map((p) =>
    `<option value="${adminEsc(p.product_id)}">${adminEsc(p.product_name || p.product_id)} (${adminEsc(p.product_id)})</option>`
  ).join('');
  document.getElementById('autoProductEnabled').checked = !!settings.auto_product_enabled;
  document.getElementById('autoProductSendImage').checked = !!settings.auto_product_send_image;
  select.value = settings.auto_product_id || '';
  renderSelectedAutoProduct(settings.product);
  select.addEventListener('change', () => {
    const selected = products.find((p) => p.product_id === select.value);
    renderSelectedAutoProduct(selected);
  });
  document.getElementById('autoProductForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      const saved = await getJSON('/api/settings/auto_product', {
        method: 'POST',
        body: JSON.stringify({
          enabled: document.getElementById('autoProductEnabled').checked,
          product_id: select.value,
          send_image: document.getElementById('autoProductSendImage').checked
        })
      });
      renderSelectedAutoProduct(saved.product);
      setAdminStatus('autoProductStatus', 'تم الحفظ');
    } catch (err) {
      setAdminStatus('autoProductStatus', err.message, false);
    }
  });
}

function renderSelectedAutoProduct(product) {
  const el = document.getElementById('autoProductCurrent');
  if (!el) return;
  if (!product) {
    el.textContent = 'لا يوجد منتج محدد.';
    return;
  }
  el.innerHTML = `
    <strong>${adminEsc(product.product_name || product.product_id)}</strong>
    <span>${adminEsc(product.price || 'بدون سعر')} · ${adminEsc(product.stock || 'بدون حالة')}</span>
  `;
}

async function initChannelsPage() {
  const data = await getJSON('/api/settings/overview');
  const ch = data.channels || {};
  document.getElementById('channelsGrid').innerHTML = [
    ['ManyChat API', ch.manychat_key_present ? 'مضبوط' : 'غير مضبوط'],
    ['ManyChat URL', ch.manychat_api_url || '-'],
    ['Telegram Bot', ch.telegram_bot_present ? 'موجود' : 'غير مضبوط'],
    ['Telegram Chat', ch.telegram_chat_present ? 'موجود' : 'غير مضبوط'],
    ['Orders Chat', ch.telegram_orders_chat_present ? 'موجود' : 'غير مضبوط'],
    ['Problems Chat', ch.telegram_problems_chat_present ? 'موجود' : 'غير مضبوط'],
    ['Public URL', ch.public_url || '-'],
    ['Human Reply Webhook', ch.human_reply_webhook_url || '-']
  ].map(([label, value]) => `<div><small>${adminEsc(label)}</small><strong>${adminEsc(value)}</strong></div>`).join('');
}

async function initStoreSettingsPage() {
  await initStoreSelector('storeSettingsStoreSelect');
  const data = await getJSON('/api/settings/store');
  document.getElementById('storeName').value = data.name || '';
  document.getElementById('storePhone').value = data.phone || '';
  document.getElementById('storeDescription').value = data.description || '';
  document.getElementById('deliveryPolicy').value = data.delivery_policy || '';
  document.getElementById('storeProvinces').value = data.provinces || '';
  document.getElementById('inspectionMessage').value = data.inspection_message || '';
  document.getElementById('storeSettingsForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await getJSON('/api/settings/store', {
        method: 'POST',
        body: JSON.stringify({
          name: document.getElementById('storeName').value,
          phone: document.getElementById('storePhone').value,
          description: document.getElementById('storeDescription').value,
          delivery_policy: document.getElementById('deliveryPolicy').value,
          provinces: document.getElementById('storeProvinces').value,
          inspection_message: document.getElementById('inspectionMessage').value
        })
      });
      setAdminStatus('storeSettingsStatus', 'تم الحفظ');
    } catch (err) {
      setAdminStatus('storeSettingsStatus', err.message, false);
    }
  });
}

async function initDeliveryPage() {
  await initStoreSelector('deliveryStoreSelect');
  const data = await getJSON('/api/settings/delivery');
  document.getElementById('allProvincesFee').value = data.all_provinces_fee || 5000;
  document.getElementById('fastDelivery').checked = !!data.fast_delivery;
  document.getElementById('inspectionMessage').value = data.inspection_message || '';
  document.getElementById('deliverySettingsForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await getJSON('/api/settings/delivery', {
        method: 'POST',
        body: JSON.stringify({
          all_provinces_fee: Number(document.getElementById('allProvincesFee').value) || 5000,
          fast_delivery: document.getElementById('fastDelivery').checked,
          inspection_message: document.getElementById('inspectionMessage').value
        })
      });
      setAdminStatus('deliverySettingsStatus', 'تم الحفظ');
    } catch (err) {
      setAdminStatus('deliverySettingsStatus', err.message, false);
    }
  });
}

async function initMaintenancePage() {
  const key = new URLSearchParams(location.search).get('key') || '';
  document.getElementById('fullBackupLink').href = `/api/export/full-backup?key=${encodeURIComponent(key)}`;
  const data = await getJSON('/api/settings/overview');
  const m = data.maintenance || {};
  document.getElementById('maintenanceGrid').innerHTML = [
    ['قاعدة البيانات', m.database_path || '-'],
    ['حجم قاعدة البيانات', `${Math.round((m.database_size || 0) / 1024)} KB`],
    ['عدد المنتجات', m.products_count || 0]
  ].map(([label, value]) => `<div><small>${adminEsc(label)}</small><strong>${adminEsc(value)}</strong></div>`).join('');
}

async function restoreFullBackup(input) {
  if (!input.files?.[0]) return;
  const warning = 'سيتم استبدال بيانات النظام الحالية بالمحادثات والمنتجات والصور الموجودة في النسخة. سيتم حفظ قاعدة البيانات الحالية تلقائياً للطوارئ. هل تريد المتابعة؟';
  if (!confirm(warning)) { input.value = ''; return; }
  const key = new URLSearchParams(location.search).get('key') || '';
  const form = new FormData();
  form.append('file', input.files[0]);
  try {
    const response = await fetch(`/api/import/full-backup?key=${encodeURIComponent(key)}`, {method: 'POST', body: form});
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'تعذرت استعادة النسخة الكاملة');
    alert(`تمت استعادة النظام بنجاح\nالمنتجات: ${result.products || 0}\nالصور: ${result.images || 0}`);
    location.reload();
  } catch (error) {
    alert(error.message);
  }
  input.value = '';
}

async function restoreProductsBackup(input) {
  if (!input.files?.[0]) return;
  if (!confirm('سيتم استبدال كتالوج المنتجات الحالي. هل تريد المتابعة؟')) { input.value = ''; return; }
  const key = new URLSearchParams(location.search).get('key') || '';
  const form = new FormData(); form.append('file', input.files[0]);
  try {
    const response = await fetch(`/api/import/products?key=${encodeURIComponent(key)}`, {method:'POST', body:form});
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'تعذرت الاستعادة');
    alert(`تمت استعادة ${result.count || 0} منتج بنجاح.`);
  } catch (error) { alert(error.message); }
  input.value = '';
}

async function restoreDatabaseBackup(input) {
  if (!input.files?.[0]) return;
  if (!confirm('سيتم استبدال قاعدة البيانات الحالية بالنسخة المحددة. هل تريد المتابعة؟')) { input.value = ''; return; }
  const key = new URLSearchParams(location.search).get('key') || '';
  const form = new FormData();
  form.append('file', input.files[0]);
  try {
    const response = await fetch(`/api/import/database?key=${encodeURIComponent(key)}`, {method: 'POST', body: form});
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'تعذرت الاستعادة');
    alert('تمت استعادة قاعدة البيانات بنجاح. حدّث الصفحة.');
  } catch (error) { alert(error.message); }
  input.value = '';
}

async function clearBrowserCacheAndReload() {
  try {
    if ('caches' in window) {
      const cacheNames = await caches.keys();
      await Promise.all(cacheNames.map(name => caches.delete(name)));
    }
    if ('serviceWorker' in navigator) {
      const registrations = await navigator.serviceWorker.getRegistrations();
      for (const registration of registrations) {
        await registration.unregister();
      }
    }
    try {
      sessionStorage.clear();
      localStorage.clear();
    } catch (err) {}
    const url = new URL(window.location.href);
    url.searchParams.set('_refresh', Date.now().toString());
    window.location.href = url.toString();
  } catch (error) {
    window.location.reload();
  }
}
