async function getJSON(path, options = {}) {
  const res = await fetch(adminApi(path), {
    headers: { 'Content-Type': 'application/json' },
    ...options
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || 'فشل الطلب');
  return data;
}

async function initAISettingsPage() {
  const data = await getJSON('/api/settings/ai');
  document.getElementById('aiEnabled').checked = !!data.ai_enabled;
  document.getElementById('mainModel').textContent = data.main_model || '-';
  document.getElementById('improveModel').textContent = data.improve_model || '-';
  document.getElementById('checkerState').textContent = data.checker_enabled ? data.checker_model : 'متوقف';
  document.getElementById('openrouterState').textContent = data.openrouter_key_present ? 'موجود' : 'غير مضبوط';
  document.getElementById('aiSettingsForm').addEventListener('submit', async (event) => {
    event.preventDefault();
    try {
      await getJSON('/api/settings/ai', {
        method: 'POST',
        body: JSON.stringify({ enabled: document.getElementById('aiEnabled').checked })
      });
      setAdminStatus('aiSettingsStatus', 'تم الحفظ');
    } catch (err) {
      setAdminStatus('aiSettingsStatus', err.message, false);
    }
  });
}

async function initAutoProductPage() {
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
  const data = await getJSON('/api/settings/store');
  document.getElementById('storeName').value = data.name || '';
  document.getElementById('storePhone').value = data.phone || '';
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

async function initMaintenancePage() {
  const data = await getJSON('/api/settings/overview');
  const m = data.maintenance || {};
  document.getElementById('maintenanceGrid').innerHTML = [
    ['قاعدة البيانات', m.database_path || '-'],
    ['حجم قاعدة البيانات', `${Math.round((m.database_size || 0) / 1024)} KB`],
    ['عدد المنتجات', m.products_count || 0]
  ].map(([label, value]) => `<div><small>${adminEsc(label)}</small><strong>${adminEsc(value)}</strong></div>`).join('');
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
    } catch (err) {
      console.warn('Storage clear failed:', err);
    }
    const url = new URL(window.location.href);
    url.searchParams.set('_refresh', Date.now().toString());
    window.location.href = url.toString();
  } catch (error) {
    console.error('Cache clear failed:', error);
    window.location.reload();
  }
}
