const DASH_KEY = new URLSearchParams(window.location.search).get('key') || '';
let allProducts = [];
let currentProductId = null;
let productImageColors = {};

const fields = {
  product_id: 'productId',
  product_name: 'productName',
  price: 'productPrice',
  status: 'productStatus',
  stock: 'productStock',
  stock_quantity: 'productStockQuantity',
  sizes: 'productSizes',
  colors: 'productColors',
  ref: 'productRef',
  ad_id: 'productAdId',
  category: 'productCategory',
  fabric: 'productFabric',
  style: 'productStyle',
  keywords: 'productKeywords',
  description: 'productDescription',
  visual_description: 'productVisualDescription',
  offer: 'productOffer',
  delivery: 'productDelivery',
  notes: 'productNotes',
};

document.addEventListener('DOMContentLoaded', async () => {
  await initProductsStoreSelector();
  loadProducts();
  document.getElementById('productImages').addEventListener('input', renderImagePreview);
});

function apiFetch(url, opts = {}) {
  const storeId = new URLSearchParams(window.location.search).get('store_id') || 'default';
  url += `${url.includes('?') ? '&' : '?'}store_id=${encodeURIComponent(storeId)}`;
  opts.headers = {
    ...(opts.headers || {}),
    'X-Dashboard-Key': DASH_KEY,
  };
  return fetch(url, opts);
}

async function initProductsStoreSelector() {
  const res = await apiFetch('/api/stores');
  const data = await res.json();
  const select = document.getElementById('productsStoreSelect');
  const selected = new URLSearchParams(location.search).get('store_id') || data.current_store_id || 'default';
  select.innerHTML = (data.stores || []).map(store =>
    `<option value="${escAttr(store.store_id)}">${esc(store.name)}</option>`
  ).join('');
  select.value = selected;
  select.addEventListener('change', () => {
    const url = new URL(location.href);
    url.searchParams.set('store_id', select.value);
    location.href = url.toString();
  });
}

function downloadUrl(path) {
  const storeId = new URLSearchParams(window.location.search).get('store_id') || 'default';
  path += `${path.includes('?') ? '&' : '?'}store_id=${encodeURIComponent(storeId)}`;
  const sep = path.includes('?') ? '&' : '?';
  return `${path}${sep}key=${encodeURIComponent(DASH_KEY)}`;
}

async function loadProducts() {
  try {
    const res = await apiFetch('/api/products/manage');
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'فشل تحميل المنتجات');
    allProducts = data.products || [];
    renderProducts();
    if (!currentProductId) newProduct(false);
  } catch (err) {
    showToast(err.message || 'فشل تحميل المنتجات', 'danger');
  }
}

function renderProducts() {
  const q = (document.getElementById('productSearch').value || '').trim().toLowerCase();
  const list = allProducts.filter(p => {
    if (!q) return true;
    return (p.product_id || '').toLowerCase().includes(q) ||
      (p.product_name || '').toLowerCase().includes(q) ||
      (p.keywords || '').toLowerCase().includes(q);
  });

  document.getElementById('productsCount').textContent = `${list.length} من ${allProducts.length} منتج`;
  const el = document.getElementById('productsList');
  if (!list.length) {
    el.innerHTML = '<div class="text-center py-5 small" style="color:var(--text-muted)">لا توجد منتجات</div>';
    return;
  }

  el.innerHTML = list.map(p => {
    const active = p.product_id === currentProductId ? 'active' : '';
    const img = (p.image_urls || [])[0] || '';
    const statusClass = p.status === 'active' ? 'bg-success' : 'bg-secondary';
    return `
      <button class="product-row ${active}" onclick="editProduct('${escAttr(p.product_id)}')">
        <div class="product-row-image">${img ? `<img src="${escAttr(img)}" alt="" loading="lazy">` : '<i class="bi bi-image"></i>'}</div>
        <div class="product-row-info">
          <div class="product-row-name">${esc(p.product_name || p.product_id)}</div>
          <div class="product-row-meta">${esc(p.product_id || '')} · ${esc(p.price || '-')}</div>
        </div>
        <span class="badge ${statusClass}">${p.status === 'active' ? 'نشط' : 'مخفي'}</span>
      </button>`;
  }).join('');
}

function newProduct(rerender = true) {
  currentProductId = null;
  productImageColors = {};
  document.getElementById('productForm').reset();
  document.getElementById('productStatus').value = 'active';
  document.getElementById('productId').disabled = false;
  document.getElementById('formTitle').textContent = 'إضافة منتج';
  document.getElementById('deleteProductBtn').style.display = 'none';
  renderImagePreview();
  if (rerender) renderProducts();
}

function editProduct(productId) {
  const product = allProducts.find(p => p.product_id === productId);
  if (!product) return;
  currentProductId = productId;
  for (const [key, id] of Object.entries(fields)) {
    document.getElementById(id).value = product[key] || '';
  }
  document.getElementById('productImages').value = imageText(product.image_url);
  productImageColors = normalizeImageColors(product.image_colors, imageLines());
  document.getElementById('productId').disabled = true;
  document.getElementById('formTitle').textContent = `تعديل ${product.product_name || product.product_id}`;
  document.getElementById('deleteProductBtn').style.display = '';
  renderImagePreview();
  renderProducts();
}

async function saveProduct(event) {
  event.preventDefault();
  const payload = collectPayload();
  if (!payload.product_id || !payload.product_name) {
    showToast('اكتب كود المنتج واسم المنتج', 'warning');
    return;
  }

  const isEdit = Boolean(currentProductId);
  const url = isEdit
    ? `/api/products/manage/${encodeURIComponent(currentProductId)}`
    : '/api/products/manage';
  const method = isEdit ? 'PUT' : 'POST';

  try {
    setSaving(true);
    const res = await apiFetch(url, {
      method,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'فشل حفظ المنتج');
    currentProductId = data.product.product_id;
    showToast('تم حفظ المنتج', 'success');
    await loadProducts();
    editProduct(currentProductId);
  } catch (err) {
    showToast(err.message || 'فشل حفظ المنتج', 'danger');
  } finally {
    setSaving(false);
  }
}

async function deleteCurrentProduct() {
  if (!currentProductId) return;
  const product = allProducts.find(p => p.product_id === currentProductId);
  if (!confirm(`حذف المنتج ${product?.product_name || currentProductId}؟`)) return;

  try {
    const res = await apiFetch(`/api/products/manage/${encodeURIComponent(currentProductId)}`, { method: 'DELETE' });
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'فشل حذف المنتج');
    showToast('تم حذف المنتج', 'success');
    currentProductId = null;
    await loadProducts();
    newProduct();
  } catch (err) {
    showToast(err.message || 'فشل حذف المنتج', 'danger');
  }
}

async function exportProducts() {
  window.location.href = downloadUrl('/api/export/products');
}

async function exportDatabase() {
  window.location.href = downloadUrl('/api/export/database');
}

async function importProductsFile(input) {
  const file = input.files && input.files[0];
  if (!file) return;
  if (!confirm('استيراد المنتجات سيستبدل ملف products.json الحالي. هل تريد المتابعة؟')) {
    input.value = '';
    return;
  }
  const form = new FormData();
  form.append('file', file);
  try {
    showToast('جاري استيراد المنتجات...', 'warning');
    const res = await apiFetch('/api/import/products', { method: 'POST', body: form });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) throw new Error(data.error || 'فشل استيراد المنتجات');
    showToast(`تم استيراد ${data.count || 0} منتج`, 'success');
    currentProductId = null;
    await loadProducts();
  } catch (err) {
    showToast(err.message || 'فشل استيراد المنتجات', 'danger');
  } finally {
    input.value = '';
  }
}

async function importDatabaseFile(input) {
  const file = input.files && input.files[0];
  if (!file) return;
  if (!confirm('استيراد قاعدة البيانات سيستبدل بيانات الداشبورد الحالية بعد إنشاء نسخة احتياطية. هل تريد المتابعة؟')) {
    input.value = '';
    return;
  }
  const form = new FormData();
  form.append('file', file);
  try {
    showToast('جاري استيراد قاعدة البيانات...', 'warning');
    const res = await apiFetch('/api/import/database', { method: 'POST', body: form });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) throw new Error(data.error || 'فشل استيراد قاعدة البيانات');
    showToast(data.message || 'تم استيراد قاعدة البيانات', 'success');
  } catch (err) {
    showToast(err.message || 'فشل استيراد قاعدة البيانات', 'danger');
  } finally {
    input.value = '';
  }
}

function collectPayload() {
  const payload = {};
  for (const [key, id] of Object.entries(fields)) {
    payload[key] = document.getElementById(id).value.trim();
  }
  payload.image_url = imageLines();
  payload.image_colors = normalizeImageColors(productImageColors, payload.image_url);
  return payload;
}

function imageLines() {
  return document.getElementById('productImages').value
    .split(/\r?\n/)
    .map(x => x.trim())
    .filter(Boolean);
}

function imageText(value) {
  if (Array.isArray(value)) return value.join('\n');
  return value || '';
}

function normalizeImageColors(value, urls) {
  const colors = value && typeof value === 'object' && !Array.isArray(value) ? value : {};
  return Object.fromEntries((urls || [])
    .filter(url => String(colors[url] || '').trim())
    .map(url => [url, String(colors[url]).trim()]));
}

function setProductImageColor(index, color) {
  const url = imageLines()[index];
  if (!url) return;
  const value = String(color || '').trim();
  if (value) productImageColors[url] = value;
  else delete productImageColors[url];
}

async function uploadProductImages(input) {
  const files = Array.from(input.files || []);
  if (!files.length) return;
  const progress = document.getElementById('productUploadProgress');
  progress.innerHTML = '<span class="spinner-border spinner-border-sm"></span> جاري رفع الصور...';
  const form = new FormData();
  files.forEach(file => form.append('images', file));
  try {
    const response = await apiFetch('/api/upload_image', { method: 'POST', body: form });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'فشل رفع الصور');
    const urls = [...imageLines(), ...(result.image_urls || [])];
    document.getElementById('productImages').value = [...new Set(urls)].join('\n');
    productImageColors = normalizeImageColors(productImageColors, imageLines());
    renderImagePreview();
    progress.textContent = `تم رفع ${result.image_urls?.length || files.length} صورة بنجاح`;
    showToast('تم رفع الصور وإضافتها إلى المنتج');
  } catch (error) {
    progress.textContent = error.message;
    showToast(error.message, 'danger');
  } finally {
    input.value = '';
  }
}

function removeProductImage(index) {
  const urls = imageLines();
  const [removedUrl] = urls.splice(index, 1);
  delete productImageColors[removedUrl];
  document.getElementById('productImages').value = urls.join('\n');
  renderImagePreview();
}

function renderImagePreview() {
  const urls = imageLines();
  const el = document.getElementById('productImagePreview');
  if (!urls.length) {
    el.innerHTML = '<span class="small" style="color:var(--text-muted)">لا توجد صور للمعاينة</span>';
    return;
  }
  el.innerHTML = urls.map((url, index) => `
    <div class="product-image-tile">
      <div class="product-image-frame">
        <img src="${escAttr(url)}" alt="صورة المنتج" loading="lazy" onerror="this.closest('.product-image-tile').classList.add('image-error')">
        <button type="button" onclick="removeProductImage(${index})" title="حذف الصورة"><i class="bi bi-x-lg"></i></button>
      </div>
      <label class="product-image-color-label" for="productImageColor${index}">لون هذه الصورة</label>
      <input class="form-control form-control-sm product-image-color" id="productImageColor${index}"
        value="${escAttr(productImageColors[url] || '')}" placeholder="مثال: وردي"
        oninput="setProductImageColor(${index}, this.value)">
    </div>`).join('');
}

function setSaving(saving) {
  const btn = document.getElementById('saveProductBtn');
  btn.disabled = saving;
  btn.innerHTML = saving
    ? '<span class="spinner-border spinner-border-sm me-1"></span>جاري الحفظ...'
    : '<i class="bi bi-save me-1"></i>حفظ المنتج';
}

function showToast(msg, type = 'success') {
  const container = document.getElementById('toastContainer');
  const id = 't' + Date.now();
  container.insertAdjacentHTML('beforeend', `
    <div id="${id}" class="toast align-items-center text-bg-${type} border-0 mb-2" role="alert">
      <div class="d-flex">
        <div class="toast-body">${esc(msg)}</div>
        <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
      </div>
    </div>`);
  const toastEl = document.getElementById(id);
  const toast = new bootstrap.Toast(toastEl, { delay: 2600 });
  toast.show();
  toastEl.addEventListener('hidden.bs.toast', () => toastEl.remove());
}

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;',
  }[c]));
}

function escAttr(s) {
  return esc(s).replace(/`/g, '&#096;');
}
