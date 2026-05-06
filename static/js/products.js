function getDashboardKey() {
  const fromUrl = new URLSearchParams(window.location.search).get('key') || '';
  const fromCookie = document.cookie.split('; ').find(row => row.startsWith('dashboard_key='))?.split('=')[1] || '';
  return fromUrl || decodeURIComponent(fromCookie);
}

const DASH_KEY = getDashboardKey();
let allProducts = [];
let currentProductId = null;

const fields = {
  product_id: 'productId',
  product_name: 'productName',
  price: 'productPrice',
  status: 'productStatus',
  stock: 'productStock',
  sizes: 'productSizes',
  colors: 'productColors',
  ref: 'productRef',
  ad_id: 'productAdId',
  category: 'productCategory',
  keywords: 'productKeywords',
  description: 'productDescription',
  visual_description: 'productVisualDescription',
  offer: 'productOffer',
  delivery: 'productDelivery',
  notes: 'productNotes',
};

document.addEventListener('DOMContentLoaded', () => {
  loadProducts();
  document.getElementById('productImages').addEventListener('input', renderImagePreview);
});

function apiFetch(url, opts = {}) {
  opts.headers = {
    ...(opts.headers || {}),
    'X-Dashboard-Key': DASH_KEY,
  };
  return fetch(url, opts);
}

function downloadUrl(path) {
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
        <div class="product-row-image">${img ? `<img src="${escAttr(img)}" alt="">` : '<i class="bi bi-image"></i>'}</div>
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

async function analyzeProducts() {
  const btn = document.getElementById('analyzeProductsBtn');
  const oldHtml = btn ? btn.innerHTML : '';
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>تحليل';
  }
  try {
    showToast('جاري تحليل المنتجات ومزامنة معرفة الذكاء الاصطناعي...', 'warning');
    const res = await apiFetch('/api/products/analyze', { method: 'POST' });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) throw new Error(data.error || 'فشل تحليل المنتجات');
    const sourceText = data.source === 'ai' ? 'بالذكاء الاصطناعي' : 'بالمزامنة المحلية';
    showToast(`تم تحليل ${data.count || 0} منتج ${sourceText}`, 'success');
  } catch (err) {
    showToast(err.message || 'فشل تحليل المنتجات', 'danger');
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = oldHtml;
    }
  }
}

function collectPayload() {
  const payload = {};
  for (const [key, id] of Object.entries(fields)) {
    payload[key] = document.getElementById(id).value.trim();
  }
  payload.image_url = imageLines();
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

function renderImagePreview() {
  const urls = imageLines();
  const el = document.getElementById('productImagePreview');
  if (!urls.length) {
    el.innerHTML = '<span class="small" style="color:var(--text-muted)">لا توجد صور للمعاينة</span>';
    return;
  }
  el.innerHTML = urls.map(url => `<img src="${escAttr(url)}" alt="صورة المنتج" onerror="this.style.display='none'">`).join('');
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
