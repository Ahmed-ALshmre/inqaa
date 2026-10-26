const DASH_KEY = new URLSearchParams(window.location.search).get('key') || '';
let allProducts = [];
let currentProductId = null;
let productImageColors = {};
let relatedProductIds = new Set();
let cleanProductSnapshot = '';
let savingProduct = false;
const catalogSchema = JSON.parse(document.getElementById('catalogSchema')?.textContent || '{"audiences":{},"product_types":{}}');
const byId = id => document.getElementById(id);

const fields = {
  product_id: 'productId',
  audience: 'productAudience', product_type: 'productType', size_system: 'productSizeSystem', recommendation_notes: 'productRecommendationNotes',
  product_name: 'productName',
  order_name: 'productOrderName',
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
  lining: 'knowledge_lining',
  transparency: 'knowledge_transparency',
  stretch: 'knowledge_stretch',
  closure: 'knowledge_closure',
  measurements: 'knowledge_measurements',
  fit_notes: 'knowledge_fit_notes',
  bundle_contents: 'knowledge_bundle_contents',
  sale_unit: 'knowledge_sale_unit',
  real_photo_notes: 'knowledge_real_photo_notes',
  video_url: 'knowledge_video_url',
  faq: 'knowledge_faq',
  inspection_policy: 'knowledge_inspection_policy',
};

document.addEventListener('DOMContentLoaded', async () => {
  await initProductsStoreSelector();
  await loadProducts(true);
  byId('productImages').addEventListener('input', () => { renderImagePreview(); markProductDirty(); });
  byId('relatedProductsSearch').addEventListener('input', renderRelatedProducts);
  byId('productId').addEventListener('input', renderRelatedProducts);
  byId('productForm').addEventListener('input', () => { updateSizeFields(); updateProductSummary(); markProductDirty(); });
  byId('productForm').addEventListener('change', () => { updateSizeFields(); updateProductSummary(); markProductDirty(); });
  byId('productForm').addEventListener('keydown', event => {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 's') { event.preventDefault(); saveProduct(event); }
  });
  byId('productForm').querySelectorAll('[role="tab"]').forEach(tab => tab.addEventListener('keydown', event => {
    if (!['ArrowLeft','ArrowRight','Home','End'].includes(event.key)) return;
    event.preventDefault();
    const tabs = [...byId('productForm').querySelectorAll('[role="tab"]')];
    let index = tabs.indexOf(tab) + (event.key === 'ArrowLeft' ? 1 : -1);
    if (event.key === 'Home') index = 0;
    if (event.key === 'End') index = tabs.length-1;
    const next = tabs[(index+tabs.length)%tabs.length];
    showProductTab(next.id.slice(4)); next.focus();
  }));
  window.addEventListener('beforeunload', event => {
    if (hasProductChanges() && !savingProduct) { event.preventDefault(); event.returnValue = ''; }
  });
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
    if (!allowProductNavigation()) { select.value = selected; return; }
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

async function loadProducts(force = false) {
  if (!force && !allowProductNavigation()) return;
  try {
    const res = await apiFetch('/api/products/manage');
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'فشل تحميل المنتجات');
    allProducts = data.products || [];
    renderGroupLists(); renderProducts();
    if (currentProductId && allProducts.some(p => p.product_id === currentProductId)) editProduct(currentProductId, true);
    else newProduct(false, true);
  } catch (err) { showToast(err.message || 'فشل تحميل المنتجات', 'danger'); }
}

function filteredProducts() {
  const query = byId('productSearch').value.trim().toLocaleLowerCase('ar');
  const audience = byId('productAudienceFilter').value;
  const availability = byId('productAvailabilityFilter').value;
  const group = byId('productGroupFilter').value;
  return allProducts.filter(p => {
    const state = p.status !== 'active' ? 'hidden' : p.catalog_meta?.stock_state || 'unknown';
    return (!audience || (p.audience || p.catalog_meta?.audience) === audience)
      && (!availability || state === availability)
      && (!group || (p.recommendation_groups || []).includes(group))
      && (!query || ['product_id','product_name','keywords','category','colors','sizes','recommendation_groups']
        .some(key => String(p[key] || '').toLocaleLowerCase('ar').includes(query)));
  }).sort((a,b) => byId('productSort').value === 'priority'
    ? (b.recommendation_priority || 3)-(a.recommendation_priority || 3)
    : String(byId('productSort').value === 'code' ? a.product_id : a.product_name).localeCompare(
      String(byId('productSort').value === 'code' ? b.product_id : b.product_name), 'ar', {numeric:true}));
}

function renderProducts() {
  const list = filteredProducts();
  byId('productsCount').textContent = `${list.length} / ${allProducts.length}`;
  const target = byId('productsList'); target.replaceChildren();
  if (!list.length) target.innerHTML = '<p class="catalog-empty">ماكو منتجات تطابق هالبحث.<br>غيّر الفلاتر أو أضف منتجاً جديداً.</p>';
  for (const product of list) {
    const button = document.createElement('button'); button.type = 'button';
    button.className = 'product-row' + (product.product_id === currentProductId ? ' active' : '');
    button.setAttribute('aria-current', product.product_id === currentProductId ? 'true' : 'false');
    const state = product.status !== 'active' ? 'hidden' : product.catalog_meta?.stock_state || 'unknown';
    const stateLabel = {available:'متوفر',out:'غير متوفر',unknown:'يحتاج تأكيد',hidden:'مخفي'}[state];
    const image = (product.image_urls || [])[0];
    const audience = catalogSchema.audiences[product.audience || product.catalog_meta?.audience] || 'غير مصنف';
    button.innerHTML = `<span class="product-row-image">${image ? `<img src="${escAttr(image)}" alt="" loading="lazy">` : '<i class="bi bi-image"></i>'}</span><span class="product-row-info"><span class="product-row-name">${esc(product.product_name)}</span><span class="product-row-meta">${esc(product.product_id)} · ${esc(product.price || 'السعر غير محدد')}</span><span class="catalog-row-foot"><span class="catalog-stock ${state}">${stateLabel}</span><span class="catalog-audience-tag">${esc(audience)}</span></span></span>`;
    button.addEventListener('click', () => editProduct(product.product_id)); target.append(button);
  }
  updateProductNavigation();
}

function newProduct(rerender = true, force = false) {
  if (!force && !allowProductNavigation()) return;
  currentProductId = null; productImageColors = {}; relatedProductIds = new Set();
  byId('productForm').reset(); byId('productImages').value = '';
  byId('productStatus').value = 'active'; byId('productSizeSystem').value = 'legacy';
  byId('productRecommendationPriority').value = '3';
  byId('productAgeMinUnit').value = 'years'; byId('productAgeMaxUnit').value = 'years';
  const numbers = allProducts.map(p => /^P(\d+)$/.exec(p.product_id)).filter(Boolean).map(m => Number(m[1]));
  byId('productId').value = 'P' + String(Math.max(0,...numbers)+1).padStart(3,'0');
  byId('productId').disabled = false; byId('formTitle').textContent = 'إضافة منتج';
  byId('deleteProductBtn').hidden = true; byId('productFormError').hidden = true;
  byId('recommendationPreview').replaceChildren();
  renderImagePreview(); renderRelatedProducts(); updateSizeFields(); updateProductSummary();
  showProductTab('basics'); rememberProductSnapshot();
  if (rerender) { renderProducts(); showProductEditor(); } else updateProductNavigation();
}

function setSelectValue(id, value) {
  const element = byId(id); value = String(value ?? '');
  if (element.tagName === 'SELECT' && ![...element.options].some(option => option.value === value)) {
    element.add(new Option(value + ' — محفوظ سابقاً', value));
  }
  element.value = value;
}

function editProduct(productId, force = false) {
  if (!force && productId === currentProductId) { showProductEditor(); return; }
  if (!force && !allowProductNavigation()) return;
  const product = allProducts.find(p => p.product_id === productId); if (!product) return;
  currentProductId = productId;
  relatedProductIds = new Set(product.related_product_ids || []);
  for (const [key, id] of Object.entries(fields)) setSelectValue(id, product[key]);
  byId('productImages').value = imageText(product.image_url);
  productImageColors = normalizeImageColors(product.image_colors, imageLines());
  byId('relatedProductsSearch').value = '';
  byId('productRecommendationEnabled').checked = product.recommendation_enabled !== false;
  byId('productRecommendationPriority').value = String(product.recommendation_priority || 3);
  byId('productGroups').value = (product.recommendation_groups || []).join('، ');
  for (const [field, id] of [['age_min_months','productAgeMin'],['age_max_months','productAgeMax']]) {
    const value = product[field]; const years = value != null && value > 0 && value % 12 === 0;
    byId(id).value = value == null ? '' : years ? value/12 : value;
    byId(id+'Unit').value = years ? 'years' : 'months';
  }
  byId('productWeightMin').value = product.weight_min_kg ?? '';
  byId('productWeightMax').value = product.weight_max_kg ?? '';
  for (const checkbox of document.querySelectorAll('#productSalesCues input')) checkbox.checked = (product.sales_cues || []).includes(checkbox.value);
  for (const checkbox of document.querySelectorAll('#productOccasions input')) checkbox.checked = (product.occasions || []).includes(checkbox.value);
  byId('productId').disabled = true; byId('formTitle').textContent = product.product_name || productId;
  byId('deleteProductBtn').hidden = false; byId('productFormError').hidden = true;
  byId('recommendationPreview').replaceChildren();
  renderImagePreview(); renderProducts(); renderRelatedProducts(); updateSizeFields(); updateProductSummary(); rememberProductSnapshot();
  showProductEditor();
  if (window.innerWidth < 741 && !force) document.querySelector('.catalog-editor').scrollIntoView({behavior:'smooth',block:'start'});
}

function showProductEditor() { document.body?.classList?.add('catalog-product-open'); }
function showProductLibrary() { document.body?.classList?.remove('catalog-product-open'); byId('productSearch').focus(); }

function showProductTab(name) {
  for (const panel of document.querySelectorAll('.catalog-tabpanel')) panel.hidden = panel.id !== 'panel-'+name;
  for (const tab of document.querySelectorAll('.catalog-tabbar [role="tab"]')) {
    const selected = tab.id === 'tab-'+name; tab.setAttribute('aria-selected', String(selected)); tab.tabIndex = selected ? 0 : -1;
  }
}
function hasProductChanges() { return cleanProductSnapshot && JSON.stringify(collectPayload()) !== cleanProductSnapshot; }
function allowProductNavigation() { return !hasProductChanges() || confirm('عندك تعديلات غير محفوظة. تريد تتركها وتنتقل؟'); }
function rememberProductSnapshot() { cleanProductSnapshot = JSON.stringify(collectPayload()); markProductDirty(); }
function markProductDirty() { const dirty = hasProductChanges(); byId('saveState').textContent = dirty ? 'تعديلات غير محفوظة' : 'لا توجد تعديلات'; byId('saveState').classList.toggle('dirty', Boolean(dirty)); }
function navigateProduct(offset) { const list = filteredProducts(); const index = list.findIndex(p => p.product_id === currentProductId); if (list[index+offset]) editProduct(list[index+offset].product_id); }
function updateProductNavigation() { const list = filteredProducts(); const index = list.findIndex(p => p.product_id === currentProductId); byId('productPosition').textContent = index >= 0 ? `${index+1} / ${list.length}` : currentProductId ? 'خارج النتائج' : 'جديد'; byId('previousProduct').disabled = index <= 0; byId('nextProduct').disabled = index < 0 || index >= list.length-1; }
function groupNames() { return [...new Set(allProducts.flatMap(p => p.recommendation_groups || []))].sort((a,b) => a.localeCompare(b,'ar')); }
function renderGroupLists() { for (const id of ['productGroupFilter','existingGroupPicker']) { const selected = byId(id).value; byId(id).innerHTML = `<option value="">${id === 'productGroupFilter' ? 'كل المجموعات' : 'اختر مجموعة'}</option>`; for (const name of groupNames()) byId(id).add(new Option(name,name)); byId(id).value = selected; } }
function addExistingGroup() { const selected = byId('existingGroupPicker').value; if (!selected) return; const groups = [...new Set([...splitGroups(byId('productGroups').value),selected])]; byId('productGroups').value = groups.join('، '); markProductDirty(); }
function splitGroups(value) { return [...new Set(value.split(/[،,;؛\n]+/).map(v => v.trim()).filter(Boolean))]; }
function numericValue(id, multiplier=1) { const value = byId(id).value.trim(); return value === '' ? null : Number(value)*multiplier; }
function updateSizeFields() {
  const mode = byId('productSizeSystem').value || 'legacy';
  byId('ageSizeFields').hidden = mode !== 'age'; byId('weightSizeFields').hidden = mode !== 'weight';
  byId('legacySizeField').hidden = ['age','weight','none'].includes(mode);
  const payload = collectPayload(); let text = '';
  if (mode === 'age') text = `نطاق العمر: ${ageLabel(payload.age_min_months)} إلى ${ageLabel(payload.age_max_months)}`;
  if (mode === 'weight') text = `نطاق وزن تقريبي: ${payload.weight_min_kg ?? '—'} إلى ${payload.weight_max_kg ?? '—'} كيلو`;
  if (mode === 'none') text = 'هذا المنتج لا يحتاج قياساً.';
  byId('sizePreview').textContent = text; byId('sizePreview').hidden = !text;
}
function ageLabel(months) { if (months == null || !Number.isFinite(months)) return '—'; const years = Math.floor(months/12), rest = months%12; return [years ? `${years} سنة` : '', rest || !years ? `${rest} شهر` : ''].filter(Boolean).join(' و'); }
function updateProductSummary() {
  const p = collectPayload(); byId('productSummaryName').textContent = p.product_name || 'منتج جديد';
  byId('productSummaryMeta').textContent = [catalogSchema.audiences[p.audience],catalogSchema.product_types[p.product_type],p.price ? p.price+' د.ع' : ''].filter(Boolean).join(' · ') || 'حدد الجمهور ونوع القطعة ليسهل اقتراحها.';
  byId('productContext').textContent = currentProductId ? `المنتجات / ${p.product_id}` : 'المنتجات / إضافة جديدة';
  const image = p.image_url[0]; byId('productSummaryImage').innerHTML = image ? `<img src="${escAttr(image)}" alt="">` : '<i class="bi bi-bag"></i>';
  const sized = p.size_system === 'none' || (p.size_system === 'age' ? p.age_min_months != null && p.age_max_months != null : p.size_system === 'weight' ? p.weight_min_kg != null && p.weight_max_kg != null : Boolean(p.sizes));
  const checks = {'الاسم':p.product_name,'السعر':/[0-9٠-٩]/.test(p.price),'التوفر':['متوفر','نفذ'].includes(p.stock),'الجمهور':p.audience,'نوع القطعة':p.product_type,'المقاس':sized,'الألوان':p.colors,'الصور':p.image_url.length};
  const missing = Object.entries(checks).filter(([,value]) => !value).map(([label]) => label);
  byId('profileProgress').max = 8; byId('profileProgress').value = 8-missing.length;
  byId('profileReadiness').textContent = `${8-missing.length} / 8 بيانات مكتملة`;
  byId('missingProfileFields').textContent = missing.length ? 'ينقص: '+missing.slice(0,4).join('، ') : 'بيانات المنتج الأساسية مكتملة.';
}
async function previewRecommendations() {
  const button = byId('previewRecommendationsBtn'); button.disabled = true; byId('recommendationPreview').textContent = 'جاري فحص البدائل…';
  try {
    const response = await apiFetch('/api/products/recommendations/preview', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({product:collectPayload(),age_months:numericValue('previewAge'),size:byId('previewSize').value.trim(),audience:byId('previewAudience').value,occasion:byId('previewOccasion').value})});
    const data = await response.json(); if (!response.ok) throw new Error(data.error || 'تعذر فحص الاقتراح');
    byId('recommendationPreview').innerHTML = data.products.length ? data.products.map(p => `<div class="catalog-preview-card"><strong>${esc(p.product_name)}</strong>${esc(p.price)} د.ع · ${esc(p.colors)}<br>${esc(p.sizes)}</div>`).join('')+`<p>${esc(data.explanation)}</p>` : 'ماكو بديل يستوفي الطلب الحالي. راجع الربط أو المجموعة، الجمهور، التوفر والقياسات.';
  } catch (error) { byId('recommendationPreview').textContent = error.message; }
  finally { button.disabled = false; }
}

function relatedProductCandidates() {
  const storeId = new URLSearchParams(window.location.search).get('store_id') || 'default';
  const sourceId = currentProductId || document.getElementById('productId').value.trim();
  return allProducts.filter(product => (product.store_id || 'default') === storeId && product.product_id !== sourceId);
}

function renderRelatedProducts() {
  const options = document.getElementById('relatedProductsOptions');
  const selected = document.getElementById('relatedProductsSelected');
  const candidates = relatedProductCandidates();
  const sourceId = currentProductId || document.getElementById('productId').value.trim();
  relatedProductIds.delete(sourceId);
  const query = document.getElementById('relatedProductsSearch').value.trim().toLocaleLowerCase('ar');
  const visible = candidates.filter(product => !query || ['product_id', 'product_name', 'category', 'sizes', 'keywords']
    .some(field => String(product[field] || '').toLocaleLowerCase('ar').includes(query)));
  options.replaceChildren();
  selected.replaceChildren();
  for (const id of relatedProductIds) {
    const product = candidates.find(item => item.product_id === id);
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'btn btn-sm btn-outline-secondary';
    const name = product ? (product.product_name || id) : `${id} — غير موجود في هذا المتجر`;
    button.textContent = `${name} ×`;
    button.setAttribute('aria-label', `إزالة ربط ${name}`);
    button.addEventListener('click', () => { relatedProductIds.delete(id); renderRelatedProducts(); markProductDirty(); });
    selected.append(button);
  }
  visible.forEach((product, index) => {
    const label = document.createElement('label');
    label.className = 'd-flex gap-2 align-items-start py-2 border-bottom';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.className = 'form-check-input flex-shrink-0';
    checkbox.id = `relatedProductOption${index}`;
    checkbox.checked = relatedProductIds.has(product.product_id);
    checkbox.addEventListener('change', () => {
      if (checkbox.checked) relatedProductIds.add(product.product_id);
      else relatedProductIds.delete(product.product_id);
      renderRelatedProducts(); markProductDirty();
    });
    const text = document.createElement('span');
    text.className = 'small';
    text.textContent = [product.product_name || product.product_id, product.product_id, product.category,
      product.sizes, product.status !== 'active' ? 'مخفي — لن يقترحه المساعد' : product.stock].filter(Boolean).join(' · ');
    label.append(checkbox, text);
    options.append(label);
  });
  if (!visible.length) {
    const empty = document.createElement('p');
    empty.className = 'small text-muted mb-0';
    empty.textContent = candidates.length ? 'لا توجد منتجات تطابق البحث.' : 'أضيفي موديلًا آخر إلى هذا المتجر حتى تربطيه.';
    options.append(empty);
  }
  document.getElementById('relatedProductsCount').textContent = `المختار: ${relatedProductIds.size} · نتائج البحث: ${visible.length}`;
}

async function saveProduct(event) {
  event.preventDefault();
  const payload = collectPayload();
  if (!payload.product_id || !payload.product_name) {
    showProductTab('basics');
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
    cleanProductSnapshot = '';
    showToast('تم حفظ المنتج', 'success');
    await loadProducts(true);
    editProduct(currentProductId, true);
  } catch (err) {
    byId('productFormError').textContent = err.message || 'فشل حفظ المنتج'; byId('productFormError').hidden = false;
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
    cleanProductSnapshot = '';
    currentProductId = null;
    await loadProducts(true);
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
  payload.age_min_months = numericValue('productAgeMin', byId('productAgeMinUnit').value === 'years' ? 12 : 1);
  payload.age_max_months = numericValue('productAgeMax', byId('productAgeMaxUnit').value === 'years' ? 12 : 1);
  payload.weight_min_kg = numericValue('productWeightMin'); payload.weight_max_kg = numericValue('productWeightMax');
  payload.recommendation_groups = splitGroups(byId('productGroups').value);
  payload.occasions = [...document.querySelectorAll('#productOccasions input:checked')].map(input => input.value);
  payload.recommendation_enabled = byId('productRecommendationEnabled').checked;
  payload.recommendation_priority = Number(byId('productRecommendationPriority').value || 3);
  payload.image_url = imageLines();
  payload.image_colors = normalizeImageColors(productImageColors, payload.image_url);
  payload.related_product_ids = [...relatedProductIds];
  payload.sales_cues = Array.from(document.querySelectorAll('#productSalesCues input[name="sales_cues"]:checked'), checkbox => checkbox.value);
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
  markProductDirty();
}

async function uploadProductImages(input) {
  const files = Array.from(input.files || []);
  if (!files.length) return;
  const progress = document.getElementById('productUploadProgress');
  progress.innerHTML = '<span class="spinner-border spinner-border-sm"></span> جاري رفع الصور...';
  const form = new FormData();
  files.forEach(file => form.append('images', file));
  form.append('purpose', 'product');
  try {
    const response = await apiFetch('/api/upload_image', { method: 'POST', body: form });
    const result = await response.json();
    if (!response.ok) throw new Error(result.error || 'فشل رفع الصور');
    const urls = [...imageLines(), ...(result.image_urls || [])];
    document.getElementById('productImages').value = [...new Set(urls)].join('\n');
    productImageColors = normalizeImageColors(productImageColors, imageLines());
    renderImagePreview(); updateProductSummary(); markProductDirty();
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
  renderImagePreview(); updateProductSummary(); markProductDirty();
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
  savingProduct = saving;
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
  if (!window.bootstrap) { setTimeout(() => toastEl.remove(), 4000); return; }
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
