let catalogImagesState = [];

async function catalogJSON(path, options = {}) {
  const res = await fetch(adminApi(path), options);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || data.message || 'فشل الطلب');
  return data;
}

function initCatalogImagesPage() {
  const form = document.getElementById('catalogImagesForm');
  form?.addEventListener('submit', uploadCatalogImages);
  loadCatalogImages();
}

async function loadCatalogImages() {
  const grid = document.getElementById('catalogImagesGrid');
  const meta = document.getElementById('catalogImagesMeta');
  if (grid) {
    grid.innerHTML = '<div class="catalog-empty-state">جاري تحميل الصور...</div>';
  }
  try {
    const data = await catalogJSON('/api/settings/catalog_images');
    catalogImagesState = data.images || [];
    renderCatalogImages(data);
    setAdminStatus('catalogImagesStatus', '', true);
  } catch (err) {
    if (meta) meta.textContent = 'تعذر تحميل صور الكتالوج';
    if (grid) grid.innerHTML = `<div class="catalog-empty-state text-danger">${adminEsc(err.message)}</div>`;
    setAdminStatus('catalogImagesStatus', err.message, false);
  }
}

async function uploadCatalogImages(event) {
  event.preventDefault();
  const input = document.getElementById('catalogImagesInput');
  const files = Array.from(input?.files || []);
  if (!files.length) {
    setAdminStatus('catalogImagesStatus', 'اختر صورة واحدة على الأقل', false);
    return;
  }

  const formData = new FormData();
  files.forEach((file) => formData.append('images', file));
  setAdminStatus('catalogImagesStatus', 'جاري رفع الصور...', true);

  try {
    const data = await catalogJSON('/api/settings/catalog_images', {
      method: 'POST',
      body: formData
    });
    if (input) input.value = '';
    catalogImagesState = data.images || [];
    renderCatalogImages(data);
    const savedCount = (data.saved || []).length;
    setAdminStatus('catalogImagesStatus', `تم رفع ${savedCount} صورة`);
  } catch (err) {
    setAdminStatus('catalogImagesStatus', err.message, false);
  }
}

function renderCatalogImages(data = {}) {
  const grid = document.getElementById('catalogImagesGrid');
  const meta = document.getElementById('catalogImagesMeta');
  const images = data.images || catalogImagesState || [];
  if (meta) {
    const model = data.catalog_match_model || '-';
    meta.textContent = `${images.length} صورة سترسل إلى الـ AI عند مطابقة صورة الزبون. الموديل: ${model}`;
  }
  if (!grid) return;
  if (!images.length) {
    grid.innerHTML = '<div class="catalog-empty-state">لا توجد صور كتالوج حالياً.</div>';
    return;
  }
  grid.innerHTML = images.map((image) => {
    const updated = image.updated_at ? new Date(image.updated_at).toLocaleString('ar-IQ') : '-';
    const size = Math.max(1, Math.round((image.size || 0) / 1024));
    const legacyLabel = image.managed ? '' : '<span class="catalog-chip">الصورة القديمة</span>';
    return `
      <article class="catalog-image-card">
        <div class="catalog-image-thumb">
          ${image.url ? `<img src="${adminEsc(image.url)}" alt="${adminEsc(image.filename || 'catalog image')}" loading="lazy">` : '<i class="bi bi-image"></i>'}
        </div>
        <div class="catalog-image-body">
          <div class="catalog-image-title" title="${adminEsc(image.filename || '')}">${adminEsc(image.filename || '-')}</div>
          <div class="catalog-image-meta">${size} KB · ${adminEsc(updated)}</div>
          ${legacyLabel}
        </div>
        <button class="btn btn-outline-danger btn-sm" type="button" onclick="deleteCatalogImage('${adminEsc(image.id)}')">
          <i class="bi bi-trash"></i>
          حذف
        </button>
      </article>
    `;
  }).join('');
}

async function deleteCatalogImage(imageId) {
  if (!imageId) return;
  if (!confirm('هل تريد حذف هذه الصورة من كتالوج الـ AI؟')) return;
  setAdminStatus('catalogImagesStatus', 'جاري الحذف...', true);
  try {
    const data = await catalogJSON(`/api/settings/catalog_images/${encodeURIComponent(imageId)}`, {
      method: 'DELETE'
    });
    catalogImagesState = data.images || [];
    renderCatalogImages(data);
    setAdminStatus('catalogImagesStatus', 'تم حذف الصورة');
  } catch (err) {
    setAdminStatus('catalogImagesStatus', err.message, false);
  }
}
