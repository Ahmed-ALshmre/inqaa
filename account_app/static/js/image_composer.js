// Image drafts belong to one conversation. Upload at most two files at a time.
let composerUploadActive = 0;
let composerImageSequence = 0;
const COMPOSER_IMAGE_LIMIT = 10;

function renderComposerImages() {
  const section = document.getElementById('imagePreview');
  const list = document.getElementById('composerImageList');
  if (!section || !list) return;
  section.hidden = composerImages.length === 0;
  const clearButton = document.getElementById('clearComposerImagesBtn');
  if (clearButton) clearButton.disabled = _isSending;
  list.replaceChildren();
  const pending = composerImages.filter(item => ['queued','uploading'].includes(item.status)).length;
  const failed = composerImages.filter(item => item.status === 'error').length;
  document.getElementById('imageUploadStatus').textContent = pending
    ? `${composerImages.length} صور · جاري تجهيز ${pending}…`
    : failed ? `تعذر رفع ${failed} · أعد المحاولة أو أزل الصورة` : `${composerImages.length} صور جاهزة · إرسال دفعة واحدة`;
  for (const item of composerImages) {
    const card = document.createElement('div'); card.className = 'composer-image-item' + (item.status === 'error' ? ' is-error' : '');
    const img = document.createElement('img'); img.src = item.preview; img.alt = item.name || 'صورة محددة';
    const remove = document.createElement('button'); remove.type = 'button'; remove.textContent = '×'; remove.disabled = _isSending;
    remove.setAttribute('aria-label', 'إزالة ' + img.alt); remove.onclick = () => removeComposerImage(item.id);
    const state = document.createElement('small'); state.textContent = {queued:'بالانتظار',uploading:'جاري الرفع…',ready:'جاهزة',error:'تعذر الرفع'}[item.status];
    card.append(img, remove, state);
    if (item.status === 'error') {
      const retry = document.createElement('button'); retry.type = 'button'; retry.className = 'retry-image'; retry.textContent = 'إعادة المحاولة'; retry.disabled = _isSending;
      retry.onclick = () => { queueComposerUpload(item); renderComposerImages(); pumpComposerUploads(); }; card.append(retry);
    }
    list.append(card);
  }
}

function addComposerImages(urls) {
  if (!currentSenderId || _isSending) return false;
  const unique = [...new Set(urls)].filter(url => safeMediaUrl(url) && !composerImages.some(item => item.url === url));
  if (unique.length + composerImages.length > COMPOSER_IMAGE_LIMIT) { showToast('يمكن إرسال 10 صور في الدفعة الواحدة', 'warning'); return false; }
  for (const url of unique) composerImages.push({id:++composerImageSequence,url,preview:url,status:'ready',ready:Promise.resolve(true)});
  imageUploadGeneration++;
  renderComposerImages();
  return true;
}

function queueComposerUpload(item) {
  item.status = 'queued'; item.error = '';
  item.ready = new Promise(resolve => { item.resolve = resolve; });
}

function pumpComposerUploads() {
  while (composerUploadActive < 2) {
    const item = composerImages.find(entry => entry.status === 'queued');
    if (!item) return;
    item.status = 'uploading'; composerUploadActive++;
    uploadComposerImage(item).finally(() => { composerUploadActive--; pumpComposerUploads(); });
  }
}

async function uploadComposerImage(item) {
  const controller = new AbortController(); item.controller = controller;
  const timeout = setTimeout(() => controller.abort(), 60000);
  try {
    const prepared = await prepareComposerImage(item.file);
    if (controller.signal.aborted) throw Object.assign(Error('Upload timed out'), {name:'AbortError'});
    if (!composerImages.includes(item) || item.sender !== currentSenderId) return;
    const body = new FormData(); body.append('image',prepared); body.append('purpose','library');
    const response = await apiFetch('/api/upload_image?store_id=' + encodeURIComponent(item.store), {method:'POST',body,signal:controller.signal});
    const data = await response.json();
    if (!response.ok || !data.image_url) throw Error(data.error || 'تعذر رفع الصورة');
    item.url = data.image_url; item.status = 'ready';
  } catch (error) {
    item.status = 'error'; item.error = error.name === 'AbortError' ? 'انتهت مهلة الرفع' : error.message;
  } finally {
    clearTimeout(timeout); item.controller = null; item.resolve?.(item.status === 'ready');
    if (composerImages.includes(item)) renderComposerImages();
  }
}

async function handleImageUpload(event) {
  const files = Array.from(event.target.files || []); event.target.value = '';
  if (!files.length || !currentSenderId || _isSending) return;
  if (files.length + composerImages.length > COMPOSER_IMAGE_LIMIT) { showToast('يمكن اختيار 10 صور في الدفعة الواحدة', 'warning'); return; }
  if (files.some(file => !['image/jpeg','image/png','image/webp'].includes(file.type))) { showToast('اختر صور JPG أو PNG أو WEBP', 'warning'); return; }
  if (files.some(file => file.size === 0 || file.size > 20 * 1024 * 1024)) { showToast('الحد الأقصى 20 ميغابايت لكل صورة؛ لا تقبل الملفات الفارغة', 'warning'); return; }
  for (const file of files) {
    const item = {id:++composerImageSequence,file,name:file.name,preview:URL.createObjectURL(file),objectUrl:true,sender:currentSenderId,store:currentCustomer?.store_id || 'default'};
    queueComposerUpload(item); composerImages.push(item);
  }
  imageUploadGeneration++;
  renderComposerImages(); pumpComposerUploads();
}

function removeComposerImage(id) {
  if (_isSending) return;
  const item = composerImages.find(entry => entry.id === id); if (!item) return;
  item.controller?.abort(); item.resolve?.(false);
  if (item.objectUrl) URL.revokeObjectURL(item.preview);
  composerImages = composerImages.filter(entry => entry !== item); imageUploadGeneration++;
  renderComposerImages(); pumpComposerUploads();
}

async function waitForComposerImages() {
  const selected = [...composerImages];
  const results = await Promise.all(selected.map(item => item.ready));
  if (results.some(ok => !ok) || selected.some(item => item.status !== 'ready')) {
    showToast('بعض الصور لم تجهز؛ أعد رفعها أو أزلها قبل الإرسال', 'warning'); return false;
  }
  return true;
}

function clearImage() {
  imageUploadGeneration++;
  composerSendIdentity = null;
  for (const item of composerImages) {
    item.controller?.abort(); item.resolve?.(false);
    if (item.objectUrl) URL.revokeObjectURL(item.preview);
  }
  composerImages = [];
  const input = document.getElementById('imageUpload'); if (input) input.value = '';
  renderComposerImages();
}
