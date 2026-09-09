function platformBadge(platform) {
  const names = {instagram:['instagram','إنستغرام'], facebook:['messenger','ماسنجر'], messenger:['messenger','ماسنجر'], whatsapp:['whatsapp','واتساب']};
  const item = names[String(platform || '').toLowerCase()] || ['chat-dots','مصدر غير محدد'];
  return `<span class="platform-badge platform-${item[0]}" title="${item[1]}" aria-label="${item[1]}"><i class="bi bi-${item[0]}" aria-hidden="true"></i></span>`;
}
function toolDialog(id, title) {
  let dialog = document.getElementById(id); if(dialog) return dialog;
  dialog = document.createElement('dialog'); dialog.id = id; dialog.className = 'workspace-dialog';
  const header = document.createElement('div'); header.className = 'dialog-heading';
  const heading = document.createElement('h2'); heading.textContent = title;
  const close = document.createElement('button'); close.type='button'; close.textContent='×'; close.setAttribute('aria-label','إغلاق'); close.onclick=()=>dialog.close();
  header.append(heading,close); dialog.append(header); document.body.append(dialog); return dialog;
}
document.addEventListener('DOMContentLoaded', () => {
  const panel = document.getElementById('controlPanel');
  if(panel) toolDialog('customerToolsDialog','بيانات الزبون وأدوات المحادثة').append(panel);
});
async function openCatalogDialog() {
  if(!currentSenderId) return showToast('اختر محادثة أولاً','warning');
  const dialog = toolDialog('catalogSendDialog','كتالوج المتجر');
  dialog.querySelector('.tool-content')?.remove();
  const content=document.createElement('div'); content.className='tool-content'; content.textContent='جاري تحميل الكتالوج…'; dialog.append(content); dialog.showModal();
  const sender=currentSenderId;
  try {
    const response=await apiFetch('/api/catalog_image?store_id='+encodeURIComponent(currentCustomer?.store_id || 'default'));
    const data=await response.json(); if(!response.ok) throw Error(data.error || 'تعذر تحميل الكتالوج');
    content.replaceChildren();
    const summary=document.createElement('p'); summary.textContent=`${data.images_count || 0} صور · سترسل إلى ${currentCustomer?.name || 'المحادثة الحالية'}`; content.append(summary);
    const grid=document.createElement('div'); grid.className='tool-image-grid';
    for(const item of data.images || []) {const image=document.createElement('img'); image.src=item.url; image.alt='معاينة الكتالوج'; grid.append(image);}
    content.append(grid);
    const send=document.createElement('button'); send.className='btn btn-primary mt-3'; send.textContent='إرسال الكتالوج'; send.disabled=!data.images_count;
    send.onclick=async()=>{if(sender!==currentSenderId)return; send.disabled=true; await sendCatalog(); dialog.close();}; content.append(send);
  } catch(error){content.textContent=error.message;}
}
function openImageDialog() {
  if(!currentSenderId) return showToast('اختر محادثة أولاً','warning');
  const dialog=toolDialog('imageChooseDialog','اختر صورة لإرفاقها'); dialog.querySelector('.tool-content')?.remove();
  const content=document.createElement('div'); content.className='tool-content';
  const upload=document.createElement('button'); upload.className='btn btn-primary'; upload.textContent='رفع صورة من الجهاز'; upload.onclick=()=>{dialog.close();document.getElementById('imageUpload').click();}; content.append(upload);
  const hint=document.createElement('p'); hint.className='mt-3'; hint.textContent='أو اختر من صور المنتجات. ستظهر الصورة بجانب الرسالة قبل إرسالها.'; content.append(hint);
  const grid=document.createElement('div');grid.className='tool-image-grid';
  for(const product of allProducts) for(const url of productImageList(product)) {
    const button=document.createElement('button'); button.type='button'; button.className='tool-image-choice';
    const image=document.createElement('img'); image.src=url; image.alt=product.product_name || 'صورة المنتج';
    const label=document.createElement('span'); label.textContent=product.product_name || 'صورة المنتج'; button.append(image,label);
    button.onclick=()=>{uploadedImageUrl=url;document.getElementById('previewImg').src=url;document.getElementById('imagePreview').style.cssText='display:block!important';dialog.close();};grid.append(button);
  }
  content.append(grid);dialog.append(content);dialog.showModal();
}
