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
  if (panel) {
    const dialog = toolDialog('customerToolsDialog','بيانات الزبون وأدوات المحادثة');
    const desktop = document.getElementById('desktopCustomerTools');
    const query = matchMedia('(min-width:1440px)');
    const arrange = () => {
      if (dialog.open) dialog.close();
      if (desktop) desktop.hidden = !query.matches;
      (query.matches && desktop ? desktop : dialog).append(panel);
    };
    query.addEventListener('change', arrange); arrange();
  }
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
async function openImageDialog() {
  if(!currentSenderId) return showToast('اختر محادثة أولاً','warning');
  if(_isSending) return showToast('انتظر اكتمال الإرسال','warning');
  const sender=currentSenderId;
  const store=currentCustomer?.store_id || 'default';
  const dialog=toolDialog('imageChooseDialog','اختيار الصور'); dialog.querySelector('.tool-content')?.remove();
  const content=document.createElement('div'); content.className='tool-content';
  const selected=new Set(), choices=new Map();
  const upload=document.createElement('button'); upload.className='btn btn-primary'; upload.textContent='اختيار عدة صور من الجهاز';
  upload.onclick=()=>{dialog.close();if(sender===currentSenderId)document.getElementById('imageUpload').click();};content.append(upload);
  const hint=document.createElement('p'); hint.className='mt-3 small'; hint.textContent='حدد الصور المطلوبة من المكتبة والمنتجات، ثم أضفها للرسالة. حتى 10 صور في الدفعة الواحدة.';content.append(hint);
  const title=document.createElement('h3');title.className='h6';title.textContent='صورك المحفوظة';content.append(title);
  const grid=document.createElement('div');grid.className='tool-image-grid';content.append(grid);
  const footer=document.createElement('div');footer.className='image-selection-footer';
  const count=document.createElement('span');
  const confirm=document.createElement('button');confirm.className='btn btn-primary';confirm.type='button';confirm.textContent='إضافة للرسالة';
  footer.append(count,confirm);
  function updateSelection(){
    count.textContent=`${selected.size} محددة · ${composerImages.length} في الرسالة`;
    confirm.disabled=!selected.size;
    for(const [url,buttons] of choices)for(const button of buttons)button.setAttribute('aria-pressed',String(selected.has(url) || composerImages.some(item=>item.url===url)));
  }
  function addImage(target,url,label){
    if(!safeMediaUrl(url))return;
    const button=document.createElement('button');button.type='button';button.className='tool-image-choice';button.setAttribute('aria-label','تحديد '+label);
    const image=document.createElement('img');image.src=url;image.alt=label;image.loading='lazy';
    const caption=document.createElement('span');caption.textContent=label;button.append(image,caption);
    if(!choices.has(url))choices.set(url,[]);choices.get(url).push(button);
    button.disabled=composerImages.some(item=>item.url===url);
    button.onclick=()=>{
      if(sender!==currentSenderId || _isSending)return;
      if(selected.has(url))selected.delete(url);
      else if(selected.size+composerImages.length<COMPOSER_IMAGE_LIMIT)selected.add(url);
      else showToast('يمكن اختيار 10 صور في الدفعة الواحدة','warning');
      updateSelection();
    };target.append(button);updateSelection();
  }
  confirm.onclick=()=>{if(sender!==currentSenderId || _isSending)return;if(addComposerImages([...selected]))dialog.close();};
  const state=document.createElement('p');state.className='small';content.append(state);
  const more=document.createElement('button');more.type='button';more.className='btn btn-outline-primary';more.textContent='عرض المزيد';more.hidden=true;content.append(more);
  let offset=0;
  async function loadSaved(){
    more.disabled=true;state.textContent='جاري تحميل الصور المحفوظة…';
    try{
      const response=await apiFetch('/api/image_library?store_id='+encodeURIComponent(store)+'&offset='+offset);
      const data=await response.json();if(!response.ok)throw Error(data.error || 'تعذر تحميل المكتبة');
      if(sender!==currentSenderId || !dialog.open)return;
      for(const item of data.images || [])addImage(grid,item.image_url,item.name || 'صورة محفوظة');
      offset=data.next_offset;more.hidden=!data.has_more;
      state.textContent=grid.children.length ? '' : 'ارفع أول صورة لتظهر في المكتبة.';
    }catch(error){state.textContent=error.message;more.hidden=false;more.textContent='إعادة المحاولة';}
    finally{more.disabled=false;}
  }
  more.onclick=loadSaved;
  const productTitle=document.createElement('h3');productTitle.className='h6 mt-3';productTitle.textContent='صور المنتجات';content.append(productTitle);
  const productsGrid=document.createElement('div');productsGrid.className='tool-image-grid';content.append(productsGrid);
  for(const product of allProducts)for(const url of productImageList(product))addImage(productsGrid,url,product.product_name || 'صورة المنتج');
  content.append(footer);updateSelection();dialog.append(content);dialog.showModal();await loadSaved();
}
