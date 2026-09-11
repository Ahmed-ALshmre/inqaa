let allOrders = [];
let editModal = null;
let orderRequest = 0;
let orderOffset = 0;
let orderSearchTimer;
function scheduleOrderSearch() { clearTimeout(orderSearchTimer); orderSearchTimer = setTimeout(() => loadOrders(), 250); }

const orderKey = new URLSearchParams(location.search).get('key') || '';

function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  }[ch]));
}

function orderApi(path) {
  const sep = path.includes('?') ? '&' : '?';
  return `${path}${sep}key=${encodeURIComponent(orderKey)}`;
}

function formatTime(value) {
  if (!value) return '-';
  const text = String(value).replace('T', ' ');
  return text.slice(0, 16);
}

function dashboardConversationUrl(senderId) {
  const params = new URLSearchParams();
  if (orderKey) params.set('key', orderKey);
  if (senderId) params.set('sender_id', senderId);
  const store = document.getElementById('orderStore')?.value;
  if (store && store !== 'all') params.set('store_id', store);
  return `/dashboard?${params.toString()}`;
}

function showOrderToast(message, type = 'success') {
  const container = document.getElementById('ordersToastContainer');
  if (!container) {
    alert(message);
    return;
  }
  const id = `toast-${Date.now()}`;
  container.insertAdjacentHTML('beforeend', `
    <div class="toast align-items-center text-bg-${type} border-0" role="alert" id="${id}">
      <div class="d-flex">
        <div class="toast-body">${esc(message)}</div>
        <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
      </div>
    </div>
  `);
  const el = document.getElementById(id);
  const toast = new bootstrap.Toast(el, { delay: 2600 });
  el.addEventListener('hidden.bs.toast', () => el.remove());
  toast.show();
}

function orderSearchText(order) {
  return [
    order.id,
    order.sender_id,
    order.customer_name,
    order.customer_display_name,
    order.phone,
    order.province,
    order.address,
    order.product_id,
    order.product_name,
    order.color,
    order.size,
    order.notes,
    order.status
  ].join(' ').toLowerCase();
}

async function loadOrders(append = false) {
  const request = ++orderRequest;
  const body = document.getElementById('ordersBody');
  const more = document.getElementById('ordersMore');
  more.disabled = true;
  if (!append) {
    orderOffset = 0;
    allOrders = [];
    more.hidden = true;
    document.getElementById('ordersLoaded').textContent = '';
    body.innerHTML = '<tr class="orders-empty-row"><td colspan="11">جاري تحميل الطلبات…</td></tr>';
    for (const id of ['ordersTotal','ordersNew','ordersPeople','ordersConversion']) document.getElementById(id).textContent = '—';
  }
  const params = new URLSearchParams({offset: String(orderOffset), limit: '100'});
  for (const [key, id] of [['date_from','orderDateFrom'],['date_to','orderDateTo'],['store_id','orderStore'],['q','orderSearch']]) {
    const value = document.getElementById(id)?.value?.trim();
    if (value) params.set(key, value);
  }
  try {
    const res = await fetch(orderApi('/api/orders?' + params));
    const data = await res.json();
    if (request !== orderRequest) return;
    if (!res.ok) throw new Error(data.error || 'فشل تحميل الطلبات');
    allOrders = append ? allOrders.concat(data.orders || []) : (data.orders || []);
    orderOffset = data.next_offset;
    document.getElementById('ordersTotal').textContent = data.total;
    document.getElementById('ordersNew').textContent = data.new_count;
    document.getElementById('ordersPeople').textContent = data.people_count;
    document.getElementById('ordersConversion').textContent = `${Number(data.people_to_order_conversion || 0).toFixed(1)}%`;
    more.hidden = !data.has_more;
    document.getElementById('ordersLoaded').textContent = `المعروض: ${allOrders.length} طلب`;
    renderOrders();
  } catch (err) {
    if (request !== orderRequest) return;
    if (!append) body.innerHTML = `<tr class="orders-empty-row"><td colspan="11">${esc(err.message)}</td></tr>`;
    else showOrderToast(err.message, 'danger');
  } finally {
    if (request === orderRequest) more.disabled = false;
  }
}

function renderOrders() {
  const body = document.getElementById('ordersBody');
  const orders = allOrders;

  if (!orders.length) {
    body.innerHTML = `<tr class="orders-empty-row"><td colspan="11"><i class="bi bi-bag-check"></i><strong>لا توجد طلبات مطابقة</strong><span>ستظهر الطلبات الجديدة هنا تلقائيًا</span></td></tr>`;
    return;
  }

  body.innerHTML = orders.map(order => {
    const customer = order.customer_name || order.customer_display_name || order.sender_id || '-';
    const items = Array.isArray(order.items) ? order.items : [];
    const product = items.length
      ? items.map(item => esc(`${item.product_name || item.product_id || '-'} ×${item.quantity || 1}${item.color ? ` · ${item.color}` : ''}${item.size ? ` · ${item.size}` : ''}`)).join('<br>')
      : esc([order.product_name, order.product_id ? `(${order.product_id})` : ''].filter(Boolean).join(' '));
    return `
      <tr>
        <td data-label="رقم الطلب" class="text-muted">#${esc(order.id)}</td>
        <td data-label="الوقت">${esc(formatTime(order.created_at))}</td>
        <td data-label="الزبون">
          <div class="fw-semibold">${esc(customer)}</div>
          <div class="small" style="color:var(--text-muted)">${esc(order.sender_id || '')}</div>
        </td>
        <td data-label="الهاتف" dir="ltr">${esc(order.phone || '-')}</td>
        <td data-label="المنتج" class="order-items-cell">${product || '-'}</td>
        <td data-label="المحافظة">${esc(order.province || '-')}</td>
        <td data-label="العنوان" class="orders-address">${esc(order.address || '-')}</td>
        <td data-label="القياس">${esc(order.size || '-')}</td>
        <td data-label="مع التوصيل">${order.total_amount == null ? 'غير محفوظ' : Number(order.total_amount).toLocaleString('en-US') + ' د.ع'}${order.is_paid ? '<div class="text-success small">مدفوع بالكامل</div>' : ''}</td>
        <td data-label="الحالة"><span class="badge bg-success">${esc(order.status || 'new')}</span></td>
        <td data-label="الإجراءات">
          <div class="orders-actions">
            <a class="btn btn-sm btn-outline-info" href="${esc(dashboardConversationUrl(order.sender_id || ''))}" title="فتح المحادثة">
              <i class="bi bi-chat-dots"></i>
              <span>المحادثة</span>
            </a>
            <button class="btn btn-sm btn-outline-warning" type="button" onclick="openEditOrder(${Number(order.id)})" title="تعديل الطلب">
              <i class="bi bi-pencil-square"></i>
              <span>تعديل</span>
            </button>
            <button class="btn btn-sm btn-outline-success" type="button" onclick="resendOrderTelegram(${Number(order.id)}, this)" title="إرسال الطلب إلى التلكرام مرة أخرى">
              <i class="bi bi-telegram"></i>
              <span>تلكرام</span>
            </button>
          </div>
        </td>
      </tr>`;
  }).join('');
}

function setEditValue(id, value) {
  const el = document.getElementById(id);
  if (el) el.value = value ?? '';
}

function openEditOrder(orderId) {
  const order = allOrders.find(item => Number(item.id) === Number(orderId));
  if (!order) return;
  setEditValue('editOrderId', order.id);
  setEditValue('editCustomerName', order.customer_name);
  setEditValue('editPhone', order.phone);
  setEditValue('editProvince', order.province);
  setEditValue('editAddress', order.address);
  setEditValue('editProductName', order.product_name);
  setEditValue('editProductId', order.product_id);
  setEditValue('editColor', order.color);
  setEditValue('editSize', order.size);
  setEditValue('editNotes', order.notes);
  setEditValue('editProductTotal', order.product_total);
  setEditValue('editDeliveryFee', order.delivery_fee);
  setEditValue('editStatus', order.status || 'new');
  document.getElementById('orderEditStatus').textContent = '';
  editModal ||= new bootstrap.Modal(document.getElementById('orderEditModal'));
  editModal.show();
}

async function saveEditedOrder(event) {
  event.preventDefault();
  const orderId = document.getElementById('editOrderId').value;
  const status = document.getElementById('orderEditStatus');
  status.textContent = 'جاري الحفظ...';
  const payload = {
    customer_name: document.getElementById('editCustomerName').value,
    phone: document.getElementById('editPhone').value,
    province: document.getElementById('editProvince').value,
    address: document.getElementById('editAddress').value,
    product_name: document.getElementById('editProductName').value,
    product_id: document.getElementById('editProductId').value,
    color: document.getElementById('editColor').value,
    size: document.getElementById('editSize').value,
    notes: document.getElementById('editNotes').value,
    status: document.getElementById('editStatus').value,
  };
  const productTotal = document.getElementById('editProductTotal').value;
  const deliveryFee = document.getElementById('editDeliveryFee').value;
  if (productTotal !== '' || deliveryFee !== '') { payload.product_total = productTotal; payload.delivery_fee = deliveryFee; }
  try {
    const res = await fetch(orderApi(`/api/orders/${encodeURIComponent(orderId)}`), {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) throw new Error(data.error || 'فشل حفظ الطلب');
    const index = allOrders.findIndex(item => Number(item.id) === Number(orderId));
    if (index >= 0) allOrders[index] = data.order;
    await loadOrders();
    editModal?.hide();
    showOrderToast('تم حفظ تعديل الطلب', 'success');
  } catch (err) {
    status.textContent = err.message || 'فشل حفظ الطلب';
  }
}

async function resendOrderTelegram(orderId, btn) {
  const oldHtml = btn?.innerHTML;
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span><span>إرسال</span>';
  }
  try {
    const res = await fetch(orderApi(`/api/orders/${encodeURIComponent(orderId)}/resend_telegram`), { method: 'POST' });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) throw new Error(data.error || 'تعذر إرسال الطلب إلى التلكرام');
    showOrderToast('تم إرسال الطلب إلى التلكرام مرة أخرى', 'success');
  } catch (err) {
    showOrderToast(err.message || 'تعذر إرسال الطلب إلى التلكرام', 'danger');
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = oldHtml;
    }
  }
}

document.addEventListener('DOMContentLoaded', () => {
  const today = new Date();
  today.setMinutes(today.getMinutes() - today.getTimezoneOffset());
  const todayStr = today.toISOString().split('T')[0];
  const df = document.getElementById('orderDateFrom');
  const dt = document.getElementById('orderDateTo');
  if (df) df.value = todayStr;
  if (dt) dt.value = todayStr;

  document.getElementById('orderEditForm')?.addEventListener('submit', saveEditedOrder);
  const selectedStore = new URLSearchParams(location.search).get('store_id') || 'all';
  const select = document.getElementById('orderStore');
  if (selectedStore !== 'all') select.add(new Option(selectedStore, selectedStore, true, true));
  fetch(orderApi('/api/stores')).then(res => res.json()).then(data => {
    for (const store of data.stores || []) {
      const existing = [...select.options].find(o => o.value === store.store_id);
      if (existing) existing.textContent = store.name || store.store_id;
      else select.add(new Option(store.name || store.store_id, store.store_id));
    }
  }).catch(() => {});
  loadOrders();
});
