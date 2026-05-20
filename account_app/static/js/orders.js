let allOrders = [];
let editModal = null;

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

async function loadOrders() {
  const body = document.getElementById('ordersBody');
  body.innerHTML = `
    <tr>
      <td colspan="10" class="text-center py-5" style="color:var(--text-muted)">
        <div class="spinner-border spinner-border-sm mb-2"></div>
        <div class="small">جاري تحميل الطلبات...</div>
      </td>
    </tr>`;
  const from = document.getElementById('orderDateFrom')?.value || '';
  const to = document.getElementById('orderDateTo')?.value || '';
  let url = '/api/orders?';
  if (from) url += `date_from=${from}&`;
  if (to) url += `date_to=${to}&`;
  
  const res = await fetch(orderApi(url));
  if (!res.ok) {
    body.innerHTML = `<tr><td colspan="10" class="text-center py-5 text-danger">فشل تحميل الطلبات</td></tr>`;
    return;
  }
  const data = await res.json();
  allOrders = data.orders || [];
  document.getElementById('ordersTotal').textContent = data.total ?? allOrders.length;
  document.getElementById('ordersNew').textContent = data.new_count ?? allOrders.filter(o => (o.status || 'new') === 'new').length;
  document.getElementById('ordersPeople').textContent = data.people_count ?? 0;
  document.getElementById('ordersConversion').textContent =
    `${Number(data.people_to_order_conversion || data.conversion_rate || 0).toFixed(1)}%`;
  renderOrders();
}

function renderOrders() {
  const body = document.getElementById('ordersBody');
  const query = (document.getElementById('orderSearch').value || '').trim().toLowerCase();
  const orders = query ? allOrders.filter(order => orderSearchText(order).includes(query)) : allOrders;

  if (!orders.length) {
    body.innerHTML = `<tr><td colspan="10" class="text-center py-5" style="color:var(--text-muted)">لا توجد طلبات مطابقة</td></tr>`;
    return;
  }

  body.innerHTML = orders.map(order => {
    const customer = order.customer_name || order.customer_display_name || order.sender_id || '-';
    const product = [order.product_name, order.product_id ? `(${order.product_id})` : ''].filter(Boolean).join(' ');
    return `
      <tr>
        <td class="text-muted">#${esc(order.id)}</td>
        <td>${esc(formatTime(order.created_at))}</td>
        <td>
          <div class="fw-semibold">${esc(customer)}</div>
          <div class="small" style="color:var(--text-muted)">${esc(order.sender_id || '')}</div>
        </td>
        <td dir="ltr">${esc(order.phone || '-')}</td>
        <td>${esc(product || '-')}</td>
        <td>${esc(order.province || '-')}</td>
        <td class="orders-address">${esc(order.address || '-')}</td>
        <td>${esc(order.size || '-')}</td>
        <td><span class="badge bg-success">${esc(order.status || 'new')}</span></td>
        <td>
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
  if (el) el.value = value || '';
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
    renderOrders();
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
  loadOrders();
});
