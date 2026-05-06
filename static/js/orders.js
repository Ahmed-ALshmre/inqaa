let allOrders = [];

function getDashboardKey() {
  const fromUrl = new URLSearchParams(window.location.search).get('key') || '';
  const fromCookie = document.cookie.split('; ').find(row => row.startsWith('dashboard_key='))?.split('=')[1] || '';
  return fromUrl || decodeURIComponent(fromCookie);
}

const orderKey = getDashboardKey();

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
      <td colspan="9" class="text-center py-5" style="color:var(--text-muted)">
        <div class="spinner-border spinner-border-sm mb-2"></div>
        <div class="small">جاري تحميل الطلبات...</div>
      </td>
    </tr>`;
  const res = await fetch(orderApi('/api/orders'));
  if (!res.ok) {
    body.innerHTML = `<tr><td colspan="9" class="text-center py-5 text-danger">فشل تحميل الطلبات</td></tr>`;
    return;
  }
  const data = await res.json();
  allOrders = data.orders || [];
  document.getElementById('ordersTotal').textContent = data.total ?? allOrders.length;
  document.getElementById('ordersNew').textContent = data.new_count ?? allOrders.filter(o => (o.status || 'new') === 'new').length;
  renderOrders();
}

function renderOrders() {
  const body = document.getElementById('ordersBody');
  const query = (document.getElementById('orderSearch').value || '').trim().toLowerCase();
  const orders = query ? allOrders.filter(order => orderSearchText(order).includes(query)) : allOrders;

  if (!orders.length) {
    body.innerHTML = `<tr><td colspan="9" class="text-center py-5" style="color:var(--text-muted)">لا توجد طلبات مطابقة</td></tr>`;
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
      </tr>`;
  }).join('');
}

document.addEventListener('DOMContentLoaded', loadOrders);
