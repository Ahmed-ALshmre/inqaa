const DASH_KEY = new URLSearchParams(window.location.search).get('key') || '';
let allOrders = [];

const STATUS_LABEL = {
  new: 'جديد',
};

document.addEventListener('DOMContentLoaded', () => {
  loadOrders();
});

function apiFetch(url, opts = {}) {
  opts.headers = { ...(opts.headers || {}), 'X-Dashboard-Key': DASH_KEY };
  return fetch(url, opts);
}

function statusLabel(status) {
  const s = (status || '').trim().toLowerCase();
  return STATUS_LABEL[s] || esc(status || '—');
}

function fillStatusFilter() {
  const sel = document.getElementById('orderStatusFilter');
  const prev = sel.value;
  const seen = new Set();
  for (const o of allOrders) {
    const raw = String(o.status ?? '').trim();
    if (raw) seen.add(raw);
  }
  const sorted = [...seen].sort((a, b) => a.localeCompare(b, 'ar'));
  sel.innerHTML =
    '<option value="">كل الحالات</option>' +
    sorted
      .map((raw) => {
        const v = escAttr(raw);
        const lab = statusLabel(raw);
        return `<option value="${v}">${lab}</option>`;
      })
      .join('');
  if ([...sel.options].some((o) => o.value === prev)) sel.value = prev;
}

function statusBadgeClass(status) {
  const s = (status || '').trim().toLowerCase();
  if (s === 'new') return 'bg-primary';
  return 'bg-secondary';
}

async function loadOrders() {
  const el = document.getElementById('ordersList');
  el.innerHTML =
    '<div class="text-center py-5" style="color:var(--text-muted)">' +
    '<div class="spinner-border spinner-border-sm mb-2"></div>' +
    '<div class="small">جاري التحميل...</div></div>';
  try {
    const res = await apiFetch('/api/orders');
    const data = await res.json();
    if (!res.ok) throw new Error(data.error || 'فشل تحميل الطلبات');
    allOrders = data.orders || [];
    fillStatusFilter();
    renderOrders();
  } catch (err) {
    showToast(err.message || 'فشل تحميل الطلبات', 'danger');
    el.innerHTML =
      '<div class="text-center py-5 small" style="color:var(--text-muted)">تعذر تحميل الطلبات</div>';
  }
}

function renderOrders() {
  const q = (document.getElementById('orderSearch').value || '').trim().toLowerCase();
  const st = (document.getElementById('orderStatusFilter').value || '').trim().toLowerCase();

  const list = allOrders.filter((o) => {
    if (st && String(o.status || '').trim().toLowerCase() !== st) return false;
    if (!q) return true;
    const hay = [
      o.customer_name,
      o.phone,
      o.province,
      o.address,
      o.product_name,
      o.product_id,
      o.color,
      o.size,
      o.notes,
      o.sender_id,
      o.status,
      o.created_at,
    ]
      .map((x) => String(x ?? '').toLowerCase())
      .join(' ');
    return hay.includes(q);
  });

  document.getElementById('ordersCount').textContent = `${list.length} من ${allOrders.length} طلب`;

  const el = document.getElementById('ordersList');
  if (!list.length) {
    el.innerHTML =
      '<div class="text-center py-5 small" style="color:var(--text-muted)">لا توجد طلبات مطابقة</div>';
    return;
  }

  el.innerHTML = list
    .map((o) => {
      const id = o.id != null ? esc(String(o.id)) : '—';
      const created = esc(o.created_at || '—');
      const badgeClass = statusBadgeClass(o.status);
      const stHtml = statusLabel(o.status);
      return `
        <article class="order-card">
          <div class="order-card-top">
            <div>
              <div class="order-card-title">طلب #${id} — ${esc(o.customer_name || 'بدون اسم')}</div>
              <div class="order-card-meta">${created}</div>
            </div>
            <div class="order-card-actions">
              <span class="badge ${badgeClass} flex-shrink-0">${stHtml}</span>
              <button class="btn btn-sm btn-telegram" type="button" onclick="sendOrderTelegram(${Number(o.id) || 0}, this)" title="إرسال الطلب إلى التليكرام">
                <i class="bi bi-telegram"></i>
                <span>إرسال</span>
              </button>
            </div>
          </div>
          <dl class="order-card-body mb-0">
            <div><dt>الهاتف</dt><dd>${esc(o.phone || '—')}</dd></div>
            <div><dt>المحافظة</dt><dd>${esc(o.province || '—')}</dd></div>
            <div style="grid-column:1/-1"><dt>العنوان</dt><dd>${esc(o.address || '—')}</dd></div>
            <div><dt>المنتج</dt><dd>${esc(o.product_name || '—')}</dd></div>
            <div><dt>كود المنتج</dt><dd>${esc(o.product_id || '—')}</dd></div>
            <div><dt>اللون</dt><dd>${esc(o.color || '—')}</dd></div>
            <div><dt>المقاس</dt><dd>${esc(o.size || '—')}</dd></div>
            <div style="grid-column:1/-1"><dt>ملاحظات</dt><dd>${esc(o.notes || '—')}</dd></div>
            <div style="grid-column:1/-1"><dt>معرّف المحادثة</dt><dd><code class="small user-select-all">${esc(
              o.sender_id || '—',
            )}</code></dd></div>
          </dl>
        </article>`;
    })
    .join('');
}

async function sendOrderTelegram(orderId, btn) {
  if (!orderId) return;
  const oldHtml = btn ? btn.innerHTML : '';
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm" style="width:12px;height:12px;"></span><span>جاري</span>';
  }
  try {
    const res = await apiFetch(`/api/orders/${orderId}/send_telegram`, { method: 'POST' });
    const data = await res.json();
    if (!res.ok || !data.ok) throw new Error(data.error || 'فشل إرسال الطلب إلى التليكرام');
    showToast('تم إرسال الطلب إلى التليكرام', 'success');
  } catch (err) {
    showToast(err.message || 'فشل إرسال الطلب إلى التليكرام', 'danger');
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = oldHtml;
    }
  }
}

function showToast(msg, type = 'success') {
  const container = document.getElementById('toastContainer');
  const id = 't' + Date.now();
  container.insertAdjacentHTML(
    'beforeend',
    `
    <div id="${id}" class="toast align-items-center text-bg-${type} border-0 mb-2" role="alert">
      <div class="d-flex">
        <div class="toast-body">${esc(msg)}</div>
        <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
      </div>
    </div>`,
  );
  const toastEl = document.getElementById(id);
  const toast = new bootstrap.Toast(toastEl, { delay: 2600 });
  toast.show();
  toastEl.addEventListener('hidden.bs.toast', () => toastEl.remove());
}

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, (c) =>
    ({
      '&': '&amp;',
      '<': '&lt;',
      '>': '&gt;',
      '"': '&quot;',
      "'": '&#039;',
    })[c],
  );
}

function escAttr(s) {
  return esc(s).replace(/`/g, '&#096;');
}
