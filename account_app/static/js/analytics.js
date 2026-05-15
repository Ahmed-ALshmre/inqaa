async function loadAnalytics() {
  const period = document.getElementById('analyticsPeriod')?.value || 'today';
  const res = await fetch(adminApi(`/api/analytics?period=${encodeURIComponent(period)}`));
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || 'فشل تحميل التحليلات');
  renderAnalyticsCards(data.cards || {});
  renderProductRows('topOrderedProducts', data.top_ordered_products || [], 'orders');
  renderProductRows('topInterestedProducts', data.top_interested_products || [], 'count');
  renderObjections(data.top_objections || []);
}

function renderAnalyticsCards(cards) {
  const items = [
    ['الرسائل', cards.messages || 0, 'bi-chat-dots'],
    ['رسائل واردة', cards.incoming_messages || 0, 'bi-inbox'],
    ['الطلبات', cards.orders || 0, 'bi-receipt'],
    ['نسبة التحويل', `${cards.message_to_order_conversion || 0}%`, 'bi-percent'],
    ['عملاء جدد', cards.new_customers || 0, 'bi-person-plus'],
    ['تدخل بشري', cards.human_reviews || 0, 'bi-person-exclamation'],
    ['مراجعات معلقة', cards.pending_reviews || 0, 'bi-hourglass-split'],
    ['محادثات بلا رد', cards.unanswered_conversations || 0, 'bi-chat-left-text']
  ];
  document.getElementById('analyticsCards').innerHTML = items.map(([label, value, icon]) => `
    <article class="metric-card">
      <div class="metric-icon"><i class="bi ${icon}"></i></div>
      <div>
        <strong>${adminEsc(value)}</strong>
        <span>${adminEsc(label)}</span>
      </div>
    </article>
  `).join('');
}

function renderProductRows(id, rows, countKey) {
  const el = document.getElementById(id);
  if (!rows.length) {
    el.innerHTML = '<tr><td colspan="2" class="text-center text-muted py-4">لا توجد بيانات</td></tr>';
    return;
  }
  el.innerHTML = rows.map((row) => `
    <tr>
      <td>${adminEsc(row.product_name || row.product_id || '-')}</td>
      <td>${adminEsc(row[countKey] || 0)}</td>
    </tr>
  `).join('');
}

function renderObjections(rows) {
  const el = document.getElementById('topObjections');
  if (!rows.length) {
    el.innerHTML = '<div class="admin-empty">لا توجد اعتراضات مسجلة.</div>';
    return;
  }
  el.innerHTML = rows.map((row) => `
    <div class="admin-list-row">
      <span>${adminEsc(row.text)}</span>
      <strong>${adminEsc(row.count)}</strong>
    </div>
  `).join('');
}

document.addEventListener('DOMContentLoaded', () => {
  loadAnalytics().catch((err) => {
    document.getElementById('analyticsCards').innerHTML = `<div class="admin-empty">${adminEsc(err.message)}</div>`;
  });
});
