from pathlib import Path
p=Path('account_app/app.py');s=p.read_text(encoding='utf-8');needle='    sent = send_order_to_telegram(order)';pos=s.index(needle,s.index('def api_resend_order_telegram('));s=s[:pos]+'''    if order.get("total_amount") is None:
        return jsonify({"ok": False, "error": "الطلب قديم بلا سعر محفوظ؛ أدخل سعر القطع والتوصيل من تعديل الطلب أولاً"}), 409
'''+s[pos:]
pos=s.index('    if not updates:',s.index('def api_update_order('));s=s[:pos]+'''    if "product_total" in data or "delivery_fee" in data:
        try:
            amounts = [int(str(data[k])) for k in ("product_total", "delivery_fee")]
            if amounts[0] <= 0 or amounts[1] < 0:
                raise ValueError()
        except (ValueError, TypeError, KeyError):
            return jsonify({"ok": False, "error": "أدخل سعر القطع الصحيح وأجرة التوصيل (صفر للتوصيل المجاني)"}), 400
        updates.update(product_total=amounts[0], delivery_fee=amounts[1], total_amount=sum(amounts))
'''+s[pos:];p.write_text(s,encoding='utf-8')
p=Path('account_app/static/js/orders.js');s=p.read_text(encoding='utf-8');s=s.replace('let editModal = null;','let editModal = null;\nlet orderRequest = 0;\nlet orderOffset = 0;\nlet orderSearchTimer;\nfunction scheduleOrderSearch() { clearTimeout(orderSearchTimer); orderSearchTimer = setTimeout(() => loadOrders(), 250); }')
s=s.replace("  if (senderId) params.set('sender_id', senderId);", "  if (senderId) params.set('sender_id', senderId);\n  const store = document.getElementById('orderStore')?.value;\n  if (store && store !== 'all') params.set('store_id', store);")
start=s.index('async function loadOrders(');end=s.index('\nfunction renderOrders()',start)
s=s[:start]+'''async function loadOrders(append = false) {
  const request = ++orderRequest;
  const body = document.getElementById('ordersBody');
  const more = document.getElementById('ordersMore');
  more.disabled = true;
  if (!append) {
    orderOffset = 0;
    allOrders = [];
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
'''+s[end:]
s=s.replace("  const query = (document.getElementById('orderSearch').value || '').trim().toLowerCase();\n  const orders = query ? allOrders.filter(order => orderSearchText(order).includes(query)) : allOrders;", "  const orders = allOrders;")
s=s.replace('colspan="10"','colspan="11"')
s=s.replace('        <td data-label="الحالة">','        <td data-label="مع التوصيل">${order.total_amount == null ? \'غير محفوظ\' : Number(order.total_amount).toLocaleString(\'en-US\') + \' د.ع\'}</td>\n        <td data-label="الحالة">')
s=s.replace("if (el) el.value = value || '';", "if (el) el.value = value ?? '';")
s=s.replace("  setEditValue('editNotes', order.notes);", "  setEditValue('editNotes', order.notes);\n  setEditValue('editProductTotal', order.product_total);\n  setEditValue('editDeliveryFee', order.delivery_fee);")
s=s.replace("  try {\n    const res = await fetch(orderApi(`/api/orders/${encodeURIComponent(orderId)}`)", "  const productTotal = document.getElementById('editProductTotal').value;\n  const deliveryFee = document.getElementById('editDeliveryFee').value;\n  if (productTotal !== '' || deliveryFee !== '') { payload.product_total = productTotal; payload.delivery_fee = deliveryFee; }\n  try {\n    const res = await fetch(orderApi(`/api/orders/${encodeURIComponent(orderId)}`)",1)
s=s.replace('    renderOrders();\n    editModal?.hide();','    await loadOrders();\n    editModal?.hide();')
s=s.replace("  loadOrders();\n});", "  const selectedStore = new URLSearchParams(location.search).get('store_id') || 'all';\n  const select = document.getElementById('orderStore');\n  if (selectedStore !== 'all') select.add(new Option(selectedStore, selectedStore, true, true));\n  fetch(orderApi('/api/stores')).then(res => res.json()).then(data => {\n    for (const store of data.stores || []) {\n      const existing = [...select.options].find(o => o.value === store.store_id);\n      if (existing) existing.textContent = store.name || store.store_id;\n      else select.add(new Option(store.name || store.store_id, store.store_id));\n    }\n  }).catch(() => {});\n  loadOrders();\n});")
p.write_text(s,encoding='utf-8')
p=Path('account_app/templates/orders.html');s=p.read_text(encoding='utf-8').replace('<html lang="ar" dir="rtl">','<html lang="ar" dir="rtl" class="orders-root">').replace('width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no','width=device-width, initial-scale=1.0')
s=s.replace('</head>','  <link rel="stylesheet" href="/static/css/orders.css?v=1">\n</head>')
s=s.replace('oninput="renderOrders()"','oninput="scheduleOrderSearch()"').replace('<small>أشخاص</small>','<small>مراسلون في الفترة</small>').replace('<small>تحويل</small>','<small>مراسلون لديهم طلب غير ملغى</small>')
s=s.replace('<section class="orders-toolbar">','<section class="orders-toolbar">\n      <label for="orderStore">المتجر</label><select id="orderStore" class="form-select mb-2" onchange="loadOrders()"><option value="all">كل المتاجر</option></select>\n      <p class="small">الإحصائيات للفترة والمتجر المحددين؛ البحث يصفّي قائمة الطلبات فقط.</p>')
s=s.replace('<th>الحالة</th>','<th>المجموع مع التوصيل</th>\n              <th>الحالة</th>').replace('colspan="10"','colspan="11"')
s=s.replace('  </main>','    <div class="orders-pagination"><span id="ordersLoaded"></span><button id="ordersMore" class="btn btn-primary" hidden onclick="loadOrders(true)">تحميل المزيد</button></div>\n  </main>')
s=s.replace('            <input type="hidden" id="editOrderId">','''            <input type="hidden" id="editOrderId">
            <div class="row g-2 mb-3">
              <label class="col-sm-6">سعر القطع مجتمعة (د.ع)<input class="form-control" id="editProductTotal" type="number" min="1" step="1"></label>
              <label class="col-sm-6">التوصيل (د.ع)<input class="form-control" id="editDeliveryFee" type="number" min="0" step="1"></label>
              <small>الطلبات القديمة قد لا تحتوي سعرًا محفوظًا. أدخل المبلغ المتفق عليه قبل إعادة إرسالها.</small>
            </div>''').replace('orders.js?v=9','orders.js?v=10')
p.write_text(s,encoding='utf-8')
