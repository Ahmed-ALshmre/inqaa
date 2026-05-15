let evaluationState = null;

function evaluationQuery() {
  const period = document.getElementById('evaluationPeriod')?.value || 'today';
  const params = new URLSearchParams({period});
  if (period === 'custom') {
    const from = document.getElementById('evaluationFrom')?.value;
    const to = document.getElementById('evaluationTo')?.value;
    if (from) params.set('from', from);
    if (to) params.set('to', to);
  }
  return params.toString();
}

async function loadEvaluation() {
  const res = await fetch(adminApi(`/api/evaluation?${evaluationQuery()}`));
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل تحميل التقييم');
  evaluationState = data;
  renderEvaluationCards(data.metrics || {});
  renderStageBreakdown(data.stage_breakdown || []);
  renderFollowupPerformance(data.followup_performance || []);
  renderSuggestions(data.suggestions || []);
  renderActiveRules(data.active_rules || []);
}

function renderEvaluationCards(metrics) {
  const items = [
    ['الرسائل', metrics.total_messages || 0, 'bi-chat-dots'],
    ['المحادثات', metrics.total_conversations || 0, 'bi-people'],
    ['الطلبات', metrics.total_orders || 0, 'bi-receipt'],
    ['تحويل المحادثات', `${metrics.conversation_conversion_rate || 0}%`, 'bi-percent'],
    ['تحويل الرسائل', `${metrics.message_conversion_rate || 0}%`, 'bi-activity'],
    ['المتابعات المرسلة', metrics.followups_sent || 0, 'bi-send'],
    ['طلبات بعد المتابعة', metrics.orders_after_followup || 0, 'bi-bag-check'],
    ['تدخل بشري', metrics.human_review_count || 0, 'bi-person-exclamation'],
    ['أعلى توقف', metrics.top_dropoff_stage || '-', 'bi-sign-stop'],
    ['أكثر اعتراض', metrics.top_objection || '-', 'bi-exclamation-diamond']
  ];
  document.getElementById('evaluationCards').innerHTML = items.map(([label, value, icon]) => `
    <article class="metric-card">
      <div class="metric-icon"><i class="bi ${icon}"></i></div>
      <div>
        <strong>${adminEsc(value)}</strong>
        <span>${adminEsc(label)}</span>
      </div>
    </article>
  `).join('');
}

function renderStageBreakdown(rows) {
  const el = document.getElementById('stageBreakdown');
  if (!rows.length) {
    el.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-4">لا توجد بيانات</td></tr>';
    return;
  }
  el.innerHTML = rows.map((row) => `
    <tr>
      <td>${adminEsc(row.stage)}</td>
      <td>${adminEsc(row.count)}</td>
      <td>${adminEsc(row.percentage)}%</td>
      <td>${adminEsc(row.evaluation)}</td>
      <td>${adminEsc(row.suggestion)}</td>
    </tr>
  `).join('');
}

function renderFollowupPerformance(rows) {
  const el = document.getElementById('followupPerformance');
  if (!rows.length) {
    el.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-4">لا توجد متابعات مرسلة في هذه الفترة</td></tr>';
    return;
  }
  el.innerHTML = rows.map((row) => `
    <tr>
      <td>${adminEsc(row.stage)}</td>
      <td>${adminEsc(row.sent)}</td>
      <td>${adminEsc(row.replies)}</td>
      <td>${adminEsc(row.orders_after)}</td>
      <td>${adminEsc(row.success_rate)}%</td>
    </tr>
  `).join('');
}

function renderSuggestions(rows) {
  const el = document.getElementById('evaluationSuggestions');
  if (!rows.length) {
    el.innerHTML = '<div class="admin-empty">لا توجد اقتراحات بعد.</div>';
    return;
  }
  el.innerHTML = rows.map((row) => `
    <article class="suggestion-card" data-id="${adminEsc(row.id)}">
      <div class="suggestion-card-head">
        <input class="form-control suggestion-title" value="${adminEsc(row.title)}">
        <span class="badge text-bg-secondary">${adminEsc(row.status)}</span>
      </div>
      <div class="suggestion-meta">
        <span>${adminEsc(row.suggestion_type || '-')}</span>
        <span>${adminEsc(row.metric_name || '-')} = ${adminEsc(row.metric_value || '-')}</span>
        <span>${adminEsc(row.conversion_rate || 0)}%</span>
      </div>
      <textarea class="form-control suggestion-content" rows="3">${adminEsc(row.content)}</textarea>
      <textarea class="form-control suggestion-notes" rows="2" placeholder="ملاحظات الأدمن">${adminEsc(row.admin_notes || '')}</textarea>
      <p class="admin-muted">${adminEsc(row.reason || '')}</p>
      <div class="admin-actions-row">
        <button class="btn btn-sm btn-outline-primary" type="button" onclick="saveSuggestion(${row.id})"><i class="bi bi-save"></i> حفظ التعديل</button>
        <button class="btn btn-sm btn-success" type="button" onclick="approveSuggestion(${row.id})"><i class="bi bi-check2-circle"></i> تفعيل كقاعدة</button>
        <button class="btn btn-sm btn-outline-secondary" type="button" onclick="setSuggestionStatus(${row.id}, 'disabled')"><i class="bi bi-pause-circle"></i> تعطيل</button>
      </div>
    </article>
  `).join('');
}

function suggestionPayload(id, status = null) {
  const card = document.querySelector(`.suggestion-card[data-id="${CSS.escape(String(id))}"]`);
  return {
    title: card?.querySelector('.suggestion-title')?.value || '',
    content: card?.querySelector('.suggestion-content')?.value || '',
    admin_notes: card?.querySelector('.suggestion-notes')?.value || '',
    ...(status ? {status} : {})
  };
}

async function saveSuggestion(id) {
  const res = await fetch(adminApi(`/api/evaluation/suggestions/${id}`), {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(suggestionPayload(id))
  });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل حفظ الاقتراح');
  await loadEvaluation();
}

async function setSuggestionStatus(id, status) {
  const res = await fetch(adminApi(`/api/evaluation/suggestions/${id}`), {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(suggestionPayload(id, status))
  });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل تحديث الاقتراح');
  await loadEvaluation();
}

async function approveSuggestion(id) {
  await saveSuggestion(id);
  const res = await fetch(adminApi(`/api/evaluation/suggestions/${id}/approve`), {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({priority: 5})
  });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل تفعيل القاعدة');
  await loadEvaluation();
}

function renderActiveRules(rows) {
  const el = document.getElementById('activeRules');
  if (!rows.length) {
    el.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-4">لا توجد قواعد مفعلة</td></tr>';
    return;
  }
  el.innerHTML = rows.map((row) => `
    <tr>
      <td>${adminEsc(row.rule_text)}</td>
      <td>${adminEsc(row.rule_type || '-')}</td>
      <td>${adminEsc(row.priority || 5)}</td>
      <td>${row.active ? 'مفعلة' : 'متوقفة'}</td>
      <td><button class="btn btn-sm btn-outline-danger" type="button" onclick="toggleRule(${row.id}, false)">تعطيل</button></td>
    </tr>
  `).join('');
}

async function toggleRule(id, active) {
  const res = await fetch(adminApi(`/api/evaluation/rules/${id}`), {
    method: 'PUT',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({active})
  });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل تحديث القاعدة');
  await loadEvaluation();
}

async function generateSuggestions() {
  const body = {
    date_from: evaluationState?.date_from,
    date_to: evaluationState?.date_to
  };
  const res = await fetch(adminApi('/api/evaluation/generate_suggestions'), {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل توليد الاقتراحات');
  await loadEvaluation();
}

async function runDailyLearning() {
  const body = {
    date_from: evaluationState?.date_from,
    date_to: evaluationState?.date_to
  };
  const res = await fetch(adminApi('/api/evaluation/run_daily_learning'), {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  });
  const data = await res.json();
  if (!res.ok || !data.ok) throw new Error(data.error || 'فشل تشغيل التحليل اليومي');
  await loadEvaluation();
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('generateSuggestions')?.addEventListener('click', () => {
    generateSuggestions().catch((err) => alert(err.message));
  });
  document.getElementById('runDailyLearning')?.addEventListener('click', () => {
    runDailyLearning().catch((err) => alert(err.message));
  });
  loadEvaluation().catch((err) => {
    document.getElementById('evaluationCards').innerHTML = `<div class="admin-empty">${adminEsc(err.message)}</div>`;
  });
});
