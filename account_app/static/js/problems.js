let allProblems = [];
let currentProblemFilter = 'all';
const problemsKey = new URLSearchParams(location.search).get('key') || '';

function adminProblemApi(path) {
  const sep = path.includes('?') ? '&' : '?';
  return `${path}${sep}key=${encodeURIComponent(problemsKey)}`;
}

function adminEsc(value) {
  return String(value ?? '').replace(/[&<>"]+/g, ch => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;'
  }[ch]));
}

function formatTime(value) {
  if (!value) return '-';
  const text = String(value).replace('T', ' ');
  return text.slice(0, 16);
}

function problemSearchText(problem) {
  return [
    problem.id,
    problem.sender_id,
    problem.customer_name,
    problem.reason,
    problem.message_text,
    problem.product_name,
    problem.product_id,
    problem.status,
  ].join(' ').toLowerCase();
}

function setProblemFilter(filter, btn) {
  currentProblemFilter = filter;
  document.getElementById('filterAllBtn').classList.toggle('active', filter === 'all');
  document.getElementById('filterOpenBtn').classList.toggle('active', filter === 'open');
  document.getElementById('filterClosedBtn').classList.toggle('active', filter === 'closed');
  renderProblems();
}

async function loadProblems() {
  const body = document.getElementById('problemsBody');
  body.innerHTML = `
    <tr>
      <td colspan="8" class="text-center py-5" style="color:var(--text-muted)">
        <div class="spinner-border spinner-border-sm mb-2"></div>
        <div class="small">جاري تحميل مشاكل الزبائن...</div>
      </td>
    </tr>`;
  const res = await fetch(adminProblemApi('/api/problems'));
  if (!res.ok) {
    body.innerHTML = `<tr><td colspan="8" class="text-center py-5 text-danger">فشل تحميل المشاكل</td></tr>`;
    return;
  }
  const data = await res.json();
  allProblems = data.problems || [];
  document.getElementById('problemsTotal').textContent = data.total ?? allProblems.length;
  document.getElementById('problemsOpen').textContent = allProblems.filter(p => (p.status || 'open') === 'open').length;
  renderProblems();
}

function renderProblems() {
  const body = document.getElementById('problemsBody');
  const query = (document.getElementById('problemSearch').value || '').trim().toLowerCase();
  const filtered = allProblems.filter(problem => {
    if (currentProblemFilter === 'open' && (problem.status || 'open') !== 'open') return false;
    if (currentProblemFilter === 'closed' && (problem.status || 'open') === 'open') return false;
    return !query || problemSearchText(problem).includes(query);
  });

  if (!filtered.length) {
    body.innerHTML = `<tr><td colspan="8" class="text-center py-5" style="color:var(--text-muted)">لا توجد مشاكل مطابقة</td></tr>`;
    return;
  }

  body.innerHTML = filtered.map(problem => {
    const customer = problem.customer_name || problem.sender_id || '-';
    const product = [problem.product_name, problem.product_id ? `(${adminEsc(problem.product_id)})` : ''].filter(Boolean).join(' ');
    const isOpen = (problem.status || 'open') === 'open';
    const actionButton = isOpen ?
      `<button class="btn btn-sm btn-success" onclick="updateProblemStatus(${problem.id}, 'closed')">إغلاق</button>` :
      `<button class="btn btn-sm btn-outline-secondary" onclick="updateProblemStatus(${problem.id}, 'open')">إعادة فتح</button>`;

    return `
      <tr>
        <td class="text-muted">#${adminEsc(problem.id)}</td>
        <td>${adminEsc(formatTime(problem.created_at))}</td>
        <td>${adminEsc(customer)}</td>
        <td>${adminEsc(problem.reason || '-')}</td>
        <td>${adminEsc(problem.message_text || '-')}</td>
        <td>${adminEsc(product || '-')}</td>
        <td><span class="badge ${isOpen ? 'bg-warning text-dark' : 'bg-success'}">${adminEsc(problem.status || 'open')}</span></td>
        <td>${actionButton}</td>
      </tr>`;
  }).join('');
}

async function updateProblemStatus(problemId, status) {
  try {
    const res = await fetch(adminProblemApi(`/api/problems/${problemId}/status`), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status }),
    });
    if (!res.ok) throw new Error('فشل تحديث الحالة');
    await loadProblems();
  } catch (err) {
    console.error(err);
    alert('تعذر تحديث حالة المشكلة.');
  }
}

document.addEventListener('DOMContentLoaded', loadProblems);
