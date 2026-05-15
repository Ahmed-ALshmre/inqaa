function getDashboardKey() {
  const fromUrl = new URLSearchParams(window.location.search).get('key') || '';
  const fromCookie = document.cookie.split('; ').find(row => row.startsWith('dashboard_key='))?.split('=')[1] || '';
  return fromUrl || decodeURIComponent(fromCookie);
}

const DASH_KEY = getDashboardKey();

let aiCommandState = { files: {} };
let activeFileKey = 'instructions';

function apiFetch(url, opts = {}) {
  opts.headers = {
    ...(opts.headers || {}),
    'X-Dashboard-Key': DASH_KEY,
  };
  return fetch(url, opts);
}

function esc(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;',
  }[ch]));
}

function showToast(message, type = 'success') {
  const el = document.getElementById('instructionsToast');
  if (!el) return;
  el.className = `alert alert-${type} instructions-toast`;
  el.textContent = message;
  el.style.display = 'block';
  clearTimeout(showToast._timer);
  showToast._timer = setTimeout(() => { el.style.display = 'none'; }, 2600);
}

function fileOrder() {
  return ['instructions', 'forbidden_rules', 'playbook', 'product_summary'];
}

function syncActiveEditorToState() {
  const editor = document.getElementById('activeFileContent');
  if (editor && aiCommandState.files && aiCommandState.files[activeFileKey]) {
    aiCommandState.files[activeFileKey].content = editor.value;
  }
}

function renderTabs() {
  const tabs = document.getElementById('fileTabs');
  if (!tabs) return;
  tabs.innerHTML = fileOrder().filter(key => aiCommandState.files[key]).map(key => {
    const file = aiCommandState.files[key];
    const active = key === activeFileKey ? 'active' : '';
    return `
      <button class="instruction-tab ${active}" type="button" onclick="selectFile('${key}')">
        <div class="fw-semibold">${esc(file.title || file.path || key)}</div>
        <div class="small" style="color:var(--text-muted)">${Number(file.size || 0).toLocaleString('ar')} بايت</div>
      </button>
    `;
  }).join('');
}

function renderActiveFile() {
  const file = aiCommandState.files?.[activeFileKey];
  if (!file) return;
  document.getElementById('activeFileName').textContent = file.title || file.path || activeFileKey;
  document.getElementById('activeFileDescription').textContent = file.description || '';
  document.getElementById('activeFileContent').value = file.content || '';
  document.getElementById('activeFileMeta').textContent = `${file.path || ''} - ${Number(file.size || 0).toLocaleString('ar')} بايت`;
  renderTabs();
}

function selectFile(key) {
  syncActiveEditorToState();
  activeFileKey = key;
  renderActiveFile();
}

function fillSideEditors(data) {
  document.getElementById('dbAiInstructions').value = data.db_ai_instructions || '';
  document.getElementById('dbForbiddenRules').value = data.db_forbidden_rules || '';
  document.getElementById('globalSupervisorInstructions').value = data.global_supervisor_instructions || '';
}

async function loadAICommands(toast = false) {
  try {
    const res = await apiFetch('/api/settings/ai_commands');
    const data = await res.json();
    if (!res.ok || !data.ok) throw new Error(data.error || 'فشل تحميل التعليمات');
    aiCommandState = data;
    if (!aiCommandState.files?.[activeFileKey]) activeFileKey = 'instructions';
    renderActiveFile();
    fillSideEditors(data);
    document.getElementById('lastSavedMeta').textContent = 'تم تحميل التعليمات من الخادم';
    if (toast) showToast('تم تحديث التعليمات من الخادم', 'success');
  } catch (err) {
    showToast(err.message || 'فشل تحميل التعليمات', 'danger');
    document.getElementById('lastSavedMeta').textContent = 'تعذر تحميل التعليمات';
  }
}

async function saveAICommands() {
  syncActiveEditorToState();
  const btn = document.getElementById('saveAllBtn');
  const oldHtml = btn ? btn.innerHTML : '';
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm"></span><span class="ms-1">حفظ</span>';
  }

  try {
    const files = {};
    for (const [key, file] of Object.entries(aiCommandState.files || {})) {
      files[key] = file.content || '';
    }
    const res = await apiFetch('/api/settings/ai_commands', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        files,
        db_ai_instructions: document.getElementById('dbAiInstructions').value,
        db_forbidden_rules: document.getElementById('dbForbiddenRules').value,
        global_supervisor_instructions: document.getElementById('globalSupervisorInstructions').value,
      }),
    });
    const data = await res.json();
    if (!res.ok || !data.ok) throw new Error(data.error || 'فشل حفظ التعليمات');
    showToast(data.message || 'تم حفظ جميع التعليمات', 'success');
    await loadAICommands(false);
    document.getElementById('lastSavedMeta').textContent = 'آخر حفظ: الآن';
  } catch (err) {
    showToast(err.message || 'فشل حفظ التعليمات', 'danger');
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = oldHtml;
    }
  }
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('activeFileContent')?.addEventListener('input', syncActiveEditorToState);
  loadAICommands(false);
});
