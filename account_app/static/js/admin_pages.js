const adminKey = new URLSearchParams(location.search).get('key') || '';

function adminApi(path) {
  const sep = path.includes('?') ? '&' : '?';
  return `${path}${sep}key=${encodeURIComponent(adminKey)}`;
}

function toggleAdminNav() {
  document.getElementById('adminSidebar')?.classList.toggle('open');
  document.getElementById('adminOverlay')?.classList.toggle('show');
}

function closeAdminNav() {
  document.getElementById('adminSidebar')?.classList.remove('open');
  document.getElementById('adminOverlay')?.classList.remove('show');
}

function adminEsc(value) {
  return String(value ?? '').replace(/[&<>"']/g, ch => ({
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#39;'
  }[ch]));
}

function setAdminStatus(id, text, ok = true) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.className = `admin-status ${ok ? 'ok' : 'error'}`;
}
