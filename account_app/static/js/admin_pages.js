const adminKey = new URLSearchParams(location.search).get('key') || '';

function adminApi(path) {
  const url = new URL(path, location.origin);
  url.searchParams.set('key', adminKey);
  const store = new URLSearchParams(location.search).get('store_id');
  if (store && !url.searchParams.has('store_id')) url.searchParams.set('store_id', store);
  return url.pathname + url.search;
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

// Keep the selected store while navigating between administration pages.
document.addEventListener('DOMContentLoaded', async () => {
  const picker = document.getElementById('globalStoreSelect');
  if (!picker) return;
  const selected = new URLSearchParams(location.search).get('store_id') || 'default';
  document.querySelectorAll('a[href^="/"]').forEach(link => {
    const url = new URL(link.href);
    if (url.pathname === '/logout') return;
    url.searchParams.set('store_id', selected);
    if (adminKey) url.searchParams.set('key', adminKey);
    link.href = url.pathname + url.search;
  });
  try {
    const response = await fetch(adminApi('/api/stores'));
    if (!response.ok) throw new Error('stores');
    const data = await response.json();
    picker.replaceChildren(...data.stores.map(s => new Option(s.name,s.store_id)));
    picker.value = selected;
    picker.disabled = false;
    picker.addEventListener('change', () => {
      const url = new URL(location.href);
      url.searchParams.set('store_id', picker.value);
      location.href = url.toString();
    });
  } catch { picker.replaceChildren(new Option('تعذر تحميل المتاجر', '')); }
});
