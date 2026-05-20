document.addEventListener('DOMContentLoaded', () => {
  loadSettings();
  
  const form = document.getElementById('smartReviewerSettingsForm');
  if (form) {
    form.addEventListener('submit', async (e) => {
      e.preventDefault();
      const btn = form.querySelector('button[type="submit"]');
      const originalText = btn.innerHTML;
      btn.disabled = true;
      btn.innerHTML = '<i class="bi bi-hourglass-split"></i> جاري الحفظ...';
      
      try {
        const payload = {
          enabled: document.getElementById('smartReviewerEnabled').checked,
          interval_minutes: parseInt(document.getElementById('smartReviewerInterval').value, 10) || 60
        };
        
        const res = await fetch(adminApi('/api/settings/smart_reviewer'), {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(payload)
        });
        const data = await res.json();
        
        if (!res.ok || !data.ok) throw new Error(data.error || 'فشل الحفظ');
        
        setAdminStatus('smartReviewerSettingsStatus', 'تم حفظ الإعدادات بنجاح.', true);
        applySettings(data.settings);
      } catch (err) {
        setAdminStatus('smartReviewerSettingsStatus', err.message, false);
      } finally {
        btn.disabled = false;
        btn.innerHTML = originalText;
      }
    });
  }
});

async function loadSettings() {
  try {
    const res = await fetch(adminApi('/api/settings/smart_reviewer'));
    const data = await res.json();
    if (!res.ok || !data.ok) throw new Error(data.error || 'فشل جلب الإعدادات');
    applySettings(data.settings);
  } catch (err) {
    setAdminStatus('smartReviewerSettingsStatus', err.message, false);
  }
}

function applySettings(settings) {
  if (!settings) return;
  const enabledEl = document.getElementById('smartReviewerEnabled');
  const intervalEl = document.getElementById('smartReviewerInterval');
  
  if (enabledEl) enabledEl.checked = settings.enabled;
  if (intervalEl) intervalEl.value = settings.interval_minutes || 60;
}
