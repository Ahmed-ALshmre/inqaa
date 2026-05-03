// â”€â”€ State â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
const DASH_KEY        = new URLSearchParams(window.location.search).get('key') || '';
let currentSenderId   = null;
let currentCustomer   = null;
let allConversations  = [];
let currentFilter     = 'all';
let searchQuery       = '';
let pollConvTimer     = null;
let pollMsgTimer      = null;
let uploadedImageUrl  = null;
let aiPendingReply    = null;
let allProducts       = [];
let globalAIEnabled   = true;
let currentConversationAIEnabled = true;
let audioUnlocked     = false;
const messageSound    = new Audio('/aud/cheerful-527.mp3');
const reviewSound     = new Audio('/aud/young-rooster-cock-a-doodle-doo.mp3');
const imageSound      = new Audio('/aud/young-rooster-cock-a-doodle-doo.mp3');
const latestIncomingBySender = {};
const pendingReviewsBySender = {};
const latestConversationTimeBySender = {};
const latestImageIdBySender = {};

messageSound.preload = 'auto';
reviewSound.preload  = 'auto';
imageSound.preload   = 'auto';
imageSound.loop      = false;

// â”€â”€ Auth Fetch â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
function apiFetch(url, opts = {}) {
  opts.headers = { ...(opts.headers || {}), 'X-Dashboard-Key': DASH_KEY };
  return fetch(url, opts);
}

// â”€â”€ Boot â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
document.addEventListener('DOMContentLoaded', () => {
  document.addEventListener('click', unlockAudioOnce, { once: true });
  loadConversations();
  loadStats();
  loadProducts();
  pollConvTimer = setInterval(() => { loadConversations(false); loadStats(); }, 8000);
});

// â•â• PWA Install â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
let _pwaDeferredPrompt = null;
window.addEventListener('beforeinstallprompt', (e) => {
  e.preventDefault();
  _pwaDeferredPrompt = e;
  const btn = document.getElementById('pwaInstallBtn');
  if (btn) btn.style.display = '';
});
window.addEventListener('appinstalled', () => {
  _pwaDeferredPrompt = null;
  const btn = document.getElementById('pwaInstallBtn');
  if (btn) btn.style.display = 'none';
  showToast('ØªÙ… ØªØ«Ø¨ÙŠØª Ø§Ù„ØªØ·Ø¨ÙŠÙ‚', 'success');
});

async function installPWA() {
  if (!_pwaDeferredPrompt) {
    showToast('Ø§Ù„ØªØ«Ø¨ÙŠØª ØºÙŠØ± Ù…ØªØ§Ø­. Ø§ÙØªØ­ Ø§Ù„Ù‚Ø§Ø¦Ù…Ø© ÙÙŠ Ø§Ù„Ù…ØªØµÙØ­ ÙˆØ§Ø®ØªØ± "Ø¥Ø¶Ø§ÙØ© Ø¥Ù„Ù‰ Ø§Ù„Ø´Ø§Ø´Ø© Ø§Ù„Ø±Ø¦ÙŠØ³ÙŠØ©".', 'info');
    return;
  }
  _pwaDeferredPrompt.prompt();
  try {
    const { outcome } = await _pwaDeferredPrompt.userChoice;
    if (outcome === 'accepted') showToast('Ø³ÙŠØªÙ… Ø§Ù„ØªØ«Ø¨ÙŠØª', 'success');
  } catch (e) {
    console.warn('install prompt error', e);
  } finally {
    _pwaDeferredPrompt = null;
    const btn = document.getElementById('pwaInstallBtn');
    if (btn) btn.style.display = 'none';
  }
}

// â•â• Mobile Panels â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
function toggleSidebar() {
  const sb = document.getElementById('sidebarPanel');
  const ov = document.getElementById('mobileOverlay');
  const cp = document.getElementById('controlPanel');
  cp.classList.remove('open');
  sb.classList.toggle('open');
  ov.classList.toggle('show', sb.classList.contains('open'));
}

function toggleControl() {
  const cp = document.getElementById('controlPanel');
  const ov = document.getElementById('mobileOverlay');
  const sb = document.getElementById('sidebarPanel');
  sb.classList.remove('open');
  cp.classList.toggle('open');
  ov.classList.toggle('show', cp.classList.contains('open'));
}

function closeAllPanels() {
  document.getElementById('sidebarPanel').classList.remove('open');
  document.getElementById('controlPanel').classList.remove('open');
  document.getElementById('mobileOverlay').classList.remove('show');
}

// â•â• Conversations â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
async function loadConversations(showSpinner = true) {
  if (showSpinner) {
    document.getElementById('customerList').innerHTML =
      '<div class="text-center py-5" style="color:var(--text-muted)">' +
      '<div class="spinner-border spinner-border-sm mb-2"></div>' +
      '<div class="small">Ø¬Ø§Ø±ÙŠ Ø§Ù„ØªØ­Ù…ÙŠÙ„...</div></div>';
  }
  try {
    const previousPending = new Map(allConversations.map(c => [c.sender_id, c.pending_reviews_count || 0]));
    const res  = await apiFetch('/api/conversations');
    const data = await res.json();
    allConversations = data.conversations || [];
    if (!showSpinner) {
      for (const c of allConversations) {
        const prev = previousPending.get(c.sender_id) || pendingReviewsBySender[c.sender_id] || 0;
        const next = c.pending_reviews_count || 0;
        const previousTime = latestConversationTimeBySender[c.sender_id] || '';
        const nextTime = c.last_time || '';
        const prevImageId = latestImageIdBySender[c.sender_id] || 0;
        const nextImageId = Number(c.last_image_id || 0);
        const newImageArrived = nextImageId > prevImageId && prevImageId > 0;

        const isCurrentlyOpen = c.sender_id === currentSenderId;

        if (newImageArrived) {
          flashConversation(c.sender_id);
          if (!isCurrentlyOpen) playDashboardSound('image');
          else showHumanIntervention();
        } else if (nextTime && previousTime && nextTime !== previousTime && !isCurrentlyOpen) {
          playDashboardSound('message');
        }
        if (next > prev && !newImageArrived) {
          if (!isCurrentlyOpen) playDashboardSound('review');
          else showHumanIntervention();
        }
        pendingReviewsBySender[c.sender_id] = next;
        latestConversationTimeBySender[c.sender_id] = nextTime;
        latestImageIdBySender[c.sender_id] = nextImageId;
      }
    } else {
      allConversations.forEach(c => {
        pendingReviewsBySender[c.sender_id] = c.pending_reviews_count || 0;
        latestConversationTimeBySender[c.sender_id] = c.last_time || '';
        latestImageIdBySender[c.sender_id] = Number(c.last_image_id || 0);
      });
    }
    renderConversations();
  } catch (e) {
    if (showSpinner) showToast('ÙØ´Ù„ ØªØ­Ù…ÙŠÙ„ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø§Øª', 'danger');
  }
}

function setFilter(f, btn) {
  currentFilter = f;
  document.querySelectorAll('.btn-group .btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  renderConversations();
}

function filterCustomers() {
  searchQuery = document.getElementById('searchInput').value.toLowerCase();
  renderConversations();
}

function renderConversations() {
  let list = allConversations;

  if (currentFilter === 'human' || currentFilter === 'pending') {
    list = list.filter(c => Number(c.pending_reviews_count || 0) > 0);
  }
  else if (currentFilter === 'unanswered') {
    list = list.filter(c => Number(c.unanswered || 0) === 1 || c.last_direction === 'incoming');
  }
  else if (currentFilter === 'today') {
    const today = new Date().toISOString().slice(0, 10);
    list = list.filter(c => c.last_time && c.last_time.startsWith(today));
  }

  if (searchQuery) {
    list = list.filter(c =>
      (c.sender_id || '').toLowerCase().includes(searchQuery) ||
      (c.name || '').toLowerCase().includes(searchQuery)
    );
  }

  const el = document.getElementById('customerList');

  if (!list.length) {
    const emptyText = currentFilter === 'human'
      ? 'Ù„Ø§ ØªÙˆØ¬Ø¯ Ù…Ø­Ø§Ø¯Ø«Ø§Øª ØªØ­ØªØ§Ø¬ ØªØ¯Ø®Ù„ Ø¨Ø´Ø±ÙŠ'
      : currentFilter === 'unanswered'
        ? 'Ù„Ø§ ØªÙˆØ¬Ø¯ Ø±Ø³Ø§Ø¦Ù„ Ø¨Ø¯ÙˆÙ† Ø±Ø¯'
        : 'Ù„Ø§ ØªÙˆØ¬Ø¯ Ù…Ø­Ø§Ø¯Ø«Ø§Øª';
    el.innerHTML = `<div class="text-center py-4 small" style="color:var(--text-muted)">${emptyText}</div>`;
    return;
  }

  el.innerHTML = list.map(c => {
    const init    = (c.name || c.sender_id || 'ØŸ').slice(0, 2).toUpperCase();
    const time    = fmtTime(c.last_time);
    const active  = c.sender_id === currentSenderId ? 'active' : '';
    const badge   = c.pending_reviews_count > 0
      ? `<span class="badge bg-danger" style="font-size:9px;">${c.pending_reviews_count}</span>` : '';
    const adBadge = (c.ad_id || c.ref)
      ? `<span class="ad-badge"><i class="bi bi-megaphone-fill"></i></span>` : '';
    const sourceMeta = customerSourceMeta(c);
    const preview = esc(c.last_message || '...');
    return `
      <div class="customer-item ${active}" onclick="selectConversation('${c.sender_id}')">
        <div class="cust-avatar">${init}</div>
        <div class="cust-info">
          <div class="cust-name">${esc(c.name || c.sender_id)} ${adBadge}</div>
          ${sourceMeta ? `<div class="cust-preview">${sourceMeta}</div>` : ''}
          <div class="cust-preview">${preview}</div>
        </div>
        <div class="d-flex flex-column align-items-end gap-1">
          <span class="cust-time">${time}</span>${badge}
        </div>
      </div>`;
  }).join('');
}

// â•â• Select Conversation â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
async function selectConversation(senderId) {
  currentSenderId = senderId;
  closeAllPanels();

  document.getElementById('chatPlaceholder').style.display  = 'none';
  document.getElementById('chatContent').style.display      = 'flex';
  document.getElementById('controlPlaceholder').style.display = 'none';
  document.getElementById('controlContent').style.display   = 'block';

  const conv = allConversations.find(c => c.sender_id === senderId) || {};
  currentCustomer = conv;
  currentConversationAIEnabled = conv.ai_enabled !== 0 && conv.ai_enabled !== false;

  const init = (conv.name || senderId || 'ØŸ').slice(0, 2).toUpperCase();
  document.getElementById('chatAvatar').textContent   = init;
  document.getElementById('chatName').textContent     = conv.name || senderId;
  document.getElementById('chatSenderId').textContent = senderId;
  renderConversationAIToggle();
  renderAskAIButton();

  // Ad badge
  const adBadge = document.getElementById('chatAdBadge');
  if (conv.ad_id || conv.ref) {
    adBadge.style.display = '';
    document.getElementById('chatAdText').textContent = [
      conv.ad_id ? `Ø¥Ø¹Ù„Ø§Ù†: ${conv.ad_id}` : '',
      conv.ref ? `Ref: ${conv.ref}` : '',
    ].filter(Boolean).join(' | ');
  } else {
    adBadge.style.display = 'none';
  }

  // Customer form
  document.getElementById('custName').value     = conv.name     || '';
  document.getElementById('custPhone').value    = conv.phone    || '';
  document.getElementById('custProvince').value = conv.province || '';
  document.getElementById('custAddress').value  = conv.address  || '';
  setCustomerGenderUI(conv.gender || '');

  // Linked product
  const linkedNames = splitPipeList(conv.product_names || conv.product_name);
  const linkedIds = splitPipeList(conv.product_ids || conv.product_id);
  if (linkedNames.length) {
    document.getElementById('linkedProductInfo').innerHTML =
      linkedNames.map((name, idx) =>
        `<div class="fw-semibold">${esc(name)}</div>` +
        `<small style="color:var(--text-muted)">${esc(linkedIds[idx] || '')}</small>`
      ).join('<hr class="my-1" style="border-color:var(--border)">') +
      `<small style="color:var(--text-muted)">` +
      (conv.ad_id ? ` | Ø¥Ø¹Ù„Ø§Ù†: ${esc(conv.ad_id)}` : '') +
      (conv.ref ? ` | Ref: ${esc(conv.ref)}` : '') + '</small>';
  } else {
    document.getElementById('linkedProductInfo').innerHTML =
      '<span style="color:var(--text-muted)" class="small">Ù„Ø§ ÙŠÙˆØ¬Ø¯ Ù…Ù†ØªØ¬ Ù…Ø±ØªØ¨Ø·</span>';
  }

  renderConversations();
  await loadMessages(senderId);
  await loadCustomerInstructions(senderId);
  if ((conv.pending_reviews_count || 0) > 0) {
    showHumanIntervention();
  } else {
    hideHumanIntervention();
  }

  if (pollMsgTimer) clearInterval(pollMsgTimer);
  pollMsgTimer = setInterval(() => {
    if (currentSenderId) loadMessages(currentSenderId, false);
  }, 8000);
}

// â•â• Messages â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
async function loadMessages(senderId, scroll = true) {
  try {
    const res  = await apiFetch(`/api/conversations/${senderId}/messages`);
    const data = await res.json();
    const messages = data.messages || [];
    const incoming = messages.filter(m => m.direction === 'incoming');
    const latestIncoming = incoming.reduce((max, m) => Math.max(max, Number(m.id || 0)), 0);
    const latestIncomingImage = incoming
      .filter(m => m.message_type === 'image' || m.image_url)
      .reduce((max, m) => Math.max(max, Number(m.id || 0)), 0);
    const prevIncoming = latestIncomingBySender[senderId] || 0;
    const prevImage = latestImageIdBySender[senderId] || 0;

    const isCurrentlyOpen = senderId === currentSenderId;
    if (!scroll && latestIncomingImage && latestIncomingImage > prevImage && prevImage > 0) {
      if (!isCurrentlyOpen) playDashboardSound('image');
      if (isCurrentlyOpen) showHumanIntervention();
    } else if (!scroll && latestIncoming && latestIncoming > prevIncoming) {
      if (!isCurrentlyOpen) playDashboardSound('message');
    }
    latestIncomingBySender[senderId] = Math.max(prevIncoming, latestIncoming);
    latestImageIdBySender[senderId] = Math.max(prevImage, latestIncomingImage);
    renderMessages(messages, scroll);
  } catch (e) { console.error('loadMessages', e); }
}

function renderMessages(msgs, scroll = true) {
  const area = document.getElementById('messagesArea');

  if (!msgs.length) {
    area.innerHTML = '<div class="text-center small py-4" style="color:var(--text-muted)">Ù„Ø§ ØªÙˆØ¬Ø¯ Ø±Ø³Ø§Ø¦Ù„ Ø¨Ø¹Ø¯</div>';
    return;
  }

  area.innerHTML = msgs.map(m => {
    const dir  = m.direction === 'incoming' ? 'incoming' : 'outgoing';
    const time = fmtDatetime(m.created_at);
    const mediaUrl = messageMediaUrl(m);
    const mediaType = messageMediaType(m);
    const imgUrl = mediaType === 'image' ? messageImageUrl(m) : '';
    let content = '';

    if (imgUrl) {
      content += `<img src="${esc(imgUrl)}" class="msg-image mb-1"
        onclick="openLightbox(${esc(jsString(imgUrl))})"
        onerror="this.outerHTML='<div class=\\'msg-image-error\\'><i class=\\'bi bi-image\\' style=\\'font-size:24px\\'></i><br>ØªØ¹Ø°Ø± Ø¹Ø±Ø¶ Ø§Ù„ØµÙˆØ±Ø©<br><a href=\\'${esc(imgUrl)}\\' target=\\'_blank\\' rel=\\'noopener\\'>ÙØªØ­ Ø§Ù„ØµÙˆØ±Ø©</a></div>'"
        alt="ØµÙˆØ±Ø©" loading="lazy" referrerpolicy="no-referrer">`;
    } else if (mediaType === 'video' && mediaUrl && /^https?:/i.test(mediaUrl) && !looksLikeReelUrl(mediaUrl)) {
      content += `<video class="msg-video mb-1" controls preload="metadata" playsinline>
        <source src="${esc(mediaUrl)}">
      </video>`;
    } else if (mediaType === 'reel' && mediaUrl) {
      content += `<a class="msg-reel mb-1" href="${esc(mediaUrl)}" target="_blank" rel="noopener">
        <span class="msg-reel-icon"><i class="bi bi-camera-reels-fill"></i></span>
        <span class="msg-reel-copy">
          <strong>Reel Instagram</strong>
          <small>افتح المقطع</small>
        </span>
        <i class="bi bi-box-arrow-up-left"></i>
      </a>`;
    }
    if (m.text && m.text !== imgUrl && m.text !== mediaUrl) {
      content += `<div class="msg-bubble">${esc(m.text)}</div>`;
    }
    if (!content) content = '<div class="msg-bubble" style="color:var(--text-muted)"><small>[Ø±Ø³Ø§Ù„Ø© ÙØ§Ø±ØºØ©]</small></div>';

    return `<div class="msg-wrapper ${dir}">${content}<span class="msg-time">${time}</span></div>`;
  }).join('');

  if (scroll) area.scrollTop = area.scrollHeight;
}

// â•â• Human Intervention (single unified dialog) â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
let _hiBusy = false;
let _hiModalInstance = null;

function _hiSetBusy(busy, btnId = null) {
  _hiBusy = busy;
  ['hiBtnLinkProduct', 'hiBtnUnavailable', 'hiBtnAskAI', 'hiBtnSendDirect', 'hiBtnCloseReview']
    .forEach(id => {
      const b = document.getElementById(id);
      if (b) b.disabled = busy;
    });
  if (btnId && busy) {
    const b = document.getElementById(btnId);
    if (b) b.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" style="width:14px;height:14px;"></span> Ø¬Ø§Ø±ÙŠâ€¦';
  }
}

function _hiResetButtons() {
  const map = {
    hiBtnLinkProduct: '<i class="bi bi-link me-1"></i>Ø±Ø¨Ø· Ø§Ù„Ù…Ù†ØªØ¬ ÙˆØ¥Ø±Ø³Ø§Ù„ Ø±Ø¯',
    hiBtnUnavailable: '<i class="bi bi-x-circle me-1"></i>ØºÙŠØ± Ù…ØªÙˆÙØ± â€” Ø£Ø¨Ù„Øº Ø§Ù„Ø²Ø¨ÙˆÙ†',
    hiBtnAskAI: '<i class="bi bi-robot me-1"></i>Ø£Ø¹Ø¯ ØµÙŠØ§ØºØ© Ø¨Ø§Ù„Ù€ AI ÙˆØ§Ø¹Ø±Ø¶ Ø§Ù„Ø§Ù‚ØªØ±Ø§Ø­',
    hiBtnSendDirect: '<i class="bi bi-send me-1"></i>Ø£Ø±Ø³Ù„ Ø§Ù„Ù†Øµ Ù…Ø¨Ø§Ø´Ø±Ø©',
    hiBtnCloseReview: '<i class="bi bi-check2-circle me-1"></i>Ø£ØºÙ„Ù‚ Ø§Ù„Ù…Ø±Ø§Ø¬Ø¹Ø© ÙÙ‚Ø·',
  };
  Object.entries(map).forEach(([id, html]) => {
    const b = document.getElementById(id);
    if (b) { b.disabled = false; b.innerHTML = html; }
  });
}

function _hiCurrentMode() {
  const conv = allConversations.find(c => c.sender_id === currentSenderId) || {};
  const lastImg = Number(conv.last_image_id || 0);
  const lastNonImage = Number(conv.last_incoming_id || 0); // optional
  const linkedNames = splitPipeList(conv.product_names || conv.product_name);
  const hasPending = Number(conv.pending_reviews_count || 0) > 0;
  // If image is the latest incoming and no product linked yet â†’ image mode
  if (hasPending && lastImg && !linkedNames.length) return 'image';
  return 'text';
}

function _hiShowBanner(visible, mode = 'text') {
  const banner = document.getElementById('hiBanner');
  if (!banner) return;
  if (!visible) { banner.style.display = 'none'; return; }
  banner.style.display = 'flex';
  const titleEl = document.getElementById('hiBannerTitle');
  const subEl = document.getElementById('hiBannerSub');
  if (mode === 'image') {
    titleEl.textContent = 'Ø§Ù„Ø²Ø¨ÙˆÙ† Ø£Ø±Ø³Ù„ ØµÙˆØ±Ø© â€” ÙŠØ­ØªØ§Ø¬ Ø±Ø¨Ø· Ù…Ù†ØªØ¬';
    subEl.textContent = 'AI Ù…ØªÙˆÙ‚Ù Ù„Ù‡Ø°Ù‡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø©. Ø§Ø¶ØºØ· Ù„Ø§Ø®ØªÙŠØ§Ø± Ø§Ù„Ù…Ù†ØªØ¬.';
  } else {
    titleEl.textContent = 'ØªØ¯Ø®Ù„ Ø¨Ø´Ø±ÙŠ Ù…Ø·Ù„ÙˆØ¨';
    subEl.textContent = 'Ø§Ø¶ØºØ· Ù„ÙØªØ­ Ø®ÙŠØ§Ø±Ø§Øª Ø§Ù„Ù…Ø¹Ø§Ù„Ø¬Ø©';
  }
}

function showHumanIntervention() {
  if (!currentSenderId) return;
  const mode = _hiCurrentMode();
  _hiShowBanner(true, mode);
  // Ø§Ù„ØµÙˆØª ÙŠÙØ´ØºÙ‘Ù„ ÙÙ‚Ø· Ù…Ù† loadConversations/loadMessages Ø¹Ù†Ø¯ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø§Øª Ø§Ù„Ø£Ø®Ø±Ù‰.
  // Ø¯Ø§Ø®Ù„ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø© Ø§Ù„Ù…ÙØªÙˆØ­Ø© Ù„Ø§ Ù†ÙØµØ¯Ø± ØµÙˆØªØ§Ù‹ØŒ ÙÙ‚Ø· Ø´Ø§Ø±Ø© Ø¨ØµØ±ÙŠØ©.
}

function hideHumanIntervention() {
  _hiShowBanner(false);
}

function _hiPopulateProducts() {
  const sel = document.getElementById('hiUnifiedProduct');
  if (!sel) return;
  const conv = allConversations.find(c => c.sender_id === currentSenderId) || {};
  const linked = splitPipeList(conv.product_ids || conv.product_id);
  sel.innerHTML = '<option value="">â€” Ø§Ø®ØªØ± Ù…Ù†ØªØ¬Ø§Ù‹ â€”</option>' +
    allProducts.map(p =>
      `<option value="${esc(p.product_id)}" ${linked.includes(p.product_id) ? 'selected' : ''}>` +
      `${esc(p.product_name)} â€” ${esc(p.price || '')} (${esc(p.stock || '')})</option>`
    ).join('');
}

function _hiPopulateTextProducts() {
  const sel = document.getElementById('hiTextProduct');
  if (!sel) return;
  const conv = allConversations.find(c => c.sender_id === currentSenderId) || {};
  const linked = splitPipeList(conv.product_ids || conv.product_id);
  const placeholder = linked.length
    ? `â€” Ù…Ø±Ø¨ÙˆØ· Ø­Ø§Ù„ÙŠØ§Ù‹: ${linked.join(', ')} â€”`
    : 'â€” Ø§Ø®ØªØ± Ù…Ù†ØªØ¬Ø§Ù‹ (Ø§Ø®ØªÙŠØ§Ø±ÙŠ) â€”';
  sel.innerHTML = `<option value="">${esc(placeholder)}</option>` +
    allProducts.map(p =>
      `<option value="${esc(p.product_id)}">` +
      `${esc(p.product_name)} â€” ${esc(p.price || '')} (${esc(p.stock || '')})</option>`
    ).join('');
  const hint = document.getElementById('hiTextProductHint');
  if (hint) hint.style.display = linked.length ? 'none' : 'block';
}

function openInterventionDialog() {
  if (!currentSenderId) { showToast('Ø§Ø®ØªØ± Ù…Ø­Ø§Ø¯Ø«Ø© Ø£ÙˆÙ„Ø§Ù‹', 'warning'); return; }
  const mode = _hiCurrentMode();
  document.getElementById('hiModeImage').style.display = mode === 'image' ? 'block' : 'none';
  document.getElementById('hiModeText').style.display  = mode === 'text'  ? 'block' : 'none';
  document.getElementById('hiModalTitle').textContent = mode === 'image'
    ? 'ØµÙˆØ±Ø© Ù…Ù† Ø§Ù„Ø²Ø¨ÙˆÙ† â€” Ø±Ø¨Ø· Ø§Ù„Ù…Ù†ØªØ¬' : 'ØªØ¯Ø®Ù„ Ø¨Ø´Ø±ÙŠ';

  const alertBox = document.getElementById('hiAlertBox');
  alertBox.style.display = 'none';
  alertBox.textContent = '';

  _hiResetButtons();
  if (mode === 'image') {
    _hiPopulateProducts();
  } else {
    _hiPopulateTextProducts();
    document.getElementById('hiUnifiedText').value = '';
    setHIGenderUI((currentCustomer && currentCustomer.gender) || '');
  }

  if (!_hiModalInstance) {
    _hiModalInstance = new bootstrap.Modal(document.getElementById('interventionModal'));
  }
  _hiModalInstance.show();
}

function _hiCloseModal() {
  if (_hiModalInstance) _hiModalInstance.hide();
}

async function hiLinkProduct() {
  if (_hiBusy || !currentSenderId) return;
  const sel = document.getElementById('hiUnifiedProduct');
  const pid = sel.value;
  if (!pid) { showToast('Ø§Ø®ØªØ± Ù…Ù†ØªØ¬Ø§Ù‹ Ø£ÙˆÙ„Ø§Ù‹', 'warning'); return; }
  _hiSetBusy(true, 'hiBtnLinkProduct');
  try {
    const res = await apiFetch(`/api/conversations/${currentSenderId}/link_product`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ product_id: pid }),
    });
    const data = await res.json();
    if (!data.ok) {
      showToast('ÙØ´Ù„ Ø±Ø¨Ø· Ø§Ù„Ù…Ù†ØªØ¬: ' + (data.error || ''), 'danger');
      return;
    }
    const sent = data.auto_reply && data.auto_reply.sent;
    showToast(sent ? 'ØªÙ… Ø±Ø¨Ø· Ø§Ù„Ù…Ù†ØªØ¬ ÙˆØ¥Ø±Ø³Ø§Ù„ Ø±Ø¯' : 'ØªÙ… Ø±Ø¨Ø· Ø§Ù„Ù…Ù†ØªØ¬ â€” Ù„Ù… ÙŠÙØ±Ø³Ù„ Ø±Ø¯ ØªÙ„Ù‚Ø§Ø¦ÙŠ', sent ? 'success' : 'warning');
    _hiCloseModal();
    hideHumanIntervention();
    await loadConversations(false);
    await loadMessages(currentSenderId);
    await refreshLinkedProductSync();
  } catch (e) {
    showToast('Ø®Ø·Ø£: ' + e.message, 'danger');
  } finally {
    _hiSetBusy(false);
    _hiResetButtons();
  }
}

async function hiMarkUnavailable() {
  if (_hiBusy || !currentSenderId) return;
  _hiSetBusy(true, 'hiBtnUnavailable');
  try {
    await apiFetch(`/api/conversations/${currentSenderId}/mark_reviewed`, { method: 'POST' });
    await sendMessage('Ø­Ø¨ÙŠØ¨ØªÙŠ Ù„Ù„Ø£Ø³Ù Ù‡Ø°Ø§ Ø§Ù„Ù…ÙˆØ¯ÙŠÙ„ ØºÙŠØ± Ù…ØªÙˆÙØ± Ø­Ø§Ù„ÙŠØ§Ù‹ ðŸŒ¸ Ø¥Ø°Ø§ ØªØ­Ø¨ÙŠÙ† Ø£Ù‚ØªØ±Ø­Ù„Ø¬ Ù…ÙˆØ¯ÙŠÙ„ Ù…Ø´Ø§Ø¨Ù‡ØŸ');
    showToast('ØªÙ… Ø¥Ø®Ø¨Ø§Ø± Ø§Ù„Ø²Ø¨ÙˆÙ† Ø¨Ø¹Ø¯Ù… Ø§Ù„ØªÙˆÙØ±', 'info');
    _hiCloseModal();
    hideHumanIntervention();
    await loadConversations(false);
    await loadMessages(currentSenderId);
  } catch (e) {
    showToast('Ø®Ø·Ø£: ' + e.message, 'danger');
  } finally {
    _hiSetBusy(false);
    _hiResetButtons();
  }
}

async function _hiSilentLinkIfChosen() {
  const sel = document.getElementById('hiTextProduct');
  if (!sel) return;
  const pid = (sel.value || '').trim();
  if (!pid) return;
  await apiFetch(`/api/conversations/${currentSenderId}/link_product`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ product_id: pid, silent: true }),
  });
  await refreshLinkedProductSync();
}

async function _hiSyncGenderIfChosen() {
  const gender = getSelectedGender('hiGender');
  if (gender === undefined) return;
  // ÙÙ‚Ø· Ø§Ø­ÙØ¸ Ø¥Ø°Ø§ Ø§Ø®ØªÙ„Ù Ø¹Ù† Ø§Ù„Ù…Ø®Ø²Ù† ÙÙŠ Ø§Ù„Ø°Ø§ÙƒØ±Ø©
  const current = (currentCustomer && currentCustomer.gender) || '';
  if ((current || '') === (gender || '')) return;
  await setCustomerGenderQuick(gender);
}

async function hiAskAI() {
  if (_hiBusy || !currentSenderId) return;
  const text = (document.getElementById('hiUnifiedText').value || '').trim();
  _hiSetBusy(true, 'hiBtnAskAI');
  try {
    await _hiSyncGenderIfChosen();
    await _hiSilentLinkIfChosen();
    document.getElementById('messageInput').value = text;
    if (text) {
      document.getElementById('aiInstructions').value = text;
      await saveInstructions();
    }
    await askAI();
    _hiCloseModal();
    hideHumanIntervention();
  } catch (e) {
    showToast('خطأ AI: ' + e.message, 'danger');
  } finally {
    _hiSetBusy(false);
    _hiResetButtons();
  }
}

async function hiSendDirect() {
  if (_hiBusy || !currentSenderId) return;
  const text = (document.getElementById('hiUnifiedText').value || '').trim();
  if (!text) { showToast('Ø§ÙƒØªØ¨ Ù†ØµØ§Ù‹ Ø£ÙˆÙ„Ø§Ù‹', 'warning'); return; }
  _hiSetBusy(true, 'hiBtnSendDirect');
  try {
    await _hiSyncGenderIfChosen();
    await _hiSilentLinkIfChosen();
    document.getElementById('messageInput').value = text;
    await apiFetch(`/api/conversations/${currentSenderId}/mark_reviewed`, { method: 'POST' });
    await sendMessage();
    _hiCloseModal();
    hideHumanIntervention();
  } catch (e) {
    showToast('Ø®Ø·Ø£: ' + e.message, 'danger');
  } finally {
    _hiSetBusy(false);
    _hiResetButtons();
  }
}

async function hiCloseReview() {
  if (_hiBusy || !currentSenderId) return;
  _hiSetBusy(true, 'hiBtnCloseReview');
  try {
    await apiFetch(`/api/conversations/${currentSenderId}/mark_reviewed`, { method: 'POST' });
    showToast('ØªÙ… Ø¥ØºÙ„Ø§Ù‚ Ø§Ù„Ù…Ø±Ø§Ø¬Ø¹Ø©', 'success');
    _hiCloseModal();
    hideHumanIntervention();
    await loadConversations(false);
  } catch (e) {
    showToast('Ø®Ø·Ø£: ' + e.message, 'danger');
  } finally {
    _hiSetBusy(false);
    _hiResetButtons();
  }
}

async function refreshLinkedProductSync() {
  if (!currentSenderId) return;
  await loadConversations(false);
  const conv = allConversations.find(c => c.sender_id === currentSenderId);
  if (!conv) return;
  const linkedNames = splitPipeList(conv.product_names || conv.product_name);
  const linkedIds = splitPipeList(conv.product_ids || conv.product_id);
  const linkedBox = document.getElementById('linkedProductInfo');
  if (linkedBox) {
    linkedBox.innerHTML = linkedNames.length
      ? linkedNames.map((name, idx) =>
          `<div class="fw-semibold">${esc(name)}</div>` +
          `<small style="color:var(--text-muted)">${esc(linkedIds[idx] || '')}</small>`
        ).join('<hr class="my-1" style="border-color:var(--border)">')
      : '<span style="color:var(--text-muted)" class="small">Ù„Ø§ ÙŠÙˆØ¬Ø¯ Ù…Ù†ØªØ¬ Ù…Ø±ØªØ¨Ø·</span>';
  }
  const productSelect = document.getElementById('productSelect');
  if (productSelect) {
    [...productSelect.options].forEach(o => { o.selected = linkedIds.includes(o.value); });
  }
  const hiSel = document.getElementById('hiUnifiedProduct');
  if (hiSel) {
    [...hiSel.options].forEach(o => { o.selected = linkedIds.includes(o.value); });
  }
  if (typeof currentConversationAIEnabled !== 'undefined') {
    currentConversationAIEnabled = conv.ai_enabled !== 0 && conv.ai_enabled !== false;
    renderConversationAIToggle();
  }
}

async function resolveWithAI() {
  if (!currentSenderId) { showToast('Ø§Ø®ØªØ± Ù…Ø­Ø§Ø¯Ø«Ø© Ø£ÙˆÙ„Ø§Ù‹', 'warning'); return; }
  openInterventionDialog();
}

// â•â• Lightbox â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
function openLightbox(url) {
  const overlay = document.createElement('div');
  overlay.className = 'lightbox-overlay';
  overlay.onclick = () => overlay.remove();
  overlay.innerHTML = `<img src="${esc(url)}" referrerpolicy="no-referrer">`;
  document.body.appendChild(overlay);
}

// â•â• Send Message â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
let _isSending = false;
let _isAskingAI = false;

function _setSendingState(busy) {
  _isSending = busy;
  const sendBtn  = document.getElementById('sendBtn');
  const aiBtn    = document.getElementById('askAIBtn');
  const sendAIBtn = document.getElementById('sendAIBtn');
  if (sendBtn) {
    sendBtn.disabled = busy;
    sendBtn.innerHTML = busy
      ? '<span class="spinner-border spinner-border-sm" role="status"></span>'
      : '<i class="bi bi-send-fill"></i>';
  }
  if (aiBtn)    aiBtn.disabled = busy || !canUseAI();
  if (sendAIBtn) sendAIBtn.disabled = busy;
  renderAIActionButtons();
}

async function sendMessage(text = null, imgUrl = null) {
  if (!currentSenderId) return;
  if (_isSending) { showToast('Ø¬Ø§Ø±ÙŠ Ø§Ù„Ø¥Ø±Ø³Ø§Ù„... Ø§Ù†ØªØ¸Ø±', 'warning'); return; }

  const txt = text  !== null ? text   : (document.getElementById('messageInput').value || '').trim();
  const img = imgUrl !== null ? imgUrl : uploadedImageUrl;

  if (!txt && !img) { showToast('Ø§ÙƒØªØ¨ Ø±Ø³Ø§Ù„Ø© Ø£Ùˆ Ø§Ø±ÙØ¹ ØµÙˆØ±Ø©', 'warning'); return; }

  _setSendingState(true);
  try {
    const platform = (currentCustomer && currentCustomer.platform) || 'facebook';
    const res  = await apiFetch(`/api/conversations/${currentSenderId}/send`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: txt, image_url: img, platform }),
    });
    const data = await res.json();
    if (data.ok) {
      document.getElementById('messageInput').value = '';
      clearImage();
      if (data.warning) showToast(data.warning, 'warning');
      else showToast('ØªÙ… Ø§Ù„Ø¥Ø±Ø³Ø§Ù„', 'success');
      await loadMessages(currentSenderId);
    } else {
      const detail = data.warning || data.error || 'ManyChat Ù„Ù… ÙŠØ¤ÙƒØ¯ Ø§Ù„Ø¥Ø±Ø³Ø§Ù„';
      console.warn('[ManualSend] failure', data);
      showToast('ÙØ´Ù„ Ø§Ù„Ø¥Ø±Ø³Ø§Ù„: ' + detail, 'danger');
    }
  } catch (e) { showToast('Ø®Ø·Ø£: ' + e.message, 'danger'); }
  finally { _setSendingState(false); }
}

async function testManyChat() {
  try {
    const res = await apiFetch('/api/manychat/test');
    const data = await res.json();
    if (data.ok) {
      showToast(`ManyChat OK â€” ${data.page_name || data.page_id || 'page connected'}`, 'success');
    } else {
      console.warn('[ManyChatTest]', data);
      showToast('ManyChat ØºÙŠØ± ØµØ§Ù„Ø­: ' + (data.message || data.reason || data.status || 'unknown'), 'danger');
    }
  } catch (e) { showToast('ÙØ´Ù„ Ø§Ø®ØªØ¨Ø§Ø± ManyChat: ' + e.message, 'danger'); }
}

// â•â• Ask AI â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
async function askAI() {
  if (!currentSenderId) return;
  if (_isAskingAI) { showToast('Ø¬Ø§Ø±ÙŠ ØªØ¬Ù‡ÙŠØ² Ø§Ù‚ØªØ±Ø§Ø­ AI...', 'warning'); return; }
  if (!canUseAI()) {
    const reason = !globalAIEnabled
      ? 'AI Ù…ØªÙˆÙ‚Ù Ø­Ø§Ù„ÙŠØ§Ù‹ Ù…Ù† Ø§Ù„Ø²Ø± Ø§Ù„Ø±Ø¦ÙŠØ³ÙŠ'
      : 'AI Ù…ØªÙˆÙ‚Ù ÙÙŠ Ù‡Ø°Ù‡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø©. Ø´ØºÙ‘Ù„Ù‡ Ù…Ù† Ø²Ø± Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø© Ø£ÙˆÙ„Ø§Ù‹';
    showToast(reason, 'warning');
    renderAskAIButton();
    renderAIActionButtons();
    return;
  }
  const text             = document.getElementById('messageInput').value.trim();
  const textForAI        = text || 'Ø§Ù‚ØªØ±Ø­ Ø±Ø¯Ø§Ù‹ Ù…Ù†Ø§Ø³Ø¨Ø§Ù‹ Ø¨Ø§Ù„Ù„Ù‡Ø¬Ø© Ø§Ù„Ø¹Ø±Ø§Ù‚ÙŠØ© Ø§Ø¹ØªÙ…Ø§Ø¯Ø§Ù‹ Ø¹Ù„Ù‰ Ø¢Ø®Ø± Ø±Ø³Ø§Ø¦Ù„ Ø§Ù„Ø²Ø¨ÙˆÙ† ÙÙŠ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø©.';
  const savedInstructions = document.getElementById('aiInstructions').value.trim();
  const rewriteInstruction = 'Ù‚Ù… Ø¨Ø¥Ø¹Ø§Ø¯Ø© ØµÙŠØ§ØºØ© Ø§Ù„Ù†Øµ Ø§Ù„Ù…ÙˆØ¬ÙˆØ¯ ÙÙŠ Ø±Ø³Ø§Ù„Ø© Ø§Ù„Ù…Ø´Ø±Ù ÙÙ‚Ø·ØŒ Ù…Ø¹ Ø§Ù„Ø§Ù„ØªØ²Ø§Ù… Ø¨Ø¨Ø§Ù‚ÙŠ Ø§Ù„ØªØ¹Ù„ÙŠÙ…Ø§Øª ÙˆØ§Ù„Ù‚ÙˆØ§Ø¹Ø¯ Ø§Ù„Ù…Ø­ÙÙˆØ¸Ø©. Ù„Ø§ ØªØ¶Ù Ø³Ø¤Ø§Ù„Ø§Ù‹ Ø¬Ø¯ÙŠØ¯Ø§Ù‹ Ø¥Ø°Ø§ ÙƒØ§Ù† Ø§Ù„Ø²Ø¨ÙˆÙ† Ø£Ø¬Ø§Ø¨ Ø¹Ù„ÙŠÙ‡ Ø³Ø§Ø¨Ù‚Ø§Ù‹.';
  const extraInstructions = savedInstructions
    ? `${savedInstructions}\n\n${rewriteInstruction}`
    : rewriteInstruction;
  const productId        = document.getElementById('productSelect').value;

  const btn = document.getElementById('askAIBtn');
  _isAskingAI = true;
  btn.disabled = true;
  btn.innerHTML = '<div class="spinner-border spinner-border-sm text-white" style="width:14px;height:14px;"></div>';

  try {
    const res  = await apiFetch(`/api/conversations/${currentSenderId}/ask_ai`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: textForAI, extra_instructions: extraInstructions, product_id: productId }),
    });
    const data = await res.json();
    if (data.reply) {
      aiPendingReply = data.reply;
      document.getElementById('aiReplyText').textContent      = data.reply;
      document.getElementById('aiReplyPreview').style.display = 'block';
    } else {
      showToast('AI Ù„Ù… ÙŠØ³ØªØ·Ø¹ Ø§Ù„Ø±Ø¯ â€” ÙŠÙ…ÙƒÙ†Ùƒ Ø§Ù„ØªØ¯Ø®Ù„ ÙŠØ¯ÙˆÙŠØ§Ù‹', 'warning');
      showHumanIntervention();
    }
  } catch (e) { showToast('Ø®Ø·Ø£ AI: ' + e.message, 'danger'); }
  finally {
    _isAskingAI = false;
    renderAskAIButton();
  }
}

function sendAIReply()    { if (aiPendingReply) { sendMessage(aiPendingReply); dismissAIPreview(); } }
function editAIReply()    { if (aiPendingReply) { document.getElementById('messageInput').value = aiPendingReply; dismissAIPreview(); } }
function dismissAIPreview() {
  aiPendingReply = null;
  document.getElementById('aiReplyPreview').style.display = 'none';
}

// â•â• Improve Message (independent of AI on/off) â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
let _isImproving = false;
async function improveMessage() {
  if (_isImproving) return;
  const ta = document.getElementById('messageInput');
  const text = (ta.value || '').trim();
  if (!text) {
    showToast('Ø§ÙƒØªØ¨ Ù†ØµØ§Ù‹ Ù‚Ø¨Ù„ Ø§Ù„ØªØ­Ø³ÙŠÙ†', 'warning');
    return;
  }
  const btn = document.getElementById('improveBtn');
  _isImproving = true;
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" style="width:14px;height:14px;"></span>';
  }
  try {
    const res = await apiFetch('/api/improve_message', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
    });
    const data = await res.json();
    if (data.ok && data.improved) {
      ta.value = data.improved;
      ta.focus();
      showToast('ØªÙ… ØªØ­Ø³ÙŠÙ† Ø§Ù„Ù†Øµ', 'success');
    } else if (data.improved) {
      ta.value = data.improved;
      showToast(data.error ? `ØªØ¹Ø°Ø± Ø§Ù„ØªØ­Ø³ÙŠÙ† Ø¨Ø§Ù„ÙƒØ§Ù…Ù„: ${data.error}` : 'Ù„Ù… ÙŠØªÙ… Ø§Ù„ØªØ­Ø³ÙŠÙ†', 'warning');
    } else {
      showToast(data.error || 'ÙØ´Ù„ ØªØ­Ø³ÙŠÙ† Ø§Ù„Ù†Øµ', 'danger');
    }
  } catch (e) {
    showToast('Ø®Ø·Ø£ ÙÙŠ Ø§Ù„ØªØ­Ø³ÙŠÙ†: ' + e.message, 'danger');
  } finally {
    _isImproving = false;
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-magic"></i>';
    }
  }
}

// â•â• Image Upload â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
async function handleImageUpload(event) {
  const file = event.target.files[0];
  if (!file) return;

  const reader = new FileReader();
  reader.onload = e => {
    document.getElementById('previewImg').src              = e.target.result;
    document.getElementById('imagePreview').style.display = 'block';
    document.getElementById('imagePreview').style.cssText = 'display:block!important;';
  };
  reader.readAsDataURL(file);

  const fd = new FormData();
  fd.append('image', file);
  try {
    const res  = await apiFetch('/api/upload_image', { method: 'POST', body: fd });
    const data = await res.json();
    if (data.image_url) {
      uploadedImageUrl = data.image_url;
      showToast(data.warning || 'تم رفع الصورة، اضغط زر الإرسال لإرسالها', data.send_ready === false ? 'warning' : 'success');
    }
    else showToast('ÙØ´Ù„ Ø±ÙØ¹ Ø§Ù„ØµÙˆØ±Ø©', 'danger');
  } catch (e) { showToast('Ø®Ø·Ø£ Ø±ÙØ¹: ' + e.message, 'danger'); }
}

function clearImage() {
  uploadedImageUrl = null;
  document.getElementById('imagePreview').style.display = 'none';
  document.getElementById('imagePreview').style.cssText = 'display:none!important;';
  document.getElementById('previewImg').src             = '';
  document.getElementById('imageUpload').value          = '';
}

// â•â• Customer Info â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
function getSelectedGender(groupName) {
  const el = document.querySelector(`input[name="${groupName}"]:checked`);
  return el ? (el.value || '') : '';
}

function setCustomerGenderUI(gender) {
  const value = (gender || '').toLowerCase();
  const ids = { 'male': 'custGenderMale', 'female': 'custGenderFemale', '': 'custGenderUnknown' };
  const id = ids[value] || 'custGenderUnknown';
  const el = document.getElementById(id);
  if (el) el.checked = true;
}

function setHIGenderUI(gender) {
  const value = (gender || '').toLowerCase();
  const ids = { 'male': 'hiGenderMale', 'female': 'hiGenderFemale', '': 'hiGenderUnknown' };
  const id = ids[value] || 'hiGenderUnknown';
  const el = document.getElementById(id);
  if (el) el.checked = true;
}

async function saveCustomerInfo() {
  if (!currentSenderId) return;
  const data = {
    name:     document.getElementById('custName').value.trim(),
    phone:    document.getElementById('custPhone').value.trim(),
    province: document.getElementById('custProvince').value.trim(),
    address:  document.getElementById('custAddress').value.trim(),
    gender:   getSelectedGender('custGender'),
  };
  try {
    const res  = await apiFetch(`/api/conversations/${currentSenderId}/customer`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data),
    });
    const r = await res.json();
    if (r.ok) {
      if (currentCustomer) currentCustomer.gender = data.gender || null;
      showToast('ØªÙ… Ø­ÙØ¸ Ø¨ÙŠØ§Ù†Ø§Øª Ø§Ù„Ø²Ø¨ÙˆÙ†', 'success');
      loadConversations(false);
    } else {
      showToast('ÙØ´Ù„ Ø§Ù„Ø­ÙØ¸', 'danger');
    }
  } catch (e) { showToast('Ø®Ø·Ø£: ' + e.message, 'danger'); }
}

async function setCustomerGenderQuick(gender) {
  if (!currentSenderId) return false;
  try {
    const res  = await apiFetch(`/api/conversations/${currentSenderId}/gender`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ gender: gender || '' }),
    });
    const r = await res.json();
    if (r.ok && currentCustomer) currentCustomer.gender = r.gender || null;
    setCustomerGenderUI(r.gender || '');
    return true;
  } catch (e) {
    console.error('setCustomerGenderQuick', e);
    return false;
  }
}

// â•â• Products â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
async function loadProducts() {
  try {
    const res  = await apiFetch('/api/products');
    const data = await res.json();
    allProducts = data.products || [];
    const opts = allProducts.map(p =>
      `<option value="${esc(p.product_id)}">${esc(p.product_name)} â€” ${esc(p.price)} (${esc(p.stock)})</option>`
    ).join('');
    document.getElementById('productSelect').innerHTML  = '<option value="">â€” Ø§Ø®ØªØ± Ù…Ù†ØªØ¬Ø§Ù‹ â€”</option>' + opts;
    document.getElementById('orderProduct').innerHTML   = '<option value="">â€” Ø§Ø®ØªØ± â€”</option>' +
      allProducts.map(p => `<option value="${esc(p.product_id)}">${esc(p.product_name)}</option>`).join('');
  } catch (e) { console.error('loadProducts', e); }
}

let _isLinking = false;
async function linkProduct() {
  if (_isLinking || !currentSenderId) return;
  const productIds = selectedValues(document.getElementById('productSelect'));
  if (!productIds.length) { showToast('Ø§Ø®ØªØ± Ù…Ù†ØªØ¬Ø§Ù‹ ÙˆØ§Ø­Ø¯Ø§Ù‹ Ø¹Ù„Ù‰ Ø§Ù„Ø£Ù‚Ù„', 'warning'); return; }
  _isLinking = true;
  const linkBtn = document.querySelector('button[onclick="linkProduct()"]');
  const original = linkBtn ? linkBtn.innerHTML : '';
  if (linkBtn) {
    linkBtn.disabled = true;
    linkBtn.innerHTML = '<span class="spinner-border spinner-border-sm" style="width:14px;height:14px;"></span>';
  }
  try {
    const res  = await apiFetch(`/api/conversations/${currentSenderId}/link_product`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ product_ids: productIds, silent: true }),
    });
    const data = await res.json();
    if (data.ok) {
      showToast('ØªÙ… Ø±Ø¨Ø· Ø§Ù„Ù…Ù†ØªØ¬ Ø¨Ø¯ÙˆÙ† Ø¥Ø±Ø³Ø§Ù„ Ø£ÙŠ ØªÙØ§ØµÙŠÙ„ Ù„Ù„Ø²Ø¨ÙˆÙ†', 'success');
      await loadMessages(currentSenderId);
      await refreshLinkedProductSync();
    } else showToast('ÙØ´Ù„ Ø±Ø¨Ø· Ø§Ù„Ù…Ù†ØªØ¬', 'danger');
  } catch (e) { showToast('Ø®Ø·Ø£: ' + e.message, 'danger'); }
  finally {
    _isLinking = false;
    if (linkBtn) { linkBtn.disabled = false; linkBtn.innerHTML = original; }
  }
}

async function sendProductDetails() {
  if (!currentSenderId) return;
  const productIds = selectedValues(document.getElementById('productSelect'));
  if (!productIds.length) { showToast('Ø§Ø®ØªØ± Ù…Ù†ØªØ¬Ø§Ù‹ ÙˆØ§Ø­Ø¯Ø§Ù‹ Ø¹Ù„Ù‰ Ø§Ù„Ø£Ù‚Ù„', 'warning'); return; }
  for (const pid of productIds) {
    const p = allProducts.find(x => x.product_id === pid);
    if (!p) continue;
    const text = `ØªØ¯Ù„Ù„ÙŠÙ† Ø¹ÙŠÙ†ÙŠ ðŸŒ¸\nÙ‡Ø°Ø§ ${p.product_name}\nØ§Ù„Ø³Ø¹Ø±: ${p.price}\nØ§Ù„Ù…Ù‚Ø§Ø³Ø§Øª: ${p.sizes || 'ØºÙŠØ± Ù…Ø­Ø¯Ø¯'}`;
    await sendMessage(text);
    for (const url of productImageList(p)) {
      await sendMessage(null, url);
    }
  }
}

// â•â• AI Instructions â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
async function loadCustomerInstructions(senderId) {
  try {
    const res  = await apiFetch(`/api/conversations/${senderId}/instructions`);
    const data = await res.json();
    document.getElementById('aiInstructions').value  = data.instructions  || '';
    document.getElementById('applyToAll').checked    = data.apply_to_all || false;
  } catch (e) { console.error('loadInstructions', e); }
}

async function saveInstructions() {
  if (!currentSenderId) return;
  const body = {
    instructions:  document.getElementById('aiInstructions').value.trim(),
    apply_to_all:  document.getElementById('applyToAll').checked,
  };
  try {
    const res  = await apiFetch(`/api/conversations/${currentSenderId}/save_instructions`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body),
    });
    const data = await res.json();
    if (data.ok) showToast('ØªÙ… Ø­ÙØ¸ Ø§Ù„ØªØ¹Ù„ÙŠÙ…Ø§Øª', 'success');
    else showToast('ÙØ´Ù„ Ø§Ù„Ø­ÙØ¸', 'danger');
  } catch (e) { showToast('Ø®Ø·Ø£: ' + e.message, 'danger'); }
}

// â•â• Stats â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
async function loadStats() {
  try {
    const res  = await apiFetch('/api/dashboard_stats');
    const data = await res.json();
    document.getElementById('statPending').textContent  = `${data.pending_reviews  || 0} Ù…Ø±Ø§Ø¬Ø¹Ø©`;
    document.getElementById('statOrders').textContent   = `${data.orders_today    || 0} Ø·Ù„Ø¨`;
    document.getElementById('statMessages').textContent = `${data.messages_today  || 0} Ø±Ø³Ø§Ù„Ø©`;
    globalAIEnabled = data.ai_enabled !== false;
    renderAIToggle();
  } catch (e) { console.error('loadStats', e); }
}

function renderAIToggle() {
  const btn = document.getElementById('aiToggleBtn');
  const txt = document.getElementById('aiToggleText');
  if (!btn || !txt) return;
  btn.classList.toggle('off', !globalAIEnabled);
  txt.textContent = globalAIEnabled ? 'AI ÙŠØ¹Ù…Ù„' : 'AI Ù…ØªÙˆÙ‚Ù';
  btn.title = globalAIEnabled ? 'Ø§Ø¶ØºØ· Ù„Ø¥ÙŠÙ‚Ø§Ù AI ÙÙŠ ÙƒÙ„ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø§Øª' : 'Ø§Ø¶ØºØ· Ù„ØªØ´ØºÙŠÙ„ AI ÙÙŠ ÙƒÙ„ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø§Øª';
  renderAskAIButton();
  renderAIActionButtons();
}

function renderConversationAIToggle() {
  const btn = document.getElementById('conversationAiBtn');
  const txt = document.getElementById('conversationAiText');
  if (!btn || !txt) return;
  btn.classList.toggle('btn-warning', !currentConversationAIEnabled);
  btn.classList.toggle('btn-outline-warning', currentConversationAIEnabled);
  txt.textContent = currentConversationAIEnabled ? 'AI ÙŠØ¹Ù…Ù„ Ù‡Ù†Ø§' : 'AI Ù…ØªÙˆÙ‚Ù Ù‡Ù†Ø§';
  btn.title = currentConversationAIEnabled
    ? 'Ø¥ÙŠÙ‚Ø§Ù AI ÙÙŠ Ù‡Ø°Ù‡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø© ÙÙ‚Ø·'
    : 'ØªØ³Ù„ÙŠÙ… Ù‡Ø°Ù‡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø© Ø¥Ù„Ù‰ AI';
  renderAskAIButton();
  renderAIActionButtons();
}

function canUseAI() {
  return Boolean(currentSenderId && globalAIEnabled && currentConversationAIEnabled);
}

function renderAskAIButton() {
  const btn = document.getElementById('askAIBtn');
  if (!btn) return;
  if (_isAskingAI) {
    btn.disabled = true;
    btn.innerHTML = '<div class="spinner-border spinner-border-sm text-white" style="width:14px;height:14px;"></div>';
    return;
  }
  const enabled = canUseAI() && !_isSending;
  btn.disabled = !enabled;
  btn.classList.toggle('disabled', !enabled);
  btn.classList.toggle('btn-secondary', !enabled);
  btn.classList.toggle('btn-info', enabled);
  btn.title = enabled
    ? 'Ø§Ù‚ØªØ±Ø§Ø­ AI'
    : (!globalAIEnabled ? 'AI Ù…ØªÙˆÙ‚Ù Ù…Ù† Ø§Ù„Ø²Ø± Ø§Ù„Ø±Ø¦ÙŠØ³ÙŠ' : 'AI Ù…ØªÙˆÙ‚Ù ÙÙŠ Ù‡Ø°Ù‡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø©');
  btn.innerHTML = '<i class="bi bi-robot d-block mb-1"></i><span style="font-size:9px;">AI</span>';
}

function renderAIActionButtons() {
  const enabled = canUseAI() && !_isSending && !_isAskingAI;
  const resolveBtn = document.getElementById('resolveAIBtn');
  if (resolveBtn) {
    resolveBtn.disabled = !enabled;
    resolveBtn.classList.toggle('disabled', !enabled);
    resolveBtn.title = enabled
      ? 'Ø­Ù„ Ø§Ù„Ù…Ø´ÙƒÙ„Ø© Ø¨Ø§Ù„Ù€ AI'
      : (!globalAIEnabled ? 'AI Ù…ØªÙˆÙ‚Ù Ù…Ù† Ø§Ù„Ø²Ø± Ø§Ù„Ø±Ø¦ÙŠØ³ÙŠ' : 'AI Ù…ØªÙˆÙ‚Ù ÙÙŠ Ù‡Ø°Ù‡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø©');
  }
  const improveBtn = document.getElementById('improveBtn');
  if (improveBtn) {
    improveBtn.disabled = _isSending || _isImproving;
    improveBtn.title = 'ØªØ­Ø³ÙŠÙ† Ø§Ù„Ø±Ø³Ø§Ù„Ø© (ÙŠØ¹Ù…Ù„ Ø¯Ø§Ø¦Ù…Ø§Ù‹ØŒ Ø­ØªÙ‰ Ù„Ùˆ AI Ù…ØªÙˆÙ‚Ù)';
  }
}

async function toggleGlobalAI() {
  const next = !globalAIEnabled;
  try {
    const res = await apiFetch('/api/settings/ai', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: next }),
    });
    const data = await res.json();
    if (data.ok) {
      globalAIEnabled = data.ai_enabled;
      renderAIToggle();
      showToast(globalAIEnabled ? 'ØªÙ… ØªØ´ØºÙŠÙ„ AI Ù„ÙƒÙ„ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø§Øª' : 'ØªÙ… Ø¥ÙŠÙ‚Ø§Ù AI Ù„ÙƒÙ„ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø§Øª', globalAIEnabled ? 'success' : 'warning');
    } else {
      showToast('ÙØ´Ù„ ØªØºÙŠÙŠØ± Ø­Ø§Ù„Ø© AI', 'danger');
    }
  } catch (e) { showToast('Ø®Ø·Ø£: ' + e.message, 'danger'); }
}

async function toggleConversationAI() {
  if (!currentSenderId) { showToast('Ø§Ø®ØªØ± Ù…Ø­Ø§Ø¯Ø«Ø© Ø£ÙˆÙ„Ø§Ù‹', 'warning'); return; }
  const next = !currentConversationAIEnabled;
  try {
    const res = await apiFetch(`/api/conversations/${currentSenderId}/ai`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled: next }),
    });
    const data = await res.json();
    if (data.ok) {
      currentConversationAIEnabled = data.ai_enabled;
      if (currentCustomer) currentCustomer.ai_enabled = data.ai_enabled ? 1 : 0;
      renderConversationAIToggle();
      showToast(
        currentConversationAIEnabled ? 'ØªÙ… ØªØ³Ù„ÙŠÙ… Ù‡Ø°Ù‡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø© Ø¥Ù„Ù‰ AI' : 'ØªÙ… Ø¥ÙŠÙ‚Ø§Ù AI ÙÙŠ Ù‡Ø°Ù‡ Ø§Ù„Ù…Ø­Ø§Ø¯Ø«Ø© ÙÙ‚Ø·',
        currentConversationAIEnabled ? 'success' : 'warning',
      );
      loadConversations(false);
    } else {
      showToast('ÙØ´Ù„ ØªØºÙŠÙŠØ± Ø­Ø§Ù„Ø© AI Ù„Ù„Ù…Ø­Ø§Ø¯Ø«Ø©', 'danger');
    }
  } catch (e) { showToast('Ø®Ø·Ø£: ' + e.message, 'danger'); }
}

// â•â• Quick Actions â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
function openOrderModal() {
  if (!currentSenderId) { showToast('Ø§Ø®ØªØ± Ù…Ø­Ø§Ø¯Ø«Ø© Ø£ÙˆÙ„Ø§Ù‹', 'warning'); return; }
  if (currentCustomer) {
    document.getElementById('orderName').value     = currentCustomer.name     || '';
    document.getElementById('orderPhone').value    = currentCustomer.phone    || '';
    document.getElementById('orderProvince').value = currentCustomer.province || '';
    document.getElementById('orderAddress').value  = currentCustomer.address  || '';
  }
  new bootstrap.Modal(document.getElementById('orderModal')).show();
}

async function submitOrder() {
  if (!currentSenderId) return;
  const selEl = document.getElementById('orderProduct');
  const selectedOptions = [...selEl.selectedOptions].filter(o => o.value);
  const data  = {
    customer_name: document.getElementById('orderName').value,
    phone:         document.getElementById('orderPhone').value,
    province:      document.getElementById('orderProvince').value,
    address:       document.getElementById('orderAddress').value,
    product_ids:   selectedOptions.map(o => o.value),
    product_names: selectedOptions.map(o => o.text),
    product_id:    selectedOptions[0]?.value || '',
    product_name:  selectedOptions[0]?.text || '',
    size:          document.getElementById('orderSize').value,
    color:         document.getElementById('orderColor').value,
    notes:         document.getElementById('orderNotes').value,
  };
  try {
    const res  = await apiFetch(`/api/conversations/${currentSenderId}/create_order`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data),
    });
    const r = await res.json();
    if (r.ok) {
      showToast('ØªÙ… ØªØ«Ø¨ÙŠØª Ø§Ù„Ø·Ù„Ø¨', 'success');
      bootstrap.Modal.getInstance(document.getElementById('orderModal')).hide();
    } else showToast('ÙØ´Ù„ ØªØ«Ø¨ÙŠØª Ø§Ù„Ø·Ù„Ø¨: ' + (r.error || ''), 'danger');
  } catch (e) { showToast('Ø®Ø·Ø£: ' + e.message, 'danger'); }
}

async function sendCatalog() {
  if (!currentSenderId) { showToast('Ø§Ø®ØªØ± Ù…Ø­Ø§Ø¯Ø«Ø© Ø£ÙˆÙ„Ø§Ù‹', 'warning'); return; }
  try {
    const res = await apiFetch(`/api/conversations/${currentSenderId}/send_catalog`, { method: 'POST' });
    const data = await res.json();
    if (data.ok) {
      showToast(`ØªÙ… Ø¥Ø±Ø³Ø§Ù„ Ø§Ù„ÙƒØªØ§Ù„ÙˆØ¬ Ù…Ø¹ ${data.image_count || 0} ØµÙˆØ±Ø©`, data.sent ? 'success' : 'warning');
      await loadMessages(currentSenderId);
    } else {
      showToast('ÙØ´Ù„ Ø¥Ø±Ø³Ø§Ù„ Ø§Ù„ÙƒØªØ§Ù„ÙˆØ¬', 'danger');
    }
  } catch (e) { showToast('Ø®Ø·Ø£: ' + e.message, 'danger'); }
}

async function markHumanReview() {
  if (!currentSenderId) { showToast('Ø§Ø®ØªØ± Ù…Ø­Ø§Ø¯Ø«Ø© Ø£ÙˆÙ„Ø§Ù‹', 'warning'); return; }
  try {
    const res  = await apiFetch(`/api/conversations/${currentSenderId}/mark_reviewed`, { method: 'POST' });
    const data = await res.json();
    if (data.ok) { showToast('ØªÙ… Ø¥ØºÙ„Ø§Ù‚ Ø§Ù„Ù…Ø±Ø§Ø¬Ø¹Ø©', 'success'); loadConversations(false); }
  } catch (e) { showToast('Ø®Ø·Ø£: ' + e.message, 'danger'); }
}

// â•â• Helpers â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•â•
function handleKeyDown(e) {
  if (e.ctrlKey && e.key === 'Enter') { e.preventDefault(); sendMessage(); }
}

function toggleSection(id) {
  const el = document.getElementById(id);
  el.style.display = el.style.display === 'none' ? '' : 'none';
}

function esc(s) {
  return String(s || '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function jsString(s) {
  return JSON.stringify(String(s || ''));
}

function parseRawPayload(m) {
  if (!m || !m.raw_payload) return null;
  if (typeof m.raw_payload === 'object') return m.raw_payload;
  try { return JSON.parse(m.raw_payload); } catch (_) { return null; }
}

function firstUrlFromValue(value) {
  if (!value) return '';
  if (typeof value === 'string') {
    const direct = value.trim();
    if (direct.startsWith('http')) return direct;
    const match = direct.match(/https?:\/\/\S+/);
    return match ? match[0].replace(/[).,Ø›ØŒ]+$/, '') : '';
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      const found = firstUrlFromValue(item);
      if (found) return found;
    }
  }
  if (typeof value === 'object') {
    for (const item of Object.values(value)) {
      const found = firstUrlFromValue(item);
      if (found) return found;
    }
  }
  return '';
}

function looksLikeReelUrl(url) {
  return /instagram\.com\/reels?\//i.test(String(url || '')) || /\/reels?\//i.test(String(url || ''));
}

function looksLikeVideoUrl(url) {
  return /^https?:\/\/\S+\.(?:mp4|mov|m4v|webm)(?:\?\S*)?$/i.test(String(url || '')) ||
    /(?:video|\/reels?\/)/i.test(String(url || ''));
}

function messageImageUrl(m) {
  const explicit = (m.image_url || '').trim();
  if (explicit && !looksLikeVideoUrl(explicit) && !looksLikeReelUrl(explicit)) return explicit;

  const text = (m.text || '').trim();
  if (/^https?:\/\/\S+\.(?:png|jpe?g|gif|webp)(?:\?\S*)?$/i.test(text)) return text;
  if (/^https?:\/\/(?:scontent|.*\.fbcdn\.net|.*facebook).*$/i.test(text)) return text;
  return '';
}

function messageMediaUrl(m) {
  const explicit = (m.image_url || '').trim();
  if (explicit) return explicit;
  const fromText = firstUrlFromValue(m.text || '');
  if (fromText) return fromText;
  return firstUrlFromValue(parseRawPayload(m));
}

function messageMediaType(m) {
  const type = String(m.message_type || '').toLowerCase();
  if (type === 'reel' || type === 'video' || type === 'image') return type;
  const url = messageMediaUrl(m);
  if (looksLikeReelUrl(url)) return 'reel';
  if (looksLikeVideoUrl(url)) return 'video';
  if (messageImageUrl(m)) return 'image';
  return '';
}

function productImageList(product) {
  const raw = product?.image_url;
  const list = Array.isArray(raw) ? raw : (raw ? [raw] : []);
  return [...new Set(list.map(x => String(x || '').trim()).filter(Boolean))];
}

function selectedValues(selectEl) {
  return [...(selectEl?.selectedOptions || [])].map(opt => opt.value).filter(Boolean);
}

function splitPipeList(value) {
  return String(value || '').split('||').map(x => x.trim()).filter(Boolean);
}

function customerSourceMeta(c) {
  const parts = [];
  if (c.ad_id) parts.push(`Ø¥Ø¹Ù„Ø§Ù†: ${esc(c.ad_id)}`);
  if (c.ref) parts.push(`Ref: ${esc(c.ref)}`);
  return parts.join(' | ');
}

function unlockAudioOnce() {
  audioUnlocked = true;
  [messageSound, reviewSound, imageSound].forEach(audio => {
    audio.volume = 0.85;
    audio.play().then(() => {
      audio.pause();
      audio.currentTime = 0;
    }).catch(() => {});
  });
}

function playDashboardSound(kind) {
  let audio = messageSound;
  if (kind === 'review') audio = reviewSound;
  else if (kind === 'image') audio = imageSound;
  if (!audio || !audioUnlocked) return;
  try {
    audio.pause();
    audio.currentTime = 0;
    audio.volume = (kind === 'image' || kind === 'review') ? 0.95 : 0.7;
    audio.play().catch(() => {});
  } catch (_) {}
}

function flashConversation(senderId) {
  setTimeout(() => {
    const items = document.querySelectorAll('.customer-item');
    items.forEach(it => {
      if (it.getAttribute('onclick')?.includes(`'${senderId}'`)) {
        it.classList.add('flash-image');
        setTimeout(() => it.classList.remove('flash-image'), 5000);
      }
    });
  }, 30);
}

const BAGHDAD_TZ = 'Asia/Baghdad';

function _normalizeIso(iso) {
  // If the timestamp has no timezone marker, assume legacy UTC and append Z.
  // Matches "+03:00", "-05:30" or trailing "Z".
  if (!iso) return iso;
  return /([zZ]|[+\-]\d{2}:?\d{2})$/.test(iso) ? iso : iso + 'Z';
}

function _baghdadDateString(d) {
  return d.toLocaleDateString('en-CA', { timeZone: BAGHDAD_TZ });
}

function fmtTime(iso) {
  if (!iso) return '';
  try {
    const d = new Date(_normalizeIso(iso)), now = new Date();
    return _baghdadDateString(d) === _baghdadDateString(now)
      ? d.toLocaleTimeString('ar', { hour: '2-digit', minute: '2-digit', timeZone: BAGHDAD_TZ })
      : d.toLocaleDateString('ar', { month: 'short', day: 'numeric', timeZone: BAGHDAD_TZ });
  } catch { return ''; }
}

function fmtDatetime(iso) {
  if (!iso) return '';
  try { return new Date(_normalizeIso(iso)).toLocaleTimeString('ar', { hour: '2-digit', minute: '2-digit', timeZone: BAGHDAD_TZ }); }
  catch { return ''; }
}

function showToast(msg, type = 'info') {
  const icons = { success: 'check-circle-fill', danger: 'exclamation-triangle-fill',
                  warning: 'exclamation-circle-fill', info: 'info-circle-fill' };
  const el = document.createElement('div');
  el.className = `toast align-items-center text-bg-${type} border-0`;
  el.setAttribute('role', 'alert');
  el.innerHTML = `<div class="d-flex"><div class="toast-body">
    <i class="bi bi-${icons[type] || icons.info} me-1"></i>${esc(msg)}
  </div><button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button></div>`;
  document.getElementById('toastContainer').appendChild(el);
  const t = new bootstrap.Toast(el, { delay: 4000 });
  t.show();
  el.addEventListener('hidden.bs.toast', () => el.remove());
}
