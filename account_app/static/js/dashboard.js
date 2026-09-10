// ── State ──────────────────────────────────────────────────────────────────
const DASH_PARAMS     = new URLSearchParams(window.location.search);
const DASH_KEY        = DASH_PARAMS.get('key') || '';
const INITIAL_SENDER_ID = DASH_PARAMS.get('sender_id') || '';
let initialSenderOpened = false;
let currentSenderId   = null;
let currentCustomer   = null;
let allConversations  = [];
let currentFilter     = 'all';
let currentStoreFilter = DASH_PARAMS.get('store_id') || 'all';
let inboxRequest = null;
let inboxSearchTimer;
let searchQuery       = '';
let convLimit = 20;
let convOffset = 0;
let isLoadingMoreConv = false;
let hasMoreConv = true;
let pollConvTimer     = null;
let pollMsgTimer      = null;
let messageRequest = null;
const messageCache = new Map();
let renderedMessageSender = null;
let uploadedImageUrl  = null;
let aiPendingReply    = null;
let allProducts       = [];
let productRequestVersion = 0;
let globalAIEnabled   = true;
let currentConversationAIEnabled = true;
let audioUnlocked     = false;
const messageSound    = new Audio('/aud/cheerful-527.mp3');
const reviewSound     = new Audio('/aud/young-rooster-cock-a-doodle-doo.mp3');
const imageSound      = new Audio('/aud/young-rooster-cock-a-doodle-doo.mp3');
const latestIncomingBySender = {};
const pendingReviewsBySender = {};
const openProblemsBySender = {};
const latestConversationTimeBySender = {};
const latestImageIdBySender = {};

messageSound.preload = 'auto';
reviewSound.preload  = 'auto';
imageSound.preload   = 'auto';
imageSound.loop      = false;

// ── Auth Fetch ─────────────────────────────────────────────────────────────
function apiFetch(url, opts = {}) {
  opts.headers = { ...(opts.headers || {}), 'X-Dashboard-Key': DASH_KEY };
  if (!opts.method || opts.method.toUpperCase() === 'GET') {
    const sep = url.includes('?') ? '&' : '?';
    url = `${url}${sep}_t=${Date.now()}`;
  }
  return fetch(url, opts);
}

// ── Boot ───────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  document.addEventListener('click', unlockAudioOnce, { once: true });
  const messagesArea = document.getElementById('messagesArea');
  for (const type of ['wheel', 'touchmove']) messagesArea.addEventListener(type, () => { messagesArea.dataset.followLatest = '0'; }, {passive:true});
  const custEl = document.getElementById('customerList');
  if (custEl) custEl.addEventListener('scroll', onCustomerListScroll);
  syncConversationFilterUI();
  loadInboxStores();
  loadConversations();
  loadStats();
  loadProducts();
  pollConvTimer = setInterval(() => { if (!document.hidden && !inboxRequest) { loadConversations(false); loadStats(); } }, 4000);
  if (INITIAL_SENDER_ID && window.innerWidth <= 768) {
    history.replaceState({...(history.state || {}), dashboardConversation: INITIAL_SENDER_ID}, '', location.href);
  }
});

window.addEventListener('popstate', () => {
  if (window.innerWidth > 768) return;
  const historySender = history.state?.dashboardConversation || '';
  if (historySender && historySender !== currentSenderId) selectConversation(historySender);
  else if (currentSenderId && !historySender) closeMobileConversation(true);
});

// ══ PWA Install ═══════════════════════════════════════════════════════════
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
  showToast('تم تثبيت التطبيق', 'success');
});

async function installPWA() {
  if (!_pwaDeferredPrompt) {
    showToast('التثبيت غير متاح. افتح القائمة في المتصفح واختر "إضافة إلى الشاشة الرئيسية".', 'info');
    return;
  }
  _pwaDeferredPrompt.prompt();
  try {
    const { outcome } = await _pwaDeferredPrompt.userChoice;
    if (outcome === 'accepted') showToast('سيتم التثبيت', 'success');
  } catch (e) {
  } finally {
    _pwaDeferredPrompt = null;
    const btn = document.getElementById('pwaInstallBtn');
    if (btn) btn.style.display = 'none';
  }
}

// ══ Mobile Panels ══════════════════════════════════════════════════════════
function toggleSidebar() {
  if (window.innerWidth <= 768) { toggleDashboardDrawer(); return; }
  const sb = document.getElementById('sidebarPanel');
  const ov = document.getElementById('mobileOverlay');
  const cp = document.getElementById('controlPanel');
  const drawer = document.getElementById('dashboardDrawer');
  cp.classList.remove('open');
  drawer?.classList.remove('open');
  sb.classList.toggle('open');
  ov.classList.toggle('show', sb.classList.contains('open'));
}

function toggleControl() {
  const dialog = document.getElementById('customerToolsDialog');
  if (dialog) { closeDashboardDrawer(); dialog.showModal(); return; }
  const cp = document.getElementById('controlPanel');
  const ov = document.getElementById('mobileOverlay');
  const sb = document.getElementById('sidebarPanel');
  const drawer = document.getElementById('dashboardDrawer');
  sb.classList.remove('open');
  drawer?.classList.remove('open');
  if (window.innerWidth > 992) {
    ov.classList.remove('show');
    cp.focus({ preventScroll: true });
    return;
  }
  cp.classList.toggle('open');
  ov.classList.toggle('show', cp.classList.contains('open'));
}

function toggleDashboardDrawer() {
  const drawer = document.getElementById('dashboardDrawer');
  const ov = document.getElementById('mobileOverlay');
  const sb = document.getElementById('sidebarPanel');
  const cp = document.getElementById('controlPanel');
  sb?.classList.remove('open');
  cp?.classList.remove('open');
  drawer?.classList.toggle('open');
  ov?.classList.toggle('show', drawer?.classList.contains('open'));
}

function closeDashboardDrawer() {
  document.getElementById('dashboardDrawer')?.classList.remove('open');
  const sbOpen = document.getElementById('sidebarPanel')?.classList.contains('open');
  const cpOpen = document.getElementById('controlPanel')?.classList.contains('open');
  document.getElementById('mobileOverlay')?.classList.toggle('show', Boolean(sbOpen || cpOpen));
}

function closeAllPanels() {
  document.getElementById('sidebarPanel').classList.remove('open');
  document.getElementById('controlPanel').classList.remove('open');
  document.getElementById('dashboardDrawer')?.classList.remove('open');
  document.getElementById('mobileOverlay').classList.remove('show');
}

// ══ Conversations ══════════════════════════════════════════════════════════
async function loadConversations(showSpinner = true, isLoadMore = false) {
  if (isLoadMore && inboxRequest) return;
  inboxRequest?.abort();
  const controller = new AbortController();
  inboxRequest = controller;
  if (showSpinner && !isLoadMore) {
    document.getElementById('customerList').innerHTML =
      '<div class="text-center py-5" style="color:var(--text-muted)">' +
      '<div class="spinner-border spinner-border-sm mb-2"></div>' +
      '<div class="small">جاري التحميل...</div></div>';
  }
  try {
    if (isLoadMore) isLoadingMoreConv = true;
    
    let fetchLimit = isLoadMore ? convLimit : Math.max(convLimit, allConversations.length);
    let fetchOffset = isLoadMore ? allConversations.length : 0;
    
    const previousPending = new Map(allConversations.map(c => [c.sender_id, c.pending_reviews_count || 0]));
    const previousProblems = new Map(allConversations.map(c => [c.sender_id, c.problem_count || 0]));
    const params = inboxQuery();
    params.set("limit", fetchLimit);
    params.set("offset", fetchOffset);
    const res = await apiFetch(`/api/conversations?${params}`, {signal: controller.signal});
    if (!res.ok) throw new Error("فشل تحميل المحادثات");
    const data = await res.json();
    
    if (isLoadMore) {
      const newConvs = data.conversations || [];
      if (newConvs.length < convLimit) hasMoreConv = false;
      
      const existingIds = new Set(allConversations.map(c => c.sender_id));
      for (const nc of newConvs) {
         if (!existingIds.has(nc.sender_id)) allConversations.push(nc);
      }
      convOffset += convLimit;
    } else {
      allConversations = data.conversations || [];
      // If we got exactly what we asked for, there might be more
      if (allConversations.length < fetchLimit && allConversations.length > 0) hasMoreConv = false;
      else if (allConversations.length === 0) hasMoreConv = false;
    }
    hasMoreConv = Boolean(data.has_more);
    document.getElementById("inboxConnection").textContent = "محدّث الآن";
    if (!showSpinner) {
      for (const c of allConversations) {
        const prev = previousPending.get(c.sender_id) || pendingReviewsBySender[c.sender_id] || 0;
        const next = c.pending_reviews_count || 0;
        const prevProblems = previousProblems.get(c.sender_id) || openProblemsBySender[c.sender_id] || 0;
        const nextProblems = c.problem_count || 0;
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
        if (nextProblems > prevProblems) {
          flashConversation(c.sender_id);
          if (!isCurrentlyOpen) playDashboardSound('review');
        }
        pendingReviewsBySender[c.sender_id] = next;
        openProblemsBySender[c.sender_id] = nextProblems;
        latestConversationTimeBySender[c.sender_id] = nextTime;
        latestImageIdBySender[c.sender_id] = nextImageId;
      }
    } else {
      allConversations.forEach(c => {
        pendingReviewsBySender[c.sender_id] = c.pending_reviews_count || 0;
        openProblemsBySender[c.sender_id] = c.problem_count || 0;
        latestConversationTimeBySender[c.sender_id] = c.last_time || '';
        latestImageIdBySender[c.sender_id] = Number(c.last_image_id || 0);
      });
    }
    renderConversations();
    if (INITIAL_SENDER_ID && !initialSenderOpened) {
      const exists = allConversations.some(c => c.sender_id === INITIAL_SENDER_ID);
      if (exists) {
        initialSenderOpened = true;
        selectConversation(INITIAL_SENDER_ID);
      }
    }
  } catch (e) {
    if (e.name === "AbortError") return;
    document.getElementById("inboxConnection").textContent = "تعذر التحديث";
    console.error("loadConversations error:", e);
    if (showSpinner) {
      showToast('فشل تحميل المحادثات', 'danger');
      document.getElementById('customerList').innerHTML = `<div class="text-danger p-3 small text-start" dir="ltr" style="white-space:pre-wrap;">تعذر تحميل المحادثات. <button class="btn btn-sm btn-light" onclick="loadConversations()">إعادة المحاولة</button></div>`;
    }
  } finally {
    if (inboxRequest === controller) { inboxRequest = null; isLoadingMoreConv = false; }
  }
}

function syncConversationFilterUI() {
  const group = document.getElementById('conversationFilters');
  if (!group) return;
  const validFilters = new Set(['all', 'booked', 'problems', 'system_issues', 'unanswered']);
  if (!validFilters.has(currentFilter)) currentFilter = 'all';
  group.querySelectorAll('[data-filter]').forEach(button => {
    button.classList.toggle('active', button.dataset.filter === currentFilter);
  });
}

function setFilter(f, btn = null) {
  const requested = String(f || btn?.dataset?.filter || 'all').trim();
  currentFilter = requested || 'all';
  syncConversationFilterUI();
  refreshInboxFilters();
}

const inboxFields = {stage:'filterStage', platform:'filterPlatform', province:'filterProvince', ai:'filterAI', date_from:'filterFrom', date_to:'filterTo', sort:'filterSort'};
function inboxQuery() {
  const params = new URLSearchParams({status:currentFilter});
  if (currentStoreFilter !== 'all') params.set('store_id', currentStoreFilter);
  const q = document.getElementById('searchInput').value.trim();
  if (q) params.set('q', q);
  for (const [key,id] of Object.entries(inboxFields)) {
    const value = document.getElementById(id).value.trim();
    if (value && value !== 'all' && value !== 'latest') params.set(key,value);
  }
  return params;
}
async function loadInboxStores() {
  try {
    const res = await apiFetch('/api/stores');
    if (!res.ok) throw new Error('stores');
    const data = await res.json();
    const select = document.getElementById('conversationStoreFilter');
    select.replaceChildren(new Option('كل المتاجر','all'), ...data.stores.map(s => new Option(s.name,s.store_id)));
    select.value = currentStoreFilter;
  } catch { showToast('تعذر تحميل قائمة المتاجر؛ أعد تحديث الصفحة', 'warning'); }
}
function refreshInboxFilters() {
  clearTimeout(inboxSearchTimer);
  const from = document.getElementById('filterFrom');
  const to = document.getElementById('filterTo');
  to.setCustomValidity(from.value && to.value && from.value > to.value ? 'يجب أن يكون تاريخ النهاية بعد البداية' : '');
  if (!to.reportValidity()) return;
  const count = [...inboxQuery()].filter(([k,v]) => k !== 'sort' && v !== 'all').length;
  document.getElementById('activeFilterCount').textContent = count ? `(${count})` : '';
  allConversations = []; convOffset = 0; hasMoreConv = true;
  loadConversations();
}
function scheduleInboxSearch() { clearTimeout(inboxSearchTimer); inboxSearchTimer = setTimeout(refreshInboxFilters,300); }
function filterCustomers() { scheduleInboxSearch(); }
function setConversationStoreFilter(storeId) { currentStoreFilter = storeId || 'all'; refreshInboxFilters(); }
function resetInboxFilters() {
  currentFilter = 'all'; currentStoreFilter = 'all';
  document.getElementById('conversationStoreFilter').value = 'all';
  document.getElementById('searchInput').value = '';
  for (const id of Object.values(inboxFields)) {
    const el = document.getElementById(id);
    if (el.tagName === 'SELECT') el.selectedIndex = 0; else el.value = '';
  }
  syncConversationFilterUI(); refreshInboxFilters();
}

let inboxScrollAnchor = 0;
function onCustomerListScroll() {
  const el = document.getElementById('customerList');
  if (!el) return;
  const panel = document.getElementById('inboxFilterPanel');
  const top = Math.max(0, el.scrollTop);
  const delta = top - inboxScrollAnchor;
  if (top < 12 || Math.abs(delta) > 18) {
    if (panel && !panel.contains(document.activeElement)) {
      const hidden = top > 80 && delta > 0;
      panel.classList.toggle('is-collapsed', hidden);
      panel.inert = hidden;
    }
    inboxScrollAnchor = top;
  }
  if (el.scrollHeight - el.scrollTop <= el.clientHeight + 100) {
    if (!isLoadingMoreConv && hasMoreConv) {
      loadConversations(false, true);
    }
  }
}

function renderConversations() {
  syncConversationFilterUI();
  let list = allConversations;

  const el = document.getElementById('customerList');

  document.getElementById('inboxResultCount').textContent = `${list.length} محادثة${hasMoreConv ? ' · مرّر لعرض المزيد' : ''}`;
  if (!list.length) {
    const emptyText = currentFilter === 'booked'
      ? 'لا توجد محادثات تم فيها تثبيت طلب'
      : currentFilter === 'problems'
        ? 'لا توجد محادثات تحتاج تدخلاً بشرياً'
      : currentFilter === 'unanswered'
        ? 'لا توجد رسائل بدون رد'
        : 'لا توجد محادثات';
    el.innerHTML = `<div class="text-center py-4 small" style="color:var(--text-muted)">${emptyText}</div>`;
    return;
  }

  const markup = list.map(c => {
    const init    = (c.name || c.sender_id || '؟').slice(0, 2).toUpperCase();
    const time    = fmtTime(c.last_time);
    const active  = c.sender_id === currentSenderId ? 'active' : '';
    const badge   = c.pending_reviews_count > 0
      ? `<span class="badge bg-danger" title="${esc(c.human_review_reason || 'تحتاج تدخلاً بشرياً')}" style="font-size:9px;"><i class="bi bi-person-exclamation"></i> ${c.pending_reviews_count}</span>` : '';
    const problemBadge = c.problem_count > 0
      ? `<span class="badge bg-warning text-dark" title="${esc(c.problem_reason || 'مشكلة مفتوحة')}" style="font-size:9px;"><i class="bi bi-exclamation-triangle-fill"></i> ${c.problem_count}</span>` : '';
    const adBadge = (c.ad_id || c.ref)
      ? `<span class="ad-badge"><i class="bi bi-megaphone-fill"></i></span>` : '';
    const sourceMeta = customerSourceMeta(c);
    const leadStage = c.lead_stage || 'new';
    const leadMeta = leadStage === 'booked'
      ? '<span class="badge bg-success" style="font-size:9px">تم الحجز</span>'
      : leadStage === 'hot'
        ? `<span class="badge bg-danger" style="font-size:9px">جاد ${Number(c.lead_score || 0)}%</span>`
        : leadStage === 'warm'
          ? `<span class="badge bg-warning text-dark" style="font-size:9px">مهتم ${Number(c.lead_score || 0)}%</span>`
          : '';
    const preview = esc(c.last_message || '...');
    const storeId = c.store_id || 'default';
    const knownStoreNames = {default: 'لمسة ستور', khuyoot: 'خيوط', 'golden-threads': 'خيوط الذهب جملة', 'al-fatena': 'الفاتنة'};
    const storeName = c.store_name || knownStoreNames[storeId] || storeId;
    const storeBadgeClass = storeId === 'khuyoot' ? 'store-khuyoot' : (storeId === 'golden-threads' ? 'store-golden-threads' : 'store-lamsa');
    return `
      <div class="customer-item ${active}" role="button" tabindex="0" data-sender="${esc(c.sender_id)}" onclick="selectConversation(this.dataset.sender)" onkeydown="if(event.key==='Enter'){selectConversation(this.dataset.sender)}">
        <div class="cust-avatar">${esc(init)}${platformBadge(c.platform)}</div>
        <div class="cust-info">
          <div class="cust-name">${esc(c.name || c.sender_id)} ${adBadge}</div>
          <div><span class="conversation-store-badge ${storeBadgeClass}"><i class="bi bi-shop"></i> ${esc(storeName)}</span></div>
          ${sourceMeta ? `<div class="cust-preview">${sourceMeta}</div>` : ''}
          <div class="cust-preview">${preview}</div>
        </div>
        <div class="d-flex flex-column align-items-end gap-1">
          <span class="cust-time">${time}</span>${leadMeta}${badge}${problemBadge}${c.system_issue_count > 0 ? '<span class="badge bg-danger" title="خطأ تقني أو تكرار خطوات الحجز؛ يحتاج مراجعة">مشكلة نظام</span>' : ''}
        </div>
      </div>`;
  }).join('');
  if (el.innerHTML !== markup) { const top = el.scrollTop; el.innerHTML = markup; el.scrollTop = top; }
}

// ══ Select Conversation ════════════════════════════════════════════════════
async function selectConversation(senderId) {
  currentSenderId = senderId;
  closeAllPanels();

  if (window.innerWidth <= 768) {
    document.body.classList.add('mobile-conversation-open');
    if (history.state?.dashboardConversation !== senderId) {
      const url = new URL(location.href);
      url.searchParams.set('sender_id', senderId);
      history.pushState({
        ...(history.state || {}),
        dashboardConversation: senderId,
        dashboardConversationPushed: true,
      }, '', url);
    }
  }

  document.getElementById('chatPlaceholder').style.display  = 'none';
  document.getElementById('chatContent').style.display      = 'flex';
  document.getElementById('controlPlaceholder').style.display = 'none';
  document.getElementById('controlContent').style.display   = 'block';

  const conv = allConversations.find(c => c.sender_id === senderId) || {};
  currentCustomer = conv;
  currentConversationAIEnabled = conv.ai_enabled !== 0 && conv.ai_enabled !== false;
  const messagesReady = loadMessages(senderId);
  const productsReady = loadProducts(conv.store_id || 'default');

  const init = (conv.name || senderId || '؟').slice(0, 2).toUpperCase();
  document.getElementById('chatAvatar').textContent   = init;
  document.getElementById('chatStoreName').textContent = conv.store_name || 'المتجر غير محدد';
  document.getElementById('chatName').textContent     = conv.name || senderId;
  document.getElementById('chatSenderId').textContent = senderId;
  document.getElementById('chatAvatar').insertAdjacentHTML('beforeend', platformBadge(conv.platform));
  renderConversationAIToggle();
  renderAskAIButton();

  // Ad badge
  const adBadge = document.getElementById('chatAdBadge');
  if (conv.ad_id || conv.ref) {
    adBadge.style.display = '';
    document.getElementById('chatAdText').textContent = [
      conv.ad_id ? `إعلان: ${conv.ad_id}` : '',
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
        `<div class="d-flex justify-content-between align-items-center mb-1">` +
        `<div><div class="fw-semibold">${esc(name)}</div><small style="color:var(--text-muted)">${esc(linkedIds[idx] || '')}</small></div>` +
        `<button class="btn btn-sm btn-outline-danger p-1" onclick="unlinkProduct('${esc(linkedIds[idx])}')" title="إلغاء الربط"><i class="bi bi-x"></i></button>` +
        `</div>`
      ).join('<hr class="my-1" style="border-color:var(--border)">') +
      `<small style="color:var(--text-muted) d-block mt-1">` +
      (conv.ad_id ? ` | إعلان: ${esc(conv.ad_id)}` : '') +
      (conv.ref ? ` | Ref: ${esc(conv.ref)}` : '') + '</small>';
  } else {
    document.getElementById('linkedProductInfo').innerHTML =
      '<span style="color:var(--text-muted)" class="small">لا يوجد منتج مرتبط</span>';
  }

  renderConversations();
  await Promise.all([messagesReady, productsReady]);
  if (currentSenderId !== senderId) return;
  await loadCustomerInstructions(senderId);
  if ((conv.pending_reviews_count || 0) > 0) {
    showHumanIntervention();
  } else {
    hideHumanIntervention();
  }

  if (pollMsgTimer) clearInterval(pollMsgTimer);
  pollMsgTimer = setInterval(() => {
    if (currentSenderId && !document.hidden) loadMessages(currentSenderId, false);
  }, 1500);
}

// ══ Messages ═══════════════════════════════════════════════════════════════
async function loadMessages(senderId, scroll = true, older = false) {
  if (!senderId) return;
  if (messageRequest?.sender === senderId && !older) return;
  messageRequest?.controller.abort();
  const controller = new AbortController();
  const pending = {sender: senderId, controller};
  messageRequest = pending;
  const cached = messageCache.get(senderId) || {messages: [], hasOlder: false};
  if (scroll && senderId === currentSenderId) renderMessages(cached.messages, true, senderId);
  const status = document.getElementById('chatUpdateStatus');
  if (status && !cached.messages.length) status.textContent = 'جاري تحميل الرسائل…';
  const params = new URLSearchParams();
  if (cached.messages.length) params.set(older ? 'before_id' : 'after_id', older ? cached.messages[0].id : cached.messages[cached.messages.length - 1].id);
  try {
    const res = await apiFetch(`/api/conversations/${encodeURIComponent(senderId)}/messages?${params}`, {signal: controller.signal});
    if (!res.ok) throw new Error('messages');
    const data = await res.json();
    const merged = new Map(cached.messages.map(m => [m.id, m]));
    (data.messages || []).forEach(m => merged.set(m.id, m));
    const messages = [...merged.values()].sort((a,b) => a.id-b.id);
    const hasOlder = older || !cached.messages.length ? Boolean(data.has_more) : cached.hasOlder;
    messageCache.set(senderId, {messages,hasOlder});
    if (messageCache.size > 10) {
      for (const key of messageCache.keys()) { if (key !== currentSenderId && key !== senderId) {messageCache.delete(key); break;} }
    }
    if (senderId === currentSenderId) {
      renderMessages(messages, scroll && !older, senderId);
      const more = document.getElementById('loadOlderMessages');
      if (more) more.hidden = !hasOlder;
      if (status) status.textContent = 'محدّثة الآن';
    }
  } catch (e) {
    if (e.name !== 'AbortError' && senderId === currentSenderId && status) status.textContent = 'تعذر التحديث · ستتم إعادة المحاولة';
  } finally {
    if (messageRequest === pending) messageRequest = null;
  }
}

function safeMediaUrl(value) {
  try {
    const url = new URL(String(value || ''), location.origin);
    return ['http:','https:'].includes(url.protocol) ? url.href : '';
  } catch { return ''; }
}

function mediaLoaded(element) {
  const holder = element.closest('.message-media');
  holder?.classList.remove('is-loading','has-error');
  const area = document.getElementById('messagesArea');
  if (area?.dataset.followLatest === '1') requestAnimationFrame(() => { area.scrollTop = area.scrollHeight; });
}
function mediaFailed(element) {
  const holder = element.closest('.message-media');
  holder?.classList.remove('is-loading');
  holder?.classList.add('has-error');
}
function retryMedia(button) {
  const holder = button.closest('.message-media');
  const media = holder.querySelector('img,audio,video');
  holder.classList.remove('has-error'); holder.classList.add('is-loading');
  if (media.tagName === 'IMG') media.src = media.dataset.src;
  else media.load();
}

function messageMarkup(m) {
  const media = Array.isArray(m.media) ? m.media : (messageImageUrl(m) ? [{type:'image',url:messageImageUrl(m)}] : []);
  let content = '';
  for (const item of media) {
    const url = safeMediaUrl(item.url);
    if (!url) continue;
    const escaped = esc(url);
    const kind = ['image','audio','video'].includes(item.type) ? item.type : 'file';
    const failure = `<div class="media-error">تعذر فتح المرفق؛ قد يكون الرابط منتهي الصلاحية.<button type="button" onclick="retryMedia(this)">إعادة المحاولة</button><a href="${escaped}" target="_blank" rel="noopener noreferrer">فتح الرابط</a></div>`;
    if (kind === 'image') {
      content += `<div class="message-media image-media is-loading"><span class="media-loading" role="status"><span class="spinner-border spinner-border-sm"></span> تحميل الصورة…</span><img src="${escaped}" data-src="${escaped}" class="msg-image" alt="صورة في المحادثة" loading="lazy" decoding="async" referrerpolicy="no-referrer" onload="mediaLoaded(this)" onerror="mediaFailed(this)" onclick="openLightbox(this.dataset.src)">${failure}</div>`;
    } else if (kind === 'audio' || kind === 'video') {
      const label = kind === 'audio' ? 'تسجيل صوتي' : 'مقطع فيديو';
      content += `<div class="message-media ${kind}-media"><div class="media-label">${label}</div><${kind} controls preload="metadata" src="${escaped}" aria-label="${label}" onloadedmetadata="mediaLoaded(this)" onerror="mediaFailed(this)" onwaiting="this.closest('.message-media').classList.add('is-loading')" oncanplay="mediaLoaded(this)"></${kind}><span class="media-loading" role="status">جاري التحميل…</span>${failure}</div>`;
    } else {
      content += `<a class="message-file" href="${escaped}" target="_blank" rel="noopener noreferrer"><i class="bi bi-paperclip"></i> فتح المرفق</a>`;
    }
  }
  if (m.text && !media.some(item => item.url === m.text.trim())) content += `<div class="msg-bubble">${esc(m.text)}</div>`;
  if (!content) content = '<div class="msg-bubble">[رسالة بلا نص أو مرفق متاح]</div>';
  return content + `<span class="msg-time">${esc(fmtDatetime(m.created_at))}</span>`;
}

const resolvingMedia = new Set();
async function resolveMessageMedia(message, sender, node) {
  const key = `${sender}:${message.id}`;
  if (resolvingMedia.has(key) || !message.media?.some(item => item.type === 'file')) return;
  resolvingMedia.add(key);
  try {
    const response = await apiFetch(`/api/conversations/${encodeURIComponent(sender)}/messages/${message.id}/media`, {method:'POST'});
    if (!response.ok) throw new Error('media lookup failed');
    const data = await response.json();
    message.media = data.media;
    if (sender === currentSenderId && node.isConnected) {
      node.innerHTML = messageMarkup(message);
      node.dataset.signature = JSON.stringify(message);
    }
  } catch (_) { resolvingMedia.delete(key); }
}

function renderMessages(messages, scroll = true, sender = currentSenderId) {
  const area = document.getElementById('messagesArea');
  const changedSender = renderedMessageSender !== sender;
  if (changedSender) { area.replaceChildren(); renderedMessageSender = sender; }
  const atBottom = area.scrollHeight - area.scrollTop - area.clientHeight < 100;
  const oldHeight = area.scrollHeight, oldTop = area.scrollTop;
  const existing = new Map([...area.querySelectorAll('[data-message-id]')].map(el => [Number(el.dataset.messageId),el]));
  area.querySelector('.messages-empty')?.remove();
  let added = false, prepended = false;
  for (const m of messages) {
    const signature = JSON.stringify(m);
    let node = existing.get(m.id);
    if (!node) {
      node = document.createElement('div');
      node.className = `msg-wrapper ${m.direction === 'incoming' ? 'incoming' : 'outgoing'}`;
      node.dataset.messageId = m.id;
      const next = [...area.children].find(el => Number(el.dataset.messageId) > m.id);
      area.insertBefore(node, next || null);
      added = true; prepended ||= Boolean(next);
    }
    if (node.dataset.signature !== signature) {
      node.innerHTML = messageMarkup(m);
      node.dataset.signature = signature;
      node.querySelectorAll('img').forEach(img => { if (img.complete && img.naturalWidth) mediaLoaded(img); });
      resolveMessageMedia(m, sender, node);
    }
  }
  if (!messages.length) area.innerHTML = '<div class="messages-empty text-center small py-4">لا توجد رسائل محمّلة بعد</div>';
  if (scroll || changedSender || (atBottom && added && !prepended)) {
    area.dataset.followLatest = '1';
    area.scrollTop = area.scrollHeight;
    document.getElementById('newMessagesButton').hidden = true;
  } else if (prepended) { area.dataset.followLatest = '0'; area.scrollTop = oldTop + area.scrollHeight - oldHeight; }
  else if (added) document.getElementById('newMessagesButton').hidden = false;
}

function scrollToLatestMessage() {
  const area = document.getElementById('messagesArea'); area.dataset.followLatest = '1'; area.scrollTop = area.scrollHeight;
  document.getElementById('newMessagesButton').hidden = true;
}

// ══ Human Intervention (single unified dialog) ════════════════════════════
let _hiBusy = false;
let _hiModalInstance = null;

function _hiSetBusy(busy, btnId = null) {
  _hiBusy = busy;
  ['hiBtnLinkProduct', 'hiBtnUnavailable', 'hiBtnCloseImageReview', 'hiBtnAskAI', 'hiBtnSendDirect', 'hiBtnCloseReview']
    .forEach(id => {
      const b = document.getElementById(id);
      if (b) b.disabled = busy;
    });
  if (btnId && busy) {
    const b = document.getElementById(btnId);
    if (b) b.innerHTML = '<span class="spinner-border spinner-border-sm" role="status" style="width:14px;height:14px;"></span> جاري…';
  }
}

function _hiResetButtons() {
  const map = {
    hiBtnLinkProduct: '<i class="bi bi-link me-1"></i>ربط المنتج وإرسال رد',
    hiBtnUnavailable: '<i class="bi bi-x-circle me-1"></i>غير متوفر — أبلغ الزبون',
    hiBtnCloseImageReview: '<i class="bi bi-check2-circle me-1"></i>إغلاق المراجع البشري',
    hiBtnAskAI: '<i class="bi bi-robot me-1"></i>أعد صياغة بالـ AI واعرض الاقتراح',
    hiBtnSendDirect: '<i class="bi bi-send me-1"></i>أرسل النص مباشرة',
    hiBtnCloseReview: '<i class="bi bi-check2-circle me-1"></i>أغلق المراجعة فقط',
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
  // If image is the latest incoming and no product linked yet → image mode
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
    titleEl.textContent = 'الزبون أرسل صورة — يحتاج ربط منتج';
    subEl.textContent = 'AI متوقف لهذه المحادثة. اضغط لاختيار المنتج.';
  } else {
    titleEl.textContent = 'تدخل بشري مطلوب';
    subEl.textContent = 'اضغط لفتح خيارات المعالجة';
  }
}

function showHumanIntervention() {
  if (!currentSenderId) return;
  const mode = _hiCurrentMode();
  _hiShowBanner(true, mode);
  // الصوت يُشغّل فقط من loadConversations/loadMessages عند المحادثات الأخرى.
  // داخل المحادثة المفتوحة لا نُصدر صوتاً، فقط شارة بصرية.
}

function hideHumanIntervention() {
  _hiShowBanner(false);
}

function _hiPopulateProducts() {
  const sel = document.getElementById('hiUnifiedProduct');
  if (!sel) return;
  const conv = allConversations.find(c => c.sender_id === currentSenderId) || {};
  const linked = splitPipeList(conv.product_ids || conv.product_id);
  sel.innerHTML = '<option value="">— اختر منتجاً —</option>' +
    allProducts.map(p =>
      `<option value="${esc(p.product_id)}" ${linked.includes(p.product_id) ? 'selected' : ''}>` +
      `${esc(p.product_name)} — ${esc(p.price || '')} (${esc(p.stock || '')})</option>`
    ).join('');
}

function _hiPopulateTextProducts() {
  const sel = document.getElementById('hiTextProduct');
  if (!sel) return;
  const conv = allConversations.find(c => c.sender_id === currentSenderId) || {};
  const linked = splitPipeList(conv.product_ids || conv.product_id);
  const placeholder = linked.length
    ? `— مربوط حالياً: ${linked.join(', ')} —`
    : '— اختر منتجاً (اختياري) —';
  sel.innerHTML = `<option value="">${esc(placeholder)}</option>` +
    allProducts.map(p =>
      `<option value="${esc(p.product_id)}">` +
      `${esc(p.product_name)} — ${esc(p.price || '')} (${esc(p.stock || '')})</option>`
    ).join('');
  const hint = document.getElementById('hiTextProductHint');
  if (hint) hint.style.display = linked.length ? 'none' : 'block';
}

function openInterventionDialog() {
  if (!currentSenderId) { showToast('اختر محادثة أولاً', 'warning'); return; }
  const mode = _hiCurrentMode();
  document.getElementById('hiModeImage').style.display = mode === 'image' ? 'block' : 'none';
  document.getElementById('hiModeText').style.display  = mode === 'text'  ? 'block' : 'none';
  document.getElementById('hiModalTitle').textContent = mode === 'image'
    ? 'صورة من الزبون — ربط المنتج' : 'تدخل بشري';

  const alertBox = document.getElementById('hiAlertBox');
  alertBox.style.display = 'none';
  alertBox.textContent = '';

  _hiResetButtons();
  if (mode === 'image') {
    _hiPopulateProducts();
  } else {
    _hiPopulateTextProducts();
    document.getElementById('hiUnifiedText').value = document.getElementById('aiInstructions')?.value || '';
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
  if (!pid) { showToast('اختر منتجاً أولاً', 'warning'); return; }
  _hiSetBusy(true, 'hiBtnLinkProduct');
  try {
    const res = await apiFetch(`/api/conversations/${currentSenderId}/link_product`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ product_id: pid }),
    });
    const data = await res.json();
    if (!data.ok) {
      showToast('فشل ربط المنتج: ' + (data.error || ''), 'danger');
      return;
    }
    const sent = data.auto_reply && data.auto_reply.sent;
    showToast(sent ? 'تم ربط المنتج وإرسال رد' : 'تم ربط المنتج — لم يُرسل رد تلقائي', sent ? 'success' : 'warning');
    _hiCloseModal();
    hideHumanIntervention();
    await loadConversations(false);
    await loadMessages(currentSenderId);
    await refreshLinkedProductSync();
  } catch (e) {
    showToast('خطأ: ' + e.message, 'danger');
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
    await sendMessage('حبيبتي للأسف هذا الموديل غير متوفر حالياً 🌸 إذا تحبين أقترحلج موديل مشابه؟');
    showToast('تم إخبار الزبون بعدم التوفر', 'info');
    _hiCloseModal();
    hideHumanIntervention();
    await loadConversations(false);
    await loadMessages(currentSenderId);
  } catch (e) {
    showToast('خطأ: ' + e.message, 'danger');
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
  // فقط احفظ إذا اختلف عن المخزن في الذاكرة
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
    await askAI({
      text,
      allowEmpty: true,
      extraInstructions: text
        ? 'مدخلات المشرف التالية هي مسودة أو توجيه للرد. أعد صياغتها باللهجة المناسبة للزبون، والتزم بسياق المحادثة ولا تضف معلومة غير مؤكدة.'
        : 'لم يكتب المشرف أي نص. حلل آخر رسائل المحادثة وسياق المنتج المرتبط إن وجد، ثم اكتب الرد الصحيح والمختصر للزبون.',
    });
    _hiCloseModal();
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
  if (!text) { showToast('اكتب نصاً أولاً', 'warning'); return; }
  _hiSetBusy(true, 'hiBtnSendDirect');
  try {
    await _hiSyncGenderIfChosen();
    await _hiSilentLinkIfChosen();
    document.getElementById('messageInput').value = text;
    const sent = await sendMessage();
    if (sent) {
      await apiFetch(`/api/conversations/${currentSenderId}/mark_reviewed`, { method: 'POST' });
      _hiCloseModal();
      hideHumanIntervention();
      await loadConversations(false);
    }
  } catch (e) {
    showToast('خطأ: ' + e.message, 'danger');
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
    showToast('تم إغلاق المراجعة', 'success');
    _hiCloseModal();
    hideHumanIntervention();
    await loadConversations(false);
  } catch (e) {
    showToast('خطأ: ' + e.message, 'danger');
  } finally {
    _hiSetBusy(false);
    _hiResetButtons();
  }
}

async function refreshLinkedProductSync() {
  if (!currentSenderId) return;
  const conv = allConversations.find(c => c.sender_id === currentSenderId);
  if (!conv) return;
  const linkedNames = splitPipeList(conv.product_names || conv.product_name);
  const linkedIds = splitPipeList(conv.product_ids || conv.product_id);
  const linkedBox = document.getElementById('linkedProductInfo');
  if (linkedBox) {
    linkedBox.innerHTML = linkedNames.length
      ? linkedNames.map((name, idx) =>
          `<div class="d-flex justify-content-between align-items-center mb-1">` +
          `<div><div class="fw-semibold">${esc(name)}</div><small style="color:var(--text-muted)">${esc(linkedIds[idx] || '')}</small></div>` +
          `<button class="btn btn-sm btn-outline-danger p-1" onclick="unlinkProduct('${esc(linkedIds[idx])}')" title="إلغاء الربط"><i class="bi bi-x"></i></button>` +
          `</div>`
        ).join('<hr class="my-1" style="border-color:var(--border)">')
      : '<span style="color:var(--text-muted)" class="small">لا يوجد منتج مرتبط</span>';
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
  if (!currentSenderId) { showToast('اختر محادثة أولاً', 'warning'); return; }
  openInterventionDialog();
}

// ══ Lightbox ═══════════════════════════════════════════════════════════════
function openLightbox(url) {
  const overlay = document.createElement('div');
  overlay.className = 'lightbox-overlay';
  overlay.onclick = () => overlay.remove();
  overlay.innerHTML = `<img src="${esc(url)}" referrerpolicy="no-referrer">`;
  document.body.appendChild(overlay);
}

// ══ Send Message ═══════════════════════════════════════════════════════════
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
      : '<i class="bi bi-send-fill"></i><span>إرسال</span>';
  }
  if (aiBtn)    aiBtn.disabled = busy || !canUseAI();
  if (sendAIBtn) sendAIBtn.disabled = busy;
  renderAIActionButtons();
}

async function sendMessage(text = null, imgUrl = null) {
  if (!currentSenderId) return;
  if (_isSending) { showToast('جاري الإرسال... انتظر', 'warning'); return; }

  const txt = text  !== null ? text   : (document.getElementById('messageInput').value || '').trim();
  const img = imgUrl !== null ? imgUrl : uploadedImageUrl;

  if (!txt && !img) { showToast('اكتب رسالة أو ارفع صورة', 'warning'); return; }

  const sendingTo = currentSenderId;
  _setSendingState(true);
  try {
    const res  = await apiFetch(`/api/conversations/${encodeURIComponent(sendingTo)}/send`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: txt, image_url: img }),
    });
    const data = await res.json();
    if (data.ok) {
      if (currentSenderId === sendingTo && document.getElementById('messageInput').value.trim() === txt) { document.getElementById('messageInput').value = ''; clearImage(); }
      if (data.warning) showToast(data.warning, 'warning');
      else showToast('تم الإرسال', 'success');
      await loadMessages(sendingTo, currentSenderId === sendingTo);
      return true;
    } else {
      const detail = data.warning || data.error || 'ManyChat لم يؤكد الإرسال';
      showToast('فشل الإرسال: ' + detail, 'danger');
      return false;
    }
  } catch (e) { showToast('خطأ: ' + e.message, 'danger'); return false; }
  finally { _setSendingState(false); }
}

async function testManyChat() {
  try {
    const res = await apiFetch('/api/manychat/test');
    const data = await res.json();
    if (data.ok) {
      showToast(`ManyChat OK — ${data.page_name || data.page_id || 'page connected'}`, 'success');
    } else {
      showToast('ManyChat غير صالح: ' + (data.message || data.reason || data.status || 'unknown'), 'danger');
    }
  } catch (e) { showToast('فشل اختبار ManyChat: ' + e.message, 'danger'); }
}

// ══ Ask AI ═════════════════════════════════════════════════════════════════
// Catalog image upload moved to /settings/catalog page

async function askAI(options = {}) {
  if (!currentSenderId) return;
  if (_isAskingAI) { showToast('جاري تجهيز اقتراح AI...', 'warning'); return; }
  if (!canUseAI()) {
    const reason = !globalAIEnabled
      ? 'AI متوقف حالياً من الزر الرئيسي'
      : 'AI متوقف في هذه المحادثة. شغّله من زر المحادثة أولاً';
    showToast(reason, 'warning');
    renderAskAIButton();
    renderAIActionButtons();
    return;
  }
  const autonomous = options.text === undefined;
  const text = autonomous ? '' : String(options.text || '').trim();
  const allowEmpty = autonomous || Boolean(options.allowEmpty);
  if (!text && !allowEmpty) {
    showToast('اكتب نصاً في حقل الرسالة حتى يعيد AI صياغته', 'warning');
    return;
  }
  const savedInstructions = options.extraInstructions !== undefined
    ? String(options.extraInstructions || '').trim()
    : document.getElementById('aiInstructions').value.trim();
  const rewriteInstruction = text
    ? 'قم بإعادة صياغة النص الموجود في رسالة المشرف فقط، مع الالتزام بباقي التعليمات والقواعد المحفوظة. لا تضف سؤالاً جديداً إذا كان الزبون أجاب عليه سابقاً.'
    : 'لا توجد رسالة من المشرف. حلل آخر رسائل الزبون وسياق المحادثة والمنتج المرتبط إن وجد، ثم حضر الرد الأنسب بدون اختراع تفاصيل.';
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
      body: JSON.stringify({ text, extra_instructions: extraInstructions, product_id: productId, allow_empty: allowEmpty }),
    });
    const data = await res.json();
    if (data.reply) {
      if (autonomous) {
        const composer = document.getElementById('messageInput');
        composer.value = data.reply;
        composer.focus();
        composer.setSelectionRange(composer.value.length, composer.value.length);
        showToast('تم تحليل المحادثة وصياغة رد جاهز للمراجعة', 'success');
      } else {
        aiPendingReply = data.reply;
        document.getElementById('aiReplyText').textContent = data.reply;
        document.getElementById('aiReplyPreview').style.display = 'block';
      }
    } else {
      showToast(data.error || 'AI لم يستطع الرد — يمكنك التدخل يدوياً', 'warning');
      showHumanIntervention();
    }
  } catch (e) { showToast('خطأ AI: ' + e.message, 'danger'); }
  finally {
    _isAskingAI = false;
    renderAskAIButton();
  }
}

function resetConversationView() {
  currentSenderId = null;
  currentCustomer = null;
  document.body.classList.remove('mobile-conversation-open');
  if (pollMsgTimer) {
    clearInterval(pollMsgTimer);
    pollMsgTimer = null;
  }
  document.getElementById('chatContent').style.display = 'none';
  document.getElementById('chatPlaceholder').style.display = 'flex';
  document.getElementById('controlContent').style.display = 'none';
  document.getElementById('controlPlaceholder').style.display = 'block';
  closeAllPanels();
  renderConversations();
}

function closeMobileConversation(fromPopState = false) {
  if (window.innerWidth > 768) return;
  if (!fromPopState && history.state?.dashboardConversationPushed) {
    history.back();
    return;
  }
  if (!fromPopState) {
    const url = new URL(location.href);
    url.searchParams.delete('sender_id');
    const nextState = {...(history.state || {})};
    delete nextState.dashboardConversation;
    delete nextState.dashboardConversationPushed;
    history.replaceState(nextState, '', url);
  }
  resetConversationView();
}

function applyReplyShortcut(type) {
  const templates = {
    price: 'السعر موضح حسب الموديل المرتبط. تحبين أثبت لج الطلب؟',
    delivery: 'التوصيل متوفر لكل محافظات العراق بـ5 آلاف، والفحص عند الاستلام 🌸',
    details: 'حتى أثبت الطلب أرسلي رقم الهاتف والمحافظة والعنوان الكامل، ويا اللون والقياس المطلوب.',
    confirm: 'تمام، أتأكد من القطع واللون والقياس وأثبت الطلب إلج الآن 🌸',
    unavailable: 'هذا الموديل غير متوفر حالياً، تحبين أقترح لج أقرب موديل متوفر؟'
  };
  const input = document.getElementById('messageInput');
  input.value = templates[type] || '';
  input.focus();
  input.setSelectionRange(input.value.length, input.value.length);
}

async function sendAIReply() {
  if (!aiPendingReply) return;
  const reply = aiPendingReply;
  const sent = await sendMessage(reply);
  if (!sent) return;
  try {
    const res = await apiFetch(`/api/conversations/${currentSenderId}/mark_reviewed`, { method: 'POST' });
    const data = await res.json();
    if (data.ok) {
      currentConversationAIEnabled = data.ai_enabled !== 0 && data.ai_enabled !== false;
      if (currentCustomer) currentCustomer.ai_enabled = currentConversationAIEnabled ? 1 : 0;
      renderConversationAIToggle();
      hideHumanIntervention();
      await loadConversations(false);
    }
  } catch (e) {
    showToast('تم إرسال الرد، لكن تعذر إغلاق المراجعة: ' + e.message, 'warning');
  }
  dismissAIPreview();
}
function editAIReply()    { if (aiPendingReply) { document.getElementById('messageInput').value = aiPendingReply; dismissAIPreview(); } }
function dismissAIPreview() {
  aiPendingReply = null;
  document.getElementById('aiReplyPreview').style.display = 'none';
}

// ══ Improve Message (independent of AI on/off) ════════════════════════════
let _isImproving = false;
async function improveMessage() {
  if (_isImproving) return;
  const ta = document.getElementById('messageInput');
  const text = (ta.value || '').trim();
  if (!text) {
    showToast('اكتب نصاً قبل التحسين', 'warning');
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
      showToast('تم تحسين النص', 'success');
    } else if (data.improved) {
      ta.value = data.improved;
      showToast(data.error ? `تعذر التحسين بالكامل: ${data.error}` : 'لم يتم التحسين', 'warning');
    } else {
      showToast(data.error || 'فشل تحسين النص', 'danger');
    }
  } catch (e) {
    showToast('خطأ في التحسين: ' + e.message, 'danger');
  } finally {
    _isImproving = false;
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-magic"></i>';
    }
  }
}

// ══ Image Upload ═══════════════════════════════════════════════════════════
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
    if (data.image_url) { uploadedImageUrl = data.image_url; showToast('تم رفع الصورة', 'success'); }
    else showToast('فشل رفع الصورة', 'danger');
  } catch (e) { showToast('خطأ رفع: ' + e.message, 'danger'); }
}

function clearImage() {
  uploadedImageUrl = null;
  document.getElementById('imagePreview').style.display = 'none';
  document.getElementById('imagePreview').style.cssText = 'display:none!important;';
  document.getElementById('previewImg').src             = '';
  document.getElementById('imageUpload').value          = '';
}

// ══ Customer Info ══════════════════════════════════════════════════════════
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
      showToast('تم حفظ بيانات الزبون', 'success');
      loadConversations(false);
    } else {
      showToast('فشل الحفظ', 'danger');
    }
  } catch (e) { showToast('خطأ: ' + e.message, 'danger'); }
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
    return false;
  }
}

// ══ Products ═══════════════════════════════════════════════════════════════
async function loadProducts(storeId = 'default') {
  const version = ++productRequestVersion;
  try {
    const res  = await apiFetch(`/api/products?store_id=${encodeURIComponent(storeId || 'default')}`);
    const data = await res.json();
    if (version !== productRequestVersion) return;
    allProducts = data.products || [];
    const opts = allProducts.map(p =>
      `<option value="${esc(p.product_id)}">${esc(p.product_name)} — ${esc(p.price)} (${esc(p.stock)})</option>`
    ).join('');
    document.getElementById('productSelect').innerHTML  = '<option value="">— اختر منتجاً —</option>' + opts;
    document.getElementById('orderProduct').innerHTML   = '<option value="">— اختر —</option>' +
      allProducts.map(p => `<option value="${esc(p.product_id)}">${esc(p.product_name)}</option>`).join('');
  } catch (e) {}
}

let _isLinking = false;
async function linkProduct() {
  if (_isLinking || !currentSenderId) return;
  const productIds = selectedValues(document.getElementById('productSelect'));
  if (!productIds.length) { showToast('اختر منتجاً واحداً على الأقل', 'warning'); return; }
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
      showToast('تم ربط المنتج بدون إرسال أي تفاصيل للزبون', 'success');
      await loadMessages(currentSenderId);
      const conv = allConversations.find(c => c.sender_id === currentSenderId);
      if (conv) {
         let pIds = conv.product_ids ? conv.product_ids.split('||') : [];
         let pNames = conv.product_names ? conv.product_names.split('||') : [];
         for (const pid of productIds) {
             if (!pIds.includes(pid)) {
                 const p = allProducts.find(x => x.product_id === pid);
                 if (p) {
                     pIds.push(pid);
                     pNames.push(p.product_name);
                 }
             }
         }
         conv.product_ids = pIds.join('||');
         conv.product_names = pNames.join('||');
      }
      await refreshLinkedProductSync();
    } else showToast('فشل ربط المنتج', 'danger');
  } catch (e) { showToast('خطأ: ' + e.message, 'danger'); }
  finally {
    _isLinking = false;
    if (linkBtn) { linkBtn.disabled = false; linkBtn.innerHTML = original; }
  }
}

let _isSendingProductDetails = false;
async function sendProductDetails() {
  if (!currentSenderId || _isSendingProductDetails) return;
  if (_isSending) { showToast('انتظر اكتمال الإرسال الحالي', 'warning'); return; }
  const senderId = currentSenderId;
  const productIds = selectedValues(document.getElementById('productSelect'));
  if (!productIds.length) { showToast('اختر منتجاً واحداً على الأقل', 'warning'); return; }
  const images = [...new Set(productIds.flatMap(pid => {
    const product = allProducts.find(p => p.product_id === pid);
    return product ? productImageList(product) : [];
  }))];
  if (!images.length) { showToast('لا توجد صور للمنتجات المختارة', 'warning'); return; }
  _isSendingProductDetails = true;
  try {
    for (const url of images) {
      if (currentSenderId !== senderId) {
        showToast('توقف إرسال الصور لأن المحادثة تغيرت', 'warning');
        break;
      }
      // An explicit empty string prevents attaching the composer draft.
      if (!await sendMessage('', url)) break;
    }
  } finally { _isSendingProductDetails = false; }
}

// ══ AI Instructions ════════════════════════════════════════════════════════
async function loadCustomerInstructions(senderId) {
  try {
    const res  = await apiFetch(`/api/conversations/${senderId}/instructions`);
    const data = await res.json();
    document.getElementById('aiInstructions').value  = data.instructions  || '';
    document.getElementById('applyToAll').checked    = data.apply_to_all || false;
  } catch (e) {}
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
    if (data.ok) showToast('تم حفظ التعليمات', 'success');
    else showToast('فشل الحفظ', 'danger');
  } catch (e) { showToast('خطأ: ' + e.message, 'danger'); }
}

// ══ Stats ══════════════════════════════════════════════════════════════════
async function loadStats() {
  try {
    const period = document.getElementById('dashboardStatsPeriod')?.value || 'all';
    const res  = await apiFetch(`/api/dashboard_stats?period=${encodeURIComponent(period)}`);
    const data = await res.json();
    document.getElementById('statPending').textContent  = `${data.pending_reviews  || 0} مراجعة`;
    document.getElementById('statOrders').textContent   = `${data.orders_today    || 0} طلب`;
    document.getElementById('statMessages').textContent = `${data.messages_today  || 0} رسالة`;
    setText('statTotalMessages', fmtNumber(data.people_total || data.total_conversations || 0));
    setText('statTotalOrders', fmtNumber(data.orders_total || 0));
    setText('statConversion', `${Number(data.message_to_order_conversion || 0).toFixed(1)}%`);
    const top = (data.top_products || [])[0];
    setText('statTopProduct', top ? `${top.product_name || top.product_id} (${top.orders})` : 'لا يوجد');
    renderTopProducts(data.top_products || []);
    globalAIEnabled = data.ai_enabled !== false;
    renderAIToggle();
  } catch (e) {}
}

async function openStatsModal() {
  await loadStats();
  new bootstrap.Modal(document.getElementById('statsModal')).show();
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

function fmtNumber(value) {
  return Number(value || 0).toLocaleString('ar-IQ');
}

function renderTopProducts(products) {
  const el = document.getElementById('topProductsList');
  if (!el) return;
  if (!products.length) {
    el.innerHTML = '<div class="small" style="color:var(--text-muted)">لا توجد طلبات بعد</div>';
    return;
  }
  el.innerHTML = products.slice(0, 5).map((p, idx) => `
    <div class="top-product-row">
      <span>${idx + 1}. ${esc(p.product_name || p.product_id || '-')}</span>
      <strong>${fmtNumber(p.orders || 0)}</strong>
    </div>
  `).join('');
}

function renderAIToggle() {
  const btn = document.getElementById('aiToggleBtn');
  const txt = document.getElementById('aiToggleText');
  if (!btn || !txt) return;
  btn.classList.toggle('off', !globalAIEnabled);
  txt.textContent = globalAIEnabled ? 'AI يعمل' : 'AI متوقف';
  btn.title = globalAIEnabled ? 'اضغط لإيقاف AI في كل المحادثات' : 'اضغط لتشغيل AI في كل المحادثات';
  renderAskAIButton();
  renderAIActionButtons();
}

function renderConversationAIToggle() {
  const btn = document.getElementById('conversationAiBtn');
  const txt = document.getElementById('conversationAiText');
  if (!btn || !txt) return;
  btn.classList.toggle('btn-warning', !currentConversationAIEnabled);
  btn.classList.toggle('btn-outline-warning', currentConversationAIEnabled);
  txt.textContent = currentConversationAIEnabled ? 'AI يعمل هنا' : 'AI متوقف هنا';
  btn.title = currentConversationAIEnabled
    ? 'إيقاف AI في هذه المحادثة فقط'
    : 'تسليم هذه المحادثة إلى AI';
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
    ? 'تحليل المحادثة وآخر رسالة وصياغة رد جاهز'
    : (!globalAIEnabled ? 'AI متوقف من الزر الرئيسي' : 'AI متوقف في هذه المحادثة');
  btn.innerHTML = '<i class="bi bi-stars"></i><span>صياغة رد</span>';
}

function renderAIActionButtons() {
  const enabled = canUseAI() && !_isSending && !_isAskingAI;
  const resolveBtn = document.getElementById('resolveAIBtn');
  if (resolveBtn) {
    resolveBtn.disabled = !enabled;
    resolveBtn.classList.toggle('disabled', !enabled);
    resolveBtn.title = enabled
      ? 'حل المشكلة بالـ AI'
      : (!globalAIEnabled ? 'AI متوقف من الزر الرئيسي' : 'AI متوقف في هذه المحادثة');
  }
  const improveBtn = document.getElementById('improveBtn');
  if (improveBtn) {
    improveBtn.disabled = _isSending || _isImproving;
    improveBtn.title = 'تحسين الرسالة (يعمل دائماً، حتى لو AI متوقف)';
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
      showToast(globalAIEnabled ? 'تم تشغيل AI لكل المحادثات' : 'تم إيقاف AI لكل المحادثات', globalAIEnabled ? 'success' : 'warning');
    } else {
      showToast('فشل تغيير حالة AI', 'danger');
    }
  } catch (e) { showToast('خطأ: ' + e.message, 'danger'); }
}

async function toggleConversationAI() {
  if (!currentSenderId) { showToast('اختر محادثة أولاً', 'warning'); return; }
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
        currentConversationAIEnabled ? 'تم تسليم هذه المحادثة إلى AI' : 'تم إيقاف AI في هذه المحادثة فقط',
        currentConversationAIEnabled ? 'success' : 'warning',
      );
      loadConversations(false);
    } else {
      showToast('فشل تغيير حالة AI للمحادثة', 'danger');
    }
  } catch (e) { showToast('خطأ: ' + e.message, 'danger'); }
}

// ══ Quick Actions ══════════════════════════════════════════════════════════
function openOrderModal() {
  if (!currentSenderId) { showToast('اختر محادثة أولاً', 'warning'); return; }
  if (currentCustomer) {
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
    customer_name: currentCustomer?.name || '',
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
      if (r.duplicate) {
        showToast(r.message || 'هذا الطلب مثبت مسبقاً ولم يتم تكراره', 'warning');
      } else if (r.telegram_sent === false) {
        showToast(r.telegram_error || 'تم تثبيت الطلب لكن لم يصل إلى تلغرام', 'warning');
      } else {
        showToast('تم تثبيت الطلب وإرساله إلى تلغرام', 'success');
      }
      bootstrap.Modal.getInstance(document.getElementById('orderModal')).hide();
    } else showToast('فشل تثبيت الطلب: ' + (r.error || ''), 'danger');
  } catch (e) { showToast('خطأ: ' + e.message, 'danger'); }
}

async function sendCatalog() {
  if (!currentSenderId) { showToast('اختر محادثة أولاً', 'warning'); return; }
  try {
    const res = await apiFetch(`/api/conversations/${currentSenderId}/send_catalog`, { method: 'POST' });
    const data = await res.json();
    if (data.ok) {
      showToast(`جاري إرسال ${data.image_count || 0} صورة من الكتالوج بدون نص`, 'success');
      await loadMessages(currentSenderId);
    } else {
      showToast(data.error || 'فشل إرسال الكتالوج', 'danger');
    }
  } catch (e) { showToast('خطأ: ' + e.message, 'danger'); }
}

async function markHumanReview() {
  if (!currentSenderId) { showToast('اختر محادثة أولاً', 'warning'); return; }
  try {
    const res  = await apiFetch(`/api/conversations/${currentSenderId}/mark_reviewed`, { method: 'POST' });
    const data = await res.json();
    if (data.ok) { showToast('تم إغلاق المراجعة', 'success'); loadConversations(false); }
  } catch (e) { showToast('خطأ: ' + e.message, 'danger'); }
}

async function deleteConversation() {
  if (!currentSenderId) { showToast('اختر محادثة أولاً', 'warning'); return; }
  const label = currentCustomer?.name || currentSenderId;
  if (!confirm(`حذف محادثة ${label}؟ سيتم حذف الرسائل والمراجعات وربط المنتجات، ولن يتم حذف الطلبات المسجلة.`)) return;
  const senderId = currentSenderId;
  try {
    const res = await apiFetch(`/api/conversations/${encodeURIComponent(senderId)}`, { method: 'DELETE' });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) throw new Error(data.error || 'فشل حذف المحادثة');
    resetConversationView();
    if (window.innerWidth <= 768 && history.state?.dashboardConversation) {
      const cleanUrl = new URL(location.href);
      cleanUrl.searchParams.delete('sender_id');
      const cleanState = {...(history.state || {})};
      delete cleanState.dashboardConversation;
      history.replaceState(cleanState, '', cleanUrl);
    }
    showToast('تم حذف المحادثة', 'success');
    await loadConversations(false);
    await loadStats();
  } catch (e) {
    showToast(e.message || 'فشل حذف المحادثة', 'danger');
  }
}

// ══ Helpers ════════════════════════════════════════════════════════════════
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

function messageImageUrl(m) {
  const candidate = String(m.image_url || m.text || '').trim();
  if (/audioclip|\.(?:mp4|mp3|ogg|m4a|aac|wav|webm)(?:[?#]|$)/i.test(candidate)) return '';
  return /\.(?:png|jpe?g|gif|webp)(?:[?#]|$)/i.test(candidate) ? safeMediaUrl(candidate) : '';
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
  if (c.ad_id) parts.push(`إعلان: ${esc(c.ad_id)}`);
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


async function unlinkProduct(productId) {
  if (!currentSenderId) return;
  if (!confirm('هل أنت متأكد من إلغاء ربط هذا المنتج؟')) return;
  try {
    const res = await apiFetch(`/api/conversations/${currentSenderId}/unlink_product`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ product_id: productId })
    });
    const data = await res.json();
    if (data.ok) {
      showToast('تم إلغاء ربط المنتج', 'success');
      const conv = allConversations.find(c => c.sender_id === currentSenderId);
      if (conv && conv.product_ids) {
         let pIds = conv.product_ids.split('||');
         let pNames = (conv.product_names || '').split('||');
         const idx = pIds.indexOf(productId);
         if (idx > -1) {
            pIds.splice(idx, 1);
            pNames.splice(idx, 1);
            conv.product_ids = pIds.join('||');
            conv.product_names = pNames.join('||');
         }
      }
      await refreshLinkedProductSync();
    } else {
      showToast('فشل إلغاء الربط', 'danger');
    }
  } catch (e) {
    showToast('خطأ: ' + e.message, 'danger');
  }
}
