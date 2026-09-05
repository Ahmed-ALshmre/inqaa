let advisorBusy = false;
let advisorProposalFilter = 'all';
let advisorProposalCache = [];
let advisorMessageCache = [];

function advisorEsc(value) {
  return String(value ?? '').replace(/[&<>"']/g, (char) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[char]));
}

function advisorInlineFormat(value) {
  return String(value || '')
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/(^|\s)\*([^*\n]+?)\*(?=\s|[،,.!?؟]|$)/g, '$1<em>$2</em>')
    .replace(/`([^`]+)`/g, '<code>$1</code>');
}

function advisorFormat(value) {
  return advisorEsc(value).trim().replace(/\n\s*\n(?:\s*\n)+/g, '\n\n').split(/\r?\n/).map((line) => {
    const clean = line.trim();
    if (!clean) return '<div class="advisor-text-gap"></div>';
    if (/^[-ـ—]{3,}$/.test(clean)) return '<hr class="advisor-text-divider">';
    const heading = clean.match(/^#{1,3}\s+(.+)/);
    if (heading) return `<strong class="advisor-text-heading">${advisorInlineFormat(heading[1])}</strong>`;
    const bullet = clean.match(/^[-•*]\s+(.+)/);
    if (bullet) return `<div class="advisor-text-bullet"><span class="advisor-bullet-dot"></span><span>${advisorInlineFormat(bullet[1])}</span></div>`;
    const numbered = clean.match(/^(\d+)[.)-]\s+(.+)/);
    if (numbered) return `<div class="advisor-text-bullet numbered"><b>${numbered[1]}</b><span>${advisorInlineFormat(numbered[2])}</span></div>`;
    const titled = clean.match(/^([^:：]{2,45})[:：]\s*(.*)$/);
    if (titled && !/^https?/.test(clean)) return `<div class="advisor-text-titled"><strong>${advisorInlineFormat(titled[1])}</strong>${titled[2] ? `<span>${advisorInlineFormat(titled[2])}</span>` : ''}</div>`;
    return `<p>${advisorInlineFormat(clean)}</p>`;
  }).join('');
}

function advisorFormatTime(value) {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return new Intl.DateTimeFormat('ar-IQ', {hour: 'numeric', minute: '2-digit'}).format(date);
}

async function copyAdvisorMessage(index, button) {
  const message = advisorMessageCache[index];
  if (!message) return;
  await navigator.clipboard.writeText([message.content, message.analysis_text].filter(Boolean).join('\n\n'));
  const old = button.innerHTML;
  button.innerHTML = '<i class="bi bi-check2"></i> تم النسخ';
  setTimeout(() => { button.innerHTML = old; }, 1400);
}

function renderAdvisorMessages(messages) {
  const container = document.getElementById('advisorMessages');
  const followLatest = !advisorMessageCache.length || container.scrollHeight - container.scrollTop - container.clientHeight < 70;
  const previousTop = container.scrollTop;
  advisorMessageCache = messages || [];
  if (!messages.length) {
    container.innerHTML = '<div class="advisor-empty"><i class="bi bi-chat-heart"></i><strong>ابدأ النقاش مع مستشار صوف</strong><span>يمكنك طلب مراجعة محادثات سابقة أو مناقشة قاعدة وتحسين طريقة العمل.</span></div>';
    return;
  }
  container.innerHTML = messages.map((message, index) => `
    <article class="advisor-message ${message.role === 'user' ? 'owner' : 'assistant'}">
      <div class="advisor-message-label">
        <span class="advisor-message-author">${message.role === 'user' ? '<i class="bi bi-person-fill"></i> أنت' : '<span class="advisor-mini-avatar">ص</span> مستشار صوف'}</span>
        ${message.role !== 'user' && message.conversations_count ? `<span class="advisor-message-evidence"><i class="bi bi-database-check"></i> ${advisorEsc(message.conversations_count)} محادثة</span>` : ''}
      </div>
      <div class="advisor-message-content">${advisorFormat(message.content)}</div>
      ${message.analysis_text ? `<section class="advisor-analysis"><div class="advisor-analysis-title"><i class="bi bi-graph-up-arrow"></i><strong>نتيجة التحليل</strong></div><div class="advisor-analysis-content">${advisorFormat(message.analysis_text)}</div></section>` : ''}
      <footer class="advisor-message-footer">
        <time>${advisorFormatTime(message.created_at)}</time>
        <button type="button" onclick="copyAdvisorMessage(${index}, this)" title="نسخ الرسالة"><i class="bi bi-copy"></i> نسخ</button>
      </footer>
    </article>
  `).join('');
  container.scrollTop = followLatest ? container.scrollHeight : previousTop;
}

function renderAdvisorConfig(config = {}) {
  const model = config.model || 'غير محدد';
  document.getElementById('advisorModelBadge').innerHTML = `<i class="bi bi-cpu"></i><span>النموذج: <b>${advisorEsc(model)}</b></span>`;
  const priorities = config.source_priority || [];
  document.getElementById('advisorPriorityList').innerHTML = priorities.map((source, index) => `
    <div class="advisor-priority-item">
      <b>${index + 1}</b>
      <div><strong>${advisorEsc(source.label)}</strong><span>${advisorEsc(source.description)}</span></div>
    </div>
  `).join('') || '<div class="advisor-priority-loading">لا توجد معلومات متاحة</div>';
}

function proposalStatusLabel(status) {
  return {pending: 'بانتظار موافقتك', approved: 'تمت الموافقة', rejected: 'مرفوض'}[status] || status;
}

function renderAdvisorProposals(proposals) {
  advisorProposalCache = proposals || [];
  const container = document.getElementById('advisorProposals');
  const visible = advisorProposalFilter === 'all'
    ? advisorProposalCache
    : advisorProposalCache.filter((proposal) => proposal.status === advisorProposalFilter);
  if (!visible.length) {
    container.innerHTML = '<div class="advisor-empty compact"><i class="bi bi-lightbulb"></i><span>لا توجد اقتراحات بعد</span></div>';
    return;
  }
  container.innerHTML = visible.map((proposal) => `
    <article class="advisor-proposal ${advisorEsc(proposal.status)}">
      <div class="advisor-proposal-top">
        <span class="advisor-proposal-type"><i class="bi ${proposal.apply_target === 'active_rule' ? 'bi-lightning-charge-fill' : 'bi-memory'}"></i> ${proposal.apply_target === 'active_rule' ? 'قاعدة تشغيل' : 'ذاكرة'}</span>
        <span class="advisor-proposal-status">${proposalStatusLabel(proposal.status)}</span>
      </div>
      <h3>${advisorEsc(proposal.title)}</h3>
      <div class="advisor-proposal-content">${advisorFormat(proposal.content)}</div>
      ${proposal.reason ? `<div class="advisor-proposal-reason"><strong>السبب:</strong> ${advisorEsc(proposal.reason)}</div>` : ''}
      ${proposal.status === 'pending' ? `<div class="advisor-proposal-actions">
        <button class="btn btn-success btn-sm" onclick="reviewAdvisorProposal(${Number(proposal.id)}, 'approve')"><i class="bi bi-check2-circle"></i> موافقة وإضافة</button>
        <button class="btn btn-outline-danger btn-sm" onclick="reviewAdvisorProposal(${Number(proposal.id)}, 'reject')"><i class="bi bi-x-circle"></i> رفض</button>
      </div>` : ''}
    </article>
  `).join('');
}

function setAdvisorProposalFilter(filter, button) {
  advisorProposalFilter = filter;
  document.querySelectorAll('#advisorProposalFilters button').forEach((item) => {
    item.classList.toggle('active', item === button);
  });
  renderAdvisorProposals(advisorProposalCache);
}

async function loadAdvisor() {
  const response = await fetch(adminApi('/api/advisor'));
  const data = await response.json();
  if (!response.ok || !data.ok) throw new Error(data.error || 'تعذر تحميل المستشار');
  renderAdvisorMessages(data.messages || []);
  renderAdvisorProposals(data.proposals || []);
  renderAdvisorConfig(data.advisor_config || {});
  document.getElementById('advisorMemoryCount').textContent = data.approved_count || 0;
  const pendingCount = (data.proposals || []).filter((item) => item.status === 'pending').length;
  document.getElementById('advisorStats').textContent = `${(data.messages || []).length} رسالة محفوظة · ${pendingCount} بانتظار الموافقة`;
  document.getElementById('advisorMessageMetric').textContent = (data.messages || []).length;
  document.getElementById('advisorPendingMetric').textContent = pendingCount;
  document.getElementById('advisorApprovedMetric').textContent = data.approved_count || 0;
}

function setAdvisorPrompt(text) {
  const input = document.getElementById('advisorInput');
  input.value = text;
  input.focus();
}

async function sendAdvisorMessage(event) {
  event.preventDefault();
  if (advisorBusy) return;
  const input = document.getElementById('advisorInput');
  const message = input.value.trim();
  if (!message) return;
  advisorBusy = true;
  const button = document.getElementById('advisorSendBtn');
  const reviewLimit = Number(document.getElementById('advisorReviewLimit')?.value || 1500);
  const mode = document.getElementById('advisorMode')?.value || 'chat';
  const messages = document.getElementById('advisorMessages');
  const thinkingText = mode === 'analysis'
    ? `يقرأ حتى ${reviewLimit.toLocaleString('ar-IQ')} رسالة ويحلل الأنماط…`
    : 'يقرأ سجل حديثكما وذاكرته والبيانات المرتبطة بسؤالك…';
  messages.insertAdjacentHTML('beforeend', `<article class="advisor-message owner"><div class="advisor-message-label"><span class="advisor-message-author"><i class="bi bi-person-fill"></i> أنت</span></div><div class="advisor-message-content">${advisorFormat(message)}</div></article><article class="advisor-message assistant advisor-thinking"><div class="advisor-message-label"><span class="advisor-message-author"><span class="advisor-mini-avatar">ص</span> مستشار صوف</span></div><div class="advisor-thinking-row"><span></span><span></span><span></span><b>${thinkingText}</b></div></article>`);
  messages.scrollTop = messages.scrollHeight;
  button.disabled = true;
  button.innerHTML = `<span class="spinner-border spinner-border-sm"></span><span>${mode === 'analysis' ? 'جاري التحليل…' : 'يفكر…'}</span>`;
  try {
    const response = await fetch(adminApi('/api/advisor/chat'), {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({message, review_limit: reviewLimit, mode})
    });
    const data = await response.json();
    if (!response.ok || !data.ok) throw new Error(data.error || 'فشل رد المستشار');
    input.value = '';
    await loadAdvisor();
  } catch (error) {
    alert(error.message);
  } finally {
    advisorBusy = false;
    button.disabled = false;
    button.innerHTML = '<i class="bi bi-send-fill"></i><span>إرسال</span>';
  }
}

async function reviewAdvisorProposal(id, action) {
  const label = action === 'approve' ? 'إضافة هذا البند إلى الذاكرة؟' : 'رفض هذا الاقتراح؟';
  if (!confirm(label)) return;
  const response = await fetch(adminApi(`/api/advisor/proposals/${id}/${action}`), {method: 'POST'});
  const data = await response.json();
  if (!response.ok || !data.ok) return alert(data.error || 'تعذر تحديث الاقتراح');
  await loadAdvisor();
}

document.addEventListener('DOMContentLoaded', () => {
  const keepComposerVisible = () => {
    if (window.innerWidth <= 900 && document.activeElement?.id === 'advisorInput') {
      requestAnimationFrame(() => document.getElementById('advisorForm')?.scrollIntoView({block: 'nearest'}));
    }
  };
  document.getElementById('advisorInput')?.addEventListener('focus', keepComposerVisible);
  window.visualViewport?.addEventListener('resize', keepComposerVisible, {passive: true});
  document.getElementById('advisorForm')?.addEventListener('submit', sendAdvisorMessage);
  document.getElementById('advisorInput')?.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      document.getElementById('advisorForm')?.requestSubmit();
    }
  });
  loadAdvisor().catch((error) => alert(error.message));
});
