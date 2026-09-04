let advisorBusy = false;
let advisorProposalFilter = 'all';
let advisorProposalCache = [];

function advisorEsc(value) {
  return String(value ?? '').replace(/[&<>"']/g, (char) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[char]));
}

function advisorFormat(value) {
  return advisorEsc(value).split(/\r?\n/).map((line) => {
    const clean = line.trim();
    if (!clean) return '<div class="advisor-text-gap"></div>';
    const heading = clean.match(/^#{1,3}\s+(.+)/);
    if (heading) return `<strong class="advisor-text-heading">${heading[1]}</strong>`;
    const bullet = clean.match(/^[-•*]\s+(.+)/);
    if (bullet) return `<div class="advisor-text-bullet"><i class="bi bi-check2"></i><span>${bullet[1]}</span></div>`;
    const numbered = clean.match(/^(\d+)[.)-]\s+(.+)/);
    if (numbered) return `<div class="advisor-text-bullet numbered"><b>${numbered[1]}</b><span>${numbered[2]}</span></div>`;
    return `<p>${clean.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')}</p>`;
  }).join('');
}

function renderAdvisorMessages(messages) {
  const container = document.getElementById('advisorMessages');
  if (!messages.length) {
    container.innerHTML = '<div class="advisor-empty"><i class="bi bi-chat-heart"></i><strong>ابدأ النقاش مع مستشار صوف</strong><span>يمكنك طلب مراجعة محادثات سابقة أو مناقشة قاعدة وتحسين طريقة العمل.</span></div>';
    return;
  }
  container.innerHTML = messages.map((message) => `
    <article class="advisor-message ${message.role === 'user' ? 'owner' : 'assistant'}">
      <div class="advisor-message-label">${message.role === 'user' ? '<i class="bi bi-person-fill"></i> أنت' : '<span><i class="bi bi-stars"></i></span> مستشار صوف'}</div>
      <div class="advisor-message-content">${advisorFormat(message.content)}</div>
      ${message.analysis_text ? `<section class="advisor-analysis"><div class="advisor-analysis-title"><i class="bi bi-graph-up-arrow"></i><strong>نتيجة التحليل</strong></div><div class="advisor-analysis-content">${advisorFormat(message.analysis_text)}</div></section>` : ''}
      ${message.conversations_count ? `<small class="advisor-reviewed"><i class="bi bi-database-check"></i> راجع ${advisorEsc(message.conversations_count)} محادثة في هذه الجولة</small>` : ''}
    </article>
  `).join('');
  container.scrollTop = container.scrollHeight;
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
  messages.insertAdjacentHTML('beforeend', `<article class="advisor-message owner"><div class="advisor-message-label"><i class="bi bi-person-fill"></i> أنت</div><div class="advisor-message-content">${advisorFormat(message)}</div></article><article class="advisor-message assistant advisor-thinking"><div class="advisor-message-label"><span><i class="bi bi-stars"></i></span> مستشار صوف</div><div class="advisor-thinking-row"><span></span><span></span><span></span><b>${thinkingText}</b></div></article>`);
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
  document.getElementById('advisorForm')?.addEventListener('submit', sendAdvisorMessage);
  document.getElementById('advisorInput')?.addEventListener('keydown', (event) => {
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      document.getElementById('advisorForm')?.requestSubmit();
    }
  });
  loadAdvisor().catch((error) => alert(error.message));
});
