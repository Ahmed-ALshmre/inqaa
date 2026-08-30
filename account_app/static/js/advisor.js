let advisorBusy = false;

function advisorEsc(value) {
  return String(value ?? '').replace(/[&<>"']/g, (char) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[char]));
}

function renderAdvisorMessages(messages) {
  const container = document.getElementById('advisorMessages');
  if (!messages.length) {
    container.innerHTML = '<div class="advisor-empty"><i class="bi bi-chat-heart"></i><strong>ابدأ النقاش مع مستشار صوف</strong><span>يمكنك طلب مراجعة محادثات سابقة أو مناقشة قاعدة وتحسين طريقة العمل.</span></div>';
    return;
  }
  container.innerHTML = messages.map((message) => `
    <article class="advisor-message ${message.role === 'user' ? 'owner' : 'assistant'}">
      <div class="advisor-message-label">${message.role === 'user' ? 'أنت' : 'مستشار صوف'}</div>
      <div class="advisor-message-content">${advisorEsc(message.content)}</div>
      ${message.analysis_text ? `<details class="advisor-analysis"><summary><i class="bi bi-graph-up-arrow"></i> عرض نتيجة التحليل</summary><div>${advisorEsc(message.analysis_text)}</div></details>` : ''}
      ${message.conversations_count ? `<small>راجع ${advisorEsc(message.conversations_count)} محادثة في هذه الجولة</small>` : ''}
    </article>
  `).join('');
  container.scrollTop = container.scrollHeight;
}

function proposalStatusLabel(status) {
  return {pending: 'بانتظار موافقتك', approved: 'تمت الموافقة', rejected: 'مرفوض'}[status] || status;
}

function renderAdvisorProposals(proposals) {
  const container = document.getElementById('advisorProposals');
  if (!proposals.length) {
    container.innerHTML = '<div class="advisor-empty compact"><i class="bi bi-lightbulb"></i><span>لا توجد اقتراحات بعد</span></div>';
    return;
  }
  container.innerHTML = proposals.map((proposal) => `
    <article class="advisor-proposal ${advisorEsc(proposal.status)}">
      <div class="advisor-proposal-top">
        <span class="advisor-proposal-type">${proposal.apply_target === 'active_rule' ? 'قاعدة تشغيل' : 'ذاكرة'}</span>
        <span class="advisor-proposal-status">${proposalStatusLabel(proposal.status)}</span>
      </div>
      <h3>${advisorEsc(proposal.title)}</h3>
      <p>${advisorEsc(proposal.content)}</p>
      ${proposal.reason ? `<div class="advisor-proposal-reason"><strong>السبب:</strong> ${advisorEsc(proposal.reason)}</div>` : ''}
      ${proposal.status === 'pending' ? `<div class="advisor-proposal-actions">
        <button class="btn btn-success btn-sm" onclick="reviewAdvisorProposal(${Number(proposal.id)}, 'approve')"><i class="bi bi-check2-circle"></i> موافقة وإضافة</button>
        <button class="btn btn-outline-danger btn-sm" onclick="reviewAdvisorProposal(${Number(proposal.id)}, 'reject')"><i class="bi bi-x-circle"></i> رفض</button>
      </div>` : ''}
    </article>
  `).join('');
}

async function loadAdvisor() {
  const response = await fetch(adminApi('/api/advisor'));
  const data = await response.json();
  if (!response.ok || !data.ok) throw new Error(data.error || 'تعذر تحميل المستشار');
  renderAdvisorMessages(data.messages || []);
  renderAdvisorProposals(data.proposals || []);
  document.getElementById('advisorMemoryCount').textContent = data.approved_count || 0;
  document.getElementById('advisorStats').textContent = `${(data.messages || []).length} رسالة محفوظة · ${(data.proposals || []).filter((item) => item.status === 'pending').length} بانتظار الموافقة`;
}

async function sendAdvisorMessage(event) {
  event.preventDefault();
  if (advisorBusy) return;
  const input = document.getElementById('advisorInput');
  const message = input.value.trim();
  if (!message) return;
  advisorBusy = true;
  const button = document.getElementById('advisorSendBtn');
  button.disabled = true;
  button.innerHTML = '<span class="spinner-border spinner-border-sm"></span><span>يحلل المحادثات…</span>';
  try {
    const response = await fetch(adminApi('/api/advisor/chat'), {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({message})
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
    button.innerHTML = '<i class="bi bi-send-fill"></i><span>إرسال للمستشار</span>';
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
