const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../static/js/dashboard.js'), 'utf8');
const code = source.slice(source.indexOf('async function waitForAIJob('), source.indexOf('function resetConversationView('));
function setup(onPoll) {
  const nodes = {};
  const calls = [];
  const document = {getElementById(id) {
    return nodes[id] ||= {value: id === 'messageInput' ? 'المسودة الأصلية' : '', style: {}, focus() {}, setSelectionRange() {}};
  }};
  const context = vm.createContext({currentSenderId: 'customer-A', _isAskingAI: false,
    composerRevision:0,rememberComposerDraft(){},
    globalAIEnabled: false, canUseAI: () => false, document,
    renderAskAIButton() {}, renderAIActionButtons() {}, showToast() {}, showHumanIntervention() {},
    setTimeout: callback => callback(),
    apiFetch: async (url, options) => {
      calls.push({url, body: options && JSON.parse(options.body)});
      if (options) return {ok: true, json: async () => ({job_id: 'job-1', status: 'queued'})};
      if (onPoll) onPoll(context);
      return {ok: true, json: async () => ({status: 'done', result: {reply: 'القماش قطن.'}})};
    },
  });
  vm.runInContext(code, context);
  return {context, nodes, calls};
}
(async () => {
  let test = setup();
  await test.context.askAI({text: 'القماش قطن', staffAction: true, rewriteMode: true});
  assert.equal(test.calls[0].body.async, true);
  assert.equal(test.calls[0].body.mode, 'rewrite');
  assert.equal(test.nodes.aiReplyText.textContent, 'القماش قطن.');
  assert.equal(test.nodes.aiReplyPreview.style.display, 'block');
  test = setup(context => { context.currentSenderId = 'customer-B'; });
  await test.context.askAI({text: 'القماش قطن', staffAction: true, rewriteMode: true});
  assert.equal(test.nodes.aiReplyText, undefined, 'Do not display A’s proposal in B’s conversation');
  test = setup();
  await test.context.askAI({text: 'القماش قطن'});
  assert.equal(test.calls.length, 0, 'Ordinary automatic actions respect pause; explicit staff drafting is separate');
  test=setup();test.context.canUseAI=()=>true;
  await test.context.askAI();
  assert.equal(test.nodes.messageInput.value,'المسودة الأصلية');
  assert.equal(test.nodes.aiReplyPreview.style.display,'block');
  test=setup(context=>{context.document.getElementById('messageInput').value='نص جديد أثناء التوليد';context.composerRevision++;});
  test.context.canUseAI=()=>true;test.context.document.getElementById('messageInput').value='';
  await test.context.askAI();
  assert.equal(test.nodes.messageInput.value,'نص جديد أثناء التوليد');
  assert.equal(test.nodes.aiReplyPreview.style.display,'block');
  console.log('PASS: async proposals, explicit staff action while paused, stale conversation protection');
})().catch(error => { console.error(error); process.exitCode = 1; });
