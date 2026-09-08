const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../static/js/dashboard.js'), 'utf8');
const batchCode = source.slice(source.indexOf('let _isSendingProductDetails ='), source.indexOf('// ══ AI Instructions'));
const sendCode = source.slice(source.indexOf('async function sendMessage('), source.indexOf('async function testManyChat('));
function batchContext(send) {
  const calls = [];
  const context = vm.createContext({currentSenderId: 'customer-A', _isSending: false,
    selectedValues: () => ['P1', 'P2'], document: {getElementById: () => ({})},
    allProducts: [{product_id: 'P1', images: ['a.jpg', 'b.jpg']}, {product_id: 'P2', images: ['b.jpg']}],
    productImageList: p => p.images, showToast: () => {},
    sendMessage: async (...args) => { calls.push(args); return send ? send(context) : true; }
  });
  vm.runInContext(batchCode, context);
  return {context, calls};
}
(async () => {
  let {context, calls} = batchContext();
  await context.sendProductDetails();
  assert.deepEqual(calls, [['', 'a.jpg'], ['', 'b.jpg']]);
  ({context, calls} = batchContext(c => {c.currentSenderId = 'customer-B'; return true;}));
  await context.sendProductDetails();
  assert.equal(calls.length, 1, 'Never send the rest to a different customer');
  ({context, calls} = batchContext(() => false));
  await context.sendProductDetails();
  assert.equal(calls.length, 1, 'Stop after a failed image');
  const requests = [];
  const composer = {value: 'UNSENT CUSTOMER DRAFT'};
  const sendContext = vm.createContext({currentSenderId: 'customer-A', _isSending: false,
    document: {getElementById: () => composer}, uploadedImageUrl: 'draft.jpg',
    showToast: () => {}, _setSendingState: () => {}, clearImage: () => {throw Error('Draft cleared');},
    loadMessages: async () => {}, apiFetch: async (url, options) => {
      requests.push({url, body: JSON.parse(options.body)});
      return {json: async () => ({ok: true})};
    }
  });
  vm.runInContext(sendCode, sendContext);
  assert.equal(await sendContext.sendMessage('', 'product.jpg'), true);
  assert.deepEqual(requests[0].body, {text: '', image_url: 'product.jpg'});
  assert.equal(composer.value, 'UNSENT CUSTOMER DRAFT');
  console.log('PASS: images only, deduplication, conversation switch, send failure, composer draft preserved');
})().catch(error => {console.error(error); process.exitCode = 1;});
