const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../static/js/dashboard.js'), 'utf8');
const nodes = Object.fromEntries(['Context','Image','Link','Video','Title','Details','Summary','Copy','Id'].map(name =>
  ['conversationAd'+name, {hidden:true, classList:{toggle() {}}, removeAttribute(key) {delete this[key];}}]));
const context = vm.createContext({URL, currentAdContext:null, currentCustomer:{store_id:'default',product_ids:'p1'}, allProducts:[],
  splitPipeList:value => String(value || '').split('||').filter(Boolean), document:{getElementById:id => nodes[id]}});
vm.runInContext(source.slice(source.indexOf('function adMatchesLinkedProduct('), source.indexOf('function safeMediaUrl(')), context);
const ad = {ad_id:'120212345678901234',headline:'Same product',image_url:'https://example.com/ad.jpg',
  video_url:'https://example.com/ad.mp4',source_url:'https://example.com/source'};
const product = {store_id:'default',product_id:'p1',ad_ids:[ad.ad_id]};
function render() { context.renderConversationAdContext(ad); }
render();
assert.equal(nodes.conversationAdImage.hidden,false,'Show details until exact product mappings arrive');
context.allProducts = [product];
context.renderConversationAdContext(context.currentAdContext);
assert.equal(nodes.conversationAdSummary.hidden,false);
assert.equal(nodes.conversationAdId.textContent,ad.ad_id,'Long Meta IDs retain every digit');
assert.equal(nodes.conversationAdSummary.href,ad.source_url,'Compact ID retains source access');
for (const name of ['Image','Copy','Link','Video']) assert.equal(nodes['conversationAd'+name].hidden,true,name);
assert.equal(nodes.conversationAdImage.src,undefined,'Repeated image is not requested');
for (const products of [[{...product,product_id:'p2'}], [{...product,store_id:'other'}],
  [product,{...product,product_id:'p2'}], [{...product,ad_ids:['120212345678901235']}], []]) {
  context.allProducts = products; render();
  assert.equal(nodes.conversationAdSummary.hidden,true,'Unmatched, other-store and ambiguous ads retain full context');
  assert.equal(nodes.conversationAdImage.hidden,false);
}
context.allProducts = [product];
context.currentCustomer.product_ids = '';
render(); assert.equal(nodes.conversationAdSummary.hidden,true,'Unlinking restores details');
context.currentCustomer.product_ids = 'p1||p3';
render(); assert.equal(nodes.conversationAdSummary.hidden,false);
context.allProducts = [{...product,ad_ids:[],ref:'exact-ref'}];
context.renderConversationAdContext({...ad,ref:'exact-ref'});
assert.equal(nodes.conversationAdSummary.hidden,false,'Owner referral mappings also identify the linked product');
context.renderConversationAdContext({...ad,ref:'exact-ref',source_url:'javascript:alert(1)'});
assert.equal(nodes.conversationAdSummary.href,undefined);
context.renderConversationAdContext(null);
assert.equal(nodes.conversationAdContext.hidden,true);
assert.equal(nodes.conversationAdId.textContent,'');
assert.equal(nodes.conversationAdSummary.href,undefined,'Switching conversations clears the prior source');
console.log('PASS: exact ad/product matching, delayed products, link changes, isolation, safe links and long IDs');
