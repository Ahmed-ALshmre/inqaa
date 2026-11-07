const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const composerCode = fs.readFileSync(path.join(__dirname, '../static/js/image_composer.js'), 'utf8');
const dashboardCode = fs.readFileSync(path.join(__dirname, '../static/js/dashboard.js'), 'utf8');
const sendCode = dashboardCode.slice(dashboardCode.indexOf('async function sendMessage('), dashboardCode.indexOf('async function testManyChat('));
const tick = () => new Promise(resolve => setImmediate(resolve));
function setup() {
  const nodes = new Map(), requests = [], revoked = [], toasts = [];
  function element() { return {children:[],value:'',classList:{toggle(){}},append(...items){this.children.push(...items);},replaceChildren(){this.children=[];},setAttribute(){}}; }
  const context = vm.createContext({
    currentSenderId:'customer-A',currentCustomer:{store_id:'store-A'},_isSending:false,
    composerRevision:0,rememberComposerDraft(){},
    document:{getElementById(id){if(!nodes.has(id))nodes.set(id,element());return nodes.get(id);},createElement:element},
    URL:{createObjectURL:file=>'blob:'+file.name,revokeObjectURL:url=>revoked.push(url)},
    FormData:class { append(){} },AbortController,setTimeout,clearTimeout,Promise,Date,Math,
    safeMediaUrl:url=>typeof url==='string' && url ? url : '',
    prepareComposerImage:async file=>file,showToast:message=>toasts.push(message),
    _setSendingState:busy=>{context._isSending=busy;},loadMessages:async()=>{},
    apiFetch: (url,options)=>new Promise((resolve,reject)=>{
      requests.push({url,options,resolve:body=>resolve({ok:true,json:async()=>body})});
      options.signal?.addEventListener('abort',()=>reject(Object.assign(Error('aborted'),{name:'AbortError'})),{once:true});
    }),
  });
  vm.runInContext('let composerImages=[]; let composerSendIdentity=null; let imageUploadGeneration=0;',context);
  vm.runInContext(composerCode+'\n'+sendCode,context);
  return {context,nodes,requests,revoked,toasts,items:()=>vm.runInContext('composerImages',context)};
}
const file = name=>({name,type:'image/jpeg',size:1024});
(async()=>{
  let test=setup();
  await test.context.handleImageUpload({target:{files:[file('a'),file('b'),file('c')],value:'chosen'}});
  await tick();
  assert.equal(test.requests.length,2,'Only two uploads may run concurrently');
  test.requests[1].resolve({image_url:'b.jpg'});await tick();
  assert.equal(test.requests.length,3,'A completed upload releases the next slot');
  test.requests[2].resolve({image_url:'c.jpg'});test.requests[0].resolve({image_url:'a.jpg'});await tick();
  assert.deepEqual(Array.from(test.items(),item=>item.url),['a.jpg','b.jpg','c.jpg'],'Selection order survives out-of-order uploads');
  test.nodes.get('messageInput') || test.context.document.getElementById('messageInput');
  test.nodes.get('messageInput').value='caption';
  const sending=test.context.sendMessage();await tick();
  const body=JSON.parse(test.requests[3].options.body);
  assert.deepEqual(body.image_urls,['a.jpg','b.jpg','c.jpg']);assert.equal(body.text,'caption');
  test.requests[3].resolve({ok:false,sent_image_urls:['a.jpg']});await sending;
  assert.equal(test.items().length,3,'A failed batch preserves the draft for retry');
  const retry=test.context.sendMessage();await tick();
  assert.equal(JSON.parse(test.requests[4].options.body).client_send_id,body.client_send_id,'A retry retains delivery identity');
  test.requests[4].resolve({ok:true});assert.equal(await retry,true);
  assert.equal(test.items().length,0);assert.equal(test.revoked.length,3);assert.equal(test.nodes.get('messageInput').value,'');

  test=setup();await test.context.handleImageUpload({target:{files:[file('a'),file('b')],value:''}});await tick();
  const pending=test.context.sendMessage();await tick();
  test.context.clearImage();test.context.currentSenderId='customer-B';await tick();
  assert.equal(await pending,false);assert.equal(test.items().length,0);
  assert.equal(test.requests.filter(request=>request.url.includes('/send')).length,0,'Switching conversations never sends old pending attachments');

  test=setup();
  test.context.addComposerImages(['one.jpg','two.jpg','one.jpg']);
  assert.equal(test.items().length,2);
  const id=test.items()[0].id;test.context.removeComposerImage(id);
  assert.deepEqual(Array.from(test.items(),item=>item.url),['two.jpg']);
  assert.equal(test.context.addComposerImages(Array.from({length:10},(_,i)=>`extra-${i}.jpg`)),false);
  assert.equal(test.items().length,1,'An oversized selection cannot replace existing images');
  console.log('PASS: concurrent uploads, ordered batch, retry identity, cancellation, deduplication and limits');
})().catch(error=>{console.error(error);process.exitCode=1;});
