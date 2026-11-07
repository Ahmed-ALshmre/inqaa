const assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const js=fs.readFileSync(path.join(__dirname,'../static/js/dashboard.js'),'utf8');
const code=js.slice(js.indexOf('async function sendMessage('),js.indexOf('async function testManyChat('))+
 js.slice(js.indexOf('async function hiSendDirect('),js.indexOf('async function hiCloseReview('))+
 js.slice(js.indexOf('async function sendAIReply('),js.indexOf('// ══ Improve Message'));
const tick=()=>new Promise(resolve=>setImmediate(resolve));
function setup(){
 const nodes={messageInput:{value:'النص المرسل'},hiUnifiedText:{value:'مسودة المساعد'},aiReplyPreview:{style:{}}};
 const calls=[];let pending;
 const c=vm.createContext({currentSenderId:'A',composerRevision:0,helperDraftRevision:0,imageUploadGeneration:0,
  composerImages:[],composerSendIdentity:null,_isSending:false,_hiBusy:false,
  aiPendingReply:'النص المرسل',aiPendingSource:{text:'مسودة المساعد',revision:0},
  document:{getElementById:id=>nodes[id]},rememberComposerDraft(){calls.push('remember');},
  _setSendingState(value){c._isSending=value;},waitForComposerImages:async()=>true,
  clearImage(){calls.push('clear-images');},showToast(){},loadMessages:async()=>{},
  _hiSetBusy(value){c._hiBusy=value;},_hiResetButtons(){},_hiCloseModal(){},
  _hiSyncGenderIfChosen:async()=>{},_hiSilentLinkIfChosen:async()=>{},
  finishHumanReview:async()=>{},
  apiFetch:async()=>new Promise(resolve=>{pending=body=>resolve({ok:body.ok,json:async()=>body});}),
 });vm.runInContext(code,c);
 return {c,nodes,ack:body=>pending(body),calls};
}
(async()=>{
 let t=setup(),p=t.c.sendMessage();await tick();t.ack({ok:true});await p;
 assert.equal(t.nodes.messageInput.value,'');assert(t.calls.includes('remember'));
 t=setup();p=t.c.sendMessage('النص المرسل');await tick();t.ack({ok:true});await p;
 assert.equal(t.nodes.messageInput.value,'','An explicitly supplied sent copy must also clear');
 t=setup();p=t.c.hiSendDirect();await tick();t.ack({ok:true});await p;
 assert.equal(t.nodes.hiUnifiedText.value,'');assert.equal(t.nodes.messageInput.value,'النص المرسل','Keep an unrelated draft');
 t=setup();p=t.c.sendAIReply();await tick();t.ack({ok:true});await p;
 assert.equal(t.nodes.hiUnifiedText.value,'');assert.equal(t.nodes.messageInput.value,'');assert.equal(t.c.aiPendingReply,null);
 t=setup();t.c.editAIReply();assert.equal(t.nodes.hiUnifiedText.value,'مسودة المساعد');
 p=t.c.sendMessage();await tick();t.ack({ok:true});await p;
 assert.equal(t.nodes.messageInput.value,'');assert.equal(t.nodes.hiUnifiedText.value,'','Editing an AI proposal must retain its source until the actual successful send');
 t=setup();p=t.c.hiSendDirect();await tick();t.ack({ok:false,error:'test failure'});await p;
 assert.equal(t.nodes.hiUnifiedText.value,'مسودة المساعد');
 t=setup();p=t.c.sendAIReply();await tick();t.ack({ok:false});await p;
 assert.equal(t.nodes.messageInput.value,'النص المرسل');assert.equal(t.nodes.hiUnifiedText.value,'مسودة المساعد');assert(t.c.aiPendingReply);
 t=setup();p=t.c.sendAIReply();await tick();t.nodes.messageInput.value='رسالة جديدة';t.c.composerRevision++;
 t.nodes.hiUnifiedText.value='ملاحظة جديدة';t.c.helperDraftRevision++;t.ack({ok:true});await p;
 assert.equal(t.nodes.messageInput.value,'رسالة جديدة');assert.equal(t.nodes.hiUnifiedText.value,'ملاحظة جديدة');
 t=setup();p=t.c.sendAIReply();await tick();t.c.currentSenderId='B';t.c.aiPendingReply='اقتراح آخر';
 t.nodes.messageInput.value='مسودة ب';t.nodes.hiUnifiedText.value='مساعد ب';t.ack({ok:true});await p;
 assert.equal(t.nodes.messageInput.value,'مسودة ب');assert.equal(t.nodes.hiUnifiedText.value,'مساعد ب');assert.equal(t.c.aiPendingReply,'اقتراح آخر');
 console.log('PASS: 9 sent-field cases: manual, explicit copy, helper, AI, edited AI, failures, concurrent edits and conversation switching');
})().catch(error=>{console.error(error);process.exitCode=1;});
