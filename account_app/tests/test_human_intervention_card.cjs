const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../static/js/dashboard.js'), 'utf8');
const code = source.slice(source.indexOf('function humanInterventionRequired('), source.indexOf('function _hiPopulateProducts('));
const nodes = {hiBanner:{hidden:true,style:{}},hiBannerTitle:{},hiBannerSub:{}};
const context = vm.createContext({currentSenderId:'A',currentCustomer:null,allConversations:[],
  document:{getElementById:id=>nodes[id]},_hiCurrentMode:()=> 'text'});
vm.runInContext(code,context);
function select(conversation) { context.allConversations=[{sender_id:'A',...conversation}]; context.showHumanIntervention(); }

select({human_attention_count:0});
assert.equal(nodes.hiBanner.hidden,true,'Ordinary conversations must not show the card');
select({pending_reviews_count:1});
assert.equal(nodes.hiBanner.hidden,false,'A pending review reveals the actionable card');
assert.equal(nodes.hiBanner.style.display,'flex');
select({problem_count:1});
assert.equal(nodes.hiBanner.hidden,false,'An open human problem reveals the card');
select({human_attention_count:0,pending_reviews_count:1,problem_count:1});
assert.equal(nodes.hiBanner.hidden,true,'The authoritative resolved total overrides stale counters');
select({human_attention_count:2});
assert.equal(nodes.hiBanner.hidden,false);

// Use the actual close-result handler, including its authoritative state patch.
Object.assign(context,{inboxMutationRevision:0,pendingReviewsBySender:{},openProblemsBySender:{},
  currentCustomer:context.allConversations[0],currentFilter:'all',renderConversationAIToggle(){},renderConversations(){}});
vm.runInContext(source.slice(source.indexOf('function applyInboxAttention('),source.indexOf('async function markHumanReview(')),context);
context.applyInboxAttention('A',{attention:{human_attention_count:0,pending_reviews_count:0,problem_count:0}});
assert.equal(nodes.hiBanner.hidden,true,'Successful resolution hides the card immediately');
context.applyInboxAttention('B',{attention:{human_attention_count:2}});
assert.equal(nodes.hiBanner.hidden,true,'An update for another customer cannot reveal this card');

const template=fs.readFileSync(path.join(__dirname,'../templates/dashboard.html'),'utf8');
assert.equal((template.match(/class="human-intervention-card"/g)||[]).length,1,'There is only one intervention card');
assert.match(template,/<button id="hiBanner"[^>]+ hidden>/,'The card starts hidden before conversation data arrives');
assert.doesNotMatch(template,/class="hi-banner"/,'The old duplicate alert is removed');
console.log('PASS: conditional intervention card, authoritative resolution, cross-conversation isolation and hidden initial state');
