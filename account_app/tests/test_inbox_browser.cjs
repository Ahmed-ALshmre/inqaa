// Run tools/preview_inbox.py first. Uses synthetic local data, never production.
const {chromium}=require('playwright');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const base=process.env.INBOX_QA_URL||'http://127.0.0.1:5117';
const output=path.resolve('outputs/inbox-qa');
fs.mkdirSync(output,{recursive:true});
const results=[];

async function ready(page) {
  await page.waitForFunction(()=>!inboxRequest&&document.querySelectorAll('#customerList .customer-item').length>0);
}
async function filter(page,value) {
  await page.locator(`#conversationFilters [data-filter="${value}"]`).click();
  await ready(page);
}
async function visibleAnchor(page) {
  return page.evaluate(()=>{
    const el=document.getElementById('customerList');
    const top=el.getBoundingClientRect().top;
    const row=[...el.children].find(n=>n.dataset.sender&&n.getBoundingClientRect().bottom>top+1);
    return {sender:row?.dataset.sender,offset:row?.getBoundingClientRect().top-top,height:el.clientHeight,scroll:el.scrollTop};
  });
}
async function anchorOffset(page,sender) {
  return page.evaluate(sender=>{
    const el=document.getElementById('customerList');
    const row=[...el.children].find(n=>n.dataset.sender===sender);
    return row?.getBoundingClientRect().top-el.getBoundingClientRect().top;
  },sender);
}

(async()=>{
  const browser=await chromium.launch({headless:true,channel:process.env.INBOX_QA_BROWSER||'msedge'});
  let page;
  try {
    page=await browser.newPage({viewport:{width:1440,height:960},ignoreHTTPSErrors:true});
    const errors=[];
    page.on('pageerror',e=>errors.push(e.message));
    await page.goto(base+'/preview-login',{waitUntil:'domcontentloaded'});
    await ready(page);
    await page.evaluate(()=>{clearInterval(pollConvTimer);clearInterval(statsPollTimer)});

    // An identical poll must not replace rows or steal keyboard focus.
    await page.evaluate(()=>{
      window.savedRow=document.querySelectorAll('#customerList .customer-item')[2];
      savedRow.focus({preventScroll:true});window.rowMutations=0;
      window.rowObserver=new MutationObserver(records=>rowMutations+=records.filter(r=>r.type==='childList').length);
      rowObserver.observe(document.getElementById('customerList'),{childList:true,subtree:true});
    });
    await page.evaluate(()=>loadConversations(false));
    assert.equal(await page.evaluate(()=>savedRow===document.querySelectorAll('#customerList .customer-item')[2]),true);
    assert.equal(await page.evaluate(()=>document.activeElement===savedRow),true);
    assert.equal(await page.evaluate(()=>rowMutations),0);
    await page.evaluate(()=>rowObserver.disconnect());
    results.push('Identical polling preserves row identity, focus and child DOM');

    // This is the reported regression: filtered pages used to drop from 40 to 20.
    await filter(page,'problems');
    await page.locator('#loadMoreConversationsButton').click();
    await page.waitForFunction(()=>!inboxRequest&&allConversations.length===40);
    await page.evaluate(()=>{document.getElementById('customerList').scrollTop=450});
    await page.waitForTimeout(250);
    const anchor=await visibleAnchor(page);
    await page.evaluate(()=>loadConversations(false));
    assert.equal(await page.evaluate(()=>allConversations.length),40);
    assert.ok(Math.abs(await anchorOffset(page,anchor.sender)-anchor.offset)<=1);
    const heights=await page.evaluate(async()=>{
      const el=document.getElementById('customerList'),values=[];
      for(let i=0;i<12;i++) {el.scrollTop+=i%2?24:-20;await new Promise(requestAnimationFrame);values.push(el.clientHeight)}
      return values;
    });
    assert.equal(new Set(heights).size,1,'Scrolling must never hide filters or resize the list');
    assert.equal(await page.locator('#inboxFilterPanel').evaluate(el=>el.inert),false);
    results.push('Filtered 40-row list and reading anchor survive polling; tools never oscillate');

    // A new arrival above the viewport should not move the conversation being read.
    const beforeNew=await visibleAnchor(page);
    const bumped=await page.evaluate(async()=>{
      const loaded=new Set(allConversations.map(c=>c.sender_id));
      const sender=Array.from({length:70},(_,i)=>`qa-${String(300-i*3).padStart(4,'0')}`).find(id=>!loaded.has(id));
      const r=await fetch('/preview/bump',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({sender})});
      if(!r.ok)throw Error('Synthetic bump failed');return sender;
    });
    await page.evaluate(()=>loadConversations(false));
    assert.equal(await page.evaluate(id=>allConversations[0].sender_id===id,bumped),true);
    assert.ok(Math.abs(await anchorOffset(page,beforeNew.sender)-beforeNew.offset)<=1);
    results.push('New head arrival preserves the visible conversation and pixel offset');

    // More than the server's 100-row page cap must remain loaded after refresh.
    await filter(page,'all');
    for(let i=0;i<5;i++) {await page.locator('#loadMoreConversationsButton').click();await ready(page)}
    assert.equal(await page.evaluate(()=>allConversations.length),120);
    await page.evaluate(()=>loadConversations(false));
    assert.equal(await page.evaluate(()=>allConversations.length),120);
    assert.equal(await page.evaluate(()=>new Set(allConversations.map(c=>c.sender_id)).size),120);
    results.push('120 loaded conversations survive the API page cap without duplicates');

    await filter(page,'all');
    const bottomAnchor=await page.evaluate(()=>{
      const el=document.getElementById('customerList');
      el.scrollTop=el.scrollHeight-el.clientHeight-20;
      return captureInboxPosition(el);
    });
    await page.waitForFunction(()=>!inboxRequest&&allConversations.length===40);
    assert.ok(Math.abs(await anchorOffset(page,bottomAnchor.anchors[0].sender)-bottomAnchor.anchors[0].offset)<=1);
    results.push('Scrolling near the bottom loads older rows without moving the reading anchor');

    await page.locator('.inbox-filters summary').click();
    for(const sort of ['oldest','score']) {
      await page.locator('#filterSort').selectOption(sort);await ready(page);
      await page.locator('#loadMoreConversationsButton').click();await ready(page);
      await page.evaluate(()=>loadConversations(false));
      assert.equal(await page.evaluate(()=>allConversations.length),40);
      assert.equal(await page.evaluate(sort=>allConversations.every((row,index)=>!index||compareInboxRows(allConversations[index-1],row,sort)<=0),sort),true);
      results.push(`${sort} ordering preserves loaded pages and their sort order`);
    }
    await page.locator('#filterSort').selectOption('latest');await ready(page);
    await page.locator('.inbox-filters summary').click();

    // The poll cannot abort an in-flight older page.
    await filter(page,'all');
    let delayNext=true;
    const slowPage=async route=>{
      const url=new URL(route.request().url());
      if(url.searchParams.has('cursor_sender')&&delayNext) {delayNext=false;await new Promise(r=>setTimeout(r,500))}
      await route.continue();
    };
    await page.route('**/api/conversations?**',slowPage);
    await page.evaluate(()=>{window.morePromise=loadConversations(false,true)});
    await page.waitForFunction(()=>isLoadingMoreConv);
    await page.evaluate(()=>loadConversations(false));
    await page.evaluate(()=>morePromise);
    assert.equal(await page.evaluate(()=>allConversations.length),40);
    await page.unroute('**/api/conversations?**',slowPage);
    results.push('Background polling cannot cancel older-page loading');

    // A failed page keeps the current rows and retries from the same boundary.
    let failNext=true;
    const failPage=async route=>{
      if(new URL(route.request().url()).searchParams.has('cursor_sender')&&failNext) {
        failNext=false;await route.fulfill({status:503,contentType:'application/json',body:'{"error":"synthetic outage"}'});
      } else await route.continue();
    };
    await page.route('**/api/conversations?**',failPage);
    await page.evaluate(()=>{window.firstBeforeFailure=document.querySelector('#customerList .customer-item')});
    await page.locator('#loadMoreConversationsButton').click();
    await page.waitForFunction(()=>!inboxRequest&&Boolean(inboxLoadError));
    assert.equal(await page.evaluate(()=>allConversations.length),40);
    assert.equal(await page.evaluate(()=>firstBeforeFailure===document.querySelector('#customerList .customer-item')),true);
    await page.locator('#loadMoreConversationsButton').click();
    await page.waitForFunction(()=>!inboxRequest&&allConversations.length===60);
    await page.unroute('**/api/conversations?**',failPage);
    results.push('Pagination failure preserves rows; explicit retry loads the next page');

    let pages=0;
    while(await page.evaluate(()=>hasMoreConv)) {
      assert.ok(pages++<20,'Pagination must finish');
      await page.locator('#loadMoreConversationsButton').click();await ready(page);
    }
    assert.equal(await page.evaluate(()=>allConversations.length),320);
    assert.equal(await page.evaluate(()=>new Set(allConversations.map(c=>c.sender_id)).size),320);
    await page.evaluate(()=>loadConversations(false));
    assert.equal(await page.evaluate(()=>allConversations.length),320);
    assert.equal(await page.locator('#loadMoreConversationsButton').isDisabled(),true);
    results.push('All 320 conversations can be reached once, and polling preserves the completed list');

    // Abort an old search while it is waiting; its result must never win later.
    const slowSearch=async route=>{
      if(new URL(route.request().url()).searchParams.get('q')==='مريم')await new Promise(r=>setTimeout(r,900));
      try {await route.continue()}catch(_){}
    };
    await page.route('**/api/conversations?**',slowSearch);
    await page.locator('#searchInput').fill('مريم');
    await page.waitForFunction(()=>Boolean(inboxRequest));
    await page.locator('#searchInput').fill('هبة');
    await page.waitForFunction(()=>!inboxRequest&&allConversations.length>0&&allConversations.every(c=>c.name.includes('هبة')));
    await page.waitForTimeout(1000);
    assert.equal(await page.evaluate(()=>allConversations.every(c=>c.name.includes('هبة'))),true);
    assert.equal(await page.locator('#customerList').evaluate(el=>el.inert),false);
    await page.unroute('**/api/conversations?**',slowSearch);
    results.push('Rapid searches discard stale responses and release the loading state');

    // A failed filter change retries that filter, rather than appending an old page.
    let failFilter=true;
    const failedFilter=async route=>{
      if(new URL(route.request().url()).searchParams.get('q')==='سارة'&&failFilter) {
        failFilter=false;await route.fulfill({status:503,body:'{}',contentType:'application/json'});
      } else await route.continue();
    };
    await page.route('**/api/conversations?**',failedFilter);
    await page.locator('#searchInput').fill('سارة');
    await page.waitForFunction(()=>!inboxRequest&&Boolean(inboxLoadError));
    await page.locator('#loadMoreConversationsButton').click();
    await page.waitForFunction(()=>!inboxRequest&&allConversations.length>0&&allConversations.every(c=>c.name.includes('سارة')));
    await page.unroute('**/api/conversations?**',failedFilter);
    results.push('A failed filter retries the intended search instead of paginating stale results');

    let failBackground=true;
    const failedBackgroundFilter=async route=>{
      if(new URL(route.request().url()).searchParams.get('q')==='زينب'&&failBackground) {
        failBackground=false;await route.fulfill({status:503,body:'{}',contentType:'application/json'});
      } else await route.continue();
    };
    await page.route('**/api/conversations?**',failedBackgroundFilter);
    await page.locator('#searchInput').fill('زينب');
    await page.waitForFunction(()=>!inboxRequest&&Boolean(inboxLoadError));
    await page.evaluate(()=>loadConversations(false));
    assert.equal(await page.evaluate(()=>allConversations.length>0&&allConversations.every(c=>c.name.includes('زينب'))),true);
    await page.unroute('**/api/conversations?**',failedBackgroundFilter);
    results.push('Background recovery after a failed filter never mixes unrelated cached results');

    // Explicit selection keeps row focus; empty and terminal states remain usable.
    const selected=await page.locator('#customerList .customer-item').first().getAttribute('data-sender');
    await page.locator('#customerList .customer-item').first().press('Enter');
    await page.waitForFunction(id=>currentSenderId===id,selected);
    assert.equal(await page.locator(`#customerList [data-sender="${selected}"]`).getAttribute('aria-pressed'),'true');
    await page.locator('#searchInput').fill('اسم غير موجود للاختبار');
    await page.waitForFunction(()=>!inboxRequest&&allConversations.length===0);
    assert.equal(await page.locator('#customerList .inbox-empty').count(),1);
    assert.equal(await page.locator('#loadMoreConversationsButton').isDisabled(),true);
    results.push('Keyboard selection, empty search and terminal pagination are accessible');
    assert.deepEqual(errors,[]);
    await page.close();page=null;

    for(const width of [1440,1024,768,390,360,320]) {
      page=await browser.newPage({viewport:{width,height:width>768?960:844},ignoreHTTPSErrors:true});
      await page.goto(base+'/preview-login',{waitUntil:'domcontentloaded'});await ready(page);
      await page.evaluate(()=>{clearInterval(pollConvTimer);clearInterval(statsPollTimer)});
      const layout=await page.evaluate(()=>({pageWidth:document.documentElement.scrollWidth,width:innerWidth,
        listHeight:document.getElementById('customerList').clientHeight,
        panelWidth:document.getElementById('sidebarPanel').clientWidth,
        listWidth:document.getElementById('customerList').clientWidth,
        listScrollWidth:document.getElementById('customerList').scrollWidth}));
      assert.ok(layout.pageWidth<=width+1,JSON.stringify(layout));
      assert.ok(layout.listHeight>=180,JSON.stringify(layout));
      assert.ok(layout.listScrollWidth<=layout.listWidth+1,JSON.stringify(layout));
      await page.screenshot({path:path.join(output,`inbox-${width}.png`)});
      results.push({width,...layout});
      await page.close();page=null;
    }
    fs.writeFileSync(path.join(output,'browser-results.json'),JSON.stringify({passed:true,results},null,2));
    console.log(JSON.stringify({passed:true,checks:results.length,results},null,2));
  } catch(error) {
    if(page)await page.screenshot({path:path.join(output,'failure.png')}).catch(()=>{});
    throw error;
  } finally {await browser.close()}
})().catch(error=>{console.error(error);process.exitCode=1});
