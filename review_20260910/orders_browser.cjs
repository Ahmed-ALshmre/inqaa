const {chromium}=require('playwright');
const assert=require('node:assert/strict');
(async()=>{
 const browser=await chromium.launch({headless:true,channel:"msedge"});
 const results=[];
 for(const width of [360,390,768,1440]){
  const page=await browser.newPage({viewport:{width,height:820},deviceScaleFactor:1});
  const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.goto('http://127.0.0.1:5101/preview-login');
  await page.waitForFunction(()=>document.querySelectorAll('#ordersBody tr').length===100);
  assert.equal(await page.locator('#ordersTotal').innerText(),'125');
  await page.locator('#ordersMore').click();
  await page.waitForFunction(()=>document.querySelectorAll('#ordersBody tr').length===125);
  await page.evaluate(()=>window.scrollTo(0,document.documentElement.scrollHeight));
  const layout=await page.evaluate(()=>({width:innerWidth,docWidth:document.documentElement.scrollWidth,scroll:scrollY,remaining:document.documentElement.scrollHeight-innerHeight-scrollY,last:document.querySelector('#ordersBody tr:last-child').getBoundingClientRect().bottom}));
  assert.ok(layout.docWidth<=width+1,JSON.stringify(layout));
  assert.ok(layout.scroll>0 && Math.abs(layout.remaining)<3,JSON.stringify(layout));
  assert.ok(layout.last<=820,JSON.stringify(layout));
  await page.screenshot({path:`review_20260910/orders-${width}-bottom.png`});
  await page.locator('#ordersBody tr:last-child button').first().click();
  await page.locator('#orderEditModal.show').waitFor();
  await page.locator('#editDeliveryFee').fill('0');
  await page.locator('#orderEditForm button[type=submit]').click();
  await page.waitForFunction(()=>!document.getElementById('orderEditModal').classList.contains('show'));
  await page.waitForFunction(()=>document.querySelectorAll('#ordersBody tr').length===100);
  await page.locator('#orderSearch').fill('preview-0');
  await page.waitForFunction(()=>document.querySelectorAll('#ordersBody tr').length===1 && document.getElementById('ordersBody').textContent.includes('preview-0'));
  assert.ok((await page.locator('#ordersBody').innerText()).includes('24,000'));
  await page.evaluate(()=>window.scrollTo(0,0));
  await page.screenshot({path:`review_20260910/orders-${width}.png`});
  assert.deepEqual(errors,[]);
  results.push({width,...layout,checks:'pagination, totals, scrolling, responsive width, edit, search passed'});
  await page.close();
 }
 await browser.close();console.log(JSON.stringify(results,null,2));
})().catch(e=>{console.error(e);process.exit(1)});

