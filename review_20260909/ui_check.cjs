const {chromium}=require('playwright');
const assert=require('node:assert/strict');
const path=require('node:path');
(async()=>{
  const browser=await chromium.launch({channel:'chrome',headless:true});
  const context=await browser.newContext({viewport:{width:1440,height:960}});
  const username='sara'+Date.now(); const page=await context.newPage(); const errors=[];page.on('pageerror',e=>errors.push(e.message));
  await page.goto('http://127.0.0.1:5099/login');await page.locator('#password').fill('local-preview-only');await page.locator('.login-submit').click();
  await page.locator('.customer-item').first().waitFor();await page.locator('.customer-item').first().click();
  await page.locator('#chatContent').waitFor({state:'visible'});
  await page.screenshot({path:path.join(__dirname,'dashboard-desktop.png')});
  assert(await page.locator('.platform-badge').count()>0);
  await page.getByRole('button',{name:'بيانات وأدوات'}).click();await page.locator('#customerToolsDialog').waitFor({state:'visible'});await page.keyboard.press('Escape');
  await page.getByRole('button',{name:'إرسال صورة'}).click();await page.locator('#imageChooseDialog').waitFor({state:'visible'});await page.keyboard.press('Escape');
  await page.getByRole('button',{name:'تثبيت طلب'}).click();await page.locator('#orderModal').waitFor({state:'visible'});await page.keyboard.press('Escape');
  await page.goto('http://127.0.0.1:5099/settings');await page.screenshot({path:path.join(__dirname,'settings-desktop.png')});
  await page.locator('#settingsSearch').fill('الموظفون');assert.equal(await page.locator('.admin-link-card:visible').count(),1);
  await page.goto('http://127.0.0.1:5099/settings/staff');await page.getByRole('button',{name:'إضافة موظف'}).click();await page.locator('#staffName').fill('سارة');await page.locator('#staffUsername').fill(username);await page.locator('#staffPassword').fill('preview-staff-password');await page.locator('#staffSave').click();await page.locator('#staffDialog').waitFor({state:'hidden'});await page.getByRole('heading',{name:'سارة'}).waitFor();
  await page.goto('http://127.0.0.1:5099/advisor');await page.locator('.advisor-message').first().waitFor();
  await page.screenshot({path:path.join(__dirname,'advisor-desktop.png')});
  for(const width of [390,360]){
    await page.setViewportSize({width,height:844});
    await page.reload();await page.locator('.advisor-message').first().waitFor();
    const metrics=await page.evaluate(()=>{const input=document.querySelector('#advisorInput').getBoundingClientRect();const messages=document.querySelector('#advisorMessages');return {bottom:input.bottom,top:input.top,height:innerHeight,overflow:document.documentElement.scrollWidth>innerWidth,messagesScrollable:messages.scrollHeight>messages.clientHeight,scroll:messages.scrollTop};});
    console.log('mobile',width,metrics);assert(metrics.bottom<=metrics.height && metrics.top>0,'Composer visible');assert(!metrics.overflow,'No horizontal overflow');assert(metrics.messagesScrollable,'Messages scroll');
    await page.getByRole('button',{name:'آخر رسالة'}).click();
    assert(await page.locator('#advisorMessages').evaluate(el=>Math.abs(el.scrollHeight-el.clientHeight-el.scrollTop)<3));
    if(width===390)await page.screenshot({path:path.join(__dirname,'advisor-mobile.png')});
    await page.getByRole('button',{name:'الذاكرة'}).click();await page.locator('#advisorMemoryDialog').waitFor({state:'visible'});await page.keyboard.press('Escape');
  }
  await page.setViewportSize({width:390,height:480});await page.locator('#advisorInput').fill('سؤال تجريبي');
  assert(await page.locator('#advisorInput').evaluate(el=>el.getBoundingClientRect().bottom<=innerHeight),'Composer visible with short viewport');
  await page.setViewportSize({width:390,height:844});await page.goto('http://127.0.0.1:5099/dashboard');await page.locator('.customer-item').first().click();await page.screenshot({path:path.join(__dirname,'dashboard-mobile.png')});
  await page.goto('http://127.0.0.1:5099/settings'); await page.screenshot({path:path.join(__dirname,'settings-mobile.png')}); console.log('JS errors',errors);assert.deepEqual(errors,[]);
  await page.goto('http://127.0.0.1:5099/logout'); await page.locator('#username').fill(username); await page.locator('#password').fill('preview-staff-password'); await page.locator('.login-submit').click();
  await page.locator('.customer-item').first().waitFor(); await page.locator('.customer-item').first().click();
  assert.equal(await page.locator('button[onclick="openOrderModal()"]:visible').count(),0);
  assert(await page.locator('#sendBtn').isVisible());
  const checks=await page.evaluate(async()=>{
    const forbidden=await fetch('/api/staff');
    const edited=await fetch('/api/conversations/al-fatena::preview-0/customer',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:'زبونة تجريبية'})});
    return {forbidden:forbidden.status,edited:edited.status};
  });
  assert.deepEqual(checks,{forbidden:403,edited:200});
  await page.screenshot({path:path.join(__dirname,'employee-mobile.png')});
  await browser.close(); console.log('PASS UI desktop, mobile, dialogs, staff creation, advisor scroll and composer');
})().catch(e=>{console.error(e);process.exit(1)});
