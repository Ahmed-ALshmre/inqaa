/* Run with NODE_PATH pointing at a Playwright installation. No live account,
   server, customer data, or outbound API mutations are used. */
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {execFileSync} = require('node:child_process');
const {chromium, devices} = require('playwright');
const root = path.resolve(__dirname, '../..');
const output = path.join(root, 'outputs/mobile-app-review');
fs.mkdirSync(output, {recursive:true});
const rendered = JSON.parse(execFileSync(process.env.PYTHON || 'python', ['-c', `
import json,sys
from pathlib import Path
from flask import Flask,render_template
root=Path(sys.argv[1])/'account_app'
app=Flask(__name__,template_folder=str(root/'templates'))
owner=dict(name='فريق صوف',owner=True,id=0,permissions=[])
@app.context_processor
def context():return dict(staff_person=owner,can=lambda x:True,csrf_token='preview-only')
with app.test_request_context('/'):
    for name in app.jinja_env.list_templates(): app.jinja_env.get_template(name)
    pages={route:render_template(name) for route,name in {'dashboard':'dashboard.html','settings':'settings/index.html','orders':'orders.html','products':'products.html','advisor':'advisor.html','catalog':'catalog.html','problems':'problems.html'}.items()}
    pages['login']=render_template('login.html',staff_person=None,can=lambda x:False)
    pages['employee']=render_template('dashboard.html',staff_person=dict(name='موظف',owner=False,id=1,permissions=['products']),can=lambda x:x=='products')
    print(json.dumps(pages))
`, root], {encoding:'utf8',maxBuffer:8*1024*1024}));
const fixtures = {
  conversations:{conversations:[{sender_id:'preview-0',name:'زبونة تجريبية',store_name:'لمسة ستور',store_id:'default',platform:'facebook',last_text:'ممكن الألوان المتوفرة؟',last_time:'2026-10-25T14:30:00',ai_enabled:true}],has_more:false},
  stores:{stores:[{store_id:'default',name:'لمسة ستور',ai_enabled:true}]},
  orders:{orders:[],total:0,new_count:0,people_count:0,has_more:false,next_offset:0},
  me:{owner:true,name:'فريق صوف',permissions:[]}
};
const fallback={ok:true,enabled:true,ai_enabled:true,items:[],reviews:[],problems:[],memory:[],proposals:[],stats:{},messages:[],products:[]};
async function routeLocal(context) {
  await context.route('**/*',async route=>{
    const url=new URL(route.request().url());
    if(url.hostname!=='soof.test'){
      const cached=path.join(output,'vendor',Buffer.from(url.href).toString('base64url'));
      if(fs.existsSync(cached+'.json'))return route.fulfill({body:fs.readFileSync(cached),contentType:JSON.parse(fs.readFileSync(cached+'.json')).type});
      // Styling assertions below test our local styles; public vendor files are
      // optional. Bootstrap dialogs are not used by these interaction tests.
      return route.fulfill({body:'',contentType:url.pathname.endsWith('.js')?'application/javascript':'text/css'});
    }
    if(url.pathname.startsWith('/api/')){
      assert.equal(route.request().method(),'GET','Preview must never mutate account data');
      return route.fulfill({json:fixtures[url.pathname.slice(5)]||fallback});
    }
    if(url.pathname.startsWith('/static/')){
      const file=path.join(root,'account_app/static',url.pathname.slice(8));
      if(!fs.existsSync(file))return route.fulfill({status:404,body:''});
      const type={'.js':'application/javascript','.css':'text/css','.png':'image/png'}[path.extname(file)]||'application/octet-stream';
      return route.fulfill({body:fs.readFileSync(file),contentType:type});
    }
    if(url.pathname==='/manifest.webmanifest')return route.fulfill({body:fs.readFileSync(path.join(root,'account_app/static/manifest.webmanifest')),contentType:'application/manifest+json'});
    return route.fulfill({body:rendered[url.pathname.slice(1)]||'',contentType:'text/html'});
  });
}
async function phone(browser, options={}, init) {
  const context=await browser.newContext({...devices['Pixel 7'],serviceWorkers:'block',...options});
  if(init)await context.addInitScript(init);
  await routeLocal(context);
  const page=await context.newPage();
  page.on('pageerror',error=>{throw error;});
  return {context,page};
}
async function open(page,name='dashboard') {await page.goto('https://soof.test/'+name,{waitUntil:'networkidle'});}
const visible=(page,selector)=>page.locator(selector).first().isVisible();
(async()=>{
  const browser=await chromium.launch({headless:true,...(process.env.BROWSER_EXECUTABLE?{executablePath:process.env.BROWSER_EXECUTABLE}:{})});
  let checks=0;
  try {
    // Phone layout and usable bottom navigation at small and large widths.
    for(const width of [320,360,390,430]){
      const {context,page}=await phone(browser,{viewport:{width,height:844},screen:{width,height:844}});
      for(const name of ['dashboard','settings','orders','products','advisor','login','catalog','problems']){
        await open(page,name);
        assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,`${name} overflows at ${width}`);
        assert.equal(await visible(page,'.app-device-gate'),false);
        if(name==='login')assert.equal(await page.locator('.app-tabbar').count(),0);
        else assert.equal(await visible(page,'.app-tabbar'),true);
        if(['dashboard','settings','orders','products','advisor'].includes(name)){
          const rect=await page.locator('.app-install-topbar').boundingBox();
          assert(rect.x>=0 && rect.x+rect.width<=width+1,`${name} install action outside phone`);
        }
        if(width===390)await page.screenshot({path:path.join(output,`phone-${name}.png`),scale:'css'});
        checks++;
      }
      await context.close();
    }
    // A phone requesting a desktop user-agent retains its phone layout.
    {
      const {context,page}=await phone(browser,{userAgent:'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/130.0 Safari/537.36'});
      await open(page);assert.equal(await visible(page,'.app-tabbar'),true);assert.equal(await visible(page,'.app-device-gate'),false);checks++;await context.close();
    }
    // Desktop support is blocked even when the window is resized to phone width.
    for(const width of [1440,390]){
      const context=await browser.newContext({viewport:{width,height:900},serviceWorkers:'block'});await routeLocal(context);
      const page=await context.newPage();await open(page);
      assert.equal(await visible(page,'.app-device-gate'),true);assert.equal(await visible(page,'.main-container'),false);
      assert.equal(await visible(page,'[data-app-install]'),false);checks++;await context.close();
    }
    // iPhone manual installation instructions and dismissing the promotion.
    {
      const {context,page}=await phone(browser,{...devices['iPhone 13']});await open(page,'login');
      await page.locator('[data-app-install]').first().click();
      assert.match(await page.locator('[data-app-install-steps]').innerText(),/Safari/);
      await page.locator('[data-app-close-install]').last().click();
      await page.locator('[data-app-dismiss-install]').click();await page.reload();
      assert.equal(await visible(page,'.app-install-card'),false);checks++;await context.close();
    }
    // A cancelled prompt remains retryable; appinstalled removes all promotions.
    {
      const {context,page}=await phone(browser);await open(page);
      await page.evaluate(()=>{window.promptCalls=0;const e=new Event('beforeinstallprompt',{cancelable:true});e.prompt=async()=>{window.promptCalls++};e.userChoice=Promise.resolve({outcome:'dismissed'});window.dispatchEvent(e);});
      await page.locator('.app-install-topbar').click();await page.locator('[data-app-native-install]').click();
      assert.equal(await page.evaluate(()=>window.promptCalls),1);
      assert.equal(await visible(page,'[data-app-native-install]'),false);
      await page.locator('[data-app-close-install]').last().click();
      await page.evaluate(()=>window.dispatchEvent(new Event('appinstalled')));
      assert.equal(await visible(page,'.app-install-card'),false);assert.equal(await visible(page,'.app-install-topbar'),false);checks++;await context.close();
    }
    // Installed launch locks portrait; unsupported rotation preserves the draft.
    {
      const {context,page}=await phone(browser,{},()=>{
        const original=window.matchMedia;
        window.matchMedia=function(query){const result=original.call(this,query);if(query==='(display-mode: standalone)')Object.defineProperty(result,'matches',{value:true});return result;};
        window.portraitLocks=[];window.previewOrientation='portrait-primary';
        Object.defineProperty(screen.orientation,'type',{get:()=>window.previewOrientation});
        Object.defineProperty(screen.orientation,'lock',{value:async value=>{window.portraitLocks.push(value);throw Error('Unsupported in this browser');}});
      });
      await open(page);assert.equal(await visible(page,'.app-install-card'),false);
      assert((await page.evaluate(()=>window.portraitLocks)).includes('portrait-primary'));
      await page.locator('.customer-item').first().click();
      await page.locator('#messageInput').fill('مسودة تبقى بعد تدوير الهاتف');
      assert.equal(await visible(page,'.app-tabbar'),false);
      await page.evaluate(()=>{window.previewOrientation='landscape-primary';screen.orientation.dispatchEvent(new Event('change'));});
      await page.waitForFunction(()=>document.documentElement.classList.contains('app-landscape'));
      assert.equal(await visible(page,'.app-orientation-gate'),true);assert.equal(await visible(page,'.main-container'),false);
      await page.evaluate(()=>{window.previewOrientation='portrait-primary';screen.orientation.dispatchEvent(new Event('change'));});
      await page.waitForFunction(()=>!document.documentElement.classList.contains('app-landscape'));
      assert.equal(await page.locator('#messageInput').inputValue(),'مسودة تبقى بعد تدوير الهاتف');
      assert.equal(await page.locator('#messageInput').evaluate(el=>el.closest('[inert]')),null);
      await page.locator('.mobile-chat-back').click();await page.waitForFunction(()=>!document.body.classList.contains('mobile-conversation-open'));
      assert.equal(await visible(page,'.app-tabbar'),true);checks++;await context.close();
    }
    // The keyboard does not trigger the physical-orientation fallback.
    {
      const {context,page}=await phone(browser,{},()=>{
        window.previewHeight=839;
        Object.defineProperty(window.visualViewport,'height',{get:()=>window.previewHeight});
        Object.defineProperty(screen.orientation,'type',{get:()=> 'portrait-primary'});
      });
      await open(page);await page.locator('.customer-item').first().click();await page.locator('#messageInput').focus();
      await page.evaluate(()=>{window.previewHeight=470;visualViewport.dispatchEvent(new Event('resize'));});
      await page.waitForFunction(()=>document.body.classList.contains('app-keyboard-open'));
      assert.equal(await visible(page,'.app-orientation-gate'),false);
      assert((await page.locator('.main-container').boundingBox()).height<=406);checks++;await context.close();
    }
    // Administration menus and page action trays retain their actions.
    {
      const {context,page}=await phone(browser);await open(page,'settings');
      assert.equal(await page.locator('#adminSidebar').evaluate(el=>el.inert),true);
      await page.locator('.admin-menu-btn').click();await page.waitForFunction(()=>!document.querySelector('#adminSidebar').inert);
      assert.equal(await visible(page,'#adminSidebar a[href^="/logout"]'),true);
      await page.keyboard.press('Escape');await page.waitForFunction(()=>document.querySelector('#adminSidebar').inert);
      await open(page,'products');assert.equal(await visible(page,'.nav-actions'),false);
      await page.locator('.app-page-more').click();assert.equal(await visible(page,'.nav-actions'),true);
      assert.equal(await visible(page,'button[onclick="exportProducts()"]'),true);
      await page.keyboard.press('Escape');assert.equal(await visible(page,'.nav-actions'),false);
      await open(page,'employee');assert.equal(await page.locator('.app-tabbar a').count(),2);checks++;await context.close();
    }
    const manifest=JSON.parse(fs.readFileSync(path.join(root,'account_app/static/manifest.webmanifest')));
    assert.equal(manifest.orientation,'portrait-primary');assert.equal(manifest.display,'standalone');checks++;
    console.log(`PASS: ${checks} phone, install, portrait, keyboard, navigation and template checks`);
  } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
