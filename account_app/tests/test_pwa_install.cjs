/* Installation controller acceptance without an OS prompt or external requests. */
const assert=require('node:assert/strict');
const vm=require('node:vm'),fs=require('node:fs'),path=require('node:path');
const source=fs.readFileSync(path.join(__dirname,'../static/js/pwa_install.js'),'utf8');
function setup(options={}){
  const handlers={},timers=[],saved=new Map(Object.entries(options.saved||{}));
  const nodes=Object.fromEntries(['mobileInstallDialog','mobileInstallAction','mobileInstallGuide','mobileInstallStatus','mobileInstallClose','mobileInstallLater','pwaInstallBtn'].map(id=>[id,{style:{},textContent:'',hidden:true,disabled:false,focus(){this.focused=true;}}]));
  const dialog=nodes.mobileInstallDialog;dialog.open=false;
  dialog.showModal=options.unsupported?undefined:()=>{dialog.open=true;};
  dialog.close=()=>{dialog.open=false;};dialog.addEventListener=(name,fn)=>{handlers['dialog:'+name]=fn;};
  const window={addEventListener:(name,fn)=>{handlers[name]=fn;}};
  const context={window,document:{readyState:'complete',getElementById:id=>nodes[id],querySelector:()=>options.otherModal||dialog.open},
    navigator:{userAgent:options.desktop?'Desktop':options.ios?'iPhone':'Android',standalone:!!options.standalone,maxTouchPoints:options.desktop?0:1},
    matchMedia:()=>({matches:false}),setTimeout:fn=>timers.push(fn),Date,
    localStorage:{getItem:key=>{if(options.storageDenied)throw Error('blocked');return saved.get(key)||null;},setItem:(key,value)=>{if(options.storageDenied)throw Error('blocked');saved.set(key,value);}}};
  vm.runInNewContext(source,context);
  return {nodes,dialog,saved,window,handlers,boot:()=>timers.forEach(fn=>fn()),event:(name,value={})=>handlers[name](value)};
}
let count=0;
async function check(name,fn){await fn();count++;}
(async()=>{
  await check('mobile invitation and snooze',()=>{const t=setup();assert.equal(t.dialog.open,false);t.boot();assert.equal(t.dialog.open,true);t.nodes.mobileInstallLater.onclick();assert.equal(t.dialog.open,false);assert.ok(Number(t.saved.get('soof.install.snooze'))>Date.now());const next=setup({saved:Object.fromEntries(t.saved)});next.boot();assert.equal(next.dialog.open,false);next.window.soofPWA.open(true);assert.equal(next.dialog.open,true);});
  await check('desktop, installed and standalone suppression',()=>{for(const options of [{desktop:true},{standalone:true},{saved:{'soof.install.installed':'1'}}]){const t=setup(options);t.boot();assert.equal(t.dialog.open,false);}const t=setup({standalone:true});t.window.soofPWA.open(true);assert.equal(t.dialog.open,false);});
  await check('manual iOS and Android guidance',async()=>{for(const ios of [true,false]){const t=setup({ios});t.boot();await t.window.soofPWA.install();assert.equal(t.nodes.mobileInstallGuide.hidden,false);assert.match(t.nodes.mobileInstallGuide.textContent,ios?/Safari/:/Chrome/);assert.equal(t.nodes.mobileInstallGuide.focused,true);}});
  await check('native install needs a click and appinstalled persists success',async()=>{const t=setup();let calls=0,prevented=false;t.event('beforeinstallprompt',{preventDefault(){prevented=true;},prompt:async()=>{calls++;},userChoice:Promise.resolve({outcome:'accepted'})});assert.ok(prevented);assert.equal(calls,0);assert.match(t.nodes.mobileInstallAction.textContent,/الآن/);await t.nodes.mobileInstallAction.onclick();assert.equal(calls,1);assert.equal(t.dialog.open,false);assert.notEqual(t.saved.get('soof.install.installed'),'1');t.event('appinstalled');assert.equal(t.saved.get('soof.install.installed'),'1');assert.equal(t.nodes.pwaInstallBtn.style.display,'none');});
  await check('native cancellation snoozes',async()=>{const t=setup();t.event('beforeinstallprompt',{preventDefault(){},prompt:async()=>{},userChoice:Promise.resolve({outcome:'dismissed'})});await t.window.soofPWA.install();assert.equal(t.dialog.open,false);assert.ok(Number(t.saved.get('soof.install.snooze'))>Date.now());});
  await check('prompt failure leaves an accessible manual fallback',async()=>{const t=setup();t.event('beforeinstallprompt',{preventDefault(){},prompt:async()=>{throw Error('unavailable');}});await t.window.soofPWA.install();assert.equal(t.dialog.open,true);assert.match(t.nodes.mobileInstallStatus.textContent,/تعذر/);assert.equal(t.nodes.mobileInstallAction.disabled,false);await t.window.soofPWA.install();assert.equal(t.nodes.mobileInstallGuide.hidden,false);});
  await check('double click launches one prompt',async()=>{const t=setup();let calls=0,resolve;t.event('beforeinstallprompt',{preventDefault(){},prompt:()=>{calls++;return new Promise(r=>resolve=r);},userChoice:Promise.resolve({outcome:'dismissed'})});const first=t.window.soofPWA.install();await t.window.soofPWA.install();assert.equal(calls,1);resolve();await first;});
  await check('blocked storage and unsupported dialogs are safe',()=>{const t=setup({storageDenied:true});t.boot();assert.equal(t.dialog.open,true);t.event('dialog:cancel');assert.equal(t.dialog.open,false);const old=setup({unsupported:true});old.boot();assert.equal(old.window.soofPWA,undefined);});
  await check('an existing modal is not interrupted',()=>{const t=setup({otherModal:true});t.boot();assert.equal(t.dialog.open,false);});
  console.log(`PASS: ${count} PWA installation cases`);
})().catch(error=>{console.error(error);process.exitCode=1;});
