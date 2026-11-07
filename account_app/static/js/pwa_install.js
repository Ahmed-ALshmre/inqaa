(() => {
  const dialog=document.getElementById('mobileInstallDialog');
  if(!dialog || typeof dialog.showModal!=='function' || window.soofPWA)return;
  const action=document.getElementById('mobileInstallAction'),guide=document.getElementById('mobileInstallGuide');
  const status=document.getElementById('mobileInstallStatus');
  let deferred=null,busy=false,shown=false;
  const read=key=>{try{return localStorage.getItem(key);}catch{return null;}};
  const write=(key,value)=>{try{localStorage.setItem(key,String(value));}catch{}};
  const standalone=()=>navigator.standalone===true||matchMedia('(display-mode: standalone)').matches;
  const ios=()=>/iPad|iPhone|iPod/.test(navigator.userAgent)||(navigator.platform==='MacIntel'&&navigator.maxTouchPoints>1);
  const mobile=()=>/Android|iPhone|iPad|iPod/.test(navigator.userAgent)||(navigator.maxTouchPoints>0&&matchMedia('(max-width: 1024px)').matches);
  function refresh(){action.textContent=deferred?'تثبيت التطبيق الآن ↓':'طريقة التثبيت ↗';}
  function close(snooze=true){if(snooze)write('soof.install.snooze',Date.now()+86400000);if(dialog.open)dialog.close();}
  function open(force=false){
    if(standalone()||(!force&&(!mobile()||shown||read('soof.install.installed')==='1'||Number(read('soof.install.snooze'))>Date.now())))return;
    if(document.querySelector('dialog[open],.modal.show'))return;
    refresh();if(!dialog.open){dialog.showModal();shown=true;}
  }
  async function install(){
    if(busy)return;
    if(!deferred){
      guide.hidden=false;
      guide.textContent=ios()?'من زر المشاركة في المتصفح، اختر «إضافة إلى الشاشة الرئيسية»، ثم اضغط «إضافة». إذا لم يظهر الخيار، افتح الرابط في Safari.':'افتح قائمة المتصفح ⋮ ثم اختر «تثبيت التطبيق» أو «إضافة إلى الشاشة الرئيسية»، وأكّد الإضافة. إذا لم يظهر الخيار، افتح الموقع في Chrome أو Edge.';
      guide.focus();return;
    }
    const prompt=deferred;deferred=null;busy=true;action.disabled=true;status.textContent='';
    try{
      await prompt.prompt();
      const choice=await prompt.userChoice;
      if(choice?.outcome==='accepted'){status.textContent='تم قبول طلب التثبيت.';close(false);}
      else close(true);
    }catch{status.textContent='تعذر فتح التثبيت الآن، جرّب طريقة التثبيت من قائمة المتصفح.';}
    finally{busy=false;action.disabled=false;refresh();}
  }
  window.soofPWA={open,install,dismiss:()=>close(true)};
  window.addEventListener('beforeinstallprompt',event=>{
    event.preventDefault();deferred=event;write('soof.install.installed','0');refresh();
    const button=document.getElementById('pwaInstallBtn');if(button)button.style.display='';
    open(false);
  });
  window.addEventListener('appinstalled',()=>{
    deferred=null;write('soof.install.installed','1');close(false);
    const button=document.getElementById('pwaInstallBtn');if(button)button.style.display='none';
  });
  dialog.addEventListener('cancel',()=>close(true));
  document.getElementById('mobileInstallClose').onclick=()=>close(true);
  document.getElementById('mobileInstallLater').onclick=()=>close(true);
  action.onclick=install;
  function boot(){
    const button=document.getElementById('pwaInstallBtn');if(button&&!standalone())button.style.display='';
    setTimeout(()=>open(false),1000);
    if('serviceWorker' in navigator)navigator.serviceWorker.register('/sw.js').catch(()=>{});
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot,{once:true});else boot();
})();
