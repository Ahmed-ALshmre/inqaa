const fs=require('fs'),vm=require('vm'),assert=require('assert');
const source=fs.readFileSync('account_app/static/js/settings_pages.js','utf8');
const code=source.slice(source.indexOf('let fullRestoreBusy'),source.indexOf('async function restoreProductsBackup'));
async function check(fail){
const nodes={};const get=id=>nodes[id]??=( {value:0,hidden:true,removeAttribute(){delete this.value;}} );
const input=get('fullRestoreInput');input.files=[{name:'backup.zip',size:2048}];
let count=0;const ctx={document:{getElementById:get},window:{addEventListener(){},removeEventListener(){}},location:{search:''},confirm:()=>true,URLSearchParams,FormData:class{append(){}},XMLHttpRequest:class{
 constructor(){this.upload={};}open(){}send(){count++;this.upload.onprogress({lengthComputable:true,loaded:50,total:100});assert.equal(get('restoreProgress').value,50);this.upload.onload();assert.equal(get('restoreProgress').value,undefined);if(fail){this.onerror();return;}this.status=200;this.responseText=JSON.stringify({ok:true,products:5,images:2});this.onload();}
}};vm.createContext(ctx);vm.runInContext(code,ctx);await ctx.restoreFullBackup(input);assert.equal(count,1);assert.equal(input.disabled,false);if(!fail){assert.equal(get('restoreProgress').value,100);assert.equal(get('restoreDone').hidden,false);}else assert.match(get('restoreStatus').textContent,/انقطع الاتصال/);
}
(async()=>{await check(false);await check(true);console.log('Upload progress, processing state, success and connection failure verified');})();
