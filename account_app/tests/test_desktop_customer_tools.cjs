const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../static/js/conversation_tools.js'),'utf8');
function container(){return {children:[],append(node){if(node.parent)node.parent.children=node.parent.children.filter(item=>item!==node);this.children.push(node);node.parent=this;}};}
const panel={},desktop=container(),dialog={...container(),open:false,close(){this.open=false;}};
const query={matches:false,addEventListener(event,fn){this.change=fn;}};
const context=vm.createContext({
  document:{getElementById:id=>id==='controlPanel'?panel:id==='desktopCustomerTools'?desktop:null,addEventListener(event,fn){fn();}},
  toolDialog:()=>dialog,matchMedia:()=>query,
});
vm.runInContext(source.slice(source.indexOf("document.addEventListener('DOMContentLoaded'"),source.indexOf('async function openCatalogDialog(')),context);
assert.equal(panel.parent,dialog);assert.equal(desktop.hidden,true);
dialog.open=true;query.matches=true;query.change();
assert.equal(dialog.open,false);assert.equal(panel.parent,desktop);assert.equal(desktop.hidden,false);
query.matches=false;query.change();assert.equal(panel.parent,dialog);assert.equal(desktop.hidden,true);
assert.equal(dialog.children.filter(node=>node===panel).length,1,'One live control panel moves between layouts without duplicate customer fields');
console.log('PASS: desktop inspector and mobile dialog retain the same customer controls across resizing');
