const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const code = fs.readFileSync(path.join(__dirname,'../static/js/voice_composer.js'),'utf8');
function setup() {
  const nodes=new Map(),permissions=[],recorders=[],requests=[],revoked=[],loaded=[],timers=[],toasts=[];
  let now=1000;
  const classes=()=>({values:new Set(),toggle(name,on){if(on)this.values.add(name);else this.values.delete(name);}});
  const inputArea={classList:classes()};
  function node(id){
    if(!nodes.has(id))nodes.set(id,{textContent:'',innerHTML:'',hidden:true,dataset:{},classList:classes(),value:id==='messageInput'?'unsent text':'',paused:true,currentTime:0,duration:NaN,listeners:{},
      setAttribute(name,value){this[name]=value;},removeAttribute(name){delete this[name];},closest(){return inputArea;},blur(){},
      addEventListener(name,fn){(this.listeners[name]||=[]).push(fn);},
      pause(){this.paused=true;(this.listeners.pause||[]).forEach(fn=>fn());},
      async play(){this.paused=false;(this.listeners.play||[]).forEach(fn=>fn());},load(){this.currentTime=0;this.duration=NaN;}});
    return nodes.get(id);
  }
  class Recorder {
    static isTypeSupported(type){return type==='audio/ogg;codecs=opus';}
    constructor(stream){this.stream=stream;this.state='inactive';recorders.push(this);}
    start(){this.state='recording';}
    stop(){this.state='inactive';this.ondataavailable?.({data:new Blob(['OggS-test'],{type:'audio/ogg'})});this.onstop?.();}
  }
  const context=vm.createContext({
    currentSenderId:'A',document:{getElementById:node,addEventListener(name,fn){if(name==='DOMContentLoaded')fn();}},
    window:{MediaRecorder:Recorder,addEventListener(){}},MediaRecorder:Recorder,
    navigator:{mediaDevices:{getUserMedia:()=>new Promise(resolve=>permissions.push(resolve))}},
    Date:{now:()=>now},Blob,URL:{createObjectURL:()=>`blob:voice-${recorders.length}`,revokeObjectURL:url=>revoked.push(url)},
    FormData:class{append(){}},File:class{constructor(parts,name,options){Object.assign(this,{parts,name,...options});}},
    setTimeout(fn,delay){timers.push({fn,delay});return timers.length;},clearTimeout(){},setInterval(){return 1;},clearInterval(){},
    apiFetch:(url,options)=>new Promise(resolve=>requests.push({url,options,resolve:body=>resolve({ok:true,json:async()=>body})})),
    loadMessages:async sender=>loaded.push(sender),showToast:message=>toasts.push(message),
  });
  vm.runInContext(code,context);
  function stream(){const track={stopped:0,stop(){this.stopped++;}};return {track,getTracks:()=>[track]};}
  return {context,node,permissions,recorders,requests,revoked,loaded,timers,toasts,inputArea,stream,setTime:value=>{now=value;},get:expression=>vm.runInContext(expression,context)};
}
async function ready(test){const start=test.context.toggleVoiceRecording();const stream=test.stream();test.permissions.at(-1)(stream);await start;return stream;}
(async()=>{
  let test=setup();let start=test.context.toggleVoiceRecording();
  assert.equal(test.node('voiceComposer').dataset.state,'starting');
  assert.equal(test.node('voiceRecordBtn').disabled,true,'Permission requests cannot be duplicated');
  const stream=test.stream();test.permissions[0](stream);await start;
  assert.equal(test.node('voiceComposer').dataset.state,'recording');
  assert.equal(test.node('voiceStopBtn').hidden,false);
  assert.equal(test.inputArea.classList.values.has('voice-active'),true);
  test.setTime(4500);test.context.updateVoiceClock();assert.equal(test.node('voiceDuration').textContent,'0:03');
  test.context.stopVoiceRecording();
  assert.equal(stream.track.stopped,1);assert.equal(test.node('voiceComposer').dataset.state,'preview');
  assert.equal(test.node('voicePlayback').hidden,false);assert.equal(test.node('voiceSendBtn').hidden,false);
  await test.context.toggleVoicePlayback();assert.equal(test.node('voicePreview').paused,false);
  test.context.seekRecordedVoice(50);assert.equal(test.node('voicePreview').currentTime,1.75);
  await test.context.toggleVoicePlayback();assert.equal(test.node('voicePreview').paused,true);
  let sending=test.context.sendRecordedVoice();assert.equal(test.node('voiceComposer').dataset.state,'sending');
  test.requests[0].resolve({ok:false,error:'temporary failure'});await sending;
  assert.equal(test.node('voiceComposer').dataset.state,'preview');assert.ok(test.get('voiceBlob'),'Failed sends preserve the draft');
  sending=test.context.sendRecordedVoice();test.requests[1].resolve({ok:true});await sending;
  assert.equal(test.node('voiceComposer').hidden,true);assert.equal(test.get('voiceBlob'),null);
  assert.equal(test.node('messageInput').value,'unsent text');assert.deepEqual(test.loaded,['A']);assert.equal(test.revoked.length,1);

  test=setup();start=test.context.toggleVoiceRecording();test.context.discardRecordedVoice();test.context.currentSenderId='B';
  const cancelledStream=test.stream();test.permissions[0](cancelledStream);await start;
  assert.equal(cancelledStream.track.stopped,1);assert.equal(test.recorders.length,0,'Late permission cannot start recording for a different customer');
  assert.equal(test.node('voiceComposer').hidden,true);

  test=setup();await ready(test);test.setTime(61000);test.timers.find(timer=>timer.delay===60000).fn();
  assert.equal(test.node('voiceDuration').textContent,'1:00');assert.equal(test.node('voiceComposer').dataset.state,'preview');
  sending=test.context.sendRecordedVoice();test.context.discardRecordedVoice();test.context.currentSenderId='B';
  test.requests[0].resolve({ok:true});await sending;
  assert.deepEqual(test.loaded,[],'Finishing an old upload cannot reload the newly opened conversation');
  assert.equal(test.node('voiceComposer').hidden,true);
  console.log('PASS: recording timer, stop/review/playback, retry preservation, automatic limit and conversation-safe permission/upload');
})().catch(error=>{console.error(error);process.exitCode=1;});
