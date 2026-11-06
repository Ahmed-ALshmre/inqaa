// A voice draft stays with its conversation, including during permission and upload.
let voiceRecorder = null;
let voiceStream = null;
let voiceBlob = null;
let voiceUrl = '';
let voiceSender = null;
let voiceTimer = null;
let voiceTickTimer = null;
let voiceStarting = false;
let voiceStopping = false;
let voiceSending = false;
let voiceStartedAt = 0;
let voiceElapsedMs = 0;
let voiceGeneration = 0;

function voiceNode(id) { return document.getElementById(id); }
function formatVoiceTime(seconds) {
  const total = Math.max(0, Math.floor(Number(seconds) || 0));
  return `${Math.floor(total / 60)}:${String(total % 60).padStart(2, '0')}`;
}
function voiceMime() {
  if (!window.MediaRecorder) return '';
  return ['audio/mp4', 'audio/ogg;codecs=opus', 'audio/webm;codecs=opus', 'audio/webm']
    .find(type => MediaRecorder.isTypeSupported(type)) || '';
}
function updateVoiceClock() {
  const elapsed = voiceRecorder?.state === 'recording' ? Math.min(60000, Date.now() - voiceStartedAt) : voiceElapsedMs;
  const clock = voiceNode('voiceDuration'); if (clock) clock.textContent = formatVoiceTime(elapsed / 1000);
}
function voiceControls() {
  const active = voiceRecorder?.state === 'recording';
  const state = voiceStarting ? 'starting' : active ? 'recording' : voiceStopping ? 'stopping' : voiceBlob ? (voiceSending ? 'sending' : 'preview') : 'idle';
  const wrap = voiceNode('voiceComposer'); if (!wrap) return;
  const changed = wrap.dataset.state !== state;
  wrap.hidden = state === 'idle'; wrap.dataset.state = state;
  wrap.closest('.input-area')?.classList.toggle('voice-active', !wrap.hidden);
  const labels = {
    idle:['رسالة صوتية','التسجيل جاهز','mic'],
    starting:['تشغيل الميكروفون','اسمح باستخدام الميكروفون للبدء','hourglass-split'],
    recording:['جارٍ التسجيل','إيقاف التسجيل لمراجعته · حتى دقيقة واحدة','mic-fill'],
    stopping:['تجهيز التسجيل','لحظة، نجهّز المعاينة…','three-dots'],
    preview:['تسجيلك جاهز','استمع إليه قبل الإرسال','soundwave'],
    sending:['جارٍ إرسال الصوت','انتظر تأكيد وصول التسجيل','cloud-arrow-up'],
  };
  const [title,status,icon] = labels[state];
  voiceNode('voiceTitle').textContent = title;
  if (voiceNode('voiceStatus').textContent !== status) voiceNode('voiceStatus').textContent = status;
  voiceNode('voiceStateIcon').className = `bi bi-${icon}`;
  voiceNode('voiceVisualizer').hidden = !active;
  voiceNode('voicePlayback').hidden = !voiceBlob;
  voiceNode('voiceStopBtn').hidden = !active;
  voiceNode('voiceRestartBtn').hidden = !voiceBlob;
  voiceNode('voiceRestartBtn').disabled = voiceSending;
  voiceNode('voiceDiscardBtn').disabled = voiceSending;
  const send = voiceNode('voiceSendBtn'); send.hidden = !voiceBlob; send.disabled = voiceSending;
  send.innerHTML = voiceSending ? '<span class="spinner-border spinner-border-sm" aria-hidden="true"></span><span>جارٍ الإرسال…</span>' : '<i class="bi bi-send-fill"></i><span>إرسال الصوت</span>';
  const record = voiceNode('voiceRecordBtn');
  if (record) {
    record.disabled = voiceStarting || voiceStopping || voiceSending || Boolean(voiceBlob);
    record.classList.toggle('is-recording', active); record.setAttribute('aria-pressed', String(active));
    const label = active ? 'إيقاف التسجيل' : voiceBlob ? 'احذف التسجيل الحالي أو أعد تسجيله' : 'تسجيل رسالة صوتية';
    record.title = label; record.setAttribute('aria-label',label);
    record.innerHTML = active ? '<i class="bi bi-stop-fill"></i>' : '<i class="bi bi-mic"></i>';
  }
  updateVoiceClock(); updateVoicePlayback();
  if (changed) window.requestAnimationFrame?.(() => {
    const messages = voiceNode('messagesArea');
    if (messages?.dataset.followLatest === '1') messages.scrollTop = messages.scrollHeight;
  });
}
function discardRecordedVoice() {
  voiceGeneration++;
  clearTimeout(voiceTimer); clearInterval(voiceTickTimer);
  const recorder = voiceRecorder; voiceRecorder = null;
  if (recorder?.state === 'recording') recorder.stop();
  voiceStream?.getTracks().forEach(track => track.stop());
  const preview = voiceNode('voicePreview');
  if (preview) { preview.pause(); preview.removeAttribute('src'); preview.load(); }
  if (voiceUrl) URL.revokeObjectURL(voiceUrl);
  voiceStream = null; voiceBlob = null; voiceUrl = ''; voiceSender = null;
  voiceStarting = false; voiceStopping = false; voiceStartedAt = 0; voiceElapsedMs = 0;
  voiceControls();
}
function stopVoiceRecording() {
  if (voiceRecorder?.state !== 'recording') return;
  voiceElapsedMs = Math.max(0, Math.min(60000, Date.now() - voiceStartedAt));
  voiceStopping = true; clearInterval(voiceTickTimer); clearTimeout(voiceTimer);
  voiceRecorder.stop(); voiceControls();
}
async function toggleVoiceRecording() {
  if (voiceRecorder?.state === 'recording') { stopVoiceRecording(); return; }
  if (voiceStarting || voiceStopping || voiceSending || voiceBlob) return;
  if (!currentSenderId) { showToast('اختر محادثة أولاً','warning'); return; }
  const mime = voiceMime();
  if (!mime || !navigator.mediaDevices?.getUserMedia) { showToast('اسمح بالميكروفون واستخدم اتصالاً آمناً لتسجيل الصوت','warning'); return; }
  discardRecordedVoice();
  const generation = voiceGeneration, sender = currentSenderId;
  voiceStarting = true; voiceControls(); voiceNode('messageInput')?.blur();
  let stream;
  try {
    stream = await navigator.mediaDevices.getUserMedia({audio:{echoCancellation:true,noiseSuppression:true}});
    if (generation !== voiceGeneration || currentSenderId !== sender) { stream.getTracks().forEach(track => track.stop()); return; }
    const chunks = [];
    const recorder = new MediaRecorder(stream,{mimeType:mime,audioBitsPerSecond:64000});
    voiceStream = stream; voiceSender = sender; voiceRecorder = recorder; voiceStarting = false;
    recorder.ondataavailable = event => { if (event.data?.size) chunks.push(event.data); };
    recorder.onerror = () => {
      if (generation !== voiceGeneration || voiceRecorder !== recorder) return;
      discardRecordedVoice(); showToast('توقف التسجيل؛ حاول تسجيل الرسالة مجدداً','danger');
    };
    recorder.onstop = () => {
      stream.getTracks().forEach(track => track.stop());
      if (generation !== voiceGeneration) return;
      clearTimeout(voiceTimer); clearInterval(voiceTickTimer);
      if (!voiceElapsedMs) voiceElapsedMs = Math.max(0,Math.min(60000,Date.now()-voiceStartedAt));
      voiceStream = null; voiceRecorder = null; voiceStopping = false;
      if (currentSenderId !== sender) { discardRecordedVoice(); return; }
      const blob = new Blob(chunks,{type:mime.split(';')[0]});
      if (!blob.size) { discardRecordedVoice(); showToast('التسجيل فارغ، أعد المحاولة','warning'); return; }
      voiceBlob = blob; voiceUrl = URL.createObjectURL(blob);
      voiceNode('voicePreview').src = voiceUrl; voiceControls();
    };
    voiceStartedAt = Date.now(); recorder.start(250);
    voiceTickTimer = setInterval(updateVoiceClock,250);
    voiceTimer = setTimeout(stopVoiceRecording,60000); voiceControls();
  } catch (error) {
    stream?.getTracks().forEach(track => track.stop());
    if (generation === voiceGeneration) { discardRecordedVoice(); showToast('تعذر تشغيل الميكروفون: '+error.message,'danger'); }
  }
}
function restartVoiceRecording() {
  if (voiceSending || voiceStarting || voiceStopping) return;
  discardRecordedVoice(); return toggleVoiceRecording();
}
function voicePlaybackDuration() {
  const duration = voiceNode('voicePreview')?.duration;
  return Number.isFinite(duration) && duration > 0 ? duration : voiceElapsedMs / 1000;
}
function updateVoicePlayback() {
  const audio = voiceNode('voicePreview'); if (!audio) return;
  const duration = voicePlaybackDuration(), time = Number(audio.currentTime) || 0;
  const seek = voiceNode('voiceSeek'), play = voiceNode('voicePlayBtn');
  if (seek) { seek.value = duration ? Math.min(100,time / duration * 100) : 0; seek.setAttribute('aria-valuetext',formatVoiceTime(time)); }
  voiceNode('voicePlaybackTime').textContent = formatVoiceTime(time);
  voiceNode('voiceTotalTime').textContent = formatVoiceTime(duration);
  if (play) { play.innerHTML = `<i class="bi bi-${audio.paused ? 'play' : 'pause'}-fill"></i>`; play.setAttribute('aria-label',audio.paused ? 'تشغيل التسجيل' : 'إيقاف المعاينة مؤقتاً'); }
}
async function toggleVoicePlayback() {
  const audio = voiceNode('voicePreview'); if (!voiceBlob || !audio) return;
  try { if (audio.paused) await audio.play(); else audio.pause(); updateVoicePlayback(); }
  catch { showToast('تعذر تشغيل المعاينة؛ حاول مرة أخرى','warning'); }
}
function seekRecordedVoice(percent) {
  const audio = voiceNode('voicePreview'); if (!voiceBlob || !audio) return;
  try { audio.currentTime = voicePlaybackDuration() * Math.max(0,Math.min(100,Number(percent) || 0)) / 100; updateVoicePlayback(); } catch (_) {}
}
async function sendRecordedVoice() {
  if (!voiceBlob || voiceSending || !voiceSender || voiceSender !== currentSenderId) return;
  const blob = voiceBlob, sender = voiceSender, generation = voiceGeneration;
  voiceSending = true; voiceControls();
  try {
    voiceNode('voicePreview').pause();
    const type = blob.type.split(';')[0], ext = {'audio/mp4':'m4a','audio/ogg':'ogg','audio/webm':'webm'}[type];
    if (!ext) throw Error('صيغة التسجيل غير مدعومة');
    const body = new FormData(); body.append('audio',new File([blob],`voice.${ext}`,{type}));
    const response = await apiFetch(`/api/conversations/${encodeURIComponent(sender)}/send_audio`,{method:'POST',body});
    const data = await response.json();
    if (!response.ok || !data.ok) throw Error(data.error || 'مزود الرسائل لم يؤكد إرسال الصوت');
    if (generation === voiceGeneration && currentSenderId === sender) { discardRecordedVoice(); await loadMessages(sender,true); }
    showToast('تم إرسال التسجيل الصوتي','success');
  } catch (error) { showToast('فشل إرسال الصوت: '+error.message,'danger'); }
  finally { voiceSending = false; voiceControls(); }
}
document.addEventListener('DOMContentLoaded',() => {
  const audio = voiceNode('voicePreview'); if (!audio) return;
  ['loadedmetadata','durationchange','timeupdate','play','pause'].forEach(event => audio.addEventListener(event,updateVoicePlayback));
  audio.addEventListener('ended',() => { audio.currentTime = 0; updateVoicePlayback(); });
});
window.addEventListener('pagehide',discardRecordedVoice);
