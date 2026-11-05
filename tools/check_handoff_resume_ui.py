"""Operator resume acceptance with synthetic Flask records and intercepted delivery."""
import json,os,sys,tempfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
OUT=ROOT/'outputs/nov05-handoff-audit';OUT.mkdir(exist_ok=True)


def main():
    results=[]
    with (tempfile.TemporaryDirectory(dir=OUT) as directory,
          patch.dict(os.environ,dict(DATA_DIR=directory,DB_PATH=str(Path(directory)/'sales.db'),ENABLE_BACKGROUND_JOBS='0',
              DISABLE_CLIP='1',STORAGE_RESTORE_REQUIRED='0',OPENROUTER_API_KEY='',DASHBOARD_PASSWORD='local-preview-only')),
          patch('dotenv.load_dotenv',return_value=False),
          patch('requests.sessions.Session.request',side_effect=RuntimeError('External delivery disabled'))):
        from account_app.tests.test_handoff_resume_nov05 import HandoffResume
        HandoffResume.setUpClass();case=HandoffResume();case.setUp()
        m=case.m;db=case.db;sender=case.sender;client=case.client();case.skirt()
        case.save('incoming','شنو نوع القماش')
        m.create_human_review(db,dict(case.ev,text='شنو نوع القماش'),'سؤال عن المنتج يحتاج جواباً معتمداً من الموظف',notify_telegram=False)
        m.set_customer_ai_enabled(db,sender,False,reason='unanswered_product_question')
        controls=dict(run_jobs=False,fail_write=False);deliveries=[]
        try:
            with sync_playwright() as p,patch.object(m,'send_webhook_result_to_facebook',side_effect=lambda result,*a:deliveries.append(result) or True):
                browser=p.chromium.launch(channel='msedge',headless=True)
                context=browser.new_context(viewport={'width':1440,'height':960},service_workers='block')
                page=context.new_page();errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
                def route_request(route):
                    request=route.request;url=urlsplit(request.url)
                    if url.hostname!='handoff-resume.test':route.continue_();return
                    path=url.path+('?' +url.query if url.query else '')
                    if controls['fail_write'] and request.method=='POST' and url.path.endswith('/ai'):
                        route.fulfill(status=503,body=json.dumps({'error':'تعذر حفظ التشغيل للاختبار'}),content_type='application/json');return
                    if controls['run_jobs'] and '/ai_job/' in url.path:
                        identity=url.path.rsplit('/',1)[1]
                        row=db.execute('SELECT * FROM ai_jobs WHERE id=?',(identity,)).fetchone()
                        if row and row['status']=='queued':
                            job=dict(row);result=m.run_ai_job(job,json.loads(job['payload']))
                            m.ai_jobs.finish(db,identity,'done' if result.get('ok',True) else 'failed',result)
                    response=client.open(path,method=request.method,data=request.post_data,headers=dict(request.headers))
                    route.fulfill(status=response.status_code,body=response.data,content_type=response.content_type)
                page.route('**/*',route_request)
                page.goto('http://handoff-resume.test/dashboard?store_id=al-fatena',wait_until='domcontentloaded')
                page.wait_for_function('!inboxRequest && allConversations.length>0')
                page.evaluate('(id)=>selectConversation(id)',sender)
                page.wait_for_function('!messageRequest')
                page.evaluate('clearInterval(pollConvTimer);clearInterval(statsPollTimer);clearInterval(pollMsgTimer)')
                assert not page.evaluate('currentConversationAIEnabled')
                page.evaluate('window.resumeTask=toggleConversationAI();void 0')
                page.wait_for_function('currentConversationAIEnabled && conversationAIWritePending.size===0')
                assert page.locator('#conversationAiBtn').is_enabled()
                results.append('Enable returns immediately with a queued job; the stop switch stays usable during generation')
                page.evaluate('toggleConversationAI()')
                controls['run_jobs']=True;page.evaluate('resumeTask')
                assert not page.evaluate('currentConversationAIEnabled')
                assert not deliveries
                results.append('Stopping while the retry is queued prevents delivery and stale job completion cannot re-enable the switch')
                page.evaluate('toggleConversationAI()')
                assert page.evaluate('currentConversationAIEnabled')
                assert page.locator('#hiBanner').is_hidden()
                assert len(deliveries)==1
                assert 'دبل ايطالي' in deliveries[0]['reply']
                results.append('Known fabric retries send successfully and clear only the resolved review')
                case.save('incoming','شكد طول التنورة')
                m.set_customer_ai_enabled(db,sender,False,reason='unanswered_product_question')
                page.evaluate('loadConversations(false)')
                assert not page.evaluate('currentConversationAIEnabled')
                results.append('Polling reflects automatic pauses instead of displaying the cached enabled state')
                page.evaluate('toggleConversationAI()')
                assert not page.evaluate('currentConversationAIEnabled')
                assert page.locator('#hiBanner').is_visible()
                assert 'الطول' in page.locator('#toastContainer').inner_text()
                assert len(deliveries)==1
                results.append('A genuine missing fact names its field and displays the actual paused state rather than a generic system failure')
                reviews=db.execute('SELECT COUNT(*) FROM human_reviews WHERE sender_id=?',(sender,)).fetchone()[0]
                page.evaluate('askAI({staffAction:true,allowEmpty:true})')
                assert db.execute('SELECT COUNT(*) FROM human_reviews WHERE sender_id=?',(sender,)).fetchone()[0]==reviews
                results.append('Staff preview of a missing fact does not create duplicate reviews')
                case.product['measurements']='الطول: 90 سم'
                page.evaluate('toggleConversationAI()')
                assert page.evaluate('currentConversationAIEnabled')
                assert page.locator('#hiBanner').is_hidden()
                assert len(deliveries)==2
                results.append('After entering the missing catalogue fact, the same customer input retries in a new job and resumes normally')
                controls['fail_write']=True
                page.evaluate('toggleConversationAI()')
                assert page.evaluate('currentConversationAIEnabled')
                assert page.locator('#conversationAiBtn').is_enabled()
                results.append('A failed switch write preserves its confirmed state and releases the button')
                assert not errors,errors
                page.screenshot(path=str(OUT/'resume-ui.png'))
                browser.close()
        finally:
            case.tearDown();HandoffResume.tearDownClass()
    (OUT/'browser-results.json').write_text(json.dumps(dict(passed=True,checks=len(results),results=results),ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps(dict(passed=True,checks=len(results),results=results),ensure_ascii=True))


if __name__=='__main__':main()
