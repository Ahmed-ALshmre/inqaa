"""Synthetic browser acceptance for drafts, followup, staff credits and layout."""
import json,os,sys,tempfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright
R=Path(__file__).resolve().parents[1];sys.path.insert(0,str(R))
OUT=R/'outputs/staff-workspace-nov07';OUT.mkdir(exist_ok=True)


def main():
    checks=[]
    with tempfile.TemporaryDirectory(dir=OUT) as tmp,patch.dict(os.environ,dict(DATA_DIR=tmp,DB_PATH=str(Path(tmp)/'sales.db'),
        ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1',OPENROUTER_API_KEY='',STORAGE_RESTORE_REQUIRED='0')),\
        patch('dotenv.load_dotenv',return_value=False),patch('requests.sessions.Session.request',side_effect=RuntimeError('Real network blocked')):
        from account_app import app as m
        from account_app.tests.test_staff_workspace_nov07 import StaffWorkspaceTests
        case=StaffWorkspaceTests();case.setUp()
        product=dict(product_id='UI',product_name='تنورة اختبار',price='16000',stock='متوفر',status='active',sizes='38 إلى 48',store_id='default',image_url=[])
        try:
            for sender in ('qa-a','qa-b'):
                m.get_or_create_customer(m.get_db(),sender,'page','facebook')
                m.save_message(m.get_db(),sender,'incoming','text','شكد السعر؟',None,None,None,{})
            m.save_message(m.get_db(),'qa-a','outgoing','text','السعر 16 ألف',None,None,None,{'sender_type':'staff','staff_name':'سارة'})
            m.bind_customer_to_product(m.get_db(),'qa-a',product,source='manual_admin')
            with patch.object(m,'load_products_from_file',return_value=[product]),patch.object(m,'load_active_products',return_value=[product]),sync_playwright() as w:
                browser=w.chromium.launch(channel='msedge',headless=True)
                page=browser.new_page(viewport={'width':1480,'height':980});errors=[];held={}
                page.on('pageerror',lambda error:errors.append(str(error)))
                def route(route):
                    req=route.request;url=urlsplit(req.url)
                    if url.hostname!='staff-workspace.test':
                        if url.hostname in {'cdn.jsdelivr.net','fonts.googleapis.com','fonts.gstatic.com'}:route.continue_()
                        else:route.abort()
                        return
                    if url.path.endswith('/ask_ai'):
                        held['ai']=route;return
                    if url.path.endswith('/send'):
                        route.fulfill(status=200,body=json.dumps({'ok':True}),content_type='application/json');return
                    response=case.client.open(url.path+('?' +url.query if url.query else ''),method=req.method,data=req.post_data,headers=dict(req.headers))
                    route.fulfill(status=response.status_code,body=response.data,content_type=response.content_type)
                page.route('**/*',route)
                def open_helper():
                    page.locator('#interventionModal').wait_for(state='hidden')
                    page.evaluate("""() => new Promise(resolve => {
                        document.querySelector('#interventionModal').addEventListener('shown.bs.modal', () => resolve(), {once:true});
                        openInterventionDialog();
                    })""")
                page.goto('http://staff-workspace.test/dashboard',wait_until='domcontentloaded')
                page.wait_for_function('allConversations.length===2')
                page.evaluate("selectConversation('qa-a')")
                page.wait_for_function("currentSenderId==='qa-a' && !!document.querySelector('.msg-sender')")
                assert 'سارة' in page.locator('.msg-sender').inner_text()
                checks.append('Employee name appears safely in the internal message footer')
                page.locator('#messageInput').fill('مسودة المحادثة الأولى')
                page.evaluate("selectConversation('qa-b')")
                assert page.locator('#messageInput').input_value()==''
                page.locator('#messageInput').fill('مسودة ثانية')
                page.evaluate("selectConversation('qa-a')")
                assert page.locator('#messageInput').input_value()=='مسودة المحادثة الأولى'
                page.wait_for_timeout(1800)
                assert page.locator('#messageInput').input_value()=='مسودة المحادثة الأولى'
                checks.append('Conversation switching and polling preserve separate drafts')
                page.evaluate('void askAI({staffAction:true})')
                page.wait_for_function('_isAskingAI')
                page.locator('#messageInput').fill('كتبت نص جديد وأنا أنتظر')
                held['ai'].fulfill(status=200,body=json.dumps({'reply':'اقتراح جديد من المساعد'}),content_type='application/json')
                page.wait_for_function('!_isAskingAI')
                assert page.locator('#messageInput').input_value()=='كتبت نص جديد وأنا أنتظر'
                assert page.locator('#aiReplyPreview').is_visible()
                checks.append('An AI response becomes a preview and never overwrites a draft edited while waiting')
                held.pop('ai',None)
                page.evaluate("document.querySelector('#hiUnifiedText').value='';void hiAskAI()")
                page.wait_for_function('_isAskingAI')
                held['ai'].fulfill(status=200,body=json.dumps({'reply':'اقتراح من المساعد دون مسودة'}),content_type='application/json')
                page.wait_for_function('!_isAskingAI && !_hiBusy')
                assert page.locator('#messageInput').input_value()=='كتبت نص جديد وأنا أنتظر'
                checks.append('Calling the intervention assistant with an empty field keeps the main composer draft')
                page.locator('#messageInput').fill('رسالة يدوية للإرسال')
                page.locator('#sendBtn').click()
                page.wait_for_function("document.querySelector('#messageInput').value==='' && !_isSending")
                checks.append('A successful manual send empties the composer')
                open_helper()
                page.locator('#hiUnifiedText').fill('رد مباشر من المساعد')
                page.evaluate('void hiSendDirect()')
                page.wait_for_function("!_hiBusy && document.querySelector('#hiUnifiedText').value===''")
                checks.append('A successful direct helper send empties its own field')
                open_helper()
                page.locator('#hiUnifiedText').fill('مسودة لتوليد اقتراح')
                held.pop('ai',None);page.evaluate('void hiAskAI()');page.wait_for_function('_isAskingAI')
                held['ai'].fulfill(status=200,body=json.dumps({'reply':'اقتراح جاهز للإرسال'}),content_type='application/json')
                page.wait_for_function('!_isAskingAI && !_hiBusy')
                assert page.locator('#hiUnifiedText').input_value()=='مسودة لتوليد اقتراح'
                page.evaluate('editAIReply()')
                assert page.locator('#hiUnifiedText').input_value()=='مسودة لتوليد اقتراح'
                page.locator('#sendBtn').click()
                page.wait_for_function("!_isSending && document.querySelector('#hiUnifiedText').value==='' && document.querySelector('#messageInput').value===''")
                checks.append('Generation and editing keep the helper input; successful manual sending of the edited AI proposal clears both sent fields')
                page.locator('#manualFollowupBtn').click()
                page.wait_for_function("document.querySelector('#manualFollowupBtn').dataset.enabled==='true'")
                page.locator('[data-filter=followup]').click()
                page.wait_for_function('!inboxRequest && allConversations.length===1')
                assert page.evaluate('allConversations[0].sender_id')=='qa-a'
                checks.append('Reply staff can mark a conversation and find it with the followup filter')
                page.locator('#manualFollowupBtn').click()
                page.wait_for_function('allConversations.length===0')
                checks.append('Removing followup updates the filtered list immediately')
                page.locator('[data-filter=all]').click();page.wait_for_function('allConversations.length===2')
                page.evaluate("selectConversation('qa-a')")
                page.wait_for_function("!document.querySelector('#chatProductCard').hidden")
                page.screenshot(path=str(OUT/'conversation-desktop.png'))
                page.set_viewport_size({'width':390,'height':844})
                page.evaluate("selectConversation('qa-a')")
                page.locator('#conversationAiBtn').wait_for(state='visible')
                page.wait_for_timeout(300)
                ai=page.locator('#conversationAiBtn').bounding_box();card=page.locator('#chatProductCard').bounding_box()
                assert ai['width']>card['width']*1.25,(ai,card)
                assert card['height']<68,card
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
                page.screenshot(path=str(OUT/'conversation-mobile.png'))
                checks.append('Mobile layout gives AI most of the row with a compact product card and no horizontal overflow')
                page.goto('http://staff-workspace.test/settings/staff',wait_until='domcontentloaded')
                page.wait_for_function('staffAccounts.length===1')
                page.evaluate('editStaff(staffAccounts[0].id)')
                page.locator('#staffRewardsEnabled').uncheck();page.locator('#staffSave').click()
                page.wait_for_function("!document.querySelector('#staffDialog').open && staffAccounts[0].rewards_enabled===false")
                page.evaluate('editStaff(staffAccounts[0].id)');assert not page.locator('#staffRewardsEnabled').is_checked()
                checks.append('Owner can disable reward counting and see the saved setting after reopening')
                assert not errors,errors
                checks.append('No JavaScript exceptions and no customer delivery requests')
                browser.close()
        finally:case.tearDown()
    result=dict(passed=True,checks=len(checks),results=checks)
    (OUT/'browser-results.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps(result,ensure_ascii=True))


if __name__=='__main__':main()
