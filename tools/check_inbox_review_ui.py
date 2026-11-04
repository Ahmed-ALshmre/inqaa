"""Browser acceptance through a disposable Flask client; no customer delivery or local ports."""
import json, os, sys, tempfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
OUT=ROOT/'outputs/inbox-performance-nov04';OUT.mkdir(parents=True,exist_ok=True)


def main():
    results=[]
    with (tempfile.TemporaryDirectory(dir=OUT) as directory, patch.dict(os.environ,dict(
            DATA_DIR=directory,DB_PATH=str(Path(directory)/'sales.db'),ENABLE_BACKGROUND_JOBS='0',
            DISABLE_CLIP='1',STORAGE_RESTORE_REQUIRED='0',OPENROUTER_API_KEY='',DASHBOARD_PASSWORD='local-preview-only')),
            patch('dotenv.load_dotenv',return_value=False),patch('requests.sessions.Session.request',side_effect=RuntimeError('External delivery disabled'))):
        from account_app import app as m
        client=m.app.test_client()
        with client.session_transaction() as session:session['dashboard_authenticated']=True
        with m.app.app_context():
            db=m.get_db()
            for i in range(160):
                sender=f'review-qa-{i:03}'
                db.execute('INSERT INTO customers(sender_id,name,store_id,last_seen_at) VALUES(?,?,?,?)',
                           (sender,'زبون اختبار '+str(i),'default',f'2026-11-04T12:{i//60:02}:{i%60:02}+03:00'))
                db.execute("INSERT INTO messages(sender_id,direction,message_type,text,created_at) VALUES(?,'incoming','text','عندي سؤال عن الطلب',?)",
                           (sender,f'2026-11-04T12:{i//60:02}:{i%60:02}+03:00'))
                db.execute("INSERT INTO human_reviews(sender_id,status,reason) VALUES(?,'pending','تدقيق يدوي')",(sender,))
            db.commit()
        with sync_playwright() as p:
            browser=p.chromium.launch(channel='msedge',headless=True)
            context=browser.new_context(viewport={'width':1440,'height':960},service_workers='block')
            page=context.new_page();errors=[];requests=[];held={};mode={'hold_list':False,'hold_close':False,'fail_close':False}
            page.on('pageerror',lambda e:errors.append(str(e)))
            def route_request(route):
                request=route.request;url=urlsplit(request.url)
                if url.hostname!='inbox-review.test':route.continue_();return
                path=url.path+('?' +url.query if url.query else '')
                requests.append((request.method,url.path))
                if mode['fail_close'] and url.path.endswith('/mark_reviewed'):
                    route.fulfill(status=503,body=json.dumps({'error':'تعذر الحفظ للاختبار'}),content_type='application/json');return
                response=client.open(path,method=request.method,data=request.post_data,headers=dict(request.headers))
                packet=dict(status=response.status_code,body=response.data,content_type=response.content_type)
                if mode['hold_list'] and url.path=='/api/conversations':
                    mode['hold_list']=False;held['list']=(route,packet);return
                if mode['hold_close'] and url.path.endswith('/mark_reviewed'):
                    mode['hold_close']=False;held['close']=(route,packet);return
                route.fulfill(**packet)
            page.route('**/*',route_request)
            page.goto('http://inbox-review.test/dashboard',wait_until='domcontentloaded')
            page.wait_for_function('!inboxRequest && allConversations.length===20')
            page.evaluate('clearInterval(pollConvTimer);clearInterval(statsPollTimer)')
            def filtered(value):
                page.evaluate('(value)=>setFilter(value)',value)
                page.wait_for_function('!inboxRequest && !inboxLoadError')
            def select(sender):
                page.evaluate('(id)=>selectConversation(id)',sender)
                page.wait_for_function('(id)=>currentSenderId===id && !messageRequest',arg=sender)
                page.evaluate('clearInterval(pollMsgTimer)')
            def release(key):
                route,packet=held.pop(key);route.fulfill(**packet)
            def pump(key):
                for _ in range(100):
                    if key in held:return
                    page.wait_for_timeout(30)
                raise AssertionError('Request not captured: '+key)
            filtered('problems')
            page.evaluate('loadMoreConversations()');page.wait_for_function('!inboxRequest && allConversations.length===40')
            sender=page.evaluate('allConversations[7].sender_id');select(sender)
            page.evaluate("document.getElementById('customerList').scrollTop=450;window.preservedRow=document.querySelectorAll('#customerList .customer-item')[10]")
            before=page.evaluate("({count:allConversations.length,scroll:document.getElementById('customerList').scrollTop,messages:messageCache.get(currentSenderId).messages.length})")
            anchor=page.evaluate('(id)=>captureInboxPosition(document.getElementById("customerList")).anchors.find(a=>a.sender!==id)',sender)
            list_requests=sum(path=='/api/conversations' for method,path in requests)
            page.evaluate('markHumanReview()')
            page.wait_for_function('reviewCloseRequests.size===0')
            assert page.evaluate('allConversations.length')==before['count']-1
            assert sum(path=='/api/conversations' for method,path in requests)==list_requests
            assert page.evaluate('currentSenderId')==sender
            assert page.evaluate('messageCache.get(currentSenderId).messages.length')==before['messages']
            assert page.evaluate('preservedRow.isConnected')
            assert abs(page.evaluate('(a)=>document.querySelector(`[data-sender="${a.sender}"]`).getBoundingClientRect().top-document.getElementById("customerList").getBoundingClientRect().top',anchor)-anchor['offset'])<=1
            assert page.locator('#hiBanner').is_hidden()
            results.append('Closing updates the target immediately without refetching, discarding loaded pages or closing the chat')

            # A poll begun before closure must not resurrect the old review.
            sender=page.evaluate('allConversations[5].sender_id');select(sender)
            mode['hold_list']=True
            page.evaluate('void loadConversations(false)');pump('list')
            page.evaluate('markHumanReview()');page.wait_for_function('reviewCloseRequests.size===0')
            release('list');page.wait_for_function('!inboxRequest')
            assert not page.evaluate('(id)=>allConversations.some(c=>c.sender_id===id)',sender)
            results.append('A delayed stale poll cannot restore a closed review')

            # Deduplicate writes; a late completion cannot change another selected chat.
            sender=page.evaluate('allConversations[3].sender_id');other=page.evaluate('allConversations[4].sender_id');select(sender)
            mode['hold_close']=True;posts=sum(path.endswith('/mark_reviewed') for method,path in requests)
            page.evaluate('void markHumanReview();void markHumanReview()');pump('close')
            assert page.locator('#markHumanReviewButton').is_disabled()
            select(other);release('close');page.wait_for_function('reviewCloseRequests.size===0')
            assert sum(path.endswith('/mark_reviewed') for method,path in requests)==posts+1
            assert page.evaluate('currentSenderId')==other
            assert page.evaluate('currentCustomer.pending_reviews_count')==1
            assert page.locator('#hiBanner').is_visible()
            results.append('Double clicks issue one write; completion remains scoped to its original chat')

            mode['fail_close']=True
            page.evaluate('markHumanReview()');page.wait_for_function('reviewCloseRequests.size===0')
            assert page.evaluate('currentCustomer.pending_reviews_count')==1
            assert page.locator('#hiBanner').is_visible()
            assert page.locator('#markHumanReviewButton').is_enabled()
            mode['fail_close']=False
            results.append('Failed save retains the review and permits retry')

            # Pending color review stays protected and identifies its order.
            with m.app.app_context():
                db=m.get_db();order_id=db.execute('INSERT INTO orders(sender_id,status) VALUES(?,?)',(other,m.COLOR_REVIEW_STATUS)).lastrowid;db.commit()
            page.evaluate('markHumanReview()');page.wait_for_function('reviewCloseRequests.size===0')
            assert page.locator('#hiBanner').is_visible()
            assert '#'+str(order_id) in page.locator('#toastContainer').inner_text()
            results.append('Color-held orders explain the block and preserve the review')
            with m.app.app_context():
                db=m.get_db();db.execute("UPDATE orders SET status='new' WHERE id=?",(order_id,))
                m.order_actions.record(db,other,order_id,{'kind':'cancel','value':'cancelled'},m.now_baghdad_iso());db.commit()
            page.once('dialog',lambda dialog:dialog.dismiss())
            page.evaluate('markHumanReview()');page.wait_for_function('reviewCloseRequests.size===0')
            assert page.locator('#hiBanner').is_visible()
            page.once('dialog',lambda dialog:dialog.accept())
            page.evaluate('markHumanReview()');page.wait_for_function('reviewCloseRequests.size===0')
            assert page.locator('#hiBanner').is_hidden()
            with m.app.app_context():
                db=m.get_db();assert db.execute('SELECT status FROM orders WHERE id=?',(order_id,)).fetchone()[0]=='new'
                assert not m.order_actions.pending(db,other)
            results.append('External-completion confirmation is explicit; canceling leaves the task pending and accepting does not cancel the shipping order')

            filtered('all')
            page.evaluate('loadMoreConversations()');page.wait_for_function('!inboxRequest && allConversations.length===40')
            sender=page.evaluate('allConversations[9].sender_id');select(sender)
            page.evaluate('markHumanReview()');page.wait_for_function('reviewCloseRequests.size===0')
            assert page.evaluate('allConversations.length')==40
            assert page.evaluate('(id)=>allConversations.find(c=>c.sender_id===id).pending_reviews_count',sender)==0
            results.append('All-conversations view keeps its loaded row count and updates only the closed badges')
            sender=page.evaluate('allConversations.find(c=>c.pending_reviews_count>0).sender_id');select(sender)
            page.evaluate('hiCloseReview()');page.wait_for_function('reviewCloseRequests.size===0 && !_hiBusy')
            assert page.locator('#markHumanReviewButton').is_enabled()
            assert page.locator('#hiBanner').is_hidden()
            results.append('Closing from the intervention dialog uses the same state update and releases all action buttons')
            for _ in range(4):
                page.evaluate('loadMoreConversations()');page.wait_for_function('!inboxRequest')
            assert page.evaluate('allConversations.length')==120
            page.evaluate('loadConversations(false)');page.wait_for_function('!inboxRequest')
            assert page.evaluate('allConversations.length')==120
            results.append('Refreshing after loading more than 100 rows preserves the cached tail and cursor pagination')
            sender=page.evaluate('allConversations[10].sender_id');select(sender)
            list_requests=sum(path=='/api/conversations' for method,path in requests)
            page.once('dialog',lambda dialog:dialog.accept())
            page.evaluate('deleteConversation()')
            assert page.evaluate('allConversations.length')==119
            assert sum(path=='/api/conversations' for method,path in requests)==list_requests
            assert page.evaluate('currentSenderId') is None
            results.append('Deleting a synthetic conversation removes only its row without reloading all loaded pages')
            assert not errors,errors
            page.screenshot(path=str(OUT/'review-close-desktop.png'))
            page.set_viewport_size({'width':390,'height':844})
            assert page.evaluate('document.documentElement.scrollWidth<=innerWidth+1')
            page.screenshot(path=str(OUT/'review-close-mobile.png'))
            browser.close()
    (OUT/'browser-review-results.json').write_text(json.dumps(dict(passed=True,checks=len(results),results=results),ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps(dict(passed=True,checks=len(results),results=results),ensure_ascii=True))


if __name__=='__main__':main()
