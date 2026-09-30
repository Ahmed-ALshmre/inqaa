import os
import tempfile
import json
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright

with tempfile.TemporaryDirectory() as directory:
    os.environ.update(DATA_DIR=directory, DB_PATH=str(Path(directory)/'test.db'),
                      ENABLE_BACKGROUND_JOBS='0', DISABLE_CLIP='1')
    from account_app.tests.test_rewards import RewardTests
    fixture = RewardTests()
    fixture.setUp()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(channel='msedge')
            context = browser.new_context(viewport={'width':390,'height':844}, is_mobile=True, has_touch=True)
            page = context.new_page()
            def route(request):
                url = urlsplit(request.request.url)
                if url.hostname != 'scroll.test':
                    request.fulfill(status=200,body='')
                    return
                response = fixture.client.get(url.path + ('?'+url.query if url.query else ''))
                request.fulfill(status=response.status_code,body=response.data,content_type=response.content_type)
            page.route('**/*',route)
            cdp = context.new_cdp_session(page)
            results=[]
            for path in ['/settings','/products','/orders','/rewards']:
                page.goto('http://scroll.test'+path)
                page.wait_for_timeout(200)
                # Empty test databases can produce short pages. Add ordinary
                # content to verify the same page's scrolling layout.
                page.evaluate("""() => { const e=document.createElement('div'); e.style.height='1800px';
                    e.textContent='Scroll verification'; (document.querySelector('main') || document.body).append(e); }""")
                page.evaluate('window.scrollTo(0,0)')
                page.wait_for_timeout(150)
                cdp.send('Input.dispatchTouchEvent', {'type':'touchStart','touchPoints':[{'x':195,'y':650}]})
                for y in range(625,174,-25):
                    cdp.send('Input.dispatchTouchEvent', {'type':'touchMove','touchPoints':[{'x':195,'y':y}]})
                    page.wait_for_timeout(20)
                cdp.send('Input.dispatchTouchEvent', {'type':'touchEnd','touchPoints':[]})
                page.wait_for_timeout(250)
                result=page.evaluate("""() => ({y:scrollY,bodyOverflow:getComputedStyle(document.body).overflowY,
                    bodyScroll:document.body.scrollTop,root:document.scrollingElement.tagName})""")
                assert result['y']>100, (path,result)
                assert result['bodyOverflow']=='visible', (path,result)
                results.append(dict(page=path,**result))
            page.goto('http://scroll.test/dashboard')
            assert page.locator('body').evaluate('(e)=>getComputedStyle(e).overflowY')=='hidden'
            context.close()
            browser.close()
            Path('outputs/mobile-scroll-check.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
            print('PASS: single-finger touch scroll on 4 mobile pages; chat layout preserved')
    finally:
        fixture.tearDown()
