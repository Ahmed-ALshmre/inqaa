"""Owner connection UI acceptance using an isolated Flask database."""
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
OUT=ROOT/'outputs/live-access-ui';OUT.mkdir(parents=True,exist_ok=True)


def main():
    results=[]
    with (tempfile.TemporaryDirectory(dir=OUT) as directory,patch.dict(os.environ,dict(DATA_DIR=directory,
            DB_PATH=str(Path(directory)/'sales.db'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1',
            OPENROUTER_API_KEY='',DASHBOARD_PASSWORD='local-preview-only',STORAGE_RESTORE_REQUIRED='0')),
            patch('requests.sessions.Session.request',side_effect=RuntimeError('External delivery disabled'))):
        from account_app.tests.test_live_access import LiveAccessTests
        case=LiveAccessTests();case.setUp()
        try:
            with sync_playwright() as p:
                browser=p.chromium.launch(channel='msedge',headless=True)
                page=browser.new_page(viewport=dict(width=1400,height=1000),service_workers='block')
                errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
                control=dict(fail_create=False)
                def route_request(route):
                    req=route.request;url=urlsplit(req.url)
                    if url.hostname!='live-access.test':route.continue_();return
                    if control['fail_create'] and req.method=='POST' and url.path=='/api/settings/live-access':
                        route.fulfill(status=503,content_type='application/json',body=json.dumps(dict(error='تعذر حفظ الاتصال للاختبار')));return
                    response=case.owner.open(url.path+('?' +url.query if url.query else ''),method=req.method,
                        data=req.post_data,headers=dict(req.headers))
                    route.fulfill(status=response.status_code,body=response.data,content_type=response.content_type)
                page.route('**/*',route_request)
                page.goto('http://live-access.test/settings/live-access',wait_until='networkidle')
                page.locator('#liveAccessList strong').first.wait_for()
                page.fill('#liveAccessLabel','اتصال مراجعة <script>ليس كوداً</script>')
                page.click('#liveAccessCreate');page.wait_for_function("!document.getElementById('liveAccessSecret').hidden")
                token=page.locator('#liveAccessToken').input_value()
                assert token.startswith('inq_live_')
                assert '<script>ليس كوداً</script>' in page.locator('#liveAccessList').inner_text()
                results.append('Owner can create a CSRF-protected connection; labels are rendered as text')
                page.click('#liveAccessHide')
                assert page.locator('#liveAccessToken').input_value()==''
                page.reload(wait_until='networkidle')
                assert page.locator('#liveAccessToken').input_value()==''
                assert page.locator('#liveAccessSecret').is_hidden()
                results.append('The one-time credential can be cleared and never reappears on reload')
                row=page.locator('#liveAccessList > div').filter(has_text='اتصال مراجعة')
                row.locator('button').click();page.wait_for_function("document.getElementById('liveAccessStatus').textContent.includes('إلغاء')")
                response=case.reader.get('/api/live/v1/manifest',headers={'Authorization':'Bearer '+token})
                assert response.status_code==401
                assert 'ملغى' in row.inner_text()
                results.append('Revoking from the UI immediately invalidates the bearer connection')
                control['fail_create']=True;page.fill('#liveAccessLabel','محاولة فاشلة')
                count=case.db.execute('SELECT COUNT(*) FROM live_access_tokens').fetchone()[0]
                page.click('#liveAccessCreate');page.wait_for_function("document.getElementById('liveAccessStatus').textContent.includes('تعذر')")
                assert page.locator('#liveAccessCreate').is_enabled()
                assert page.locator('#liveAccessSecret').is_hidden()
                assert case.db.execute('SELECT COUNT(*) FROM live_access_tokens').fetchone()[0]==count
                results.append('A failed save displays its error, releases the button and creates no phantom key')
                page.set_viewport_size(dict(width=390,height=844))
                page.wait_for_function("document.getElementById('adminSidebar').getBoundingClientRect().left >= innerWidth - 1")
                assert page.evaluate('document.documentElement.scrollWidth<=window.innerWidth')
                bounds=page.locator('.admin-main').bounding_box()
                (OUT/'mobile-bounds.json').write_text(json.dumps(dict(bounds=bounds,main_style=page.locator('.admin-main').evaluate('(e)=>({margin:getComputedStyle(e).marginRight,width:getComputedStyle(e).width})'))),encoding='utf8')
                assert bounds['x']>=0 and bounds['x']+bounds['width']<=391,bounds
                page.screenshot(path=str(OUT/'owner-access-mobile.png'),full_page=True)
                page.set_viewport_size(dict(width=1400,height=1000))
                page.wait_for_function("Math.abs(document.getElementById('adminSidebar').getBoundingClientRect().right-innerWidth)<1")
                page.screenshot(path=str(OUT/'owner-access.png'),full_page=True)
                results.append('Owner controls fit desktop and mobile without horizontal scrolling')
                assert not errors,errors
                browser.close()
        finally:
            case.tearDown()
    data=dict(passed=True,checks=len(results),results=results)
    (OUT/'results.json').write_text(json.dumps(data,ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps(data,ensure_ascii=True))


if __name__=='__main__':main()
