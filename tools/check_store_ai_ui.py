"""Browser acceptance against preview_store_ai.py; only synthetic loopback storage."""
import json
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

BASE='http://127.0.0.1:5119'
OUT=Path('outputs/store-ai-qa');OUT.mkdir(parents=True,exist_ok=True)


def main():
    results=[]
    with sync_playwright() as p:
        browser=p.chromium.launch(channel='msedge',headless=True)
        admin=browser.new_context(service_workers='block')
        setup=admin.new_page();setup.goto(BASE+'/preview-login/owner',wait_until='domcontentloaded')
        token=setup.locator('meta[name=csrf-token]').get_attribute('content')
        for sid,enabled in [('default',True),('al-fatena',False),('baraah-kids',True),('khuyoot',True),('golden-threads',False)]:
            response=admin.request.post(BASE+f'/api/stores/{sid}/ai',data={'enabled':enabled},headers={'X-CSRF-Token':token})
            assert response.ok
        admin.close()
        context=browser.new_context(viewport={'width':1440,'height':960},service_workers='block')
        page=context.new_page();errors=[]
        page.on('pageerror',lambda error:errors.append(str(error)))
        page.goto(BASE+'/preview-login/agent',wait_until='domcontentloaded')
        expect(page.locator('#storeAIList [role=switch]')).to_have_count(5)
        assert page.evaluate('currentSenderId') is None
        page.locator('.store-ai-shortcut').click()
        expect(page.locator('#dashboardDrawer')).to_have_class('dashboard-drawer open')
        expect(page.locator('#storeAIQuickCount')).to_have_text('3/5')
        page.screenshot(path=str(OUT/'desktop.png'))
        page.locator('#storeAIControls').screenshot(path=str(OUT/'switches.png'))
        results.append('Reply staff sees all five switches before opening any conversation')
        target=page.locator('[data-store-id=default] [role=switch]')
        expect(target).to_have_attribute('aria-checked','true')
        posts=[]
        def delay_write(route):
            if route.request.method=='POST':
                posts.append(route.request.post_data_json)
                expect(target).to_be_disabled()
                route.continue_()
            else:route.continue_()
        page.route('**/api/stores/default/ai',delay_write)
        target.click()
        expect(target).to_have_attribute('aria-checked','false')
        assert len(posts)==1 and posts[0]=={'enabled':False}
        states=page.request.get(BASE+'/api/stores').json()['stores']
        assert next(s for s in states if s['store_id']=='al-fatena')['ai_enabled'] is False
        assert next(s for s in states if s['store_id']=='baraah-kids')['ai_enabled'] is True
        assert page.request.get(BASE+'/api/settings/ai').status==403
        results.append('Switch persists the selected store only; staff retains no settings access')
        page.unroute('**/api/stores/default/ai',delay_write)
        target.focus();target.press('Space')
        expect(target).to_have_attribute('aria-checked','true')
        results.append('Keyboard operation restores store state')

        def fail_write(route):route.fulfill(status=500,json={'ok':False,'error':'فشل تجريبي في الحفظ'})
        page.route('**/api/stores/al-fatena/ai',fail_write)
        fatena=page.locator('[data-store-id=al-fatena] [role=switch]')
        fatena.click();expect(fatena).to_be_enabled()
        expect(page.locator('#toastContainer')).to_contain_text('فشل تجريبي في الحفظ')
        expect(fatena).to_have_attribute('aria-checked','false')
        page.unroute('**/api/stores/al-fatena/ai',fail_write)
        results.append('Save failure retains confirmed state, exposes error and permits retry')

        # Complete an older refresh after a write: stale data cannot undo the switch.
        saved={'route':None,'response':None}
        def hold_first_read(route):
            if saved['route'] is None:
                saved['route']=route;saved['response']=route.fetch()
            else:route.continue_()
        page.route('**/api/stores?*',hold_first_read)
        page.evaluate('void refreshStoreAIStates(false)')
        page.wait_for_timeout(150)
        assert saved['route'] is not None
        khuyoot=page.locator('[data-store-id=khuyoot] [role=switch]')
        khuyoot.click();expect(khuyoot).to_have_attribute('aria-checked','false')
        saved['route'].fulfill(response=saved['response'])
        page.wait_for_timeout(150)
        expect(khuyoot).to_have_attribute('aria-checked','false')
        page.unroute('**/api/stores?*',hold_first_read)
        results.append('A delayed pre-write refresh cannot overwrite the saved switch state')

        page.set_viewport_size({'width':390,'height':844})
        page.evaluate("document.querySelectorAll('#toastContainer .toast').forEach(el => bootstrap.Toast.getOrCreateInstance(el).hide())")
        expect(page.locator('#toastContainer .toast')).to_have_count(0)
        page.screenshot(path=str(OUT/'mobile.png'))
        assert page.locator('.store-ai-panel').evaluate('(e)=>e.getBoundingClientRect().right<=innerWidth')
        assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
        results.append('Mobile layout fits a 390px viewport')
        viewer=browser.new_context(viewport={'width':1100,'height':850},service_workers='block')
        readonly=viewer.new_page();readonly.goto(BASE+'/preview-login/viewer',wait_until='domcontentloaded')
        expect(readonly.locator('#storeAIList [role=switch]')).to_have_count(5)
        assert readonly.locator('#storeAIList [role=switch]:disabled').count()==5
        token=readonly.locator('meta[name=csrf-token]').get_attribute('content')
        response=readonly.request.post(BASE+'/api/stores/default/ai',data={'enabled':False},headers={'X-CSRF-Token':token})
        assert response.status==403
        results.append('Read-only employee can view states but cannot mutate through UI or API')
        assert not errors,errors
        (OUT/'results.json').write_text(json.dumps({'passed':results,'browser_errors':errors},ensure_ascii=False,indent=2),encoding='utf-8')
        print(json.dumps({'passed':len(results),'checks':results,'browser_errors':errors},ensure_ascii=False))
        browser.close()


if __name__=='__main__':main()
