"""Desktop/mobile acceptance for bulk linking, with Flask and all sends isolated."""
import json,os,sys,tempfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
OUT=ROOT/'outputs/product-linking-nov06';OUT.mkdir(exist_ok=True)


def main():
    checks=[]
    with tempfile.TemporaryDirectory(dir=OUT) as tmp,patch.dict(os.environ,dict(DATA_DIR=tmp,
        DB_PATH=str(Path(tmp)/'sales.db'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1',
        OPENROUTER_API_KEY='',DASHBOARD_PASSWORD='local-only',STORAGE_RESTORE_REQUIRED='0')),\
        patch('dotenv.load_dotenv',return_value=False),patch('requests.sessions.Session.request',side_effect=RuntimeError('Real network disabled')):
        from account_app.tests.test_product_linking_nov06 import ProductLinkingApi
        case=ProductLinkingApi();case.setUp()
        try:
            case.seed();case.seed('khuyoot')
            catalog_rows=lambda:json.loads(case.catalog.read_text(encoding='utf8'))
            with sync_playwright() as w:
                browser=w.chromium.launch(channel='msedge',headless=True)
                page=browser.new_page(viewport={'width':1500,'height':1080});errors=[]
                page.on('pageerror',lambda error:errors.append(str(error)))
                def route(route):
                    req=route.request;url=urlsplit(req.url)
                    if url.hostname!='product-linking.test':
                        if url.hostname in {'cdn.jsdelivr.net','fonts.googleapis.com','fonts.gstatic.com'}:route.continue_()
                        else:route.abort()
                        return
                    response=case.client.open(url.path+('?' +url.query if url.query else ''),method=req.method,
                        data=req.post_data,headers=dict(req.headers))
                    route.fulfill(status=response.status_code,body=response.data,content_type=response.content_type)
                page.route('**/*',route)
                def go(path):
                    page.goto('http://product-linking.test'+path,wait_until='domcontentloaded')
                go('/product-links?store_id=default')
                page.wait_for_function("document.querySelectorAll('.link-row').length===2")
                assert page.locator('#linkSave').is_disabled()
                assert page.locator('#linkType option[value=tracksuits]').count()==1
                checks.append('An empty selection cannot save; tracksuits and suits have distinct classification options')
                page.locator('#linkSelectVisible').click();page.locator('#linkAudience').select_option('boys')
                page.locator('#linkType').select_option('tracksuits');page.locator('#linkApplyUses').check()
                page.locator('#linkUses input[value=home]').check();page.locator('#linkUses input[value=winter]').check()
                page.locator('#linkApplyGroups').check();page.locator('#linkGroups').fill('ولادي بيتي شتوي')
                assert page.locator('#linkSave').is_enabled()
                before=case.catalog.read_bytes()
                page.locator('#linkPreviewSource').select_option('A');page.locator('#linkPreviewAge').fill('36')
                page.locator('#linkPreviewUse').select_option('home');page.locator('#linkPreviewButton').click()
                page.wait_for_function("document.querySelector('#linkPreviewResult').textContent.includes('تراك B')")
                assert case.catalog.read_bytes()==before
                checks.append('Draft preview links both selected products, respects a three-year-old homewear request, and does not save')
                page.evaluate("document.querySelector('.link-panel').scrollTop=0;window.scrollTo(0,0)")
                page.screenshot(path=str(OUT/'linking-desktop.png'))
                page.locator('#linkSave').click()
                page.wait_for_function("document.querySelector('#linkSaveStatus').textContent.includes('تم حفظ التصنيف')")
                assert all(p['occasions']==['home','winter'] for p in catalog_rows() if p['store_id']=='default')
                assert all(not p['occasions'] for p in catalog_rows() if p['store_id']=='khuyoot')
                checks.append('One explicit save classifies the selected store only, leaving the other store unchanged')
                page.locator('#linkAudienceFilter').select_option('girls')
                assert page.locator('.link-row').count()==0
                page.locator('#linkAudienceFilter').select_option('boys');page.locator('#linkUseFilter').select_option('home')
                assert page.locator('.link-row').count()==2
                page.locator('#linkSearch').fill('تراك A');page.locator('#linkSelectVisible').click()
                assert page.locator('#linkSelectedCount').inner_text()=='1 محدد'
                checks.append('Audience, use and search filters narrow the list; select-visible never selects hidden products')
                page.locator('#linkAudience').select_option('girls')
                case.client.put('/api/products/manage/A',headers=case.headers,json={'price':'17000'})
                page.locator('#linkSave').click()
                page.wait_for_function("document.querySelector('#linkSaveStatus').dataset.error==='true'")
                assert 'تغيّرت' in page.locator('#linkSaveStatus').inner_text()
                assert next(p for p in catalog_rows() if p['store_id']=='default' and p['product_id']=='A')['audience']=='boys'
                checks.append('A stale catalogue is rejected without overwriting a newer price or classification')
                page.locator('#linkRefresh').click()
                page.wait_for_function("linkProducts.find(p=>p.product_id==='A').price==='17000'")
                page.locator('#linkSave').click()
                page.wait_for_function("document.querySelector('#linkSaveStatus').textContent.includes('تم حفظ التصنيف')")
                assert next(p for p in catalog_rows() if p['store_id']=='default' and p['product_id']=='A')['price']=='17000'
                checks.append('After a deliberate refresh, the classification saves while preserving the newer commercial data')
                go('/products?store_id=default&product_id=A')
                page.wait_for_function("currentProductId==='A'")
                page.locator('#tab-fit').click()
                assert page.locator('#productType').input_value()=='tracksuits'
                assert page.locator('#productOccasions input[value=home]').is_checked()
                assert page.locator('#manageProductLinks').is_visible()
                checks.append('Product deep links open the intended product and show its use cases beside audience and type')
                go('/product-links?store_id=default');page.wait_for_function('linkProducts.length===2')
                page.locator('#linkStore').select_option('khuyoot')
                page.wait_for_url('**store_id=khuyoot');page.wait_for_function('linkProducts.length===2')
                assert page.locator('#linkSelectedCount').inner_text()=='0 محدد'
                assert page.locator('#linkMissingCount').inner_text()=='2'
                checks.append('Changing stores resets selection and loads that store’s own unclassified products')
                page.set_viewport_size({'width':390,'height':844})
                page.wait_for_function("getComputedStyle(document.querySelector('#adminSidebar')).visibility==='hidden'")
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
                bounds=page.locator('.link-library').bounding_box()
                assert bounds['x']>=0 and bounds['x']+bounds['width']<=391
                page.screenshot(path=str(OUT/'linking-mobile.png'),full_page=True)
                page.locator('#linkSelectVisible').click();page.locator('#linkApplyGroups').check()
                page.locator('#linkGroups').fill('مجموعة من الموبايل')
                page.locator('#linkSave').click()
                page.wait_for_function("document.querySelector('#linkSaveStatus').textContent.includes('تم حفظ التصنيف')")
                checks.append('The mobile layout has no horizontal overflow and supports selecting, grouping and saving')
                assert not errors,errors
                checks.append('No browser JavaScript exceptions; all tests use synthetic products and intercept application requests')
                browser.close()
        finally:case.tearDown()
    result=dict(passed=True,checks=len(checks),results=checks)
    (OUT/'browser-results.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps(result,ensure_ascii=True))


if __name__=='__main__':main()
