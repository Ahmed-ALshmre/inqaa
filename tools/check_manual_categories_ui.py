"""Managed categories/groups in the real editor, against an isolated Flask client."""
import json,os,sys,tempfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
OUT=ROOT/'outputs/manual-categories';OUT.mkdir(exist_ok=True)


def main():
    checks=[]
    with tempfile.TemporaryDirectory(dir=OUT) as temp,patch.dict(os.environ,dict(DATA_DIR=temp,
        DB_PATH=str(Path(temp)/'sales.db'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1',
        OPENROUTER_API_KEY='',STORAGE_RESTORE_REQUIRED='0',DASHBOARD_PASSWORD='local-only')), \
        patch('dotenv.load_dotenv',return_value=False), \
        patch('requests.sessions.Session.request',side_effect=RuntimeError('External delivery disabled')):
        from account_app.tests.test_manual_categories_nov05 import ManualCategoriesApi
        case=ManualCategoriesApi();case.setUp()
        try:
            with sync_playwright() as p:
                browser=p.chromium.launch(channel='msedge',headless=True)
                page=browser.new_page(viewport={'width':1440,'height':1050});errors=[]
                page.on('pageerror',lambda error:errors.append(str(error)))
                def route(route):
                    req=route.request;url=urlsplit(req.url)
                    if url.hostname!='categories.test':route.continue_();return
                    path=url.path+('?' +url.query if url.query else '')
                    response=case.client.open(path,method=req.method,data=req.post_data,headers=dict(req.headers))
                    route.fulfill(status=response.status_code,body=response.data,content_type=response.content_type)
                page.route('**/*',route)
                def goto(path):page.goto('http://categories.test'+path,wait_until='domcontentloaded')
                goto('/product-categories?store_id=default')
                page.wait_for_function("document.querySelectorAll('.classification-row').length>0")
                page.locator('#classificationName').fill('حقائب يومية')
                page.locator('#classificationFamily').select_option('bags')
                assert page.locator('#classificationSize').input_value()=='none'
                page.locator('#saveClassification').click()
                page.wait_for_function("document.querySelector('#classificationFormStatus').textContent.includes('تم الحفظ')")
                assert page.locator('.classification-row h3',has_text='حقائب يومية').count()==1
                checks.append('A manually entered bag category exists before any product and defaults to no size')
                page.locator('#classificationName').fill('حقائب يومية');page.locator('#saveClassification').click()
                page.wait_for_function("document.querySelector('#classificationFormStatus').dataset.error==='true'")
                assert page.locator('.classification-row h3',has_text='حقائب يومية').count()==1
                checks.append('Duplicate names show a useful inline error without creating a second category')
                page.locator('#groupsTab').click();page.locator('#classificationName').fill('حقائب الدوام');page.locator('#saveClassification').click()
                page.wait_for_function("document.querySelector('#classificationFormStatus').textContent.includes('تم الحفظ')")
                checks.append('An unused recommendation group can be created independently')
                goto('/products?store_id=default')
                page.wait_for_function("document.querySelector('#productType').options.length>11")
                def save_bag(code,name,color):
                    page.evaluate('newProduct()');page.locator('#productName').fill(name);page.locator('#productId').fill(code);page.locator('#productPrice').fill('15000');page.locator('#productStock').select_option('متوفر')
                    page.locator('#tab-fit').click();page.locator('#productType').select_option(label='حقائب يومية');page.locator('#productColors').fill(color)
                    assert page.locator('#productSizeSystem').input_value()=='none'
                    page.locator('#tab-links').click();page.locator('#existingGroupPicker').select_option(label='حقائب الدوام');page.get_by_role('button',name='إضافة',exact=True).click()
                    page.locator('#saveProductBtn').click();page.wait_for_function('(id)=>currentProductId===id&&!savingProduct',arg=code)
                save_bag('B1','حقيبة أولى','اسود');save_bag('B2','حقيبة ثانية','بيج')
                checks.append('Both product-type and existing-group pickers contain manually created unused options and save them')
                page.evaluate("editProduct('B1')");page.locator('#tab-links').click();page.locator('.catalog-preview summary').click();page.locator('#previewRecommendationsBtn').click()
                page.wait_for_function("document.querySelector('#recommendationPreview').textContent.includes('حقيبة ثانية')")
                checks.append('The real editor preview finds a compatible bag in the shared group without asking for clothing size')
                goto('/product-categories?store_id=default');page.wait_for_function("document.querySelectorAll('.classification-row').length>0")
                page.screenshot(path=str(OUT/'categories-desktop.png'))
                page.locator('.classification-row',has=page.locator('h3',has_text='حقائب يومية')).get_by_role('button',name='تعديل').click()
                page.locator('#classificationName').fill('حقائب المدرسة');page.locator('#classificationActive').uncheck();page.locator('#saveClassification').click()
                page.wait_for_function("document.querySelector('#classificationFormStatus').textContent.includes('تم الحفظ')")
                goto('/products?store_id=default');page.wait_for_function('allProducts.length===2')
                assert page.locator('#productType option',has_text='حقائب المدرسة').count()==0
                page.evaluate("editProduct('B1')")
                assert 'مخفية' in page.locator('#productType option:checked').inner_text()
                checks.append('Archiving hides a category from new products while existing products retain the renamed category')
                goto('/product-categories?store_id=khuyoot');page.wait_for_function("document.querySelectorAll('.classification-row').length>0")
                assert page.locator('.classification-row h3',has_text='حقائب المدرسة').count()==0
                checks.append('Another store has its own categories and groups')
                page.set_viewport_size({'width':390,'height':844})
                page.wait_for_timeout(200)
                assert page.evaluate('document.documentElement.scrollWidth<=window.innerWidth+2')
                page.locator('#groupsTab').click();page.locator('#classificationName').fill('مجموعة موبايل');page.locator('#saveClassification').click()
                page.wait_for_function("document.querySelector('#classificationFormStatus').textContent.includes('تم الحفظ')")
                page.screenshot(path=str(OUT/'categories-mobile.png'))
                checks.append('The mobile page has no horizontal overflow and its create form works')
                assert not errors,errors
                browser.close()
        finally:case.tearDown()
    (OUT/'browser-results.json').write_text(json.dumps(dict(passed=True,checks=checks),ensure_ascii=False,indent=2),encoding='utf8')
    print(json.dumps(dict(passed=True,checks=len(checks)),ensure_ascii=True))


if __name__=='__main__':main()
