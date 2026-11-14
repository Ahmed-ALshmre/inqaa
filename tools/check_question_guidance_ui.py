"""Temporary Flask/Edge acceptance: product facts and coaching, no real sends or models."""
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
OUT = ROOT / 'outputs/question-guidance-nov08'
OUT.mkdir(exist_ok=True)


def main():
    checks, errors, requests_seen = [], [], []
    with tempfile.TemporaryDirectory(dir=OUT) as folder, patch.dict(os.environ, {
        'DATA_DIR': folder, 'DB_PATH': str(Path(folder) / 'test.db'),
        'ENABLE_BACKGROUND_JOBS': '0', 'DISABLE_CLIP': '1',
        'OPENROUTER_API_KEY': '', 'STORAGE_RESTORE_REQUIRED': '0',
    }), patch('dotenv.load_dotenv', return_value=False), patch(
        'requests.sessions.Session.request', side_effect=RuntimeError('Real network disabled')
    ):
        from account_app import app as m, product_questions as questions
        from account_app.tests.test_staff_access import StaffAccessTests
        case = StaffAccessTests();case.setUp()
        try:
            db = m.get_db();db.execute('DELETE FROM product_questions');db.commit()
            m.ensure_store(db, 'khuyoot', 'خيوط')
            products = []
            for index in range(18):
                store = 'khuyoot' if index % 2 else 'default'
                product = dict(store_id=store, product_id=f'Q{index}',
                               product_name='طقم اختبار' if index % 2 else 'قميص اختبار')
                products.append(product)
                questions.capture(db, store, f'{store}::test', product,
                    'بيه بطانة لو لا؟' if index % 2 else 'قماشه ثخين لو خفيف؟',
                    ['lining' if index % 2 else 'fabric_feel'], None, m.now_baghdad_iso())
            with patch.object(m, 'load_products_from_file', return_value=products), patch.object(m, 'load_active_products', return_value=[]), sync_playwright() as w:
                browser = w.chromium.launch(channel='msedge', headless=True)
                controls = {'fail': False, 'draft': False, 'fail_load': False}

                def route(request_route):
                    request = request_route.request;url = urlsplit(request.url)
                    if url.hostname != 'question-guidance.test':
                        if url.hostname in {'cdn.jsdelivr.net', 'fonts.googleapis.com', 'fonts.gstatic.com'}:
                            request_route.continue_()
                        else:request_route.abort()
                        return
                    path = url.path + ('?' + url.query if url.query else '')
                    requests_seen.append((request.method, path))
                    if controls['fail_load'] and url.path == '/api/product-questions':
                        request_route.fulfill(status=503, json={'error': 'تعذر تحميل الأسئلة للاختبار'})
                        return
                    if request.method != 'GET':
                        assert url.path.startswith('/api/product-questions/') and url.path.endswith('/answer'), path
                        if controls['fail']:
                            controls['fail'] = False
                            request_route.fulfill(status=503, json={'error': 'تعذر الحفظ للاختبار. شرحك ما زال في الحقل.'})
                            return
                    if controls['draft'] and url.path == '/api/product-questions':
                        request.frame.page.evaluate("""() => {
                            const editor=document.createElement('textarea');editor.id='testExistingDraft';
                            editor.value='مسودة الموظفة قبل تحميل الأسئلة';document.body.prepend(editor);editor.focus();
                        }""")
                    response = case.client.open(path, method=request.method, data=request.post_data, headers=dict(request.headers))
                    request_route.fulfill(status=response.status_code, body=response.data, content_type=response.content_type)

                def page_for(**options):
                    context = browser.new_context(**options);context.route('**/*', route)
                    page = context.new_page();page.on('pageerror', lambda error: errors.append(str(error)))
                    return context, page

                def go(page, path='/dashboard'):
                    page.goto('http://question-guidance.test' + path, wait_until='domcontentloaded')

                context, page = page_for(viewport={'width': 1440, 'height': 1050})
                go(page);dialog = page.locator('#productQuestionsDialog');dialog.wait_for(state='visible')
                assert page.locator('#pqDialogList .pq-card').count() == 12
                assert '18 سؤال' in page.locator('#pqDialogCount').inner_text()
                assert {'لمسة ستور', 'خيوط'} <= {label.split(' · ')[0] for label in page.locator('#pqDialogList .pq-scope').all_inner_texts()}
                assert page.evaluate('document.activeElement.id') == 'pqDialogTitle'
                page.screenshot(path=str(OUT / 'questions-desktop.png'))
                checks.append('On opening the dashboard, a focused dialog shows a bounded queue from both stores')

                first = page.locator('#pqDialogList .pq-card').first
                fact = 'بيه بطانة قطن.'
                first.locator('textarea').fill(fact);controls['fail'] = True
                first.locator('button').click()
                first.locator('[data-error=true]').wait_for()
                assert first.locator('textarea').input_value() == fact
                saved_id = int(first.locator('textarea').get_attribute('id').replace('pqGuidance', ''))
                first.locator('button').click()
                page.wait_for_function("document.querySelectorAll('#pqDialogList .pq-card').length===11")
                row = db.execute("SELECT * FROM product_questions WHERE status='answered'").fetchone()
                assert row['guidance'] == '' and row['answer'] == fact and row['id'] == saved_id
                assert row['store_id'] == 'khuyoot'
                assert '17 سؤال' in page.locator('#pqDialogCount').inner_text()
                checks.append('A failed save retains the draft; retry saves the missing product fact in the correct store')

                other = page.locator('#pqDialogList .pq-card').first
                identity = int(other.locator('textarea').get_attribute('id').replace('pqGuidance', ''))
                old = db.execute('SELECT store_id FROM product_questions WHERE id=?', (identity,)).fetchone()
                questions.save_guidance(db, old['store_id'], identity, 'شرح جديد من موظفة أخرى.', 'اختبار', m.now_baghdad_iso())
                other.locator('textarea').fill('شرح كتبته في نافذة قديمة ولا أريد فقده.');other.locator('button').click()
                other.locator('[data-error=true]').wait_for()
                assert 'موظف آخر' in other.locator('.pq-card-status').inner_text()
                assert 'لا أريد فقده' in other.locator('textarea').input_value()
                checks.append('Concurrent staff edits are rejected without losing the current draft')

                page.locator('#pqDialogMore').click()
                page.wait_for_function("document.querySelectorAll('#pqDialogList .pq-card').length===17")
                assert 'لا أريد فقده' in other.locator('textarea').input_value()
                assert page.locator('#pqDialogMore').is_hidden()
                page.locator('#pqDialogClose').click();page.evaluate('loadStats()')
                page.wait_for_timeout(150)
                assert not dialog.is_visible()
                page.reload(wait_until='domcontentloaded');dialog.wait_for(state='visible')
                assert '16 سؤال' in page.locator('#pqDialogCount').inner_text()
                checks.append('Pagination preserves text; closing stays closed until the next page visit, which shows only pending questions')
                context.close()

                mobile, page = page_for(viewport={'width': 390, 'height': 844}, is_mobile=True, has_touch=True)
                go(page);page.locator('#productQuestionsDialog').wait_for(state='visible')
                page.wait_for_timeout(1200)
                assert not page.locator('#mobileInstallDialog').is_visible()
                assert page.locator('#productQuestionsDialog').evaluate('(e)=>e.scrollWidth<=e.clientWidth')
                assert page.locator('#productQuestionsDialog').bounding_box()['width'] <= 390
                page.screenshot(path=str(OUT / 'questions-mobile.png'))
                page.keyboard.press('Escape');assert not page.locator('#productQuestionsDialog').is_visible()
                checks.append('The mobile dialog fits one column; app installation never covers it and Escape closes it')
                mobile.close()

                context, page = page_for(viewport={'width': 1280, 'height': 900})
                controls['draft'] = True;go(page)
                page.wait_for_function('window.soofQuestionPrompt.loading===false')
                assert not page.locator('#productQuestionsDialog').is_visible()
                assert page.locator('#testExistingDraft').input_value() == 'مسودة الموظفة قبل تحميل الأسئلة'
                controls['draft'] = False;page.locator('#testExistingDraft').blur()
                page.locator('#productQuestionsDialog').wait_for(state='visible')
                checks.append('An employee already typing keeps focus and text; the queued dialog waits for blur')
                context.close()

                context, page = page_for(viewport={'width': 1280, 'height': 900})
                go(page, '/dashboard?store_id=default');page.locator('#productQuestionsDialog').wait_for(state='visible')
                assert all(label.startswith('لمسة ستور') for label in page.locator('#pqDialogList .pq-scope').all_inner_texts())
                page.locator('#pqDialogManage').click();page.wait_for_url('**/product-questions?store_id=default')
                page.locator('#pqList .pq-card').first.wait_for()
                assert page.locator('#pqList textarea').first.input_value() == ''
                page.locator('#pqStatus').select_option('answered');page.wait_for_function("document.querySelector('#pqList textarea')?.value.includes('موظفة أخرى')")
                assert 'اشرح للبوت' in page.locator('#pqList label').first.inner_text()
                page.locator('#pqList textarea').first.fill('توجيه محدّث من صفحة الأسئلة نفسها، بدون نسخ رسالة جاهزة.')
                page.locator('#pqList .pq-card').first.locator('button').click()
                page.locator('#pqList .pq-card-status').filter(has_text='تم حفظ التوجيه').wait_for()
                checks.append('A store-filtered dashboard and the management page share the explanatory editor and saved guidance')
                context.close()

                controls['fail_load'] = True
                context, page = page_for(viewport={'width': 1280, 'height': 900})
                go(page);page.wait_for_function('window.soofQuestionPrompt.loading===false')
                assert not page.locator('#productQuestionsDialog').is_visible()
                assert 'تعذر تحميل' in page.locator('#productQuestionsLink').get_attribute('title')
                checks.append('A queue-loading failure leaves the dashboard usable and provides a manual questions link')
                context.close();controls['fail_load'] = False

                injected = '<img src=x onerror="window.questionXss=1"> بيه بطانة؟'
                product = dict(store_id='default', product_id='XSS', product_name='قميص اختبار')
                products.append(product)
                questions.capture(db, 'default', 'test', product, injected, ['lining'], None, m.now_baghdad_iso())
                context, page = page_for(viewport={'width': 1280, 'height': 900})
                go(page);page.locator('#productQuestionsDialog').wait_for(state='visible')
                assert page.locator('#pqDialogList .pq-question').first.inner_text() == injected
                assert page.locator('#pqDialogList .pq-question img').count() == 0
                assert page.evaluate('window.questionXss===undefined')
                checks.append('Customer question markup is rendered as plain text without executing HTML')
                context.close()

                db.execute("UPDATE product_questions SET status='answered'");db.commit()
                context, page = page_for(viewport={'width': 390, 'height': 844}, is_mobile=True, has_touch=True)
                go(page);page.wait_for_function('window.soofQuestionPrompt.loading===false')
                assert not page.locator('#productQuestionsDialog').is_visible()
                page.locator('#mobileInstallDialog').wait_for(state='visible')
                checks.append('With no pending questions there is no empty popup, and the mobile install invitation still works')
                context.close()

                case.employee();db.execute('UPDATE staff_accounts SET permissions=? WHERE id=?', ('[]', case.id));db.commit()
                context, page = page_for(viewport={'width': 1280, 'height': 900})
                go(page);assert page.locator('#productQuestionsDialog').count() == 0
                checks.append('Read-only staff never receive the question editor or its automatic request')
                context.close();browser.close()
            assert not errors, errors
            assert db.execute('SELECT COUNT(*) FROM orders').fetchone()[0] == 0
            assert all(method == 'GET' or '/api/product-questions/' in path for method, path in requests_seen)
            (OUT / 'results.json').write_text(json.dumps({'checks': checks, 'count': len(checks), 'errors': errors}, ensure_ascii=False, indent=2), encoding='utf8')
            print(json.dumps({'passed': len(checks), 'errors': errors, 'output': str(OUT)}, ensure_ascii=False))
        finally:case.tearDown()


if __name__ == '__main__':main()
