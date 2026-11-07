"""Synthetic Edge checks for mobile installation and the conversation AI ribbon.

Uses a temporary Flask database. Customer delivery and model requests are blocked.
Native installation is simulated; this never installs an app on the test computer.
"""
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
OUT = ROOT / 'outputs/mobile-install-nov07'
OUT.mkdir(exist_ok=True)


def main():
    checks, errors = [], []
    with tempfile.TemporaryDirectory(dir=OUT) as tmp, patch.dict(os.environ, {
        'DATA_DIR': tmp, 'DB_PATH': str(Path(tmp) / 'sales.db'),
        'ENABLE_BACKGROUND_JOBS': '0', 'DISABLE_CLIP': '1',
        'OPENROUTER_API_KEY': '', 'STORAGE_RESTORE_REQUIRED': '0',
    }), patch('dotenv.load_dotenv', return_value=False), patch(
        'requests.sessions.Session.request', side_effect=RuntimeError('Real network blocked')
    ):
        from account_app import app as m
        from account_app.tests.test_staff_workspace_nov07 import StaffWorkspaceTests
        case = StaffWorkspaceTests()
        case.setUp()
        product = dict(product_id='UI', product_name='تنورة اختبار', price='16000',
                       stock='متوفر', status='active', sizes='38 إلى 48',
                       store_id='default', image_url=[])
        try:
            m.get_or_create_customer(m.get_db(), 'qa-a', 'page', 'facebook')
            m.save_message(m.get_db(), 'qa-a', 'incoming', 'text', 'شكد السعر؟', None, None, None, {})
            m.bind_customer_to_product(m.get_db(), 'qa-a', product, source='manual_admin')
            with patch.object(m, 'load_products_from_file', return_value=[product]), patch.object(
                m, 'load_active_products', return_value=[product]
            ), sync_playwright() as w:
                browser = w.chromium.launch(channel='msedge', headless=True)

                def route(request_route):
                    req = request_route.request
                    url = urlsplit(req.url)
                    if url.hostname != 'mobile-install.test':
                        if url.hostname in {'cdn.jsdelivr.net', 'fonts.googleapis.com', 'fonts.gstatic.com'}:
                            request_route.continue_()
                        else:
                            request_route.abort()
                        return
                    assert req.method == 'GET', f'Unexpected mutation: {url.path}'
                    response = case.client.get(url.path + ('?' + url.query if url.query else ''))
                    request_route.fulfill(status=response.status_code, body=response.data,
                                          content_type=response.content_type)

                def page_for(**kwargs):
                    context = browser.new_context(**kwargs)
                    context.route('**/*', route)
                    page = context.new_page()
                    page.on('pageerror', lambda error: errors.append(str(error)))
                    return context, page

                context, page = page_for(viewport={'width': 390, 'height': 844}, is_mobile=True,
                                         has_touch=True, device_scale_factor=1)
                page.goto('http://mobile-install.test/dashboard', wait_until='domcontentloaded')
                dialog = page.locator('#mobileInstallDialog')
                dialog.wait_for(state='visible')
                page.wait_for_timeout(350)
                box = dialog.bounding_box()
                assert abs(box['x'] + box['width'] / 2 - 195) < 2, box
                assert abs(box['y'] + box['height'] / 2 - 422) < 2, box
                page.screenshot(path=str(OUT / 'mobile-install.png'))
                checks.append('Mobile invitation is centered and fits the viewport')
                page.locator('#mobileInstallAction').click()
                assert page.locator('#mobileInstallGuide').is_visible()
                assert 'Chrome' in page.locator('#mobileInstallGuide').inner_text()
                assert page.evaluate('document.activeElement.id') == 'mobileInstallGuide'
                checks.append('Manual install instructions receive keyboard focus')
                page.locator('#mobileInstallLater').click()
                page.reload(wait_until='domcontentloaded')
                page.wait_for_timeout(1300)
                assert not dialog.is_visible()
                checks.append('Later postpones automatic invitations across page loads')
                page.wait_for_function('allConversations.length===1')
                page.evaluate("selectConversation('qa-a')")
                page.locator('#conversationAiBtn').wait_for(state='visible')
                page.wait_for_function("!document.querySelector('#chatProductCard').hidden")
                page.evaluate('globalAIEnabled=true;currentConversationAIEnabled=true;renderConversationAIToggle()')
                ai = page.locator('#conversationAiBtn')
                for width in (390, 320, 1480):
                    page.set_viewport_size({'width': width, 'height': 980 if width > 600 else 844})
                    page.wait_for_timeout(150)
                    a, card = ai.bounding_box(), page.locator('#chatProductCard').bounding_box()
                    focus = page.locator('.conversation-focus-bar').bounding_box()
                    assert a['width'] >= focus['width'] - 2, (a, focus)
                    assert a['height'] >= 78, a
                    assert card['y'] >= a['y'] + a['height'], (a, card)
                    assert card['height'] < 50, card
                    assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
                checks.append('AI ribbon takes the full row with compact product chip at 320, 390 and 1480 pixels')
                page.set_viewport_size({'width': 390, 'height': 844})
                assert ai.locator('.ribbon-emoji-stage').evaluate("e=>getComputedStyle(e).animationName") == 'soof-breathe'
                assert ai.locator('.ribbon-sparkles').is_visible()
                assert not ai.is_disabled()
                page.screenshot(path=str(OUT / 'ribbon-active.png'))
                checks.append('Active AI has a breathing emoji, sparkles and visible active status')
                page.evaluate('currentConversationAIEnabled=false;renderConversationAIToggle()')
                assert ai.locator('.ribbon-emoji-stage').evaluate("e=>getComputedStyle(e).animationName") == 'none'
                assert not ai.locator('.ribbon-sparkles').is_visible()
                assert ai.locator('.ribbon-paused-emoji').is_visible()
                assert not ai.is_disabled()
                checks.append('Conversation pause is quiet and remains operable')
                page.evaluate('globalAIEnabled=false;renderConversationAIToggle()')
                assert ai.is_disabled()
                assert 'المتجر' in page.locator('#conversationAiText').inner_text()
                page.screenshot(path=str(OUT / 'ribbon-paused.png'))
                checks.append('Store-wide pause remains disabled and explains the store switch')
                page.evaluate('globalAIEnabled=true;currentConversationAIEnabled=true;renderConversationAIToggle()')
                page.emulate_media(reduced_motion='reduce')
                assert ai.locator('.ribbon-emoji-stage').evaluate("e=>getComputedStyle(e).animationName") == 'none'
                assert not ai.locator('.ribbon-sparkles').is_visible()
                checks.append('Reduced motion disables decorative movement')
                # Fake browser availability: only a user click can open its native prompt.
                page.evaluate("""window.soofPWA.open(true);window.installCalls=0;
                  const event=new Event('beforeinstallprompt',{cancelable:true});
                  event.prompt=async()=>{window.installCalls++};
                  event.userChoice=Promise.resolve({outcome:'accepted'});
                  window.dispatchEvent(event);""")
                assert page.evaluate('installCalls') == 0
                page.locator('#mobileInstallAction').click()
                page.wait_for_function('installCalls===1 && !document.querySelector("#mobileInstallDialog").open')
                page.evaluate("window.dispatchEvent(new Event('appinstalled'))")
                assert page.evaluate("localStorage.getItem('soof.install.installed')") == '1'
                checks.append('Native install is invoked only on click and appinstalled saves completion')
                context.close()
                context, desktop = page_for(viewport={'width': 1480, 'height': 980})
                desktop.goto('http://mobile-install.test/dashboard', wait_until='domcontentloaded')
                desktop.wait_for_timeout(1300)
                assert not desktop.locator('#mobileInstallDialog').is_visible()
                checks.append('Desktop does not receive an automatic mobile invitation')
                context.close()
                context, ios = page_for(viewport={'width': 320, 'height': 568}, is_mobile=True,
                                       has_touch=True, user_agent='Mozilla/5.0 (iPhone) Version/18.0 Mobile Safari/604.1')
                ios.goto('http://mobile-install.test/dashboard', wait_until='domcontentloaded')
                ios.locator('#mobileInstallDialog').wait_for(state='visible')
                ios.locator('#mobileInstallAction').click()
                assert 'Safari' in ios.locator('#mobileInstallGuide').inner_text()
                box = ios.locator('#mobileInstallDialog').bounding_box()
                assert box['x'] >= 0 and box['width'] <= 320 and box['height'] <= 568, box
                ios.screenshot(path=str(OUT / 'ios-guide.png'))
                checks.append('iPhone guidance names Share/Add to Home Screen and fits a small phone')
                assert not errors, errors
                checks.append('No JavaScript errors or requests that send messages or change AI state')
                browser.close()
        finally:
            case.tearDown()
    result = dict(passed=True, checks=len(checks), results=checks)
    (OUT / 'browser-results.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf8')
    print(json.dumps(result, ensure_ascii=True))


if __name__ == '__main__':
    main()
