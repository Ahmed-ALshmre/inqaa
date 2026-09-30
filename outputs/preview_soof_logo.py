from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright

from account_app.tests.test_inbox_filters import InboxFilterTests


InboxFilterTests.setUpClass()
try:
    public_client = InboxFilterTests.module.app.test_client()
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel='msedge')
        errors = []
        for width in (1440, 390):
            page = browser.new_page(viewport={'width': width, 'height': 900})

            def route_request(route):
                url = urlsplit(route.request.url)
                if url.hostname != 'soof-logo.test':
                    route.continue_()
                    return
                path = url.path + ('?' + url.query if url.query else '')
                client = public_client if url.path == '/login' else InboxFilterTests.client
                response = client.get(path)
                route.fulfill(status=response.status_code, body=response.data,
                              content_type=response.content_type)

            page.route('**/*', route_request)
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.goto('http://soof-logo.test/dashboard', wait_until='domcontentloaded')
            page.locator('[data-mascot-action]').click()
            assert 'soof-mascot-wave.png' in page.locator('[data-soof-mascot]').get_attribute('src')
            page.evaluate("document.dispatchEvent(new Event('soof:celebrate'))")
            assert 'soof-mascot-celebrate.png' in page.locator('[data-soof-mascot]').get_attribute('src')
            page.screenshot(path=f'outputs/soof-dashboard-{width}.png')
            page.goto('http://soof-logo.test/login', wait_until='domcontentloaded')
            assert 'soof-mascot-wave.png' in page.locator('.login-brand-mark img').get_attribute('src')
            page.screenshot(path=f'outputs/soof-login-{width}.png')
            page.close()
        assert not errors, errors
        browser.close()
finally:
    InboxFilterTests.tearDownClass()
