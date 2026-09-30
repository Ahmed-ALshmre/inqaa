from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright

from account_app.tests.test_inbox_filters import InboxFilterTests


InboxFilterTests.setUpClass()
try:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel='msedge')
        page = browser.new_page(viewport={'width': 390, 'height': 780})
        errors = []

        def route_request(route):
            url = urlsplit(route.request.url)
            if url.hostname != 'dashboard-scroll.test':
                route.continue_()
                return
            path = url.path + ('?' + url.query if url.query else '')
            response = InboxFilterTests.client.get(path)
            route.fulfill(status=response.status_code, body=response.data,
                          content_type=response.content_type)

        page.route('**/*', route_request)
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto('http://dashboard-scroll.test/dashboard', wait_until='domcontentloaded')
        page.wait_for_function('allConversations.length === 20')
        page.locator('#customerList').evaluate('element => { element.scrollTop = element.scrollHeight; element.dispatchEvent(new Event("scroll")); }')
        page.wait_for_function('allConversations.length === 25')
        page.evaluate('loadConversations(false)')
        page.wait_for_timeout(300)
        assert page.locator('.customer-item').count() == 25
        assert page.evaluate('new Set(allConversations.map(row => row.sender_id)).size') == 25
        assert page.locator('#inboxResultCount').inner_text() == '25 محادثة'
        assert not errors, errors
        browser.close()
finally:
    InboxFilterTests.tearDownClass()
