from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright

from account_app.tests.test_rewards import RewardTests


fixture = RewardTests()
fixture.setUp()
try:
    fixture.employee()
    for index in range(5):
        sender = f'mascot-{index}'
        fixture.review(sender, 60)
        assert fixture.close(sender).status_code == 200
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel='msedge')
        page = browser.new_page(viewport={'width': 390, 'height': 900})
        errors = []

        def route_request(route):
            url = urlsplit(route.request.url)
            if url.hostname != 'soof-reward.test':
                route.continue_()
                return
            path = url.path + ('?' + url.query if url.query else '')
            response = fixture.client.get(path)
            route.fulfill(status=response.status_code, body=response.data,
                          content_type=response.content_type)

        page.route('**/*', route_request)
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto('http://soof-reward.test/dashboard', wait_until='domcontentloaded')
        page.locator('.reward-celebration-card .reward-mascot').wait_for(timeout=10000)
        assert 'soof-mascot-celebrate.png' in page.locator('[data-soof-mascot]').get_attribute('src')
        page.screenshot(path='outputs/soof-reward-mobile.png')
        assert not errors, errors
        browser.close()
finally:
    fixture.tearDown()
