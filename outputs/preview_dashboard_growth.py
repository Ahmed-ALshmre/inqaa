from pathlib import Path
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright

from account_app.tests.test_dashboard_growth import DashboardGrowthTests


DashboardGrowthTests.setUpClass()
try:
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(channel='msedge')
        errors = []
        for width in (1440, 390):
            page = browser.new_page(viewport={'width': width, 'height': 900}, device_scale_factor=1)

            def route_request(route):
                url = urlsplit(route.request.url)
                if url.hostname != 'dashboard.test':
                    route.continue_()
                    return
                path = url.path + ('?' + url.query if url.query else '')
                response = DashboardGrowthTests.client.get(path)
                route.fulfill(status=response.status_code, body=response.data,
                              content_type=response.content_type)

            page.route('**/*', route_request)
            page.on('pageerror', lambda error: errors.append(str(error)))
            page.goto('http://dashboard.test/dashboard', wait_until='domcontentloaded')
            page.evaluate("document.getElementById('dashboardStatsPeriod').value = '24h'")
            page.evaluate('loadStats()')
            page.locator('#statsModal').evaluate("element => bootstrap.Modal.getOrCreateInstance(element).show()")
            page.wait_for_timeout(400)
            assert page.locator('#statConversion').inner_text() == '50.0%', page.locator('#statConversion').inner_text()
            assert page.locator('#statTotalMessages').inner_text() == '٦'
            page.locator('#dashboardStatsStore').select_option('default')
            page.wait_for_function("document.querySelector('#statConversion').textContent === '40.0%'")
            assert page.locator('#statTotalOrders').inner_text() == '٤'
            assert page.locator('#statsModal .modal-body').evaluate('element => element.scrollWidth <= element.clientWidth')
            page.screenshot(path=f'outputs/dashboard-growth-{width}.png')
            page.close()
        assert not errors, errors
        browser.close()
finally:
    DashboardGrowthTests.tearDownClass()
