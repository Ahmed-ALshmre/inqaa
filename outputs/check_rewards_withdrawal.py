import os, tempfile, json
from pathlib import Path
from urllib.parse import urlsplit
from playwright.sync_api import sync_playwright
with tempfile.TemporaryDirectory() as directory:
    os.environ.update(DATA_DIR=directory, DB_PATH=str(Path(directory)/'test.db'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
    from account_app.tests.test_rewards import RewardTests
    from account_app import app as m
    fixture=RewardTests();fixture.setUp()
    try:
        fixture.employee()
        for index in range(5):
            fixture.review('reward-'+str(index));fixture.close('reward-'+str(index))
        with sync_playwright() as p:
            browser=p.chromium.launch(channel='msedge')
            page=browser.new_page(viewport={'width':390,'height':844},is_mobile=True,has_touch=True)
            def route(r):
                u=urlsplit(r.request.url)
                if u.hostname!='reward.test':r.fulfill(status=200,body='');return
                response=fixture.client.open(u.path,method=r.request.method,data=r.request.post_data,headers={k:v for k,v in r.request.headers.items() if k in ['content-type','x-csrf-token']})
                r.fulfill(status=response.status_code,body=response.data,content_type=response.content_type)
            page.route('**/*',route)
            page.goto('http://reward.test/rewards')
            page.locator('.reward-celebration').wait_for()
            page.wait_for_timeout(800)
            assert page.locator('[data-credit-progress]').first.get_attribute('max')=='10'
            page.screenshot(path='outputs/rewards-milestone-mobile.png')
            page.on('dialog',lambda d:d.accept())
            page.locator('[data-withdraw]').click()
            page.wait_for_function("document.querySelector('[data-withdraw]').disabled")
            page.wait_for_timeout(500)
            assert fixture.client.get('/api/rewards').json['reserved']>0
            page.reload()
            page.locator('[data-withdraw]').wait_for()
            assert page.locator('.reward-celebration').count()==0
            assert page.locator('[data-withdraw]').is_disabled()
            page.screenshot(path='outputs/rewards-withdrawal-mobile.png',full_page=True)
            browser.close()
        print('PASS: mobile milestone, no repeat on reload, withdrawal saved and button disabled')
    finally:fixture.tearDown()
