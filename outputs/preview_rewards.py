import os,tempfile
from pathlib import Path
from unittest.mock import patch
from playwright.sync_api import sync_playwright
with tempfile.TemporaryDirectory() as d:
 os.environ.update(DATA_DIR=d,DB_PATH=str(Path(d)/'tests.db'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
 from account_app.tests.test_rewards import RewardTests
 t=RewardTests();t.setUp()
 try:
  t.employee();t.review('sample',600);t.close('sample');t.review('sample2',60);t.close('sample2')
  with sync_playwright() as p:
   browser=p.chromium.launch(channel='msedge')
   page=browser.new_page(viewport={'width':1440,'height':1000},device_scale_factor=1)
   def local(route):
    from urllib.parse import urlsplit
    u=urlsplit(route.request.url)
    if u.hostname!='reward.test':
     route.continue_();return
    response=t.client.get(u.path+('?' + u.query if u.query else ''))
    route.fulfill(status=response.status_code,body=response.data,content_type=response.content_type)
   page.route('**/*',local)
   page.goto('http://reward.test/rewards');page.wait_for_selector('.reward-cards');page.screenshot(path='outputs/rewards-desktop.png',full_page=True)
   page.set_viewport_size({'width':390,'height':844});page.reload();page.wait_for_selector('.reward-cards');page.screenshot(path='outputs/rewards-mobile.png',full_page=True)
   page.goto('http://reward.test/dashboard');page.wait_for_selector('[data-credit-balance]');page.wait_for_timeout(500);page.screenshot(path='outputs/rewards-dashboard-mobile.png')
   browser.close()
 finally:t.tearDown()

