import os,tempfile
from pathlib import Path
from unittest.mock import patch
from playwright.sync_api import sync_playwright
with tempfile.TemporaryDirectory() as d:
 os.environ.update(DATA_DIR=d,DB_PATH=str(Path(d)/'tests.db'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
 from account_app.tests.test_rewards import RewardTests
 t=RewardTests();t.setUp()
 try:
  t.review('sample',600);t.close('sample');t.review('sample2',60);t.close('sample2')
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
   errors=[]
   page.on('pageerror',lambda error:errors.append(str(error)))
   for width in [1440,390]:
    page.set_viewport_size({'width':width,'height':900})
    for route_name in ['dashboard','orders','products','rewards','settings']:
     page.goto('http://reward.test/'+route_name)
     page.wait_for_timeout(350)
     page.locator('[data-open-navigation]').click()
     page.locator('#navigationSearch').fill('رصيد')
     assert page.locator('#quickNavigation .page-nav-link:visible').count()==1
     page.keyboard.press('Escape')
     assert not page.locator('#quickNavigation').evaluate('(d)=>d.open')
     if route_name in ['dashboard','orders','products']:
      page.screenshot(path=f'outputs/navigation-{route_name}-{width}.png',full_page=True)
   Path('outputs/navigation-browser-check.txt').write_text('Browser errors: '+repr(errors),encoding='utf-8')
   assert not errors,errors
   browser.close()
 finally:t.tearDown()

