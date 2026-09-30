import os,sys,json,tempfile,socket
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
with tempfile.TemporaryDirectory() as d, patch('requests.sessions.Session.request',side_effect=RuntimeError('Network blocked')), patch('socket.socket.connect',side_effect=RuntimeError('Network blocked')):
 os.environ.update(DATA_DIR=d,DB_PATH=str(Path(d)/'audit.db'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
 from account_app.tests.test_checkout_regressions import CheckoutRegressionTests,CATALOG
 from account_app.checkout import is_confirmation,contact_fields
 CheckoutRegressionTests.setUpClass()
 results=[]
 for phrase in ['اي حياتي','نعم','تمام ثبتي','يي عيني ثبتي','ثبتي الحجز يمعوده','لا تثبتين','اي بس غيري القياس']:
  t=CheckoutRegressionTests();t.setUp()
  try:
   result={'order':t.data(True)}
   _,reply=t.m.create_order_if_valid(t.db,t.sender,result,None)
   t.m.saved_checkout_reply(t.db,t.ev,reply,{}, {'checkout_proposal':result['_checkout_proposal']})
   t.incoming(phrase)
   accepted=t.m.accept_checkout_proposal(t.db,t.ev,[],CATALOG)
   actual=bool(accepted and accepted.get('meta',{}).get('order_created'))
   expected=phrase not in ['لا تثبتين','اي بس غيري القياس']
   results.append(dict(case=phrase,expected=expected,actual=actual,passed=actual==expected,orders=t.db.execute('select count(*) from orders where sender_id=?',(t.sender,)).fetchone()[0]))
  finally:t.tearDown()
 CheckoutRegressionTests.tearDownClass()
 for phrase in ['البصره / القبله / حي القائم','البصرة / القبلة / حي القائم']:
  actual=contact_fields(phrase)
  results.append(dict(case=phrase,expected='البصرة',actual=actual,passed=actual.get('province')=='البصرة'))
Path(__file__).with_name('targeted-results.json').write_text(json.dumps(results,ensure_ascii=False,indent=2),encoding='utf-8')
print(json.dumps(results,ensure_ascii=False,indent=2))
