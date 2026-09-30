import os,sys,tempfile,sqlite3,zipfile,json
from pathlib import Path
from unittest.mock import patch
root=Path.cwd();sys.path.insert(0,str(root))
with tempfile.TemporaryDirectory(prefix='lamsa-close-review-') as directory:
 p=Path(directory)
 with zipfile.ZipFile(r'C:\Users\Ghost الشبح\Downloads\lamsa-store-backup-20260916-105632.zip') as z:
  for name in ['sales.db','products.json']:p.joinpath(name).write_bytes(z.read(name))
 os.environ.update(DATA_DIR=directory,DB_PATH=str(p/'sales.db'),PRODUCTS_FILE=str(p/'products.json'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
 with patch('requests.sessions.Session.request',side_effect=AssertionError('Network forbidden')):
  from account_app import app as m
  client=m.app.test_client()
  with client.session_transaction() as s:s['dashboard_authenticated']=True
  before=client.get('/api/conversations?status=problems&limit=10000').json
  with m.app.app_context():
   db=m.get_db()
   counts_before={t:db.execute('select count(*) from '+t).fetchone()[0] for t in ['customers','messages','orders','human_reviews','problem_reports']}
   paused_before=[tuple(r) for r in db.execute('select sender_id,enabled from customer_ai_settings order by sender_id')]
  result=client.post('/api/maintenance/close_human_reviews')
  assert result.status_code==200,result.json
  after=client.get('/api/conversations?status=problems&limit=10000').json
  with m.app.app_context():
   db=m.get_db()
   counts_after={t:db.execute('select count(*) from '+t).fetchone()[0] for t in counts_before}
   paused_after=[tuple(r) for r in db.execute('select sender_id,enabled from customer_ai_settings order by sender_id')]
  assert counts_before==counts_after
  assert paused_before==paused_after
  assert after['conversations']==[]
  again=client.post('/api/maintenance/close_human_reviews').json
  assert again['closed_conversations']==0
  summary={'filter_before':len(before['conversations']),'filter_after':len(after['conversations']),'operation':result.json,'rows_preserved':counts_after,'ai_states_unchanged':paused_before==paused_after,'second_click':again,'external_requests':0}
  (root/'outputs/close-human-reviews-20260916/simulation.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
  print(json.dumps(summary,ensure_ascii=False))
