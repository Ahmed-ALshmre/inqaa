import os,sys,tempfile,shutil,time,json
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
with tempfile.TemporaryDirectory() as root:
 shutil.copyfile(Path(__file__).with_name('sales.db'),Path(root)/'copy.db')
 os.environ.update(DATA_DIR=root,DB_PATH=str(Path(root)/'copy.db'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
 import account_app.app as m
 client=m.app.test_client()
 with client.session_transaction() as s:s['dashboard_authenticated']=True
 start=time.perf_counter();result=client.get('/api/conversations?status=system_issues&limit=2000')
 assert result.status_code==200,result.status_code
 data=result.get_json();print(json.dumps({'system_candidates':len(data['conversations']),'elapsed_seconds':round(time.perf_counter()-start,3)},ensure_ascii=False))
 for status in ['problems','all']:
  r=client.get('/api/conversations?status='+status+'&limit=2000');assert r.status_code==200
  print(status,len(r.get_json()['conversations']))
 with m.app.app_context():
  m.init_db();assert {'product_total','delivery_fee','total_amount'} <= {r[1] for r in m.get_db().execute('pragma table_info(orders)')}
  assert m.get_db().execute('pragma integrity_check').fetchone()[0]=='ok'
 print('Backup copy migration, repeated initialization and filters passed')
