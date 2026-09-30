import os,sys,tempfile
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
root=tempfile.TemporaryDirectory()
os.environ.update(DATA_DIR=root.name,DB_PATH=str(Path(root.name)/'preview.db'),ENABLE_BACKGROUND_JOBS='0',DISABLE_CLIP='1')
import account_app.app as m
from flask import session,redirect
m.requests.sessions.Session.request=lambda *a,**k:(_ for _ in ()).throw(RuntimeError('No outbound services in preview'))
@m.app.route('/preview-login')
def preview_login():
 session['dashboard_authenticated']=True
 return redirect('/orders')
with m.app.app_context():
 db=m.get_db()
 for i in range(125):
  sender='preview-'+str(i)
  db.execute('insert into customers(sender_id,name,store_id,first_seen_at) values(?,?,?,?)',(sender,'زبونة تجريبية '+str(i),'default',m.now_baghdad_iso()))
  db.execute('insert into orders(sender_id,customer_name,phone,province,address,product_name,size,status,created_at,store_id,product_total,delivery_fee,total_amount) values(?,?,?,?,?,?,?,?,?,?,?,?,?)',(sender,'زبونة تجريبية '+str(i),'07700000000','البصرة','القبلة شارع تجريبي طويل لاختبار التفاف النص والوصول إلى جميع إجراءات الطلب','تنورة وقطعتان للاختبار','44','new',m.now_baghdad_iso(),'default',24000,5000,29000))
 db.commit()
m.app.run(host='127.0.0.1',port=5101,use_reloader=False)
