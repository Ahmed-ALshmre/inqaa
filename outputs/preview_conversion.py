import os, tempfile, sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
root=tempfile.TemporaryDirectory()
os.environ.update(DATA_DIR=root.name,DB_PATH=str(Path(root.name)/"preview.db"),BOOKINGS_FILE=str(Path(root.name)/"bookings.jsonl"),ENABLE_BACKGROUND_JOBS="0",DISABLE_CLIP="1")
import account_app.app as m
from flask import session, redirect
with m.app.app_context():
 db=m.get_db()
 m.activate_conversion_plan(db)
 now=m.now_baghdad_iso()
 for i in range(34):
  sid=f"default::preview-{i}"
  db.execute("INSERT INTO customers(sender_id,store_id,name,lead_score) VALUES(?,'default',?,80)",(sid,f"زبون تجريبي {i+1}"))
  db.execute("INSERT INTO messages(sender_id,store_id,direction,message_type,text,created_at) VALUES(?,'default','incoming','text','اريد قياس 44',?)",(sid,now))
  if i<3:db.execute("INSERT INTO orders(sender_id,store_id,status,created_at) VALUES(?,'default','new',?)",(sid,now))
  if i in (5,6):db.execute("INSERT INTO human_reviews(sender_id,store_id,status,reason,created_at) VALUES(?,'default','pending','تأكيد تفاصيل الحجز',?)",(sid,now))
 db.commit()
@m.app.route('/preview-growth')
def preview_login():
 session['dashboard_authenticated']=True
 return redirect('/settings/followup')
m.app.run(host='127.0.0.1',port=5078,debug=False,use_reloader=False)
