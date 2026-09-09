import os
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
preview = tempfile.TemporaryDirectory()
os.environ.update(DATA_DIR=preview.name, DB_PATH=str(Path(preview.name)/'preview.db'), ENABLE_BACKGROUND_JOBS='0', DISABLE_CLIP='1')
import account_app.app as m
m.DASHBOARD_PASSWORD = 'local-preview-only'
def blocked(*args, **kwargs): raise RuntimeError('Preview: outbound service requests disabled')
m.requests.sessions.Session.request = blocked
with m.app.app_context():
    db=m.get_db()
    for i,platform in enumerate(['instagram','facebook','instagram','facebook']):
        sender=f'al-fatena::preview-{i}'
        m.get_or_create_customer(db,sender,'page',platform)
        db.execute('UPDATE customers SET name=?,store_id=?,store_name=? WHERE sender_id=?',(f'زبونة تجريبية {i+1}','al-fatena','الفاتنة',sender))
        for direction,text in [('incoming','السلام عليكم، متوفر هذا الموديل؟'),('outgoing','وعليكم السلام، الموديل متوفر باللون الأسود.'),('outgoing','شنو القياس اللي تلبسينه؟'),('incoming','قياس 44، شكد سعره؟')]:
            m.save_message(db,sender,direction,'text',text,None,None,None,{})
    for i in range(24):
        db.execute('INSERT INTO advisor_chat_messages(role,content,created_at) VALUES(?,?,?)',('user' if i%2==0 else 'assistant','رسالة تجريبية للمراجعة. نركز على سؤال الزبون ونحافظ على اختيار الموديل قبل الانتقال للحجز. '+str(i),m.now_baghdad_iso()))
    db.commit()
m.app.run(host='127.0.0.1',port=5099,use_reloader=False)
