"""Local inbox QA with disposable synthetic records and no external delivery."""
from datetime import datetime, timedelta
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def main():
    output=ROOT/'outputs'/'inbox-qa'
    output.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='runtime-',dir=output) as directory:
        env=dict(DATA_DIR=directory,DB_PATH=str(Path(directory)/'sales.db'),
                 PRODUCTS_FILE=str(Path(directory)/'products.json'),ENABLE_BACKGROUND_JOBS='0',
                 DISABLE_CLIP='1',STORAGE_RESTORE_REQUIRED='0',OPENROUTER_API_KEY='',
                 TELEGRAM_BOT_TOKEN='',MANYCHAT_API_KEY='',DASHBOARD_PASSWORD='local-preview-only')
        with patch.dict(os.environ,env),patch('dotenv.load_dotenv',return_value=False),patch(
                'requests.sessions.Session.request',side_effect=RuntimeError('External calls disabled in inbox QA')):
            from account_app import app as m
            from flask import redirect,session,request,jsonify
            m.app.config.update(TEMPLATES_AUTO_RELOAD=True)
            with m.app.app_context():
                db=m.get_db()
                db.execute("INSERT OR IGNORE INTO stores(store_id,name) VALUES('al-fatena','الفاتنة')")
                start=datetime(2026,10,27,16,0,tzinfo=m.BAGHDAD_TZ)
                names=['مريم علي','نور الهدى','هبة محمد','زينب أحمد','سارة محمود','فاطمة حسن']
                messages=['شنو الألوان المتوفرة من هذا الموديل؟','أريد الأسود قياس 44، شكد يصير ويا التوصيل؟',
                          'ممكن تفاصيل القماش والمقاسات؟','تمام حبيبتي، دزيلي صور الألوان','الصورة مطابقة للموديل؟','شكراً، تم استلام الطلب 🌷']
                for i in range(320):
                    sender=f'qa-{i:04d}'
                    store='al-fatena' if i%2 else 'default'
                    created=(start-timedelta(minutes=i*9)).isoformat()
                    m._current_store_id.set(store)
                    m.get_or_create_customer(db,sender,'qa-page','instagram' if i%3 else 'facebook')
                    db.execute('UPDATE customers SET name=?,lead_stage=?,lead_score=?,last_seen_at=? WHERE sender_id=?',
                               (names[i%len(names)]+f' {i+1}', 'booked' if i%11==0 else 'warm' if i%3 else 'new',i%100,created,sender))
                    db.execute('INSERT INTO messages(sender_id,direction,message_type,text,created_at,store_id) VALUES(?,?,?,?,?,?)',
                               (sender,'incoming','text',messages[i%len(messages)],created,store))
                    if i%3==0:
                        db.execute("INSERT INTO human_reviews(sender_id,status,reason,created_at) VALUES(?,'pending',?,?)",
                                   (sender,'مراجعة تفاصيل الموديل',created))
                db.commit()

            @m.app.get('/preview-login')
            def preview_login():
                session['dashboard_authenticated']=True
                return redirect('/dashboard')

            @m.app.post('/preview/bump')
            def bump():
                data=request.get_json() or {}
                sender=data.get('sender','qa-0300')
                with m.get_db() as db:
                    row=db.execute('SELECT store_id FROM customers WHERE sender_id=?',(sender,)).fetchone()
                    if not row:return jsonify(error='unknown synthetic customer'),404
                    db.execute('INSERT INTO messages(sender_id,direction,message_type,text,created_at,store_id) VALUES(?,?,?,?,?,?)',
                               (sender,'incoming','text',data.get('text','عندي سؤال جديد عن الموديل'),
                                (start+timedelta(days=1)).isoformat(),row['store_id']))
                return jsonify(ok=True)

            from werkzeug.serving import make_server
            server=make_server('127.0.0.1',5117,m.app,threaded=True)
            print('Inbox preview ready: http://127.0.0.1:5117/preview-login',flush=True)
            server.serve_forever()


if __name__=='__main__':main()
