"""Loopback-only store switch preview using synthetic staff and temporary storage."""
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main():
    with tempfile.TemporaryDirectory(prefix='store-ai-preview-') as directory:
        env = dict(DATA_DIR=directory, DB_PATH=str(Path(directory)/'sales.db'),
                   PRODUCTS_FILE=str(Path(directory)/'products.json'), ENABLE_BACKGROUND_JOBS='0',
                   DISABLE_CLIP='1', STORAGE_RESTORE_REQUIRED='0', OPENROUTER_API_KEY='',
                   TELEGRAM_BOT_TOKEN='', MANYCHAT_API_KEY='', DASHBOARD_PASSWORD='preview-only',
                   ZERNIO_AL_FATENA_ENABLED='0', ZERNIO_BARAAH_KIDS_ENABLED='0')
        with patch.dict(os.environ, env), patch('dotenv.load_dotenv', return_value=False), patch(
                'requests.sessions.Session.request', side_effect=RuntimeError('Preview blocks external calls')):
            from account_app import app as m
            from flask import redirect, session
            from werkzeug.security import generate_password_hash
            m.app.config['TEMPLATES_AUTO_RELOAD'] = True
            with m.app.app_context():
                db = m.get_db()
                m.staff_tables(db)
                for sid, name, enabled in [('default','لمسة ستور',True),('al-fatena','الفاتنة',False),
                        ('baraah-kids','عالم البراءة',True),('khuyoot','خيوط',True),('golden-threads','خيوط الذهب جملة',False)]:
                    m.ensure_store(db,sid,name)
                    m.set_store_ai_enabled(db,sid,enabled)
                for username, permissions in [('agent','["reply"]'),('viewer','[]')]:
                    db.execute('INSERT INTO staff_accounts(username,name,password_hash,permissions) VALUES(?,?,?,?)',
                               (username,'موظف تجريبي',generate_password_hash('preview-only-password'),permissions))
                db.commit()

            @m.app.get('/preview-login/<role>')
            def preview_login(role):
                session.clear()
                session['dashboard_authenticated'] = True
                if role != 'owner':
                    row=m.get_db().execute('SELECT id,version FROM staff_accounts WHERE username=?',(role,)).fetchone()
                    if not row:return 'Unknown preview role',404
                    session['staff_id']=row['id'];session['staff_version']=row['version']
                return redirect('/dashboard')

            from werkzeug.serving import make_server
            server=make_server('127.0.0.1',5119,m.app,threaded=True)
            print('Store AI preview ready: http://127.0.0.1:5119/preview-login/agent',flush=True)
            server.serve_forever()


if __name__ == '__main__':
    main()
