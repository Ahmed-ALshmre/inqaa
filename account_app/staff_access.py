"""Dashboard staff accounts. Permissions are enforced on every request."""
import json
import secrets
import sqlite3
import time
from flask import g, jsonify, redirect, render_template, request, session
from werkzeug.security import generate_password_hash, check_password_hash

PERMISSIONS = {'reply': 'الرد والصور وربط المنتجات', 'orders': 'إنشاء وإدارة الطلبات',
               'products': 'إدارة المنتجات', 'advisor': 'المستشار وذاكرته',
               'settings': 'إعدادات المتاجر والذكاء الاصطناعي'}


class StaffPermissionDenied(Exception):
    pass


def tables(db):
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='staff_audit'").fetchone():
        return
    db.executescript('''CREATE TABLE IF NOT EXISTS staff_accounts (
        id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE COLLATE NOCASE NOT NULL,
        name TEXT NOT NULL, password_hash TEXT NOT NULL, permissions TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1, version INTEGER NOT NULL DEFAULT 1);
        CREATE TABLE IF NOT EXISTS staff_login_limits (address TEXT PRIMARY KEY, attempts INTEGER, started REAL);
        CREATE TABLE IF NOT EXISTS staff_audit (id INTEGER PRIMARY KEY AUTOINCREMENT,
        staff_id INTEGER, method TEXT, path TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP);''')


def authenticate(db, username, password, address):
    tables(db)
    limit = db.execute('SELECT * FROM staff_login_limits WHERE address=?', (address,)).fetchone()
    if limit and time.time() - limit['started'] < 900 and limit['attempts'] >= 8:
        return None
    row = db.execute('SELECT * FROM staff_accounts WHERE username=?', (username,)).fetchone()
    if row and row['active'] and check_password_hash(row['password_hash'], password):
        db.execute('DELETE FROM staff_login_limits WHERE address=?', (address,)); db.commit()
        return row
    if not limit or time.time() - limit['started'] >= 900:
        db.execute('INSERT OR REPLACE INTO staff_login_limits VALUES(?,1,?)', (address, time.time()))
    else:
        db.execute('UPDATE staff_login_limits SET attempts=attempts+1 WHERE address=?', (address,))
    db.commit()
    return None


def required_permission(path, method):
    read = method in {'GET', 'HEAD', 'OPTIONS'}
    if path.startswith(('/static/', '/product_image/', '/catalog_image_file/', '/aud/')) or path in {'/login', '/logout', '/manifest.webmanifest', '/sw.js'}:
        return 'public'
    if path.startswith(('/api/staff', '/settings/staff', '/api/export/', '/api/import/', '/settings/maintenance')):
        return 'owner'
    if path in {'/', '/dashboard', '/api/me', '/api/conversations', '/api/products', '/api/stores'} and read:
        return 'read'
    if path.startswith('/api/conversations/'):
        tail = path.rsplit('/', 1)[-1]
        if read and tail in {'messages', 'instructions'}: return 'read'
        if tail == 'create_order': return 'orders'
        if method == 'DELETE': return 'owner'
        if tail in {'send', 'ask_ai', 'send_catalog', 'customer', 'gender', 'link_product', 'unlink_product', 'mark_reviewed', 'ai', 'media'}:
            return 'reply'
        if tail == 'save_instructions': return 'settings'
    if path == '/api/upload_image': return 'reply|products'
    if path == '/api/improve_message': return 'reply'
    if path == '/api/catalog_image' and read: return 'read'
    if path == '/api/dashboard_stats': return 'read'
    if path == '/orders' or path.startswith('/api/orders'): return 'orders'
    if path == '/products' or path.startswith('/api/products/manage'): return 'products'
    if path == '/advisor' or path == '/settings/advisor' or path.startswith('/api/advisor'): return 'advisor'
    if path.startswith(('/settings', '/api/settings', '/api/stores', '/api/followups')): return 'settings'
    return 'owner'  # New endpoints do not silently grant new staff privileges.


def install(app, get_db):
    @app.errorhandler(StaffPermissionDenied)
    def permission_denied(error):
        return jsonify(error=str(error)), 403

    def current():
        if not session.get('dashboard_authenticated'): return None
        if not session.get('staff_id'): return {'owner': True, 'name': 'المالك', 'permissions': list(PERMISSIONS)}
        tables(get_db())
        row = get_db().execute('SELECT * FROM staff_accounts WHERE id=?', (session['staff_id'],)).fetchone()
        if not row or not row['active'] or row['version'] != session.get('staff_version'):
            session.clear(); return None
        return {'id': row['id'], 'name': row['name'], 'owner': False, 'permissions': json.loads(row['permissions'])}

    @app.before_request
    def staff_guard():
        if not session.get('staff_id'): return
        person = current()
        if not person:
            return jsonify(error='انتهت صلاحية الدخول. سجل الدخول مجدداً.'), 401
        g.staff_person = person
        needed = required_permission(request.path, request.method)
        if needed not in {'read', 'public'} and not set(needed.split('|')).intersection(person['permissions']):
            if request.path.startswith('/api/'):
                return jsonify(error='ليس لديك صلاحية لهذا الإجراء'), 403
            return render_template('access_denied.html'), 403
        if request.method not in {'GET', 'HEAD', 'OPTIONS'} and needed != 'public':
            if not secrets.compare_digest(request.headers.get('X-CSRF-Token', ''), session.get('csrf_token') or secrets.token_hex(32)):
                return jsonify(error='حدّث الصفحة ثم حاول مجدداً'), 403

    @app.after_request
    def staff_audit(response):
        if getattr(g, 'staff_person', None) and request.method not in {'GET', 'HEAD', 'OPTIONS'} and response.status_code < 400:
            get_db().execute('INSERT INTO staff_audit(staff_id,method,path) VALUES(?,?,?)',
                             (g.staff_person['id'], request.method, request.path))
            get_db().commit()
        return response

    @app.context_processor
    def staff_context():
        person = current()
        if person: session.setdefault('csrf_token', secrets.token_hex(32))
        def can(permission): return bool(person and (person['owner'] or permission in person['permissions']))
        return {'can': can, 'staff_person': person, 'csrf_token': session.get('csrf_token', '')}

    @app.get('/api/me')
    def staff_me():
        person = current()
        return jsonify(person) if person else (jsonify(error='Unauthorized'), 401)

    @app.get('/settings/staff')
    def staff_page():
        person = current()
        if not person: return redirect('/login')
        if not person['owner']: return render_template('access_denied.html'), 403
        return render_template('settings/staff.html', permissions=PERMISSIONS)

    @app.route('/api/staff', methods=['GET', 'POST'])
    @app.route('/api/staff/<int:account_id>', methods=['PUT'])
    def staff_accounts(account_id=None):
        person = current()
        if not person or not person['owner']: return jsonify(error='هذه الصفحة للمالك فقط'), 403
        db = get_db(); tables(db)
        if request.method == 'GET':
            rows = db.execute('SELECT id,username,name,permissions,active FROM staff_accounts ORDER BY id DESC').fetchall()
            return jsonify(accounts=[dict(dict(r), permissions=json.loads(r['permissions'])) for r in rows])
        if not secrets.compare_digest(request.headers.get('X-CSRF-Token', ''), session.get('csrf_token') or secrets.token_hex(32)):
            return jsonify(error='حدّث الصفحة ثم حاول مجدداً'), 403
        data = request.get_json(silent=True) or {}
        name, username = str(data.get('name', '')).strip(), str(data.get('username', '')).strip().lower()
        password, permissions = str(data.get('password', '')), data.get('permissions', [])
        if not name or len(name) > 80 or not username or len(username) > 80 or username == 'admin':
            return jsonify(error='أدخل الاسم واسم دخول صالحاً؛ admin مخصص للمالك'), 400
        if not isinstance(permissions, list) or any(not isinstance(p, str) or p not in PERMISSIONS for p in permissions):
            return jsonify(error='الصلاحيات غير صالحة'), 400
        if (not account_id or password) and not 10 <= len(password) <= 200:
            return jsonify(error='كلمة المرور من 10 إلى 200 حرف'), 400
        if not isinstance(data.get('active', True), bool): return jsonify(error='حالة الحساب غير صالحة'), 400
        try:
            if account_id:
                old = db.execute('SELECT * FROM staff_accounts WHERE id=?', (account_id,)).fetchone()
                if not old: return jsonify(error='الموظف غير موجود'), 404
                hashed = generate_password_hash(password) if password else old['password_hash']
                db.execute('UPDATE staff_accounts SET name=?,username=?,password_hash=?,permissions=?,active=?,version=version+1 WHERE id=?',
                           (name, username, hashed, json.dumps(permissions), int(data.get('active', True)), account_id))
            else:
                db.execute('INSERT INTO staff_accounts(name,username,password_hash,permissions,active) VALUES(?,?,?,?,?)',
                           (name, username, generate_password_hash(password), json.dumps(permissions), int(data.get('active', True))))
            db.commit()
        except sqlite3.IntegrityError:
            db.rollback(); return jsonify(error='اسم الدخول مستخدم بالفعل'), 409
        return jsonify(ok=True)

    return current
