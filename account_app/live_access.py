"""Revocable, read-only operational access. No SQL or dashboard write proxy."""
import hashlib
import json
import re
import secrets
import time
from functools import wraps
from pathlib import Path

from flask import g, jsonify, render_template, request, send_file, session


# New schema columns and tables are never exposed implicitly.
RESOURCES = {
    'classifications': ('catalog_classifications','store_id kind id name family size_system active aliases'),
    'conversations': ('customers', 'id sender_id page_id platform first_seen_at last_seen_at name store_name phone province address notes gender store_id lead_score lead_stage inbox_reviewed_message_id'),
    'messages': ('messages', 'id sender_id direction message_type text image_url ad_id ref created_at store_id media_json'),
    'orders': ('orders', 'id sender_id customer_name phone province address product_id product_name color size notes status order_items created_at store_id product_total delivery_fee total_amount is_paid'),
    'reviews': ('human_reviews', 'id sender_id message_text image_url candidates_json reason status admin_reply created_at replied_at store_id'),
    'problems': ('problem_reports', 'id sender_id customer_name message_text reason product_id product_name status created_at updated_at store_id'),
    'product_questions': ('product_questions', 'id store_id product_id product_name topic question answer status occurrences sender_id review_id created_at updated_at answered_by'),
    'product_interests': ('customer_product_interests', 'id sender_id product_id product_name match_method confidence last_seen_at source status rejected_at notes image_sent'),
    'choices': ('customer_product_choices', 'sender_id previous_ids image_ids created_at'),
    'context': ('sales_conversation_context', 'store_id sender_id last_message_id data'),
    'memory': ('conversation_memory', 'id sender_id role content timestamp'),
    'ad_context': ('conversation_ad_context', 'sender_id message_id data'),
    'ad_events': ('zernio_ad_events', 'event_id sender_id occurred_at data'),
    'conversation_ai': ('customer_ai_settings', 'sender_id enabled updated_at'),
    'pauses': ('conversation_pause_state', 'sender_id reason paused_at updated_at'),
    'customer_instructions': ('customer_instructions', 'id sender_id instructions apply_to_all created_at updated_at'),
    'instructions': ('ai_instructions', 'id store_id title content active'),
    'rules': ('forbidden_rules', 'id store_id rule active'),
    'active_rules': ('active_ai_rules', 'id store_id source_suggestion_id rule_type rule_text priority active created_at updated_at'),
    'followups': ('followups', 'id store_id sender_id stage message_text status scheduled_at sent_at created_at order_after_id meta_json'),
    'order_actions': ('order_action_requests', 'id sender_id order_id kind value status created_at resolved_at'),
    'fulfillment': ('menger_deliveries', 'order_id local_store status attempts next_attempt remote_order_id last_error synced_at remote_status remote_updated_at status_checked_at next_status_check status_error booking_actor'),
    'jobs': ('ai_jobs', 'id store_id sender_id kind status result created_at available_at started_at finished_at'),
    'ai_requests': ('ai_request_events', 'id store_id job_id model queue_ms elapsed_ms attempts http_status error created_at'),
    'ai_usage': ('ai_usage_events', 'id store_id purpose model prompt_tokens completion_tokens reasoning_tokens cached_tokens finish_reason elapsed_ms created_at cache_write_tokens cache_discount cost'),
    'incidents': ('ai_service_incidents', 'store_id code occurrences updated_at'),
    'voice': ('voice_interpretations', 'message_id store_id model transcripts created_at'),
}
PRODUCT_FIELDS = set(('store_id product_id product_name ref ad_id ad_ids post_id post_ids keywords category description visual_description price offer colors sizes stock delivery image_url image_urls images status notes fabric material length age_range size_chart variants customer_answers '
    'age_max_months age_min_months audience bundle_contents closure faq fit_notes image_colors inspection_policy lining measurements occasions order_name product_type product_type_label product_family real_photo_notes recommendation_enabled recommendation_groups recommendation_notes recommendation_priority related_product_ids reply_category sale_unit sales_cues size_system stock_quantity stretch style transparency video_url weight_max_kg weight_min_kg').split())
SECRET_KEY = re.compile(r'password|secret|authorization|api.?key|webhook.?key|cookie|^headers$|^(?:access_|refresh_|bearer_|auth_|bot_|csrf_)?token$|token_hash', re.I)
SECRET_TEXT = re.compile(r'inq_live_[A-Za-z0-9_-]{43}|sk-(?:or-v1-)?[A-Za-z0-9_-]{20,}|Bearer\s+[A-Za-z0-9_.-]{20,}', re.I)


def redact(value):
    if isinstance(value, dict):
        return {k: '[redacted]' if SECRET_KEY.search(k) else redact(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(v) for v in value]
    if isinstance(value, str):
        if value.lstrip().startswith(('{', '[')):
            try:
                return json.dumps(redact(json.loads(value)), ensure_ascii=False)
            except ValueError:
                pass
        return SECRET_TEXT.sub('[redacted]', value)
    return value


def init_db(db):
    db.executescript('''CREATE TABLE IF NOT EXISTS live_access_tokens (
        id TEXT PRIMARY KEY, label TEXT NOT NULL, token_hash TEXT UNIQUE NOT NULL,
        created_at REAL NOT NULL, expires_at REAL NOT NULL, revoked_at REAL, last_used_at REAL);
        CREATE TABLE IF NOT EXISTS live_access_audit (
        id INTEGER PRIMARY KEY, token_id TEXT NOT NULL, path TEXT NOT NULL,
        status INTEGER NOT NULL, created_at REAL NOT NULL);
        CREATE INDEX IF NOT EXISTS live_access_audit_token_time ON live_access_audit(token_id,created_at);
        CREATE INDEX IF NOT EXISTS live_access_audit_time ON live_access_audit(created_at);''')


def integer(name, default=0, maximum=2**63-1):
    value = request.args.get(name)
    if value is None:
        return default
    if not re.fullmatch(r'\d{1,19}', value) or int(value) > maximum:
        raise ValueError('Invalid ' + name)
    return int(value)


def columns(db, table):
    return {r['name'] for r in db.execute(f'PRAGMA table_info("{table}")')}


def install(m):
    app, get_db = m.app, m.get_db

    def owner(fn):
        @wraps(fn)
        def guard(*args, **kwargs):
            person = m.current_dashboard_person()
            if not person or not person.get('owner'):
                return jsonify(error='هذه الصفحة للمالك فقط'), 403
            if request.method != 'GET' and not secrets.compare_digest(
                    request.headers.get('X-CSRF-Token', ''), session.get('csrf_token') or secrets.token_hex(32)):
                return jsonify(error='حدّث الصفحة ثم حاول مجدداً'), 403
            return fn(*args, **kwargs)
        return guard

    def reader(fn):
        @wraps(fn)
        def guard(*args, **kwargs):
            auth = request.headers.get('Authorization', '')
            if not re.fullmatch(r'Bearer inq_live_[A-Za-z0-9_-]{43}', auth):
                return jsonify(error='A live access bearer token is required'), 401
            digest = hashlib.sha256(auth[7:].encode()).hexdigest()
            db = get_db()
            row = db.execute('SELECT id FROM live_access_tokens WHERE token_hash=? AND revoked_at IS NULL AND expires_at>?',
                             (digest, time.time())).fetchone()
            if not row:
                return jsonify(error='Live access token is invalid, revoked or expired'), 401
            g.live_access_token = row['id']
            recent = db.execute('SELECT COUNT(*) FROM live_access_audit WHERE token_id=? AND created_at>?',
                                (row['id'], time.time()-60)).fetchone()[0]
            if recent >= 120:
                response = jsonify(error='Read limit reached; retry after 60 seconds')
                response.headers['Retry-After'] = '60'
                return response, 429
            try:
                return fn(*args, **kwargs)
            except ValueError as exc:
                return jsonify(error=str(exc)), 400
        return guard

    @app.after_request
    def protect_and_audit(response):
        if request.path.startswith(('/api/live/', '/api/settings/live-access', '/settings/live-access')):
            response.headers['Cache-Control'] = 'no-store'
            response.headers['X-Content-Type-Options'] = 'nosniff'
            response.headers['Referrer-Policy'] = 'no-referrer'
        token_id = getattr(g, 'live_access_token', None)
        if token_id:
            db = get_db()
            db.execute('INSERT INTO live_access_audit(token_id,path,status,created_at) VALUES(?,?,?,?)',
                       (token_id, request.path[:240], response.status_code, time.time()))
            db.execute('UPDATE live_access_tokens SET last_used_at=? WHERE id=?', (time.time(), token_id))
            # Keep diagnostic access logs bounded. No message contents or bearer values are logged.
            db.execute('DELETE FROM live_access_audit WHERE created_at<?', (time.time()-90*86400,))
            db.commit()
        return response

    @app.get('/settings/live-access')
    @owner
    def live_access_page():
        return render_template('settings/live_access.html')

    @app.route('/api/settings/live-access', methods=['GET', 'POST'])
    @owner
    def live_access_keys():
        db = get_db()
        if request.method == 'GET':
            rows = db.execute('SELECT id,label,created_at,expires_at,revoked_at,last_used_at FROM live_access_tokens ORDER BY created_at DESC')
            return jsonify(keys=[dict(r) for r in rows], scope='read:operations:all-stores')
        data = request.get_json(silent=True) or {}
        label = str(data.get('label') or '').strip()
        days = data.get('days', 365)
        if not label or len(label)>80 or isinstance(days, bool) or not isinstance(days, int) or not 1<=days<=365:
            return jsonify(error='أدخل اسم اتصال ومدة من يوم إلى 365 يوماً'), 400
        token, token_id, now = 'inq_live_' + secrets.token_urlsafe(32), secrets.token_hex(12), time.time()
        db.execute('INSERT INTO live_access_tokens(id,label,token_hash,created_at,expires_at) VALUES(?,?,?,?,?)',
                   (token_id, label, hashlib.sha256(token.encode()).hexdigest(), now, now+days*86400))
        db.commit()
        return jsonify(id=token_id, token=token, expires_at=now+days*86400, scope='read:operations:all-stores'), 201

    @app.post('/api/settings/live-access/<token_id>/revoke')
    @owner
    def live_access_revoke(token_id):
        db = get_db()
        changed = db.execute('UPDATE live_access_tokens SET revoked_at=COALESCE(revoked_at,?) WHERE id=?',
                             (time.time(), token_id)).rowcount
        db.commit()
        return (jsonify(ok=True), 200) if changed else (jsonify(error='الاتصال غير موجود'), 404)

    @app.get('/api/settings/live-access/audit')
    @owner
    def live_access_audit():
        rows = get_db().execute('''SELECT a.id,t.label,a.path,a.status,a.created_at FROM live_access_audit a
            JOIN live_access_tokens t ON t.id=a.token_id ORDER BY a.id DESC LIMIT 100''')
        return jsonify(events=[dict(r) for r in rows])

    def spec(db, resource):
        table, allowed = RESOURCES[resource]
        actual = columns(db, table)
        return table, [c for c in allowed.split() if c in actual], actual

    @app.get('/api/live/v1/manifest')
    @reader
    def live_manifest():
        db = get_db()
        resources = {}
        for name in RESOURCES:
            table, fields, actual = spec(db, name)
            resources[name] = dict(available=bool(actual), fields=fields,
                count=db.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0] if actual else None)
        return jsonify(version=1, scope='read:operations:all-stores', resources=resources,
            special_resources=['inbox', 'stores', 'products', 'settings', 'catalog', 'media'],
            code_version=hashlib.sha256(Path(m.__file__).read_bytes().replace(b'\r\n', b'\n')).hexdigest()[:16],
            read_at=time.time(), max_page_size=200,
            consistency='Live paginated reads, not a simultaneous database snapshot; existing rows can change.',
            excluded=['credentials and environment', 'arbitrary SQL and files', 'raw provider payloads',
                      'staff authentication and payroll', 'embeddings and caches', 'customer sends and all operational writes'])

    @app.get('/api/live/v1/inbox')
    @reader
    def live_inbox():
        # Only this audited SELECT-only handler is reused; no general route proxy
        # and no synthetic dashboard session are created for bearer connections.
        response=app.make_response(m.api_conversations.__wrapped__())
        return jsonify(redact(response.get_json())),response.status_code

    @app.get('/api/live/v1/records/<resource>')
    @reader
    def live_records(resource):
        if resource not in RESOURCES:
            return jsonify(error='Unknown resource'), 404
        db = get_db()
        table, fields, actual = spec(db, resource)
        if not actual:
            return jsonify(error='Resource unavailable in this installation'), 404
        limit = integer('limit', 100, 200)
        if not limit:
            raise ValueError('limit must be positive')
        order = request.args.get('order', 'asc')
        if order not in {'asc', 'desc'}:
            raise ValueError('order must be asc or desc')
        where, values = [], []
        store = request.args.get('store_id')
        sender = request.args.get('sender_id')
        if store:
            if 'store_id' in actual:
                where.append('COALESCE(NULLIF(r.store_id,\'\'),\'default\')=?'); values.append(store)
            elif 'local_store' in actual:
                where.append('r.local_store=?'); values.append(store)
            elif 'sender_id' in actual:
                where.append("r.sender_id IN (SELECT sender_id FROM customers WHERE COALESCE(NULLIF(store_id,''),'default')=?)"); values.append(store)
            else:
                raise ValueError('This resource has no store filter')
        if sender:
            if 'sender_id' not in actual:
                raise ValueError('This resource has no sender filter')
            where.append('r.sender_id=?'); values.append(sender)
        if request.args.get('status'):
            if 'status' not in actual:
                raise ValueError('This resource has no status filter')
            where.append('r.status=?'); values.append(request.args['status'])
        if request.args.get('q'):
            if resource != 'conversations':
                raise ValueError('q is supported only for conversations')
            q = request.args['q'][:120].replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_')
            where.append("(r.name LIKE ? ESCAPE '\\' OR r.sender_id LIKE ? ESCAPE '\\')"); values.extend(['%'+q+'%']*2)
        base = (' WHERE ' + ' AND '.join(where)) if where else ''
        count = db.execute(f'SELECT COUNT(*) FROM "{table}" r'+base, values).fetchone()[0]
        upper = integer('upper_rowid') if 'upper_rowid' in request.args else db.execute(
            f'SELECT COALESCE(MAX(r.rowid),0) FROM "{table}" r'+base, values).fetchone()[0]
        where.append('r.rowid<=?'); values.append(upper)
        if 'cursor' in request.args:
            where.append('r.rowid'+('>?' if order=='asc' else '<?')); values.append(integer('cursor'))
        select = ','.join('r."'+c+'"' for c in fields)
        rows = [redact(dict(r)) for r in db.execute(f'SELECT r.rowid AS _cursor,{select} FROM "{table}" r WHERE '
                + ' AND '.join(where)+f' ORDER BY r.rowid {order} LIMIT ?', values+[limit+1])]
        items = rows[:limit]
        return jsonify(resource=resource, items=items, has_more=len(rows)>limit,
                       next_cursor=items[-1]['_cursor'] if items else None, upper_rowid=upper,
                       live_count=count, read_at=time.time())

    @app.get('/api/live/v1/stores')
    @reader
    def live_stores():
        db = get_db()
        rows = db.execute('SELECT store_id,name,page_id,active,created_at,updated_at FROM stores ORDER BY store_id')
        return jsonify(items=[dict(dict(r), ai_enabled=m.get_app_setting('ai_enabled', '1', db, r['store_id'])!='0',
            messaging_provider='zernio' if m.zernio.enabled_for_store(r['store_id']) else 'chatwoot' if m.chatwoot.enabled() else 'manychat') for r in rows])

    @app.get('/api/live/v1/products')
    @reader
    def live_products():
        products = m.load_products_from_file(all_stores=True)
        store = request.args.get('store_id')
        return jsonify(items=[redact({k:v for k,v in p.items() if k in PRODUCT_FIELDS})
                              for p in products if not store or p.get('store_id')==store])

    @app.get('/api/live/v1/settings')
    @reader
    def live_settings():
        # Explicit product behavior keys, never channel credentials or connection URLs.
        allowed = set(m.DEFAULT_APP_SETTINGS) | {'ai_enabled', 'followup_enabled', 'followup_max_per_day',
            'followup_stop_on_order', 'followup_stop_on_rejection', 'followup_default_delay_minutes',
            'followup_min_interest_score', 'followup_message_template', 'smart_reviewer_enabled', 'smart_reviewer_interval_minutes'}
        allowed = {key for key in allowed if not SECRET_KEY.search(key)}
        stores = [r['store_id'] for r in get_db().execute('SELECT store_id FROM stores')]
        requested = request.args.get('store_id')
        if requested and requested not in stores:
            raise ValueError('Unknown store')
        return jsonify(items=[dict(store_id=sid, values=redact({key:m.get_app_setting(key, '', get_db(), sid)
                         for key in sorted(allowed)})) for sid in stores if not requested or sid==requested])

    @app.get('/api/live/v1/catalog')
    @reader
    def live_catalog():
        items=[]
        for row in get_db().execute('SELECT store_id FROM stores ORDER BY store_id'):
            sid=row['store_id']
            if request.args.get('store_id') and request.args['store_id']!=sid:
                continue
            token=m._current_store_id.set(sid)
            try:
                for path in m._resolve_catalog_image_paths():
                    items.append(dict(store_id=sid,name=Path(path).name,
                        media_path='/api/live/v1/catalog/'+sid+'/'+Path(path).name))
            finally:
                m._current_store_id.reset(token)
        return jsonify(items=items)

    @app.get('/api/live/v1/catalog/<store>/<filename>')
    @reader
    def live_catalog_media(store, filename):
        token=m._current_store_id.set(store)
        try:
            path=next((p for p in m._resolve_catalog_image_paths() if Path(p).name==filename),None)
        finally:
            m._current_store_id.reset(token)
        return send_file(path) if path else (jsonify(error='Catalogue image not found'),404)

    @app.get('/api/live/v1/media/<int:message_id>/<int:index>')
    @reader
    def live_media(message_id,index):
        row=get_db().execute('SELECT * FROM messages WHERE id=?',(message_id,)).fetchone()
        if row:
            entries=m._incoming_media_entries(row)
            if index<len(entries):
                filename=str(entries[index].get('local_file') or '')
                root=Path(m.INCOMING_MEDIA_DIR).resolve()
                path=(root/filename).resolve()
                if m._incoming_media_filename.fullmatch(filename) and path.parent==root and path.is_file():
                    return send_file(path,as_attachment=True)
        return jsonify(error='Archived media unavailable; no external fetch was performed'),404
