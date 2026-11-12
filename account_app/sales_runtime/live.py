"""Production routing through authenticated intake and the existing AI worker."""
import hashlib
import json
import os
import re
from datetime import datetime, timedelta
from functools import partial
from urllib.parse import urlsplit

from .. import ad_attribution, checkout, delivery_state, sales_playbook
from .delivery import deliver_confirmation, deliver_replies
from .entrypoint import Entrypoint
from .gateway import BACKUP_MODEL, BackupGateway, DecisionGateway, WriterGateway
from .intent import purchase_selected
from .orders import migrate_tasks
from .preview import ImageObserver, media_input
from .store import Conflict, Scope, Store, encode, initial_state


PILOT_STORE = 'khuyoot'
JOB_KIND = 'sales_runtime'
FINAL_ORDER_STATUSES = ('cancelled', 'canceled', 'ملغي', 'delivered', 'returned', 'تم التوصيل', 'مرتجع')


def init_db(db):
    Store(db).migrate()
    migrate_tasks(db)
    with Store(db).transaction():
        db.execute('''CREATE TABLE IF NOT EXISTS sales_v2_live_routes (
            sender_id TEXT PRIMARY KEY, store_id TEXT NOT NULL, account_id TEXT NOT NULL,
            first_incoming_id INTEGER NOT NULL, handled_id INTEGER NOT NULL DEFAULT 0,
            control_revision INTEGER NOT NULL DEFAULT 0, channel TEXT NOT NULL DEFAULT 'chatwoot')''')
        if 'channel' not in {row[1] for row in db.execute('PRAGMA table_info(sales_v2_live_routes)')}:
            db.execute("ALTER TABLE sales_v2_live_routes ADD COLUMN channel TEXT NOT NULL DEFAULT 'chatwoot'")
        db.execute('CREATE INDEX IF NOT EXISTS sales_v2_routes_store ON sales_v2_live_routes(store_id)')


def all_stores():
    return os.environ.get('SALES_RUNTIME_MODE', '').strip().lower() == 'all'


def percent(store_id):
    mode = os.environ.get('SALES_RUNTIME_MODE', '').strip().lower()
    if mode == 'all':
        return 100
    if mode == 'off' or store_id != PILOT_STORE:
        return 0
    raw = os.environ.get('SALES_RUNTIME_KHUYOOT_PERCENT', '0')
    try:
        return max(0, min(100, int(raw)))
    except ValueError:
        return 0


def route(db, sender_id):
    if not any(marker in str(sender_id or '') for marker in ('::cw-', '::zn-')):
        return None
    return db.execute('SELECT * FROM sales_v2_live_routes WHERE sender_id=?', (sender_id,)).fetchone()


def scope_for(row):
    return Scope(row['store_id'], row['channel'], row['account_id'], row['sender_id'])


def delete_conversation(db, sender_id):
    """Purge every runtime scope for the sender inside the caller's transaction."""
    scopes = {row[0] for row in db.execute('SELECT scope FROM sales_v2_scopes WHERE sender=?', (sender_id,))}
    existing = route(db, sender_id)
    if existing:
        scopes.add(scope_for(existing).key)
    deleted = {}
    for table in ('sales_v2_events', 'sales_v2_operations', 'sales_v2_evidence',
                  'sales_v2_outbox', 'sales_v2_model_attempts', 'sales_v2_incidents', 'sales_v2_scopes'):
        deleted[table] = db.executemany(f'DELETE FROM {table} WHERE scope=?',
                                        ((scope,) for scope in scopes)).rowcount
    deleted['sales_v2_order_tasks'] = db.executemany("""DELETE FROM sales_v2_order_tasks
        WHERE scope=? AND (kind='confirmation' OR NOT EXISTS
            (SELECT 1 FROM orders WHERE orders.id=sales_v2_order_tasks.order_id))""",
        ((scope,) for scope in scopes)).rowcount
    deleted['sales_v2_live_routes'] = db.execute('DELETE FROM sales_v2_live_routes WHERE sender_id=?',
                                                  (sender_id,)).rowcount
    return deleted


def configured(module, store_id, channel):
    if not percent(store_id) or not module.OPENROUTER_KEY or os.environ.get('ENABLE_BACKGROUND_JOBS', '1') != '1':
        return False
    if channel == 'zernio':
        return module.zernio.configuration(store_id)['ready']
    settings = module.chatwoot.configuration()
    return bool(channel == 'chatwoot' and module.chatwoot.enabled() and not module.zernio.enabled_for_store(store_id)
                and settings['chatwoot_api_url'].startswith('https://') and settings['chatwoot_key_present']
                and settings['chatwoot_webhook_secret_present'])


def rollout_status(module, db):
    stores = []
    for store in module.list_stores(db):
        store_id = store['store_id']
        channel = 'zernio' if module.zernio.enabled_for_store(store_id) else 'chatwoot'
        enabled = module.get_app_setting('ai_enabled', '1', db, store_id) != '0'
        stores.append(dict(store=store_id, channel=channel, percent=percent(store_id),
                           active=bool(store['active'] and enabled and configured(module, store_id, channel))))
    return dict(mode='all' if all_stores() else 'pilot' if percent(PILOT_STORE) else 'off',
                active=any(store['active'] for store in stores), stores=stores)


def terminal_order(status, remote_status=''):
    return any(str(value or '').strip().lower() in FINAL_ORDER_STATUSES for value in (status, remote_status))


def open_order(db, store_id, sender_id, exclude=0):
    rows = db.execute('''SELECT o.id,o.status,m.remote_status FROM orders o LEFT JOIN menger_deliveries m ON m.order_id=o.id
        WHERE o.store_id=? AND o.sender_id=? AND o.id<>? ORDER BY o.id DESC''', (store_id, sender_id, exclude))
    return next((row for row in rows if not terminal_order(row['status'], row['remote_status'])), None)


def start_new_purchase(db, scope, incoming_id, text, allowed):
    if (not purchase_selected(text) or checkout.is_existing_order_followup(text)
            or re.search(r'طلبي|حجزي|الطلب\s+(?:السابق|القديم)|استبدال|ارجاع|الغاء|ترجيع|ارجع القطع[ةه]',
                         checkout.normalized(text))):
        return
    store = Store(db)
    snapshot = store.snapshot(scope)
    order_id = snapshot['data'].get('order_id')
    if not order_id or open_order(db, scope.store, scope.sender):
        return

    def reset(state):
        if not allowed(scope) or open_order(db, scope.store, scope.sender):
            raise Conflict('turn_superseded')
        state.update(initial_state())
        for key in ('order_id', 'order_status', 'checkout_prompt', 'contact_events', 'price_request', 'quote_province',
                    'stated_clothing_size', 'focus_source'):
            state.pop(key, None)
        return dict(previous_order_id=order_id)

    store.apply(scope, f'new-purchase:{incoming_id}', snapshot['version'], {'order_id': order_id}, reset)


def legacy_context(module, db, store_id, sender_id, incoming_id):
    """Return a settled history for context, or None while the legacy engine owns an active turn."""
    if (db.execute("""SELECT 1 FROM ai_jobs WHERE store_id=? AND sender_id=? AND kind IN ('webhook','resume','link')
                    AND status IN ('queued','running') LIMIT 1""", (store_id, sender_id)).fetchone()
            or open_order(db, store_id, sender_id)
            or module.pending_product_choice(db, sender_id)):
        return None
    rows = db.execute('''SELECT direction,text,raw_payload,created_at FROM messages
        WHERE store_id=? AND sender_id=? AND id<? AND ''' + delivery_state.VISIBLE_SQL + '''
        ORDER BY id DESC LIMIT 16''', (store_id, sender_id, incoming_id)).fetchall()
    # A shipping question can be answered after a checkout prompt. Check older
    # accepted replies too, until the last completed checkout or expiry.
    outgoing = db.execute('''SELECT text,raw_payload,created_at FROM messages
        WHERE store_id=? AND sender_id=? AND direction='outgoing' AND message_type='text' AND id<? AND '''
        + delivery_state.VISIBLE_SQL + ' ORDER BY id DESC LIMIT 24', (store_id, sender_id, incoming_id))
    cutoff = datetime.now(module.BAGHDAD_TZ) - timedelta(hours=24)
    for reply in outgoing:
        stamp = module.sales_engagement.timestamp(reply['created_at'])
        meta = delivery_state.metadata(reply['raw_payload'])
        if not stamp or stamp < cutoff or meta.get('_order_confirmation_id'):
            break
        prompt = re.search(r'(?:دزي|ارسلي|انطي|احتاج|باقي|شنو).{0,30}(?:رقم|موبايل|عنوان)',
                           module.sales_strategy.normalized(reply['text']))
        if meta.get('checkout_draft') or meta.get('checkout_proposal') or prompt:
            return None
    return [dict(actor='customer' if row['direction'] == 'incoming' else 'staff', text=row['text'][:700])
            for row in reversed(rows) if row['text']]


def enroll(module, db, store_id, sender_id, channel, account_id, incoming_id):
    """Pin a trusted provider and adopt settled threads on their next real message."""
    if not sender_id.startswith(f'{store_id}::'):
        return False
    if channel == 'chatwoot' and not sender_id.startswith(f'{store_id}::cw-{account_id}-'):
        return False
    if channel == 'zernio' and not db.execute('''SELECT 1 FROM zernio_conversations
            WHERE sender_id=? AND store_id=? AND account_id=?''', (sender_id, store_id, account_id)).fetchone():
        return False
    existing = route(db, sender_id)
    if existing:
        if (existing['store_id'], existing['channel'], existing['account_id']) != (store_id, channel, account_id):
            return False
        db.execute('UPDATE sales_v2_scopes SET version=version+1 WHERE scope=?', (scope_for(existing).key,))
        return True
    bucket = int(hashlib.sha256(sender_id.encode()).hexdigest()[:8], 16) % 100
    if not configured(module, store_id, channel) or bucket >= percent(store_id) or not module.is_ai_enabled(db):
        return False
    if not module.is_customer_ai_enabled(db, sender_id):
        return False
    if db.execute("""SELECT 1 FROM human_reviews WHERE sender_id=? AND status='pending' LIMIT 1""",
                  (sender_id,)).fetchone():
        return False
    historical = db.execute('SELECT 1 FROM messages WHERE sender_id=? AND id<? LIMIT 1',
                            (sender_id, incoming_id)).fetchone()
    context = []
    if historical:
        if not all_stores():
            return False
        context = legacy_context(module, db, store_id, sender_id, incoming_id)
        if context is None:
            return False
    elif open_order(db, store_id, sender_id):
        return False
    cur = db.execute('''INSERT OR IGNORE INTO sales_v2_live_routes
        (sender_id,store_id,account_id,first_incoming_id,channel) VALUES(?,?,?,?,?)''',
        (sender_id, store_id, account_id, incoming_id, channel))
    if cur.rowcount and context:
        scope = Scope(store_id, channel, account_id, sender_id)
        Store(db)._ensure(scope)
        db.execute("""UPDATE sales_v2_scopes SET data=json_set(data,'$.prior_conversation',json(?)) WHERE scope=?""",
                   (encode(context), scope.key))
    return bool(cur.rowcount)


def invalidate(db, row, mode=None):
    """Run in the same transaction as the dashboard's existing AI switch."""
    scope = scope_for(row)
    db.execute('UPDATE sales_v2_live_routes SET control_revision=control_revision+1 WHERE sender_id=?',
               (scope.sender,))
    if mode is None:
        db.execute('UPDATE sales_v2_scopes SET version=version+1 WHERE scope=?', (scope.key,))
    else:
        db.execute('''UPDATE sales_v2_scopes SET version=version+1,data=json_set(data,'$.control_mode',?)
                      WHERE scope=?''', (mode, scope.key))
    db.execute("UPDATE sales_v2_outbox SET status='superseded' WHERE scope=? AND status='queued'", (scope.key,))


def sync_control(db, sender_id, enabled, *, explicit=False):
    row = route(db, sender_id)
    if row:
        invalidate(db, row, 'active' if enabled else 'human')
        if enabled and explicit:
            scope = scope_for(row)
            db.execute("""UPDATE sales_v2_outbox SET status='superseded'
                          WHERE scope=? AND status IN ('unknown','rejected')""", (scope.key,))
            db.execute("""UPDATE sales_v2_order_tasks SET status='superseded'
                          WHERE scope=? AND kind='confirmation' AND status IN ('unknown','rejected')""", (scope.key,))


def pause_store(db, store_id):
    db.execute('UPDATE sales_v2_live_routes SET control_revision=control_revision+1 WHERE store_id=?', (store_id,))
    db.execute('''UPDATE sales_v2_scopes SET version=version+1 WHERE store=? AND sender IN
                  (SELECT sender_id FROM sales_v2_live_routes WHERE store_id=?)''', (store_id, store_id))


def staff_product(db, sender_id, product_id):
    row = route(db, sender_id)
    if not row:
        return
    scope = scope_for(row)
    Store(db)._ensure(scope)
    invalidate(db, row)
    db.execute('''UPDATE sales_v2_scopes SET data=json_set(data,'$.focus_product_id',?,'$.focus_source','staff')
                  WHERE scope=?''', (product_id, scope.key))
    db.commit()


def pending_input(module, db, row, incoming_id, staff_reply_id):
    rows = db.execute('''SELECT * FROM messages WHERE sender_id=? AND store_id=? AND direction='incoming'
        AND id>=? AND id>? AND id<=? ORDER BY id''',
        (row['sender_id'], row['store_id'], row['first_incoming_id'],
         max(row['handled_id'], staff_reply_id), incoming_id)).fetchall()
    texts, attachments, seen = [], [], set()
    ad = {}
    audio_pending = False
    for message in rows:
        text = (message['text'] or '').strip()
        if text and text != message['image_url']:
            texts.append(text)
        ad = ad_attribution.extract(delivery_state.metadata(message['raw_payload'])) or ad
        for item in module.message_media(message):
            if item['type'] == 'audio':
                interpreted = db.execute('SELECT 1 FROM voice_interpretations WHERE message_id=? AND store_id=?',
                                         (message['id'], row['store_id'])).fetchone()
                audio_pending |= not bool(interpreted)
                if interpreted:
                    continue
            key = (item['type'], item['url'])
            if key not in seen:
                seen.add(key)
                attachments.append(dict(type=item['type'], payload={'url': item['url']}))
    kind, reference = media_input(attachments)
    if not ad and row['channel'] == 'zernio' and not row['handled_id']:
        # A referral-only webhook can arrive before the first message.
        saved = db.execute('SELECT data FROM conversation_ad_context WHERE sender_id=?',
                           (row['sender_id'],)).fetchone()
        context = delivery_state.metadata(saved[0]) if saved else {}
        stamp = module.sales_engagement.timestamp(context.get('captured_at'))
        if stamp and timedelta(0) <= datetime.now(module.BAGHDAD_TZ) - stamp < timedelta(hours=24):
            ad = context
    # A staff request takes priority even when several messages arrive together.
    text = '\n'.join(texts)
    if any(sales_playbook.human_requested(part) for part in texts):
        text = next(part for part in texts if sales_playbook.human_requested(part))
    return dict(text=text, ad=ad, media_kind=kind, media_ref=reference), audio_pending


def recent_staff_stock_conflict(module, db, scope, incoming_id, staff_reply_id):
    if not staff_reply_id:
        return None
    focus = Store(db).snapshot(scope)['data'].get('focus_product_id')
    if not focus:
        binding = module.get_active_product_binding(db, scope.sender)
        focus = binding['product_id'] if binding else None
    if not focus:
        return None
    rows = db.execute("""SELECT text,created_at FROM messages
        WHERE sender_id=? AND store_id=? AND id<? AND direction='outgoing' AND message_type='text'
        AND json_extract(CASE WHEN json_valid(raw_payload) THEN raw_payload ELSE '{}' END,
                         '$.sender_type')='staff' AND """ + delivery_state.VISIBLE_SQL + " ORDER BY id DESC LIMIT 12",
        (scope.sender, scope.store, incoming_id))
    cutoff = datetime.now(module.BAGHDAD_TZ) - timedelta(hours=24)
    for row in rows:
        stamp = module.sales_engagement.timestamp(row['created_at'])
        if not stamp or stamp < cutoff:
            break
        clean = checkout.normalized(row['text'])
        if re.search(r'خلصت\s+(?:كل\s+)?الكمي[هة]|ولا\s+قطع[هة]\s+بقت|نفدت\s+(?:كل\s+)?الكمي[هة]', clean):
            return focus
        if re.search(r'(?:رجع|وصل|صار|هسه)\s+متوفر|وصلت\s+كمي[هة]\s+جديد[هة]', clean):
            break
    return None


def finish_order(module, db, scope, order, products, held):
    from .contracts import ServiceRequired
    if open_order(db, scope.store, scope.sender, exclude=order['id']):
        raise ServiceRequired('existing_order_requires_staff')
    customer = db.execute('SELECT name FROM customers WHERE sender_id=? AND store_id=?',
                          (scope.sender, scope.store)).fetchone()
    if not order.get('customer_name') and customer and customer[0]:
        order['customer_name'] = customer[0]
        db.execute('UPDATE orders SET customer_name=? WHERE id=?', (customer[0], order['id']))
    db.execute('''UPDATE customers SET phone=?,address=?,province=?,lead_score=100,lead_stage='booked'
                  WHERE sender_id=? AND store_id=?''',
               (order['phone'], order['address'], order['province'], scope.sender, scope.store))
    db.execute("UPDATE followups SET status='cancelled' WHERE sender_id=? AND status='pending'", (scope.sender,))
    if held:
        return
    telegram_only = scope.store in module.menger.TELEGRAM_ONLY_STORES
    if not telegram_only:
        module.menger.enqueue(db, order['id'], order, products)
    db.execute("""UPDATE sales_v2_order_tasks SET status=?,payload=?
                  WHERE order_id=? AND kind='fulfillment'""",
               ('superseded' if telegram_only else 'prepared', encode(order), order['id']))
    db.execute('''INSERT INTO sales_v2_order_tasks(scope,order_id,kind,payload)
                  VALUES(?,?,'notification',?)''', (scope.key, order['id'], encode(order)))


def notify_orders(module, db, scope):
    tasks = db.execute('''SELECT id,payload FROM sales_v2_order_tasks WHERE scope=? AND kind='notification'
                          AND status='queued' ORDER BY id''', (scope.key,)).fetchall()
    for task in tasks:
        with db:
            claimed = db.execute("""UPDATE sales_v2_order_tasks SET status='sending'
                WHERE id=? AND status='queued' AND EXISTS
                (SELECT 1 FROM orders WHERE id=sales_v2_order_tasks.order_id AND status='new')""", (task['id'],)).rowcount
        if not claimed:
            continue
        booking = json.loads(task['payload'])
        try:
            module.save_booking_to_file(booking)
            sent = module.send_order_to_telegram(booking)
        except Exception:
            module.app.logger.warning('Sales runtime order notification did not complete')
            sent = False
        with db:
            db.execute('UPDATE sales_v2_order_tasks SET status=? WHERE id=?',
                       ('accepted' if sent else 'unknown', task['id']))


def make_entry(module, db, scope, allow_write):
    model = module.get_ai_model(db, 'main_model', module.MAIN_MODEL)
    # The runtime owns its attempt budget, including exactly one Argon request.
    post = partial(module.ai_transport.post, max_attempts=1, budget_seconds=48)
    observe = ImageObserver(module.inbound_media.fetch, module.match_customer_image_with_catalog,
                            lambda current, url: module._archived_incoming_image_data(db, current.sender, url))

    def media(current, kind, references, products):
        from .contracts import DecisionUnavailable
        try:
            return observe(current, kind, references, products)
        except DecisionUnavailable:
            return dict(status='unavailable')

    def text_reference(current, text, catalog, *, preferred=None):
        if module.is_product_objection(text) or checkout.is_existing_order_followup(text):
            return None
        active = [product for product in catalog.products.values() if product.get('status', 'active') == 'active']
        named = module.first_message_named_products(text, active)
        if named:
            return dict(source='text', product_id=named[0]['product_id']) if len(named) == 1 else None
        if preferred:
            product = next((item for item in active if item['product_id'] == preferred), None)
            if product and module.select_customer_context_product(text, [product]):
                return dict(source='conversation', product_id=preferred)
            return None
        binding = module.get_active_product_binding(db, current.sender)
        if binding:
            product = next((item for item in active if item['product_id'] == binding['product_id']), None)
            if product and module.select_customer_context_product(text, [product]):
                return dict(source='conversation', product_id=product['product_id'])
            return None
        if module.should_use_auto_product(db, current.sender, {'text': text}, 'text', []):
            product_id = module.get_auto_product_settings(db, current.store)['product_id']
            if any(product['product_id'] == product_id for product in active):
                return dict(source='auto_product', product_id=product_id)
        return None

    def product_photos(product, colors):
        origin = urlsplit(module.PUBLIC_URL).netloc
        color_by_url = {url: color for color in colors for url in module.select_product_image_urls(product, color)}
        photos = []
        for variant in module.product_image_variants(product):
            url = urlsplit(variant['url'])
            if not origin or url.scheme != 'https' or url.netloc != origin or not url.path.startswith('/product_image/'):
                continue
            if colors and variant['url'] not in color_by_url:
                continue
            photos.append(dict(type='image', url=variant['url'], product_id=product['product_id'],
                               color=color_by_url.get(variant['url'], variant['color'])))
        return photos

    def remember_photo_focus(current, product):
        module.remember_customer_product(db, current.sender, product, 'image_recognition',
                                         confidence=100, commit=False)

    return Entrypoint(db, lambda store: module.load_products_from_file(store_id=store),
        lambda store, province: module.delivery_fee_for_province(province, db),
        DecisionGateway(model, module.OPENROUTER_KEY, post), module.get_order_confirmation_text(db),
        media=media, writer=WriterGateway(model, module.OPENROUTER_KEY, post),
        backup=BackupGateway(module.OPENROUTER_KEY, post),
        policy_loader=lambda store: {'delivery_time': module.get_delivery_settings(db)['delivery_time']},
        allow_write=allow_write, on_booking=partial(finish_order, module, db), text_reference=text_reference,
        product_photos=product_photos, on_photo_focus=remember_photo_focus)


def delivery_callbacks(module, db, scope, incoming_id, enabled):
    def send(payload, key):
        if not enabled(scope):
            return dict(paused=True)
        part = {'type': 'text', **payload}
        if scope.channel == 'zernio':
            response = module.zernio.send(module, scope.sender, [part], operation_key=key)
        else:
            response = module.chatwoot.send(module, scope.sender, [part])
        if response.get('ok') and response.get('provider_id'):
            return dict(accepted=True, provider_id=response['provider_id'])
        if response.get('status_code') in {400, 401, 403, 404, 422}:
            return dict(accepted=False)
        return {}

    def record(payload, provider_id, *, order_id=None):
        kind = payload.get('type', 'text')
        text, image_url = payload.get('text'), payload.get('url') if kind == 'image' else None
        meta = dict(sender_type='ai', sales_runtime='v2', provider_id=provider_id,
                    _delivery_status='accepted', _reply_to_message_id=incoming_id)
        if order_id is not None:
            meta['_order_confirmation_id'] = order_id
        module.save_message(db, scope.sender, 'outgoing', kind, text, image_url, raw_payload=meta, commit=False)
        if text:
            module.save_conversation_message(db, scope.sender, 'assistant', text, commit=False)

    return send, record


def staff_confirmation(module, db, row, task_id):
    scope = scope_for(row)
    latest = module.latest_incoming_message(db, scope.sender)
    allowed = lambda scope: True  # Explicit staff action under the existing sender lock.
    send, record = delivery_callbacks(module, db, scope, latest.get('id'), allowed)
    with db:
        db.execute("""UPDATE sales_v2_order_tasks SET status='queued'
                      WHERE id=? AND scope=? AND kind='confirmation' AND status='rejected'""",
                   (task_id, scope.key))
    return deliver_confirmation(Store(db), scope, task_id, send, allowed, record=record, staff_override=True)


def drain(module, db, entry, scope, incoming_id, enabled):
    send, record = delivery_callbacks(module, db, scope, incoming_id, enabled)
    pending = db.execute('''SELECT id FROM sales_v2_order_tasks WHERE scope=? AND kind='confirmation'
                  AND status IN ('queued','sending','unknown','rejected') ORDER BY id LIMIT 1''', (scope.key,)).fetchone()
    if pending:
        status = deliver_confirmation(entry.store, scope, pending[0], send, enabled, record=record)['status']
        if status == 'order_changed':
            with db:
                db.execute("UPDATE sales_v2_order_tasks SET status='superseded' WHERE id=? AND status='queued'", (pending[0],))
        elif status != 'accepted':
            return [status]
    results = deliver_replies(entry, scope, send, enabled, record=record)
    blocked = db.execute('''SELECT status FROM sales_v2_outbox WHERE scope=?
                            AND status IN ('sending','unknown','rejected') ORDER BY id LIMIT 1''', (scope.key,)).fetchone()
    return results or ([blocked[0]] if blocked else [])


def process(module, db, row, incoming_id):
    """Called with the existing per-conversation worker lock held."""
    scope = scope_for(row)
    latest = module.latest_incoming_message(db, scope.sender)
    if latest.get('id') != incoming_id:
        return dict(ok=True, reason='conversation_changed')
    if not configured(module, scope.store, scope.channel) or not module.is_ai_enabled(db) or not module.is_customer_ai_enabled(db, scope.sender):
        return dict(ok=True, reason='ai_disabled')

    def allowed(_scope):
        current = route(db, scope.sender)
        latest_id = db.execute("SELECT MAX(id) FROM messages WHERE sender_id=? AND direction='incoming'",
                               (scope.sender,)).fetchone()[0]
        return (current and current['control_revision'] == row['control_revision'] and latest_id == incoming_id
                and module.is_ai_enabled(db) and module.is_customer_ai_enabled(db, scope.sender))

    def mark_handled():
        db.execute('UPDATE sales_v2_live_routes SET handled_id=MAX(handled_id,?) WHERE sender_id=?',
                   (incoming_id, scope.sender))

    def review(reason, result=None):
        result = result or {}
        pause = 'customer_requested_human' if result.get('reason') == 'customer_requested_human' else 'sales_runtime_review'
        with Store(db).transaction():
            if not allowed(scope):
                return dict(ok=True, reason='conversation_changed')
            review_id = result.get('review_id')
            if not review_id and result.get('action') == 'human':
                existing = db.execute("""SELECT id,reason FROM human_reviews WHERE sender_id=? AND store_id=?
                    AND status='pending' AND reason NOT LIKE 'تدقيق لون القطعة للطلب %' ORDER BY id DESC LIMIT 1""",
                    (scope.sender, scope.store)).fetchone()
                if existing:
                    review_id = existing['id']
                    if 'customer_requested_human' in existing['reason']:
                        pause = 'customer_requested_human'
            if not review_id:
                review_id = module.create_human_review(db, dict(latest, sender_id=scope.sender), reason,
                                                       notify_telegram=False, commit=False)
            db.execute("UPDATE sales_v2_outbox SET status='unknown' WHERE scope=? AND status='sending'", (scope.key,))
            db.execute("""UPDATE sales_v2_order_tasks SET status='unknown'
                          WHERE scope=? AND kind='confirmation' AND status='sending'""", (scope.key,))
            module.set_customer_ai_enabled(db, scope.sender, False, reason=pause, commit=False)
            module.cancel_followups_for_sender(db, scope.sender, pause, commit=False)
            mark_handled()
        return dict(ok=True, needs_human=True, human_review_id=review_id, reason=pause)

    if incoming_id <= row['handled_id']:
        return dict(ok=True, reason='already_handled')
    staff_reply_id = db.execute("""SELECT COALESCE(MAX(id),0) FROM messages WHERE sender_id=? AND store_id=?
        AND direction='outgoing' AND json_extract(CASE WHEN json_valid(raw_payload) THEN raw_payload ELSE '{}' END,
        '$.sender_type')='staff'""", (scope.sender, scope.store)).fetchone()[0]
    if staff_reply_id > incoming_id:
        with db:
            mark_handled()
        return dict(ok=True, reason='already_answered')
    external_id = f'incoming:{incoming_id}'
    prior = db.execute('SELECT payload FROM sales_v2_events WHERE scope=? AND external_id=?',
                       (scope.key, external_id)).fetchone()
    if not prior and delivery_state.answered_turn(db, scope.store, scope.sender, incoming_id):
        with db:
            mark_handled()
        return dict(ok=True, reason='already_answered')
    event, audio_pending = ((json.loads(prior[0]), False) if prior
                            else pending_input(module, db, row, incoming_id, staff_reply_id))
    if audio_pending and not sales_playbook.human_requested(event['text']):
        customer = db.execute('SELECT * FROM customers WHERE sender_id=? AND store_id=?',
                              (scope.sender, scope.store)).fetchone()
        voice_input = dict(latest, sender_id=scope.sender, text=event['text'])
        _, voice_review = module.inbound_voice.prepare(
            module, db, voice_input, incoming_id, dict(customer) if customer else {},
            module.get_conversation_history(db, scope.sender),
            after_id=max(row['first_incoming_id'] - 1, row['handled_id'], staff_reply_id))
        if voice_review:
            meta = voice_review.get('meta') or {}
            if meta.get('human_review_id'):
                with db:
                    mark_handled()
            return dict(ok=True, reason=meta.get('reason'), needs_human=bool(meta.get('human_review_id')),
                        human_review_id=meta.get('human_review_id'))
        if not allowed(scope):
            return dict(ok=True, reason='conversation_changed')
        event, _ = pending_input(module, db, row, incoming_id, staff_reply_id)
    if event['media_kind'] == 'sticker' and not event['text']:
        with db:
            mark_handled()
        return dict(ok=True, reason='sticker')
    if not prior:
        staff_product_id = recent_staff_stock_conflict(module, db, scope, incoming_id, staff_reply_id)
        if staff_product_id:
            event['staff_sold_out_product'] = staff_product_id
    entry = make_entry(module, db, scope, allowed)
    try:
        if not prior:
            start_new_purchase(db, scope, incoming_id, event['text'], allowed)
        result = entry.receive(store_id=scope.store, channel=scope.channel, account_id=scope.account,
                               sender_id=scope.sender, message_id=external_id, **event)
        if result['action'] == 'superseded':
            return dict(ok=True, reason='conversation_changed')
        if result['action'] in {'human', 'needs_review', 'service_failure'}:
            response = review('محرك البيع يحتاج مراجعة الموظف', result)
        else:
            deliveries = drain(module, db, entry, scope, incoming_id, allowed)
            response = (review('تحقق من قبول الرسالة لدى مزود الإرسال قبل إعادة المحاولة')
                        if any(status in {'sending','unknown','rejected'} for status in deliveries) else
                        dict(ok=True, action=result['action'], deliveries=deliveries))
        notify_orders(module, db, scope)
        with db:
            mark_handled()
        return response
    except Conflict as exc:
        if str(exc) in {'turn_superseded','state_version_changed','reply_superseded_or_paused',
                        'response_superseded','human_control_active'} or not allowed(scope):
            return dict(ok=True, reason='conversation_changed')
        return review('تعذر إكمال خطوة في محرك البيع؛ راجع المحادثة')
