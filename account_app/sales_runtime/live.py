"""Bounded production rollout through the existing signed intake and AI worker."""
import hashlib
import json
import os
from functools import partial

from .. import ad_attribution, sales_playbook
from .delivery import deliver_confirmation, deliver_replies
from .entrypoint import Entrypoint
from .gateway import BackupGateway, DecisionGateway, WriterGateway
from .orders import migrate_tasks
from .preview import ImageObserver, media_input
from .store import Conflict, Scope, Store, encode


PILOT_STORE = 'khuyoot'
JOB_KIND = 'sales_runtime'


def init_db(db):
    Store(db).migrate()
    migrate_tasks(db)
    db.execute('''CREATE TABLE IF NOT EXISTS sales_v2_live_routes (
        sender_id TEXT PRIMARY KEY, store_id TEXT NOT NULL, account_id TEXT NOT NULL,
        first_incoming_id INTEGER NOT NULL, handled_id INTEGER NOT NULL DEFAULT 0,
        control_revision INTEGER NOT NULL DEFAULT 0)''')
    db.commit()


def percent():
    raw = os.environ.get('SALES_RUNTIME_KHUYOOT_PERCENT', '0')
    try:
        return max(0, min(100, int(raw)))
    except ValueError:
        return 0


def route(db, sender_id):
    if not str(sender_id or '').startswith(f'{PILOT_STORE}::cw-'):
        return None
    return db.execute('SELECT * FROM sales_v2_live_routes WHERE sender_id=?', (sender_id,)).fetchone()


def scope_for(row):
    return Scope(row['store_id'], 'chatwoot', row['account_id'], row['sender_id'])


def configured(module):
    if not percent() or not module.OPENROUTER_KEY or os.environ.get('ENABLE_BACKGROUND_JOBS', '1') != '1':
        return False
    channel = module.chatwoot.configuration()
    return bool(module.chatwoot.enabled() and not module.zernio.enabled_for_store(PILOT_STORE)
                and channel['chatwoot_api_url'].startswith('https://') and channel['chatwoot_key_present']
                and channel['chatwoot_webhook_secret_present'])


def enroll(module, db, store_id, sender_id, account_id, incoming_id):
    """Only signed Chatwoot intake can assign a never-processed conversation."""
    if store_id != PILOT_STORE or not sender_id.startswith(f'{PILOT_STORE}::cw-{account_id}-'):
        return False
    existing = route(db, sender_id)
    if existing:
        if existing['store_id'] != store_id or existing['account_id'] != account_id:
            return False
        db.execute('UPDATE sales_v2_scopes SET version=version+1 WHERE scope=?', (scope_for(existing).key,))
        return True
    bucket = int(hashlib.sha256(sender_id.encode()).hexdigest()[:8], 16) % 100
    if not configured(module) or bucket >= percent() or not module.is_ai_enabled(db):
        return False
    if not module.is_customer_ai_enabled(db, sender_id):
        return False
    cur = db.execute('''INSERT OR IGNORE INTO sales_v2_live_routes
        (sender_id,store_id,account_id,first_incoming_id)
        SELECT ?,?,?,? WHERE NOT EXISTS (SELECT 1 FROM messages WHERE sender_id=? AND id<?)
        AND NOT EXISTS (SELECT 1 FROM orders WHERE sender_id=?)
        AND NOT EXISTS (SELECT 1 FROM human_reviews WHERE sender_id=? AND status='pending')''',
        (sender_id, store_id, account_id, incoming_id, sender_id, incoming_id, sender_id, sender_id))
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
    db.execute('''UPDATE sales_v2_scopes SET data=json_set(data,'$.focus_product_id',?)
                  WHERE scope=?''', (product_id, scope.key))
    db.commit()


def pending_input(module, db, row, incoming_id, staff_reply_id):
    rows = db.execute('''SELECT * FROM messages WHERE sender_id=? AND store_id=? AND direction='incoming'
        AND id>=? AND id>? AND id<=? ORDER BY id''',
        (row['sender_id'], row['store_id'], row['first_incoming_id'],
         max(row['handled_id'], staff_reply_id), incoming_id)).fetchall()
    texts, attachments, seen = [], [], set()
    ad = {}
    for message in rows:
        text = (message['text'] or '').strip()
        if text and text != message['image_url']:
            texts.append(text)
        ad = ad_attribution.extract(json.loads(message['raw_payload'] or '{}')) or ad
        for item in module.message_media(message):
            key = (item['type'], item['url'])
            if key not in seen:
                seen.add(key)
                attachments.append(dict(type=item['type'], payload={'url': item['url']}))
    kind, reference = media_input(attachments)
    # A staff request takes priority even when several messages arrive together.
    text = '\n'.join(texts)
    if any(sales_playbook.human_requested(part) for part in texts):
        text = next(part for part in texts if sales_playbook.human_requested(part))
    return dict(text=text, ad=ad, media_kind=kind, media_ref=reference)


def finish_order(module, db, scope, order, products, held):
    from .contracts import ServiceRequired
    if db.execute("""SELECT 1 FROM orders WHERE store_id=? AND sender_id=? AND id<>?
                    AND status NOT IN ('cancelled','canceled') LIMIT 1""",
                  (scope.store, scope.sender, order['id'])).fetchone():
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
    module.menger.enqueue(db, order['id'], order, products)
    db.execute("""UPDATE sales_v2_order_tasks SET status='prepared',payload=?
                  WHERE order_id=? AND kind='fulfillment'""", (encode(order), order['id']))
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
    observe = ImageObserver(module.inbound_media.fetch, module.match_customer_image_with_catalog)

    def media(current, kind, references, products):
        from .contracts import DecisionUnavailable
        try:
            return observe(current, kind, references, products)
        except DecisionUnavailable:
            return dict(status='unknown')

    return Entrypoint(db, lambda store: module.load_products_from_file(store_id=store),
        lambda store, province: module.delivery_fee_for_province(province, db),
        DecisionGateway(model, module.OPENROUTER_KEY, post), module.get_order_confirmation_text(db),
        media=media, writer=WriterGateway(model, module.OPENROUTER_KEY, post),
        backup=BackupGateway(module.OPENROUTER_KEY, post),
        policy_loader=lambda store: {'delivery_time': module.get_delivery_settings(db)['delivery_time']},
        allow_write=allow_write, on_booking=partial(finish_order, module, db))


def delivery_callbacks(module, db, scope, incoming_id, enabled):
    def send(payload, key):
        if not enabled(scope):
            return dict(paused=True)
        part = {'type': 'text', **payload}
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
    if not configured(module) or not module.is_ai_enabled(db) or not module.is_customer_ai_enabled(db, scope.sender):
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
    event = json.loads(prior[0]) if prior else pending_input(module, db, row, incoming_id, staff_reply_id)
    if event['media_kind'] == 'sticker' and not event['text']:
        with db:
            mark_handled()
        return dict(ok=True, reason='sticker')
    entry = make_entry(module, db, scope, allowed)
    try:
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
