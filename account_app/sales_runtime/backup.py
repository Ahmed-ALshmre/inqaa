"""One scoped backup attempt per customer message, across all recovery phases."""
import json
import re
import time

from .. import checkout
from ..reply_consultant import scrub
from .contact import declarations
from .contracts import DecisionRejected, DecisionUnavailable
from .store import Conflict


class BackupFailed(DecisionRejected):
    """The single backup attempt did not produce a usable recovery."""


class BackupSuperseded(Conflict):
    """Staff or a newer message superseded the backup in flight."""


def consultation_history(store, scope, event_id, load_history):
    if load_history:
        history = load_history(scope, event_id, consultation=True)
    else:
        rows = store.db.execute("""SELECT actor,payload FROM sales_v2_events
            WHERE scope=? AND kind='message' AND id<? ORDER BY id""", (scope.key, event_id))
        history = [dict(actor=actor, **{key: value for key, value in json.loads(payload).items()
                                       if key in {'text', 'media_kind'}}) for actor, payload in rows]
    return history


def redact_delivery(text):
    cleaned = []
    for line in text.translate(checkout.DIGITS).splitlines():
        details = declarations(line, {})
        address = details.get('address')
        if address:
            line = line.replace(address, '[عنوان محفوظ]')
        if details.get('phone'):
            line = re.sub(r'(?<!\d)(?:\+?964|00964|0)?[\s-]*7(?:[\s-]*\d){9}(?!\d)',
                          ' [رقم محفوظ]', line)
        cleaned.append(line)
    return '\n'.join(cleaned)


def scrub_context(value):
    if isinstance(value, dict):
        return {key: redact_delivery(item) if key in {'text', 'customer_message'} and isinstance(item, str)
                else scrub_context(item) for key, item in value.items()}
    if isinstance(value, list):
        return [scrub_context(item) for item in value]
    return value


def consult(store, scope, event_id, version, stage, backup, context, reason, previous=None, *, history_loader=None):
    if backup is None:
        return None
    with store.transaction():
        snapshot = store.snapshot(scope)
        if snapshot['version'] != version or snapshot['data']['control_mode'] != 'active':
            raise BackupSuperseded('backup_turn_superseded_or_paused')
        event = store.evidence(scope, event_id)
        external_id = store.db.execute('SELECT external_id FROM sales_v2_events WHERE scope=? AND id=?',
                                       (scope.key, event_id)).fetchone()[0]
        incoming = re.fullmatch(r'(incoming:\d+)(?::resume:\d+)?', external_id)
        if incoming:
            original = incoming[1]
            turn_ids = """SELECT id FROM sales_v2_events WHERE scope=? AND
                          (external_id=? OR external_id GLOB ?)"""
            params = (scope.key, original, original + ':resume:*')
        else:
            turn_ids, params = '?', (event_id,)
        existing = store.db.execute(f"""SELECT status,created_at FROM sales_v2_model_attempts
            WHERE scope=? AND event_id IN ({turn_ids}) AND stage IN
            ('backup_decision','backup_writer','backup_fact','sol_decision','sol_writer','sol_fact')
            LIMIT 1""", (scope.key, *params)).fetchone()
        if existing:
            if existing[0] == 'running' and time.time() - existing[1] < 60:
                raise DecisionUnavailable('backup_in_progress')
            raise BackupFailed('backup_attempt_already_used')
        attempt = store.db.execute("""INSERT INTO sales_v2_model_attempts
            (scope,event_id,stage,status,created_at) VALUES(?,?,?,'running',?)""",
            (scope.key, event_id, 'sol_' + stage, time.time())).lastrowid

    state = snapshot['data']
    contact = state.get('contact', {})
    relevant = {state.get('focus_product_id'), (context.get('reference') or {}).get('product_id')}
    relevant.update(line.get('product_id') for line in state.get('cart', []))
    product_list = context.get('products', [])
    if product_list:
        context = dict(context, products=sorted(product_list,
                                               key=lambda product: product.get('product_id') not in relevant))
    payload = scrub(dict(scrub_context(context), state=state, recovery=dict(stage=stage, reason=reason, previous=previous),
                         known_contact=dict(phone_saved=bool(contact.get('phone')),
                                            address_saved=bool(contact.get('address')),
                                            province=contact.get('province'))), limit=4000)
    # Keep history separate from the short regular prompt and its per-list cap.
    try:
        if product_list:
            payload['products'] = [scrub(product, limit=4000) for product in context['products']]
        current = event['payload']
        payload['current_message'] = scrub(dict(text=redact_delivery(current.get('text') or ''),
                                                media_kind=current.get('media_kind'),
                                                ad=current.get('ad'),
                                                observation=state.get('media_observations', {}).get(str(event_id))),
                                           limit=8000)
        history = consultation_history(store, scope, event_id, history_loader)
        budget = 600000 - len(json.dumps(payload, ensure_ascii=False))
        tail = []
        for message in reversed(history):
            if message.get('actor') == 'customer':
                message = dict(message, text=redact_delivery(message.get('text') or ''))
            message = scrub(message, limit=8000)
            budget -= len(json.dumps(message, ensure_ascii=False)) + 2
            if budget < 0:
                break
            tail.append(message)
        payload['conversation_history'] = list(reversed(tail))
        payload['older_messages_omitted'] = len(history) - len(tail)
        address = contact.get('address')
        if address:
            payload = json.loads(json.dumps(payload, ensure_ascii=False).replace(
                json.dumps(address, ensure_ascii=False)[1:-1], '[عنوان محفوظ]'))
        result = backup.advise(payload)
        status, error = ('returned', None) if result is not None else ('failed', 'no_backup_proposal')
    except (DecisionRejected, DecisionUnavailable, ValueError) as exc:
        status, error, result = 'failed', type(exc).__name__, None
    with store.transaction():
        store.db.execute("UPDATE sales_v2_model_attempts SET status=?,error_code=? WHERE id=?",
                         (status, error, attempt))
    snapshot = store.snapshot(scope)
    if snapshot['version'] != version or snapshot['data']['control_mode'] != 'active':
        raise BackupSuperseded('backup_turn_superseded_or_paused')
    if result is None:
        raise BackupFailed('backup_model_failed')
    return result


def consultation_markers(db, scope, message_ids):
    if not message_ids:
        return {}
    ids = [f'incoming:{i}' for i in sorted(set(message_ids))]
    values = ','.join('(?)' for _ in ids)
    rows = db.execute(f"""WITH requested(external_id) AS (VALUES {values})
        SELECT SUBSTR(requested.external_id,10), attempt.status
        FROM requested JOIN sales_v2_events AS event
          ON event.scope=? AND (event.external_id=requested.external_id OR
             event.external_id GLOB requested.external_id || ':resume:*')
        JOIN sales_v2_model_attempts AS attempt
          ON attempt.scope=event.scope AND attempt.event_id=event.id
        WHERE attempt.stage IN ('sol_decision','sol_writer','sol_fact') ORDER BY attempt.id""",
        (*ids, scope.key)).fetchall()
    return {str(row[0]): row[1] for row in rows}
