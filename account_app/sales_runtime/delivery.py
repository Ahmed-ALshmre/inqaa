"""Durable confirmation delivery; unknown outcomes require reconciliation."""
import json

from ..checkout import COLOR_REVIEW_STATUS
from .store import Conflict


def _send_once(send, payload, key):
    try:
        receipt = send(payload, key)
        if isinstance(receipt, dict):
            if receipt.get('accepted') is True and receipt.get('provider_id'):
                return 'accepted', str(receipt['provider_id'])
            if receipt.get('accepted') is False:
                return 'rejected', None
    except Exception:
        # A timeout may have happened after the provider accepted the message.
        pass
    return 'unknown', None


def deliver_replies(entry, scope, send, can_send):
    """Drain ordered reply parts under the caller's channel lease."""
    results = []
    while can_send(scope):
        part = entry.store.claim_reply(scope)
        if part is None:
            break
        payload = part['payload']
        status, provider_id = _send_once(send, payload, f'reply:{scope.key}:{part["id"]}')
        entry.store.settle_reply(scope, part['id'], status, provider_id)
        results.append(status)
        if status != 'accepted':
            break
        if payload.get('purpose') == 'checkout_contacts':
            version = entry.store.snapshot(scope)['version']
            entry.runtime.accept_checkout_prompt(scope, f'prompt:{part["id"]}', version, part['id'])
    return results


def deliver_confirmation(store, scope, task_id, send, can_send):
    """send(payload, stable_key) returns a channel receipt; no blind retry.

    Caller owns the per-conversation channel lease through the network call.
    can_send checks live store and conversation permissions under that lease.
    A channel receipt means provider acceptance, not customer read/delivery.
    """
    if not can_send(scope):
        return dict(status='paused')
    with store.transaction():
        row = store.db.execute('''SELECT order_id,payload,status FROM sales_v2_order_tasks
            WHERE scope=? AND id=? AND kind='confirmation' ''', (scope.key, task_id)).fetchone()
        if not row:
            raise Conflict('confirmation_not_in_scope')
        if row[2] != 'queued':
            return dict(status=row[2])
        if store.snapshot(scope)['data']['control_mode'] != 'active':
            return dict(status='paused')
        order = store.db.execute('SELECT status FROM orders WHERE id=? AND store_id=? AND sender_id=?',
                                 (row[0], scope.store, scope.sender)).fetchone()
        if not order or order[0] not in {'new', COLOR_REVIEW_STATUS}:
            return dict(status='order_changed')
        store.db.execute("UPDATE sales_v2_order_tasks SET status='sending' WHERE id=?", (task_id,))
    status, provider_id = _send_once(send, json.loads(row[1]), f'order:{scope.key}:{row[0]}:confirmation')
    with store.transaction():
        store.db.execute("UPDATE sales_v2_order_tasks SET status=?,provider_id=? WHERE id=? AND scope=? AND status='sending'",
                         (status, provider_id, task_id, scope.key))
    return dict(status=status)


def recover_confirmations(store):
    """Exclusive worker startup only; never resend an interrupted network call."""
    with store.transaction():
        store.db.execute("UPDATE sales_v2_order_tasks SET status='unknown' WHERE kind='confirmation' AND status='sending'")
