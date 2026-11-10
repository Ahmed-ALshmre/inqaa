"""Durable confirmation delivery; unknown outcomes require reconciliation."""
import json

from .store import Conflict


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
        if not order or order[0] != 'new':
            return dict(status='order_changed')
        store.db.execute("UPDATE sales_v2_order_tasks SET status='sending' WHERE id=?", (task_id,))
    status = 'unknown'
    provider_id = None
    try:
        receipt = send(json.loads(row[1]), f'order:{scope.key}:{row[0]}:confirmation')
        if isinstance(receipt, dict) and receipt.get('accepted') is True and receipt.get('provider_id'):
            status = 'accepted'
            provider_id = str(receipt['provider_id'])
        elif isinstance(receipt, dict) and receipt.get('accepted') is False:
            status = 'rejected'
    except Exception:
        # Persist the uncertain result even on timeout. Do not log customer data.
        status = 'unknown'
    with store.transaction():
        store.db.execute("UPDATE sales_v2_order_tasks SET status=?,provider_id=? WHERE id=? AND scope=? AND status='sending'",
                         (status, provider_id, task_id, scope.key))
    return dict(status=status)


def recover_confirmations(store):
    """Exclusive worker startup only; never resend an interrupted network call."""
    with store.transaction():
        store.db.execute("UPDATE sales_v2_order_tasks SET status='unknown' WHERE kind='confirmation' AND status='sending'")
