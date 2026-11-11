"""One scoped backup attempt per customer message, across all recovery phases."""
import time

from ..reply_consultant import scrub
from .contracts import DecisionRejected, DecisionUnavailable
from .store import Conflict


class BackupFailed(DecisionRejected):
    """The single backup attempt did not produce a usable recovery."""


class BackupSuperseded(Conflict):
    """Staff or a newer message superseded the backup in flight."""


def consult(store, scope, event_id, version, stage, callback, context, reason, previous=None):
    if callback is None:
        return None
    stage = 'backup_' + stage
    with store.transaction():
        snapshot = store.snapshot(scope)
        if snapshot['version'] != version or snapshot['data']['control_mode'] != 'active':
            raise BackupSuperseded('backup_turn_superseded_or_paused')
        existing = store.db.execute("""SELECT status,created_at FROM sales_v2_model_attempts
            WHERE scope=? AND event_id=? AND stage IN ('backup_decision','backup_writer','backup_fact')
            LIMIT 1""", (scope.key, event_id)).fetchone()
        if existing:
            if existing[0] == 'running' and time.time() - existing[1] < 60:
                raise DecisionUnavailable('backup_in_progress')
            raise BackupFailed('backup_attempt_already_used')
        attempt = store.db.execute("""INSERT INTO sales_v2_model_attempts
            (scope,event_id,stage,status,created_at) VALUES(?,?,?,'running',?)""",
            (scope.key, event_id, stage, time.time())).lastrowid

    state = context.get('state', {})
    contact = state.get('contact', {})
    relevant = {state.get('focus_product_id'), context.get('reference', {}).get('product_id')}
    relevant.update(line.get('product_id') for line in state.get('cart', []))
    if context.get('products'):
        context = dict(context, products=sorted(context['products'],
                                               key=lambda product: product.get('product_id') not in relevant))
    payload = scrub(dict(context, recovery=dict(reason=reason, previous=previous),
                         known_contact=dict(phone_saved=bool(contact.get('phone')),
                                            address_saved=bool(contact.get('address')),
                                            province=contact.get('province'))), limit=4000)
    try:
        result = callback(payload)
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
