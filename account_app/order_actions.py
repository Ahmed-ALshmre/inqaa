"""Durable after-sales requests; closing a review is not executing a change."""
import json
import re
try:
    from .checkout import normalized
except ImportError:
    from checkout import normalized


def size_key(value):
    text = normalized(value).strip()
    text = re.sub(r'^(?:عمر|قياس)\s*', '', text)
    text = {'سنتين': '2 سنة', 'سنه': '1 سنة', 'سنة': '1 سنة',
            'سنتان': '2 سنة', 'شهرين': '2 شهر', 'شهر': '1 شهر'}.get(text, text)
    return re.sub(r'سنه|سنوات|سنين', 'سنة', re.sub(r'اشهر|شهور', 'شهر', text))


def single_size(order):
    try:
        items = json.loads(order.get('order_items') or '[]')
    except (ValueError, TypeError):
        return ''
    if not isinstance(items, list) or len(items) > 1:
        return ''
    return (items[0].get('size') or '') if items and isinstance(items[0], dict) else order.get('size') or ''


def init(db):
    db.execute("""CREATE TABLE IF NOT EXISTS order_action_requests (
        id INTEGER PRIMARY KEY, sender_id TEXT NOT NULL, order_id INTEGER NOT NULL,
        kind TEXT NOT NULL, value TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL, resolved_at TEXT,
        UNIQUE(order_id, kind, value))""")


def record(db, sender, order_id, action, now):
    init(db)
    # The same size/cancellation may be requested again after another change.
    # Keep pending duplicates idempotent, but never let a previously resolved
    # request make the new instruction disappear from the staff review queue.
    db.execute("""INSERT INTO order_action_requests(sender_id,order_id,kind,value,created_at) VALUES(?,?,?,?,?)
        ON CONFLICT(order_id,kind,value) DO UPDATE SET status='pending',resolved_at=NULL,created_at=excluded.created_at
        WHERE order_action_requests.status<>'pending' AND order_action_requests.sender_id=excluded.sender_id""",
               (sender, order_id, action['kind'], action['value'], now))
    db.commit()
    return db.execute("SELECT id FROM order_action_requests WHERE order_id=? AND kind=? AND value=?", (order_id,action['kind'],action['value'])).fetchone()[0]


def pending(db, sender):
    init(db)
    reconcile(db, sender)
    return db.execute("SELECT id FROM order_action_requests WHERE sender_id=? AND status='pending' LIMIT 1", (sender,)).fetchone() is not None


def reconcile(db, sender=None):
    """Recover completed actions from the saved order before blocking review closure."""
    init(db)
    clause = ' AND r.sender_id=?' if sender is not None else ''
    params = (sender,) if sender is not None else ()
    orders = db.execute('''SELECT DISTINCT o.* FROM orders o
        JOIN order_action_requests r ON r.order_id=o.id AND r.sender_id=o.sender_id
        WHERE r.status='pending' ''' + clause, params).fetchall()
    from datetime import datetime
    from zoneinfo import ZoneInfo
    now = datetime.now(ZoneInfo('Asia/Baghdad')).isoformat()
    with db:
        for order in orders:
            resolve_applied(db, dict(order), now)


def resolve_applied(db, order, now):
    init(db)
    for row in db.execute("SELECT id,kind,value FROM order_action_requests WHERE order_id=? AND sender_id=? AND status='pending'", (order['id'], order.get('sender_id'))).fetchall():
        applied = row['kind'] == 'cancel' and order.get('status') in {'cancelled','canceled','ملغي'}
        if row['kind'] == 'size':
            try:
                items = json.loads(order.get('order_items') or '[]')
            except (ValueError, TypeError):
                items = []
            if isinstance(items, list):
                applied = len(items) == 1 and isinstance(items[0], dict) and size_key(items[0].get('size', '')) == size_key(row['value'])
                if not items:
                    applied = size_key(order.get('size') or '') == size_key(row['value'])
        if applied:
            db.execute("UPDATE order_action_requests SET status='applied',resolved_at=? WHERE id=?", (now,row['id']))
