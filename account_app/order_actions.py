"""Durable after-sales requests; closing a review is not executing a change."""
import json


def init(db):
    db.execute("""CREATE TABLE IF NOT EXISTS order_action_requests (
        id INTEGER PRIMARY KEY, sender_id TEXT NOT NULL, order_id INTEGER NOT NULL,
        kind TEXT NOT NULL, value TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
        created_at TEXT NOT NULL, resolved_at TEXT,
        UNIQUE(order_id, kind, value))""")


def record(db, sender, order_id, action, now):
    init(db)
    db.execute("INSERT INTO order_action_requests(sender_id,order_id,kind,value,created_at) VALUES(?,?,?,?,?) ON CONFLICT(order_id,kind,value) DO NOTHING",
               (sender, order_id, action['kind'], action['value'], now))
    db.commit()
    return db.execute("SELECT id FROM order_action_requests WHERE order_id=? AND kind=? AND value=?", (order_id,action['kind'],action['value'])).fetchone()[0]


def pending(db, sender):
    init(db)
    return db.execute("SELECT id FROM order_action_requests WHERE sender_id=? AND status='pending' LIMIT 1", (sender,)).fetchone() is not None


def resolve_applied(db, order, now):
    init(db)
    for row in db.execute("SELECT id,kind,value FROM order_action_requests WHERE order_id=? AND status='pending'", (order['id'],)).fetchall():
        applied = row['kind'] == 'cancel' and order.get('status') in {'cancelled','canceled','ملغي'}
        if row['kind'] == 'size':
            items = json.loads(order.get('order_items') or '[]')
            applied = len(items) == 1 and str(items[0].get('size')) == row['value']
        if applied:
            db.execute("UPDATE order_action_requests SET status='applied',resolved_at=? WHERE id=?", (now,row['id']))
