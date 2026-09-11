"""Durable order delivery. Payloads are frozen in the booking transaction."""
import json
import os
import re
import sqlite3
import threading
import time
from urllib.parse import urlparse

import requests


def init_db(db):
    db.execute("""CREATE TABLE IF NOT EXISTS menger_store_settings (
        local_store TEXT PRIMARY KEY, api_url TEXT NOT NULL DEFAULT '',
        api_key TEXT NOT NULL DEFAULT '', store_id TEXT NOT NULL DEFAULT '',
        source TEXT NOT NULL DEFAULT 'lamsa-project', telegram_chat_id TEXT NOT NULL DEFAULT ''
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS menger_connection (
        id INTEGER PRIMARY KEY CHECK(id=1), api_url TEXT NOT NULL DEFAULT '',
        api_key TEXT NOT NULL DEFAULT '', source TEXT NOT NULL DEFAULT 'lamsa-project'
    )""")
    # Preserve an unambiguous previous connection; conflicting store connections
    # require selecting one shared connection in the settings screen.
    if not db.execute('SELECT 1 FROM menger_connection').fetchone():
        previous = db.execute("SELECT DISTINCT api_url,api_key,source FROM menger_store_settings WHERE api_url!='' AND api_key!=''").fetchall()
        if len(previous) == 1:
            db.execute('INSERT INTO menger_connection VALUES(1,?,?,?)', tuple(previous[0]))
    db.execute("""CREATE TABLE IF NOT EXISTS menger_deliveries (
        order_id INTEGER PRIMARY KEY, local_store TEXT NOT NULL,
        payload TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
        remote_order_id TEXT, last_error TEXT, synced_at REAL
    )""")


def connection(db, public=False):
    row = db.execute('SELECT api_url,api_key,source FROM menger_connection WHERE id=1').fetchone()
    data = dict(row) if row else dict(api_url=os.environ.get('MENGER_ORDERS_API_URL', '').strip(),
                                    api_key=os.environ.get('MENGER_ORDERS_API_KEY', '').strip(), source='lamsa-project')
    if public:
        data['key_configured'] = bool(data.pop('api_key'))
    return data


def save_connection(db, data):
    current = connection(db)
    for field in ('api_url', 'source'):
        if field in data:
            current[field] = str(data[field] or '').strip()
    if data.get('api_key'):
        current['api_key'] = str(data['api_key']).strip()
    if data.get('clear_api_key') is True:
        current['api_key'] = ''
    if current['api_url']:
        parsed = urlparse(current['api_url'])
        if parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password or parsed.fragment:
            raise ValueError('أدخل رابط API صحيحاً يبدأ بـ https://')
    if not current['source']:
        raise ValueError('اسم المشروع المرسل مطلوب')
    db.execute('INSERT INTO menger_connection VALUES(1,?,?,?) ON CONFLICT(id) DO UPDATE SET api_url=excluded.api_url,api_key=excluded.api_key,source=excluded.source',
               tuple(current[k] for k in ('api_url','api_key','source')))
    # The old per-store credential fields are no longer used.
    db.execute("UPDATE menger_store_settings SET api_url='',api_key=''")


def settings(db, store, public=False):
    row = db.execute('SELECT store_id,telegram_chat_id FROM menger_store_settings WHERE local_store=?', (store,)).fetchone()
    return dict(row) if row else dict(store_id='', telegram_chat_id='')


def save_settings(db, store, data):
    if any(field in data for field in ('api_url','api_key','source','clear_api_key')):
        raise ValueError('احفظ رابط API والمفتاح من إعدادات الربط الموحدة أعلى الصفحة')
    current = settings(db, store)
    for field in ('store_id', 'telegram_chat_id'):
        if field in data:
            current[field] = str(data[field] or '').strip()
    db.execute('INSERT INTO menger_store_settings(local_store,store_id,telegram_chat_id) VALUES(?,?,?) ON CONFLICT(local_store) DO UPDATE SET store_id=excluded.store_id,telegram_chat_id=excluded.telegram_chat_id',
               (store, current['store_id'], current['telegram_chat_id']))


def snapshot_prices(order, catalog):
    products = {p['product_id']: p for p in catalog}
    for item in order['items']:
        product = products.get(item.get('product_id'), {})
        item.pop('send_to', None)
        item['order_name'] = str(product.get('order_name') or '').strip() or item['product_name']
        item['unit_price'] = int(re.sub(r'\D', '', str(item.get('unit_price') or product.get('price') or '')) or '0')


def enqueue(db, order_id, order, catalog):
    init_db(db)
    products = {p['product_id']: p for p in catalog}
    items = []
    for item in order['items']:
        product = products.get(item.get('product_id'), {})
        price = re.sub(r'\D', '', str(item.get('unit_price') or product.get('price') or ''))
        items.append(dict(product_name=item.get('order_name') or item['product_name'],
                          quantity=int(item.get('quantity') or 1),
                          color=item.get('color') or '', size=item.get('size') or '',
                          unit_price=int(price) if price else 0))
    if not items:
        return
    config = connection(db)
    payload = dict(source=config['source'], external_order_id=str(order_id),
                   created_at=order['created_at'],
                   customer=dict(name=order.get('customer_name') or '', phone=order['phone'],
                                 province=order['province'], address=order['address']),
                   items=items, total_price=sum(i['unit_price'] * i['quantity'] for i in items), notes=order.get('notes') or '')
    if order.get('is_paid'):
        payload['total_price'] = 0
        payload['notes'] = paid_notes(payload['notes'])
    db.execute('INSERT OR IGNORE INTO menger_deliveries(order_id,local_store,payload) VALUES(?,?,?)',
               (order_id, order.get('store_id') or 'default', json.dumps(payload, ensure_ascii=False)))


def paid_notes(notes):
    text = str(notes or '').strip()
    marker = 'مدفوع بالكامل شاملاً التوصيل — لا يُحصّل أي مبلغ من الزبون'
    return text if marker in text else '\n'.join(filter(None, [text, marker]))


def deliver_due(db, post=None, now=None):
    post = post or requests.post
    now = time.time() if now is None else now
    shared = connection(db)
    url, key = shared['api_url'], shared['api_key']
    try:
        stores = json.loads(os.environ.get('MENGER_STORE_MAP', '{}'))
        if not isinstance(stores, dict):
            stores = {}
    except ValueError:
        stores = {}
    default = os.environ.get('MENGER_STORE_ID', '').strip()
    if default:
        stores.setdefault('default', default)
    rows = db.execute("SELECT * FROM menger_deliveries WHERE status IN ('pending','retry','sending') AND next_attempt<=? ORDER BY order_id", (now,)).fetchall()
    delivered = 0
    for row in rows:
        if delivered >= 20:
            break
        config = settings(db, row['local_store'])
        configured = db.execute('SELECT 1 FROM menger_store_settings WHERE local_store=?', (row['local_store'],)).fetchone()
        target_url, target_key = url, key
        parsed = urlparse(target_url)
        if not target_key or parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password:
            continue
        payload = json.loads(row['payload'])
        destination = payload.get('store_id') or (config['store_id'] if configured else stores.get(row['local_store']))
        if not isinstance(destination, str) or not destination.strip():
            continue
        delivered += 1
        payload['store_id'] = destination.strip()
        # Atomic lease supports several server processes; destination stays fixed on retry.
        claim = db.execute("UPDATE menger_deliveries SET status='sending', next_attempt=?, attempts=attempts+1,payload=? WHERE order_id=? AND status IN ('pending','retry','sending') AND next_attempt<=?",
                           (now + 120, json.dumps(payload, ensure_ascii=False), row['order_id'], now))
        db.commit()
        if not claim.rowcount:
            continue
        status, remote, error = 'retry', None, 'network_error'
        try:
            response = post(target_url, json=payload, headers={'Authorization': f'Bearer {target_key}',
                            'Content-Type': 'application/json'}, timeout=20, allow_redirects=False)
            if response.status_code in (200, 201):
                try:
                    result = response.json()
                except ValueError:
                    result = {}
                if isinstance(result, dict) and result.get('ok') is True and isinstance(result.get('order_id'), str) and result['order_id'].strip():
                    status, remote, error = 'sent', result['order_id'], None
                else:
                    error = 'invalid_success_response'
            else:
                status = 'retry' if response.status_code in (500, 502, 503, 504) else 'blocked'
                error = f'http_{response.status_code}'
        except requests.RequestException:
            pass
        delay = min(3600, 60 * 2 ** min(row['attempts'], 6))
        db.execute('UPDATE menger_deliveries SET status=?,remote_order_id=?,last_error=?,next_attempt=?,synced_at=? WHERE order_id=?',
                   (status, remote, error, now + delay, now if status == 'sent' else None, row['order_id']))
        db.commit()


def start_worker(db_path):
    def loop():
        while True:
            db = None
            try:
                db = sqlite3.connect(db_path, timeout=30)
                db.row_factory = sqlite3.Row
                deliver_due(db)
            except Exception:
                # The durable lease expires so a subsequent cycle can recover.
                pass
            finally:
                if db is not None:
                    db.close()
            time.sleep(5)
    threading.Thread(target=loop, daemon=True, name='menger-orders').start()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Inspect Menger delivery status or retry a corrected blocked order.')
    parser.add_argument('--db', required=True, help='Existing sales.db path')
    parser.add_argument('--retry-order', type=int)
    args = parser.parse_args()
    if not os.path.isfile(args.db):
        parser.error('Database does not exist')
    with sqlite3.connect(args.db, timeout=30) as db:
        db.row_factory = sqlite3.Row
        init_db(db)
        if args.retry_order is not None:
            db.execute("UPDATE menger_deliveries SET status='pending',next_attempt=0 WHERE order_id=? AND status='blocked'", (args.retry_order,))
        rows = db.execute('SELECT order_id,local_store,status,attempts,remote_order_id,last_error FROM menger_deliveries ORDER BY order_id DESC LIMIT 100').fetchall()
        print(json.dumps([dict(row) for row in rows], ensure_ascii=False, indent=2))
